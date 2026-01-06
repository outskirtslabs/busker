(ns ol.busker.native
  (:require
   [clojure.string :as str]
   [coffi.ffi :as ffi :refer [defcfn]]
   [coffi.layout :as layout]
   [coffi.mem :as mem]
   [ol.busker.native.loader])
  (:import
   [java.io InputStream]
   [java.lang.foreign MemorySegment]))

(set! *warn-on-reflection* true)

(def ^:const H2O_SOCKET_FLAG_DONT_READ 0x20)

(def ^:const H2O_SEND_STATE_IN_PROGRESS 0)
(def ^:const H2O_SEND_STATE_FINAL 1)
(def ^:const H2O_SEND_STATE_ERROR 2)

(def ^:const CLJ_HANDLER_OK 0)
(def ^:const CLJ_HANDLER_DECLINED -1)
(def ^:const CLJ_HANDLER_OVERLOADED -2)
(def ^:const CLJ_HANDLER_SHUTTING_DOWN -3)

(def ^{:const true :doc "Let h2o negotiate compression based on the configuration"}
  H2O_COMPRESS_HINT_AUTO 0)
(def ^{:const true :doc "Compression explictly disabled for the request"}
  H2O_COMPRESS_HINT_DISABLE 1)
(def ^{:const true :doc "Compression negotiation explicitly enabled for this request"}
  H2O_COMPRESS_HINT_ENABLE 2)
(def ^{:const true :doc "Compression negotiation explicitly enabled for this request, preferring gzip"}
  H2O_COMPRESS_HINT_ENABLE_GZIP 3)
(def ^{:const true :doc "Compression negotiation explicitly enabled for this request, preferring brotli"}
  H2O_COMPRESS_HINT_ENABLE_BR 4)
(def ^{:const true :doc "Compression negotiation explicitly enabled for this request, preferring zstd"}
  H2O_COMPRESS_HINT_ENABLE_ZSTD 5)

(def ^{:const true} ->compress-hint
  {:h2o.compress/auto        H2O_COMPRESS_HINT_AUTO
   :h2o.compress/disable     H2O_COMPRESS_HINT_DISABLE
   :h2o.compress/enable      H2O_COMPRESS_HINT_ENABLE
   :h2o.compress/enable-gzip H2O_COMPRESS_HINT_ENABLE_GZIP
   :h2o.compress/enable-br   H2O_COMPRESS_HINT_ENABLE_BR
   :h2o.compress/enable-zstd H2O_COMPRESS_HINT_ENABLE_ZSTD})

(import 'java.lang.foreign.MemoryLayout)
(import 'java.lang.foreign.MemoryLayout$PathElement)

(defn cstr-array->string
  "Convert a null-terminated C string (char array) to a Clojure string."
  [char-array]
  (let [sb (StringBuilder.)]
    (loop [v (seq char-array)]
      (when-let [b (first v)]
        (let [octet (bit-and (long b) 0xFF)]
          (when (pos? octet)
            (.append sb (char octet))
            (recur (next v))))))
    (str sb)))

(defn offset-of
  "Given a `struct-def`, returns the byte offset of the `field`."
  [struct-def field]
  (let [layout ^MemoryLayout (mem/c-layout struct-def)
        path-elts
        ^"[Ljava.lang.foreign.MemoryLayout$PathElement;"
        (into-array MemoryLayout$PathElement
                    [(MemoryLayout$PathElement/groupElement (name field))])]
    (.byteOffset layout path-elts)))

(defn print-offsets-for [struct-def-vec]
  (let [layout (layout/with-c-layout struct-def-vec)
        [_struct-type fields] struct-def-vec
        struct-name (or (some-> struct-def-vec meta :name str) "struct")]
    (println (str "COFFI: " struct-name " field offsets:"))
    (println (str "  sizeof(" struct-name ") = " (mem/size-of layout)))
    (doseq [[field-name _field-type] fields]
      (println (str "  " (name field-name) " = " (offset-of layout field-name))))))

(mem/defalias ::clj-header-t
  (layout/with-c-layout
    [::mem/struct
     [[:name ::mem/pointer]
      [:name_len ::mem/int]
      [:value ::mem/pointer]
      [:value_len ::mem/int]]]))

;; h2o_iovec_t is:
;;   typedef struct { char *base; size_t len; } h2o_iovec_t;
(mem/defalias ::h2o-iovec-t
  (layout/with-c-layout
    [::mem/struct
     [[:base ::mem/pointer]
      [:len ::mem/long]]]))

;; h2o_accept_ctx_t structure
;; typedef struct st_h2o_accept_ctx_t {
;;     h2o_context_t *ctx;
;;     h2o_hostconf_t **hosts;
;;     SSL_CTX *ssl_ctx;
;;     h2o_iovec_t *http2_origin_frame;
;;     int expect_proxy_line;
;;     h2o_multithread_receiver_t *libmemcached_receiver;
;; } h2o_accept_ctx_t;
(mem/defalias ::h2o-accept-ctx-t
  (layout/with-c-layout
    [::mem/struct
     [[:ctx ::mem/pointer]
      [:hosts ::mem/pointer]
      [:ssl_ctx ::mem/pointer]
      [:http2_origin_frame ::mem/pointer]
      [:expect_proxy_line ::mem/int]
      [:libmemcached_receiver ::mem/pointer]]]))

(mem/defalias ::h2o-sendvec-t
  (layout/with-c-layout
    [::mem/struct
     [[:callbacks ::mem/pointer]
      [:len ::mem/long]
      ;; Union of raw pointer OR cb_arg[2], takes 16 bytes (size of larger member)
      [:raw ::mem/pointer]
      [:cb_arg_padding ::mem/long]]]))

(def size-of-h2o-sendvec-t (mem/size-of ::h2o-sendvec-t))

;; h2o_header_t structure
;; typedef struct {
;;     h2o_iovec_t *name;
;;     const char *orig_name;
;;     h2o_iovec_t value;
;;     h2o_header_flags_t flags;
;; } h2o_header_t;
(mem/defalias ::h2o-header-t
  (layout/with-c-layout
    [::mem/struct
     [[:name ::mem/pointer]
      [:orig_name ::mem/pointer]
      [:value ::h2o-iovec-t]
      [:flags ::mem/char]]]))

;; h2o_generator_t structure
;; typedef struct st_h2o_generator_t {
;;     void (*proceed)(struct st_h2o_generator_t *self, h2o_req_t *req);
;;     void (*stop)(struct st_h2o_generator_t *self, h2o_req_t *req);
;; } h2o_generator_t;
(mem/defalias ::h2o-generator-t
  (layout/with-c-layout
    [::mem/struct
     [[:proceed ::mem/pointer]
      [:stop ::mem/pointer]]]))

(mem/defalias ::clj-req-meta-t
  (layout/with-c-layout
    [::mem/struct
     [[:authority ::mem/pointer]
      [:method ::mem/pointer]
      [:path ::mem/pointer]
      [:remote_addr ::mem/pointer]
      [:scheme ::mem/pointer]
      [:headers ::mem/pointer]

      [:authority_len ::mem/long]
      [:method_len ::mem/long]
      [:path_len ::mem/long]
      [:remote_addr_len ::mem/long]
      [:scheme_len ::mem/long]
      [:headers_len ::mem/long]

      [:http_version ::mem/int]
      [:has_body ::mem/short]
      [:is_early_data ::mem/short]]]))
#_(print-offsets-for (layout/with-c-layout
                       [::mem/struct
                        [[:authority ::mem/pointer]
                         [:method ::mem/pointer]
                         [:path ::mem/pointer]
                         [:remote_addr ::mem/pointer]
                         [:scheme ::mem/pointer]
                         [:headers ::mem/pointer]

                         [:authority_len ::mem/long]
                         [:method_len ::mem/long]
                         [:path_len ::mem/long]
                         [:remote_addr_len ::mem/long]
                         [:scheme_len ::mem/long]
                         [:headers_len ::mem/long]

                         [:http_version ::mem/int]
                         [:has_body ::mem/int]]]))

(mem/defalias ::clj-req-ctx-t
  (layout/with-c-layout
    [::mem/struct
     [[:req ::mem/pointer]
      [:meta ::clj-req-meta-t]
      [:on-cleanup ::mem/pointer]
      [:on-request-body-chunk ::mem/pointer]
      [:generator ::h2o-generator-t]
      [:on-response-generator-proceed ::mem/pointer]
      [:on-response-generator-stop ::mem/pointer]
      [:preferred-chunk-size ::mem/long]
      [:req-id [::mem/array ::mem/char 64]]
      [:cleanup ::mem/int]
      [:closing ::mem/int]
      [:response_started ::mem/int]]]))

#_(print-offsets-for (layout/with-c-layout
                       [::mem/struct
                        [[:req ::mem/pointer]
                         [:meta ::clj-req-meta-t]
                         [:on-cleanup ::mem/pointer]
                         [:on-request-body-chunk ::mem/pointer]
                         [:generator ::h2o-generator-t]
                         [:on-response-generator-proceed ::mem/pointer]
                         [:on-response-generator-stop ::mem/pointer]
                         [:preferred-chunk-size ::mem/long]
                         [:req-id [::mem/array ::mem/char 64]]
                         [:cleanup ::mem/int]
                         [:closing ::mem/int]
                         [:response_started ::mem/int]]]))

(mem/defalias ::clj-h2o-flat-globalconf-t
  (layout/with-c-layout
    [::mem/struct
     [[:has_server_name ::mem/int]
      [:server_name ::mem/c-string]

      [:has_proxy_status_identity ::mem/int]
      [:proxy_status_identity ::mem/c-string]

      [:has_max_request_entity_size ::mem/int]
      [:max_request_entity_size ::mem/long]

      [:has_max_delegations ::mem/int]
      [:max_delegations ::mem/int]

      [:has_max_reprocesses ::mem/int]
      [:max_reprocesses ::mem/int]

      [:has_handshake_timeout ::mem/int]
      [:handshake_timeout ::mem/long]

      [:has_max_spare_pipes ::mem/int]
      [:max_spare_pipes ::mem/long]

      [:has_http1__req_timeout ::mem/int]
      [:http1__req_timeout ::mem/long]

      [:has_http1__req_io_timeout ::mem/int]
      [:http1__req_io_timeout ::mem/long]

      [:has_http1__upgrade_to_http2 ::mem/int]
      [:http1__upgrade_to_http2 ::mem/int]

      [:has_http2__idle_timeout ::mem/int]
      [:http2__idle_timeout ::mem/long]

      [:has_http2__graceful_shutdown_timeout ::mem/int]
      [:http2__graceful_shutdown_timeout ::mem/long]

      [:has_http2__max_streams ::mem/int]
      [:http2__max_streams ::mem/int]

      [:has_http2__max_concurrent_requests_per_connection ::mem/int]
      [:http2__max_concurrent_requests_per_connection ::mem/long]

      [:has_http2__max_concurrent_streaming_requests_per_connection ::mem/int]
      [:http2__max_concurrent_streaming_requests_per_connection ::mem/long]

      [:has_http2__max_streams_for_priority ::mem/int]
      [:http2__max_streams_for_priority ::mem/long]

      [:has_http2__active_stream_window_size ::mem/int]
      [:http2__active_stream_window_size ::mem/int]

      [:has_http2__dos_delay ::mem/int]
      [:http2__dos_delay ::mem/long]

      [:has_http3__idle_timeout ::mem/int]
      [:http3__idle_timeout ::mem/long]

      [:has_http3__graceful_shutdown_timeout ::mem/int]
      [:http3__graceful_shutdown_timeout ::mem/long]

      [:has_http3__active_stream_window_size ::mem/int]
      [:http3__active_stream_window_size ::mem/int]

      [:has_http3__ack_frequency ::mem/int]
      [:http3__ack_frequency ::mem/int]

      [:has_compress_args ::mem/int]
      [:compress_args_mine_size ::mem/long]
      [:compress_args_gzip_quality ::mem/int]
      [:compress_args_brotli_quality ::mem/int]
      [:compress_args_zstd_quality ::mem/int]]]))

#_(print-offsets-for
   (layout/with-c-layout
     [::mem/struct
      [[:req ::mem/pointer]
       [:meta ::clj-req-meta-t]
       [:on-cleanup ::mem/pointer]
       [:generator ::h2o-generator-t]
       [:cleanup ::mem/int]]]))

(defcfn evloop-create
  "Creates a new event loop. Returns a pointer to h2o_evloop_t."
  h2o_evloop_create
  [] ::mem/pointer)

(defcfn evloop-destroy
  "Destroys an event loop and frees associated resources."
  h2o_evloop_destroy
  [::mem/pointer] ::mem/void)

(defcfn evloop-run
  "Runs the event loop once. Returns 0 if successful, -1 on error (typically EINTR).

   Parameters:
   - loop: pointer to h2o_evloop_t
   - max-wait: maximum time to wait in milliseconds (int32)"
  h2o_evloop_run
  [::mem/pointer ::mem/int] ::mem/int)

(defcfn globalconf-size
  "Get size of h2o_globalconf_t structure"
  clj_h2o_globalconf_size
  [] ::mem/long)

(defcfn create-global-conf
  "Create, initialize, and configure a new h2o_globalconf_t with our configuration"
  clj_h2o_create_globalconf
  [::mem/pointer ::mem/pointer] ::mem/void)

(defcfn config-dispose
  "Dispose h2o global configuration and free resources"
  h2o_config_dispose
  [::mem/pointer] ::mem/void)

(defcfn context-size
  "Get size of h2o_context_t structure"
  clj_h2o_context_size
  [] ::mem/long)

(defcfn accept-ctx-size
  "Get size of h2o_accept_ctx_t structure"
  clj_h2o_accept_ctx_size
  [] ::mem/long)

(defcfn globalconf-get-hosts
  "Get hosts pointer (h2o_hostconf_t**) from h2o_globalconf_t"
  clj_h2o_globalconf_get_hosts
  [::mem/pointer] ::mem/pointer)

(defcfn config-register-host
  "Register a virtual host with the h2o configuration.
   Returns pointer to h2o_hostconf_t"
  h2o_config_register_host
  [::mem/pointer ::h2o-iovec-t ::mem/int] ::mem/pointer)

(defcfn context-init
  "Initialize h2o context for an event loop.

   Parameters:
   - context: pointer to h2o_context_t
   - loop: pointer to h2o_evloop_t
   - config: pointer to h2o_globalconf_t"
  h2o_context_init
  [::mem/pointer ::mem/pointer ::mem/pointer] ::mem/void)

(defcfn context-dispose
  "Dispose h2o context and free resources"
  h2o_context_dispose
  [::mem/pointer] ::mem/void)

(defcfn context-request-shutdown
  "Request graceful shutdown of all connections in this context (sends GOAWAY)"
  h2o_context_request_shutdown
  [::mem/pointer] ::mem/void)

(defcfn evloop-socket-create
  "Create h2o socket wrapper for file descriptor.

   Parameters:
   - loop: pointer to h2o_evloop_t
   - fd: file descriptor (int)
   - flags: socket flags (int)

   Returns: pointer to h2o_socket_t"
  h2o_evloop_socket_create
  [::mem/pointer ::mem/int ::mem/int] ::mem/pointer)

(defcfn socket-read-start
  "Start reading from socket with callback.

   Parameters:
   - sock: pointer to h2o_socket_t
   - cb: callback function pointer"
  h2o_socket_read_start
  [::mem/pointer ::mem/pointer] ::mem/void)

(defcfn socket-read-stop
  "Stop reading from socket.

   Parameters:
   - sock: pointer to h2o_socket_t"
  h2o_socket_read_stop
  [::mem/pointer] ::mem/void)

(defcfn socket-reading? "clj_h2o_socket_is_reading" [::mem/pointer] ::mem/int)
(defcfn socket-writing? "clj_h2o_socket_is_writing" [::mem/pointer] ::mem/int)
(defcfn socket-read-cb "clj_h2o_socket_get_read_cb" [::mem/pointer] ::mem/pointer)
(defcfn socket-write-cb "clj_h2o_socket_get_write_cb" [::mem/pointer] ::mem/pointer)

(defcfn socket-set-on-close
  "Set socket close callback for connection tracking.

   Parameters:
   - sock: pointer to h2o_socket_t
   - callback: function pointer for on_close callback
   - data: user data pointer passed to callback"
  clj_h2o_socket_set_on_close
  [::mem/pointer ::mem/pointer ::mem/pointer] ::mem/void)

(defcfn evloop-socket-accept
  "Accept new connection from listening socket.
   Returns pointer to h2o_socket_t or NULL"
  h2o_evloop_socket_accept
  [::mem/pointer] ::mem/pointer)

(defcfn socket-close
  "Close h2o socket"
  h2o_socket_close
  [::mem/pointer] ::mem/void)

(defcfn h2o-accept
  "Pass accepted socket to h2o for HTTP handling.

   Parameters:
   - ctx: pointer to h2o_accept_ctx_t
   - sock: pointer to h2o_socket_t"
  h2o_accept
  [::mem/pointer ::mem/pointer] ::mem/void)

(defcfn sendvec-init-raw
  "Initialize a sendvec with raw bytes"
  h2o_sendvec_init_raw
  [::mem/pointer ::mem/pointer ::mem/long] ::mem/void)

(defcfn sendvec
  "Send response data using sendvec"
  h2o_sendvec
  [::mem/pointer ::mem/pointer ::mem/long ::mem/int] ::mem/void)

(defcfn start-response
  "Start sending HTTP response"
  clj_h2o_start_response
  [::mem/pointer ::mem/int ::mem/pointer ::mem/long ::mem/long ::mem/int ::mem/pointer ::mem/pointer] ::mem/long)

(defcfn send-informational
  "Sends 1xx response"
  clj_h2o_send_informational
  [::mem/pointer ::mem/int ::mem/pointer ::mem/long]
  ::mem/void)

(defcfn cancel-request
  "Cancel a request after the response has started"
  clj_h2o_cancel_request
  [::mem/pointer] ::mem/int)

(defn report-almost-fatal-error [msg e]
  #p msg
  #p e)

(defcfn create-handler*
  "FFI binding for handler construction."
  "clj_h2o_create_handler"
  [::mem/pointer ::mem/pointer ::mem/pointer ::mem/pointer] ::mem/pointer)

(defn create-handler
  "Create and configure h2o handler with optional callbacks.
   Registers path '/', creates handler, and configures callbacks.

   Returns map containing handler pointer and pinned callback references."
  [hostconf-ptr on-req-callback on-cleanup-callback flat-config-ptr arena]
  {:pre [(some? hostconf-ptr) (not (mem/null? hostconf-ptr))
         (fn? on-req-callback)
         (fn? on-cleanup-callback)
         (some? flat-config-ptr) (not (mem/null? flat-config-ptr))
         (some? arena)]}
  (let [on-request-cb (fn on-request-cb [ctx-ptr]
                        (on-req-callback ctx-ptr (mem/deserialize (mem/reinterpret ctx-ptr (mem/size-of ::clj-req-ctx-t)) ::clj-req-ctx-t)))
        on-request-cb-ptr (mem/serialize on-request-cb [::ffi/fn [::mem/pointer] ::mem/int] arena)

        on-request-cleanup-cb (fn on-request-cleanup-cb [ctx-ptr]
                                (on-cleanup-callback ctx-ptr (mem/deserialize (mem/reinterpret ctx-ptr (mem/size-of ::clj-req-ctx-t)) ::clj-req-ctx-t)))
        on-request-cleanup-cb-ptr (mem/serialize on-request-cleanup-cb [::ffi/fn [::mem/pointer] ::mem/void] arena)]

    {::on-request-cb on-request-cb
     ::on-request-cleanup-cb on-request-cleanup-cb
     ::on-request-cb-ptr on-request-cb-ptr
     ::on-request-cleanup-cb-ptr on-request-cleanup-cb-ptr
     ::handler-ptr (create-handler* hostconf-ptr on-request-cb-ptr on-request-cleanup-cb-ptr flat-config-ptr)}))

(defcfn handler-set-shutting-down
  "Update the handler shutting_down flag (1 means shutdown in progress)."
  clj_h2o_handler_set_shutting_down
  [::mem/pointer ::mem/int] ::mem/void)

(defcfn proceed-req
  "Call req->proceed_req to signal readiness for next request body chunk"
  clj_h2o_proceed_req
  [::mem/pointer] ::mem/void)

(defcfn set-on-request-body-chunk-callback
  "Sets the on_request_body_chunk callback in the clj_req_ctx_t struct."
  clj_h2o_set_on_request_body_chunk
  [::mem/pointer ::mem/pointer] ::mem/void)

(defcfn evloop-now
  "Get current time in milliseconds from event loop"
  clj_h2o_evloop_now
  [::mem/pointer] ::mem/long)

(defcfn context-get-active-conns
  "Get count of active connections for this context"
  clj_h2o_context_get_active_conns
  [::mem/pointer] ::mem/long)

(defcfn context-get-idle-conns
  "Get count of idle connections for this context"
  clj_h2o_context_get_idle_conns
  [::mem/pointer] ::mem/long)

(defcfn context-get-shutdown-conns
  "Get count of shutdown connections for this context"
  clj_h2o_context_get_shutdown_conns
  [::mem/pointer] ::mem/long)

(defcfn cleanup-thread
  "Perform periodic cleanup tasks for a context.
   Returns maximum wait time in milliseconds before next cleanup.

   Parameters:
   - now: current time in milliseconds (from evloop-now)
   - ctx: pointer to h2o_context_t"
  h2o_cleanup_thread
  [::mem/long ::mem/pointer] ::mem/int)

(defcfn mt-create-wakeup-receiver
  "Register a wakeup receiver on ctx->queue (returns opaque pointer)"
  clj_h2o_mt_create_wakeup_receiver
  [::mem/pointer] ::mem/pointer)

(defcfn mt-destroy-wakeup-receiver
  "Unregister and free the wakeup receiver"
  clj_h2o_mt_destroy_wakeup_receiver
  [::mem/pointer] ::mem/void)

(defcfn mt-wakeup
  "Send a wakeup message to the loop owning this receiver"
  clj_h2o_mt_wakeup
  [::mem/pointer] ::mem/void)

(defcfn create-ssl-ctx
  "Create and configure SSL_CTX for TLS listener.

   Parameters:
   - cert-file: path to PEM certificate file
   - key-file: path to PEM private key file
   - enable-http2: 1 to register HTTP/2 ALPN protocols, 0 for HTTP/1.1 only

   Returns: SSL_CTX pointer on success, NULL on error"
  clj_h2o_create_ssl_ctx
  [::mem/c-string ::mem/c-string ::mem/int] ::mem/pointer)

(defcfn free-ssl-ctx
  "Free SSL_CTX created by [[create-ssl-ctx]]."
  clj_h2o_free_ssl_ctx
  [::mem/pointer] ::mem/void)

#_(defcfn req-print-offsets
    "Debug helper: print h2o_req_t field offsets to stderr for struct layout verification"
    clj_h2o_req_print_offsets
    [] ::mem/void)

(defn str->iovec
  [s arena]
  (let [str-ptr (mem/serialize s ::mem/c-string arena)
        len (max 0 (dec (.byteSize ^MemorySegment str-ptr)))]
    {:base str-ptr :len len}))

(defn create-context
  "Create and initialize h2o context for an event loop.
   Returns pointer to h2o_context_t"
  [arena loop-ptr config-ptr]
  {:pre [(some? arena)
         (some? loop-ptr) (not (mem/null? loop-ptr))
         (some? config-ptr) (not (mem/null? config-ptr))]}
  (let [size (context-size)
        ctx-ptr (mem/alloc size arena)]
    (context-init ctx-ptr loop-ptr config-ptr)
    ctx-ptr))

(defn create-loops
  "Create n event loops.
   Returns vector of h2o_evloop_t pointers"
  [n]
  (vec (repeatedly n evloop-create)))

(defn create-contexts
  "Create h2o contexts for the given event loops.
   Returns vector of h2o_context_t pointers"
  [arena loops config-ptr]
  {:pre [(some? arena)
         (seq loops)
         (some? config-ptr) (not (mem/null? config-ptr))]}
  (vec (for [loop loops]
         (create-context arena loop config-ptr))))

(defn create-socket-for-loop
  "Create h2o socket wrapper for file descriptor in event loop"
  [loop-ptr fd flags]
  {:pre [(some? loop-ptr) (not (mem/null? loop-ptr))
         (nat-int? fd) (>= fd 0)]}
  (evloop-socket-create loop-ptr fd flags))

(defn dispose-contexts
  "Dispose all h2o contexts"
  [contexts]
  (doseq [ctx contexts]
    (context-dispose ctx)))

(defn destroy-loops
  "Destroy all event loops"
  [loops]
  (doseq [loop loops]
    (evloop-destroy loop)))

(defn create-accept-ctx
  "Create h2o_accept_ctx_t for accepting connections.

   Parameters:
   - arena: memory arena for allocation
   - ctx-ptr: pointer to h2o_context_t
   - config-ptr: pointer to h2o_globalconf_t
   - ssl-ctx-ptr: pointer to SSL_CTX for TLS, or nil/NULL for plaintext

   Returns: pointer to h2o_accept_ctx_t"
  [arena ctx-ptr config-ptr ssl-ctx-ptr]
  {:pre [(some? arena)
         (some? ctx-ptr) (not (mem/null? ctx-ptr))
         (some? config-ptr) (not (mem/null? config-ptr))]}
  (let [hosts-ptr (globalconf-get-hosts config-ptr)
        ssl-ctx (if (and ssl-ctx-ptr (not (mem/null? ssl-ctx-ptr)))
                  ssl-ctx-ptr
                  mem/null)
        accept-ctx-data {:ctx ctx-ptr
                         :hosts hosts-ptr
                         :ssl_ctx ssl-ctx
                         :http2_origin_frame mem/null
                         :expect_proxy_line 0
                         :libmemcached_receiver mem/null}]
    (mem/serialize accept-ctx-data ::h2o-accept-ctx-t arena)))

(defn ->string
  "Read bytes from a pointer with given length as a UTF-8 string. Returns nil if pointer is null."
  [ptr ^long len]
  (when (and ptr (not (mem/null? ptr)) (pos? len))
    (String. (mem/read-bytes (mem/reinterpret ptr len) len) "UTF-8")))

(defn build-ring-headers-map [headers headers_len]
  (when (and (not (mem/null? headers)) (pos? headers_len))
    (let [header-size (mem/size-of ::clj-header-t)
          total-size (* headers_len header-size)
          sized-headers (mem/reinterpret headers total-size)]
      (into {}
            (for [i (range headers_len)]
              (let [header-seg (mem/slice sized-headers (* i header-size) header-size)
                    header (mem/deserialize header-seg ::clj-header-t)
                    {:keys [name name_len
                            value value_len]} header
                    name-str (->string name name_len)
                    value-str (->string value value_len)]
                [(str/lower-case name-str) value-str]))))))

(defn build-ring-request
  "Build Ring-compliant request map from h2o request metadata.

   Maps h2o request structure to Ring spec with:
   - `:server-port`, `:server-name` from authority field (host:port)
   - `:remote-addr` from client address
   - `:uri`, `:query-string` from request line path (split on ?)
   - `:request-method` as keyword (lowercase)
   - `:headers` as lowercase string keys
   - `:body` as InputStream when `has_body` is true
   - `:ol.busker/early-data?` true when request arrived via 0-RTT

   Protocol version (HTTP/1.1, HTTP/2, HTTP/3) determined from `http_version` field."
  [{:keys [method method_len path path_len authority authority_len
           http_version headers headers_len has_body is_early_data
           scheme scheme_len remote_addr remote_addr_len]}
   ^InputStream input-stream]
  (let [method-str (->string method method_len)
        path-str (->string path path_len)
        authority-str (->string authority authority_len)
        scheme-str (->string scheme scheme_len)
        remote-addr-str (->string remote_addr remote_addr_len)
        headers-map (build-ring-headers-map headers headers_len)
        [uri query-string] (if path-str
                             (let [idx (str/index-of path-str "?")]
                               (if idx
                                 [(subs path-str 0 idx) (subs path-str (inc idx))]
                                 [path-str nil]))
                             [nil nil])
        version (case (int http_version)
                  0x0101 [1 1]
                  0x0200 [2 0]
                  0x0300 [3 0]
                  [1 1])]
    {:server-port (if authority-str
                    (if-let [colon-idx (str/last-index-of authority-str ":")]
                      (Integer/parseInt (subs authority-str (inc colon-idx)))
                      80)
                    80)
     :server-name (if authority-str
                    (if-let [colon-idx (str/last-index-of authority-str ":")]
                      (subs authority-str 0 colon-idx)
                      authority-str)
                    "localhost")
     :remote-addr (or remote-addr-str "")
     :uri uri
     :query-string query-string
     :scheme (keyword (or scheme-str "http"))
     :request-method (keyword (str/lower-case (or method-str "get")))
     :protocol (str "HTTP/" (first version) "." (second version))
     :headers headers-map
     :body (when (= 1 has_body)
             input-stream)
     :ol.busker/early-data? (= 1 is_early_data)}))

(defcfn http3-create-ptls-ctx
  "Create picotls context for QUIC TLS 1.3.
   Uses OpenSSL-backed primitives for cryptographic operations.
   Configures ALPN callback for HTTP/3 protocol negotiation.

   Parameters:
   - cert-file: path to PEM certificate file
   - key-file: path to PEM private key file

   Returns: ptls_context_t pointer on success, NULL on error"
  clj_h2o_create_ptls_ctx
  [::mem/c-string ::mem/c-string] ::mem/pointer)

(defcfn http3-free-ptls-ctx
  "Free picotls context and associated resources."
  clj_h2o_free_ptls_ctx
  [::mem/pointer] ::mem/void)

(defcfn http3-create-quicly-ctx
  "Create quicly context configured for HTTP/3.
   Starts from quicly_spec_context and configures TLS, CID encryptor,
   and HTTP/3 transport parameters.

   Parameters:
   - ptls-ctx: picotls context from [[http3-create-ptls-ctx]]
   - globalconf: h2o global configuration for HTTP/3 settings

   Returns: quicly_context_t pointer on success, NULL on error"
  clj_h2o_create_quicly_ctx
  [::mem/pointer ::mem/pointer] ::mem/pointer)

(defcfn http3-free-quicly-ctx
  "Free quicly context and CID encryptor."
  clj_h2o_free_quicly_ctx
  [::mem/pointer] ::mem/void)

(defcfn http3-create-worker-ctx
  "Create HTTP/3 worker context with UDP listener.
   Creates a UDP socket bound to host:port, configures it for QUIC
   (IP_PKTINFO, DF bit, H2O_SOCKET_FLAG_DONT_READ), and initializes
   the h2o_http3_server_ctx_t.

   Parameters:
   - h2o-ctx: h2o context for this worker
   - loop: event loop for this worker
   - quic-ctx: shared quicly context from [[http3-create-quicly-ctx]]
   - hosts: hosts array from globalconf
   - host: bind address (e.g., \"0.0.0.0\" or \"127.0.0.1\")
   - port: UDP port to bind
   - thread-id: unique worker thread ID for CID routing

   Returns: opaque context pointer on success, NULL on error"
  clj_h2o_http3_create_worker_ctx
  [::mem/pointer ::mem/pointer ::mem/pointer ::mem/pointer
   ::mem/c-string ::mem/short ::mem/int] ::mem/pointer)

(defcfn http3-stop-accepting
  "Stop accepting new HTTP/3 connections by setting acceptor to NULL.
   Existing connections (including those in handshake) continue to completion.
   Call this before requesting shutdown to prevent new connection attempts."
  clj_h2o_http3_stop_accepting
  [::mem/pointer] ::mem/void)

(defcfn http3-num-connections
  "Get the number of active HTTP/3 connections on this context."
  clj_h2o_http3_num_connections
  [::mem/pointer] ::mem/long)

(defcfn http3-free-worker-ctx
  "Free HTTP/3 worker context and close UDP socket.
   Note: all connections must be closed first (num_connections == 0)."
  clj_h2o_http3_dispose_worker_ctx
  [::mem/pointer] ::mem/void)

(defcfn conn-limit-set-max
  "Set the maximum allowed connections. Zero means unlimited.
   Process-global counter shared across all workers and listeners."
  clj_h2o_conn_limit_set_max
  [::mem/int] ::mem/void)

(defcfn conn-limit-current
  "Get current global connection count."
  clj_h2o_conn_limit_current
  [] ::mem/int)

(defcfn conn-limit-try-acquire
  "Atomically try to acquire a connection slot.
   Returns 1 if acquired (count was < max), 0 if at limit."
  clj_h2o_conn_limit_try_acquire
  [] ::mem/int)

(defcfn conn-limit-release
  "Release a connection slot (decrement counter)."
  clj_h2o_conn_limit_release
  [] ::mem/void)

;; Session ticket key structure - matches C clj_session_ticket_t
(mem/defalias ::clj-session-ticket-t
  (layout/with-c-layout
    [::mem/struct
     [[:name [::mem/array ::mem/byte 16]]       ; 16 bytes key identifier
      [:aes_key [::mem/array ::mem/byte 32]]    ; 32 bytes AES-256 key
      [:hmac_key [::mem/array ::mem/byte 64]]   ; 64 bytes HMAC key
      [:not_before ::mem/long]                  ; activation time (ms epoch)
      [:not_after ::mem/long]]]))               ; expiration time (ms epoch)

(def size-of-session-ticket-t (mem/size-of ::clj-session-ticket-t))

(defcfn ticket-manager-create
  "Create a new ticket manager with specified ticket lifetime.
   Returns pointer to clj_ticket_manager_t, or NULL on failure."
  clj_ticket_manager_create
  [::mem/int] ::mem/pointer)

(defcfn ticket-manager-destroy
  "Destroy ticket manager and securely erase all keys."
  clj_ticket_manager_destroy
  [::mem/pointer] ::mem/void)

(defcfn ticket-manager-set-keys
  "Replace all keys atomically. Thread-safe via copy-on-write.
   Returns 0 on success, -1 on error."
  clj_ticket_manager_set_keys
  [::mem/pointer ::mem/pointer ::mem/long] ::mem/int)

(defcfn ticket-manager-key-count
  "Get current key count (for monitoring)."
  clj_ticket_manager_key_count
  [::mem/pointer] ::mem/long)

(defcfn ticket-manager-set-quic-tag
  "Set the QUIC transport params hash for 0-RTT validation."
  clj_ticket_manager_set_quic_tag
  [::mem/pointer ::mem/pointer] ::mem/void)

(defcfn ticket-manager-create-encrypt-ticket
  "Create encrypt_ticket callback wired to manager.
   is_quic: 1 for QUIC mode (appends transport params tag), 0 for TCP TLS."
  clj_ticket_manager_create_encrypt_ticket
  [::mem/pointer ::mem/int] ::mem/pointer)

(defcfn ptls-ctx-set-tickets
  "Configure ptls context for session tickets.
   Sets encrypt_ticket callback, ticket lifetime, and max early data size."
  clj_ptls_ctx_set_tickets
  [::mem/pointer ::mem/pointer ::mem/int ::mem/int] ::mem/void)

(defcfn ssl-ctx-set-tickets
  "Configure SSL_CTX for TCP TLS session tickets and 0-RTT.
   Wires the ticket manager into the OpenSSL ticket key callback."
  clj_ssl_ctx_set_tickets
  [::mem/pointer ::mem/pointer ::mem/int] ::mem/void)
