(ns ^:no-doc ol.busker.native
  (:require
   [clojure.string :as str]
   [coffi.ffi :as ffi :refer [defcfn]]
   [coffi.layout :as layout]
   [coffi.mem :as mem]
   [ol.busker.native.loader]
   [ol.busker.util :as util]
   [taoensso.trove :as trove])
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
      [:dispatch-module-id ::mem/long]
      [:dispatch-request-seq ::mem/long]
      [:cleanup ::mem/int]
      [:closing ::mem/int]
      [:response_started ::mem/int]]]))

(def ^:private size-of-clj-req-ctx-t
  (mem/size-of ::clj-req-ctx-t))

(defn read-request-context
  "Returns the `:req`, `:meta`, and `:req-id` projection from `ctx-ptr`.

  `ctx-ptr` must remain valid while native values are read."
  [ctx-ptr]
  (let [ctx (mem/reinterpret ctx-ptr size-of-clj-req-ctx-t)]
    {:req    (mem/read-address ctx 0)
     :meta   {:authority       (mem/read-address ctx 8)
              :method          (mem/read-address ctx 16)
              :path            (mem/read-address ctx 24)
              :remote_addr     (mem/read-address ctx 32)
              :scheme          (mem/read-address ctx 40)
              :headers         (mem/read-address ctx 48)
              :authority_len   (mem/read-long ctx 56)
              :method_len      (mem/read-long ctx 64)
              :path_len        (mem/read-long ctx 72)
              :remote_addr_len (mem/read-long ctx 80)
              :scheme_len      (mem/read-long ctx 88)
              :headers_len     (mem/read-long ctx 96)
              :http_version    (mem/read-int ctx 104)
              :has_body        (mem/read-short ctx 108)
              :is_early_data   (mem/read-short ctx 110)}
     :req-id (mapv (fn [offset]
                     (char (.get ^MemorySegment ctx
                                 java.lang.foreign.ValueLayout/JAVA_BYTE
                                 (long (+ 168 offset)))))
                   (range 64))}))

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

(defcfn req-ctx-size
  "Gets the native size of `clj_req_ctx_t`."
  clj_h2o_req_ctx_size
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
  (binding [*out* *err*]
    (tap> [msg e])
    (println msg)
    (when e
      (.printStackTrace ^Throwable e ^java.io.PrintWriter *err*))))

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
                        (on-req-callback ctx-ptr (read-request-context ctx-ptr)))
        on-request-cb-ptr (mem/serialize on-request-cb [::ffi/fn [::mem/pointer] ::mem/int] arena)

        on-request-cleanup-cb (fn on-request-cleanup-cb [module-id request-seq]
                                (on-cleanup-callback module-id request-seq))
        on-request-cleanup-cb-ptr
        (mem/serialize on-request-cleanup-cb
                       [::ffi/fn [::mem/long ::mem/long] ::mem/void]
                       arena)]

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

(defcfn install-request-dispatch
  "Installs scalar request identity and its stable body callback."
  clj_h2o_install_request_dispatch
  [::mem/pointer ::mem/long ::mem/long ::mem/pointer] ::mem/void)

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
   - cert-file: path to PEM certificate file (optional, must pair with key-file)
   - key-file: path to PEM private key file (optional, must pair with cert-file)
   - enable-http2: 1 to register HTTP/2 ALPN protocols, 0 for HTTP/1.1 only
   - tls-lookup-cb-ptr: callback pointer for SNI lookup, or NULL
   - tls-lookup-user-ctx: user context pointer passed to callback, or NULL

   Returns: SSL_CTX pointer on success, NULL on error"
  clj_h2o_create_ssl_ctx
  [::mem/c-string ::mem/c-string ::mem/int ::mem/pointer ::mem/pointer] ::mem/pointer)

(defcfn free-ssl-ctx
  "Free SSL_CTX created by [[create-ssl-ctx]]."
  clj_h2o_free_ssl_ctx
  [::mem/pointer] ::mem/void)

(defcfn tls-bytes-dup
  "Duplicate a byte buffer onto native heap."
  clj_h2o_tls_memdup
  [::mem/pointer ::mem/long] ::mem/pointer)

(defn- string->tls-native-bytes
  [^String s]
  (let [bytes (.getBytes s "UTF-8")
        len (alength ^bytes bytes)]
    (if (pos? len)
      (with-open [arena (mem/confined-arena)]
        (let [source (MemorySegment/ofArray bytes)
              staged (mem/alloc len arena)
              _ (MemorySegment/copy source 0 staged 0 len)
              ptr (tls-bytes-dup staged len)]
          {:ptr ptr :len len}))
      {:ptr mem/null :len 0})))

(defn- write-tls-native-outputs!
  [cert-out-seg cert-len-out-seg key-out-seg key-len-out-seg
   cert-chain-pem private-key-pem]
  (let [{cert-ptr :ptr cert-len :len}
        (string->tls-native-bytes cert-chain-pem)
        {key-ptr :ptr key-len :len}
        (string->tls-native-bytes private-key-pem)
        valid? (and (not (mem/null? cert-ptr))
                    (not (mem/null? key-ptr))
                    (pos? cert-len)
                    (pos? key-len))]
    (mem/write-address cert-out-seg cert-ptr)
    (mem/write-long cert-len-out-seg 0 (long cert-len))
    (mem/write-address key-out-seg key-ptr)
    (mem/write-long key-len-out-seg 0 (long key-len))
    (if valid? 1 -1)))

(defn- reset-tls-native-outputs!
  [cert-out-seg cert-len-out-seg key-out-seg key-len-out-seg]
  (mem/write-address cert-out-seg mem/null)
  (mem/write-long cert-len-out-seg 0 0)
  (mem/write-address key-out-seg mem/null)
  (mem/write-long key-len-out-seg 0 0))

(defn build-tls-lookup-callback
  "Build Clojure callback pointer for native TLS handshakes.
   `lookup-fn` takes hostname and returns either:
   - nil for miss
   - {:cert-chain-pem \"...\" :private-key-pem \"...\"} for success

   Hostname is nil when client hello omits SNI.

   Returns pinned callback refs that must be retained while server runs.
   This function does not mutate process-global native state."
  [lookup-fn]
  {:pre [(ifn? lookup-fn)]}
  (let [cb     (fn [sni sni-len cert-out cert-len-out key-out key-len-out _user-ctx]
                 (try
                   (let [sni-len          (long sni-len)
                         hostname         (when (pos? sni-len)
                                            (let [sni-seg (mem/reinterpret sni sni-len)]
                                              (when-not (mem/null? sni-seg)
                                                (String. (mem/read-bytes sni-seg sni-len)
                                                         "UTF-8"))))
                         cert-out-seg     (mem/reinterpret cert-out mem/pointer-size)
                         cert-len-out-seg (mem/reinterpret cert-len-out (mem/size-of ::mem/long))
                         key-out-seg      (mem/reinterpret key-out mem/pointer-size)
                         key-len-out-seg  (mem/reinterpret key-len-out (mem/size-of ::mem/long))]
                     (reset-tls-native-outputs! cert-out-seg
                                                cert-len-out-seg
                                                key-out-seg
                                                key-len-out-seg)
                     (if-let [{:keys [cert-chain-pem private-key-pem]} (lookup-fn hostname)]
                       (if (and (string? cert-chain-pem) (string? private-key-pem))
                         (write-tls-native-outputs! cert-out-seg
                                                    cert-len-out-seg
                                                    key-out-seg
                                                    key-len-out-seg
                                                    cert-chain-pem
                                                    private-key-pem)
                         -1)
                       0))
                   (catch Throwable t
                     (try
                       (trove/log! {:level :error
                                    :id    ::tls-lookup-callback-exception
                                    :ex    t
                                    :data  {:sni-len sni-len}})
                       (let [cert-out-seg     (mem/reinterpret cert-out mem/pointer-size)
                             cert-len-out-seg (mem/reinterpret cert-len-out (mem/size-of ::mem/long))
                             key-out-seg      (mem/reinterpret key-out mem/pointer-size)
                             key-len-out-seg  (mem/reinterpret key-len-out (mem/size-of ::mem/long))]
                         (reset-tls-native-outputs! cert-out-seg
                                                    cert-len-out-seg
                                                    key-out-seg
                                                    key-len-out-seg))
                       (catch Throwable _
                         nil))
                     -1)))
        cb-ptr (mem/serialize cb [::ffi/fn [::mem/pointer ::mem/long
                                            ::mem/pointer ::mem/pointer
                                            ::mem/pointer ::mem/pointer
                                            ::mem/pointer]
                                  ::mem/int])]
    {:callback     cb
     :callback-ptr cb-ptr}))

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
      (reduce
       (fn [result i]
         (let [header-seg (mem/slice sized-headers (* i header-size) header-size)
               header (mem/deserialize header-seg ::clj-header-t)
               {:keys [name name_len
                       value value_len]} header
               name-str (str/lower-case (->string name name_len))
               value-str (->string value value_len)
               delimiter (if (= "cookie" name-str) ";" ",")]
           (if (contains? result name-str)
             (update result name-str str delimiter value-str)
             (assoc result name-str value-str))))
       {}
       (range headers_len)))))

(defn- default-server-port
  [scheme-str]
  (if (= "https" scheme-str)
    443
    80))

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
        default-port (default-server-port scheme-str)
        {:keys [server-name server-port]}
        (util/parse-authority authority-str default-port)
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
    {:server-port server-port
     :server-name server-name
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
   - cert-file: path to PEM certificate file (optional, must pair with key-file)
   - key-file: path to PEM private key file (optional, must pair with cert-file)
   - tls-lookup-cb-ptr: callback pointer for SNI lookup, or NULL
   - tls-lookup-user-ctx: user context pointer passed to callback, or NULL

   Returns: ptls_context_t pointer on success, NULL on error"
  clj_h2o_create_ptls_ctx
  [::mem/c-string ::mem/c-string ::mem/pointer ::mem/pointer] ::mem/pointer)

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

(defcfn http3-open-udp-transport
  "Open a pooled UDP transport reservation for HTTP/3.
   The returned handle owns the pooled transport metadata and any reservation
   socket until [[http3-release-udp-transport]] is called."
  clj_h2o_http3_open_udp_transport
  [::mem/c-string ::mem/short] ::mem/pointer)

(defcfn http3-attach-udp-transport
  "Attach an HTTP/3 worker context to a pooled UDP transport.
   The worker attachment starts non-accepting until the transport activates
   the generation identified by `node-id`."
  clj_h2o_http3_attach_udp_transport
  [::mem/pointer ::mem/pointer ::mem/pointer ::mem/pointer
   ::mem/pointer ::mem/long ::mem/int] ::mem/pointer)

(defcfn http3-activate-udp-transport-generation
  "Mark `node-id` as the active acceptor on a pooled UDP transport.
   Existing connections for older generations continue to route by CID,
   while new Initial packets are forwarded to the active generation."
  clj_h2o_http3_activate_udp_transport_generation
  [::mem/pointer ::mem/long] ::mem/void)

(defcfn http3-detach-udp-transport
  "Detach an HTTP/3 worker context from a pooled UDP transport.
   This closes only the worker-owned dup'd fd."
  clj_h2o_http3_detach_udp_transport
  [::mem/pointer] ::mem/void)

(defcfn http3-release-udp-transport
  "Release a pooled UDP transport and close any reservation socket it owns."
  clj_h2o_http3_release_udp_transport
  [::mem/pointer] ::mem/void)

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

(defn session-ticket-keys->native-array
  "Allocate and populate a contiguous native array of `::clj-session-ticket-t`.

  Input is a seq of maps ordered newest-first, where each map contains:
  `:name` as a 16-byte array, `:aes-key` as a 32-byte array, `:hmac-key` as a
  64-byte array, and `:not-before` / `:not-after` as epoch-millisecond longs.

  Returns a native memory segment allocated in `arena` containing one
  `::clj-session-ticket-t` struct per input key in the same order."
  [keys arena]
  (let [struct-size size-of-session-ticket-t
        n (count keys)
        segment (mem/alloc (* n struct-size) arena)]
    (doseq [[i k] (map-indexed vector keys)]
      (let [offset (* i struct-size)
            name-bytes ^bytes (:name k)
            aes-bytes ^bytes (:aes-key k)
            hmac-bytes ^bytes (:hmac-key k)]
        (doseq [j (range 16)]
          (mem/write-byte segment (+ offset j) (aget name-bytes j)))
        (doseq [j (range 32)]
          (mem/write-byte segment (+ offset 16 j) (aget aes-bytes j)))
        (doseq [j (range 64)]
          (mem/write-byte segment (+ offset 48 j) (aget hmac-bytes j)))
        (mem/write-long segment (+ offset 112) (:not-before k))
        (mem/write-long segment (+ offset 120) (:not-after k))))
    segment))

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
