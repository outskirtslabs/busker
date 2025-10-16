(ns ol.h2o.native
  (:import
   [java.io InputStream])
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [coffi.ffi :as ffi :refer [defcfn]]
   [coffi.layout :as layout]
   [coffi.mem :as mem]))

(set! *warn-on-reflection* true)

(def ^:const H2O_SEND_STATE_IN_PROGRESS 0)
(def ^:const H2O_SEND_STATE_FINAL 1)
(def ^:const H2O_SEND_STATE_ERROR 2)

(import 'java.lang.foreign.MemoryLayout)
(import 'java.lang.foreign.MemoryLayout$PathElement)

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

(defn copy-resource [resource-path output-path]
  (with-open [in (io/input-stream (io/resource resource-path))
              out (io/output-stream (io/file output-path))]
    (io/copy in out)))

(defn get-arch+os []
  (let [os-name (str/lower-case (System/getProperty "os.name"))]
    (str (System/getProperty "os.arch") "-"
         (cond (str/includes? os-name "win") "windows"
               (str/includes? os-name "nux") "linux"
               (str/includes? os-name "mac") "macos"))))

#_(defn load-bundled-library []
    (let [res-file (case (get-arch+os)
                     "aarch64-linux" "libh2o_aarch64-linux-gnu.so"
                     "aarch64-macos" "libh2o_aarch64-macos-none.so"
                     ("x86-linux"
                      "amd64-linux") "libh2o_x86_64-linux-gnu.so"
                     ("x86-macos"
                      "amd64-macos") "libh2o_x86_64-macos-none.so"
                     ("x86-windows"
                      "amd64-windows") "libh2o_x86_64-windows-gnu.dll")
          temp-lib-filename (str "h2oclj_temp_" res-file)]
      (copy-resource res-file temp-lib-filename)
      (ffi/load-library temp-lib-filename)
      (Files/deleteIfExists (.toPath (io/file temp-lib-filename)))))

#_(defn load-system-library []
    (ffi/load-system-library "libh2o"))

;; (ffi/load-system-library "h2o-evloop")
(ffi/load-library "result/lib/libh2o-evloop.so")
(ffi/load-library (System/getProperty "ol.libh2o.path"))

(mem/defalias ::clj-header-t
  (layout/with-c-layout
    [::mem/struct
     [[:name ::mem/pointer]
      [:name_len ::mem/int]
      [:value ::mem/pointer]
      [:value_len ::mem/int]]]))

(mem/defalias ::clj-iovec-t
  (layout/with-c-layout
    [::mem/struct
     [[:data ::mem/pointer]
      [:len ::mem/int]]]))

;; h2o_iovec_t is:
;;   typedef struct { char *base; size_t len; } h2o_iovec_t;
;; Use a typed pointer for clarity; size_t→::mem/long is OK on typical *nix.
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
      [:charset ::mem/pointer]
      [:method ::mem/pointer]
      [:path ::mem/pointer]
      [:remote_addr ::mem/pointer]
      [:scheme ::mem/pointer]
      [:headers ::mem/pointer]

      [:authority_len ::mem/long]
      [:charset_len ::mem/long]
      [:method_len ::mem/long]
      [:path_len ::mem/long]
      [:remote_addr_len ::mem/long]
      [:scheme_len ::mem/long]
      [:headers_len ::mem/long]

      [:http_version ::mem/int]
      [:has_body ::mem/int]]]))
#_(print-offsets-for (layout/with-c-layout
                       [::mem/struct
                        [[:authority ::mem/pointer]
                         [:charset ::mem/pointer]
                         [:method ::mem/pointer]
                         [:path ::mem/pointer]
                         [:remote_addr ::mem/pointer]
                         [:scheme ::mem/pointer]
                         [:headers ::mem/pointer]

                         [:authority_len ::mem/long]
                         [:charset_len ::mem/long]
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
      [:on-response-generator-stop ::mem/pointer]
      [:on-response-generator-proceed ::mem/pointer]
      [:preferred-chunk-size ::mem/int]
      [:cleanup ::mem/int]
      [:closing ::mem/int]
      [:response_started ::mem/int]]]))

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

(defcfn config-init
  "Initialize h2o global configuration structure"
  h2o_config_init
  [::mem/pointer] ::mem/void)

(defcfn config-dispose
  "Dispose h2o global configuration and free resources"
  h2o_config_dispose
  [::mem/pointer] ::mem/void)

(defcfn globalconf-size
  "Get size of h2o_globalconf_t structure"
  clj_h2o_globalconf_size
  [] ::mem/long)

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
  [::mem/pointer ::mem/int ::mem/pointer ::mem/long ::mem/long ::mem/pointer ::mem/pointer] ::mem/long)

(defcfn cancel-request
  "Cancel a request after the response has started"
  clj_h2o_cancel_request
  [::mem/pointer] ::mem/int)

(def CLJ_HANDLER_OVERLOADED -2)
(def CLJ_HANDLER_DECLINED -1)
(def CLJ_HANDLER_OK 0)

(defn report-almost-fatal-error [msg e]
  #p msg
  #p e)

(defcfn create-handler
  "Create and configure h2o handler with optional callbacks.
   Registers path '/', creates handler, and configures callbacks.

   Parameters:
   - hostconf-ptr: h2o_hostconf_t* pointer
   - on-req-callback: Clojure fn (req-ctx-ptr, clj_req_ctx map) (required)
   - on-cleanup-callback: Clojure fn  (req-ctx-ptr, clj_req_ctx map) (required)
   - supports-request-streaming: boolean
   - handles-expect: boolean

   Returns handler pointer."
  "clj_h2o_create_handler"
  [::mem/pointer ::mem/pointer ::mem/pointer ::mem/int ::mem/int] ::mem/pointer
  native-fn
  [hostconf-ptr on-req-callback on-cleanup-callback supports-request-streaming handles-expect]
  (let [on-request-cb-ptr (mem/serialize (fn [ctx-ptr]
                                           (try
                                             (on-req-callback ctx-ptr (mem/deserialize (mem/reinterpret ctx-ptr (mem/size-of ::clj-req-ctx-t)) ::clj-req-ctx-t))
                                             (catch Exception e
                                               (report-almost-fatal-error "The request handler errored with" e)
                                               CLJ_HANDLER_OVERLOADED)))
                                         [::ffi/fn [::mem/pointer] ::mem/int])
        on-request-cleanup-cb-ptr (mem/serialize (fn [ctx-ptr]
                                                   (try
                                                     (on-cleanup-callback ctx-ptr (mem/deserialize (mem/reinterpret ctx-ptr (mem/size-of ::clj-req-ctx-t)) ::clj-req-ctx-t))
                                                     (catch Exception e
                                                       (report-almost-fatal-error "The request cleanup callback errored" e))))

                                                 [::ffi/fn [::mem/pointer] ::mem/void])]
    {:on-request-cb-ptr         on-request-cb-ptr
     ::on-request-cleanup-cb-ptr on-request-cleanup-cb-ptr
     ::handler-ptr
     (native-fn hostconf-ptr
                on-request-cb-ptr
                on-request-cleanup-cb-ptr
                (if supports-request-streaming 1 0)
                (if handles-expect 1 0))}))
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

#_(defcfn req-print-offsets
    "Debug helper: print h2o_req_t field offsets to stderr for struct layout verification"
    clj_h2o_req_print_offsets
    [] ::mem/void)

(defn create-iovec
  "Create an h2o_iovec_t from a string.
   IMPORTANT: Caller must provide arena to ensure string memory lives long enough
   Returns a memory segment containing the h2o_iovec_t struct"
  [s arena]
  (let [str-ptr (mem/serialize s ::mem/c-string arena)
        len (count s)
        iovec-data {:base str-ptr :len len}]
    (mem/serialize iovec-data ::h2o-iovec-t arena)))

(defn create-context
  "Create and initialize h2o context for an event loop.
   Returns pointer to h2o_context_t"
  [arena loop-ptr config-ptr]
  (let [size (context-size)
        ctx-ptr (mem/alloc size arena)]
    (context-init ctx-ptr loop-ptr config-ptr)
    ctx-ptr))

#_(defn dispose-server-config
    "Dispose h2o configuration and free resources"
    [config-ptr]
    (config-dispose config-ptr))

(defn create-loops
  "Create n event loops.
   Returns vector of h2o_evloop_t pointers"
  [n]
  (vec (repeatedly n evloop-create)))

(defn create-contexts
  "Create h2o contexts for the given event loops.
   Returns vector of h2o_context_t pointers"
  [arena loops config-ptr]
  (vec (for [loop loops]
         (create-context arena loop config-ptr))))

(defn create-socket-for-loop
  "Create h2o socket wrapper for file descriptor in event loop"
  [loop-ptr fd flags]
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
   
   Returns: pointer to h2o_accept_ctx_t"
  [arena ctx-ptr config-ptr]
  (let [hosts-ptr (globalconf-get-hosts config-ptr)
        accept-ctx-data {:ctx ctx-ptr
                         :hosts hosts-ptr
                         :ssl_ctx (mem/as-segment 0) ; NULL for now (no TLS)
                         :http2_origin_frame (mem/as-segment 0) ; NULL
                         :expect_proxy_line 0
                         :libmemcached_receiver (mem/as-segment 0)}] ; NULL
    (mem/serialize accept-ctx-data ::h2o-accept-ctx-t arena)))

(defn ->string
  "Read bytes from a pointer with given length as a UTF-8 string. Returns nil if pointer is null."
  [ptr ^long len]
  (when (and ptr (not (mem/null? ptr)) (pos? len))
    (String. (mem/read-bytes (mem/reinterpret ptr len) len) "UTF-8")))

(defn build-ring-headers-map [headers headers_len]
  (when (and (not (mem/null? headers)) (pos? headers_len))
    (let [header-size   (mem/size-of ::clj-header-t)
          total-size    (* headers_len header-size)
          sized-headers (mem/reinterpret headers total-size)]
      (into {}
            (for [i (range headers_len)]
              (let [header-seg                (mem/slice sized-headers (* i header-size) header-size)
                    header                    (mem/deserialize header-seg ::clj-header-t)
                    {:keys [name name_len
                            value value_len]} header
                    name-str                  (->string name name_len)
                    value-str                 (->string value value_len)]
                [(str/lower-case name-str) value-str]))))))

(defn build-ring-request
  "Build a Ring request map from clj_req_meta_t.
   Returns: Ring request map"
  [{:keys [method method_len path path_len authority authority_len
           http_version headers headers_len has_body
           scheme scheme_len remote_addr remote_addr_len
           #_#_charset charset_len]}
   ^InputStream input-stream]
  (let [method-str         (->string method method_len)
        path-str           (->string path path_len)
        authority-str      (->string authority authority_len)
        scheme-str         (->string scheme scheme_len)
        remote-addr-str    (->string remote_addr remote_addr_len)
        #_#_charset-str    (->string charset charset_len)
        headers-map        (build-ring-headers-map headers headers_len)
        [uri query-string] (if path-str
                             (let [idx (str/index-of path-str "?")]
                               (if idx
                                 [(subs path-str 0 idx) (subs path-str (inc idx))]
                                 [path-str nil]))
                             [nil nil])
        version            (case (int http_version)
                             0x0101 [1 1]
                             0x0200 [2 0]
                             0x0300 [3 0]
                             [1 1])]
    {:server-port    (if authority-str
                       (if-let [colon-idx (str/last-index-of authority-str ":")]
                         (Integer/parseInt (subs authority-str (inc colon-idx)))
                         80)
                       80)
     :server-name    (if authority-str
                       (if-let [colon-idx (str/last-index-of authority-str ":")]
                         (subs authority-str 0 colon-idx)
                         authority-str)
                       "localhost")
     :remote-addr    (or remote-addr-str "")
     :uri            uri
     :query-string   query-string
     :scheme         (keyword (or scheme-str "http"))
     :request-method (keyword (str/lower-case (or method-str "get")))
     :protocol       (str "HTTP/" (first version) "." (second version))
     :headers        headers-map
     :body           (when (= 1 has_body)
                       input-stream)}))
