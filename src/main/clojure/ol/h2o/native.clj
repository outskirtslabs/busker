(ns ol.h2o.native
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [coffi.ffi :as ffi :refer [defcfn]]
   [coffi.layout :as layout]
   [coffi.mem :as mem :refer [defalias]]
   [ring.core.protocols :as ring-protocols])
  (:import [java.nio.file Files]))

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

(ffi/load-system-library "h2o-evloop")
(ffi/load-library (System/getProperty "ol.libh2o.path"))

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

(def ^:const H2O_SEND_STATE_IN_PROGRESS 0)
(def ^:const H2O_SEND_STATE_FINAL 1)
(def ^:const H2O_SEND_STATE_ERROR 2)

(mem/defalias ::h2o-sendvec-t
  (layout/with-c-layout
    [::mem/struct
     [[:callbacks ::mem/pointer]
      [:len ::mem/long]
      [:raw ::mem/pointer]]]))

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
  h2o_start_response
  [::mem/pointer ::mem/pointer] ::mem/void)

(defcfn config-register-path
  "Register a path with the h2o configuration.
   Returns pointer to h2o_pathconf_t"
  h2o_config_register_path
  [::mem/pointer ::mem/c-string ::mem/int] ::mem/pointer)

(defcfn create-handler
  "Create h2o handler for pathconf. Returns pointer to h2o_handler_t"
  h2o_create_handler
  [::mem/pointer ::mem/long] ::mem/pointer)

(defcfn handler-set-on-req
  "Set on_req callback for handler"
  clj_handler_set_on_req
  [::mem/pointer ::mem/pointer] ::mem/void)

(defcfn handler-size
  "Get size of h2o_handler_t structure"
  clj_h2o_handler_size
  [] ::mem/long)

(defcfn add-header-by-str
  "Add response header by string.
   Parameters:
   - pool: memory pool pointer
   - headers: headers structure pointer
   - lowercase_name: lowercase header name (raw char pointer)
   - lowercase_name_len: length of header name
   - maybe_token: whether to check for token (0=no, 1=yes)
   - orig_name: original case header name (raw char pointer)
   - value: header value (raw char pointer)
   - value_len: length of header value
   Returns: ssize_t (header index or -1 on error)"
  h2o_add_header_by_str
  [::mem/pointer ::mem/pointer ::mem/pointer ::mem/long ::mem/int ::mem/pointer ::mem/pointer ::mem/long] ::mem/long)

(defcfn get-content-type-token
  "Get H2O_TOKEN_CONTENT_TYPE pointer"
  clj_h2o_get_content_type_token
  [] ::mem/pointer)

(defcfn get-static-generator
  "Get static h2o_generator_t pointer"
  clj_h2o_get_static_generator
  [] ::mem/pointer)

(defcfn create-streaming-generator
  "Create a streaming generator with proceed/stop callbacks for backpressure.
   Returns h2o_generator_t* pointer."
  clj_create_streaming_generator
  [::mem/pointer ::mem/pointer ::mem/pointer ::mem/pointer] ::mem/pointer)

(defcfn req-get-res-headers
  "Get response headers pointer"
  clj_h2o_req_get_res_headers
  [::mem/pointer] ::mem/pointer)

(defcfn evloop-now
  "Get current time in milliseconds from event loop"
  clj_h2o_evloop_now
  [::mem/pointer] ::mem/long)

(defcfn context-get-active-conns
  "Get count of active connections for this context"
  clj_h2o_context_get_active_conns
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

(defcfn req-print-offsets
  "Debug helper: print h2o_req_t field offsets to stderr for struct layout verification"
  clj_h2o_req_print_offsets
  [] ::mem/void)

(defcfn mem-alloc-shared
  "Allocate memory from h2o pool. Returns pointer to allocated memory."
  h2o_mem_alloc_shared
  [::mem/pointer ::mem/long ::mem/pointer] ::mem/pointer)

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

(defn dispose-server-config
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

(defn create-accept-callback
  "Create accept callback for a listener socket with connection tracking.
   The callback signature is: void on_accept(h2o_socket_t *listener, const char *err)
   
   Parameters:
   - accept-ctx-ptr: pointer to h2o_accept_ctx_t
   - active-connections: AtomicLong for connection counting
   - on-close-callback: callback function pointer for socket close"
  [accept-ctx-ptr active-connections on-close-callback]
  (mem/serialize
   (fn [listener-ptr err-ptr]
     (when-not (mem/null? err-ptr)
       nil)

     (let [sock-ptr (evloop-socket-accept listener-ptr)]
       (when-not (mem/null? sock-ptr)
         (.incrementAndGet ^java.util.concurrent.atomic.AtomicLong active-connections)
         (socket-set-on-close sock-ptr on-close-callback (mem/as-segment 0))
         (h2o-accept accept-ctx-ptr sock-ptr))))
   [::ffi/fn [::mem/pointer ::mem/c-string] ::mem/void]))

(defn create-request-callback
  "Create the on-req upcall callback for h2o.
   This is called by native code when a request arrives."
  [callback]
  (mem/serialize
   (fn [_self-ptr req-ptr]
     (try
       (callback req-ptr)
       0
       (catch Exception e
         (println "Error enqueuing request:" (.getMessage e))
         (.printStackTrace e)
         -1)))
   [::ffi/fn [::mem/pointer ::mem/pointer] ::mem/int]))

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

;; POSIX socket syscalls via libc


