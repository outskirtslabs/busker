(ns ol.h2o.native
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [coffi.ffi :as ffi :refer [defcfn]]
   [coffi.layout :as layout]
   [coffi.mem :as mem :refer [defalias]])
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

(defcfn socket-reading? "clj_h2o_socket_is_reading" [::mem/pointer] ::mem/int)
(defcfn socket-writing? "clj_h2o_socket_is_writing" [::mem/pointer] ::mem/int)
(defcfn socket-read-cb "clj_h2o_socket_get_read_cb" [::mem/pointer] ::mem/pointer)
(defcfn socket-write-cb "clj_h2o_socket_get_write_cb" [::mem/pointer] ::mem/pointer)

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

(defcfn h2o-send
  "Send response data. iovec-array is pointer to h2o_iovec_t array, count is array length"
  h2o_send
  [::mem/pointer ::mem/pointer ::mem/long ::mem/int] ::mem/void)

(defcfn add-header
  "Add response header"
  h2o_add_header
  [::mem/pointer ::mem/pointer ::mem/pointer ::mem/pointer ::mem/c-string ::mem/long] ::mem/void)

(defcfn get-content-type-token
  "Get H2O_TOKEN_CONTENT_TYPE pointer"
  clj_h2o_get_content_type_token
  [] ::mem/pointer)

(defcfn get-static-generator
  "Get static h2o_generator_t pointer"
  clj_h2o_get_static_generator
  [] ::mem/pointer)

(defcfn req-set-status
  "Set response status code"
  clj_h2o_req_set_status
  [::mem/pointer ::mem/int] ::mem/void)

(defcfn req-set-reason
  "Set response reason phrase"
  clj_h2o_req_set_reason
  [::mem/pointer ::mem/c-string] ::mem/void)

(defcfn req-get-pool
  "Get request memory pool pointer"
  clj_h2o_req_get_pool
  [::mem/pointer] ::mem/pointer)

(defcfn req-get-res-headers
  "Get response headers pointer"
  clj_h2o_req_get_res_headers
  [::mem/pointer] ::mem/pointer)

(defcfn req-get-method
  "Get request method iovec pointer"
  clj_h2o_req_get_method
  [::mem/pointer] ::mem/pointer)

(defcfn req-get-path
  "Get request path iovec pointer"
  clj_h2o_req_get_path
  [::mem/pointer] ::mem/pointer)

(defcfn req-get-authority
  "Get request authority iovec pointer"
  clj_h2o_req_get_authority
  [::mem/pointer] ::mem/pointer)

(defcfn req-get-query-at
  "Get request query_at (SIZE_MAX if no query)"
  clj_h2o_req_get_query_at
  [::mem/pointer] ::mem/pointer)

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

(defn read-iovec-string
  "Read a string from an h2o_iovec_t pointer"
  [iovec-ptr]
  (let [iovec-size (mem/size-of ::h2o-iovec-t)
        iovec-seg (mem/reinterpret iovec-ptr iovec-size)
        iovec-data (mem/deserialize iovec-seg ::h2o-iovec-t)
        base-ptr (:base iovec-data)
        len (:len iovec-data)]
    (when (and base-ptr (pos? len))
      (let [reinterpreted (mem/reinterpret base-ptr len)
            byte-arr (byte-array len)]
        (java.lang.foreign.MemorySegment/copy reinterpreted 0
                                              (java.lang.foreign.MemorySegment/ofArray byte-arr) 0
                                              len)
        (String. byte-arr "UTF-8")))))

(defn build-ring-request
  "Build a Ring request map from h2o_req_t pointer"
  [req-ptr]
  (let [method-iovec (req-get-method req-ptr)
        path-iovec (req-get-path req-ptr)
        authority-iovec (req-get-authority req-ptr)

        method-str (read-iovec-string method-iovec)
        path-str (read-iovec-string path-iovec)
        authority-str (read-iovec-string authority-iovec)]

    {:request-method (keyword (clojure.string/lower-case method-str))
     :uri path-str
     :server-name (or authority-str "localhost")
     :server-port 8080
     :scheme :http
     :headers {}
     :protocol "HTTP/1.1"
     :remote-addr "127.0.0.1"}))

(defn create-ring-handler
  "Create an h2o handler that delegates to a Ring handler.
   Returns handler pointer that must be kept alive."
  [pathconf-ptr ring-handler]
  (let [handler-ptr (create-handler pathconf-ptr (handler-size))
        on-req-callback (mem/serialize
                         (fn [_self-ptr req-ptr]
                           (try
                             ;; Build Ring request from h2o request
                             (let [ring-req (build-ring-request req-ptr)

                                   ;; Call user's Ring handler
                                   ring-resp (ring-handler ring-req)

                                   ;; Extract response fields
                                   status (:status ring-resp 200)
                                   body (:body ring-resp "")]

                               ;; Set response status
                               (req-set-status req-ptr status)
                               (req-set-reason req-ptr "OK")

                               ;; Get request pool for allocations
                               (let [pool-ptr (req-get-pool req-ptr)]

                                 ;; Handle body as String for now
                                 (let [body-str (str body)
                                       body-bytes (.getBytes ^String body-str "UTF-8")
                                       body-len (long (alength body-bytes))

                                       ;; Allocate body using h2o's pool allocator
                                       body-ptr-raw (mem-alloc-shared pool-ptr body-len java.lang.foreign.MemorySegment/NULL)
                                       body-ptr (mem/reinterpret body-ptr-raw body-len)]

                                   ;; Write body bytes
                                   (let [byte-array-seg (java.lang.foreign.MemorySegment/ofArray body-bytes)]
                                     (java.lang.foreign.MemorySegment/copy byte-array-seg 0 body-ptr 0 body-len))

                                   ;; Allocate sendvec structure from pool
                                   (let [sendvec-size (long (mem/size-of ::h2o-sendvec-t))
                                         sendvec-ptr-raw (mem-alloc-shared pool-ptr sendvec-size java.lang.foreign.MemorySegment/NULL)
                                         sendvec-seg (mem/reinterpret sendvec-ptr-raw sendvec-size)]

                                     ;; Initialize sendvec with raw bytes
                                     (sendvec-init-raw sendvec-seg body-ptr-raw body-len)

                                     ;; Start response with generator
                                     (let [generator-ptr (get-static-generator)]
                                       (start-response req-ptr generator-ptr))

                                     ;; Send response body using sendvec (final chunk)
                                     (sendvec req-ptr sendvec-seg 1 H2O_SEND_STATE_FINAL)))

                                 ;; Return 0 for success
                                 0))
                             (catch Exception e
                               (println "Handler error:" (.getMessage e))
                               (.printStackTrace e)
                               -1)))
                         [::ffi/fn [::mem/pointer ::mem/pointer] ::mem/int])]
    (handler-set-on-req handler-ptr on-req-callback)
    handler-ptr))

(defn create-server-config
  "Create and initialize h2o global configuration with a default host and Ring handler.
   Returns map with ::arena, ::config-ptr, ::hostconf-ptr, ::pathconf-ptr, ::handler-ptr"
  [ring-handler]
  (let [arena (mem/auto-arena)
        size (globalconf-size)
        config-ptr (mem/alloc size arena)]
    (config-init config-ptr)
    (let [host-iovec-seg (create-iovec "default" arena)
          host-iovec-data (mem/deserialize host-iovec-seg ::h2o-iovec-t)
          hostconf-ptr (config-register-host config-ptr host-iovec-data 65535)
          pathconf-ptr (config-register-path hostconf-ptr "/" 0)
          handler-ptr (create-ring-handler pathconf-ptr ring-handler)]
      {::arena arena
       ::config-ptr config-ptr
       ::hostconf-ptr hostconf-ptr
       ::pathconf-ptr pathconf-ptr
       ::handler-ptr handler-ptr})))

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
  "Create accept callback for a listener socket.
   The callback signature is: void on_accept(h2o_socket_t *listener, const char *err)"
  [accept-ctx-ptr]
  (mem/serialize
   (fn [listener-ptr err-ptr]
     (when-not (mem/null? err-ptr)
       ;; Error occurred during accept - just return
       nil)

     ;; Accept the connection
     (let [sock-ptr (evloop-socket-accept listener-ptr)]
       (when-not (mem/null? sock-ptr)
         ;; Pass the socket to h2o for HTTP processing
         (h2o-accept accept-ctx-ptr sock-ptr))))
   [::ffi/fn [::mem/pointer ::mem/c-string] ::mem/void]))

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


