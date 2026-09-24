(ns ^:no-doc ol.busker.native
  "Defines FFM layouts and native calls for the Busker shim.

  This namespace maps C structures, callbacks, and libh2o operations. Key functions copy
  request data, create handlers, and manage multithread receivers.

  ## Related Namespaces

  - [[ol.busker.native.loader]] loads the shim.
  - [[ol.busker.generation]] manages native lifecycle."
  (:require
   [babashka.ffi :as ffi :refer [defcfn]]
   [clojure.string :as str]
   [ol.busker.native.loader]
   [ol.busker.util :as util]
   [taoensso.trove :as trove])
  (:import
   [java.io InputStream]
   [java.lang.foreign Arena MemorySegment]))

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

(def ffi-clj-header-t
  [:struct [[:name :pointer]
            [:name_len :long]
            [:value :pointer]
            [:value_len :long]]])

(def ffi-h2o-iovec-t
  [:struct [[:base :pointer]
            [:len :long]]])

(def ffi-clj-fixed-response-slot-data-t
  [:struct [[:claim-token :long]
            [:module-id :long]
            [:request-seq :long]
            [:headers-len :long]
            [:content-length :long]
            [:body-offset :long]
            [:body-len :long]
            [:payload-len :long]
            [:status :int]
            [:compress-hint :int]
            [:headers [:array ffi-clj-header-t 64]]]])

(def ffi-h2o-accept-ctx-t
  [:struct [[:ctx :pointer]
            [:hosts :pointer]
            [:ssl_ctx :pointer]
            [:http2_origin_frame :pointer]
            [:expect_proxy_line :int]
            [:libmemcached_receiver :pointer]]])

(def ffi-h2o-sendvec-t
  [:struct [[:callbacks :pointer]
            [:len :long]
            [:raw :pointer]
            [:cb_arg_padding :long]]])

(def size-of-h2o-sendvec-t (ffi/sizeof ffi-h2o-sendvec-t))

(def ffi-h2o-header-t
  [:struct [[:name :pointer]
            [:orig_name :pointer]
            [:value ffi-h2o-iovec-t]
            [:flags :char]]])

(def ffi-h2o-generator-t
  [:struct [[:proceed :pointer]
            [:stop :pointer]]])

(def ffi-clj-req-meta-t
  [:struct [[:authority :pointer]
            [:method :pointer]
            [:path :pointer]
            [:remote_addr :pointer]
            [:scheme :pointer]
            [:headers :pointer]
            [:authority_len :long]
            [:method_len :long]
            [:path_len :long]
            [:remote_addr_len :long]
            [:scheme_len :long]
            [:headers_len :long]
            [:http_version :int]
            [:has_body :int16]
            [:is_early_data :int16]]])

(def ffi-clj-req-ctx-t
  [:struct [[:req :pointer]
            [:meta ffi-clj-req-meta-t]
            [:on-cleanup :pointer]
            [:on-request-body-chunk :pointer]
            [:response-receiver :pointer]
            [:response-hash-next :pointer]
            [:generator ffi-h2o-generator-t]
            [:on-response-generator-proceed :pointer]
            [:on-response-generator-stop :pointer]
            [:preferred-chunk-size :long]
            [:dispatch-module-id :long]
            [:dispatch-request-seq :long]
            [:cleanup :int]
            [:closing :int]
            [:response_started :int]]])

(def ffi-clj-h2o-flat-globalconf-t
  [:struct [[:has_server_name :int]
            [:server_name :string]
            [:has_proxy_status_identity :int]
            [:proxy_status_identity :string]
            [:has_max_request_entity_size :int]
            [:max_request_entity_size :long]
            [:has_max_delegations :int]
            [:max_delegations :uint]
            [:has_max_reprocesses :int]
            [:max_reprocesses :uint]
            [:has_handshake_timeout :int]
            [:handshake_timeout :long]
            [:has_max_spare_pipes :int]
            [:max_spare_pipes :long]
            [:has_http1__req_timeout :int]
            [:http1__req_timeout :long]
            [:has_http1__req_io_timeout :int]
            [:http1__req_io_timeout :long]
            [:has_http1__upgrade_to_http2 :int]
            [:http1__upgrade_to_http2 :int]
            [:has_http2__idle_timeout :int]
            [:http2__idle_timeout :long]
            [:has_http2__graceful_shutdown_timeout :int]
            [:http2__graceful_shutdown_timeout :long]
            [:has_http2__max_streams :int]
            [:http2__max_streams :uint32]
            [:has_http2__max_concurrent_requests_per_connection :int]
            [:http2__max_concurrent_requests_per_connection :long]
            [:has_http2__max_concurrent_streaming_requests_per_connection :int]
            [:http2__max_concurrent_streaming_requests_per_connection :long]
            [:has_http2__max_streams_for_priority :int]
            [:http2__max_streams_for_priority :long]
            [:has_http2__active_stream_window_size :int]
            [:http2__active_stream_window_size :uint32]
            [:has_http2__dos_delay :int]
            [:http2__dos_delay :long]
            [:has_http3__idle_timeout :int]
            [:http3__idle_timeout :long]
            [:has_http3__graceful_shutdown_timeout :int]
            [:http3__graceful_shutdown_timeout :long]
            [:has_http3__active_stream_window_size :int]
            [:http3__active_stream_window_size :uint32]
            [:has_http3__ack_frequency :int]
            [:http3__ack_frequency :uint16]
            [:has_compress_args :int]
            [:compress_args_mine_size :long]
            [:compress_args_gzip_quality :int]
            [:compress_args_brotli_quality :int]
            [:compress_args_zstd_quality :int]]])

(def ffi-clj-session-ticket-t
  [:struct [[:name [:array :byte 16]]
            [:aes_key [:array :byte 32]]
            [:hmac_key [:array :byte 64]]
            [:not_before :long]
            [:not_after :long]]])

(defcfn evloop-create
  "Creates a new event loop. Returns a pointer to h2o_evloop_t."
  "h2o_evloop_create"
  [] :pointer)

(defcfn evloop-destroy
  "Destroys an event loop and frees associated resources."
  "h2o_evloop_destroy"
  [:pointer] :void)

(defcfn evloop-run
  "Runs the event loop once. Returns 0 if successful, -1 on error (typically EINTR).

   Parameters:
   - loop: pointer to h2o_evloop_t
   - max-wait: maximum time to wait in milliseconds (int32)"
  "h2o_evloop_run"
  [:pointer :int] :int)

(defcfn globalconf-size
  "Get size of h2o_globalconf_t structure"
  "clj_h2o_globalconf_size"
  [] :long)

(defcfn create-global-conf
  "Create, initialize, and configure a new h2o_globalconf_t with our configuration"
  "clj_h2o_create_globalconf"
  [:pointer :pointer] :void)

(defcfn config-dispose
  "Dispose h2o global configuration and free resources"
  "h2o_config_dispose"
  [:pointer] :void)

(defcfn context-size
  "Get size of h2o_context_t structure"
  "clj_h2o_context_size"
  [] :long)

(defcfn req-ctx-size
  "Gets the native size of `clj_req_ctx_t`."
  "clj_h2o_req_ctx_size"
  [] :long)

(defcfn accept-ctx-size
  "Get size of h2o_accept_ctx_t structure"
  "clj_h2o_accept_ctx_size"
  [] :long)

(defcfn globalconf-get-hosts
  "Get hosts pointer (h2o_hostconf_t**) from h2o_globalconf_t"
  "clj_h2o_globalconf_get_hosts"
  [:pointer] :pointer)

(ffi/defcfn config-register-host
  "Register a virtual host with the h2o configuration. Returns its native pointer."
  "h2o_config_register_host"
  [:pointer ffi-h2o-iovec-t :uint16] :pointer)
(defcfn context-init
  "Initialize h2o context for an event loop.

   Parameters:
   - context: pointer to h2o_context_t
   - loop: pointer to h2o_evloop_t
   - config: pointer to h2o_globalconf_t"
  "h2o_context_init"
  [:pointer :pointer :pointer] :void)

(defcfn context-dispose
  "Dispose h2o context and free resources"
  "h2o_context_dispose"
  [:pointer] :void)

(defcfn context-request-shutdown
  "Request graceful shutdown of all connections in this context (sends GOAWAY)"
  "h2o_context_request_shutdown"
  [:pointer] :void)

(defcfn evloop-socket-create
  "Create h2o socket wrapper for file descriptor.

   Parameters:
   - loop: pointer to h2o_evloop_t
   - fd: file descriptor (int)
   - flags: socket flags (int)

   Returns: pointer to h2o_socket_t"
  "h2o_evloop_socket_create"
  [:pointer :int :int] :pointer)

(defcfn socket-read-start
  "Start reading from socket with callback.

   Parameters:
   - sock: pointer to h2o_socket_t
   - cb: callback function pointer"
  "h2o_socket_read_start"
  [:pointer :pointer] :void)

(defcfn socket-read-stop
  "Stop reading from socket.

   Parameters:
   - sock: pointer to h2o_socket_t"
  "h2o_socket_read_stop"
  [:pointer] :void)

(defcfn socket-reading? "clj_h2o_socket_is_reading" [:pointer] :int)
(defcfn socket-writing? "clj_h2o_socket_is_writing" [:pointer] :int)
(defcfn socket-read-cb "clj_h2o_socket_get_read_cb" [:pointer] :pointer)
(defcfn socket-write-cb "clj_h2o_socket_get_write_cb" [:pointer] :pointer)

(defcfn socket-set-on-close
  "Set socket close callback for connection tracking.

   Parameters:
   - sock: pointer to h2o_socket_t
   - callback: function pointer for on_close callback
   - data: user data pointer passed to callback"
  "clj_h2o_socket_set_on_close"
  [:pointer :pointer :pointer] :void)

(defcfn evloop-socket-accept
  "Accept new connection from listening socket.
   Returns pointer to h2o_socket_t or NULL"
  "h2o_evloop_socket_accept"
  [:pointer] :pointer)

(defcfn socket-close
  "Close h2o socket"
  "h2o_socket_close"
  [:pointer] :void)

(defcfn h2o-accept
  "Pass accepted socket to h2o for HTTP handling.

   Parameters:
   - ctx: pointer to h2o_accept_ctx_t
   - sock: pointer to h2o_socket_t"
  "h2o_accept"
  [:pointer :pointer] :void)

(defcfn sendvec-init-raw
  "Initialize a sendvec with raw bytes"
  "h2o_sendvec_init_raw"
  [:pointer :pointer :long] :void)

(defcfn sendvec
  "Send response data using sendvec"
  "h2o_sendvec"
  [:pointer :pointer :long :int] :void)

(defcfn start-response
  "Start sending HTTP response"
  "clj_h2o_start_response"
  [:pointer :int :pointer :long :long :int :pointer :pointer] :long)

(defcfn send-informational
  "Sends 1xx response"
  "clj_h2o_send_informational"
  [:pointer :int :pointer :long]
  :void)

(defcfn send-fixed-final
  "Sends a fixed final response with inline body bytes."
  "clj_h2o_send_fixed_final"
  [:pointer :int :pointer :long :long :int
   :pointer :long]
  :void)

(defcfn cancel-request
  "Cancel a request after the response has started"
  "clj_h2o_cancel_request"
  [:pointer] :int)

(defn report-almost-fatal-error [msg e]
  (binding [*out* *err*]
    (tap> [msg e])
    (println msg)
    (when e
      (.printStackTrace ^Throwable e ^java.io.PrintWriter *err*))))

(ffi/defcfn create-handler*
  "Construct an H2O handler with its request and cleanup callbacks."
  "clj_h2o_create_handler"
  [:pointer :pointer :pointer :pointer] :pointer)

(defcfn handler-set-shutting-down
  "Update the handler shutting_down flag (1 means shutdown in progress)."
  "clj_h2o_handler_set_shutting_down"
  [:pointer :int] :void)

(defcfn proceed-req
  "Call req->proceed_req to signal readiness for next request body chunk"
  "clj_h2o_proceed_req"
  [:pointer] :void)

(defcfn install-request-dispatch
  "Installs scalar request identity and its stable body callback."
  "clj_h2o_install_request_dispatch"
  [:pointer :pointer :long :long :pointer] :int)

(defcfn evloop-now
  "Get current time in milliseconds from event loop"
  "clj_h2o_evloop_now"
  [:pointer] :long)

(defcfn context-get-active-conns
  "Get count of active connections for this context"
  "clj_h2o_context_get_active_conns"
  [:pointer] :long)

(defcfn context-get-idle-conns
  "Get count of idle connections for this context"
  "clj_h2o_context_get_idle_conns"
  [:pointer] :long)

(defcfn context-get-shutdown-conns
  "Get count of shutdown connections for this context"
  "clj_h2o_context_get_shutdown_conns"
  [:pointer] :long)

(defcfn cleanup-thread
  "Perform periodic cleanup tasks for a context.
   Returns maximum wait time in milliseconds before next cleanup.

   Parameters:
   - now: current time in milliseconds (from evloop-now)
   - ctx: pointer to h2o_context_t"
  "h2o_cleanup_thread"
  [:long :pointer] :int)

(defcfn mt-create-wakeup-receiver
  "Register a wakeup receiver on ctx->queue (returns opaque pointer)"
  "clj_h2o_mt_create_wakeup_receiver"
  [:pointer] :pointer)

(defcfn mt-create-response-receiver
  "Registers a fixed-response receiver on the H2O context queue."
  "clj_h2o_mt_create_response_receiver"
  [:pointer :long :long] :pointer)

(defcfn mt-destroy-response-receiver
  "Unregisters a drained fixed-response receiver."
  "clj_h2o_mt_destroy_response_receiver"
  [:pointer] :void)

(defcfn mt-response-pending
  "Returns the number of submitted fixed responses not yet consumed by H2O."
  "clj_h2o_mt_response_pending"
  [:pointer] :long)

(defcfn mt-response-slot-data-size*
  "clj_h2o_response_slot_data_size"
  [] :long)

(defn mt-response-slot-data-size
  "Returns the fixed metadata size before slot payload bytes."
  []
  #_{:clj-kondo/ignore [:type-mismatch]}
  (long (mt-response-slot-data-size*)))

(defcfn mt-response-ring-capacity*
  "clj_h2o_response_ring_capacity"
  [:pointer] :long)

(defn mt-response-ring-capacity
  "Returns the fixed number of response slots."
  [receiver]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (long (mt-response-ring-capacity* receiver)))

(defcfn mt-response-slot-payload-capacity*
  "clj_h2o_response_slot_payload_capacity"
  [:pointer] :long)

(defn mt-response-slot-payload-capacity
  "Returns the byte capacity after each slot metadata area."
  [receiver]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (long (mt-response-slot-payload-capacity* receiver)))

(defcfn mt-response-claimed*
  "clj_h2o_response_ring_claimed"
  [:pointer] :long)

(defn mt-response-claimed
  "Returns the current number of claimed response slots."
  [receiver]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (long (mt-response-claimed* receiver)))

(defcfn mt-response-ring-ready*
  "clj_h2o_response_ring_ready"
  [:pointer] :long)

(defn mt-response-ring-ready
  "Returns the current number of ready response slots."
  [receiver]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (long (mt-response-ring-ready* receiver)))

(defcfn mt-response-slot-data*
  "clj_h2o_response_slot_data"
  [:pointer :long] :pointer)

(defn mt-response-slot-data
  "Returns one slot data pointer for generation-time binding."
  [receiver index]
  (mt-response-slot-data* receiver index))

(defcfn mt-response-try-claim*
  "clj_h2o_response_try_claim"
  [:pointer] :long)

(defn mt-response-try-claim
  "Claims a response slot immediately and returns its scalar handle, or zero.

   A nonzero handle permits a sequential publication attempt or an abort.
   When packing succeeds, call [[mt-response-publish]] at most once; if it
   returns zero, call [[mt-response-abort]] once. When packing fails, skip
   publication and call [[mt-response-abort]] once. Do not share a handle,
   reuse it, or make concurrent stale calls; those uses have no supported contract."
  [receiver]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (long (mt-response-try-claim* receiver)))

(defcfn mt-response-publish*
  "clj_h2o_response_publish"
  [:pointer :long] :int)

(defn mt-response-publish
  "Attempts publication for a claimed response slot.

   Returns `1` when publication consumes `claim-handle`. On `0`, call
   [[mt-response-abort]] once to release the unpublished claim. This function
   requires the claimant's sequential, single-use handle; concurrent stale
   calls have no supported contract."
  [receiver claim-handle]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (long (mt-response-publish* receiver claim-handle)))

(defcfn mt-response-abort*
  "clj_h2o_response_abort"
  [:pointer :long] :int)

(defn mt-response-abort
  "Releases an unpublished claimed response slot.

   Call this once after a failed [[mt-response-publish]], or instead of
   publication. Returns `1` when it releases `claim-handle`. This function
   requires the claimant's sequential, single-use handle; concurrent stale
   calls have no supported contract."
  [receiver claim-handle]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (long (mt-response-abort* receiver claim-handle)))

(defcfn mt-response-ring-drain*
  "clj_h2o_response_ring_drain"
  [:pointer] :long)

(defn mt-response-ring-drain
  "Applies ready responses on the H2O event-loop thread."
  [receiver]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (long (mt-response-ring-drain* receiver)))

(defcfn mt-destroy-wakeup-receiver
  "Unregister and free the wakeup receiver"
  "clj_h2o_mt_destroy_wakeup_receiver"
  [:pointer] :void)

(defcfn mt-wakeup
  "Send a wakeup message to the loop owning this receiver"
  "clj_h2o_mt_wakeup"
  [:pointer] :void)

(defcfn create-ssl-ctx
  "Create and configure SSL_CTX for TLS listener.

   Parameters:
   - cert-file: path to PEM certificate file (optional, must pair with key-file)
   - key-file: path to PEM private key file (optional, must pair with cert-file)
   - enable-http2: 1 to register HTTP/2 ALPN protocols, 0 for HTTP/1.1 only
   - tls-lookup-cb-ptr: callback pointer for SNI lookup, or NULL
   - tls-lookup-user-ctx: user context pointer passed to callback, or NULL

   Returns: SSL_CTX pointer on success, NULL on error"
  "clj_h2o_create_ssl_ctx"
  [:string :string :int :pointer :pointer] :pointer)

(defcfn free-ssl-ctx
  "Free SSL_CTX created by [[create-ssl-ctx]]."
  "clj_h2o_free_ssl_ctx"
  [:pointer] :void)

(ffi/defcfn tls-bytes-dup
  "Duplicate bytes into native-heap memory used by TLS callbacks."
  "clj_h2o_tls_memdup"
  [:pointer :long] :pointer)

(defn- string->tls-native-bytes
  [^String s]
  (let [bytes (.getBytes s "UTF-8")
        len (alength ^bytes bytes)]
    (if (pos? len)
      (with-open [arena (ffi/confined-arena)]
        (let [source (MemorySegment/ofArray bytes)
              staged (ffi/alloc arena len)
              _ (MemorySegment/copy source 0 staged 0 len)
              ptr (tls-bytes-dup staged len)]
          {:ptr ptr :len len}))
      {:ptr ffi/null :len 0})))

(defn- write-tls-native-outputs!
  [cert-out-seg cert-len-out-seg key-out-seg key-len-out-seg
   cert-chain-pem private-key-pem]
  (let [{cert-ptr :ptr cert-len :len}
        (string->tls-native-bytes cert-chain-pem)
        {key-ptr :ptr key-len :len}
        (string->tls-native-bytes private-key-pem)
        valid? (and (not (ffi/null? cert-ptr))
                    (not (ffi/null? key-ptr))
                    (pos? cert-len)
                    (pos? key-len))]
    (ffi/write cert-out-seg :pointer cert-ptr)
    (ffi/write cert-len-out-seg :long (long cert-len))
    (ffi/write key-out-seg :pointer key-ptr)
    (ffi/write key-len-out-seg :long (long key-len))
    (if valid? 1 -1)))

(defn- reset-tls-native-outputs!
  [cert-out-seg cert-len-out-seg key-out-seg key-len-out-seg]
  (ffi/write cert-out-seg :pointer ffi/null)
  (ffi/write cert-len-out-seg :long 0)
  (ffi/write key-out-seg :pointer ffi/null)
  (ffi/write key-len-out-seg :long 0))

(defn build-tls-lookup-callback
  "Build Clojure callback pointer for native TLS handshakes.
   `lookup-fn` takes hostname and returns either:
   - nil for miss
   - {:cert-chain-pem \"...\" :private-key-pem \"...\"} for success

   Hostname is nil when client hello omits SNI.

   Returns pinned callback refs that must be retained while server runs.
   The seven-argument callback requires a JVM; native-image upcalls cannot support
   this unchanged signature.
   This function does not mutate process-global native state."
  [lookup-fn]
  {:pre [(ifn? lookup-fn)]}
  (let [cb     (fn [sni sni-len cert-out cert-len-out key-out key-len-out _user-ctx]
                 (try
                   (let [sni-len          (long sni-len)
                         hostname         (when (pos? sni-len)
                                            (when-not (ffi/null? sni)
                                              (String. ^bytes (ffi/read-array
                                                               (ffi/reinterpret sni sni-len)
                                                               :byte sni-len)
                                                       "UTF-8")))
                         cert-out-seg     (ffi/reinterpret cert-out (ffi/sizeof :pointer))
                         cert-len-out-seg (ffi/reinterpret cert-len-out (ffi/sizeof :long))
                         key-out-seg      (ffi/reinterpret key-out (ffi/sizeof :pointer))
                         key-len-out-seg  (ffi/reinterpret key-len-out (ffi/sizeof :long))]
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
                       (let [cert-out-seg     (ffi/reinterpret cert-out (ffi/sizeof :pointer))
                             cert-len-out-seg (ffi/reinterpret cert-len-out (ffi/sizeof :long))
                             key-out-seg      (ffi/reinterpret key-out (ffi/sizeof :pointer))
                             key-len-out-seg  (ffi/reinterpret key-len-out (ffi/sizeof :long))]
                         (reset-tls-native-outputs! cert-out-seg
                                                    cert-len-out-seg
                                                    key-out-seg
                                                    key-len-out-seg))
                       (catch Throwable _
                         nil))
                     -1)))
        cb-ptr (ffi/callback (ffi/auto-arena) cb
                             [:pointer :long :pointer :pointer
                              :pointer :pointer :pointer] :int)]
    {:callback     cb
     :callback-ptr cb-ptr}))

(defn str->iovec
  [s arena]
  (let [str-ptr (ffi/string->ptr arena s)
        len (dec (.byteSize ^MemorySegment str-ptr))]
    {:base str-ptr :len len}))

(defn create-context
  "Create and initialize h2o context for an event loop.
   Returns pointer to h2o_context_t"
  [arena loop-ptr config-ptr]
  {:pre [(some? arena)
         (some? loop-ptr) (not (ffi/null? loop-ptr))
         (some? config-ptr) (not (ffi/null? config-ptr))]}
  (let [size (context-size)
        ctx-ptr (ffi/alloc arena size)]
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
         (some? config-ptr) (not (ffi/null? config-ptr))]}
  (vec (for [loop loops]
         (create-context arena loop config-ptr))))

(defn create-socket-for-loop
  "Create h2o socket wrapper for file descriptor in event loop"
  [loop-ptr fd flags]
  {:pre [(some? loop-ptr) (not (ffi/null? loop-ptr))
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
         (some? ctx-ptr) (not (ffi/null? ctx-ptr))
         (some? config-ptr) (not (ffi/null? config-ptr))]}
  (let [hosts-ptr (globalconf-get-hosts config-ptr)
        ssl-ctx (if (and ssl-ctx-ptr (not (ffi/null? ssl-ctx-ptr)))
                  ssl-ctx-ptr
                  ffi/null)
        accept-ctx-data {:ctx ctx-ptr
                         :hosts hosts-ptr
                         :ssl_ctx ssl-ctx
                         :http2_origin_frame ffi/null
                         :expect_proxy_line 0
                         :libmemcached_receiver ffi/null}
        ptr (ffi/alloc arena ffi-h2o-accept-ctx-t)]
    (ffi/write ptr ffi-h2o-accept-ctx-t accept-ctx-data)
    ptr))

(defn ->string
  "Reads `len` UTF-8 bytes from `ptr`. Returns nil for a null pointer.
  The optional `arena` supplies the scope for the temporary native view."
  ([ptr ^long len]
   (when (and ptr (not (ffi/null? ptr)) (pos? len))
     (->string ptr len (ffi/auto-arena))))
  ([ptr ^long len ^Arena arena]
   (when (and ptr (not (ffi/null? ptr)) (pos? len))
     (String. ^bytes (ffi/read-array (ffi/reinterpret ptr len arena) :byte len)
              "UTF-8"))))

(defn build-ring-headers-map
  ([headers headers-len]
   (when (and (not (ffi/null? headers)) (pos? headers-len))
     (build-ring-headers-map headers headers-len (ffi/auto-arena))))
  ([headers headers-len ^Arena arena]
   (when (and (not (ffi/null? headers)) (pos? headers-len))
     (let [header-size (ffi/sizeof ffi-clj-header-t)
           total-size (* headers-len header-size)
           sized-headers (ffi/reinterpret headers total-size arena)]
       (reduce
        (fn [result i]
          (let [header-seg (ffi/slice sized-headers (* i header-size) header-size)
                {:keys [name name_len value value_len]}
                (ffi/read header-seg ffi-clj-header-t)
                name-str (str/lower-case (->string name name_len arena))
                value-str (->string value value_len arena)
                delimiter (if (= "cookie" name-str) ";" ",")]
            (if (contains? result name-str)
              (update result name-str str delimiter value-str)
              (assoc result name-str value-str))))
        {}
        (range headers-len))))))

(defn- default-server-port
  [scheme-str]
  (if (= "https" scheme-str)
    443
    80))

(defn ^:no-doc copy-request-context
  ([ctx-ptr]
   (copy-request-context ctx-ptr (ffi/auto-arena)))
  ([ctx-ptr ^Arena arena]
   (let [ctx (ffi/reinterpret ctx-ptr (ffi/sizeof ffi-clj-req-ctx-t) arena)
         req (ffi/read ctx (ffi/place ffi-clj-req-ctx-t :req))
         {:keys [method method_len path path_len authority authority_len
                 scheme scheme_len remote_addr remote_addr_len headers headers_len
                 http_version has_body is_early_data]}
         (ffi/read ctx (ffi/place ffi-clj-req-ctx-t :meta))]
     {:req req
      :has-body has_body
      :ring-data
      {:method (->string method method_len arena)
       :path (->string path path_len arena)
       :authority (->string authority authority_len arena)
       :scheme (->string scheme scheme_len arena)
       :remote-addr (->string remote_addr remote_addr_len arena)
       :headers (build-ring-headers-map headers headers_len arena)
       :http-version http_version
       :has-body has_body
       :early-data is_early_data}})))

(defn create-handler
  "Creates and configures an H2O handler with its request and cleanup callbacks.

  `hostconf-ptr` and `flat-config-ptr` are native configuration pointers; callback
  functions receive decoded request data and request identity respectively; `arena`
  retains the native callback trampolines. Returns the handler pointer and references
  that keep both trampolines reachable."
  [hostconf-ptr on-req-callback on-cleanup-callback flat-config-ptr arena]
  {:pre [(some? hostconf-ptr) (not (ffi/null? hostconf-ptr))
         (fn? on-req-callback)
         (fn? on-cleanup-callback)
         (some? flat-config-ptr) (not (ffi/null? flat-config-ptr))
         (some? arena)]}
  (let [on-request-cb (fn on-request-cb [ctx-ptr]
                        (int (on-req-callback ctx-ptr
                                              (copy-request-context ctx-ptr arena))))
        on-request-cb-ptr (ffi/callback arena on-request-cb [:pointer] :int)
        on-request-cleanup-cb (fn on-request-cleanup-cb [module-id request-seq]
                                (on-cleanup-callback module-id request-seq))
        on-request-cleanup-cb-ptr
        (ffi/callback arena on-request-cleanup-cb [:long :long] :void)]
    {::on-request-cb on-request-cb
     ::on-request-cleanup-cb on-request-cleanup-cb
     ::on-request-cb-ptr on-request-cb-ptr
     ::on-request-cleanup-cb-ptr on-request-cleanup-cb-ptr
     ::handler-ptr (create-handler* hostconf-ptr on-request-cb-ptr
                                    on-request-cleanup-cb-ptr flat-config-ptr)}))
(defn- force-overlay-request
  ^clojure.lang.IPersistentMap [^clojure.lang.Delay request_ overlay removed]
  (reduce dissoc (merge @request_ overlay) removed))

(deftype OverlayLazyRingRequest [^clojure.lang.Delay request_ overlay removed metadata]
  clojure.lang.IPersistentMap
  (assoc [_ key value]
    (OverlayLazyRingRequest. request_ (assoc overlay key value) (disj removed key) metadata))
  (assocEx [this key value]
    (if (.containsKey ^clojure.lang.Associative this key)
      (throw (RuntimeException. "Key already present"))
      (.assoc ^clojure.lang.Associative this key value)))
  (without [_ key]
    (OverlayLazyRingRequest. request_ (dissoc overlay key) (conj removed key) metadata))

  clojure.lang.Associative
  (containsKey [_ key]
    (and (not (contains? removed key))
         (or (contains? overlay key) (contains? @request_ key))))
  (entryAt [this key]
    (when (.containsKey ^clojure.lang.Associative this key)
      (clojure.lang.MapEntry/create key (.valAt ^clojure.lang.ILookup this key))))

  clojure.lang.ILookup
  (valAt [_ key]
    (cond
      (contains? removed key) nil
      (contains? overlay key) (get overlay key)
      :else (get @request_ key)))
  (valAt [_ key not-found]
    (cond
      (contains? removed key) not-found
      (contains? overlay key) (get overlay key)
      :else (get @request_ key not-found)))

  clojure.lang.IPersistentCollection
  (count [_] (count (force-overlay-request request_ overlay removed)))
  (cons [_ value] (conj (force-overlay-request request_ overlay removed) value))
  (empty [_] (with-meta {} metadata))
  (equiv [_ other] (= (force-overlay-request request_ overlay removed) other))

  clojure.lang.Seqable
  (seq [_] (seq (force-overlay-request request_ overlay removed)))

  java.lang.Iterable
  (iterator [_] (.iterator ^java.lang.Iterable (force-overlay-request request_ overlay removed)))

  clojure.lang.IFn
  (invoke [this key] (.valAt ^clojure.lang.ILookup this key))
  (invoke [this key not-found] (.valAt ^clojure.lang.ILookup this key not-found))
  (applyTo [this args]
    (case (count args)
      1 (.invoke ^clojure.lang.IFn this (first args))
      2 (.invoke ^clojure.lang.IFn this (first args) (second args))
      (throw (clojure.lang.ArityException. (count args) "OverlayLazyRingRequest"))))

  clojure.lang.MapEquivalence

  clojure.lang.IHashEq
  (hasheq [_] (hash (force-overlay-request request_ overlay removed)))

  java.util.Map
  (size [_] (count (force-overlay-request request_ overlay removed)))
  (isEmpty [_] (empty? (force-overlay-request request_ overlay removed)))
  (containsValue [_ value] (.containsValue ^java.util.Map (force-overlay-request request_ overlay removed) value))
  (get [this key] (.valAt ^clojure.lang.ILookup this key))
  (put [_ key value] (.put ^java.util.Map (force-overlay-request request_ overlay removed) key value))
  (remove [_ key] (.remove ^java.util.Map (force-overlay-request request_ overlay removed) key))
  (putAll [_ values] (.putAll ^java.util.Map (force-overlay-request request_ overlay removed) values))
  (clear [_] (.clear ^java.util.Map (force-overlay-request request_ overlay removed)))
  (keySet [_] (.keySet ^java.util.Map (force-overlay-request request_ overlay removed)))
  (values [_] (.values ^java.util.Map (force-overlay-request request_ overlay removed)))
  (entrySet [_] (.entrySet ^java.util.Map (force-overlay-request request_ overlay removed)))

  clojure.lang.IMeta
  (meta [_] metadata)

  clojure.lang.IObj
  (withMeta [_ new-metadata]
    (OverlayLazyRingRequest. request_ overlay removed new-metadata))

  Object
  (equals [_ other] (.equals ^Object (force-overlay-request request_ overlay removed) other))
  (hashCode [_] (.hashCode ^Object (force-overlay-request request_ overlay removed)))
  (toString [_] (.toString ^Object (force-overlay-request request_ overlay removed))))

(defn ^:no-doc overlay-lazy-ring-request
  [request-fn overlay]
  (OverlayLazyRingRequest. (delay (request-fn)) overlay #{} nil))

(defn ^:no-doc assemble-ring-request
  [{:keys [method path authority scheme remote-addr headers
           http-version has-body early-data]}
   ^InputStream input-stream]
  (let [default-port (default-server-port scheme)
        {:keys [server-name server-port]}
        (util/parse-authority authority default-port)
        [uri query-string] (if path
                             (let [idx (str/index-of path "?")]
                               (if idx
                                 [(subs path 0 idx) (subs path (inc idx))]
                                 [path nil]))
                             [nil nil])
        protocol (case (int http-version)
                   0x0101 "HTTP/1.1"
                   0x0200 "HTTP/2.0"
                   0x0300 "HTTP/3.0"
                   "HTTP/1.1")]
    {:server-port server-port
     :server-name server-name
     :remote-addr (or remote-addr "")
     :uri uri
     :query-string query-string
     :scheme (keyword (or scheme "http"))
     :request-method (keyword (str/lower-case (or method "get")))
     :protocol protocol
     :headers headers
     :body (when (= 1 has-body)
             input-stream)
     :ol.busker/early-data? (= 1 early-data)}))

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
  "clj_h2o_create_ptls_ctx"
  [:string :string :pointer :pointer] :pointer)

(defcfn http3-free-ptls-ctx
  "Free picotls context and associated resources."
  "clj_h2o_free_ptls_ctx"
  [:pointer] :void)

(defcfn http3-create-quicly-ctx
  "Create quicly context configured for HTTP/3.
   Starts from quicly_spec_context and configures TLS, CID encryptor,
   and HTTP/3 transport parameters.

   Parameters:
   - ptls-ctx: picotls context from [[http3-create-ptls-ctx]]
   - globalconf: h2o global configuration for HTTP/3 settings

   Returns: quicly_context_t pointer on success, NULL on error"
  "clj_h2o_create_quicly_ctx"
  [:pointer :pointer] :pointer)

(defcfn http3-free-quicly-ctx
  "Free quicly context and CID encryptor."
  "clj_h2o_free_quicly_ctx"
  [:pointer] :void)

(defcfn http3-open-udp-transport
  "Open a pooled UDP transport reservation for HTTP/3.
   The returned handle owns the pooled transport metadata and any reservation
   socket until [[http3-release-udp-transport]] is called."
  "clj_h2o_http3_open_udp_transport"
  [:string :int16] :pointer)

(defcfn http3-attach-udp-transport
  "Attach an HTTP/3 worker context to a pooled UDP transport.
   The worker attachment starts non-accepting until the transport activates
   the generation identified by `node-id`."
  "clj_h2o_http3_attach_udp_transport"
  [:pointer :pointer :pointer :pointer
   :pointer :long :int] :pointer)

(defcfn http3-activate-udp-transport-generation
  "Mark `node-id` as the active acceptor on a pooled UDP transport.
   Existing connections for older generations continue to route by CID,
   while new Initial packets are forwarded to the active generation."
  "clj_h2o_http3_activate_udp_transport_generation"
  [:pointer :long] :void)

(defcfn http3-detach-udp-transport
  "Detach an HTTP/3 worker context from a pooled UDP transport.
   This closes only the worker-owned dup'd fd."
  "clj_h2o_http3_detach_udp_transport"
  [:pointer] :void)

(defcfn http3-release-udp-transport
  "Release a pooled UDP transport and close any reservation socket it owns."
  "clj_h2o_http3_release_udp_transport"
  [:pointer] :void)

(defcfn http3-stop-accepting
  "Stop accepting new HTTP/3 connections by setting acceptor to NULL.
   Existing connections (including those in handshake) continue to completion.
   Call this before requesting shutdown to prevent new connection attempts."
  "clj_h2o_http3_stop_accepting"
  [:pointer] :void)

(defcfn http3-num-connections
  "Get the number of active HTTP/3 connections on this context."
  "clj_h2o_http3_num_connections"
  [:pointer] :long)

(defcfn http3-free-worker-ctx
  "Free HTTP/3 worker context and close UDP socket.
   Note: all connections must be closed first (num_connections == 0)."
  "clj_h2o_http3_dispose_worker_ctx"
  [:pointer] :void)

(defcfn conn-limit-set-max
  "Set the maximum allowed connections. Zero means unlimited.
   Process-global counter shared across all workers and listeners."
  "clj_h2o_conn_limit_set_max"
  [:int] :void)

(defcfn conn-limit-current
  "Get current global connection count."
  "clj_h2o_conn_limit_current"
  [] :int)

(defcfn conn-limit-try-acquire
  "Atomically try to acquire a connection slot.
   Returns 1 if acquired (count was < max), 0 if at limit."
  "clj_h2o_conn_limit_try_acquire"
  [] :int)

(defcfn conn-limit-release
  "Release a connection slot (decrement counter)."
  "clj_h2o_conn_limit_release"
  [] :void)

(def size-of-session-ticket-t (ffi/sizeof ffi-clj-session-ticket-t))

(defn session-ticket-keys->native-array
  "Allocate and populate a contiguous native array of `clj_session_ticket_t`.

  Input is a seq of maps ordered newest-first, where each map contains:
  `:name` as a 16-byte array, `:aes-key` as a 32-byte array, `:hmac-key` as a
  64-byte array, and `:not-before` / `:not-after` as epoch-millisecond longs.

  Returns a native memory segment allocated in `arena` containing one
   `clj_session_ticket_t` struct per input key in the same order."
  [keys arena]
  (let [struct-size size-of-session-ticket-t
        n (count keys)
        segment (ffi/alloc arena (* n struct-size))]
    (doseq [[i k] (map-indexed vector keys)]
      (let [offset (* i struct-size)
            name-bytes ^bytes (:name k)
            aes-bytes ^bytes (:aes-key k)
            hmac-bytes ^bytes (:hmac-key k)]
        (doseq [j (range 16)]
          (ffi/write segment :byte (aget name-bytes j) (+ offset j)))
        (doseq [j (range 32)]
          (ffi/write segment :byte (aget aes-bytes j) (+ offset 16 j)))
        (doseq [j (range 64)]
          (ffi/write segment :byte (aget hmac-bytes j) (+ offset 48 j)))
        (ffi/write segment :long (:not-before k) (+ offset 112))
        (ffi/write segment :long (:not-after k) (+ offset 120))))
    segment))

(defcfn ticket-manager-create
  "Create a new ticket manager with specified ticket lifetime.
   Returns pointer to clj_ticket_manager_t, or NULL on failure."
  "clj_ticket_manager_create"
  [:int] :pointer)

(defcfn ticket-manager-destroy
  "Destroy ticket manager and securely erase all keys."
  "clj_ticket_manager_destroy"
  [:pointer] :void)

(defcfn ticket-manager-set-keys
  "Replace all keys atomically. Thread-safe via copy-on-write.
   Returns 0 on success, -1 on error."
  "clj_ticket_manager_set_keys"
  [:pointer :pointer :long] :int)

(defcfn ticket-manager-key-count
  "Get current key count (for monitoring)."
  "clj_ticket_manager_key_count"
  [:pointer] :long)

(defcfn ticket-manager-set-quic-tag
  "Set the QUIC transport params hash for 0-RTT validation."
  "clj_ticket_manager_set_quic_tag"
  [:pointer :pointer] :void)

(defcfn ticket-manager-create-encrypt-ticket
  "Create encrypt_ticket callback wired to manager.
   is_quic: 1 for QUIC mode (appends transport params tag), 0 for TCP TLS."
  "clj_ticket_manager_create_encrypt_ticket"
  [:pointer :int] :pointer)

(defcfn ptls-ctx-set-tickets
  "Configure ptls context for session tickets.
   Sets encrypt_ticket callback, ticket lifetime, and max early data size."
  "clj_ptls_ctx_set_tickets"
  [:pointer :pointer :int :int] :void)

(defcfn ssl-ctx-set-tickets
  "Configure SSL_CTX for TCP TLS session tickets and 0-RTT.
   Wires the ticket manager into the OpenSSL ticket key callback."
  "clj_ssl_ctx_set_tickets"
  [:pointer :pointer :int] :void)
