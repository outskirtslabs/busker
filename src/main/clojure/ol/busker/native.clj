(ns ^:no-doc ol.busker.native
  "Defines FFM layouts and native calls for the Busker shim.

  This namespace maps C structures, callbacks, and libh2o operations. Key functions copy
  request data, create handlers, and manage multithread receivers.

  Allocation, socket I/O, H2O request operations and disposal run on platform
  threads. Fixed-slot claim, publish, abort and use-reference calls are bounded
  native atomic operations permitted on application virtual threads; they must
  not wait or invoke application code. They are not a general pin-free FFM API.

  Callback stubs and retained payloads must outlive every possible native use.
  Non-final streaming proceed ends one send; final payloads remain valid until
  request-pool cleanup. Receiver use references protect producers while filling
  slots, and receivers must be unregistered before their contexts are disposed.
  See [[ol.busker.fixed-final]] and [[ol.busker.response-queue]] for these contracts.

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
   [java.lang.foreign Arena MemorySegment ValueLayout]
   [java.nio.charset StandardCharsets]
   [java.util Arrays]))

(set! *warn-on-reflection* true)

(def ^:const H2O_SOCKET_FLAG_DONT_READ 0x20)

(def ^:const H2O_SEND_STATE_IN_PROGRESS 0)
(def ^:const H2O_SEND_STATE_FINAL 1)
(def ^:const H2O_SEND_STATE_ERROR 2)

(def ^:const CLJ_HANDLER_OK 0)
(def ^:const CLJ_HANDLER_DECLINED -1)
(def ^:const CLJ_HANDLER_OVERLOADED -2)
(def ^:const CLJ_HANDLER_SHUTTING_DOWN -3)
(def ^:const CLJ_HANDLER_ERROR -4)

(def ^:const CLJ_ENTRYPOINT_UNKNOWN
  "The entrypoint index the shim reports when the listener of a request's
  connection is not known."
  0xFFFFFFFF)

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
            [:is_early_data :int16]
            [:entrypoint :uint32]]])

(def ffi-clj-req-ctx-t
  [:struct [[:req :pointer]
            [:meta ffi-clj-req-meta-t]
            [:on-cleanup :pointer]
            [:on-request-body-chunk :pointer]
            [:response-receiver :pointer]
            [:response-hash-next :pointer]
            [:fixed-slot :pointer]
            [:fixed-claim :long]
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
            [:has_http1__req_io_timeout_while_generating :int]
            [:http1__req_io_timeout_while_generating :int]
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

(defcfn entrypoint-hosts-size
  "Gets the native size of `clj_entrypoint_hosts_t`."
  "clj_h2o_entrypoint_hosts_size"
  [] :long)

(defcfn entrypoint-hosts-init
  "Fills the `clj_entrypoint_hosts_t` at `record` for entrypoint index
  `entrypoint` and returns the hosts array for that entrypoint's accept
  contexts, or NULL."
  "clj_h2o_entrypoint_hosts_init"
  [:pointer :pointer :uint32] :pointer)

(defcfn config-register-host
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

(defcfn create-handler*
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
  "Unregisters a drained fixed-response receiver before its context is disposed.
  Requires a platform thread, closed admission and zero pending references."
  "clj_h2o_mt_destroy_response_receiver"
  [:pointer] :void)

(defcfn mt-response-pending
  "Returns fixed-slot and producer-use references that prevent receiver retirement."
  "clj_h2o_mt_response_pending"
  [:pointer] :long)

(defcfn mt-response-use-acquire
  "Retains a receiver during preparation. Call under receiver admission protection."
  "clj_h2o_response_use_acquire"
  [:pointer] :int)

(defcfn mt-response-use-release
  "Releases a receiver reference after publication or abort."
  "clj_h2o_response_use_release"
  [:pointer] :void)

(defcfn mt-response-ring-retained
  "Returns slots retained by native requests, excluding runnable ready slots."
  "clj_h2o_response_ring_retained"
  [:pointer] :long)

(defcfn mt-response-slot-data-size*
  "clj_h2o_response_slot_data_size"
  [] :long)

(defn mt-response-slot-data-size
  "Returns the fixed metadata size before slot payload bytes."
  []
  (long (mt-response-slot-data-size*)))

(defcfn mt-response-ring-capacity*
  "clj_h2o_response_ring_capacity"
  [:pointer] :long)

(defn mt-response-ring-capacity
  "Returns the fixed number of response slots."
  [receiver]
  (long (mt-response-ring-capacity* receiver)))

(defcfn mt-response-slot-payload-capacity*
  "clj_h2o_response_slot_payload_capacity"
  [:pointer] :long)

(defn mt-response-slot-payload-capacity
  "Returns the byte capacity after each slot metadata area."
  [receiver]
  (long (mt-response-slot-payload-capacity* receiver)))

(defcfn mt-response-claimed*
  "clj_h2o_response_ring_claimed"
  [:pointer] :long)

(defn mt-response-claimed
  "Returns the current number of claimed response slots."
  [receiver]
  (long (mt-response-claimed* receiver)))

(defcfn mt-response-ring-ready*
  "clj_h2o_response_ring_ready"
  [:pointer] :long)

(defn mt-response-ring-ready
  "Returns the current number of ready response slots."
  [receiver]
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
  (long (mt-response-abort* receiver claim-handle)))

(defcfn mt-response-ring-drain*
  "clj_h2o_response_ring_drain"
  [:pointer] :long)

(defn mt-response-ring-drain
  "Applies ready responses on the H2O event-loop thread and returns the count.
  Sending retains slot bodies until request cleanup; a return from this call
  does not make a slot reusable. Stale request identities release without sending."
  [receiver]
  (long (mt-response-ring-drain* receiver)))

(defcfn mt-destroy-wakeup-receiver
  "Unregister and free the wakeup receiver"
  "clj_h2o_mt_destroy_wakeup_receiver"
  [:pointer] :void)

(defcfn mt-wakeup
  "Send a wakeup message to this receiver's event loop"
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

(defcfn tls-bytes-dup
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
   - hosts-ptr: the hosts array of the listener's entrypoint, from
     [[entrypoint-hosts-init]]
   - ssl-ctx-ptr: pointer to SSL_CTX for TLS, or nil/NULL for plaintext

   Returns: pointer to h2o_accept_ctx_t"
  [arena ctx-ptr hosts-ptr ssl-ctx-ptr]
  {:pre [(some? arena)
         (some? ctx-ptr) (not (ffi/null? ctx-ptr))
         (some? hosts-ptr) (not (ffi/null? hosts-ptr))]}
  (let [ssl-ctx (if (and ssl-ctx-ptr (not (ffi/null? ssl-ctx-ptr)))
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

(defmacro ^:private ordered-array-map
  "Expands to a `PersistentArrayMap` of the key and value forms `kvs`, in the order
  given. The keys must be distinct. A map literal with more than eight entries
  builds a hash map; this keeps the smaller array form for maps that are read
  but rarely updated."
  [& kvs]
  (let [entries (gensym "entries")]
    `(let [~entries (object-array ~(count kvs))]
       ~@(map-indexed (fn [i form] `(aset ~entries ~i ~form)) kvs)
       (clojure.lang.PersistentArrayMap. ~entries))))

;; Byte offsets that copy-request-context reads. They follow ffi-clj-req-ctx-t,
;; ffi-clj-req-meta-t and ffi-clj-header-t; layout tests check them against the
;; shim. Text field i of the metadata (authority, method, path, remote address,
;; scheme) has its pointer at 8i and its length at 48 + 8i.
(def ^:no-doc ^:const req-ctx-req-offset 0)
(def ^:no-doc ^:const req-ctx-meta-offset 8)
(def ^:no-doc ^:const req-meta-text-offset 0)
(def ^:no-doc ^:const req-meta-headers-offset 40)
(def ^:no-doc ^:const req-meta-text-length-offset 48)
(def ^:no-doc ^:const req-meta-headers-length-offset 88)
(def ^:no-doc ^:const req-meta-http-version-offset 96)
(def ^:no-doc ^:const req-meta-has-body-offset 100)
(def ^:no-doc ^:const req-meta-early-data-offset 102)
(def ^:no-doc ^:const req-meta-entrypoint-offset 104)
(def ^:no-doc ^:const header-size 32)
(def ^:no-doc ^:const header-value-offset 16)
(def ^:no-doc ^:const header-length-offset 8)

;; One segment over the whole address space, so the request copy reads and
;; copies native memory by address.
;; It has no bounds and no lifetime check. Reading through it is valid only in
;; the request callback on the worker thread: H2O allocates the request, its
;; metadata strings and the header array in the request's memory pool, which
;; stays allocated while the callback runs and which only the worker's event
;; loop can free. After the callback returns, or from another thread, the same
;; addresses may already be freed or reused.
(def ^:private ^MemorySegment native-memory
  (.reinterpret MemorySegment/NULL Long/MAX_VALUE))

(defn- read-native-long
  ^long [^long address]
  (.get native-memory ValueLayout/JAVA_LONG_UNALIGNED address))

(defn- copied-field-pointer-address
  "Returns the address of the pointer to copied text field `i`: the five
  metadata strings, then the name and value of each header."
  ^long [^long meta ^long headers ^long i]
  (if (< i 5)
    (+ meta req-meta-text-offset (* 8 i))
    (let [j (- i 5)]
      (+ headers (* header-size (quot j 2)) (if (even? j) 0 header-value-offset)))))

(defn- copied-field-length
  "Returns the byte length of copied text field `i`, 0 for a null pointer."
  ^long [^long meta ^long headers ^long i]
  (let [pointer-address (copied-field-pointer-address meta headers i)]
    (if (zero? (read-native-long pointer-address))
      0
      (max 0 (read-native-long
              (+ pointer-address
                 (if (< i 5)
                   (- req-meta-text-length-offset req-meta-text-offset)
                   header-length-offset)))))))

(defn- bytes-of
  ^bytes [^String s]
  (.getBytes s StandardCharsets/US_ASCII))

(def ^:private ^"[Ljava.lang.Object;" method-keywords
  "Alternating method bytes and their Ring keyword, for the usual methods."
  (object-array (mapcat (fn [[method k]] [(bytes-of method) k])
                        [["GET" :get] ["get" :get] ["POST" :post] ["post" :post]
                         ["PUT" :put] ["put" :put] ["DELETE" :delete] ["delete" :delete]
                         ["HEAD" :head] ["head" :head] ["OPTIONS" :options]
                         ["options" :options] ["PATCH" :patch] ["patch" :patch]])))

(def ^:private ^bytes http-bytes (bytes-of "http"))
(def ^:private ^bytes https-bytes (bytes-of "https"))

(defn- region-equals?
  [^bytes bytes ^long start ^long length ^bytes expected]
  (and (= length (alength expected))
       (Arrays/equals bytes (int start) (int (+ start length)) expected 0 (int length))))

(defn- method-keyword-at
  "Returns the Ring `:request-method` keyword for the `length` method bytes at
  `start`: the lower-cased method as a keyword, or nil when there are none."
  [^bytes bytes ^long start ^long length]
  (when (pos? length)
    (loop [i 0]
      (if (< i (alength method-keywords))
        (if (region-equals? bytes start length (aget method-keywords i))
          (aget method-keywords (inc i))
          (recur (+ i 2)))
        (keyword (str/lower-case (String. bytes (int start) (int length) StandardCharsets/UTF_8)))))))

(defn- scheme-keyword-at
  "Returns the Ring `:scheme` keyword for the `length` scheme bytes at `start`,
  or `:http` when there are none."
  [^bytes bytes ^long start ^long length]
  (cond
    (zero? length) :http
    (region-equals? bytes start length http-bytes) :http
    (region-equals? bytes start length https-bytes) :https
    :else (keyword (String. bytes (int start) (int length) StandardCharsets/UTF_8))))

(defn ^:no-doc copy-request-context
  "Copies the request metadata and headers of the native request context at
  `ctx-ptr` into one byte array, on the worker thread inside the request
  callback. Returns the native request pointer, `:has-body`, the
  `:request-method` keyword (nil when the request has no method), the
  `:entrypoint` index of the listener that accepted the connection
  ([[CLJ_ENTRYPOINT_UNKNOWN]] when the shim does not know it), and the bytes
  and field end offsets that [[assemble-ring-request]] decodes. The copy does
  not refer to native memory except through `:req`."
  [^MemorySegment ctx-ptr]
  (let [base (.address ctx-ptr)
        meta (+ base req-ctx-meta-offset)
        headers (read-native-long (+ meta req-meta-headers-offset))
        header-count (if (zero? headers)
                       0
                       (read-native-long (+ meta req-meta-headers-length-offset)))
        n (+ 5 (* 2 header-count))
        ends (long-array n)
        total (loop [i 0
                     offset 0]
                (if (= i n)
                  offset
                  (let [end (+ offset (copied-field-length meta headers i))]
                    (aset ends i end)
                    (recur (inc i) end))))
        bytes (byte-array total)]
    (dotimes [i n]
      (let [start (if (zero? i) 0 (aget ends (dec i)))
            length (- (aget ends i) start)]
        (when (pos? length)
          (MemorySegment/copy native-memory ValueLayout/JAVA_BYTE
                              (read-native-long (copied-field-pointer-address meta headers i))
                              bytes (int start) (int length)))))
    {:req (MemorySegment/ofAddress (read-native-long (+ base req-ctx-req-offset)))
     :has-body (long (.get native-memory ValueLayout/JAVA_SHORT_UNALIGNED
                           (+ meta req-meta-has-body-offset)))
     :early-data (long (.get native-memory ValueLayout/JAVA_SHORT_UNALIGNED
                             (+ meta req-meta-early-data-offset)))
     :http-version (long (.get native-memory ValueLayout/JAVA_INT_UNALIGNED
                               (+ meta req-meta-http-version-offset)))
     :entrypoint (Integer/toUnsignedLong
                  (.get native-memory ValueLayout/JAVA_INT_UNALIGNED
                        (+ meta req-meta-entrypoint-offset)))
     :request-method (let [start (aget ends 0)]
                       (method-keyword-at bytes start (- (aget ends 1) start)))
     :bytes bytes
     :ends ends}))

(defn create-handler
  "Creates and configures an H2O handler with its request and cleanup callbacks.

  `hostconf-ptr` and `flat-config-ptr` are native configuration pointers; callback
  functions receive copied request data and request identity respectively; `arena`
  retains the native callback trampolines. Returns the handler pointer and references
  that keep both trampolines reachable."
  [hostconf-ptr on-req-callback on-cleanup-callback flat-config-ptr arena]
  {:pre [(some? hostconf-ptr) (not (ffi/null? hostconf-ptr))
         (fn? on-req-callback)
         (fn? on-cleanup-callback)
         (some? flat-config-ptr) (not (ffi/null? flat-config-ptr))
         (some? arena)]}
  (let [on-request-cb (fn on-request-cb [ctx-ptr]
                        (int (on-req-callback ctx-ptr (copy-request-context ctx-ptr))))
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

;; `base-keys` contains every key that the map from `request_` can have. A
;; lookup of any other key is answered from `overlay` and `removed` without
;; realizing `request_`; operations over the whole map realize it.
(deftype OverlayLazyRingRequest [^clojure.lang.Delay request_ base-keys overlay removed metadata]
  clojure.lang.IPersistentMap
  (assoc [_ key value]
    (OverlayLazyRingRequest. request_ base-keys (assoc overlay key value) (disj removed key)
                             metadata))
  (assocEx [this key value]
    (if (.containsKey ^clojure.lang.Associative this key)
      (throw (RuntimeException. "Key already present"))
      (.assoc ^clojure.lang.Associative this key value)))
  (without [_ key]
    (OverlayLazyRingRequest. request_ base-keys (dissoc overlay key) (conj removed key)
                             metadata))

  clojure.lang.Associative
  (containsKey [_ key]
    (and (not (contains? removed key))
         (or (contains? overlay key)
             (and (contains? base-keys key) (contains? @request_ key)))))
  (entryAt [this key]
    (when (.containsKey ^clojure.lang.Associative this key)
      (clojure.lang.MapEntry/create key (.valAt ^clojure.lang.ILookup this key))))

  clojure.lang.ILookup
  (valAt [_ key]
    (cond
      (contains? removed key) nil
      (contains? overlay key) (get overlay key)
      (contains? base-keys key) (get @request_ key)
      :else nil))
  (valAt [_ key not-found]
    (cond
      (contains? removed key) not-found
      (contains? overlay key) (get overlay key)
      (contains? base-keys key) (get @request_ key not-found)
      :else not-found))

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
    (OverlayLazyRingRequest. request_ base-keys overlay removed new-metadata))

  Object
  (equals [_ other] (.equals ^Object (force-overlay-request request_ overlay removed) other))
  (hashCode [_] (.hashCode ^Object (force-overlay-request request_ overlay removed)))
  (toString [_] (.toString ^Object (force-overlay-request request_ overlay removed))))

(defn ^:no-doc overlay-lazy-ring-request
  "Returns a Ring request map that realizes `(request-fn)` on first use. Lookups
  of keys outside the set `base-keys` read only `overlay` and later changes, so
  they do not call `request-fn`."
  [request-fn base-keys overlay]
  (OverlayLazyRingRequest. (delay (request-fn)) base-keys overlay #{} nil))

(def ^:no-doc assembled-request-keys
  "The keys of every map that [[assemble-ring-request]] returns."
  #{:protocol :remote-addr :headers :server-port :ol.busker/early-data? :uri
    :server-name :query-string :body :scheme :request-method})

(defn- copied-text
  "Decodes copied text field `i` as UTF-8, or returns nil when it is empty."
  [^bytes bytes ^longs ends ^long i]
  (let [start (if (zero? i) 0 (aget ends (dec i)))
        length (- (aget ends i) start)]
    (when (pos? length)
      (String. bytes (int start) (int length) StandardCharsets/UTF_8))))

(defn- copied-headers
  "Returns the Ring headers map of the copied header fields, or nil when the
  request has none. Names are lower-cased; an empty value is the empty string;
  repeated headers are joined with `,`, or with `;` for `cookie`, keeping empty
  occurrences."
  [^bytes bytes ^longs ends]
  (let [n (alength ends)]
    (when (> n 5)
      (loop [i 5
             result {}]
        (if (< i n)
          (let [name (str/lower-case (copied-text bytes ends i))
                value (or (copied-text bytes ends (inc i)) "")]
            (recur (+ i 2)
                   (if (contains? result name)
                     (update result name str (if (= "cookie" name) ";" ",") value)
                     (assoc result name value))))
          result)))))

(defn ^:no-doc assemble-ring-request
  "Decodes the request copy `req-ctx` from [[copy-request-context]] into the
  Ring request map. `input-stream` becomes `:body` when the request has one."
  [req-ctx ^InputStream input-stream]
  (let [^bytes bytes (:bytes req-ctx)
        ^longs ends (:ends req-ctx)
        scheme-start (aget ends 3)
        scheme (scheme-keyword-at bytes scheme-start (- (aget ends 4) scheme-start))
        {:keys [server-name server-port]}
        (util/parse-authority (copied-text bytes ends 0) (if (identical? :https scheme) 443 80))
        ^String path (copied-text bytes ends 2)
        query-index (if path (.indexOf path "?") -1)
        protocol (case (int (:http-version req-ctx))
                   0x0101 "HTTP/1.1"
                   0x0200 "HTTP/2.0"
                   0x0300 "HTTP/3.0"
                   "HTTP/1.1")]
    ;; The order matches the order in which a hash map of these keys lists them.
    (ordered-array-map
     :protocol protocol
     :remote-addr (or (copied-text bytes ends 3) "")
     :headers (copied-headers bytes ends)
     :server-port server-port
     ;; `if` yields a boxed Boolean, which `aset` on an Object array accepts
     ;; without reflection.
     :ol.busker/early-data? (if (= 1 (:early-data req-ctx)) true false)
     :uri (if (neg? query-index) path (subs path 0 query-index))
     :server-name server-name
     :query-string (when-not (neg? query-index) (subs path (inc query-index)))
     :body (when (= 1 (:has-body req-ctx))
             input-stream)
     :scheme scheme
     :request-method (or (:request-method req-ctx) :get))))

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
   The returned handle retains the pooled transport metadata and any reservation
   socket until [[http3-release-udp-transport]] is called. `port` is 0 to 65535;
   the shim takes it as `uint16_t`."
  "clj_h2o_http3_open_udp_transport"
  [:string :uint16] :pointer)

(defcfn http3-attach-udp-transport
  "Attach an HTTP/3 worker context to a pooled UDP transport.
   The worker attachment starts non-accepting until the transport activates
   the generation identified by `node-id`. Address tokens use keys derived
   from the session ticket keys of `ticket-mgr`, which may be null."
  "clj_h2o_http3_attach_udp_transport"
  [:pointer :pointer :pointer :pointer
   :pointer :pointer :long :int] :pointer)

(defcfn http3-activate-udp-transport-generation
  "Mark `node-id` as the active acceptor on a pooled UDP transport.
   Existing connections for older generations continue to route by CID,
   while new Initial packets are forwarded to the active generation."
  "clj_h2o_http3_activate_udp_transport_generation"
  [:pointer :long] :void)

(defcfn http3-detach-udp-transport
  "Detach an HTTP/3 worker context from a pooled UDP transport.
   This closes only the worker's duplicated file descriptor."
  "clj_h2o_http3_detach_udp_transport"
  [:pointer] :void)

(defcfn http3-release-udp-transport
  "Release a pooled UDP transport and close its reservation socket, if present."
  "clj_h2o_http3_release_udp_transport"
  [:pointer] :void)

(defcfn http3-stop-accepting
  "Stop accepting new HTTP/3 connections by setting acceptor to NULL.
   Existing connections (including those in handshake) continue to completion.
   Call this before requesting shutdown to prevent new connection attempts."
  "clj_h2o_http3_stop_accepting"
  [:pointer] :void)

(defcfn http3-withdraw-worker
  "Stop sending new HTTP/3 connections from other workers to this worker.
   Safe to call from any thread."
  "clj_h2o_http3_withdraw_worker"
  [:pointer] :void)

(defcfn http3-num-connections
  "Get the number of active HTTP/3 connections on this context."
  "clj_h2o_http3_num_connections"
  [:pointer] :long)

(defcfn http3-forwarded-datagrams
  "Datagrams worker `thread-id` of generation `node-id` has forwarded to other
   workers over the UDP transport. For tests and diagnostics; safe from any
   thread until the transport is released, also after the worker stopped."
  "clj_h2o_http3_forwarded_datagrams"
  [:pointer :long :int] :long)

(defcfn http3-connected-sockets
  "UDP sockets worker `thread-id` of generation `node-id` holds connected to
   clients whose datagrams the kernel delivers to another worker. For tests
   and diagnostics; safe from any thread until the transport is released,
   also after the worker stopped."
  "clj_h2o_http3_connected_sockets"
  [:pointer :long :int] :int)

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
