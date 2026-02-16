(ns ol.busker.server
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.byte-bounded-queue :as bbq]
   [ol.busker.config :as config]
   [ol.busker.evloop :as evloop]
   [ol.busker.internal.protocols :as p]
   [ol.busker.native :as h2o]
   [ol.busker.native.socket :as socket]
   [ol.busker.request :as request]
   [ol.busker.response-queue :as response-queue]
   [ol.busker.tickets :as tickets])
  (:import
   [java.util.concurrent ExecutorService TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong AtomicReference]))

(set! *warn-on-reflection* true)

(defn- response-state-pending?
  [st]
  (let [scheduled? (.get ^AtomicBoolean (:scheduled?_ st))
        in-flight? (some? (.get ^AtomicReference (:in-flight_ st)))
        queued-bytes (bbq/queued-bytes (:bbq st))]
    (or scheduled?
        in-flight?
        (pos? queued-bytes))))

(defn- request-awaiting-final?
  [req]
  (when-let [latency (:latency/state req)]
    (let [^AtomicLong handler-start (:handler-start latency)
          ^AtomicLong response-final (:response-final latency)]
      (and handler-start response-final
           (pos? (.get handler-start))
           (zero? (.get response-final))))))

(defn- pending-response-work?
  "Return true if any live request on this worker still needs JVM-driven response work."
  [worker]
  (let [^java.util.HashMap requests (:requests worker)]
    (boolean
     (some
      (fn [req]
        (or (when-let [write-resp (:write-resp req)]
              (when-let [st (::response-queue/state write-resp)]
                (response-state-pending? st)))
            (request-awaiting-final? req)))
      (.values requests)))))

(defn evloop-msg-processor
  "Called by drain-mailbox! inside each worker thread for each message on the evloop"
  [op args]
  (case op
    :h2o/proceed-request
    (let [[req-ctx] args]
      (h2o/proceed-req (:req req-ctx)))

    :h2o/sendvec
    (let [[send-vecs] args]
      (send-vecs))

    :h2o/send-informational
    (let [[send-fn] args]
      (send-fn))

    :h2o/start-response
    (let [[start-fn] args]
      (start-fn))
    nil))

(defn create-connection-close-callback
  "Create callback for socket close events to release global connection limit slot.
   The callback signature is: void on_close(void *data)"
  []
  (let [cb (fn [_data-ptr]
             (h2o/conn-limit-release))]
    {::connection-close-cb cb
     ::connection-close-cb-ptr (mem/serialize cb [::ffi/fn [::mem/pointer] ::mem/void])}))

(defn create-accept-callback
  "Create accept callback for a listener socket with global connection limit enforcement.
   h2o_accept handles both plaintext and TLS connections automatically:
   - For plaintext: directly starts HTTP/1.1
   - For TLS: initiates SSL handshake, negotiates protocol (HTTP/1.1 or HTTP/2 via ALPN)

   Parameters:
   - accept-ctx-ptr: pointer to h2o_accept_ctx_t (contains ssl_ctx if TLS)
   - on-close-callback: callback function pointer for socket close"
  [accept-ctx-ptr on-close-callback]
  (let [accept-cb (fn [listener-ptr err-ptr]
                    (when-not (mem/null? err-ptr)
                      nil)
                    (let [sock-ptr (h2o/evloop-socket-accept listener-ptr)]
                      (when-not (mem/null? sock-ptr)
                        #_{:clj-kondo/ignore [:type-mismatch]}
                        (if (pos? (h2o/conn-limit-try-acquire))
                          (do
                            (h2o/socket-set-on-close sock-ptr on-close-callback (mem/as-segment 0))
                            (h2o/h2o-accept accept-ctx-ptr sock-ptr))
                          (h2o/socket-close sock-ptr)))))]
    {::accept-cb accept-cb
     ::accept-cb-ptr (mem/serialize accept-cb [::ffi/fn [::mem/pointer ::mem/c-string] ::mem/void])}))

(defn config->flat-globalconf-t
  "Convert public config map to flat globalconf struct format.
   Arena keeps string fields alive during native call (C function will h2o_strdup them)."
  [config]
  (let [has? (fn [k] (if (contains? config k) 1 0))
        num-val (fn [k] (get config k 0))
        bool->int (fn [k] (if (get config k) 1 0))]

    {:has_server_name (has? :server-name)
     :server_name (get config :server-name "")

     :has_proxy_status_identity (has? :proxy-status-identity)
     :proxy_status_identity (get config :proxy-status-identity "")

     :has_max_request_entity_size (has? :max-request-entity-size)
     :max_request_entity_size (num-val :max-request-entity-size)

     :has_max_delegations (has? :max-delegations)
     :max_delegations (num-val :max-delegations)

     :has_max_reprocesses (has? :max-reprocesses)
     :max_reprocesses (num-val :max-reprocesses)

     :has_handshake_timeout (has? :handshake-timeout)
     :handshake_timeout (num-val :handshake-timeout)

     :has_max_spare_pipes (has? :max-spare-pipes)
     :max_spare_pipes (num-val :max-spare-pipes)

     :has_http1__req_timeout (has? :http1-req-timeout)
     :http1__req_timeout (num-val :http1-req-timeout)

     :has_http1__req_io_timeout (has? :http1-req-io-timeout)
     :http1__req_io_timeout (num-val :http1-req-io-timeout)

     :has_http1__upgrade_to_http2 (has? :http1-upgrade?)
     :http1__upgrade_to_http2 (bool->int :http1-upgrade?)

     :has_http2__idle_timeout (has? :http2-idle-timeout)
     :http2__idle_timeout (num-val :http2-idle-timeout)

     :has_http2__graceful_shutdown_timeout (has? :http2-graceful-shutdown-timeout)
     :http2__graceful_shutdown_timeout (num-val :http2-graceful-shutdown-timeout)

     :has_http2__max_streams (has? :http2-max-streams)
     :http2__max_streams (num-val :http2-max-streams)

     :has_http2__max_concurrent_requests_per_connection (has? :http2-max-requests)
     :http2__max_concurrent_requests_per_connection (num-val :http2-max-requests)

     :has_http2__max_concurrent_streaming_requests_per_connection (has? :http2-max-streaming-requests)
     :http2__max_concurrent_streaming_requests_per_connection (num-val :http2-max-streaming-requests)

     :has_http2__max_streams_for_priority (has? :http2-max-priority-streams)
     :http2__max_streams_for_priority (num-val :http2-max-priority-streams)

     :has_http2__active_stream_window_size (has? :http2-stream-window-size)
     :http2__active_stream_window_size (num-val :http2-stream-window-size)

     :has_http2__dos_delay (has? :http2-dos-delay)
     :http2__dos_delay (num-val :http2-dos-delay)

     :has_http3__idle_timeout (has? :http3-idle-timeout)
     :http3__idle_timeout (num-val :http3-idle-timeout)

     :has_http3__graceful_shutdown_timeout (has? :http3-graceful-shutdown-timeout)
     :http3__graceful_shutdown_timeout (num-val :http3-graceful-shutdown-timeout)

     :has_http3__active_stream_window_size (has? :http3-stream-window-size)
     :http3__active_stream_window_size (num-val :http3-stream-window-size)

     :has_http3__ack_frequency (has? :http3-ack-frequency)
     :http3__ack_frequency (num-val :http3-ack-frequency)

     :has_compress_args (if (:compress? config false) 1 0)
     :compress_args_mine_size (num-val :compress-min-size)
     :compress_args_gzip_quality (num-val :compress-gzip-level)
     :compress_args_brotli_quality (num-val :compress-brotli-level)
     :compress_args_zstd_quality (num-val :compress-zstd-level)}))

(defn create-server-config
  "Create and initialize h2o global configuration with a default host and Ring handler.
   Uses provided arena for server lifetime resources.
   Returns map with ::config-ptr, ::hostconf-ptr, ::handler-ptr"
  [arena ring-handler config]
  (let [;; allocate and configure h2o_globalconf_t
        config-ptr (mem/alloc (h2o/globalconf-size) arena)
        flat-config-ptr (mem/serialize (config->flat-globalconf-t config) ::h2o/clj-h2o-flat-globalconf-t arena)
        config-ptr (do
                     (h2o/create-global-conf config-ptr flat-config-ptr)
                     config-ptr)
        ;; we need at least one h2o_hostconf_t, prepare that here
        hostconf-ptr (with-open [arena2 (mem/confined-arena)]
                       (h2o/config-register-host config-ptr (h2o/str->iovec "default" arena2) 65535))
        on-request-cb (partial request/on-request (:executor config)
                               config
                               ring-handler)
        on-request-cleanup-cb (partial request/on-request-cleanup ring-handler)
        handler (h2o/create-handler hostconf-ptr on-request-cb on-request-cleanup-cb flat-config-ptr arena)]
    ;; all of these things may not be used again, but they must not be GCed
    ;; until the server itself is reaped
    {::config-ptr config-ptr
     ::hostconf-ptr hostconf-ptr
     ::on-request-cb on-request-cb
     ::on-request-cleanup-cb on-request-cleanup-cb
     ::handler handler}))

(defn update-listener-state!
  "Throttle TCP listeners by starting/stopping accept callbacks based on global connection count.
   Prevents accepting new connections when at/over max-connections limit.

   Uses the global connection counter shared with HTTP/3 accept path.
   QUIC accept is gated in the shim; TCP accept is gated here."
  [listener-socks accept-callbacks max-connections]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (let [current-conns (h2o/conn-limit-current)
        should-accept? (or (zero? max-connections)
                           (< current-conns max-connections))]
    (doseq [listener-idx (range (count listener-socks))]
      (let [sock-ptr (nth listener-socks listener-idx)
            accept-callback (nth accept-callbacks listener-idx)]
        (when-not (mem/null? sock-ptr)
          (if should-accept?
            ;; Below limit: ensure TCP listeners are accepting
            (when (zero? (h2o/socket-reading? sock-ptr))
              (h2o/socket-read-start sock-ptr accept-callback))
            ;; At/over limit: stop accepting new connections
            (when-not (zero? (h2o/socket-reading? sock-ptr))
              (h2o/socket-read-stop sock-ptr))))))))

(defn- initiate-worker-shutdown!
  "Phase 1-4 of graceful shutdown following h2o main.c pattern:
   stop accepting TCP/HTTP3, close listeners, request context shutdown."
  [{:keys [listener-socks loop-ptr ctx-ptr http3-ctxs]}]
  ;; Phase 1: Stop accepting new TCP connections
  (doseq [listener-idx (range (count listener-socks))]
    (let [sock-ptr (nth listener-socks listener-idx)]
      (when (and (not (mem/null? sock-ptr))
                 #_{:clj-kondo/ignore [:type-mismatch]}
                 (not (zero? (h2o/socket-reading? sock-ptr))))
        (h2o/socket-read-stop sock-ptr))))

  ;; Phase 2: Stop accepting new HTTP/3 connections (set acceptor = NULL)
  ;; Stops new QUIC Initial packets while allowing
  ;; existing handshakes to complete. Unlike TCP (socket-read-stop), UDP socket
  ;; remains open but new connection attempts receive Version Negotiation rejection.
  (doseq [http3-ctx http3-ctxs]
    (when (and http3-ctx (not (mem/null? http3-ctx)))
      (h2o/http3-stop-accepting http3-ctx)))

  ;; then process stop events immediately
  (h2o/evloop-run loop-ptr 0)

  ;; Phase 3: Close listener sockets
  (doseq [listener-idx (range (count listener-socks))]
    (let [sock-ptr (nth listener-socks listener-idx)]
      (when-not (mem/null? sock-ptr)
        (h2o/socket-close sock-ptr))))

  ;; Phase 4: Request graceful shutdown (sends GOAWAY to HTTP/2 clients)
  (h2o/context-request-shutdown ctx-ptr))

(defn- has-active-http3-connections?
  "Returns true if the HTTP/3 context has active connections."
  [http3-ctx]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (and http3-ctx
       (not (mem/null? http3-ctx))
       (pos? (h2o/http3-num-connections http3-ctx))))

(defn- all-connections-drained?
  "Check if all connections for this context are closed, including HTTP/3"
  [ctx-ptr http3-ctxs]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (and (zero? (h2o/context-get-active-conns ctx-ptr))
       (zero? (h2o/context-get-shutdown-conns ctx-ptr))
       (not-any? has-active-http3-connections? http3-ctxs)))

(defn- ready-to-dispose?
  "Returns true if all preconditions for context disposal are met."
  [{:keys [context-disposed? shutdown-initiated? receiver-destroyed?]} {:keys [ctx-ptr http3-ctxs]}]
  (and (not context-disposed?)
       shutdown-initiated?
       receiver-destroyed?
       (all-connections-drained? ctx-ptr http3-ctxs)))

(defn- check-and-initiate-shutdown!
  "Advances shutdown initiation once the main thread has signalled it.
   Returns updated loop-state (may flip :shutdown-initiated? to true)."
  [loop-state {:keys [shutting-down?] :as state}]
  (let [should-shutdown? (.get ^AtomicBoolean shutting-down?)
        already?         (true? (:shutdown-initiated? loop-state))]
    (when (and should-shutdown? (not already?))
      ;; initiate-worker-shutdown! likely needs listener/callback info -> keep using `state`
      (initiate-worker-shutdown! (assoc state :shutdown-initiated? true)))
    (assoc loop-state :shutdown-initiated? (or already? should-shutdown?))))

(defn- update-receiver-destruction
  "Returns updated loop-state (may flip :receiver-destroyed? to true once detected)."
  [loop-state worker]
  (cond
    (:receiver-destroyed? loop-state)
    loop-state

    (nil? (.get ^AtomicReference (:wakeup-receiver_ worker)))
    (assoc loop-state :receiver-destroyed? true)

    :else
    loop-state))

(defn- dispose-context-if-ready
  "If preconditions are met, disposes ctx + http3 ctxs and queues ::stop.
   Returns updated loop-state (may flip :context-disposed? to true)."
  [loop-state worker {:keys [ctx-ptr http3-ctxs] :as state}]
  (if (ready-to-dispose? loop-state state)
    (do
      (doseq [http3-ctx http3-ctxs]
        (when (and http3-ctx (not (mem/null? http3-ctx)))
          (h2o/http3-free-worker-ctx http3-ctx)))
      (h2o/context-dispose ctx-ptr)
      (p/send-msg worker evloop/stop-msg)
      (assoc loop-state :context-disposed? true))
    loop-state))

(defn worker-loop
  [worker loop-state
   {:keys [loop-ptr ctx-ptr listener-socks accept-callbacks max-connections] :as state}]
  (let [loop-state (-> loop-state
                       (check-and-initiate-shutdown! state)
                       (update-receiver-destruction worker)
                       (dispose-context-if-ready worker state))
        disposed?  (true? (:context-disposed? loop-state))
        shutting?  (true? (:shutdown-initiated? loop-state))]
    (if disposed?
      (h2o/evloop-run loop-ptr 0)
      (let [now       (h2o/evloop-now loop-ptr)
            base-wait (h2o/cleanup-thread now ctx-ptr)
            wait-ms   (cond
                        (pending-response-work? worker) 5
                        shutting?                       10
                        :else                           base-wait)]
        (when-not shutting?
          (update-listener-state! listener-socks accept-callbacks max-connections))
        (h2o/evloop-run loop-ptr (if (pos? (p/count-msgs worker)) 0 wait-ms))))
    loop-state))

(defn http3-enabled?
  "Returns true if HTTP/3 should be enabled for this listener.
   HTTP/3 is enabled by default for TLS listeners unless explicitly disabled
   with :http3? false in the TLS config."
  [listener]
  (boolean
   (and (:tls listener)
        (get-in listener [:tls :http3?] true))))

(defn- create-ssl-context
  "Create SSL_CTX for a TLS listener, or nil for non-TLS listeners."
  [listener]
  (when-let [{:keys [cert-file key-file protocols]} (:tls listener)]
    (let [enable-http2? (or (nil? protocols)
                            (contains? (set protocols) :http2))
          ssl-ctx (h2o/create-ssl-ctx cert-file key-file (if enable-http2? 1 0))]
      (when (mem/null? ssl-ctx)
        (throw (ex-info "Failed to create SSL_CTX (check OpenSSL errors in stderr)"
                        {:listener (dissoc listener :tls)
                         :listener-tls (:tls listener)
                         :cert-file cert-file
                         :key-file key-file})))
      ssl-ctx)))

(defn- create-http3-contexts
  "Create shared HTTP/3 contexts (ptls + quicly) for each TLS listener with HTTP/3 enabled.
   These contexts are shared across all workers.
   Returns map: listener-index -> {:ptls-ctx ptls-ctx-ptr :quicly-ctx quicly-ctx-ptr :encrypt-ticket encrypt-ticket-ptr}
   Throws if context creation fails."
  [listeners config-ptr ticket-mgr-ptr ticket-lifetime-seconds]
  (into {}
        (keep-indexed
         (fn [idx listener]
           (when (http3-enabled? listener)
             (let [{:keys [cert-file key-file]} (:tls listener)
                   ptls-ctx (h2o/http3-create-ptls-ctx cert-file key-file)]
               (when (or (nil? ptls-ctx) (mem/null? ptls-ctx))
                 (throw (ex-info "Failed to create ptls context for HTTP/3"
                                 {:listener-index idx
                                  :cert-file cert-file
                                  :key-file key-file})))
               ;; Wire ticket manager into ptls context for session tickets
               (let [encrypt-ticket (when (and ticket-mgr-ptr (not (mem/null? ticket-mgr-ptr)))
                                      (h2o/ticket-manager-create-encrypt-ticket ticket-mgr-ptr 1))
                     _ (when encrypt-ticket
                         (h2o/ptls-ctx-set-tickets ptls-ctx encrypt-ticket
                                                   ticket-lifetime-seconds
                                                   8192))  ; CLJ_MAX_EARLY_DATA_SIZE
                     quicly-ctx (h2o/http3-create-quicly-ctx ptls-ctx config-ptr)]
                 (when (or (nil? quicly-ctx) (mem/null? quicly-ctx))
                   (h2o/http3-free-ptls-ctx ptls-ctx)
                   (throw (ex-info "Failed to create quicly context for HTTP/3"
                                   {:listener-index idx})))
                 ;; Set QUIC tag for 0-RTT validation after quicly context is created
                 (when (and ticket-mgr-ptr (not (mem/null? ticket-mgr-ptr)))
                   (h2o/ticket-manager-set-quic-tag ticket-mgr-ptr quicly-ctx))
                 [idx {:ptls-ctx ptls-ctx
                       :quicly-ctx quicly-ctx
                       :encrypt-ticket encrypt-ticket}]))))
         listeners)))

(defn- create-http3-worker-contexts
  "Create per-worker HTTP/3 contexts (UDP listeners) for each TLS listener with HTTP/3 enabled.

   Returns vector of vectors: [worker-idx][listener-idx] -> http3-worker-ctx-ptr or nil

   Uses nested vector structure (not map) because each worker needs its own UDP socket
   for each HTTP/3 listener. Vector provides O(1) indexed access during worker-loop iteration."
  [n-workers listeners loops contexts config-ptr http3-contexts]
  (let [hosts-ptr (h2o/globalconf-get-hosts config-ptr)]
    (vec (for [thread-idx (range n-workers)]
           (vec (for [listener-idx (range (count listeners))]
                  (let [listener (nth listeners listener-idx)]
                    (when (http3-enabled? listener)
                      (let [{:keys [quicly-ctx]} (get http3-contexts listener-idx)
                            {:keys [host port]} listener
                            bind-host (or host "0.0.0.0")
                            loop-ptr (nth loops thread-idx)
                            ctx-ptr (nth contexts thread-idx)
                            http3-ctx (h2o/http3-create-worker-ctx
                                       ctx-ptr
                                       loop-ptr
                                       quicly-ctx
                                       hosts-ptr
                                       bind-host
                                       (short port)
                                       (int thread-idx))]
                        (when (or (nil? http3-ctx) (mem/null? http3-ctx))
                          (throw (ex-info "Failed to create HTTP/3 worker context"
                                          {:listener-index listener-idx
                                           :thread-idx thread-idx
                                           :port port})))
                        http3-ctx)))))))))

(defn- free-http3-contexts
  "Free shared HTTP/3 contexts (quicly + ptls)."
  [http3-contexts]
  (doseq [[_idx {:keys [quicly-ctx ptls-ctx]}] http3-contexts]
    (when quicly-ctx
      (h2o/http3-free-quicly-ctx quicly-ctx))
    (when ptls-ctx
      (h2o/http3-free-ptls-ctx ptls-ctx))))

(defn- prepare-server-state
  [ring-handler user-config]
  (let [{:keys [n-workers max-connections executor] :as config}
        (config/load! user-config)]
    {::ring-handler    ring-handler
     ::config          config
     ::n-workers       n-workers
     ::max-connections max-connections
     ::executor        executor
     ::message-handler evloop-msg-processor
     ::shutting-down?  (AtomicBoolean. false)}))

(defn- init-core-state
  [{::keys [ring-handler config n-workers max-connections] :as state}]
  (let [arena             (mem/shared-arena)
        _                 (h2o/conn-limit-set-max max-connections)
        server-config     (create-server-config arena ring-handler config)
        config-ptr        (::config-ptr server-config)
        loops             (h2o/create-loops n-workers)
        contexts          (h2o/create-contexts arena loops config-ptr)
        wakeup-receivers  (vec (for [ctx-ptr contexts]
                                 (h2o/mt-create-wakeup-receiver ctx-ptr)))
        on-close-callback (create-connection-close-callback)]
    (assoc state
           ::arena arena
           ::server-config server-config
           ::config-ptr config-ptr
           ::handler (::handler server-config)
           ::loops loops
           ::contexts contexts
           ::wakeup-receivers wakeup-receivers
           ::on-close-callback on-close-callback)))

(defn- init-tls-http3-state
  [{::keys [listener-runtimes config config-ptr n-workers loops contexts]
    :as state}]
  (let [listeners                       (mapv :listener listener-runtimes)
        ssl-ctx-ptrs                    (mapv :ssl-ctx-ptr listener-runtimes)
        has-tls-listeners?              (some :tls listeners)
        session-ticket-lifetime-seconds (:session-ticket-lifetime-seconds config)
        ticket-store                    (tickets/memory-ticket-store)
        native-ticket-mgr               (when has-tls-listeners?
                                          (h2o/ticket-manager-create
                                           session-ticket-lifetime-seconds))
        key-manager                     (when native-ticket-mgr
                                          (-> (tickets/create-key-manager ticket-store
                                                                          native-ticket-mgr
                                                                          config)
                                              (tickets/start-key-manager!)))]
    (when native-ticket-mgr
      (doseq [ssl-ctx-ptr ssl-ctx-ptrs]
        (when ssl-ctx-ptr
          (h2o/ssl-ctx-set-tickets ssl-ctx-ptr native-ticket-mgr 8192))))
    (let [http3-contexts        (create-http3-contexts listeners
                                                       config-ptr
                                                       native-ticket-mgr
                                                       session-ticket-lifetime-seconds)
          http3-worker-contexts (create-http3-worker-contexts n-workers
                                                              listeners
                                                              loops
                                                              contexts
                                                              config-ptr
                                                              http3-contexts)]
      (assoc state
             ::ticket-store ticket-store
             ::native-ticket-mgr native-ticket-mgr
             ::key-manager key-manager
             ::http3-contexts http3-contexts
             ::http3-worker-contexts http3-worker-contexts))))

(defn- init-single-listener-state
  [listener n-workers loops contexts arena config-ptr on-close-callback]
  (let [ssl-ctx-ptr   (create-ssl-context listener)
        listener-fd   (if-let [unix-path (:unix listener)]
                        (throw (ex-info "Unix listeners are not yet supported by server startup."
                                        {:listener listener
                                         :unix-path unix-path}))
                        (socket/open-master-listener
                         (cond-> {:port (:port listener)}
                           (:host listener) (assoc :host (:host listener)))))
        dup-fds       (socket/dup-for-threads listener-fd n-workers)
        thread-states (vec
                       (for [thread-idx (range n-workers)]
                         (let [accept-ctx
                               (h2o/create-accept-ctx arena
                                                      (nth contexts thread-idx)
                                                      config-ptr
                                                      ssl-ctx-ptr)
                               accept-callback
                               (create-accept-callback
                                accept-ctx
                                (::connection-close-cb-ptr on-close-callback))
                               accept-callback-ptr (::accept-cb-ptr accept-callback)
                               sock-ptr            (h2o/create-socket-for-loop
                                                    (nth loops thread-idx)
                                                    (nth dup-fds thread-idx)
                                                    h2o/H2O_SOCKET_FLAG_DONT_READ)]
                           (h2o/socket-read-start sock-ptr accept-callback-ptr)
                           {:accept-ctx          accept-ctx
                            :accept-callback     accept-callback
                            :accept-callback-ptr accept-callback-ptr
                            :socket              sock-ptr})))]
    {:listener      listener
     :ssl-ctx-ptr   ssl-ctx-ptr
     :listener-fd   listener-fd
     :dup-fds       dup-fds
     :thread-states thread-states}))

(defn- init-listener-state
  [{::keys [n-workers loops contexts arena config-ptr on-close-callback]
    :as    state}]
  (-> state
      ::config
      :entrypoints
      (->> (into [] (mapcat config/entrypoint->listeners))
           (mapv #(init-single-listener-state %
                                              n-workers
                                              loops
                                              contexts
                                              arena
                                              config-ptr
                                              on-close-callback)))
      (->> (assoc state ::listener-runtimes))))

(defn- init-worker-state
  [{::keys [n-workers loops contexts listener-runtimes http3-worker-contexts
            max-connections shutting-down?
            message-handler wakeup-receivers]
    :as    state}]
  (let [workers
        (vec
         (for [thread-idx (range n-workers)]
           (let [loop-ptr                  (nth loops thread-idx)
                 ctx-ptr                   (nth contexts thread-idx)
                 thread-listener-states
                 (mapv #(nth (:thread-states %) thread-idx) listener-runtimes)
                 listener-socks-for-thread (mapv :socket thread-listener-states)
                 accept-callbacks-for-thread
                 (mapv :accept-callback-ptr thread-listener-states)
                 http3-ctxs-for-thread     (nth http3-worker-contexts thread-idx)]
             (evloop/start-worker!
              (fn [worker loop-state]
                (worker-loop worker
                             loop-state
                             {:listener-socks   listener-socks-for-thread
                              :accept-callbacks accept-callbacks-for-thread
                              :loop-ptr         loop-ptr
                              :ctx-ptr          ctx-ptr
                              :http3-ctxs       http3-ctxs-for-thread
                              :max-connections  max-connections
                              :shutting-down?   shutting-down?}))
              message-handler
              (nth wakeup-receivers thread-idx)))))]
    (assoc state ::workers workers)))

(defn- finalize-server-state
  [state]
  (dissoc state ::message-handler))

(defn run-server
  ([handler]
   (run-server handler {}))
  ([handler user-config]
   (when-not handler (throw (ex-info "Handler is required" {:handler handler})))
   (-> (prepare-server-state handler user-config)
       (init-core-state)
       (init-listener-state)
       (init-tls-http3-state)
       (init-worker-state)
       (finalize-server-state))))

(defn stop-server
  "Synchronously shut down the server, blocking until all requests and native
   loops have stopped and associated resources are released."
  ([server]
   (stop-server server 60 TimeUnit/SECONDS))
  ([{::keys [^ExecutorService executor] :as server} ^long timeout ^TimeUnit timeunit]
   (assert server)
   ;; Implementation notes:
   ;; - Handler callbacks are arena-backed, so we mark the handler as shutting
   ;;   down before signalling workers; they refuse new work immediately.
   ;; - Workers dispose their own contexts once connections drain. We destroy
   ;;   wakeup receivers on the main thread before joining worker threads so the
   ;;   libh2o multithread queues can be torn down cleanly.
   ;; - Only after workers exit do we destroy loops, close sockets, release
   ;;   configs, and finally close the shared arena, guaranteeing no native
   ;;   upcalls occur after this function returns.
   (when-let [handler-ptr (some-> server ::handler ::h2o/handler-ptr)]
     (h2o/handler-set-shutting-down handler-ptr 1))
   ;; Signal shutdown to all workers
   (.set ^AtomicBoolean (::shutting-down? server) true)

   ;; Stop key manager rotation thread early
   (when-let [key-mgr (::key-manager server)]
     (tickets/stop-key-manager! key-mgr))

   ;; Wake up all workers so they see the shutdown signal
   (evloop/broadcast-wake! (::workers server))

   ;; Stop new requests from being executed
   (.shutdown executor)

   ;; Wait for all virtual threads to finish
   (when-not (.awaitTermination executor timeout timeunit)
     (.shutdownNow executor)
     (when-not (.awaitTermination executor timeout timeunit)
       (println "Virtual thread request executor pool did not shutdown cleanly")))

   (evloop/broadcast-wake! (::workers server))

   ;; Destroy wakeup receivers so workers can finish draining without further wakeups.
   (doseq [[worker wr] (map vector (::workers server) (::wakeup-receivers server))]
     (when (and wr (not (mem/null? wr)))
       (h2o/mt-destroy-wakeup-receiver wr))
     (.set ^AtomicReference (:wakeup-receiver_ worker) nil))

   ;; Wait for all workers to stop themselves after draining connections
   ;; (workers dispose their own HTTP/3 worker contexts before exiting)
   (evloop/join-all! (::workers server))

   ;; Free shared HTTP/3 contexts (quicly + ptls)
   ;; Note: worker contexts are already disposed by workers themselves
   (when-let [http3-contexts (::http3-contexts server)]
     (free-http3-contexts http3-contexts))

   ;; Destroy native ticket manager after HTTP/3 contexts
   (when-let [ticket-mgr (::native-ticket-mgr server)]
     (h2o/ticket-manager-destroy ticket-mgr))

   ;; All workers have exited; clean up h2o resources
   (h2o/destroy-loops (::loops server))

   ;; Close file descriptors
   (doseq [{:keys [dup-fds]} (::listener-runtimes server)
           fd dup-fds]
     (socket/close-fd! fd))
   (doseq [{:keys [listener-fd]} (::listener-runtimes server)]
     (socket/close-fd! listener-fd))

   ;; Dispose config and arena
   (when-let [config-ptr (::config-ptr server)]
     (h2o/config-dispose config-ptr))

   (doseq [{:keys [ssl-ctx-ptr]} (::listener-runtimes server)]
     (when ssl-ctx-ptr
       (h2o/free-ssl-ctx ssl-ctx-ptr)))

   (bp/dispose (-> server ::config :buffer-pool))

   (when-let [arena (::arena server)]
     (.close ^java.lang.AutoCloseable arena))
   nil))

(comment
  (def _server (run-server {}))

  (stop-server _server)

  ;;
  )
