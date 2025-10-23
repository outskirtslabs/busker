(ns ol.h2o.server
  (:require
   [clojure.java.io :as io]
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.h2o.buffer-pool :as bp]
   [ol.h2o.byte-bounded-queue :as bbq]
   [ol.h2o.evloop :as evloop]
   [ol.h2o.internal.protocols :as p]
   [ol.h2o.native :as h2o]
   [ol.h2o.native.socket :as socket]
   [ol.h2o.request :as request]
   [ol.h2o.response-queue :as response-queue])
  (:import
   [java.util.concurrent ExecutorService Executors TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong AtomicReference]
   [ol.h2o.response_queue ResponseState]))

(set! *warn-on-reflection* true)

(def ^:const default-max-connections 1024)

(defn- response-state-pending?
  [^ResponseState st]
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
              (when-let [^ResponseState st (::response-queue/state write-resp)]
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

    :h2o/start-response
    (let [[start-fn] args]
      (start-fn))
    nil))

(defn create-connection-close-callback
  "Create callback for socket close events to track connection count.
   The callback signature is: void on_close(void *data)"
  [active-connections]
  (let [cb (fn [_data-ptr]
             (.decrementAndGet ^AtomicLong active-connections))]
    {::connection-close-cb cb
     ::connection-close-cb-ptr (mem/serialize cb [::ffi/fn [::mem/pointer] ::mem/void])}))

(defn create-accept-callback
  "Create accept callback for a listener socket with connection tracking.
   h2o_accept handles both plaintext and TLS connections automatically:
   - For plaintext: directly starts HTTP/1.1
   - For TLS: initiates SSL handshake, negotiates protocol (HTTP/1.1 or HTTP/2 via ALPN)

   Parameters:
   - accept-ctx-ptr: pointer to h2o_accept_ctx_t (contains ssl_ctx if TLS)
   - active-connections: AtomicLong for connection counting
   - on-close-callback: callback function pointer for socket close"
  [accept-ctx-ptr active-connections on-close-callback]
  (let [accept-cb (fn [listener-ptr err-ptr]
                    (when-not (mem/null? err-ptr)
                      nil)
                    (let [sock-ptr (h2o/evloop-socket-accept listener-ptr)]
                      (when-not (mem/null? sock-ptr)
                        (.incrementAndGet ^java.util.concurrent.atomic.AtomicLong active-connections)
                        (h2o/socket-set-on-close sock-ptr on-close-callback (mem/as-segment 0))
                        (h2o/h2o-accept accept-ctx-ptr sock-ptr))))]
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
  "Throttle TCP listeners by starting/stopping accept callbacks based on connection count.
   Prevents accepting new connections when at/over max-connections limit.
   
   QUIC listeners (when supported) must remain active as their UDP socket handles
   all connections; only TCP listeners are throttled here."
  [listener-socks accept-callbacks]
  (let [;; TODO implement connection limit
        should-accept? true]
    (doseq [listener-idx (range (count listener-socks))]
      (let [sock-ptr (nth listener-socks listener-idx)
            accept-callback (nth accept-callbacks listener-idx)]
        #_{:clj-kondo/ignore [:type-mismatch]}
        (when-not (mem/null? sock-ptr)
          ;; TODO: Skip QUIC listeners when implemented (check listener config)
          (if should-accept?
            ;; Below limit: ensure TCP listeners are accepting
            (when (zero? (h2o/socket-reading? sock-ptr))
              (h2o/socket-read-start sock-ptr accept-callback))
            ;; At/over limit: stop accepting new connections
            (when-not (zero? (h2o/socket-reading? sock-ptr))
              (h2o/socket-read-stop sock-ptr))))))))

(defn- initiate-worker-shutdown!
  "Phase 1-3 of graceful shutdown: stop accepting, close listeners, request context shutdown"
  [{:keys [listener-socks loop-ptr ctx-ptr]}]
  ;; Phase 1: Stop accepting new connections
  (doseq [listener-idx (range (count listener-socks))]
    (let [sock-ptr (nth listener-socks listener-idx)]
      (when (and (not (mem/null? sock-ptr))
                 #_{:clj-kondo/ignore [:type-mismatch]}
                 (not (zero? (h2o/socket-reading? sock-ptr))))
        (h2o/socket-read-stop sock-ptr))))

  ;; Process stop events immediately
  (h2o/evloop-run loop-ptr 0)

  ;; Phase 2: Close listener sockets
  (doseq [listener-idx (range (count listener-socks))]
    (let [sock-ptr (nth listener-socks listener-idx)]
      (when-not (mem/null? sock-ptr)
        (h2o/socket-close sock-ptr))))

  ;; Phase 3: Request graceful shutdown (sends GOAWAY to HTTP/2 clients)
  (h2o/context-request-shutdown ctx-ptr))

(defn- all-connections-drained?
  "Check if all connections for this context are closed"
  [ctx-ptr]

  #_{:clj-kondo/ignore [:type-mismatch]}
  (and (zero? (h2o/context-get-active-conns ctx-ptr))
       (zero? (h2o/context-get-shutdown-conns ctx-ptr))))

(defn- check-and-initiate-shutdown!
  "Check shutdown flag and initiate shutdown phases if needed.
   Returns true if shutdown is active (either just initiated or already in progress)."
  [{:keys [shutdown-initiated? shutting-down?] :as state}]
  (let [should-shutdown? (.get ^AtomicBoolean shutting-down?)]
    (when (and should-shutdown? (not shutdown-initiated?))
      (initiate-worker-shutdown! state))
    should-shutdown?))

(defn- receiver-destroyed?*
  "Has the wakeup receiver been removed (either already noted or cleared by the main thread)?"
  [worker destroyed-flag]
  (or (true? destroyed-flag)
      (nil? (.get ^AtomicReference (:wakeup-receiver_ worker)))))

(defn- dispose-context-if-ready
  "If we're past shutdown initiation, the receiver is gone, and connections are drained,
   dispose the worker's context and queue an ::stop message."
  [worker ctx-ptr context-disposed? shutdown-initiated? receiver-destroyed?]
  (if (or context-disposed?
          (not shutdown-initiated?)
          (not receiver-destroyed?)
          (not (all-connections-drained? ctx-ptr)))
    context-disposed?
    (do
      (h2o/context-dispose ctx-ptr)
      (p/send-msg worker evloop/stop-msg)
      true)))

;; TODO this fn call is a tragedy
(defn worker-loop
  "Drive a worker event-loop iteration, advancing staged shutdown once the main
   thread has signalled it. The worker moves through three phases:
     1. `shutdown-initiated?` is recorded after we stop accepting and close listeners.
     2. `receiver-destroyed?` flips true when stop-server destroys the wakeup receiver,
        ensuring no more cross-thread wakeups are delivered.
     3. Once connections drain, `context-disposed?` becomes true after the worker
        disposes its `h2o_context_t` and posts ::stop to exit the loop."
  [worker {:keys [shutdown-initiated? context-disposed? receiver-destroyed?] :as _loop-state}
   {:keys [loop-ptr ctx-ptr listener-socks accept-callbacks] :as state}]
  (let [shutdown-recorded?  (true? shutdown-initiated?)
        shutdown-initiated? (check-and-initiate-shutdown! (assoc state :shutdown-initiated? shutdown-recorded?))
        receiver-destroyed? (receiver-destroyed?* worker receiver-destroyed?)
        context-disposed?   (dispose-context-if-ready worker ctx-ptr (true? context-disposed?) shutdown-initiated? receiver-destroyed?)]
    (if context-disposed?
      (h2o/evloop-run loop-ptr 0)
      (let [now      (h2o/evloop-now loop-ptr)
            max-wait (h2o/cleanup-thread now ctx-ptr)
            max-wait (if (pending-response-work? worker) 5 max-wait)]
        (when-not shutdown-initiated?
          (update-listener-state! listener-socks accept-callbacks))
        (h2o/evloop-run loop-ptr (if (pos? (p/count-msgs worker)) 0 max-wait))))
    {:shutdown-initiated? shutdown-initiated?
     :context-disposed?   context-disposed?
     :receiver-destroyed? receiver-destroyed?}))

(defn- validate-tls-config
  "Validate TLS configuration for a listener. Throws on invalid config."
  [{:keys [cert-file key-file] :as tls-config}]
  (when-not (and cert-file key-file)
    (throw (ex-info "TLS config requires both :cert-file and :key-file" {:tls-config tls-config})))
  (when-not (.exists (io/file cert-file))
    (throw (ex-info "TLS certificate file not found" {:cert-file cert-file})))
  (when-not (.exists (io/file key-file))
    (throw (ex-info "TLS private key file not found" {:key-file key-file})))
  tls-config)

(defn- validate-listener
  "Validate a single listener configuration. Throws on invalid config."
  [{:keys [port tls] :as listener}]
  (when-not (and (int? port) (pos? port) (<= port 65535))
    (throw (ex-info "Listener port must be integer between 1 and 65535"
                    {:listener listener})))
  (when tls
    (validate-tls-config tls))
  listener)

(defn- create-ssl-contexts
  "Create SSL_CTX for each TLS listener.
   Returns map: listener-index -> {:ssl-ctx ssl-ctx-ptr}
   Throws if SSL_CTX creation fails."
  [listeners]
  (into {}
        (keep-indexed
         (fn [idx {:keys [tls]}]
           (when tls
             (let [{:keys [cert-file key-file protocols]} tls
                   enable-http2? (or (nil? protocols)
                                     (contains? (set protocols) :http2))
                   ssl-ctx (h2o/create-ssl-ctx cert-file key-file (if enable-http2? 1 0))]
               (when (mem/null? ssl-ctx)
                 (throw (ex-info "Failed to create SSL_CTX (check OpenSSL errors in stderr)"
                                 {:listener-index idx
                                  :cert-file cert-file
                                  :key-file key-file})))
               [idx {:ssl-ctx ssl-ctx}])))
         listeners)))

(defn with-defaults [{:keys [n-workers listeners max-connections executor server-name
                             compress? compress-min-size compress-gzip-level output-buffer-size
                             compress-brotli-level compress-zstd-level buffer-pool]
                      :or {compress-brotli-level 1
                           compress-gzip-level 1
                           compress-min-size 100
                           compress? true
                           compress-zstd-level 3
                           executor (Executors/newVirtualThreadPerTaskExecutor)
                           listeners [{:port 8080}]
                           max-connections default-max-connections
                           n-workers 1
                           output-buffer-size 32768
                           server-name "ol.h2o/dev"}
                      :as config}]

  (let [validated-listeners (mapv validate-listener listeners)]
    (merge config {:buffer-pool (or buffer-pool (bp/make-bytebuffer-pool {}))
                   :compress-brotli-level compress-brotli-level
                   :compress? compress?
                   :compress-gzip-level compress-gzip-level
                   :compress-min-size compress-min-size
                   :compress-zstd-level compress-zstd-level
                   :executor executor
                   :listeners validated-listeners
                   :max-connections max-connections
                   :n-workers n-workers
                   :output-buffer-size output-buffer-size
                   :server-name server-name})))

(defn run-server
  "Start an h2o webserver to serve the given Ring handler according to the
  supplied options:

  Core Options:
  :listeners              - vector of listener maps, each with :port (required)
                            (defaults to [{:port 8080}])
  :n-workers              - number of event loop worker threads
                            (defaults to available CPU cores)
  :executor               - ExecutorService for handler execution
                            (defaults to virtual thread executor)
  :max-connections        - maximum concurrent connections across all workers
                            (defaults to 1024)

  :ouput-buffer-size      - the size of the buffer into which response data is aggregated before being sent to the client
                            (defaults to 32768)

  Server Identity:
  :server-name            - server name for Server header
                            (defaults to h2o version string)
  :proxy-status-identity  - identity for Proxy-Status header (RFC 9209)

  Request Limits:
  :max-request-entity-size       - maximum request entity size in bytes
                            (defaults to 1GB)
  :max-delegations        - maximum internal request delegations
                            (defaults to 5)
  :max-reprocesses        - maximum internal request reprocesses
                            (defaults to 5)

  Timeouts (all in milliseconds):
  :handshake-timeout      - SSL/TLS handshake timeout
                            (defaults to 10000)
  :max-spare-pipes        - maximum idle connection pipes to retain
                            (defaults to 0)

  HTTP/1.1 Options:
  :http1-req-timeout      - HTTP/1.1 request timeout
                            (defaults to 10000)
  :http1-req-io-timeout   - HTTP/1.1 request I/O timeout
                            (defaults to 5000)
  :http1-upgrade?         - allow HTTP/1.1 to HTTP/2 upgrade
                            (defaults to true)

  HTTP/2 Options:
  :http2-idle-timeout     - HTTP/2 idle timeout
                            (defaults to 10000)
  :http2-graceful-shutdown-timeout
                          - HTTP/2 graceful shutdown timeout (0 = no timeout)
                            (defaults to 0)
  :http2-max-streams      - maximum concurrent HTTP/2 streams
                            (defaults to 100)
  :http2-max-requests     - maximum concurrent HTTP/2 requests per connection
                            (defaults to 100)
  :http2-max-streaming-requests
                          - maximum concurrent streaming requests per connection
                            (defaults to 1)
  :http2-max-priority-streams
                          - maximum streams in IDLE/CLOSED for priority tracking
                            (defaults to 16)
  :http2-stream-window-size
                          - HTTP/2 stream-level flow control window size
                            (defaults to 16777216, 16MB)
  :http2-dos-delay        - delay in ms when suspicious behavior detected
                            (defaults to 100)

  HTTP/3 Options:
  :http3-idle-timeout     - HTTP/3 idle timeout (from quicly)
  :http3-graceful-shutdown-timeout
                          - HTTP/3 graceful shutdown timeout
                            (defaults to 0)
  :http3-stream-window-size
                          - HTTP/3 stream-level flow control window size
                            (defaults to 16777216, 16MB)
  :http3-ack-frequency    - ACK frequency for HTTP/3
                            (defaults to 0, uses quicly default)

  Compression Options:
  :compress?              - Enables on-the-fly compression of HTTP response
                            (defaults to true)
  :compress-min-size      - The minimum size in bytes a response needs to have before compression kicks in
                            (defaults to 100)
  :compress-gzip-level    - The gzip compression level
                            (defaults to 1)
  :compress-brotli-level   - The brotli compression level
                            (defaults to 1)
  :compress-zstd-level    - The zstd compression level
                            (defaults to 3)

  Returns a server map that can be passed to stop-server."
  ([handler]
   (run-server handler {}))
  ([handler config]
   (when-not handler (throw (ex-info "Handler is required" {:handler handler})))
   (let [{:keys [n-workers listeners max-connections executor] :as config} (with-defaults config)
         arena (mem/shared-arena)
         {::keys [config-ptr]} (create-server-config arena handler config)
         active-connections (AtomicLong. 0)
         shutting-down? (AtomicBoolean. false)

         message-handler evloop-msg-processor

         ssl-contexts (create-ssl-contexts listeners)

         loops (h2o/create-loops n-workers)
         contexts (h2o/create-contexts arena loops config-ptr)

         wakeup-receivers (vec (for [ctx-ptr contexts]
                                 (h2o/mt-create-wakeup-receiver ctx-ptr)))

         on-close-callback (create-connection-close-callback active-connections)

         listener-fds (vec (for [{:keys [port]} listeners]
                             (socket/open-master-listener {:port port})))

         dup-fds (vec (for [master-fd listener-fds]
                        (socket/dup-for-threads master-fd n-workers)))

         accept-ctxs (vec (for [thread-idx (range n-workers)
                                listener-idx (range (count listeners))]
                            (let [ssl-ctx-ptr (get-in ssl-contexts [listener-idx :ssl-ctx])]
                              (h2o/create-accept-ctx
                               arena
                               (nth contexts thread-idx)
                               config-ptr
                               ssl-ctx-ptr))))

         accept-callbacks (vec (for [accept-ctx-ptr accept-ctxs]
                                 (create-accept-callback accept-ctx-ptr active-connections (::connection-close-cb-ptr on-close-callback))))

         listener-sockets (vec (for [thread-idx (range n-workers)]
                                 (vec (for [listener-idx (range (count listeners))]
                                        (let [fd (nth (nth dup-fds listener-idx) thread-idx)
                                              sock-ptr (h2o/create-socket-for-loop
                                                        (nth loops thread-idx)
                                                        fd
                                                        h2o/H2O_SOCKET_FLAG_DONT_READ)
                                              cb-idx (+ (* thread-idx (count listeners)) listener-idx)
                                              callback (::accept-cb-ptr (nth accept-callbacks cb-idx))]
                                          (h2o/socket-read-start sock-ptr callback)
                                          sock-ptr)))))

         workers (vec (for [thread-idx (range n-workers)]
                        (let [loop-ptr (nth loops thread-idx)
                              ctx-ptr (nth contexts thread-idx)
                              listener-socks-for-thread (nth listener-sockets thread-idx)
                              thread-accept-callback (::accept-cb-ptr (nth accept-callbacks thread-idx))
                              accept-callbacks-for-thread (vec (repeat (count listener-socks-for-thread)
                                                                       thread-accept-callback))]

                          (evloop/start-worker!
                           (fn [worker loop-state] (worker-loop worker loop-state {:listener-socks listener-socks-for-thread
                                                                                   :accept-callbacks accept-callbacks-for-thread
                                                                                   :loop-ptr loop-ptr
                                                                                   :ctx-ptr ctx-ptr
                                                                                   :shutting-down? shutting-down?}))
                           message-handler
                           (nth wakeup-receivers thread-idx)))))]
     {::arena arena
      ::config config
      ::handler handler
      ::executor executor

      ::n-workers n-workers
      ::listeners listeners
      ::max-connections max-connections
      ::active-connections active-connections
      ::shutting-down? shutting-down?
      ::loops loops
      ::contexts contexts
      ::accept-ctxs accept-ctxs
      ::ssl-contexts ssl-contexts
      ::accept-callbacks accept-callbacks
      ::on-close-callback on-close-callback
      ::listener-fds listener-fds
      ::wakeup-receivers wakeup-receivers
      ::dup-fds dup-fds
      ::listener-sockets listener-sockets
      ::workers workers})))

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
   (evloop/join-all! (::workers server))

   ;; All workers have exited; clean up h2o resources
   (h2o/destroy-loops (::loops server))

   ;; Close file descriptors
   (doseq [dup-fd-vec (::dup-fds server)
           fd dup-fd-vec]
     (socket/close-fd! fd))
   (doseq [fd (::listener-fds server)]
     (socket/close-fd! fd))

   ;; Dispose config and arena
   (when-let [config-ptr (::config-ptr server)]
     (h2o/config-dispose config-ptr))

   (doseq [[_idx {:keys [ssl-ctx]}] (::ssl-contexts server)]
     (when ssl-ctx
       (h2o/free-ssl-ctx ssl-ctx)))

   (bp/dispose (-> server ::config :buffer-pool))

   (when-let [arena (::arena server)]
     (.close ^java.lang.AutoCloseable arena))
   nil))

(comment
  (def _server (run-server {}))

  (stop-server _server)

  ;;
  )
