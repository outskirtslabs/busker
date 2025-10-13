(ns ol.h2o.server
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.h2o.evloop :as evloop]
   [ol.h2o.native :as h2o]
   [ol.h2o.native.socket :as socket]
   [ol.h2o.streaming-input :as streaming]
   [ol.h2o.response :as response])
  (:import
   [java.util.concurrent Executors]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]))

(def ^:const default-max-connections 1024)
(def ^:const H2O_SOCKET_FLAG_DONT_READ 0x20)

(defonce vthread-executor
  (delay (Executors/newVirtualThreadPerTaskExecutor)))

(defrecord Request [req-ptr ring-req write-req])

(defn proceed-request [req-ctx]
  [:h2o/proceed-request req-ctx])

(defn send-response [req resp]
  [:h2o/send-response req resp])

(defn evloop-msg-processor
  "Called by drain-mailbox! inside each worker thread for each message on the evloop"
  [op args]
  (case op
    :h2o/proceed-request
    (let [[req-ctx] args]
      (h2o/proceed-req (:req req-ctx)))

    :h2o/send-response
    (let [[req ring-resp] args]
      (response/send-ring-response! (:req-ptr req) ring-resp))
    nil))

(defn enqueue-request!
  "Process request asynchronously on virtual thread.

   The handler runs on a vthread and when complete, the response
   is enqueued back to the same worker thread that received the request.

   Parameters:
   - worker-id: ID of the worker thread that received this request
   - req: The Request
   - ring-handler: Ring handler function (request-map -> response-map)
   - evloop-system: Event loop system for sending messages back to worker"
  [worker-id ^Request req ring-handler evloop-system]
  (.submit @vthread-executor
           (fn []
             (try
               (let [ring-resp (ring-handler (:ring-req req))]
                 (evloop/send-msg! evloop-system worker-id
                                   (send-response req ring-resp)))
               (catch Exception e
                 (println "Handler error:" (.getMessage e))
                 (.printStackTrace e)
                 (evloop/send-msg! evloop-system worker-id
                                   (send-response req {:status 500
                                                       :headers {"content-type" "text/plain"}
                                                       :body "Internal Server Error"})))))))

(defn set-req-body-channel [evloop-system worker-id req-ctx-ptr req-ctx]
  (let [proceed-callback  (fn []
                            (evloop/send-msg! evloop-system worker-id
                                              (proceed-request req-ctx)))
        write-req-channel  (streaming/create-write-req-channel proceed-callback)
        on-req-body-chunk (mem/serialize (fn [_ chunk-seg chunk-len is-last]
                                           #_#p{:chunk-len chunk-len :is-last is-last}
                                           (streaming/add-chunk write-req-channel (mem/read-bytes (mem/reinterpret chunk-seg chunk-len) chunk-len) (if (= 1 is-last) true false)))
                                         [::ffi/fn [::mem/pointer ::mem/pointer ::mem/long ::mem/int] ::mem/void])]
    (h2o/set-on-request-body-chunk-callback req-ctx-ptr on-req-body-chunk)
    write-req-channel))

(defn on-request [ring-handler evloop-system req-ctx-ptr req-ctx]
  (let [worker (evloop/get-current-worker)
        worker-id (:id worker)
        write-req-channel (set-req-body-channel evloop-system worker-id req-ctx-ptr req-ctx)
        ring-req (h2o/build-ring-request (:meta req-ctx) (streaming/input-stream write-req-channel))
        req (Request. req-ctx ring-req write-req-channel)]

    (enqueue-request! worker-id req ring-handler evloop-system)
    ;; TODO: return CLJ_HANDLER_OVERLOADED if system cannot handle more requests
    h2o/CLJ_HANDLER_OK))

(defn on-request-cleanup [ring-handler evloop-system req-ctx-ptr req-ctx]
  (println "CLEANUP!"))

(defn create-ring-handler
  "Create an h2o handler that delegates to a Ring handler.
   Returns handler pointer that must be kept alive."
  [hostconf-ptr ring-handler evloop-system]
  (h2o/create-handler
   hostconf-ptr
   (partial on-request ring-handler evloop-system)
   (partial on-request-cleanup ring-handler evloop-system)
   true false))

(defn create-connection-close-callback
  "Create callback for socket close events to track connection count.
   The callback signature is: void on_close(void *data)"
  [active-connections]
  (mem/serialize
   (fn [_data-ptr]
     (.decrementAndGet ^AtomicLong active-connections))
   [::ffi/fn [::mem/pointer] ::mem/void]))

(defn create-server-config
  "Create and initialize h2o global configuration with a default host and Ring handler.
   Uses provided arena for server lifetime resources.
   Returns map with ::config-ptr, ::hostconf-ptr, ::handler-ptr"
  [arena ring-handler evloop-system]
  (let [size (h2o/globalconf-size)
        config-ptr (mem/alloc size arena)]
    (h2o/config-init config-ptr)
    (let [host-iovec-seg (h2o/create-iovec "default" arena)
          host-iovec-data (mem/deserialize host-iovec-seg ::h2o/h2o-iovec-t)
          hostconf-ptr (h2o/config-register-host config-ptr host-iovec-data 65535)
          handler-ptr (create-ring-handler hostconf-ptr ring-handler evloop-system)]
      {::config-ptr config-ptr
       ::hostconf-ptr hostconf-ptr
       ::handler-ptr handler-ptr})))

(defn update-listener-state!
  "Throttle TCP listeners by starting/stopping accept callbacks based on connection count.
   Prevents accepting new connections when at/over max-connections limit.
   
   QUIC listeners (when supported) must remain active as their UDP socket handles
   all connections; only TCP listeners are throttled here."
  [server thread-idx]
  (let [active (.get ^AtomicLong (::active-connections server))
        max-conns (::max-connections server)
        should-accept? (< active max-conns)
        listeners (::listeners server)
        n-listeners (count listeners)
        listener-socks (nth (::listener-sockets server) thread-idx)
        accept-callbacks (::accept-callbacks server)]

    (doseq [listener-idx (range n-listeners)]
      (let [sock-ptr (nth listener-socks listener-idx)]
        (when-not (mem/null? sock-ptr)
          ;; TODO: Skip QUIC listeners when implemented (check listener config)
          (if should-accept?
            ;; Below limit: ensure TCP listeners are accepting
            (when (zero? (h2o/socket-reading? sock-ptr))
              (let [cb-idx (+ (* thread-idx n-listeners) listener-idx)
                    callback (nth accept-callbacks cb-idx)]
                (h2o/socket-read-start sock-ptr callback)))
            ;; At/over limit: stop accepting new connections
            (when-not (zero? (h2o/socket-reading? sock-ptr))
              (h2o/socket-read-stop sock-ptr))))))))

(defn- initiate-worker-shutdown!
  "Phase 1-3 of graceful shutdown: stop accepting, close listeners, request context shutdown"
  [listener-socks n-listeners loop-ptr ctx-ptr]
  ;; Phase 1: Stop accepting new connections
  (doseq [listener-idx (range n-listeners)]
    (let [sock-ptr (nth listener-socks listener-idx)]
      (when (and (not (mem/null? sock-ptr))
                 (not (zero? (h2o/socket-reading? sock-ptr))))
        (h2o/socket-read-stop sock-ptr))))

  ;; Process stop events immediately
  (h2o/evloop-run loop-ptr 0)

  ;; Phase 2: Close listener sockets
  (doseq [listener-idx (range n-listeners)]
    (let [sock-ptr (nth listener-socks listener-idx)]
      (when-not (mem/null? sock-ptr)
        (h2o/socket-close sock-ptr))))

  ;; Phase 3: Request graceful shutdown (sends GOAWAY to HTTP/2 clients)
  (h2o/context-request-shutdown ctx-ptr))

(defn- all-connections-drained?
  "Check if all connections for this context are closed"
  [ctx-ptr]
  (and (zero? (h2o/context-get-active-conns ctx-ptr))
       (zero? (h2o/context-get-shutdown-conns ctx-ptr))))

(defn- check-and-initiate-shutdown!
  "Check shutdown flag and initiate shutdown phases if needed.
   Returns true if shutdown is active (either just initiated or already in progress)."
  [shutdown-initiated? shutting-down? listener-socks n-listeners loop-ptr ctx-ptr]
  (let [should-shutdown? (.get ^AtomicBoolean shutting-down?)]
    (when (and should-shutdown? (not shutdown-initiated?))
      (initiate-worker-shutdown! listener-socks n-listeners loop-ptr ctx-ptr))
    should-shutdown?))

(defn worker-loop [server thread-idx shutting-down? loop-ptr ctx-ptr listener-socks worker {:keys [shutdown-initiated?]}]
  (let [n-listeners (count (::listeners server))
        shutdown-initiated? (check-and-initiate-shutdown! shutdown-initiated? shutting-down? listener-socks n-listeners loop-ptr ctx-ptr)]

    ;; Exit condition: shutdown initiated AND all connections drained
    ;; this will stop the thread on the next iteration
    (when (and shutdown-initiated? (all-connections-drained? ctx-ptr))
      (evloop/send-msg! (::evloop-system server) (:id worker) evloop/stop-msg))

    ;; Perform periodic cleanup tasks
    (let [now (h2o/evloop-now loop-ptr)
          max-wait (h2o/cleanup-thread now ctx-ptr)]

      ;; Throttle listeners based on connection count (only if not shutting down)
      (when-not shutdown-initiated?
        (update-listener-state! server thread-idx))

      ;; Responsive during shutdown: cap wait at 100ms
      (let [wait-ms (if shutdown-initiated?
                      100
                      (min max-wait 100))]
        (h2o/evloop-run loop-ptr wait-ms)))
    {:shutdown-initiated? shutdown-initiated?}))

(defn create-server
  "Create an h2o server with the given configuration.
   
   Options:
   - :handler      Ring handler function (fn [request-map] response-map) (required)
   - :n-workers    Number of worker threads (default: 2)
   - :listeners    Vector of listener configs [{:port 8080}]
   - :max-connections Maximum concurrent connections (default: 1024)"
  [{:keys [handler n-workers listeners max-connections]
    :or {n-workers 2
         listeners [{:port 8080}]
         max-connections default-max-connections}}]
  (when-not handler
    (throw (ex-info "Handler is required" {:handler handler})))
  (let [arena (mem/shared-arena)
        evloop-system (evloop/create-system)
        config (create-server-config arena handler evloop-system)]
    (merge config
           {::arena arena
            ::handler handler
            ::n-workers n-workers
            ::listeners listeners
            ::max-connections max-connections
            ::active-connections (AtomicLong. 0)
            ::started? (AtomicBoolean. false)
            ::shutting-down? (AtomicBoolean. false)
            ::loops []
            ::contexts []
            ::evloop-system evloop-system
            ::worker-ids []})))

(defn start-server
  "Start the h2o server and begin accepting connections"
  [server]
  (when (.get ^AtomicBoolean (::started? server))
    (throw (ex-info "Server already started" {:server server})))

  (let [config-ptr (::config-ptr server)
        arena (::arena server)
        n-workers (::n-workers server)
        listeners (::listeners server)
        shutting-down? (::shutting-down? server)
        evloop-system (::evloop-system server)
        active-connections (::active-connections server)
        message-handler evloop-msg-processor

        loops (h2o/create-loops n-workers)
        contexts (h2o/create-contexts arena loops config-ptr)

        on-close-callback (create-connection-close-callback active-connections)

        listener-fds (vec (for [{:keys [port]} listeners]
                            (socket/open-master-listener {:port port})))

        dup-fds (vec (for [master-fd listener-fds]
                       (socket/dup-for-threads master-fd n-workers)))

        accept-ctxs (vec (for [thread-idx (range n-workers)
                               listener-idx (range (count listeners))]
                           (h2o/create-accept-ctx
                            arena
                            (nth contexts thread-idx)
                            config-ptr)))

        accept-callbacks (vec (for [accept-ctx-ptr accept-ctxs]
                                (h2o/create-accept-callback accept-ctx-ptr active-connections on-close-callback)))

        listener-sockets (vec (for [thread-idx (range n-workers)]
                                (vec (for [listener-idx (range (count listeners))]
                                       (let [fd (nth (nth dup-fds listener-idx) thread-idx)
                                             sock-ptr (h2o/create-socket-for-loop
                                                       (nth loops thread-idx)
                                                       fd
                                                       H2O_SOCKET_FLAG_DONT_READ)
                                             cb-idx (+ (* thread-idx (count listeners)) listener-idx)
                                             callback (nth accept-callbacks cb-idx)]
                                         (h2o/socket-read-start sock-ptr callback)
                                         sock-ptr)))))

        worker-ids (vec (for [thread-idx (range n-workers)]
                          (let [loop-ptr (nth loops thread-idx)
                                ctx-ptr (nth contexts thread-idx)
                                listener-socks-for-thread (nth listener-sockets thread-idx)]
                            (evloop/start-worker!
                             evloop-system
                             (fn [worker loop-state] (worker-loop server thread-idx shutting-down? loop-ptr ctx-ptr listener-socks-for-thread worker loop-state))
                             {:loop-ptr loop-ptr
                              :thread-idx thread-idx}
                             {:thread-name-prefix "h2o-worker"
                              :max-wait-ms 100
                              :message-handler message-handler}))))]

    (.set ^AtomicBoolean (::started? server) true)

    (assoc server
           ::loops loops
           ::contexts contexts
           ::accept-ctxs accept-ctxs
           ::accept-callbacks accept-callbacks
           ::on-close-callback on-close-callback
           ::listener-fds listener-fds
           ::dup-fds dup-fds
           ::listener-sockets listener-sockets
           ::evloop-system evloop-system
           ::worker-ids worker-ids)))

(defn stop-server
  "Stop the h2o server and clean up resources.
   Follows proper shutdown order:
   1. Signal shutdown to workers
   2. Wait for workers to drain connections and exit naturally
   3. Dispose h2o contexts (cleans up connections)
   4. Destroy event loops
   5. Close file descriptors
   6. Dispose h2o config
   7. Close arena (frees all server-scoped memory)"
  [server]
  (when-not (.get ^AtomicBoolean (::started? server))
    (throw (ex-info "Server not started" {:server server})))

  ;; Signal shutdown to all workers
  (.set ^AtomicBoolean (::shutting-down? server) true)

  ;; Wake up all workers so they see the shutdown signal (send dummy message)
  (evloop/broadcast! (::evloop-system server) [:h2o/wake-up])

  ;; Wait for all workers to stop themselves after draining connections
  (evloop/stop-all! (::evloop-system server))

  ;; All workers have exited; clean up h2o resources
  (h2o/dispose-contexts (::contexts server))
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

  (when-let [arena (::arena server)]
    (.close ^java.lang.AutoCloseable arena))

  (.set ^AtomicBoolean (::started? server) false)
  server)

(comment
  (def _server (create-server {}))

  (def _server (start-server _server))

  (stop-server _server)

  ;;
  )
