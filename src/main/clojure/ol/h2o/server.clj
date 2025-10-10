(ns ol.h2o.server
  (:require
   [coffi.mem :as mem]
   [ol.h2o.evloop :as evloop]
   [ol.h2o.native :as h2o]
   [ol.h2o.native.socket :as socket]
   [ol.h2o.response :as response])
  (:import
   [java.util.concurrent Executors]
   [java.util.concurrent.atomic AtomicBoolean]))

(def ^:const default-max-connections 1024)
(def ^:const H2O_SOCKET_FLAG_DONT_READ 0x20)

(defonce vthread-executor
  (delay (Executors/newVirtualThreadPerTaskExecutor)))

(defrecord Request [req-ptr ring-req])

(defn evloop-msg-processor
  "Called by drain-mailbox! inside each worker thread for each message on the evloop"
  [op args]
  (case op
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
                                   [:h2o/send-response req ring-resp]))
               (catch Exception e
                 (println "Handler error:" (.getMessage e))
                 (.printStackTrace e)
                 (evloop/send-msg! evloop-system worker-id
                                   [:h2o/send-response req {:status 500
                                                            :headers {"content-type" "text/plain"}
                                                            :body "Internal Server Error"}]))))))

(defn on-request-callback
  [ring-handler evloop-system req-ptr]
  (let [worker (evloop/get-current-worker)
        worker-id (:id worker)
        req (Request. req-ptr (h2o/build-ring-request req-ptr))]
    (enqueue-request! worker-id req ring-handler evloop-system)))

(defn create-ring-handler
  "Create an h2o handler that delegates to a Ring handler.
   Returns handler pointer that must be kept alive."
  [pathconf-ptr ring-handler evloop-system]
  (let [handler-ptr (h2o/create-handler pathconf-ptr (h2o/handler-size))
        on-req-callback (h2o/create-request-callback (partial on-request-callback ring-handler evloop-system))]
    (h2o/handler-set-on-req handler-ptr on-req-callback)
    handler-ptr))

(defn create-server-config
  "Create and initialize h2o global configuration with a default host and Ring handler.
   Uses provided arena for server lifetime resources.
   Returns map with ::config-ptr, ::hostconf-ptr, ::pathconf-ptr, ::handler-ptr"
  [arena ring-handler evloop-system]
  (let [size (h2o/globalconf-size)
        config-ptr (mem/alloc size arena)]
    (h2o/config-init config-ptr)
    (let [host-iovec-seg (h2o/create-iovec "default" arena)
          host-iovec-data (mem/deserialize host-iovec-seg ::h2o/h2o-iovec-t)
          hostconf-ptr (h2o/config-register-host config-ptr host-iovec-data 65535)
          pathconf-ptr (h2o/config-register-path hostconf-ptr "/" 0)
          handler-ptr (create-ring-handler pathconf-ptr ring-handler evloop-system)]
      {::config-ptr config-ptr
       ::hostconf-ptr hostconf-ptr
       ::pathconf-ptr pathconf-ptr
       ::handler-ptr handler-ptr})))

(defn worker-loop [shutting-down? loop-ptr worker]
  (when-not (.get ^AtomicBoolean shutting-down?)
    (h2o/evloop-run loop-ptr (int (:max-wait-ms worker)))))

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
        message-handler evloop-msg-processor

        loops (h2o/create-loops n-workers)
        contexts (h2o/create-contexts arena loops config-ptr)

        listener-fds (vec (for [{:keys [port]} listeners]
                            (socket/open-master-listener {:port port})))

        dup-fds (vec (for [master-fd listener-fds]
                       (socket/dup-for-threads master-fd n-workers)))

        ;; Create accept contexts for each worker/listener pair
        accept-ctxs (vec (for [thread-idx (range n-workers)
                               listener-idx (range (count listeners))]
                           (h2o/create-accept-ctx
                            arena
                            (nth contexts thread-idx)
                            config-ptr)))

        ;; Create accept callbacks for each accept context
        accept-callbacks (vec (for [accept-ctx-ptr accept-ctxs]
                                (h2o/create-accept-callback accept-ctx-ptr)))

        ;; Create listener sockets and start accepting
        listener-sockets (vec (for [thread-idx (range n-workers)]
                                (vec (for [listener-idx (range (count listeners))]
                                       (let [fd (nth (nth dup-fds listener-idx) thread-idx)
                                             sock-ptr (h2o/create-socket-for-loop
                                                       (nth loops thread-idx)
                                                       fd
                                                       H2O_SOCKET_FLAG_DONT_READ)
                                             cb-idx (+ (* thread-idx (count listeners)) listener-idx)
                                             callback (nth accept-callbacks cb-idx)]
                                         ;; Start reading to accept connections
                                         (h2o/socket-read-start sock-ptr callback)
                                         sock-ptr)))))

        worker-ids (vec (for [thread-idx (range n-workers)]
                          (let [loop-ptr (nth loops thread-idx)]
                            (evloop/start-worker!
                             evloop-system
                             (partial worker-loop shutting-down? loop-ptr)
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
           ::listener-fds listener-fds
           ::dup-fds dup-fds
           ::listener-sockets listener-sockets
           ::evloop-system evloop-system
           ::worker-ids worker-ids)))

(defn stop-server
  "Stop the h2o server and clean up resources.
   Follows proper shutdown order:
   1. Signal shutdown to workers
   2. Stop event loops and wait for workers to exit
   3. Dispose h2o contexts (sends GOAWAY, cleans up connections)
   4. Destroy event loops
   5. Close file descriptors
   6. Dispose h2o config
   7. Close arena (frees all server-scoped memory)"
  [server]
  (when-not (.get ^AtomicBoolean (::started? server))
    (throw (ex-info "Server not started" {:server server})))
  (.set ^AtomicBoolean (::shutting-down? server) true)
  (evloop/stop-all! (::evloop-system server))
  (h2o/dispose-contexts (::contexts server))
  (h2o/destroy-loops (::loops server))
  (doseq [dup-fd-vec (::dup-fds server)
          fd dup-fd-vec]
    (socket/close-fd! fd))
  (doseq [fd (::listener-fds server)]
    (socket/close-fd! fd))
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
