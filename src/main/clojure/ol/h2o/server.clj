(ns ol.h2o.server
  (:require
   [ol.h2o.evloop :as evloop]
   [ol.h2o.native :as native]
   [ol.h2o.native.socket :as socket])
  (:import
   [java.util.concurrent.atomic AtomicBoolean]))

(def ^:const default-max-connections 1024)
(def ^:const H2O_SOCKET_FLAG_DONT_READ 0x20)

(defn worker-loop [shutting-down? loop-ptr worker]
  (when-not (.get ^AtomicBoolean shutting-down?)
    (native/evloop-run loop-ptr (int (:max-wait-ms worker)))))

(defn create-server
  "Create an h2o server with the given configuration.
   
   Options:
   - :n-workers    Number of worker threads (default: 2)
   - :listeners    Vector of listener configs [{:port 8080}]
   - :max-connections Maximum concurrent connections (default: 1024)"
  [{:keys [n-workers listeners max-connections]
    :or {n-workers 2
         listeners [{:port 8080}]
         max-connections default-max-connections}}]
  (let [config (native/create-server-config)]
    (merge config
           {::n-workers n-workers
            ::listeners listeners
            ::max-connections max-connections
            ::started? (AtomicBoolean. false)
            ::shutting-down? (AtomicBoolean. false)
            ::loops []
            ::contexts []
            ::evloop-system nil
            ::worker-ids []})))

(defn start-server
  "Start the h2o server and begin accepting connections"
  [server]
  (when (.get ^AtomicBoolean (::started? server))
    (throw (ex-info "Server already started" {:server server})))

  (let [config-ptr (::native/config-ptr server)
        arena (::native/arena server)
        n-workers (::n-workers server)
        listeners (::listeners server)
        shutting-down? (::shutting-down? server)

        loops (native/create-loops n-workers)
        contexts (native/create-contexts arena loops config-ptr)

        listener-fds (vec (for [{:keys [port]} listeners]
                            (socket/open-master-listener {:port port})))

        dup-fds (vec (for [master-fd listener-fds]
                       (socket/dup-for-threads master-fd n-workers)))

        listener-sockets (vec (for [thread-idx (range n-workers)]
                                (vec (for [listener-idx (range (count listeners))]
                                       (let [fd (nth (nth dup-fds listener-idx) thread-idx)]
                                         (native/create-socket-for-loop
                                          (nth loops thread-idx)
                                          fd
                                          H2O_SOCKET_FLAG_DONT_READ))))))

        evloop-system (evloop/create-system)

        worker-ids (vec (for [thread-idx (range n-workers)]
                          (let [loop-ptr (nth loops thread-idx)]
                            (evloop/start-worker!
                             evloop-system
                             (partial worker-loop shutting-down? loop-ptr)
                             {:loop-ptr loop-ptr
                              :thread-idx thread-idx}
                             {:thread-name-prefix "h2o-worker"
                              :max-wait-ms 100}))))]

    (.set ^AtomicBoolean (::started? server) true)

    (assoc server
           ::loops loops
           ::contexts contexts
           ::listener-fds listener-fds
           ::dup-fds dup-fds
           ::listener-sockets listener-sockets
           ::evloop-system evloop-system
           ::worker-ids worker-ids)))

(defn stop-server
  "Stop the h2o server and clean up resources"
  [server]
  (when-not (.get ^AtomicBoolean (::started? server))
    (throw (ex-info "Server not started" {:server server})))
  (.set ^AtomicBoolean (::shutting-down? server) true)
  (evloop/stop-all! (::evloop-system server))
  (native/dispose-contexts (::contexts server))
  (native/destroy-loops (::loops server))
  (doseq [dup-fd-vec (::dup-fds server)
          fd dup-fd-vec]
    (socket/close-fd! fd))
  (doseq [fd (::listener-fds server)]
    (socket/close-fd! fd))
  (native/dispose-server-config (::config-ptr server))
  (.set ^AtomicBoolean (::started? server) false)
  server)

(comment
  (def _server (create-server {}))

  (def _server (start-server _server))

  (stop-server _server)

  ;;
  )
