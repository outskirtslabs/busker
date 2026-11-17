(ns ol.busker.failed-worker-test
  "Reload, drain and stop after a worker's event loop has failed."
  (:require
   [babashka.ffi :as mem]
   [clojure.test :refer [deftest is use-fixtures]]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.config :as config]
   [ol.busker.generation :as generation]
   [ol.busker.generation.accept :as accept]
   [ol.busker.internal.protocols :as p]
   [ol.busker.listen :as listen]
   [ol.busker.native :as h2o]
   [ol.busker.runtime :as runtime]
   [ol.busker.test-utils :as util]
   [taoensso.trove :as trove])
  (:import
   [clojure.lang ExceptionInfo]
   [java.net BindException ConnectException InetSocketAddress ServerSocket Socket]
   [java.nio.channels DatagramChannel]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent TimeUnit]
   [java.util.concurrent.atomic AtomicLongArray AtomicReference]))

(defn- short-lifecycle-waits
  "Shortens the 60 s lifecycle waits, so a wait that should not happen fails
  the test in seconds."
  [f]
  (let [stop! generation/stop!]
    (with-redefs-fn {#'generation/stop-accepting-wait-ms 2000
                     #'generation/stop! (fn
                                          ([instance] (stop! instance 5 TimeUnit/SECONDS))
                                          ([instance ^long timeout unit] (stop! instance timeout unit)))}
      f)))

(use-fixtures :each short-lifecycle-waits)

(defn- server-config
  [port body & {:as extra}]
  (merge {:n-workers 2
          :entrypoints {:http {:bind (str "127.0.0.1:" port)
                               :tls false}}
          :dispatch [{:handler (fn [_] {:status 200 :body body})}]}
         extra))

(defn- managed-config
  [port body]
  (assoc (server-config port body)
         :tls {:certificates {:manage ["managed.example"]}
               :issuers [{:directory-url "https://acme.example/directory"}]}))

(defn- active-instance
  [server]
  (:instance (:active @(:busker/state server))))

(defn- eventually
  [f]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (or (f)
          (when (< (System/currentTimeMillis) deadline)
            (Thread/sleep 20)
            (recur))))))

(defn- connection-counts
  [instance]
  (let [^AtomicLongArray counts (::generation/worker-connection-counts instance)]
    (mapv #(.get counts (int %)) (range (.length counts)))))

(defn- fail-worker!
  "Throws on the event-loop thread of worker `idx` of `instance`, which ends its
  loop through `:on-native-failure!` as any unexpected failure does, and waits
  for the thread to end."
  [instance idx]
  (let [worker (nth (::generation/workers instance) idx)
        update-listener-state! @#'accept/update-listener-state!]
    (with-redefs-fn {#'accept/update-listener-state!
                     (fn [state]
                       (when (= idx (:worker-idx state))
                         (throw (ex-info "simulated worker failure" {})))
                       (update-listener-state! state))}
      #(do (p/wake worker)
           (.join ^Thread (:thread worker) 5000)))
    (is (not (.isAlive ^Thread (:thread worker))))
    (is (some? (.get ^AtomicReference (:native-failure_ worker))))
    worker))

(defn- fail-idle-worker!
  "Waits until no worker of `instance` holds a connection, then fails worker
  `idx` as [[fail-worker!]] does."
  [instance idx]
  (is (eventually #(every? zero? (connection-counts instance))))
  (fail-worker! instance idx))

(defn- port-bindable?
  [port]
  (try
    (with-open [socket (ServerSocket.)]
      (.setReuseAddress socket true)
      (.bind socket (InetSocketAddress. "127.0.0.1" (int port)))
      true)
    (catch Throwable _
      false)))

(defn- datagram-bindable?
  "True when a UDP socket without SO_REUSEPORT can bind `port`."
  [port]
  (try
    (with-open [channel (DatagramChannel/open)]
      (.bind channel (InetSocketAddress. "127.0.0.1" (int port)))
      true)
    (catch BindException _
      false)))

(defn- body
  [port]
  (:out (util/curl :http :h1 port "/" :max-time 5)))

(defn- open-served-connection!
  "Opens a keep-alive connection to `port` and reads one response on it, so the
  server holds the connection open."
  ^Socket [port]
  (let [socket (doto (Socket.)
                 (.connect (InetSocketAddress. "127.0.0.1" (int port)) 2000)
                 (.setSoTimeout 5000))
        out (.getOutputStream socket)
        in (.getInputStream socket)
        response (StringBuilder.)]
    (.write out (.getBytes "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n" StandardCharsets/US_ASCII))
    (.flush out)
    (while (not (.endsWith (str response) "\r\n\r\nheld"))
      (.append response (char (.read in))))
    socket))

(def ^:private kept-message
  "Worker threads ended with connections, responses or blocks nothing can finish. TCP and Unix listeners and HTTP/3 UDP sockets closed. Loops, contexts and memory kept.")

(defn- refuses-connections?
  [port]
  (try
    (with-open [socket (Socket.)]
      (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 1000)
      false)
    (catch ConnectException _
      true)))

(defn- forget-kept-generation!
  "Gives back the connection-limit slots of the `held` connections a failed
  worker of `instance` keeps, and drops the kept generation, since the limit
  counter and the list of kept generations live for the whole test process."
  [instance held]
  (dotimes [_ held] (h2o/conn-limit-release))
  (swap! @#'generation/failed-retirements_
         (fn [kept] (vec (remove #(identical? instance %) kept)))))

(defn- elapsed-ms
  [f]
  (let [start (System/nanoTime)
        result (f)]
    [result (quot (- (System/nanoTime) start) 1000000)]))

(deftest generation-stop-releases-a-generation-after-a-worker-failed-test
  (let [port (util/free-port)
        instance (generation/start! (config/load! (server-config port "x")) nil)
        disposed_ (atom [])
        context-dispose h2o/context-dispose]
    (is (zero? (:exit (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2))))
    (fail-idle-worker! instance 1)
    (with-redefs [h2o/context-dispose (fn [ctx]
                                        (swap! disposed_ conj ctx)
                                        (context-dispose ctx))]
      (is (nil? (generation/stop! instance 5 TimeUnit/SECONDS))))
    (is (= :stopped (generation/phase instance)))
    (is (= (frequencies (::generation/contexts instance))
           (frequencies @disposed_)))
    (is (port-bindable? port))))

(deftest generation-stop-keeps-a-generation-whose-failed-worker-holds-a-connection-test
  (let [port (util/free-port)
        instance (generation/start! (config/load! (server-config port "held")) nil)]
    (with-open [_ (open-served-connection! port)]
      (let [idx (.indexOf ^java.util.List (connection-counts instance) 1)
            failed (fail-worker! instance idx)
            healthy (nth (::generation/workers instance) (- 1 idx))
            [error ms] (elapsed-ms
                        #(try
                           (generation/stop! instance 5 TimeUnit/SECONDS)
                           nil
                           (catch ExceptionInfo e e)))]
        (is (= kept-message (ex-message error)))
        (is (= [(:id failed)] (:unfinished-workers (ex-data error))))
        (is (< ms 2000) "the healthy worker does not wait for the failed worker's connection")
        (is (not (.isAlive ^Thread (:thread healthy))))
        (is (= :kept (generation/phase instance)))
        (is (refuses-connections? port))
        (is (port-bindable? port))
        (forget-kept-generation! instance 1)))))

(deftest generation-stop-keeps-a-generation-whose-failed-worker-pool-cannot-retire-test
  (let [port (util/free-port)
        instance (generation/start! (config/load! (server-config port "x")) nil)
        retire! bp/retire!]
    (is (zero? (:exit (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2))))
    (let [failed (fail-idle-worker! instance 1)
          failed-pool (:stream-pool failed)
          error (with-redefs [bp/retire! (fn [pool] (and (not (identical? failed-pool pool))
                                                         (retire! pool)))]
                  (try
                    (generation/stop! instance 5 TimeUnit/SECONDS)
                    nil
                    (catch ExceptionInfo e e)))]
      (is (= kept-message (ex-message error)))
      (is (= [(:id failed)] (:unfinished-workers (ex-data error))))
      (is (= :kept (generation/phase instance)))
      (is (port-bindable? port))
      (is (retire! failed-pool))
      (forget-kept-generation! instance 0))))

(deftest generation-stop-records-a-kept-generation-when-a-claim-release-fails-test
  (let [port (util/free-port)
        instance (generation/start! (config/load! (server-config port "x")) nil)
        retire! bp/retire!
        release! listen/release!
        release-error (ex-info "simulated unlink failure" {})]
    (is (zero? (:exit (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2))))
    (let [failed-pool (:stream-pool (fail-idle-worker! instance 1))
          error (with-redefs [bp/retire! (fn [pool] (and (not (identical? failed-pool pool))
                                                         (retire! pool)))
                              listen/release! (fn [_] (throw release-error))]
                  (try
                    (generation/stop! instance 5 TimeUnit/SECONDS)
                    nil
                    (catch ExceptionInfo e e)))]
      (is (= kept-message (ex-message error)))
      (is (= [release-error] (vec (.getSuppressed ^Throwable error))))
      (is (= :kept (generation/phase instance)))
      (is (some #(identical? instance %) @@#'generation/failed-retirements_))
      (run! release! (vals (::generation/listener-claims instance)))
      (is (port-bindable? port))
      (is (retire! failed-pool))
      (forget-kept-generation! instance 0))))

(deftest stop-after-a-worker-failed-completes-test
  (let [port (util/free-port)
        server (runtime/start! (server-config port "x"))
        instance (active-instance server)
        logged_ (atom [])]
    (is (zero? (:exit (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2))))
    (with-redefs [trove/*log-fn* (fn [_ns _coords level id _lazy]
                                   (swap! logged_ conj [level id]))]
      (fail-idle-worker! instance 1))
    (is (= 1 (count (filter #{[:error ::generation/worker-failed]} @logged_))))
    (is (nil? (runtime/stop! server)))
    (is (= :stopped (:phase (runtime/state server))))
    (is (= :stopped (generation/phase instance)))
    (is (port-bindable? port))))

(deftest a-worker-ended-by-a-failed-retirement-logs-no-worker-failure-test
  (let [port (util/free-port)
        instance (generation/start! (config/load! (server-config port "x")) nil)
        logged_ (atom [])]
    (is (zero? (:exit (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2))))
    (is (eventually #(every? zero? (connection-counts instance))))
    (let [worker (first (::generation/workers instance))
          cause (ex-info "simulated retirement failure" {})]
      (with-redefs [trove/*log-fn* (fn [_ns _coords _level id _lazy] (swap! logged_ conj id))]
        (.set ^AtomicReference (:termination-cause_ worker) cause)
        (p/wake worker)
        (.join ^Thread (:thread worker) 5000))
      (is (identical? cause (.get ^AtomicReference (:native-failure_ worker))))
      (is (not-any? #{::generation/worker-failed} @logged_)))
    (is (nil? (generation/stop! instance 5 TimeUnit/SECONDS)))))

(deftest reload-after-a-worker-failed-activates-the-new-generation-and-retires-the-old-test
  (let [port (util/free-port)
        server (runtime/start! (server-config port "old"))]
    (try
      (is (zero? (:exit (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2))))
      (let [old (active-instance server)]
        (fail-idle-worker! old 0)
        (is (= "old" (body port)))
        (let [logged_ (atom [])]
          (with-redefs [trove/*log-fn* (fn [_ns _coords _level id _lazy] (swap! logged_ conj id))]
            (is (= :activated (runtime/reload! server (server-config port "new" :server-name "new")))))
          (is (not-any? #{::runtime/stop-accepting-failed} @logged_)
              "the reload does not wait for the failed worker to stop accepting"))
        (is (= ["new" "new" "new" "new"] (vec (repeatedly 4 #(body port)))))
        (is (eventually #(empty? (:draining @(:busker/state server)))))
        (is (= :stopped (generation/phase old)))
        (is (= :unchanged (runtime/reload! server (server-config port "new" :server-name "new")))))
      (finally
        (is (nil? (runtime/stop! server)))))
    (is (= :stopped (:phase (runtime/state server))))
    (is (port-bindable? port))))

(deftest reload-with-the-same-configuration-replaces-a-generation-with-a-failed-worker-test
  (let [port (util/free-port)
        server (runtime/start! (server-config port "same"))]
    (try
      (is (zero? (:exit (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2))))
      (let [old (active-instance server)]
        (fail-idle-worker! old 0)
        (is (= :activated (runtime/reload! server (server-config port "same"))))
        (is (not (identical? old (active-instance server))))
        (is (= :unchanged (runtime/reload! server (server-config port "same"))))
        (is (= "same" (body port)))
        (is (eventually #(= :stopped (generation/phase old)))))
      (finally
        (is (nil? (runtime/stop! server)))))))

(deftest reload-keeps-the-candidate-serving-when-the-old-generation-cannot-drain-test
  (let [port (util/free-port)
        stopped-runtimes_ (atom [])]
    (with-redefs [clave-adapter/start! (fn [plan] {:runtime-id (gensym "runtime") :managed-plan plan})
                  clave-adapter/stop! (fn [cert-runtime]
                                        (swap! stopped-runtimes_ conj (:runtime-id cert-runtime)))
                  clave-adapter/wrap-handler (fn [handler _] handler)]
      (let [server (runtime/start! (server-config port "old"))
            old (active-instance server)]
        (try
          (is (zero? (:exit (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2))))
          (with-redefs [generation/await-stop-accepting!
                        (fn [_] (throw (ex-info "simulated drain failure" {})))]
            (is (= :activated (runtime/reload! server (managed-config port "new")))))
          (let [{:keys [active]} @(:busker/state server)]
            (is (= 2 (:generation-id active)))
            (is (= :running (generation/phase (:instance active)))))
          (is (= "new" (body port)))
          (is (eventually #(empty? (:draining @(:busker/state server)))))
          (is (= :stopped (generation/phase old)))
          (is (= :unchanged (runtime/reload! server (managed-config port "new"))))
          (finally
            (is (nil? (runtime/stop! server)))))
        (is (= 1 (count @stopped-runtimes_)))
        (is (port-bindable? port))))))

(deftest stop-ends-stopped-and-reports-a-generation-it-must-keep-test
  (let [port (util/free-port)
        server (runtime/start! (server-config port "held"))
        instance (active-instance server)]
    (with-open [_ (open-served-connection! port)]
      (fail-worker! instance (.indexOf ^java.util.List (connection-counts instance) 1))
      (let [stop (fn []
                   (try
                     (runtime/stop! server)
                     nil
                     (catch ExceptionInfo e
                       (ex-message e))))]
        (is (= kept-message (stop)))
        (is (= :stopped (:phase (runtime/state server))))
        (is (= kept-message (stop)))
        (is (refuses-connections? port))
        (is (port-bindable? port))
        (forget-kept-generation! instance 1)))))

(defn- http3-config
  [plain-port tls-port body]
  (util/with-static-tls
    (server-config plain-port body
                   :entrypoints {:http {:bind (str "127.0.0.1:" plain-port) :tls false}
                                 :https {:bind (str "127.0.0.1:" tls-port) :http3? true}})))

(defn- http3-body
  [port]
  (:out (util/curl :https :h3 port "/" :max-time 3)))

(defn- http3-connections
  [instance]
  (reduce + (for [http3-ctxs (::generation/http3-worker-contexts instance)
                  http3-ctx http3-ctxs
                  :when (and http3-ctx (not (mem/null? http3-ctx)))]
              (h2o/http3-num-connections http3-ctx))))

(deftest stop-closes-the-http3-sockets-of-a-generation-it-must-keep-test
  (let [plain-port (util/free-port)
        tls-port (util/free-port)
        limit-before (h2o/conn-limit-current)
        server (runtime/start! (http3-config plain-port tls-port "held"))
        instance (active-instance server)]
    (is (zero? (:exit (util/wait-for-curl-ready! :https :h3 tls-port "/" :max-time 2))))
    (is (eventually #(every? zero? (connection-counts instance))))
    (is (eventually #(zero? (http3-connections instance)))
        "no QUIC connection is left for the failed worker to keep")
    (with-open [_ (open-served-connection! plain-port)]
      (fail-worker! instance (.indexOf ^java.util.List (connection-counts instance) 1))
      (let [stop (fn []
                   (try
                     (runtime/stop! server)
                     nil
                     (catch ExceptionInfo e
                       (ex-message e))))]
        (is (= kept-message (stop)))
        (is (= kept-message (stop))))
      (let [[result ms] (elapsed-ms #(util/curl :https :h3 tls-port "/" :max-time 3))]
        (is (not (zero? (:exit result))))
        (is (< ms 2000) "HTTP/3 to the port fails at once and does not time out"))
      (is (datagram-bindable? tls-port))
      (let [second-server (runtime/start! (http3-config plain-port tls-port "second"))]
        (try
          (is (= (repeat 12 "second") (repeatedly 12 #(http3-body tls-port))))
          (finally
            (is (nil? (runtime/stop! second-server))))))
      (forget-kept-generation! instance 1))
    (is (= limit-before (h2o/conn-limit-current)))))

(defn- udp-transport-address
  [instance port]
  (some (fn [[claim-key claim]]
          (when (= {:transport :udp :port port} (select-keys claim-key [:transport :port]))
            (.address ^java.lang.foreign.MemorySegment (listen/resource claim))))
        (::generation/listener-claims instance)))

(deftest reload-keeps-http3-answering-when-the-old-generation-must-be-kept-test
  (let [plain-port (util/free-port)
        tls-port (util/free-port)
        limit-before (h2o/conn-limit-current)
        server (runtime/start! (http3-config plain-port tls-port "held"))
        old (active-instance server)
        stop (fn []
               (try
                 (runtime/stop! server)
                 nil
                 (catch ExceptionInfo e
                   (ex-message e))))]
    (is (zero? (:exit (util/wait-for-curl-ready! :https :h3 tls-port "/" :max-time 2))))
    (is (eventually #(every? zero? (connection-counts old))))
    (is (eventually #(zero? (http3-connections old)))
        "no QUIC connection is left for the failed worker to keep")
    (with-open [_ (open-served-connection! plain-port)]
      (fail-worker! old (.indexOf ^java.util.List (connection-counts old) 1))
      (is (= :activated (runtime/reload! server (http3-config plain-port tls-port "new"))))
      (let [new (active-instance server)]
        (is (= (udp-transport-address old tls-port) (udp-transport-address new tls-port))
            "the new generation uses the UDP transport of the old one")
        (is (eventually #(= :kept (generation/phase old))))
        (is (= (repeat 12 "new") (repeatedly 12 #(http3-body tls-port))))
        (is (= "new" (body plain-port))))
      (is (= kept-message (stop)))
      (is (= :stopped (:phase (runtime/state server))))
      (is (datagram-bindable? tls-port)))
    (forget-kept-generation! old 1)
    (is (= limit-before (h2o/conn-limit-current)))))
