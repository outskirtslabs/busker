(ns ol.busker.failed-worker-test
  "Reload, drain and stop after a worker's event loop has failed."
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.config :as config]
   [ol.busker.generation :as generation]
   [ol.busker.internal.protocols :as p]
   [ol.busker.listen :as listen]
   [ol.busker.native :as h2o]
   [ol.busker.runtime :as runtime]
   [ol.busker.test-utils :as util]
   [taoensso.trove :as trove])
  (:import
   [clojure.lang ExceptionInfo]
   [java.net ConnectException InetSocketAddress ServerSocket Socket]
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
        update-listener-state! @#'generation/update-listener-state!]
    (with-redefs-fn {#'generation/update-listener-state!
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
  "Worker threads ended with connections, responses or blocks nothing can finish. TCP and Unix listeners closed. Loops, contexts, HTTP/3 UDP sockets and memory kept.")

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
    (is (= :stopped @(::generation/phase instance)))
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
        (is (= :retirement-failed @(::generation/phase instance)))
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
      (is (= :retirement-failed @(::generation/phase instance)))
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
      (is (= :retirement-failed @(::generation/phase instance)))
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
    (is (= :stopped @(::generation/phase instance)))
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
