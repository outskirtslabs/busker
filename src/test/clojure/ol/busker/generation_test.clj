(ns ol.busker.generation-test
  (:require
   [clojure.test :refer [deftest is]]
   [coffi.mem :as mem]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.config :as config]
   [ol.busker.evloop :as evloop]
   [ol.busker.generation :as generation]
   [ol.busker.protocols :as protocols]
   [ol.busker.request :as request]
   [ol.busker.response-queue :as response-queue]
   [ol.busker.test-utils :as util])
  (:import
   [java.net InetSocketAddress Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent AbstractExecutorService]
   [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

(defn- connect
  [port]
  (doto (Socket.)
    (.connect (InetSocketAddress. "127.0.0.1" (int port)) 2000)))

(defn- request-stream!
  [^Socket socket path]
  (let [request (.getBytes (str "GET " path " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
                           StandardCharsets/US_ASCII)
        out (.getOutputStream socket)]
    (.write out request)
    (.flush out)))

(defn- read-until!
  [^Socket socket ^String expected]
  (let [input (.getInputStream socket)
        response (StringBuilder.)]
    (loop []
      (if (not= -1 (.indexOf response expected))
        (str response)
        (let [b (.read input)]
          (when (neg? b)
            (throw (ex-info "Server closed before sending expected stream data"
                            {:response (str response)
                             :expected expected})))
          (.append response (char b))
          (recur))))))

(deftest generation-starts-and-stops-without-runtime-bridge-test
  (let [port 18584
        compiled-config
        (config/load!
         {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                               :tls false}}
          :dispatch [{:handler (fn [_]
                                 {:status 200
                                  :body "generation-ok"})}]})
        instance (generation/start! compiled-config nil)]
    (try
      (let [result (util/curl :http nil port "/" :max-time 5)]
        (is (= 0 (:exit result))
            (str "The generation should serve requests directly. stderr: "
                 (:err result)))
        (is (= "generation-ok" (:out result))))
      (finally
        (generation/stop! instance)))))

(deftest failed-worker-retirement-retains-the-callback-arena-test
  (with-open [arena (mem/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          worker {:callback-dispatch dispatch
                  :wakeup-receiver_ (AtomicReference.)}
          phase (atom :running)
          generation-state {::generation/phase phase
                            ::generation/stop-lock (Object.)
                            ::generation/workers [worker]
                            ::generation/wakeup-receivers [nil]
                            ::generation/listener-runtimes []
                            ::generation/listener-claims {}
                            ::generation/config {}
                            ::generation/arena arena}]
      (try
        (with-redefs [evloop/broadcast-wake! (constantly nil)
                      evloop/join-all! (fn [_]
                                         (throw (InterruptedException. "simulated")))]
          (generation/stop! generation-state))
        (is (= :retirement-failed @phase))
        (is (= 1 (.byteSize (mem/alloc 1 arena))))
        (finally
          (swap! @#'generation/failed-retirements_
                 (fn [retirements]
                   (vec (remove #(identical? generation-state %) retirements)))))))))

(deftest successful-retirement-retry-releases-retained-generation-test
  (let [arena (mem/shared-arena)
        dispatch (callback-dispatch/create arena)
        worker {:callback-dispatch dispatch
                :wakeup-receiver_ (AtomicReference.)
                :thread (Thread.)}
        phase (atom :running)
        generation-state {::generation/phase phase
                          ::generation/stop-lock (Object.)
                          ::generation/workers [worker]
                          ::generation/wakeup-receivers [nil]
                          ::generation/listener-runtimes []
                          ::generation/listener-claims {}
                          ::generation/config {}
                          ::generation/arena arena}
        attempts (atom 0)]
    (try
      (with-redefs [evloop/broadcast-wake! (constantly nil)
                    evloop/join-all! (fn [_]
                                       (when (= 1 (swap! attempts inc))
                                         (throw (InterruptedException. "simulated"))))]
        (generation/stop! generation-state)
        (generation/stop! generation-state))
      (is (= :stopped @phase))
      (is (not (some #(identical? generation-state %)
                     @@#'generation/failed-retirements_)))
      (finally
        (swap! @#'generation/failed-retirements_
               (fn [retirements]
                 (vec (remove #(identical? generation-state %) retirements))))))))

(deftest native-callbacks-deliver-and-retire-request-state-test
  (let [port 18585
        instance
        (generation/start!
         (config/load!
          {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                :tls false}}
           :dispatch [{:handler (fn [request]
                                  {:status 200
                                   :body (slurp (:body request))})}]})
         nil)
        dispatches (mapv :callback-dispatch (::generation/workers instance))]
    (try
      (let [result (util/curl :http nil port "/"
                              :max-time 5
                              :args ["--data-binary" "callback-body"])]
        (is (= 0 (:exit result)) (:err result))
        (is (= "callback-body" (:out result)))
        (is (loop [remaining 100]
              (if (pos? (reduce + (map #(get (callback-dispatch/diagnostics %) :retired)
                                       dispatches)))
                true
                (when (pos? remaining)
                  (Thread/sleep 10)
                  (recur (dec remaining)))))))
      (finally
        (generation/stop! instance)))))

(deftest native-callback-lifecycle-runs-on-worker-in-order-test
  (let [port 18588
        events_ (atom [])
        case_ (atom :normal)
        cleanup?_ (AtomicBoolean. false)
        normal-proceed_ (promise)
        normal-cleanup_ (promise)
        active-stop_ (promise)
        active-cleanup_ (promise)
        probe-after-disconnect_ (promise)
        release-active_ (promise)
        create-write-req-channel request/create-write-req-channel
        on-proceed response-queue/on-proceed
        on-stop response-queue/on-stop
        on-request-cleanup request/on-request-cleanup
        record! (fn [event]
                  (let [entry {:case   @case_
                               :event  event
                               :thread (Thread/currentThread)
                               :worker (evloop/get-current-worker)}]
                    (swap! events_ conj entry)
                    (when (= :proceed event)
                      (case @case_
                        :normal (deliver normal-proceed_ true)
                        nil))
                    (when (= :stop event)
                      (case @case_
                        :active (deliver active-stop_ true)
                        nil))
                    (when (= :cleanup event)
                      (case @case_
                        :normal (deliver normal-cleanup_ true)
                        :active (deliver active-cleanup_ true)
                        nil))))]
    (with-redefs [request/create-write-req-channel
                  (fn [proceed-callback]
                    (update (create-write-req-channel proceed-callback)
                            :write-chunk
                            (fn [write-chunk]
                              (fn [chunk is-last]
                                (record! :body)
                                (write-chunk chunk is-last)))))
                  response-queue/on-proceed
                  (fn [st]
                    (record! :proceed)
                    (on-proceed st))
                  response-queue/on-stop
                  (fn [st reason]
                    (when-not (.get cleanup?_)
                      (record! :stop))
                    (on-stop st reason))
                  request/on-request-cleanup
                  (fn [module-id request-seq]
                    (.set cleanup?_ true)
                    (try
                      (on-request-cleanup module-id request-seq)
                      (finally
                        (record! :cleanup)
                        (.set cleanup?_ false))))]
      (let [instance
            (generation/start!
             (config/load!
              {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                    :tls false}}
               :dispatch [{:handler (fn [{:keys [uri] :as ring-request}]
                                      (if (= "/active" uri)
                                        (let [emitter (:ol.busker.request/emitter ring-request)]
                                          (Thread/startVirtualThread
                                           (fn []
                                             (protocols/emit! emitter {:status 200
                                                                       :headers {"content-type" "text/event-stream"}})
                                             (protocols/emit! emitter "event: ready\n\n")
                                             (protocols/flush emitter)
                                             @probe-after-disconnect_
                                             (try
                                               (protocols/emit! emitter "event: probe\n\n")
                                               (protocols/flush emitter)
                                               (catch Exception _
                                                 nil))
                                             @release-active_
                                             (when (protocols/open? emitter)
                                               (protocols/close emitter))))
                                          {:body emitter})
                                        (let [body (slurp (:body ring-request))
                                              emitter (:ol.busker.request/emitter ring-request)]
                                          (Thread/startVirtualThread
                                           (fn []
                                             (protocols/emit! emitter {:status 200 :headers {}})
                                             (protocols/emit! emitter (subs body 0 6))
                                             (protocols/flush emitter)
                                             @normal-proceed_
                                             (protocols/emit! emitter (subs body 6))
                                             (protocols/close emitter)))
                                          {:body emitter})))}]})
             nil)]
        (try
          (let [result (util/curl :http :h1 port "/normal"
                                  :max-time 5
                                  :args ["-X" "POST"
                                         "--data-binary" "trace-body"])]
            (is (= {:exit 0
                    :out "trace-body"}
                   (select-keys result [:exit :out]))
                (:err result))
            (is (true? (deref normal-cleanup_ 5000 false))))
          (let [normal-events (vec (filter #(= :normal (:case %)) @events_))
                proceed-index (some (fn [[index entry]]
                                      (when (= :proceed (:event entry)) index))
                                    (map-indexed vector normal-events))]
            (is (= {:proceed?          true
                    :stop?             false
                    :cleanup-last?     true
                    :worker-affinity?  true}
                   {:proceed?         (some? proceed-index)
                    :stop?            (boolean (some #(= :stop (:event %)) normal-events))
                    :cleanup-last?    (= :cleanup (:event (last normal-events)))
                    :worker-affinity? (every? (fn [{:keys [thread worker]}]
                                                (and worker
                                                     (identical? thread (:thread worker))))
                                              normal-events)})))
          (reset! case_ :active)
          (with-open [^Socket socket (doto (connect port)
                                       (.setSoTimeout 2000)
                                       (.setSoLinger true 0))]
            (request-stream! socket "/active")
            (read-until! socket "event: ready\n\n"))
          (deliver probe-after-disconnect_ true)
          (is (true? (deref active-stop_ 5000 false)))
          (is (true? (deref active-cleanup_ 5000 false)))
          (let [active-events (vec (filter #(= :active (:case %)) @events_))
                stop-index (some (fn [[index entry]]
                                   (when (= :stop (:event entry)) index))
                                 (map-indexed vector active-events))
                cleanup-index (some (fn [[index entry]]
                                      (when (= :cleanup (:event entry)) index))
                                    (map-indexed vector active-events))]
            (is (= {:stop-before-cleanup? true
                    :cleanup-last?        true
                    :worker-affinity?     true}
                   {:stop-before-cleanup? (and (some? stop-index)
                                               (some? cleanup-index)
                                               (< stop-index cleanup-index))
                    :cleanup-last?        (= :cleanup (:event (last active-events)))
                    :worker-affinity?     (every? (fn [{:keys [thread worker]}]
                                                    (and worker
                                                         (identical? thread (:thread worker))))
                                                  active-events)})))
          (finally
            (deliver release-active_ true)
            (generation/stop! instance)))))))

(deftest repeated-start-stop-releases-native-callback-state-test
  (doseq [[port request-body] [[18586 "first-cycle"]
                               [18587 "second-cycle"]]]
    (let [instance
          (generation/start!
           (config/load!
            {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                  :tls false}}
             :dispatch [{:handler (fn [request]
                                    {:status 200
                                     :body (slurp (:body request))})}]})
           nil)]
      (try
        (let [result (util/curl :http nil port "/"
                                :max-time 5
                                :args ["--data-binary" request-body])]
          (is (= 0 (:exit result)) (:err result))
          (is (= request-body (:out result))))
        (finally
          (generation/stop! instance))))))

(deftest interrupted-executor-retirement-can-retry-test
  (let [attempts (atom 0)
        executor (proxy [AbstractExecutorService] []
                   (execute [task]
                     (.run ^Runnable task))
                   (shutdown [] nil)
                   (shutdownNow [] [])
                   (isShutdown [] true)
                   (isTerminated [] true)
                   (awaitTermination [_timeout _timeunit]
                     (if (= 1 (swap! attempts inc))
                       (throw (InterruptedException. "simulated"))
                       true)))
        phase (atom :running)
        generation-state {::generation/phase phase
                          ::generation/stop-lock (Object.)
                          ::generation/executor executor
                          ::generation/workers []
                          ::generation/wakeup-receivers []
                          ::generation/listener-runtimes []
                          ::generation/listener-claims {}
                          ::generation/config {}}]
    (is (thrown? InterruptedException (generation/stop! generation-state)))
    (is (= :stopping @phase))
    (generation/stop! generation-state)
    (is (= :stopped @phase))))
