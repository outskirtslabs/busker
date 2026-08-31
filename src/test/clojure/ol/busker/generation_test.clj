(ns ol.busker.generation-test
  (:require
   [clojure.test :refer [deftest is]]
   [coffi.mem :as mem]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.config :as config]
   [ol.busker.evloop :as evloop]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.generation :as generation]
   [ol.busker.internal.protocols :as p]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as protocols]
   [ol.busker.request :as request]
   [ol.busker.response-queue :as response-queue]
   [ol.busker.wake-notifier :as wake-notifier]
   [ol.busker.test-utils :as util])
  (:import
   [java.lang.foreign Arena]
   [java.net InetSocketAddress Socket]
   [java.nio.charset StandardCharsets]
   [ol.busker.fixed_final DirectResponsePlan]
   [java.util.concurrent AbstractExecutorService]
   [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

(defrecord RetirementWorker
           [callback-dispatch wakeup-receiver_ thread send-fn]
  p/WorkerThread
  (running? [_] true)
  (wake [_] true)
  (send-msg [_ msg] (send-fn msg))
  (send-required-msg [_ msg] (send-fn msg))
  (count-msgs [_] 0)
  (add-req [_ _ _] nil)
  (reap-req [_ _] nil))

(defn- direct-plan
  [module-id request-seq ^bytes body]
  (DirectResponsePlan. (long module-id) (long request-seq) 200 [] 0 body
                       (alength body) (alength body) 0))
(defn- connect
  [port]
  (doto (Socket.)
    (.connect (InetSocketAddress. "127.0.0.1" (int port)) 2000)))

(def ^:private response-claim-index-mask 0xffff)

(defn- response-claim-index
  [claim-handle]
  (dec (bit-and claim-handle response-claim-index-mask)))

(defn- response-claim-slot
  [worker claim-handle]
  (nth (:response-slots worker) (response-claim-index claim-handle)))

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

(deftest mailbox-work-forces-a-nonblocking-native-iteration
  (let [wait-ms_ (atom nil)
        worker (evloop/map->Worker
                {:mailbox (java.util.concurrent.ArrayBlockingQueue. 1)
                 :response-slots []
                 :response-slot-payload-capacity 0
                 :callback-dispatch nil})
        loop-state {::evloop/mailbox-work? true}
        native-state {:loop-ptr :loop
                      :ctx-ptr :context
                      :listener-socks []
                      :accept-callbacks []
                      :max-connections 0}]
    (with-redefs-fn
      {#'generation/check-and-initiate-shutdown! (fn [state _] state)
       #'generation/update-receiver-destruction (fn [state _] state)
       #'generation/dispose-context-if-ready (fn [state _ _] state)
       #'callback-dispatch/pending-response-work? (constantly false)
       #'generation/update-listener-state! (fn [& _])
       #'h2o/evloop-now (constantly 0)
       #'h2o/cleanup-thread (constantly 1000)
       #'h2o/evloop-run (fn [_ wait-ms]
                          (reset! wait-ms_ wait-ms)
                          0)}
      #(let [result (#'generation/worker-loop worker loop-state native-state)]
         (is (= 0 @wait-ms_))
         (is (not (contains? result ::evloop/mailbox-work?)))))))

(deftest response-ring-work-forces-a-nonblocking-native-iteration
  (let [wait-ms_ (atom nil)
        receiver (Object.)
        worker (evloop/map->Worker
                {:mailbox (java.util.concurrent.ArrayBlockingQueue. 1)
                 :callback-dispatch nil
                 :response-slots []
                 :response-slot-payload-capacity 0
                 :response-receiver_ (AtomicReference. receiver)})
        native-state {:loop-ptr :loop
                      :ctx-ptr :context
                      :listener-socks []
                      :accept-callbacks []
                      :max-connections 0}]
    (with-redefs-fn
      {#'generation/check-and-initiate-shutdown! (fn [state _] state)
       #'generation/update-receiver-destruction (fn [state _] state)
       #'generation/dispose-context-if-ready (fn [state _ _] state)
       #'callback-dispatch/pending-response-work? (constantly false)
       #'generation/update-listener-state! (fn [& _])
       #'h2o/mt-response-ring-drain
       (fn [actual]
         (is (identical? receiver actual))
         1)
       #'h2o/evloop-now (constantly 0)
       #'h2o/cleanup-thread (constantly 1000)
       #'h2o/evloop-run (fn [_ wait-ms]
                          (reset! wait-ms_ wait-ms)
                          0)}
      #(let [result (#'generation/worker-loop worker {} native-state)]
         (is (= 0 @wait-ms_))
         (is (= {} result))))))
(deftest wake-receiver-retires-on-event-loop-worker-test
  (let [events_ (atom [])
        receiver (Object.)
        receiver_ (AtomicReference. receiver)
        worker {:wake-endpoint ::endpoint
                :wakeup-receiver_ receiver_}]
    (.set evloop/worker-context worker)
    (try
      (with-redefs [wake-notifier/quiesce-endpoint!
                    (fn [endpoint]
                      (is (= ::endpoint endpoint))
                      (swap! events_ conj :quiesce))
                    h2o/mt-destroy-wakeup-receiver
                    (fn [actual-receiver]
                      (is (identical? receiver actual-receiver))
                      (is (false? (.isVirtual (Thread/currentThread))))
                      (swap! events_ conj :destroy))]
        (generation/evloop-msg-processor :h2o/retire-wakeup-receiver []))
      (is (= [:quiesce :destroy] @events_))
      (is (nil? (.get receiver_)))
      (finally
        (.remove evloop/worker-context)))))
(deftest startup-receiver-without-worker-retires-on-lifecycle-platform-test
  (let [wakeup-receiver (Object.)
        response-receiver (Object.)
        destroyed_ (atom {:wakeup [] :response []})
        phase (atom :running)
        generation-state {::generation/phase phase
                          ::generation/stop-lock (Object.)
                          ::generation/workers []
                          ::generation/wakeup-receivers [wakeup-receiver]
                          ::generation/response-receivers [response-receiver]
                          ::generation/listener-runtimes []
                          ::generation/listener-claims {}
                          ::generation/config {}}]
    (with-redefs [h2o/mt-destroy-wakeup-receiver
                  (fn [receiver]
                    (swap! destroyed_ update :wakeup conj receiver))
                  h2o/mt-destroy-response-receiver
                  (fn [receiver]
                    (swap! destroyed_ update :response conj receiver))]
      (generation/stop! generation-state))
    (is (= {:wakeup [wakeup-receiver]
            :response [response-receiver]}
           @destroyed_))
    (is (= :stopped @phase))))

(deftest response-ring-claim-is-bounded-and-reusable-from-a-virtual-thread
  (let [port (util/free-port)
        instance (generation/start!
                  (config/load!
                   {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                         :tls false}}
                    :dispatch [{:handler (constantly {:status 200 :body "ring-ready"})}]})
                  nil)]
    (try
      (let [worker (first (::generation/workers instance))
            receiver (.get ^AtomicReference (:response-receiver_ worker))
            result_ (promise)
            thread (Thread/startVirtualThread
                    #(let [capacity (h2o/mt-response-ring-capacity receiver)
                           handles (mapv (fn [_] (h2o/mt-response-try-claim receiver))
                                         (range capacity))
                           overflow (h2o/mt-response-try-claim receiver)
                           fallback (fixed-final/try-publish-direct-response!
                                     worker (direct-plan 999 999 (byte-array [120])))
                           aborted (mapv (fn [claim-handle]
                                           (h2o/mt-response-abort receiver claim-handle))
                                         handles)
                           reused (h2o/mt-response-try-claim receiver)]
                       (deliver result_
                                {:virtual? (.isVirtual (Thread/currentThread))
                                 :capacity capacity
                                 :unique-slots (count (distinct (map response-claim-index handles)))
                                 :all-slots? (every? pos? handles)
                                 :overflow? (zero? overflow)
                                 :fallback fallback
                                 :aborted aborted
                                 :reused? (not (zero? reused))})
                       (h2o/mt-response-abort receiver reused)))]
        (.join thread 5000)
        (is (= {:virtual? true
                :capacity 256
                :unique-slots 256
                :all-slots? true
                :overflow? true
                :fallback :overloaded
                :aborted (vec (repeat 256 1))
                :reused? true}
               (deref result_ 1000 :timeout)))
        (is (zero? (h2o/mt-response-claimed receiver)))
        (is (zero? (h2o/mt-response-ring-ready receiver))))
      (finally
        (generation/stop! instance)))))

(deftest response-ring-storage-does-not-scale-past-the-eligible-body-limit
  (let [port (util/free-port)
        instance
        (generation/start!
         (config/load!
          {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                :tls false}}
           :output-buffer-size (* 4 fixed-final/response-ring-max-body-bytes)
           :dispatch [{:handler (constantly {:status 200 :body "ring-ready"})}]})
         nil)]
    (try
      (let [worker (first (::generation/workers instance))
            receiver (.get ^AtomicReference (:response-receiver_ worker))]
        (is (= (inc (+ fixed-final/max-header-staging-bytes
                       fixed-final/response-ring-max-body-bytes))
               (h2o/mt-response-slot-payload-capacity receiver))))
      (finally
        (generation/stop! instance)))))

(deftest response-ring-claims-scalar-handles-for-prebound-slots
  (let [port (util/free-port)
        instance
        (generation/start!
         (config/load!
          {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                :tls false}}
           :dispatch [{:handler (constantly {:status 200 :body "ring-ready"})}]})
         nil)
        worker (first (::generation/workers instance))
        receiver (.get ^AtomicReference (:response-receiver_ worker))
        claim (h2o/mt-response-try-claim receiver)]
    (try
      (is (= {:scalar-handle? true
              :positive? true
              :slot-count 256
              :payload-capacity (+ fixed-final/max-header-staging-bytes
                                   fixed-final/response-ring-max-body-bytes)}
             {:scalar-handle? (integer? claim)
              :positive? (and (integer? claim) (pos? claim))
              :slot-count (count (:response-slots worker))
              :payload-capacity (:response-slot-payload-capacity worker)}))
      (finally
        (h2o/mt-response-abort receiver claim)
        (generation/stop! instance)))))

(deftest prebound-response-slot-views-expire-with-the-generation-arena
  (let [port (util/free-port)
        instance
        (generation/start!
         (config/load!
          {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                :tls false}}
           :dispatch [{:handler (constantly {:status 200 :body "ring-ready"})}]})
         nil)
        worker (first (::generation/workers instance))
        slot (first (:response-slots worker))]
    (generation/stop! instance)
    (is (thrown? IllegalStateException (mem/read-long slot)))))

(deftest stale-ring-response-is-drained-after-a-platform-notifier-wake
  (let [port (util/free-port)
        instance (generation/start!
                  (config/load!
                   {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                         :tls false}}
                    :dispatch [{:handler (constantly {:status 200 :body "ring-ready"})}]})
                  nil)]
    (try
      (let [worker (first (::generation/workers instance))
            receiver (.get ^AtomicReference (:response-receiver_ worker))
            body (.getBytes "stale" StandardCharsets/UTF_8)
            result_ (promise)
            thread (Thread/startVirtualThread
                    #(deliver result_
                              {:result (fixed-final/try-publish-direct-response!
                                        worker (direct-plan 999 999 body))
                               :virtual? (.isVirtual (Thread/currentThread))}))]
        (.join thread 5000)
        (is (= {:result :accepted :virtual? true}
               (deref result_ 1000 :timeout)))
        (loop [remaining 200]
          (when (and (pos? remaining)
                     (pos? (h2o/mt-response-ring-ready receiver)))
            (Thread/sleep (long 10))
            (recur (dec remaining))))
        (is (zero? (h2o/mt-response-ring-ready receiver)))
        (is (zero? (h2o/mt-response-claimed receiver))))
      (finally
        (generation/stop! instance)))))

(deftest response-ring-rejects-invalid-and-duplicate-slot-transitions
  (let [port (util/free-port)
        instance (generation/start!
                  (config/load!
                   {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                         :tls false}}
                    :dispatch [{:handler (constantly {:status 200 :body "ring-ready"})}]})
                  nil)]
    (try
      (let [worker (first (::generation/workers instance))
            receiver (.get ^AtomicReference (:response-receiver_ worker))
            invalid-handle (h2o/mt-response-try-claim receiver)]
        (is (zero? (h2o/mt-response-publish receiver invalid-handle)))
        (is (= 1 (h2o/mt-response-abort receiver invalid-handle)))
        (is (zero? (h2o/mt-response-abort receiver invalid-handle)))
        (dotimes [_ 255]
          (let [claim-handle (h2o/mt-response-try-claim receiver)]
            (h2o/mt-response-abort receiver claim-handle)))
        (let [body (.getBytes "ready" StandardCharsets/UTF_8)
              claim-handle (h2o/mt-response-try-claim receiver)
              slot (response-claim-slot worker claim-handle)]
          (fixed-final/write-direct-response-slot!
           slot (:response-slot-payload-capacity worker)
           (direct-plan 999 999 body))
          (is (= (response-claim-index invalid-handle)
                 (response-claim-index claim-handle)))
          (is (zero? (h2o/mt-response-publish receiver invalid-handle)))
          (is (zero? (h2o/mt-response-abort receiver invalid-handle)))
          (is (zero? (h2o/mt-response-publish receiver (inc claim-handle))))
          (is (zero? (h2o/mt-response-abort receiver (inc claim-handle))))
          (is (= 1 (h2o/mt-response-publish receiver claim-handle)))
          (is (zero? (h2o/mt-response-publish receiver claim-handle)))
          (is (zero? (h2o/mt-response-abort receiver claim-handle)))
          (p/wake worker)
          (loop [remaining 200]
            (when (and (pos? remaining)
                       (pos? (h2o/mt-response-ring-ready receiver)))
              (Thread/sleep (long 10))
              (recur (dec remaining))))
          (is (zero? (h2o/mt-response-ring-ready receiver)))
          (dotimes [_ 255]
            (let [intermediate (h2o/mt-response-try-claim receiver)]
              (h2o/mt-response-abort receiver intermediate)))
          (let [unwritten-reuse (h2o/mt-response-try-claim receiver)]
            (is (= (response-claim-index claim-handle)
                   (response-claim-index unwritten-reuse)))
            (is (zero? (h2o/mt-response-publish receiver unwritten-reuse)))
            (is (= 1 (h2o/mt-response-abort receiver unwritten-reuse))))
          (is (zero? (h2o/mt-response-claimed receiver)))))
      (finally
        (generation/stop! instance)))))

(deftest response-ring-claim-and-abort-remain-bounded-under-contention
  (let [port (util/free-port)
        instance (generation/start!
                  (config/load!
                   {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                         :tls false}}
                    :dispatch [{:handler (constantly {:status 200 :body "ring-ready"})}]})
                  nil)]
    (try
      (let [worker (first (::generation/workers instance))
            receiver (.get ^AtomicReference (:response-receiver_ worker))
            start (promise)
            first-result (promise)
            second-result (promise)
            run-claims
            (fn [result]
              @start
              (let [started (System/nanoTime)
                    counts
                    (loop [remaining 10000
                           accepted 0
                           unavailable 0]
                      (if (zero? remaining)
                        {:accepted accepted :unavailable unavailable}
                        (let [claim-handle (h2o/mt-response-try-claim receiver)]
                          (if (zero? claim-handle)
                            (recur (dec remaining) accepted (inc unavailable))
                            (do
                              (h2o/mt-response-abort receiver claim-handle)
                              (recur (dec remaining) (inc accepted) unavailable))))))]
                (deliver result (assoc counts :elapsed-nanos (- (System/nanoTime) started)))))
            first-thread (Thread/startVirtualThread (fn [] (run-claims first-result)))
            second-thread (Thread/startVirtualThread (fn [] (run-claims second-result)))]
        (deliver start true)
        (.join first-thread 10000)
        (.join second-thread 10000)
        (let [results [(deref first-result 1000 :timeout)
                       (deref second-result 1000 :timeout)]]
          (is (every? map? results))
          (is (every? (fn [{:keys [accepted unavailable]}]
                        (= 10000 (+ accepted unavailable)))
                      results))
          (is (every? (fn [{:keys [elapsed-nanos]}]
                        (< elapsed-nanos 10000000000))
                      results)))
        (is (zero? (h2o/mt-response-claimed receiver)))
        (is (zero? (h2o/mt-response-ring-ready receiver))))
      (finally
        (generation/stop! instance)))))

(deftest generation-stop-completes-after-response-slot-publication
  (let [port (util/free-port)
        instance (generation/start!
                  (config/load!
                   {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                         :tls false}}
                    :dispatch [{:handler (constantly {:status 200 :body "ring-ready"})}]})
                  nil)
        stopped?_ (atom false)]
    (try
      (let [worker (first (::generation/workers instance))
            receiver (.get ^AtomicReference (:response-receiver_ worker))
            body (.getBytes "ready" StandardCharsets/UTF_8)
            claim-handle (h2o/mt-response-try-claim receiver)
            slot (response-claim-slot worker claim-handle)]
        (fixed-final/write-direct-response-slot!
         slot (:response-slot-payload-capacity worker)
         (direct-plan 999 999 body))
        (is (= 1 (h2o/mt-response-publish receiver claim-handle)))
        (generation/stop! instance)
        (reset! stopped?_ true)
        (is (= :stopped @(::generation/phase instance))))
      (finally
        (when-not @stopped?_
          (generation/stop! instance))))))
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

(deftest ordinary-fixed-response-uses-ring-without-the-platform-dispatcher
  (let [port 18594
        claim-threads_ (atom [])
        try-claim h2o/mt-response-try-claim]
    (with-redefs [h2o/mt-response-try-claim
                  (fn [receiver]
                    (swap! claim-threads_ conj (Thread/currentThread))
                    (try-claim receiver))]
      (let [instance
            (generation/start!
             (config/load!
              {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                    :tls false}}
               :dispatch [{:handler (fn [_]
                                      {:status 200
                                       :body "ring-submit"})}]})
             nil)]
        (try
          (let [result (util/curl :http nil port "/" :max-time 5)
                workers (::generation/workers instance)]
            (is (= {:response {:exit 0 :out "ring-submit"}
                    :claim? true
                    :all-claims-virtual? true
                    :old-submit-binding? false
                    :dispatcher-fields 0}
                   {:response (select-keys result [:exit :out])
                    :claim? (boolean (seq @claim-threads_))
                    :all-claims-virtual?
                    (every? (fn [^Thread thread] (.isVirtual thread))
                            @claim-threads_)
                    :old-submit-binding?
                    (boolean (ns-resolve 'ol.busker.native 'mt-submit-fixed-final))
                    :dispatcher-fields
                    (count (filter #(contains? % :fixed-final-dispatcher) workers))})))
          (finally
            (generation/stop! instance)))))))

(deftest ordinary-fixed-response-falls-back-when-the-ring-is-full
  (let [port (util/free-port)
        instance
        (generation/start!
         (config/load!
          {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                :tls false}}
           :dispatch [{:handler (constantly {:status 200 :body "ring-fallback"})}]})
         nil)
        worker (first (::generation/workers instance))
        receiver (.get ^AtomicReference (:response-receiver_ worker))
        handles (mapv (fn [_] (h2o/mt-response-try-claim receiver))
                      (range (h2o/mt-response-ring-capacity receiver)))]
    (try
      (is (every? pos? handles))
      (let [result (util/curl :http nil port "/" :max-time 5)]
        (is (= {:exit 0 :out "ring-fallback"}
               (select-keys result [:exit :out]))))
      (finally
        (doseq [claim-handle handles]
          (h2o/mt-response-abort receiver claim-handle))
        (generation/stop! instance)))))

(deftest worker-receiver-retirement-precedes-notifier-stop-test
  (with-open [arena (mem/shared-arena)]
    (let [events_ (atom [])
          dispatch (callback-dispatch/create arena)
          receiver (Object.)
          notifier (Object.)
          receiver_ (AtomicReference. receiver)
          worker (->RetirementWorker
                  dispatch receiver_ (Thread.)
                  (fn [msg]
                    (is (= [:h2o/retire-wakeup-receiver] msg))
                    (swap! events_ conj :receiver-destroy)
                    (.set receiver_ nil)
                    :accepted))
          phase (atom :running)
          generation-state {::generation/phase phase
                            ::generation/stop-lock (Object.)
                            ::generation/workers [worker]
                            ::generation/wake-notifier notifier
                            ::generation/wakeup-receivers [receiver]
                            ::generation/listener-runtimes []
                            ::generation/listener-claims {}
                            ::generation/config {}}]
      (with-redefs [evloop/broadcast-wake! (fn [_] (swap! events_ conj :broadcast))
                    wake-notifier/stop-and-join! (fn [_]
                                                   (swap! events_ conj :notifier-stop)
                                                   true)
                    evloop/join-all! (fn [_] (swap! events_ conj :join))]
        (generation/stop! generation-state))
      (is (= {:retirement-order [:receiver-destroy :join :notifier-stop]
              :phase :stopped
              :receiver nil}
             {:retirement-order (filterv #{:notifier-stop :receiver-destroy :join} @events_)
              :phase @phase
              :receiver (.get receiver_)})))))

(deftest failed-worker-retirement-retains-the-callback-arena-test
  (with-open [arena (mem/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          worker (->RetirementWorker dispatch (AtomicReference.) nil (constantly :accepted))
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
          (Thread/interrupted)
          (swap! @#'generation/failed-retirements_
                 (fn [retirements]
                   (vec (remove #(identical? generation-state %) retirements)))))))))

(deftest successful-retirement-retry-releases-retained-generation-test
  (let [arena (mem/shared-arena)
        dispatch (callback-dispatch/create arena)
        worker (->RetirementWorker dispatch (AtomicReference.) (Thread.) (constantly :accepted))
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
        (Thread/interrupted)
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

(deftest request-copy-uses-generation-arena-on-worker-test
  (let [port 18596
        observed_ (promise)
        copy-request-context h2o/copy-request-context]
    (with-redefs [h2o/copy-request-context
                  (fn [ctx-ptr & [arena]]
                    (deliver observed_
                             {:arena arena
                              :active? (boolean (some-> ^Arena arena .scope .isAlive))
                              :virtual? (.isVirtual (Thread/currentThread))
                              :worker? (some? (evloop/get-current-worker))})
                    (if arena
                      (copy-request-context ctx-ptr arena)
                      (copy-request-context ctx-ptr)))]
      (let [instance
            (generation/start!
             (config/load!
              {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                    :tls false}}
               :dispatch [{:handler (fn [_]
                                      {:status 200
                                       :body "generation-arena"})}]})
             nil)
            ^Arena generation-arena (::generation/arena instance)]
        (try
          (let [response (util/curl :http nil port "/" :max-time 5)
                observed (deref observed_ 5000 :timeout)]
            (is (= {:response {:exit 0 :out "generation-arena"}
                    :copy {:same-arena? true
                           :active? true
                           :virtual? false
                           :worker? true}}
                   {:response (select-keys response [:exit :out])
                    :copy (if (map? observed)
                            (-> observed
                                (assoc :same-arena? (identical? generation-arena (:arena observed)))
                                (dissoc :arena))
                            observed)})))
          (finally
            (generation/stop! instance)))
        (is (false? (.isAlive (.scope generation-arena))))))))

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
