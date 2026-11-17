(ns ol.busker.generation-test
  (:require
   [babashka.ffi :as mem]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.worker-mailbox :as mailbox]
   [ol.busker.config :as config]
   [ol.busker.evloop :as evloop]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.worker-context :as worker-context]
   [ol.busker.generation :as generation]
   [ol.busker.internal.protocols :as p]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as protocols]
   [ol.busker.request :as request]
   [ol.busker.request-dispatch :as request-dispatch]
   [ol.busker.response-queue :as response-queue]
   [ol.busker.wake-endpoint :as wake-endpoint]
   [ol.busker.test-utils :as util])
  (:import
   [java.lang.foreign Arena]
   [java.net InetSocketAddress Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent AbstractExecutorService CountDownLatch ExecutorService Executors
    TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicReference AtomicReferenceArray]
   [java.util.concurrent.locks ReentrantReadWriteLock]))

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

(defn- retirement-worker
  [dispatch receiver_ thread send-fn]
  (assoc (->RetirementWorker dispatch receiver_ thread send-fn)
         :native-failure_ (AtomicReference.)
         :termination-cause_ (AtomicReference.)
         :mailbox (mailbox/create)
         :stream-pool (bp/create-stream-pool 1)))

(defn- direct-plan
  [module-id request-seq ^bytes body]
  (fixed-final/direct-response-plan (long module-id) (long request-seq) 200 [] 0 body
                                    (alength body) (alength body) 0))

(defn- publish-producer-without-waiting!
  "Publishes a one-byte native producer for `module-id` and `request-seq`
  without waiting for a slot."
  [worker module-id request-seq]
  (fixed-final/publish-direct-producer!
   worker (fixed-final/direct-response-plan (long module-id) (long request-seq) 200 [] 0 nil 0 -1 0)
   1 (fn [^java.nio.ByteBuffer buffer] (.put buffer (byte 120)))
   false (constantly false) (fn [publish!] (publish!))))

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

(deftest fixed-final-constructor-docs-describe-record-fields
  (doseq [constructor [#'fixed-final/->DirectResponsePlan]]
    (is (string? (:doc (meta constructor)))))
  (is (.contains ^String (:doc (meta #'fixed-final/->DirectResponsePlan))
                 "header-bytes")))

(def ^:private response-slot-data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t))
(def ^:private response-slot-header-name (mem/place h2o/ffi-clj-fixed-response-slot-data-t [:headers 0 :name]))
(def ^:private response-slot-header-name-len (mem/place h2o/ffi-clj-fixed-response-slot-data-t [:headers 0 :name_len]))
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
                {:mailbox (mailbox/create)
                 :response-slots []
                 :response-slot-payload-capacity 0
                 :hot-poll-ns 0
                 :native-failure_ (AtomicReference.)
                 :response-slot-monitor (Object.)
                 :callback-dispatch nil})
        native-state {:loop-ptr :loop
                      :ctx-ptr :context
                      :listener-socks []
                      :accept-callbacks []
                      :max-connections 0
                      :shutting-down? (AtomicBoolean. false)}]
    (with-redefs-fn
      {#'callback-dispatch/pending-response-work? (constantly false)
       #'callback-dispatch/drain-responses! (constantly nil)
       #'generation/update-listener-state! (fn [& _])
       #'h2o/evloop-now (constantly 0)
       #'h2o/cleanup-thread (constantly 1000)
       #'h2o/evloop-run (fn [_ wait-ms]
                          (reset! wait-ms_ wait-ms)
                          0)}
      #(let [result (#'generation/worker-loop worker :serving true native-state)]
         (is (= 0 @wait-ms_))
         (is (= :serving result))))))

(deftest response-ring-work-forces-a-nonblocking-native-iteration
  (let [wait-ms_ (atom nil)
        receiver (mem/segment 1)
        worker (evloop/map->Worker
                {:mailbox (mailbox/create)
                 :callback-dispatch nil
                 :response-slots []
                 :response-slot-payload-capacity 0
                 :hot-poll-ns 0
                 :response-receiver_ (AtomicReference. receiver)
                 :native-failure_ (AtomicReference.)
                 :response-slot-monitor (Object.)})
        native-state {:loop-ptr :loop
                      :ctx-ptr :context
                      :listener-socks []
                      :accept-callbacks []
                      :max-connections 0
                      :shutting-down? (AtomicBoolean. false)}]
    (with-redefs-fn
      {#'callback-dispatch/pending-response-work? (constantly false)
       #'callback-dispatch/drain-responses! (constantly nil)
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
      #(let [result (#'generation/worker-loop worker :serving false native-state)]
         (is (= 0 @wait-ms_))
         (is (= :serving result))))))
(deftest output-sent-between-turns-forces-a-nonblocking-native-iteration
  (doseq [[sent? expected-wait] [[true 0] [false 1000]]]
    (let [wait-ms_ (atom nil)
          worker (evloop/map->Worker
                  {:mailbox (mailbox/create)
                   :callback-dispatch nil
                   :response-slots []
                   :response-slot-payload-capacity 0
                   :hot-poll-ns 0
                   :native-failure_ (AtomicReference.)
                   :response-slot-monitor (Object.)})
          native-state {:loop-ptr :loop
                        :ctx-ptr :context
                        :listener-socks []
                        :accept-callbacks []
                        :max-connections 0
                        :shutting-down? (AtomicBoolean. false)}]
      (with-redefs-fn
        {#'callback-dispatch/pending-response-work? (constantly false)
         #'callback-dispatch/drain-responses! (constantly sent?)
         #'generation/update-listener-state! (fn [& _])
         #'h2o/evloop-now (constantly 0)
         #'h2o/cleanup-thread (constantly 1000)
         #'h2o/evloop-run (fn [_ wait-ms]
                            (reset! wait-ms_ wait-ms)
                            0)}
        #(#'generation/worker-loop worker :serving false native-state))
      (is (= expected-wait @wait-ms_) (str "sent? " sent?)))))

(deftest queued-request-tasks-limit-the-native-wait-to-one-millisecond
  (doseq [[queued? expected-wait] [[true 1] [false 1000]]]
    (let [wait-ms_ (atom nil)
          executor (proxy [AbstractExecutorService] []
                     (execute [_])
                     (isShutdown [] false))
          dispatch (request-dispatch/create executor)
          worker (evloop/map->Worker
                  {:mailbox (mailbox/create)
                   :callback-dispatch nil
                   :response-slots []
                   :response-slot-payload-capacity 0
                   :hot-poll-ns 0
                   :native-failure_ (AtomicReference.)
                   :response-slot-monitor (Object.)
                   :request-dispatch dispatch})
          native-state {:loop-ptr :loop
                        :ctx-ptr :context
                        :listener-socks []
                        :accept-callbacks []
                        :max-connections 0
                        :shutting-down? (AtomicBoolean. false)}]
      (request-dispatch/dispatch! dispatch 1 (fn []))
      (when queued?
        (request-dispatch/dispatch! dispatch 1 (fn [])))
      (with-redefs-fn
        {#'callback-dispatch/pending-response-work? (constantly false)
         #'callback-dispatch/drain-responses! (constantly false)
         #'generation/update-listener-state! (fn [& _])
         #'h2o/evloop-now (constantly 0)
         #'h2o/cleanup-thread (constantly 1000)
         #'h2o/evloop-run (fn [_ wait-ms]
                            (reset! wait-ms_ wait-ms)
                            0)}
        #(#'generation/worker-loop worker :serving false native-state))
      (is (= expected-wait @wait-ms_) (str "queued? " queued?)))))

(defn- recording-executor
  "Returns an executor that records submitted runnables in `submitted_`."
  [submitted_]
  (proxy [AbstractExecutorService] []
    (execute [task] (swap! submitted_ conj task))
    (isShutdown [] false)))

(deftest hot-poll-turn-sweeps-while-request-tasks-are-queued
  (let [submitted_ (atom [])
        turns_ (atom 0)
        dispatch (request-dispatch/create (recording-executor submitted_))
        release (CountDownLatch. 1)
        started (CountDownLatch. 1)
        queued (fn [])]
    (with-redefs [h2o/evloop-run (fn [_ wait-ms] (swap! turns_ inc) (is (zero? wait-ms)) 0)]
      (testing "without queued tasks the turn is only the native turn"
        ((#'generation/zero-wait-turn-fn :loop dispatch))
        (is (= 1 @turns_)))
      (request-dispatch/dispatch! dispatch 1 (fn []
                                               (.countDown started)
                                               (.await release 10 TimeUnit/SECONDS)))
      (request-dispatch/dispatch! dispatch 1 queued)
      (let [drainer (Thread/startVirtualThread ^Runnable (first @submitted_))]
        (is (.await started 10 TimeUnit/SECONDS))
        (Thread/sleep 5)
        (testing "a queued task behind a slow drainer gets a thread during the poll"
          ((#'generation/zero-wait-turn-fn :loop dispatch))
          (is (identical? queued (peek @submitted_))))
        (.countDown release)
        (is (.join drainer (java.time.Duration/ofSeconds 10)))))))

(deftest stop-gives-each-request-queued-at-that-moment-a-new-thread
  (let [real (Executors/newVirtualThreadPerTaskExecutor)
        first_ (atom nil)
        executor (proxy [AbstractExecutorService] []
                   (execute [task]
                     (when-not (compare-and-set! first_ nil task)
                       (.execute real task)))
                   (shutdown [] (.shutdown real))
                   (shutdownNow [] (.shutdownNow real))
                   (isShutdown [] (.isShutdown real))
                   (isTerminated [] (.isTerminated real))
                   (awaitTermination [n unit] (.awaitTermination real n unit)))
        dispatch (request-dispatch/create executor)
        done (CountDownLatch. 5)
        generation {::generation/progress_ (atom {})
                    ::generation/stop-lock (Object.)
                    ::generation/executor executor
                    ::generation/workers [{:callback-dispatch nil :request-dispatch dispatch}]}]
    (try
      (dotimes [_ 5]
        (request-dispatch/dispatch! dispatch 1 (fn []
                                                 (Thread/sleep 200)
                                                 (.countDown done))))
      (with-redefs [callback-dispatch/begin-drain! identity
                    evloop/broadcast-wake! (fn [_]
                                             (Thread/startVirtualThread
                                              #(request-dispatch/sweep! dispatch)))]
        (let [start (System/nanoTime)]
          (generation/begin-stop! generation)
          (is (.isShutdown ^ExecutorService executor))
          (Thread/startVirtualThread ^Runnable @first_)
          (is (.await done 10 TimeUnit/SECONDS))
          (let [ms (/ (- (System/nanoTime) start) 1e6)]
            (is (< ms 600) (str "five 200 ms requests took " ms " ms")))))
      (finally
        (.shutdownNow real)))))

(deftest interrupted-stop-shuts-the-executor-down-without-waiting-for-sweeps
  (let [executor (Executors/newVirtualThreadPerTaskExecutor)
        generation {::generation/progress_ (atom {})
                    ::generation/stop-lock (Object.)
                    ::generation/executor executor
                    ::generation/workers [{:callback-dispatch nil
                                           :request-dispatch (request-dispatch/create executor)}
                                          {:callback-dispatch nil
                                           :request-dispatch (request-dispatch/create executor)}]}]
    (try
      (with-redefs [callback-dispatch/begin-drain! identity
                    evloop/broadcast-wake! (fn [_])]
        (.interrupt (Thread/currentThread))
        (let [start (System/nanoTime)
              _ (generation/begin-stop! generation)
              ms (/ (- (System/nanoTime) start) 1e6)
              interrupted? (Thread/interrupted)]
          (is (< ms 300) (str "an interrupted stop took " ms " ms with workers that never sweep"))
          (is (.isShutdown executor))
          (is (= :stopping (generation/phase generation)))
          (is interrupted? "the interrupt stays set for the caller")))
      (finally
        (Thread/interrupted)
        (.shutdownNow executor)))))

(deftest wake-receiver-retires-on-event-loop-worker-test
  (let [events_ (atom [])
        receiver (mem/segment 1)
        receiver_ (AtomicReference. receiver)
        worker {:wake-endpoint ::endpoint
                :wakeup-receiver_ receiver_}]
    (.set worker-context/worker-context worker)
    (try
      (with-redefs [wake-endpoint/quiesce-endpoint!
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
        (.remove worker-context/worker-context)))))
(deftest startup-receiver-without-worker-retires-on-lifecycle-platform-test
  (let [wakeup-receiver (mem/segment 1)
        response-receiver (mem/segment 2)
        destroyed_ (atom {:wakeup [] :response []})
        events_ (atom [])
        generation-state {::generation/progress_ (atom {})
                          ::generation/stop-lock (Object.)
                          ::generation/workers []
                          ::generation/wakeup-receivers [wakeup-receiver]
                          ::generation/response-receivers [response-receiver]
                          ::generation/contexts [:startup-context]
                          ::generation/listener-runtimes []
                          ::generation/listener-claims {}
                          ::generation/config {}}]
    (with-redefs [h2o/context-dispose (fn [_] (swap! events_ conj :context))
                  h2o/mt-destroy-wakeup-receiver
                  (fn [receiver]
                    (swap! events_ conj :wakeup)
                    (swap! destroyed_ update :wakeup conj receiver))
                  h2o/mt-destroy-response-receiver
                  (fn [receiver]
                    (swap! events_ conj :response)
                    (swap! destroyed_ update :response conj receiver))]
      (generation/stop! generation-state))
    (is (= {:wakeup [wakeup-receiver]
            :response [response-receiver]}
           @destroyed_))
    (is (= [:response :wakeup :context] @events_))
    (is (= :stopped (generation/phase generation-state)))))

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
                           fallback (publish-producer-without-waiting! worker 999 999)
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

(defn- response-receiver-worker
  [receiver slot]
  {:response-receiver_             (AtomicReference. receiver)
   :response-receiver-open?_       (AtomicBoolean. true)
   :response-receiver-lock         (ReentrantReadWriteLock.)
   :response-slot-monitor          (Object.)
   :response-slots                 [slot]
   :response-slot-payload-capacity 1
   :fixed-body-limit               1})

(defn- unary-long-fn
  [f]
  (proxy [clojure.lang.AFn clojure.lang.IFn$OL] []
    (invokePrim [value] (long (f value)))
    (invoke [value] (f value))))

(defn- receiver-handle->long-fn
  [f]
  (proxy [clojure.lang.AFn clojure.lang.IFn$OLL] []
    (invokePrim [receiver claim-handle] (long (f receiver claim-handle)))
    (invoke [receiver claim-handle] (f receiver claim-handle))))

(defn- receiver-capacities->object-fn
  [f]
  (proxy [clojure.lang.AFn clojure.lang.IFn$OLLO] []
    (invokePrim [receiver capacity payload-capacity]
      (f receiver capacity payload-capacity))
    (invoke [receiver capacity payload-capacity]
      (f receiver capacity payload-capacity))))

(deftest failed-response-receiver-allocation-unwinds-core-startup
  (let [events_ (atom [])
        response-receivers_ (atom [:response-1 nil])
        arena (reify java.lang.AutoCloseable
                (close [_] (swap! events_ conj :close-arena)))
        create-wakeup (fn [context]
                        (keyword "wakeup" (name context)))
        create-response (receiver-capacities->object-fn
                         (fn [context _ _]
                           (swap! events_ conj [:create-response context])
                           (let [receiver (first @response-receivers_)]
                             (swap! response-receivers_ rest)
                             receiver)))]
    (with-redefs [mem/shared-arena (constantly arena)
                  mem/null? (constantly false)
                  generation/create-server-config
                  (constantly {::generation/config-ptr :config})
                  h2o/create-loops (constantly [:loop-1 :loop-2])
                  h2o/create-contexts (constantly [:context-1 :context-2])
                  h2o/mt-create-wakeup-receiver create-wakeup
                  h2o/mt-create-response-receiver create-response
                  h2o/mt-destroy-response-receiver
                  (fn [receiver]
                    (swap! events_ conj [:destroy-response receiver]))
                  h2o/mt-destroy-wakeup-receiver
                  (fn [receiver]
                    (swap! events_ conj [:destroy-wakeup receiver]))
                  h2o/context-dispose
                  (fn [context]
                    (swap! events_ conj [:dispose-context context]))
                  h2o/evloop-destroy
                  (fn [loop]
                    (swap! events_ conj [:destroy-loop loop]))
                  h2o/config-dispose
                  (fn [config]
                    (swap! events_ conj [:dispose-config config]))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Could not create native response receiver"
                            (#'generation/init-core-state
                             {::generation/config {:output-buffer-size 1 :fixed-response-max-body-size 1
                                                   :fixed-response-slots 1}
                              ::generation/n-workers 2
                              ::generation/max-connections 10})))
      (is (= [[:create-response :context-1]
              [:create-response :context-2]]
             (take 2 @events_)))
      (is (= [[:destroy-response :response-1]
              [:destroy-wakeup :wakeup/context-2]
              [:destroy-wakeup :wakeup/context-1]
              [:dispose-context :context-2]
              [:dispose-context :context-1]
              [:destroy-loop :loop-2]
              [:destroy-loop :loop-1]
              [:dispose-config :config]
              :close-arena]
             (drop 2 @events_))))))

;; The sizes reach the native allocation exactly, with no int overflow, also at
;; the largest option values. The stub refuses each ring, as the native call
;; does when it cannot allocate one, and startup reports the ring size.
(deftest response-receiver-sizes-follow-the-fixed-response-options
  (doseq [[options expected]
          [[{:output-buffer-size 32768 :fixed-response-max-body-size 32768
             :fixed-response-slots 256}
            [256 (+ 16384 32768 1)]]
           [{:output-buffer-size 4096 :fixed-response-max-body-size 65536
             :fixed-response-slots 3}
            [3 (+ 16384 4096 1)]]
           [{:output-buffer-size Integer/MAX_VALUE :fixed-response-max-body-size 1000
             :fixed-response-slots 1}
            [1 (+ 16384 1000 1)]]
           [{:output-buffer-size Integer/MAX_VALUE
             :fixed-response-max-body-size Integer/MAX_VALUE
             :fixed-response-slots 65535}
            [65535 (+ 16384 Integer/MAX_VALUE 1)]]]]
    (let [calls_ (atom [])]
      (with-redefs [mem/shared-arena (constantly (reify java.lang.AutoCloseable (close [_])))
                    generation/create-server-config
                    (constantly {::generation/config-ptr :config})
                    h2o/create-loops (constantly [:loop])
                    h2o/create-contexts (constantly [:context])
                    h2o/mt-create-wakeup-receiver (constantly nil)
                    h2o/mt-create-response-receiver
                    (receiver-capacities->object-fn
                     (fn [_ slots storage]
                       (swap! calls_ conj [slots storage])
                       nil))
                    h2o/context-dispose (fn [_])
                    h2o/evloop-destroy (fn [_])
                    h2o/config-dispose (fn [_])]
        (let [[slots storage] expected
              error (try
                      (#'generation/init-core-state
                       {::generation/config options
                        ::generation/n-workers 1
                        ::generation/max-connections 10})
                      nil
                      (catch clojure.lang.ExceptionInfo e e))]
          (is (re-find #"Could not create native response receiver of \d+ slots of \d+ bytes"
                       (ex-message error)))
          (is (= {:fixed-response-slots slots
                  :slot-bytes (+ (h2o/mt-response-slot-data-size) storage)}
                 (ex-data error))))
        (is (= [expected] @calls_) (pr-str options))))))

(deftest failed-response-receiver-allocation-completes-cleanup-after-a-destructor-error
  (let [events_ (atom [])
        response-receivers_ (atom [:response-1 nil])
        arena (reify java.lang.AutoCloseable
                (close [_] (swap! events_ conj :close-arena)))
        create-wakeup (fn [context]
                        (keyword "wakeup" (name context)))
        create-response (receiver-capacities->object-fn
                         (fn [context _ _]
                           (swap! events_ conj [:create-response context])
                           (let [receiver (first @response-receivers_)]
                             (swap! response-receivers_ rest)
                             receiver)))]
    (with-redefs [mem/shared-arena (constantly arena)
                  mem/null? (constantly false)
                  generation/create-server-config
                  (constantly {::generation/config-ptr :config})
                  h2o/create-loops (constantly [:loop-1 :loop-2])
                  h2o/create-contexts (constantly [:context-1 :context-2])
                  h2o/mt-create-wakeup-receiver create-wakeup
                  h2o/mt-create-response-receiver create-response
                  h2o/mt-destroy-response-receiver
                  (fn [receiver]
                    (swap! events_ conj [:destroy-response receiver])
                    (throw (ex-info "simulated response receiver cleanup failure" {})))
                  h2o/mt-destroy-wakeup-receiver
                  (fn [receiver]
                    (swap! events_ conj [:destroy-wakeup receiver]))
                  h2o/context-dispose
                  (fn [context]
                    (swap! events_ conj [:dispose-context context]))
                  h2o/evloop-destroy
                  (fn [loop]
                    (swap! events_ conj [:destroy-loop loop]))
                  h2o/config-dispose
                  (fn [config]
                    (swap! events_ conj [:dispose-config config]))]
      (let [^Throwable failure (try
                                 (#'generation/init-core-state
                                  {::generation/config {:output-buffer-size 1 :fixed-response-max-body-size 1
                                                        :fixed-response-slots 1}
                                   ::generation/n-workers 2
                                   ::generation/max-connections 10})
                                 (catch clojure.lang.ExceptionInfo error
                                   error))]
        (is (str/starts-with? (ex-message failure) "Could not create native response receiver"))
        (is (= ["simulated response receiver cleanup failure"]
               (mapv ex-message (.getSuppressed failure))))
        (is (= [[:create-response :context-1]
                [:create-response :context-2]
                [:destroy-response :response-1]
                [:destroy-wakeup :wakeup/context-2]
                [:destroy-wakeup :wakeup/context-1]
                [:dispose-context :context-2]
                [:dispose-context :context-1]
                [:destroy-loop :loop-2]
                [:destroy-loop :loop-1]
                [:dispose-config :config]
                :close-arena]
               @events_))))))

(deftest response-receiver-destruction-waits-for-producer-before-claim
  (with-open [arena (mem/shared-arena)]
    (let [receiver (mem/alloc arena 1)
          slot (mem/alloc arena h2o/ffi-clj-fixed-response-slot-data-t)
          worker (response-receiver-worker receiver slot)
          claim-entered_ (promise)
          release-claim_ (promise)
          result_ (promise)
          destroyed_ (atom [])
          uses_ (atom 0)
          claim-attempts_ (atom 0)
          claim-fn (unary-long-fn
                    (fn [_]
                      (swap! claim-attempts_ inc)
                      (deliver claim-entered_ true)
                      @release-claim_
                      0))
          pending-fn (unary-long-fn (fn [_] @uses_))]
      (with-redefs [h2o/mt-response-use-acquire (fn [_] (swap! uses_ inc) 1)
                    h2o/mt-response-use-release (fn [_] (swap! uses_ dec))
                    fixed-final/notify-capacity! (constantly nil)
                    fixed-final/notify-all-capacity! (constantly nil)
                    h2o/mt-response-try-claim claim-fn
                    h2o/mt-response-pending pending-fn
                    h2o/mt-destroy-response-receiver
                    (fn [actual-receiver]
                      (swap! destroyed_ conj actual-receiver))
                    p/wake (constantly nil)]
        (let [producer
              (Thread/startVirtualThread
               #(try
                  (deliver result_ {:result (publish-producer-without-waiting! worker 1 1)})
                  (catch Throwable error
                    (deliver result_ {:error error}))))]
          (try
            (is (true? (deref claim-entered_ 5000 false)))
            (let [destroyed? (#'generation/destroy-response-receiver! worker)]
              (is (= {:receiver-present? true
                      :admission-open?  false
                      :destroyed?        false
                      :destroyed         []}
                     {:receiver-present? (some? (.get ^AtomicReference (:response-receiver_ worker)))
                      :admission-open?  (.get ^AtomicBoolean (:response-receiver-open?_ worker))
                      :destroyed?        destroyed?
                      :destroyed         @destroyed_})
                  "Receiver destruction must defer while a producer has read it before claim."))
            (deliver release-claim_ true)
            (.join producer 5000)
            (is (= {:response :closed
                    :claim-attempts 1}
                   {:response (publish-producer-without-waiting! worker 1 1)
                    :claim-attempts @claim-attempts_})
                "Closing admission rejects another producer before it claims a slot.")
            (deliver release-claim_ true)
            (is (= {:result :overloaded
                    :error  nil}
                   (let [{:keys [result error]} (deref result_ 5000 ::timeout)]
                     {:result result
                      :error  (some-> error class .getName)})))
            (is (true? (#'generation/destroy-response-receiver! worker)))
            (is (= [receiver] @destroyed_))
            (is (nil? (.get ^AtomicReference (:response-receiver_ worker))))
            (finally
              (deliver release-claim_ true)
              (.join producer 5000)
              (is (false? (.isAlive producer))))))))))

(deftest response-receiver-destruction-waits-for-modeled-claim-accounting-interval
  (with-open [arena (mem/shared-arena)]
    (let [receiver (mem/alloc arena 1)
          slot (mem/alloc arena h2o/ffi-clj-fixed-response-slot-data-t)
          worker (response-receiver-worker receiver slot)
          uses_ (atom 0)
          ready_ (atom 0)
          claim-state-changed_ (promise)
          release-claim_ (promise)
          result_ (promise)
          destroyed_ (atom [])
          claim-fn (unary-long-fn
                    (fn [_]
                      (deliver claim-state-changed_ true)
                      @release-claim_
                      1))
          pending-fn (unary-long-fn (fn [_] (+ @uses_ @ready_)))
          publish-fn (receiver-handle->long-fn (fn [_ _] (reset! ready_ 1)))
          abort-fn (receiver-handle->long-fn (constantly 1))
          slot-writer (proxy [clojure.lang.AFn clojure.lang.IFn$OLOO] []
                        (invokePrim [_ _ _] nil)
                        (invoke [_ _ _] nil))]
      (with-redefs [h2o/mt-response-use-acquire (fn [_] (swap! uses_ inc) 1)
                    h2o/mt-response-use-release (fn [_] (swap! uses_ dec))
                    fixed-final/notify-capacity! (constantly nil)
                    fixed-final/notify-all-capacity! (constantly nil)
                    h2o/mt-response-try-claim claim-fn
                    h2o/mt-response-pending pending-fn
                    h2o/mt-response-publish publish-fn
                    h2o/mt-response-abort abort-fn
                    fixed-final/write-direct-response-slot! slot-writer
                    h2o/mt-destroy-response-receiver
                    (fn [actual-receiver]
                      (swap! destroyed_ conj actual-receiver))
                    p/wake (constantly nil)]
        (let [producer
              (Thread/startVirtualThread
               #(try
                  (deliver result_
                           {:result (fixed-final/publish-direct-response!
                                     worker (direct-plan 1 1 (byte-array 0)))})
                  (catch Throwable error
                    (deliver result_ {:error error}))))]
          (try
            (is (true? (deref claim-state-changed_ 5000 false)))
            (is (= 1 (h2o/mt-response-pending receiver)))
            (is (= {:destroyed?        false
                    :receiver-present? true}
                   {:destroyed?        (boolean (#'generation/destroy-response-receiver! worker))
                    :receiver-present? (some? (.get ^AtomicReference (:response-receiver_ worker)))})
                "The producer reference protects the claim-accounting interval.")
            (deliver release-claim_ true)
            (is (= {:result :accepted
                    :error  nil}
                   (let [{:keys [result error]} (deref result_ 5000 ::timeout)]
                     {:result result
                      :error  (some-> error class .getName)})))
            (is (zero? @uses_))
            (is (false? (#'generation/destroy-response-receiver! worker)))
            (is (empty? @destroyed_))
            (reset! ready_ 0)
            (is (true? (#'generation/destroy-response-receiver! worker)))
            (is (= [receiver] @destroyed_))
            (finally
              (deliver release-claim_ true)
              (.join producer 5000)
              (is (false? (.isAlive producer))))))))))

(deftest response-receiver-destruction-waits-for-producer-exception
  (with-open [arena (mem/shared-arena)]
    (let [receiver (mem/alloc arena 1)
          slot (mem/alloc arena h2o/ffi-clj-fixed-response-slot-data-t)
          worker (response-receiver-worker receiver slot)
          uses_ (atom 0)
          writer-entered_ (promise)
          release-writer_ (promise)
          result_ (promise)
          destroyed_ (atom [])
          aborts_ (atom 0)
          claim-fn (unary-long-fn (constantly 1))
          pending-fn (unary-long-fn (fn [_] @uses_))
          abort-fn (receiver-handle->long-fn
                    (fn [_ _]
                      (swap! aborts_ inc)
                      1))
          writer (fn [_ _ _]
                   (deliver writer-entered_ true)
                   @release-writer_
                   (throw (ex-info "simulated slot-writing failure" {})))
          slot-writer (proxy [clojure.lang.AFn clojure.lang.IFn$OLOO] []
                        (invokePrim [slot payload-capacity plan]
                          (writer slot payload-capacity plan))
                        (invoke [slot payload-capacity plan]
                          (writer slot payload-capacity plan)))]
      (with-redefs [h2o/mt-response-use-acquire (fn [_] (swap! uses_ inc) 1)
                    h2o/mt-response-use-release (fn [_] (swap! uses_ dec))
                    h2o/mt-response-try-claim claim-fn
                    h2o/mt-response-abort abort-fn
                    h2o/mt-response-pending pending-fn
                    fixed-final/write-direct-response-slot! slot-writer
                    h2o/mt-destroy-response-receiver
                    (fn [actual-receiver]
                      (swap! destroyed_ conj actual-receiver))
                    p/wake (constantly nil)]
        (let [producer
              (Thread/startVirtualThread
               #(try
                  (deliver result_
                           {:result (fixed-final/publish-direct-response!
                                     worker (direct-plan 1 1 (byte-array 0)))})
                  (catch Throwable error
                    (deliver result_ {:error error}))))]
          (try
            (is (true? (deref writer-entered_ 5000 false)))
            (is (zero? (.getReadLockCount ^ReentrantReadWriteLock (:response-receiver-lock worker))))
            (is (= 1 @uses_))
            (is (false? (#'generation/destroy-response-receiver! worker)))
            (is (= [] @destroyed_))
            (deliver release-writer_ true)
            (is (= {:result nil
                    :error  "clojure.lang.ExceptionInfo"}
                   (let [{:keys [result error]} (deref result_ 5000 ::timeout)]
                     {:result result
                      :error  (some-> error class .getName)})))
            (is (= 1 @aborts_))
            (is (true? (#'generation/destroy-response-receiver! worker)))
            (is (= [receiver] @destroyed_))
            (is (nil? (.get ^AtomicReference (:response-receiver_ worker))))
            (finally
              (deliver release-writer_ true)
              (.join producer 5000)
              (is (false? (.isAlive producer))))))))))

(deftest receiver-destruction-rechecks-pending-after-an-admitted-producer-publishes
  (with-open [arena (mem/shared-arena)]
    (let [receiver (mem/alloc arena 1)
          slot (mem/alloc arena h2o/ffi-clj-fixed-response-slot-data-t)
          worker (response-receiver-worker receiver slot)
          uses_ (atom 0)
          first-zero-observed_ (promise)
          release-disposal_ (promise)
          claim-entered_ (promise)
          release-claim_ (promise)
          producer-result_ (promise)
          disposal-result_ (promise)
          destroyed_ (atom [])
          disposed_ (atom [])
          pending_ (atom 0)
          pending-checks_ (atom 0)
          claim-fn (unary-long-fn
                    (fn [_]
                      (deliver claim-entered_ true)
                      @release-claim_
                      1))
          pending-fn (unary-long-fn
                      (fn [_]
                        (if (= 1 (swap! pending-checks_ inc))
                          (do
                            (deliver first-zero-observed_ true)
                            @release-disposal_
                            0)
                          (+ @uses_ @pending_))))
          publish-fn (receiver-handle->long-fn
                      (fn [_ _]
                        (reset! pending_ 1)
                        1))
          slot-writer (proxy [clojure.lang.AFn clojure.lang.IFn$OLOO] []
                        (invokePrim [_ _ _] nil)
                        (invoke [_ _ _] nil))
          disposer (Thread.
                    #(try
                       (deliver disposal-result_
                                {:result (#'generation/lifecycle-turn!
                                          worker
                                          :draining
                                          {:ctx-ptr :context
                                           :http3-ctxs []
                                           :shutting-down? (AtomicBoolean. true)
                                           :statuses (AtomicReferenceArray.
                                                      ^objects (object-array [{:stage :draining}]))
                                           :worker-idx 0
                                           :stopped-accepting_ (promise)})})
                       (catch Throwable error
                         (deliver disposal-result_ {:error error}))))]
      (with-redefs [h2o/mt-response-use-acquire (fn [_] (swap! uses_ inc) 1)
                    h2o/mt-response-use-release (fn [_] (swap! uses_ dec))
                    mailbox/pending? (constantly false)
                    bp/checked-out (constantly 0)
                    generation/all-connections-drained? (constantly true)
                    callback-dispatch/no-live-entries? (constantly true)
                    h2o/mt-response-try-claim claim-fn
                    h2o/mt-response-pending pending-fn
                    h2o/mt-response-publish publish-fn
                    fixed-final/write-direct-response-slot! slot-writer
                    h2o/mt-destroy-response-receiver
                    (fn [actual-receiver]
                      (swap! destroyed_ conj actual-receiver))
                    h2o/context-dispose (fn [context] (swap! disposed_ conj context))
                    p/wake (constantly nil)]
        (let [producer
              (Thread/startVirtualThread
               #(try
                  (deliver producer-result_
                           {:result (fixed-final/publish-direct-response!
                                     worker (direct-plan 1 1 (byte-array 0)))})
                  (catch Throwable error
                    (deliver producer-result_ {:error error}))))]
          (try
            (is (true? (deref claim-entered_ 5000 false)))
            (.start disposer)
            (is (true? (deref first-zero-observed_ 5000 false)))
            (deliver release-claim_ true)
            (is (= {:result :accepted
                    :error  nil}
                   (let [{:keys [result error]} (deref producer-result_ 5000 ::timeout)]
                     {:result result
                      :error  (some-> error class .getName)})))
            (deliver release-disposal_ true)
            (is (= {:context-disposed? false
                    :error             nil}
                   (let [{:keys [result error]} (deref disposal-result_ 5000 ::timeout)]
                     {:context-disposed? (= :released result)
                      :error             (some-> error class .getName)})))
            (is (= 2 @pending-checks_))
            (is (= [] @destroyed_))
            (is (= [] @disposed_))
            (is (some? (.get ^AtomicReference (:response-receiver_ worker))))
            (reset! pending_ 0)
            (is (true? (#'generation/destroy-response-receiver! worker)))
            (is (= [receiver] @destroyed_))
            (finally
              (deliver release-claim_ true)
              (deliver release-disposal_ true)
              (.join producer 5000)
              (.join disposer 5000)
              (is (false? (.isAlive producer)))
              (is (false? (.isAlive disposer))))))))))

(deftest response-ring-storage-does-not-scale-past-the-eligible-body-limit
  (let [port (util/free-port)
        instance
        (generation/start!
         (config/load!
          {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                :tls false}}
           :output-buffer-size (* 4 32768)
           :dispatch [{:handler (constantly {:status 200 :body "ring-ready"})}]})
         nil)]
    (try
      (let [worker (first (::generation/workers instance))
            receiver (.get ^AtomicReference (:response-receiver_ worker))]
        (is (= (inc (+ fixed-final/max-header-staging-bytes 32768))
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
              :payload-capacity (+ fixed-final/max-header-staging-bytes 32768)
              :body-limit 32768}
             {:scalar-handle? (integer? claim)
              :positive? (and (integer? claim) (pos? claim))
              :slot-count (count (:response-slots worker))
              :payload-capacity (:response-slot-payload-capacity worker)
              :body-limit (:fixed-body-limit worker)}))
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
    (is (thrown? IllegalStateException (mem/read slot :long)))))

(deftest stale-ring-response-is-drained-after-a-direct-native-wake
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
                              {:result (fixed-final/publish-direct-response!
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

(deftest stale-streaming-command-is-discarded-and-released
  (with-open [arena (mem/shared-arena)]
    (let [dispatch (doto (callback-dispatch/create arena)
                     (callback-dispatch/bind-thread! (Thread/currentThread)))
          pool (bp/create-stream-pool 8)
          m (mailbox/create)
          sends_ (atom 0)
          st (response-queue/create-response-writer
              {:config {:output-buffer-size 8}
               :dispatch-module-id (:module-id dispatch) :dispatch-request-seq 1
               :worker {:stream-pool pool
                        :reserve-command! (fn [_ wait? cancelled?] (mailbox/reserve! m wait? cancelled?))
                        :publish-command! (fn [_ permit command] (mailbox/publish! m permit command))
                        :release-command! (fn [_ permit] (mailbox/release! m permit))
                        :wake-command-waiters! (fn [_] (mailbox/wake-waiters! m))}
               :apply-head! (fn [_ _] (throw (AssertionError. "Stale output must not start a response")))})]
      (.set worker-context/worker-context {:callback-dispatch dispatch})
      (try
        (with-redefs [p/wake (constantly nil)
                      h2o/sendvec (fn [& _] (swap! sends_ inc))]
          (let [result (response-queue/emit-bounded!
                        st nil 1 (fn [^java.nio.ByteBuffer buffer] (.put buffer (byte 1)))
                        {:flush? true})
                [op & args] (mailbox/poll! m)]
            (is (= :h2o/output op))
            (generation/evloop-msg-processor op args)
            (response-queue/send-vecs st)
            (is (= {:status :closed} (deref (:progress result) 1000 ::timeout)))
            (is (= {:open? false :sends 0 :blocks 0 :permits 0}
                   {:open? (response-queue/open? st) :sends @sends_
                    :blocks (bp/checked-out pool) :permits (:used @(:state_ m))}))))
        (finally
          (.remove worker-context/worker-context)
          (response-queue/on-cleanup st)
          (bp/retire! pool))))))

(deftest stop-retains-stream-storage-while-a-delayed-producer-fills
  (let [port (util/free-port)
        entered (promise)
        release (promise)
        late-write (promise)
        handler (fn [{emitter :ol.busker.request/emitter}]
                  (Thread/startVirtualThread
                   #(try
                      (protocols/emit! emitter {:status 200} {:flush? true})
                      (protocols/emit! emitter
                                       (fn [^java.nio.ByteBuffer buffer]
                                         (.put buffer (byte 65))
                                         (deliver entered true)
                                         (deref release)
                                         (deliver late-write
                                                  (try (.put buffer (byte 66)) :written
                                                       (catch Throwable t t))))
                                       {:capacity 2 :end? true})
                      (catch Throwable _ nil)))
                  {:body emitter})
        instance (generation/start!
                  (config/load!
                   {:entrypoints {:http {:bind (str "127.0.0.1:" port) :tls false}}
                    :dispatch [{:handler handler}]})
                  nil)
        pools (::generation/stream-pools instance)
        checked-out #(reduce + (map bp/checked-out pools))
        stopped (promise)]
    (try
      (with-open [socket (connect port)]
        (request-stream! socket "/")
        (is (true? (deref entered 5000 false))))
      (let [stopper (doto (Thread. #(deliver stopped (try (generation/stop! instance) :stopped
                                                          (catch Throwable t t))))
                      (.start))]
        (is (= ::pending (deref stopped 500 ::pending))
            "Stop cannot finish while a producer fills a stream block.")
        (is (pos? (checked-out)))
        (deliver release true)
        (is (= :written (deref late-write 5000 ::timeout)))
        (is (= :stopped (deref stopped 30000 ::timeout)))
        (.join stopper 5000)
        (is (zero? (checked-out))))
      (finally
        (deliver release true)
        (when-not (realized? stopped) (generation/stop! instance))))))

(deftest response-ring-rejects-invalid-and-duplicate-sequential-slot-transitions
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

(deftest response-ring-rejects-malformed-header-descriptors-before-dereference
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
            headers [["name" "value"]]
            body (.getBytes "body" StandardCharsets/UTF_8)
            plan (fixed-final/direct-response-plan 999 999 200 headers
                                                   (fixed-final/header-utf8-bytes headers)
                                                   body (alength body) (alength body) 0)
            corrupt-and-reject!
            (fn [corrupt!]
              (let [claim-handle (h2o/mt-response-try-claim receiver)
                    slot (response-claim-slot worker claim-handle)
                    payload (mem/slice slot response-slot-data-size
                                       (inc (:response-slot-payload-capacity worker)))]
                (fixed-final/write-direct-response-slot!
                 slot (:response-slot-payload-capacity worker) plan)
                (let [target (corrupt! slot payload)
                      descriptor (mem/read slot (mem/place h2o/ffi-clj-fixed-response-slot-data-t [:headers 0]))]
                  (is (= (.address ^java.lang.foreign.MemorySegment target)
                         (.address ^java.lang.foreign.MemorySegment (:name descriptor)))))
                (is (zero? (h2o/mt-response-publish receiver claim-handle)))
                (is (= 1 (h2o/mt-response-abort receiver claim-handle)))
                (let [reused-handle (h2o/mt-response-try-claim receiver)]
                  (is (pos? reused-handle))
                  (is (= 1 (h2o/mt-response-abort receiver reused-handle))))))]
        (corrupt-and-reject!
         (fn [slot _]
           (let [target (mem/segment 1)]
             (mem/write slot response-slot-header-name target)
             target)))
        (corrupt-and-reject!
         (fn [slot payload]
           (let [target (mem/slice payload
                                   (:response-slot-payload-capacity worker) 1)]
             (mem/write slot response-slot-header-name target)
             target)))
        (corrupt-and-reject!
         (fn [slot payload]
           (mem/write slot response-slot-header-name payload)
           (mem/write slot response-slot-header-name-len Long/MAX_VALUE)
           payload))
        (let [claim-handle (h2o/mt-response-try-claim receiver)
              slot (response-claim-slot worker claim-handle)
              payload (mem/slice slot response-slot-data-size
                                 (inc (:response-slot-payload-capacity worker)))]
          (fixed-final/write-direct-response-slot!
           slot (:response-slot-payload-capacity worker) plan)
          (mem/write slot response-slot-header-name
                     (mem/slice payload
                                (fixed-final/header-utf8-bytes headers) 1))
          (mem/write slot response-slot-header-name-len 0)
          (is (= 1 (h2o/mt-response-publish receiver claim-handle)))
          (p/wake worker)
          (loop [remaining 200]
            (when (and (pos? remaining) (pos? (h2o/mt-response-ring-ready receiver)))
              (Thread/sleep (long 10))
              (recur (dec remaining))))
          (is (zero? (h2o/mt-response-ring-ready receiver)))
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
        (is (= :stopped (generation/phase instance))))
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

(deftest ordinary-fixed-response-parks-until-a-ring-slot-is-released
  (let [port (util/free-port)
        instance (generation/start!
                  (config/load!
                   {:entrypoints {:http {:bind (str "127.0.0.1:" port) :tls false}}
                    :n-workers 1
                    :dispatch [{:handler (constantly {:status 200 :body "ring-wakeup"})}]})
                  nil)
        worker (first (::generation/workers instance))
        receiver (.get ^AtomicReference (:response-receiver_ worker))
        handles (mapv (fn [_] (h2o/mt-response-try-claim receiver))
                      (range (h2o/mt-response-ring-capacity receiver)))
        monitor (:response-slot-monitor worker)
        full_ (promise)
        result_ (promise)
        try-claim h2o/mt-response-try-claim]
    (try
      (is (every? pos? handles))
      (with-redefs [h2o/mt-response-try-claim
                    (unary-long-fn
                     (fn [receiver]
                       (let [claim (try-claim receiver)]
                         (when (zero? claim) (deliver full_ true))
                         claim)))]
        (let [client (Thread/startVirtualThread
                      #(deliver result_ (util/curl :http nil port "/" :max-time 5)))]
          (try
            (is (true? (deref full_ 5000 false)))
            ;; Taking this monitor proves admission has released it to park.
            (locking monitor
              (is (not (realized? result_)))
              (is (= 256 (h2o/mt-response-claimed receiver)))
              (is (= 1 (h2o/mt-response-abort receiver (first handles))))
              (fixed-final/notify-capacity! worker))
            (is (= {:exit 0 :out "ring-wakeup"}
                   (select-keys (deref result_ 6000 {}) [:exit :out])))
            (finally (.join client 6000)))))
      (finally
        (doseq [claim-handle handles]
          (h2o/mt-response-abort receiver claim-handle))
        (fixed-final/notify-capacity! worker)
        (generation/stop! instance)))))

(deftest failed-worker-with-a-block-checked-out-keeps-the-callback-arena-test
  (with-open [arena (mem/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          worker (retirement-worker dispatch (AtomicReference.) (Thread.) (constantly :accepted))
          generation-state {::generation/progress_ (atom {})
                            ::generation/stop-lock (Object.)
                            ::generation/workers [worker]
                            ::generation/worker-statuses (AtomicReferenceArray. 1)
                            ::generation/wakeup-receivers [nil]
                            ::generation/listener-runtimes []
                            ::generation/listener-claims {}
                            ::generation/config {}
                            ::generation/arena arena}
          block (bp/try-borrow! (:stream-pool worker))]
      (try
        (.set ^AtomicReference (:native-failure_ worker)
              (ex-info "simulated worker failure" {}))
        (with-redefs [evloop/broadcast-wake! (constantly nil)]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"nothing can finish"
                                (generation/stop! generation-state))))
        (is (= :kept (generation/phase generation-state)))
        (is (= 1 (.byteSize (mem/alloc arena 1))))
        (finally
          (bp/release! (:stream-pool worker) block)
          (bp/retire! (:stream-pool worker))
          (swap! @#'generation/failed-retirements_
                 (fn [retirements]
                   (vec (remove #(identical? generation-state %) retirements)))))))))

(deftest successful-retirement-retry-releases-retained-generation-test
  (let [arena (mem/shared-arena)
        dispatch (callback-dispatch/create arena)
        worker (retirement-worker dispatch (AtomicReference.) (Thread.) (constantly :accepted))
        generation-state {::generation/progress_ (atom {})
                          ::generation/stop-lock (Object.)
                          ::generation/workers [worker]
                          ::generation/worker-statuses (AtomicReferenceArray. 1)
                          ::generation/wakeup-receivers [nil]
                          ::generation/listener-runtimes []
                          ::generation/listener-claims {}
                          ::generation/config {}
                          ::generation/arena arena}
        permit (mailbox/reserve! (:mailbox worker) false (constantly false))]
    (try
      (with-redefs [evloop/broadcast-wake! (constantly nil)]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"nothing can finish"
                              (generation/stop! generation-state)))
        (is (= :kept (generation/phase generation-state)))
        (is (.isAlive (.scope ^Arena arena)))
        (mailbox/release! (:mailbox worker) permit)
        (generation/stop! generation-state))
      (is (false? (.isAlive (.scope ^Arena arena))))
      (is (= :stopped (generation/phase generation-state)))
      (is (not (some #(identical? generation-state %)
                     @@#'generation/failed-retirements_)))
      (finally
        (bp/retire! (:stream-pool worker))
        (when (.isAlive (.scope ^Arena arena))
          (.close ^Arena arena))
        (swap! @#'generation/failed-retirements_
               (fn [retirements]
                 (vec (remove #(identical? generation-state %) retirements))))))))

(deftest a-failed-stop-accepting-wait-leaves-the-generation-stopping-test
  (let [generation-state {::generation/progress_ (atom {:signalled? true})
                          ::generation/stopped-accepting_ (promise)
                          ::generation/generation-id 3}]
    (with-redefs-fn {#'generation/stop-accepting-wait-ms 20}
      #(is (thrown-with-msg? clojure.lang.ExceptionInfo #"did not stop accepting"
                             (generation/await-stop-accepting! generation-state))))
    (is (= :stopping (generation/phase generation-state)))
    (is (not-any? #(identical? generation-state %) @@#'generation/failed-retirements_))))

(deftest retirement-of-a-live-worker-thread-times-out-test
  (with-open [arena (mem/shared-arena)]
    (let [release (CountDownLatch. 1)
          thread (doto (Thread. #(.await release)) (.start))
          worker (assoc (retirement-worker (callback-dispatch/create arena) (AtomicReference.)
                                           thread (constantly :accepted))
                        :id 7)
          generation-state {::generation/progress_ (atom {})
                            ::generation/stop-lock (Object.)
                            ::generation/workers [worker]
                            ::generation/worker-statuses (AtomicReferenceArray. 1)
                            ::generation/wakeup-receivers [nil]
                            ::generation/listener-runtimes []
                            ::generation/listener-claims {}
                            ::generation/config {}
                            ::generation/arena arena}]
      (try
        (with-redefs [evloop/broadcast-wake! (constantly nil)]
          (let [error (try
                        (generation/stop! generation-state 50 TimeUnit/MILLISECONDS)
                        nil
                        (catch clojure.lang.ExceptionInfo e e))]
            (is (= "Generation retirement timed out while worker threads still run. Native resources kept."
                   (ex-message error)))
            (is (= [7] (:alive-workers (ex-data error))))))
        (is (= :kept (generation/phase generation-state)))
        (finally
          (.countDown release)
          (.join thread)
          (bp/retire! (:stream-pool worker))
          (swap! @#'generation/failed-retirements_
                 (fn [retirements]
                   (vec (remove #(identical? generation-state %) retirements)))))))))

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

(deftest request-copy-runs-on-worker-test
  (let [port 18596
        observed_ (promise)
        copy-request-context h2o/copy-request-context]
    (with-redefs [h2o/copy-request-context
                  (fn [ctx-ptr]
                    (deliver observed_
                             {:virtual? (.isVirtual (Thread/currentThread))
                              :worker? (some? (worker-context/get-current-worker))})
                    (copy-request-context ctx-ptr))]
      (let [instance
            (generation/start!
             (config/load!
              {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                    :tls false}}
               :dispatch [{:handler (fn [_]
                                      {:status 200
                                       :body "copied-on-worker"})}]})
             nil)]
        (try
          (let [response (util/curl :http nil port "/" :max-time 5)
                observed (deref observed_ 5000 :timeout)]
            (is (= {:response {:exit 0 :out "copied-on-worker"}
                    :copy {:virtual? false
                           :worker? true}}
                   {:response (select-keys response [:exit :out])
                    :copy observed})))
          (finally
            (generation/stop! instance)))))))

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
                               :worker (worker-context/get-current-worker)}]
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
                  (fn [module-id request-seqs n]
                    (.set cleanup?_ true)
                    (try
                      (on-request-cleanup module-id request-seqs n)
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
                                             (protocols/flush! emitter)
                                             @probe-after-disconnect_
                                             (try
                                               (protocols/emit! emitter "event: probe\n\n")
                                               (protocols/flush! emitter)
                                               (catch Exception _
                                                 nil))
                                             @release-active_
                                             (when (protocols/open? emitter)
                                               (protocols/close! emitter))))
                                          {:body emitter})
                                        (let [body (slurp (:body ring-request))
                                              emitter (:ol.busker.request/emitter ring-request)]
                                          (Thread/startVirtualThread
                                           (fn []
                                             (protocols/emit! emitter {:status 200 :headers {}})
                                             (protocols/emit! emitter (subs body 0 6))
                                             (protocols/flush! emitter)
                                             @normal-proceed_
                                             (protocols/emit! emitter (subs body 6))
                                             (protocols/close! emitter)))
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

(deftest request-cleanups-reach-the-jvm-in-batches-inside-native-turns-test
  (let [port (util/free-port)
        n 100
        in-turn (ThreadLocal.)
        batches_ (atom [])
        evloop-run h2o/evloop-run
        ring-drain h2o/mt-response-ring-drain
        on-request-cleanup request/on-request-cleanup
        in-turn! (fn [f]
                   (.set in-turn true)
                   (try (f) (finally (.set in-turn false))))]
    (with-redefs [h2o/evloop-run (fn [loop-ptr max-wait]
                                   (in-turn! #(evloop-run loop-ptr max-wait)))
                  h2o/mt-response-ring-drain (fn [receiver]
                                               (in-turn! #(ring-drain receiver)))
                  request/on-request-cleanup
                  (fn [module-id request-seqs n-seqs]
                    (swap! batches_ conj
                           {:in-turn? (true? (.get in-turn))
                            :seqs (mapv #(h2o/request-seq-at request-seqs %) (range n-seqs))})
                    (on-request-cleanup module-id request-seqs n-seqs))]
      (let [instance (generation/start!
                      (config/load!
                       {:n-workers 1
                        :entrypoints {:http {:bind (str "127.0.0.1:" port) :tls false}}
                        :dispatch [{:handler (fn [_] {:status 200 :body "ok"})}]})
                      nil)
            url (str "http://127.0.0.1:" port "/")]
        (try
          (let [result (util/curl :http nil port "/"
                                  :max-time 10
                                  :args (concat ["--http2-prior-knowledge" "--parallel"
                                                 "--parallel-max" (str n)]
                                                (repeat (dec n) url)))]
            (is (= 0 (:exit result)) (:err result))
            (is (= (apply str (repeat n "ok")) (:out result))))
          (let [deadline (+ (System/currentTimeMillis) 5000)]
            (while (and (< (count (mapcat :seqs @batches_)) n)
                        (< (System/currentTimeMillis) deadline))
              (Thread/sleep 10)))
          (let [batches @batches_
                seqs (mapcat :seqs batches)]
            (is (every? :in-turn? batches) "each delivery happens before the native turn returns")
            (is (= n (count seqs) (count (distinct seqs))) "each request is retired once")
            (is (some #(< 1 (count (:seqs %))) batches) "a turn delivers several requests at once"))
          (finally
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
        generation-state {::generation/progress_ (atom {})
                          ::generation/stop-lock (Object.)
                          ::generation/executor executor
                          ::generation/workers []
                          ::generation/wakeup-receivers []
                          ::generation/listener-runtimes []
                          ::generation/listener-claims {}
                          ::generation/config {}}]
    (is (thrown? InterruptedException (generation/stop! generation-state)))
    (is (= :stopping (generation/phase generation-state)))
    (generation/stop! generation-state)
    (is (= :stopped (generation/phase generation-state)))))