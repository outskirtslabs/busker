(ns ol.busker.evloop-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.evloop :as evloop]
   [ol.busker.internal.protocols :as p]
   [ol.busker.wake-notifier :as notifier])
  (:import
   [java.util HashMap]
   [java.util.concurrent ArrayBlockingQueue CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong AtomicReference]))

(defn- worker
  [handler capacity receiver]
  (evloop/map->Worker
   {:id 1
    :running?_ (AtomicBoolean. true)
    :accepting?_ (AtomicBoolean. true)
    :stop-requested?_ (AtomicBoolean. false)
    :admissions_ (AtomicInteger.)
    :mailbox-signal_ (AtomicLong.)
    :wake-generation_ (AtomicLong.)
    :sleep-armed?_ (AtomicBoolean. false)
    :wake-notifier (Object.)
    :wake-endpoint (Object.)
    :mailbox (ArrayBlockingQueue. capacity)
    :message-handler handler
    :wakeup-receiver_ (AtomicReference. receiver)
    :requests (HashMap.)}))

(defn- drain! [worker]
  (@#'evloop/drain-mailbox! worker))

(deftest worker-loop-reports-mailbox-work-before-native-iteration
  (let [seen_ (atom nil)
        worker (assoc (worker (fn [_ _]) 1 nil)
                      :loop-fn
                      (fn [worker state]
                        (reset! seen_ state)
                        (.set ^AtomicBoolean (:running?_ worker) false)
                        state))]
    (.offer ^ArrayBlockingQueue (:mailbox worker) [:response])
    (@#'evloop/run-evloop-on-thread! worker)
    (is (true? (::evloop/mailbox-work? @seen_)))))

(deftest unarmed-worker-records-wake-without-notifier-test
  (let [requests_ (atom 0)
        worker (worker (fn [_ _]) 1 (Object.))]
    (.set ^AtomicBoolean (:sleep-armed?_ worker) false)
    (with-redefs [notifier/request! (fn [_ _] (swap! requests_ inc))]
      (p/wake worker)
      (is (= 1 (.get ^AtomicLong (:wake-generation_ worker))))
      (is (zero? @requests_)))))

(deftest pending-java-wake-forces-a-nonblocking-native-iteration-test
  (let [seen_ (atom nil)
        worker (assoc (worker (fn [_ _]) 1 (Object.))
                      :loop-fn
                      (fn [worker state]
                        (reset! seen_ state)
                        (.set ^AtomicBoolean (:running?_ worker) false)
                        state))]
    (.set ^AtomicBoolean (:sleep-armed?_ worker) false)
    (with-redefs [notifier/request! (fn [_ _] nil)]
      (p/wake worker)
      (@#'evloop/run-evloop-on-thread! worker)
      (is (true? (::evloop/mailbox-work? @seen_))))))

(deftest worker-rechecks-mailbox-after-arming-for-native-wait-test
  (let [checking (CountDownLatch. 1)
        release (CountDownLatch. 1)
        states_ (atom [])
        requests_ (atom 0)
        mailbox (ArrayBlockingQueue. 8)
        mailbox-empty? @#'evloop/mailbox-empty?
        worker (assoc (worker (fn [_ _]) 8 (Object.))
                      :mailbox mailbox
                      :loop-fn
                      (fn [worker state]
                        (swap! states_ conj state)
                        (.set ^AtomicBoolean (:running?_ worker) false)
                        state))
        thread (Thread. #(@#'evloop/run-evloop-on-thread! worker))]
    (with-redefs [notifier/request! (fn [_ _] (swap! requests_ inc))
                  evloop/mailbox-empty?
                  (fn [worker]
                    (.countDown checking)
                    (.await release 5 TimeUnit/SECONDS)
                    (mailbox-empty? worker))]
      (.start thread)
      (is (.await checking 5 TimeUnit/SECONDS))
      (is (true? (p/send-msg worker [:raced])))
      (.countDown release)
      (.join thread 5000)
      (is (false? (.isAlive thread)))
      (is (= 1 @requests_))
      (is (= [true] (mapv ::evloop/mailbox-work? @states_))))))

(deftest coalesces-wakeups-before-a-drain
  (let [handled_ (atom [])
        wakeups_ (atom 0)
        worker (worker (fn [op _] (swap! handled_ conj op)) 8 (Object.))]
    (.set ^AtomicBoolean (:sleep-armed?_ worker) true)
    (with-redefs [notifier/request! (fn [_ _] (swap! wakeups_ inc))]
      (is (true? (p/send-msg worker [:first])))
      (is (true? (p/send-msg worker [:second])))
      (is (= 1 @wakeups_))
      (drain! worker)
      (is (= [:first :second] @handled_))
      (is (= 2 (.get ^AtomicLong (:mailbox-signal_ worker)))))))

(deftest clears-and-rechecks-a-racing-mailbox-offer
  (let [handled_ (atom [])
        accepted_ (atom nil)
        offered?_ (AtomicBoolean. false)
        worker (worker (fn [op _] (swap! handled_ conj op)) 8 (Object.))
        clear-signal! @#'evloop/clear-mailbox-signal!]
    (.set ^AtomicBoolean (:sleep-armed?_ worker) true)
    (with-redefs [notifier/request! (fn [_ _] nil)
                  evloop/clear-mailbox-signal!
                  (fn [worker]
                    (let [result (clear-signal! worker)]
                      (when (.compareAndSet offered?_ false true)
                        (reset! accepted_ (p/send-msg worker [:raced])))
                      result))]
      (drain! worker)
      (is (true? @accepted_))
      (is (= [:raced] @handled_)))))

(deftest failed-wakeup-cannot-clear-a-newer-signal
  (let [worker (worker (fn [_ _]) 8 (Object.))
        clear-signal! @#'evloop/clear-mailbox-signal!
        mark-signalled! @#'evloop/mark-mailbox-signalled!]
    (.set ^AtomicBoolean (:sleep-armed?_ worker) true)
    (with-redefs [notifier/request!
                  (fn [_ _]
                    (clear-signal! worker)
                    (mark-signalled! worker)
                    (throw (ex-info "wake failed" {})))]
      (is (true? (p/send-msg worker [:first])))
      (is (= 3 (.get ^AtomicLong (:mailbox-signal_ worker)))))))

(deftest later-admission-retries-after-two-wakeup-failures
  (let [handled_ (atom [])
        attempts_ (atom 0)
        logs_ (atom [])
        worker (worker (fn [op _] (swap! handled_ conj op)) 8 (Object.))]
    (.set ^AtomicBoolean (:sleep-armed?_ worker) true)
    (with-redefs [notifier/request!
                  (fn [_ _]
                    (if (<= (swap! attempts_ inc) 2)
                      (throw (ex-info "wake failed" {}))
                      nil))
                  evloop/log-mailbox-wakeup-failure!
                  (fn [_ message first-error second-error reset?]
                    (swap! logs_ conj {:message message
                                       :first-error? (some? first-error)
                                       :second-error? (some? second-error)
                                       :reset? reset?}))]
      (is (true? (p/send-msg worker [:first])))
      (is (= 4 (.get ^AtomicLong (:mailbox-signal_ worker))))
      (is (= [{:message [:first]
               :first-error? true
               :second-error? true
               :reset? true}]
             @logs_))
      (is (true? (p/send-msg worker [:second])))
      (drain! worker)
      (is (= [:first :second] @handled_))
      (is (= 3 @attempts_)))))

(deftest rejects-full-and-stopped-mailboxes-without-waking
  (let [wakeups_ (atom 0)
        worker (worker (fn [_ _]) 1 (Object.))]
    (.offer ^ArrayBlockingQueue (:mailbox worker) [:already-full])
    (with-redefs [notifier/request! (fn [_ _] (swap! wakeups_ inc))]
      (is (false? (p/send-msg worker [:full])))
      (is (= 0 @wakeups_))
      (.set ^AtomicReference (:wakeup-receiver_ worker) nil)
      (.set ^AtomicBoolean (:sleep-armed?_ worker) false)
      (is (true? (p/send-msg worker evloop/stop-msg)))
      (is (false? (p/send-msg worker [:after-stop])))
      (is (= 0 @wakeups_)))))

(deftest stop-waits-for-admission-and-drains-accepted-messages
  (let [handled_ (atom [])
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        admitted (promise)
        stopped (promise)
        worker (worker (fn [op _] (swap! handled_ conj op)) 8 nil)
        offer! @#'evloop/offer-mailbox!]
    (with-redefs [evloop/offer-mailbox!
                  (fn [worker msg]
                    (.countDown entered)
                    (.await release)
                    (offer! worker msg))]
      (Thread/startVirtualThread #(deliver admitted (p/send-msg worker [:accepted])))
      (is (.await entered 5 TimeUnit/SECONDS))
      (Thread/startVirtualThread #(deliver stopped (p/send-msg worker evloop/stop-msg)))
      (is (= :pending (deref stopped 50 :pending)))
      (.countDown release)
      (is (true? @admitted))
      (is (true? @stopped))
      (drain! worker)
      (is (= [:accepted] @handled_))
      (is (false? (p/send-msg worker [:after-stop]))))))
