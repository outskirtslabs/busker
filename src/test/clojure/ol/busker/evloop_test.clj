(ns ol.busker.evloop-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.evloop :as evloop]
   [ol.busker.internal.protocols :as p]
   [ol.busker.wake-endpoint :as wake-endpoint]
   [ol.busker.worker-mailbox :as mailbox])
  (:import
   [java.util HashMap]
   [ol.busker.evloop Worker]
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong AtomicReference]))

(defn- worker [handler]
  (evloop/map->Worker
   {:id 1 :running?_ (AtomicBoolean. true) :accepting?_ (AtomicBoolean. true)
    :stop-requested?_ (AtomicBoolean. false) :termination-cause_ (AtomicReference.)
    :mailbox-signal_ (AtomicLong.) :wake-generation_ (AtomicLong.)
    :sleep-armed?_ (AtomicBoolean. false) :wake-sent?_ (AtomicBoolean. false)
    :wake-endpoint (Object.)
    :mailbox (mailbox/create) :message-handler handler
    :wakeup-receiver_ (AtomicReference.) :requests (HashMap.)
    :response-slot-payload-capacity 0 :hot-poll-ns 0}))

(defn- drain! [worker] (@#'evloop/drain-mailbox! worker))

(deftest hot-worker-keys-are-record-fields-test
  (is (every? (set (map keyword (Worker/getBasis)))
              [:callback-dispatch :response-slot-monitor :response-slot-waiters :stream-pool
               :native-failure_ :termination-cause_ :on-native-failure! :discard-command!
               :reserve-command! :publish-command! :release-command! :wake-command-waiters!])))

(deftest worker-directly-implements-worker-thread
  (is (instance? ol.busker.internal.protocols.WorkerThread (worker (fn [_ _])))))

(deftest worker-loop-reports-mailbox-work-before-native-iteration
  (let [seen_ (atom nil)
        w (assoc (worker (fn [_ _])) :loop-fn
                 (fn [w state]
                   (reset! seen_ state)
                   (.set ^AtomicBoolean (:running?_ w) false)
                   state))]
    (p/send-msg w [:response])
    (@#'evloop/run-evloop-on-thread! w)
    (is (true? (::evloop/mailbox-work? @seen_)))))

(deftest unarmed-worker-records-wake-without-native-wake
  (let [w (worker (fn [_ _]))]
    (with-redefs [wake-endpoint/wake-now! (fn [& _] (throw (AssertionError. "Unarmed wake")))]
      (p/wake w)
      (is (= 1 (.get ^AtomicLong (:wake-generation_ w)))))))

(deftest producer-signal-is-coalesced-and-rearmed-after-drain
  (let [w (worker (fn [_ _])) calls_ (atom 0)]
    (.set ^AtomicBoolean (:sleep-armed?_ w) true)
    (with-redefs [wake-endpoint/wake-now! (fn [& _] (swap! calls_ inc))]
      (p/send-msg w [:one])
      (p/send-msg w [:two])
      (is (= 1 @calls_))
      (drain! w)
      (p/send-msg w [:two-b])
      (is (= 1 @calls_))
      (drain! w)
      (.set ^AtomicBoolean (:wake-sent?_ w) false)
      (.set ^AtomicBoolean (:sleep-armed?_ w) true)
      (p/send-msg w [:three])
      (is (= 2 @calls_)))))

(deftest one-turn-processes-at-most-256-commands
  (let [seen_ (atom [])
        w_ (atom nil)
        w (worker (fn [_ [n]]
                    (swap! seen_ conj n)
                    (when (< n 300) (p/send-msg @w_ [:next (inc n)]))))]
    (reset! w_ w)
    (p/send-msg w [:next 0])
    (is (true? (drain! w)))
    (is (= (vec (range 256)) @seen_))
    (is (= 1 (p/count-msgs w)))
    (is (odd? (.get ^AtomicLong (:mailbox-signal_ w))))
    (drain! w)
    (is (= (vec (range 301)) @seen_))))

(deftest stop-preserves-reserved-publication-without-blocking
  (let [seen_ (atom [])
        w (worker (fn [op args] (swap! seen_ conj [op args])))
        permit (evloop/reserve-command! w false (constantly false))]
    (is (= :accepted (p/send-msg w evloop/stop-msg)))
    (is (= :closed (p/send-msg w [:late])))
    (drain! w)
    (is (.get ^AtomicBoolean (:running?_ w)))
    (evloop/publish-command! w permit [:accepted 1])
    (drain! w)
    (is (= [[:accepted '(1)]] @seen_))
    (is (false? (.get ^AtomicBoolean (:running?_ w))))))

(deftest failure-discards-late-publication
  (let [discarded_ (atom [])
        w (assoc (worker (fn [_ _])) :discard-command! #(swap! discarded_ conj %))
        permit (evloop/reserve-command! w false (constantly false))]
    (.set ^AtomicBoolean (:running?_ w) false)
    (mailbox/close! (:mailbox w))
    (evloop/publish-command! w permit [:accepted])
    (is (= [[:accepted]] @discarded_))
    (is (false? (mailbox/pending? (:mailbox w))))))

(deftest worker-rechecks-mailbox-after-arming-for-native-wait
  (let [checking (CountDownLatch. 1) release (CountDownLatch. 1)
        states_ (atom []) original @#'evloop/mailbox-empty?
        w (assoc (worker (fn [_ _])) :loop-fn
                 (fn [w state]
                   (swap! states_ conj state)
                   (.set ^AtomicBoolean (:running?_ w) false)
                   state))
        thread (Thread. #(@#'evloop/run-evloop-on-thread! w))]
    (with-redefs [wake-endpoint/wake-now! (fn [& _])
                  evloop/mailbox-empty? (fn [w]
                                          (.countDown checking)
                                          (.await release 5 TimeUnit/SECONDS)
                                          (original w))]
      (try
        (.start thread)
        (is (.await checking 5 TimeUnit/SECONDS))
        (p/send-msg w [:response])
        (.countDown release)
        (.join thread 5000)
        (is (false? (.isAlive thread)))
        (is (true? (::evloop/mailbox-work? (first @states_))))
        (finally (.countDown release) (.interrupt thread) (.join thread 5000))))))

(defn- polling-worker
  "Returns a worker with one live request, `hot-poll-micros` of poll budget and a
  loop-fn that records each call and stops the worker. `on-zero-wait-turn` runs on each
  zero-wait native turn."
  [hot-poll-micros live-requests on-zero-wait-turn]
  (let [calls_ (atom [])
        turns_ (atom 0)
        w (worker (fn [_ _]))
        w (assoc w
                 :hot-poll-ns (* 1000 (long hot-poll-micros))
                 :zero-wait-turn! (fn []
                                    (swap! turns_ inc)
                                    (on-zero-wait-turn w)
                                    0)
                 :loop-fn (fn [w state]
                            (swap! calls_ conj
                                   {:mailbox-work? (::evloop/mailbox-work? state)
                                    :armed? (.get ^AtomicBoolean (:sleep-armed?_ w))})
                            (.set ^AtomicBoolean (:running?_ w) false)
                            state))]
    (dotimes [n live-requests]
      (p/add-req w n ::request))
    {:worker w :calls_ calls_ :turns_ turns_}))

(deftest wake-during-hot-poll-is-handled-without-arming-sleep
  (let [armed-during-turn_ (atom [])
        {:keys [worker calls_ turns_]}
        (polling-worker 10000000 1
                        (fn [w]
                          (swap! armed-during-turn_ conj
                                 (.get ^AtomicBoolean (:sleep-armed?_ w)))
                          (p/wake w)))]
    (@#'evloop/run-evloop-on-thread! worker)
    (is (= 1 @turns_))
    (is (= [false] @armed-during-turn_))
    (is (= [{:mailbox-work? true :armed? false}] @calls_))))

(deftest zero-hot-poll-budget-arms-sleep-at-once
  (let [{:keys [worker calls_ turns_]} (polling-worker 0 1 (fn [_]))]
    (@#'evloop/run-evloop-on-thread! worker)
    (is (zero? @turns_))
    (is (= [{:mailbox-work? false :armed? true}] @calls_))))

(deftest worker-without-live-requests-arms-sleep-at-once
  (let [{:keys [worker calls_ turns_]} (polling-worker 10000000 0 (fn [_]))]
    (@#'evloop/run-evloop-on-thread! worker)
    (is (zero? @turns_))
    (is (= [{:mailbox-work? false :armed? true}] @calls_))))
