(ns ol.busker.wake-notifier-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.native :as h2o]
   [ol.busker.wake-notifier :as notifier])
  (:import
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicReference]))

(defn- await-count
  [values expected]
  (loop [remaining 100]
    (if (= expected (count @values))
      true
      (when (pos? remaining)
        (Thread/sleep 10)
        (recur (dec remaining))))))

(deftest native-wake-runs-on-notifier-platform-thread-test
  (let [calls_ (atom [])
        receiver (Object.)
        wake-notifier (notifier/start!)
        endpoint (notifier/endpoint (AtomicReference. receiver))]
    (try
      (with-redefs [h2o/mt-wakeup
                    (fn [actual-receiver]
                      (swap! calls_ conj {:receiver actual-receiver
                                          :virtual? (.isVirtual (Thread/currentThread))}))]
        (let [submitted (promise)]
          (Thread/startVirtualThread
           #(deliver submitted (notifier/request! wake-notifier endpoint)))
          (is (true? @submitted))
          (is (true? (await-count calls_ 1)))
          (is (= [{:receiver receiver :virtual? false}] @calls_))))
      (finally
        (notifier/stop-and-join! wake-notifier)))))

(deftest work-during-native-wake-can-request-another-wake-test
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        calls_ (atom [])
        wake-notifier (notifier/start!)
        endpoint (notifier/endpoint (AtomicReference. (Object.)))]
    (try
      (with-redefs [h2o/mt-wakeup
                    (fn [_]
                      (swap! calls_ conj :wake)
                      (when (= 1 (count @calls_))
                        (.countDown entered)
                        (.await release 5 TimeUnit/SECONDS)))]
        (is (true? (notifier/request! wake-notifier endpoint)))
        (is (.await entered 5 TimeUnit/SECONDS))
        (is (true? (notifier/request! wake-notifier endpoint)))
        (is (true? (notifier/request! wake-notifier endpoint)))
        (.countDown release)
        (is (true? (await-count calls_ 2)))
        (is (= [:wake :wake] @calls_)))
      (finally
        (.countDown release)
        (notifier/stop-and-join! wake-notifier)))))

(deftest stop-drains-admitted-wakes-and-rejects-later-requests-test
  (let [calls_ (atom [])
        wake-notifier (notifier/start!)
        endpoint (notifier/endpoint (AtomicReference. (Object.)))]
    (with-redefs [h2o/mt-wakeup (fn [_] (swap! calls_ conj :wake))]
      (is (true? (notifier/request! wake-notifier endpoint)))
      (is (true? (notifier/stop-and-join! wake-notifier)))
      (is (= [:wake] @calls_))
      (is (false? (notifier/request! wake-notifier endpoint))))))

(deftest endpoint-quiescence-waits-for-native-wake-test
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        receiver_ (AtomicReference. (Object.))
        wake-notifier (notifier/start!)
        endpoint (notifier/endpoint receiver_)]
    (try
      (with-redefs [h2o/mt-wakeup
                    (fn [_]
                      (.countDown entered)
                      (.await release 5 TimeUnit/SECONDS))]
        (is (true? (notifier/request! wake-notifier endpoint)))
        (is (.await entered 5 TimeUnit/SECONDS))
        (let [quiesced (future (notifier/quiesce-endpoint! endpoint))]
          (is (= ::waiting (deref quiesced 100 ::waiting)))
          (.countDown release)
          (is (true? (deref quiesced 5000 false)))
          (is (some? (.get receiver_)))
          (is (false? (notifier/request! wake-notifier endpoint)))))
      (finally
        (.countDown release)
        (notifier/stop-and-join! wake-notifier)))))

(deftest closed-queued-endpoint-does-not-enter-native-test
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        calls_ (atom [])
        wake-notifier (notifier/start!)
        first-endpoint (notifier/endpoint (AtomicReference. ::first))
        stale-endpoint (notifier/endpoint (AtomicReference. ::stale))]
    (try
      (with-redefs [h2o/mt-wakeup
                    (fn [receiver]
                      (swap! calls_ conj receiver)
                      (when (= ::first receiver)
                        (.countDown entered)
                        (.await release 5 TimeUnit/SECONDS)))]
        (is (true? (notifier/request! wake-notifier first-endpoint)))
        (is (.await entered 5 TimeUnit/SECONDS))
        (is (true? (notifier/request! wake-notifier stale-endpoint)))
        (is (true? (notifier/quiesce-endpoint! stale-endpoint)))
        (.countDown release)
        (is (true? (await-count calls_ 1)))
        (Thread/sleep 100)
        (is (= [::first] @calls_)))
      (finally
        (.countDown release)
        (notifier/stop-and-join! wake-notifier)))))
