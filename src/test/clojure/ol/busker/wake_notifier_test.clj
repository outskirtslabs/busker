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
        wake-notifier (notifier/start! 1)
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
        wake-notifier (notifier/start! 1)
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
        wake-notifier (notifier/start! 1)
        endpoint (notifier/endpoint (AtomicReference. (Object.)))]
    (with-redefs [h2o/mt-wakeup (fn [_] (swap! calls_ conj :wake))]
      (is (true? (notifier/request! wake-notifier endpoint)))
      (is (true? (notifier/stop-and-join! wake-notifier)))
      (is (= [:wake] @calls_))
      (is (false? (notifier/request! wake-notifier endpoint))))))
