(ns ol.busker.wake-endpoint-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.native :as h2o]
   [ol.busker.wake-endpoint :as wake-endpoint]
   [taoensso.trove :as trove])
  (:import
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicReference]))

(deftest wake-now-calls-native-wake-on-calling-thread-test
  (let [calls_ (atom [])
        receiver (Object.)
        endpoint (wake-endpoint/endpoint (AtomicReference. receiver))]
    (with-redefs [h2o/mt-wakeup
                  (fn [actual-receiver]
                    (swap! calls_ conj {:receiver actual-receiver
                                        :thread (Thread/currentThread)}))]
      (let [result_ (promise)
            thread (Thread/startVirtualThread
                    #(deliver result_ (do (wake-endpoint/wake-now! endpoint)
                                          (Thread/currentThread))))]
        (.join thread 5000)
        (is (= [{:receiver receiver :thread (deref result_ 1000 ::timeout)}]
               @calls_))))))

(deftest failed-native-wake-is-retried-once-and-logged-test
  (let [calls_ (atom 0)
        logged_ (atom [])
        endpoint (wake-endpoint/endpoint (AtomicReference. (Object.)))]
    (with-redefs [h2o/mt-wakeup (fn [_]
                                  (swap! calls_ inc)
                                  (throw (ex-info "wake failed" {})))]
      (binding [trove/*log-fn* (fn [_ns _coords level id _lazy]
                                 (swap! logged_ conj [level id]))]
        (is (nil? (wake-endpoint/wake-now! endpoint))))
      (is (= 2 @calls_))
      (is (= [[:error ::wake-endpoint/native-wake-failed]] @logged_)))))

(deftest quiescence-waits-for-in-flight-wake-test
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        calls_ (atom 0)
        receiver_ (AtomicReference. (Object.))
        endpoint (wake-endpoint/endpoint receiver_)]
    (try
      (with-redefs [h2o/mt-wakeup
                    (fn [_]
                      (swap! calls_ inc)
                      (.countDown entered)
                      (.await release 5 TimeUnit/SECONDS))]
        (let [waking (future (wake-endpoint/wake-now! endpoint))]
          (is (.await entered 5 TimeUnit/SECONDS))
          (let [quiesced (future (wake-endpoint/quiesce-endpoint! endpoint))]
            (is (= ::waiting (deref quiesced 100 ::waiting)))
            (.countDown release)
            (is (true? (deref quiesced 5000 false)))
            (deref waking 5000 nil)
            (is (some? (.get receiver_)))
            (wake-endpoint/wake-now! endpoint)
            (is (= 1 @calls_)))))
      (finally
        (.countDown release)))))

(deftest closed-endpoint-does-not-enter-native-test
  (let [endpoint (wake-endpoint/endpoint (AtomicReference. (Object.)))]
    (is (true? (wake-endpoint/quiesce-endpoint! endpoint)))
    (with-redefs [h2o/mt-wakeup (fn [_] (throw (AssertionError. "Closed endpoint woke")))]
      (is (nil? (wake-endpoint/wake-now! endpoint))))))
