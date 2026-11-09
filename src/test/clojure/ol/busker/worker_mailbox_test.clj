(ns ol.busker.worker-mailbox-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.worker-mailbox :as mailbox]))

(defn- fill! [m]
  (vec (repeatedly 256 #(mailbox/reserve! m false (constantly false)))))

(deftest reserved-capacity-does-not-reserve-fifo-position
  (let [m (mailbox/create)
        permits (fill! m)]
    (is (= :full (mailbox/reserve! m false (constantly false))))
    (mailbox/publish! m (second permits) [:second])
    (is (= [:second] (mailbox/poll! m)))
    (mailbox/close! m)
    (is (= :closed (mailbox/reserve! m false (constantly false))))
    (mailbox/publish! m (first permits) [:first])
    (is (= [:first] (mailbox/poll! m)))
    (doseq [permit (drop 2 permits)] (mailbox/release! m permit))
    (is (false? (mailbox/pending? m)))))

(deftest permit-releases-exactly-once
  (let [m (mailbox/create)
        permit (mailbox/reserve! m false (constantly false))]
    (mailbox/release! m permit)
    (mailbox/release! m permit)
    (is (false? (mailbox/pending? m)))
    (is (thrown? IllegalStateException (mailbox/publish! m permit [:invalid])))))

(deftest release-or-close-cannot-be-lost-before-parking
  (doseq [action [:release :close :cancel]]
    (let [m (mailbox/create)
          permits (fill! m)
          checked (promise)
          cancelled_ (atom false)
          result (promise)
          thread (Thread/startVirtualThread
                  #(deliver result
                            (mailbox/reserve! m true
                                              (fn [] (deliver checked true) @cancelled_))))]
      (try
        (is (= true (deref checked 5000 :timeout)))
        (case action
          :release (mailbox/release! m (first permits))
          :close (mailbox/close! m)
          :cancel (do (reset! cancelled_ true) (mailbox/wake-waiters! m)))
        (let [value (deref result 5000 :timeout)]
          (if (= :release action)
            (do (is (not (keyword? value))) (mailbox/release! m value))
            (is (= :closed value))))
        (finally
          (mailbox/close! m)
          (.interrupt thread)
          (.join thread 5000)
          (doseq [permit permits] (mailbox/release! m permit)))))))

(deftest interrupted-reservation-does-not-consume-capacity
  (let [m (mailbox/create)
        permits (fill! m)
        checked (promise)
        result (promise)
        thread (Thread/startVirtualThread
                #(deliver result
                          (try (mailbox/reserve! m true (fn [] (deliver checked true) false))
                               (catch InterruptedException _ :interrupted))))]
    (try
      (is (= true (deref checked 5000 :timeout)))
      (.interrupt thread)
      (is (= :interrupted (deref result 5000 :timeout)))
      (doseq [permit permits] (mailbox/release! m permit))
      (is (false? (mailbox/pending? m)))
      (finally (mailbox/close! m) (.interrupt thread) (.join thread 5000)))))
