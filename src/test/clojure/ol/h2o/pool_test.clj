(ns ol.h2o.pool-test
  (:require [ol.h2o.pool :refer [borrow fixed-pool invalidate release with-pool stats]]
            [clojure.test :refer [deftest is testing]]))

(deftest basic-pool-operations-test
  (testing "Pool creation and basic borrow/release"
    (let [counter (atom 0)
          pool (fixed-pool #(swap! counter inc) {:size 3 :block-start true})]

      (is (= 3 @counter) "Pool should create exactly 3 resources on initialization")

      (let [r1 (borrow pool 0.001)
            r2 (borrow pool 0.001)
            r3 (borrow pool 0.001)]
        (is (= #{1 2 3} #{r1 r2 r3}) "Should get all 3 distinct resources")

        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"Pool borrow timed out"
                              (borrow pool 0.001))
            "Borrow on empty pool should timeout")

        (release pool r1)
        (let [r4 (borrow pool 0.001)]
          (is (= r1 r4) "Released resource should be reused")

          (release pool r2)
          (release pool r3)
          (release pool r4))

        (is (= 3 @counter) "No additional resources should be created")))))

(deftest timeout-test
  (testing "Borrow with timeout"
    (let [pool (fixed-pool #(identity :resource) {:size 1 :block-start true})
          r1 (borrow pool)]

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Pool borrow timed out after 50 ms"
                            (borrow pool 0.05))
          "Borrow should timeout when pool is exhausted")

      (let [ex (try
                 (borrow pool 0.1)
                 (catch clojure.lang.ExceptionInfo e e))]
        (is (= :ol.h2o.pool/timeout (:type (ex-data ex)))
            "Timeout exception should have correct type")
        (is (= 100 (:timeout-ms (ex-data ex)))
            "Timeout exception should include timeout-ms"))

      (release pool r1)
      (is (= :resource (borrow pool)) "After release, borrow should succeed"))))

(deftest invalidation-test
  (testing "Resource invalidation and regeneration"
    (let [counter (atom 0)
          pool (fixed-pool #(swap! counter inc) {:size 2 :block-start true :regenerate-interval 0.1})]

      (is (= 2 @counter) "Initial pool creation")

      (let [r1 (borrow pool)]
        (is (future? (invalidate pool r1)) "Invalidate returns a future")

        (Thread/sleep 200)

        (is (= 3 @counter) "Invalidated resource should trigger regeneration")

        (let [r2 (borrow pool)
              r3 (borrow pool)]
          (is (= #{2 3} #{r2 r3}) "Pool should have regenerated new resource"))))))

(deftest with-pool-macro-test
  (testing "with-pool normal operation"
    (let [pool (fixed-pool #(identity :my-resource) {:size 1 :block-start true})
          result (with-pool [r pool 1]
                   (is (= :my-resource r))
                   :computation-result)]
      (is (= :computation-result result) "with-pool should return body result")
      (is (= :my-resource (borrow pool)) "Resource should be released after with-pool")))

  (testing "with-pool error handling"
    (let [counter (atom 0)
          pool (fixed-pool #(swap! counter inc) {:size 1 :block-start true :regenerate-interval 0.1})]

      (is (thrown? RuntimeException
                   (with-pool [r pool 1]
                     (is (= 1 r))
                     (throw (RuntimeException. "test error"))))
          "Exception should propagate")

      (Thread/sleep 200)

      (is (= 2 @counter) "Failed resource should be invalidated and regenerated"))))

(deftest concurrent-stress-test
  (testing "Pool under concurrent load"
    (let [operations (atom {:borrows 0 :releases 0 :errors 0})
          pool (fixed-pool
                #(rand-int 1000)
                {:size 5 :block-start true})

          workers (doall
                   (for [_ (range 10)]
                     (future
                       (dotimes [_ 50]
                         (try
                           (when-let [r (borrow pool 0.1)]
                             (swap! operations update :borrows inc)
                             (Thread/sleep (rand-int 5))
                             (try
                               (release pool r)
                               (swap! operations update :releases inc)
                               (catch InterruptedException _
                                 (swap! operations update :errors inc))))
                           (catch Exception _
                             (swap! operations update :errors inc)))))))]

      (doseq [w workers] @w)

      (is (= (:borrows @operations) (:releases @operations))
          "All borrowed resources should be released")
      (is (> (:borrows @operations) 0)
          "Some operations should succeed")
      (is (< (:errors @operations) 100)
          "Errors should be minimal"))))

(deftest stats-test
  (testing "Pool stats reporting"
    (let [pool (fixed-pool #(identity :resource) {:size 5 :block-start true})
          initial-stats (stats pool)]

      (is (= 5 (:capacity initial-stats)))
      (is (= 5 (:available initial-stats)))

      (let [r1 (borrow pool)
            r2 (borrow pool)
            borrowed-stats (stats pool)]

        (is (= 5 (:capacity borrowed-stats)))
        (is (= 3 (:available borrowed-stats)))

        (release pool r1)
        (release pool r2)

        (let [final-stats (stats pool)]
          (is (= 5 (:available final-stats))))))))
