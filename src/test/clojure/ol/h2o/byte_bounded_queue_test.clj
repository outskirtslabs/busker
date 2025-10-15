(ns ol.h2o.byte-bounded-queue-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.h2o.byte-bounded-queue :as bbq])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(extend-protocol bbq/Sized
  String
  (byte-size [s] (long (count s)))

  Long
  (byte-size [_] 8)

  clojure.lang.IPersistentVector
  (byte-size [v] (long (reduce + 0 (map bbq/byte-size v)))))

(deftest byte-bounded-spsc-queue-creation-test
  (testing "Creates queue with valid capacity"
    (let [q (bbq/byte-bounded-spsc-queue 1024)]
      (is (= 1024 (bbq/capacity-bytes q)))
      (is (= 0 (bbq/queued-bytes q)))
      (is (= 1024 (bbq/remaining-bytes q)))
      (is (false? (bbq/closed? q)))))

  (testing "Rejects invalid capacity"
    (is (thrown? IllegalArgumentException (bbq/byte-bounded-spsc-queue 0)))
    (is (thrown? IllegalArgumentException (bbq/byte-bounded-spsc-queue -1)))))

(deftest put-and-drain-test
  (testing "Put and drain single item"
    (let [q (bbq/byte-bounded-spsc-queue 100)]
      (bbq/put q "hello")
      (is (= 5 (bbq/queued-bytes q)))
      (is (= 95 (bbq/remaining-bytes q)))

      (let [items (bbq/drain q 100)]
        (is (= ["hello"] items))
        (is (= 0 (bbq/queued-bytes q)))
        (is (= 100 (bbq/remaining-bytes q))))))

  (testing "Put and drain multiple items"
    (let [q (bbq/byte-bounded-spsc-queue 100)]
      (bbq/put q "foo")
      (bbq/put q "bar")
      (bbq/put q "baz")
      (is (= 9 (bbq/queued-bytes q)))
      (is (= 91 (bbq/remaining-bytes q)))

      (let [items (bbq/drain q 100)]
        (is (= ["foo" "bar" "baz"] items))
        (is (= 0 (bbq/queued-bytes q)))
        (is (= 100 (bbq/remaining-bytes q))))))

  (testing "Drain respects max-bytes limit"
    (let [q (bbq/byte-bounded-spsc-queue 100)]
      (bbq/put q "hello")
      (bbq/put q "world")
      (bbq/put q "test")

      (let [items (bbq/drain q 7)]
        (is (= ["hello"] items))
        (is (= 9 (bbq/queued-bytes q)))
        (is (= 91 (bbq/remaining-bytes q))))

      (let [items (bbq/drain q 10)]
        (is (= ["world" "test"] items))
        (is (= 0 (bbq/queued-bytes q)))
        (is (= 100 (bbq/remaining-bytes q))))))

  (testing "Drain returns empty vector when queue is empty"
    (let [q (bbq/byte-bounded-spsc-queue 100)]
      (is (= [] (bbq/drain q 100))))))

(deftest backpressure-test
  (testing "Put blocks when capacity exceeded"
    (let [q (bbq/byte-bounded-spsc-queue 10)
          latch (CountDownLatch. 2)
          result (atom nil)
          producer (Thread/startVirtualThread
                    (fn []
                      (try
                        (bbq/put q "12345")
                        (bbq/put q "67890")
                        (.countDown latch)
                        (bbq/put q "ABCDE")
                        (.countDown latch)
                        (reset! result :success)
                        (catch Exception e
                          (reset! result e)))))]

      (.await latch 1 TimeUnit/SECONDS)
      (is (= 1 (.getCount latch)))
      (is (= 10 (bbq/queued-bytes q)))
      (is (= 0 (bbq/remaining-bytes q)))
      (is (nil? @result))

      (bbq/drain q 10)
      (.await latch 1 TimeUnit/SECONDS)
      (is (= 0 (.getCount latch)))
      (is (= 5 (bbq/queued-bytes q)))

      (bbq/drain q 10)
      (.join producer)
      (is (= :success @result))
      (is (= 0 (bbq/queued-bytes q)))))

  (testing "Rejects item larger than capacity"
    (let [q (bbq/byte-bounded-spsc-queue 10)]
      (is (thrown? IllegalArgumentException (bbq/put q "12345678901"))))))

(deftest close-test
  (testing "Close prevents new puts"
    (let [q (bbq/byte-bounded-spsc-queue 100)]
      (bbq/put q "hello")
      (bbq/close q)
      (is (true? (bbq/closed? q)))
      (is (thrown? IllegalStateException (bbq/put q "world")))))

  (testing "Close allows draining existing items"
    (let [q (bbq/byte-bounded-spsc-queue 100)]
      (bbq/put q "foo")
      (bbq/put q "bar")
      (bbq/close q)

      (let [items (bbq/drain q 100)]
        (is (= ["foo" "bar"] items))
        (is (= 0 (bbq/queued-bytes q))))))

  (testing "Close is idempotent"
    (let [q (bbq/byte-bounded-spsc-queue 100)]
      (bbq/close q)
      (bbq/close q)
      (is (true? (bbq/closed? q))))))

(deftest zero-byte-items-test
  (testing "Zero-byte items don't consume capacity"
    (let [q (bbq/byte-bounded-spsc-queue 10)]
      (extend-protocol bbq/Sized
        clojure.lang.Keyword
        (byte-size [_] 0))

      (bbq/put q :marker1)
      (bbq/put q :marker2)
      (is (= 0 (bbq/queued-bytes q)))
      (is (= 10 (bbq/remaining-bytes q)))

      (let [items (bbq/drain q 10)]
        (is (= [:marker1 :marker2] items))))))

(deftest accounting-accuracy-test
  (testing "Accounting remains accurate across operations"
    (let [q (bbq/byte-bounded-spsc-queue 100)]
      (is (= 100 (+ (bbq/queued-bytes q) (bbq/remaining-bytes q))))

      (bbq/put q "test")
      (is (= 100 (+ (bbq/queued-bytes q) (bbq/remaining-bytes q))))

      (bbq/put q "hello")
      (is (= 100 (+ (bbq/queued-bytes q) (bbq/remaining-bytes q))))

      (bbq/drain q 5)
      (is (= 100 (+ (bbq/queued-bytes q) (bbq/remaining-bytes q))))

      (bbq/drain q 100)
      (is (= 100 (+ (bbq/queued-bytes q) (bbq/remaining-bytes q)))))))
