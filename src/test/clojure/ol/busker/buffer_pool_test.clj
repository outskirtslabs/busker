(ns ol.busker.buffer-pool-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.buffer-pool :as pool])
  (:import
   [java.lang.foreign MemorySegment ValueLayout]
   [java.nio ByteBuffer]))

(deftest bounded-storage-retains-checked-out-blocks
  (let [p (pool/create-stream-pool 16)
        blocks (mapv (fn [_] (pool/try-borrow! p)) (range 256))]
    (try
      (is (= 256 (pool/checked-out p)))
      (is (= :full (pool/try-borrow! p)))
      (is (false? (pool/retire! p)))
      (is (= :closed (pool/try-borrow! p)))
      (doseq [block blocks] (pool/release! p block))
      (is (= 0 (pool/checked-out p)))
      (is (true? (pool/retire! p)))
      (is (true? (pool/retire! p)))
      (finally (pool/retire! p)))))

(deftest confined-aliases-expire-but-backing-remains-valid
  (let [p (pool/create-stream-pool 16)
        block (pool/try-borrow! p)
        aliases (atom nil)
        foreign-access (promise)]
    (try
      (is (= 1
             (pool/fill-native-range!
              (:segment block) 4
              (fn [^ByteBuffer buffer]
                (.put buffer (byte 42))
                (reset! aliases [buffer (.slice buffer) (.duplicate buffer)])
                (let [thread (Thread/startVirtualThread
                              (fn []
                                (deliver foreign-access
                                         (try (.put buffer 0 (byte 1)) :unexpected
                                              (catch java.lang.WrongThreadException _ :confined)))))]
                  (.join thread 1000))))))
      (is (= :confined (deref foreign-access 1000 :timeout)))
      (doseq [^ByteBuffer alias @aliases]
        (is (thrown? IllegalStateException (.put alias (byte 0)))))
      (is (= 42 (.get ^MemorySegment (:segment block) ValueLayout/JAVA_BYTE 0)))
      (finally
        (pool/release! p block)
        (pool/retire! p)))))

(deftest stale-claim-cannot-release-a-reused-block
  (let [p (pool/create-stream-pool 4)
        block (pool/try-borrow! p)]
    (try
      (pool/release! p block)
      (is (thrown? IllegalStateException (pool/release! p block)))
      (finally (pool/retire! p)))))

(deftest rejects-zero-buffer-configuration
  (is (thrown? IllegalArgumentException (pool/create-stream-pool 0))))
