(ns ol.busker.buffer-pool-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.buffer-pool :as pool])
  (:import
   [java.lang.foreign MemorySegment ValueLayout]
   [java.nio ByteBuffer]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger]))

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

(defn- borrow-or-wait
  [p]
  (let [result (promise)]
    (Thread/startVirtualThread
     #(deliver result (pool/await-block!
                       p (fn [] (let [claim (pool/try-borrow! p)]
                                  (when-not (= :full claim) claim))))))
    result))

(deftest each-released-block-goes-to-a-waiting-borrower
  (let [p (pool/create-stream-pool 16)
        blocks (mapv (fn [_] (pool/try-borrow! p)) (range 256))
        waiting (vec (repeatedly 3 #(borrow-or-wait p)))
        borrowed #(filterv map? (keep (fn [r] (deref r 0 nil)) waiting))]
    (try
      (Thread/sleep 50)
      (is (empty? (borrowed)))
      (pool/release! p (first blocks))
      (pool/release! p (second blocks))
      (let [deadline (+ (System/nanoTime) (* 2000 1000000))]
        (while (and (< (count (borrowed)) 2) (< (System/nanoTime) deadline))
          (Thread/sleep 1)))
      (Thread/sleep 50)
      (is (= 2 (count (borrowed))))
      (is (= 256 (pool/checked-out p)))
      (finally
        (pool/retire! p)
        (let [results (mapv #(deref % 2000 :timeout) waiting)]
          (is (= [:closed] (remove map? results)))
          (doseq [block (concat (drop 2 blocks) (filter map? results))] (pool/release! p block)))
        (is (true? (pool/retire! p)))))))

(deftest retirement-ends-every-wait
  (let [p (pool/create-stream-pool 16)
        blocks (mapv (fn [_] (pool/try-borrow! p)) (range 256))
        waiting (vec (repeatedly 3 #(borrow-or-wait p)))]
    (try
      (Thread/sleep 50)
      (is (false? (pool/retire! p)))
      (is (= [:closed :closed :closed] (mapv #(deref % 2000 :timeout) waiting)))
      (finally
        (doseq [block blocks] (pool/release! p block))
        (is (true? (pool/retire! p)))))))

(defn- unserved-round
  "Runs `rounds` rounds on a full pool. In each round `n` virtual threads wait
  for a block while `n` platform threads release one block each, after a random
  spin of up to `jitter` steps. A waiter sleeps 1 ms after it claims a block,
  as a producer thread that is descheduled. Returns the first round where a
  waiter has no block 2 s later, or nil."
  [rounds n jitter]
  (loop [round 0]
    (when (< round rounds)
      (let [p (pool/create-stream-pool 16)
            blocks (mapv (fn [_] (pool/try-borrow! p)) (range 256))
            ready (AtomicInteger. 0)
            go?_ (AtomicBoolean.)
            in-step! (fn [] (.incrementAndGet ready) (while (not (.get go?_)) (Thread/yield)))
            results (vec (repeatedly n promise))
            borrow! (fn [] (let [claim (pool/try-borrow! p)]
                             (when-not (= :full claim)
                               (Thread/sleep 1)
                               claim)))
            releasers (mapv (fn [block]
                              (let [delay (rand-int jitter)]
                                (doto (Thread. (fn []
                                                 (in-step!)
                                                 (dotimes [_ delay] (Thread/onSpinWait))
                                                 (pool/release! p block)))
                                  (.start))))
                            (take n blocks))]
        (doseq [result results]
          (Thread/startVirtualThread
           (fn []
             (in-step!)
             (deliver result (pool/await-block! p borrow!)))))
        (while (< (.get ready) (* 2 n)) (Thread/yield))
        (.set go?_ true)
        (let [deadline (+ (System/nanoTime) (* 2000 1000000))
              served? (every? #(map? (deref % (max 0 (quot (- deadline (System/nanoTime)) 1000000)) nil))
                              results)]
          (doseq [^Thread thread releasers] (.join thread 2000))
          (pool/retire! p)
          (doseq [block (concat (drop n blocks) (filter map? (map #(deref % 2000 nil) results)))]
            (pool/release! p block))
          (pool/retire! p)
          (if served? (recur (inc round)) round))))))

(deftest every-waiter-gets-a-block-when-releases-race-new-waiters
  (is (nil? (unserved-round 300 8 100000))))

(defn- wait-until
  "Calls `done?` every 1 ms for up to 10 s. Returns its last result."
  [done?]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (or (done?)
          (when (< (System/nanoTime) deadline)
            (Thread/sleep 1)
            (recur))))))

(defn- parked?
  "Returns true once `thread` parks after at least two `attempts`, within 10 s."
  [^Thread thread ^AtomicInteger attempts]
  (wait-until #(and (<= 2 (.get attempts))
                    (= Thread$State/WAITING (.getState thread)))))

(deftest relief-after-a-failed-flush-flushes-each-response-once
  (let [p (pool/create-stream-pool 16)
        blocks (mapv (fn [_] (pool/try-borrow! p)) (range 256))
        attempts (AtomicInteger. 0)
        borrower (promise)
        flushes (atom {})
        calls (AtomicInteger. 0)
        flush-fn (fn [token]
                   #(if (= 2 (.incrementAndGet calls))
                      (throw (IllegalStateException. "flush"))
                      (swap! flushes update token (fnil inc 0))))]
    (doseq [token [:a :b :c :d]] (pool/track-pressure! p token (flush-fn token)))
    (let [thread (Thread/startVirtualThread
                  #(deliver borrower (pool/await-block!
                                      p (fn []
                                          (.incrementAndGet attempts)
                                          (let [claim (pool/try-borrow! p)]
                                            (when-not (= :full claim) claim))))))]
      (try
        (is (parked? thread attempts))
        (is (thrown? IllegalStateException (pool/relieve-pressure! p)))
        (is (= [1 1 1] (vals @flushes)))
        (is (wait-until #(<= 3 (.get attempts))))
        (pool/relieve-pressure! p)
        (pool/relieve-pressure! p)
        (is (= {:a 1 :b 1 :c 1 :d 1} @flushes))
        (finally
          (pool/retire! p)
          (is (= :closed (deref borrower 10000 :timeout)))
          (doseq [block blocks] (pool/release! p block))
          (pool/retire! p))))))

(deftest rejects-zero-buffer-configuration
  (is (thrown? IllegalArgumentException (pool/create-stream-pool 0))))
