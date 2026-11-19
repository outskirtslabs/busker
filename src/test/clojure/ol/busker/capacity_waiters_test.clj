(ns ol.busker.capacity-waiters-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.capacity-waiters :as waiters])
  (:import
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger]
   [java.util.concurrent.locks LockSupport]))

(defn- take-unit!
  [^AtomicInteger free ^AtomicBoolean closed?_]
  (fn []
    (if (.get closed?_)
      :closed
      (loop []
        (let [available (.get free)]
          (cond
            (zero? available) nil
            (.compareAndSet free available (dec available)) true
            :else (recur)))))))

(defn- start-waiter
  [queue attempt! result]
  (Thread/startVirtualThread
   #(deliver result (try (waiters/await! queue attempt!)
                         (catch InterruptedException _ :interrupted)
                         (catch Exception e e)))))

(defn- parked?
  [^Thread thread]
  (let [deadline (+ (System/nanoTime) (* 2000 1000000))]
    (loop []
      (cond
        (= Thread$State/WAITING (.getState thread)) true
        (> (System/nanoTime) deadline) false
        :else (do (Thread/sleep 1) (recur))))))

(defn- stop-all! [threads]
  (doseq [^Thread thread threads]
    (.interrupt thread)
    (.join thread 2000)))

(deftest returns-at-once-when-capacity-is-free
  (let [queue (waiters/create)]
    (is (true? (waiters/await! queue (take-unit! (AtomicInteger. 1) (AtomicBoolean.)))))))

(deftest each-released-unit-lets-one-waiter-proceed
  (dotimes [round 200]
    (let [queue (waiters/create)
          free (AtomicInteger. 0)
          closed?_ (AtomicBoolean.)
          done (AtomicInteger. 0)
          n-waiters 8
          threads (vec (repeatedly n-waiters
                                   #(Thread/startVirtualThread
                                     (fn []
                                       (waiters/await! queue (take-unit! free closed?_))
                                       (.incrementAndGet done)))))]
      (try
        (when (odd? round) (Thread/sleep 1))
        (doseq [released (range 1 (inc n-waiters))]
          (.incrementAndGet free)
          (waiters/signal! queue 1)
          (let [deadline (+ (System/nanoTime) (* 2000 1000000))]
            (while (and (< (.get done) released) (< (System/nanoTime) deadline))
              (Thread/onSpinWait)))
          (is (= released (.get done)) (str "round " round)))
        (finally (stop-all! threads))))))

(deftest a-woken-waiter-that-stops-passes-its-wake-on
  (let [queue (waiters/create)
        free (AtomicInteger. 0)
        cancelled?_ (AtomicBoolean.)
        cancelled (promise)
        waiting (promise)
        first-waiter (start-waiter queue (take-unit! free cancelled?_) cancelled)
        _ (is (parked? first-waiter))
        second-waiter (start-waiter queue (take-unit! free (AtomicBoolean.)) waiting)]
    (try
      (is (parked? second-waiter))
      (.set cancelled?_ true)
      (.set free 1)
      (waiters/signal! queue 1)
      (is (= [:closed true] [(deref cancelled 2000 :timeout) (deref waiting 2000 :timeout)]))
      (finally (stop-all! [first-waiter second-waiter])))))

(deftest signal-all-wakes-every-waiter-to-see-closure
  (let [queue (waiters/create)
        closed?_ (AtomicBoolean.)
        results (vec (repeatedly 4 promise))
        started (mapv #(start-waiter queue (take-unit! (AtomicInteger. 0) closed?_) %) results)]
    (try
      (is (every? parked? started))
      (.set closed?_ true)
      (waiters/signal-all! queue)
      (is (= [:closed :closed :closed :closed] (mapv #(deref % 2000 :timeout) results)))
      (finally (stop-all! started)))))

(deftest interruption-ends-the-wait
  (let [queue (waiters/create)
        free (AtomicInteger. 0)
        interrupted (promise)
        waiting (promise)
        first-waiter (start-waiter queue (take-unit! free (AtomicBoolean.)) interrupted)
        _ (is (parked? first-waiter))
        second-waiter (start-waiter queue (take-unit! free (AtomicBoolean.)) waiting)]
    (try
      (is (parked? second-waiter))
      (.interrupt ^Thread first-waiter)
      (is (= :interrupted (deref interrupted 2000 :timeout)))
      (.set free 1)
      (waiters/signal! queue 1)
      (is (true? (deref waiting 2000 :timeout)))
      (finally (stop-all! [first-waiter second-waiter])))))

(deftest many-waiters-that-stop-together-all-leave-quickly
  (let [queue (waiters/create)
        free (AtomicInteger. 0)
        closed?_ (AtomicBoolean.)
        n-waiters 200000
        finished (CountDownLatch. n-waiters)
        threads (vec (repeatedly n-waiters
                                 #(Thread/startVirtualThread
                                   (fn []
                                     (waiters/await! queue (take-unit! free closed?_))
                                     (.countDown finished)))))]
    (try
      (is (every? parked? threads))
      (.set closed?_ true)
      (doseq [^Thread thread threads]
        (LockSupport/unpark thread))
      (is (.await finished 5 TimeUnit/SECONDS))
      (let [result (promise)
            late-waiter (start-waiter queue (take-unit! free (AtomicBoolean.)) result)]
        (try
          (is (parked? late-waiter))
          (.set free 1)
          (waiters/signal! queue 1)
          (is (true? (deref result 2000 :timeout)))
          (finally (stop-all! [late-waiter]))))
      (finally
        (doseq [^Thread thread threads]
          (.interrupt thread))))))

(deftest a-woken-waiter-whose-attempt-throws-passes-its-wake-on
  (let [queue (waiters/create)
        free (AtomicInteger. 0)
        failure (ex-info "attempt failed" {})
        failing?_ (AtomicBoolean.)
        failed (promise)
        waiting (promise)
        take! (take-unit! free (AtomicBoolean.))
        first-waiter (start-waiter queue #(if (.get failing?_) (throw failure) (take!)) failed)
        _ (is (parked? first-waiter))
        second-waiter (start-waiter queue take! waiting)]
    (try
      (is (parked? second-waiter))
      (.set failing?_ true)
      (.set free 1)
      (waiters/signal! queue 1)
      (is (identical? failure (deref failed 2000 :timeout)))
      (is (true? (deref waiting 2000 :timeout)))
      (finally (stop-all! [first-waiter second-waiter])))))

(deftest an-unpark-makes-a-waiter-try-again
  (let [queue (waiters/create)
        closed?_ (AtomicBoolean.)
        result (promise)
        waiter (start-waiter queue (take-unit! (AtomicInteger. 0) closed?_) result)]
    (try
      (is (parked? waiter))
      (.set closed?_ true)
      (LockSupport/unpark waiter)
      (is (= :closed (deref result 2000 :timeout)))
      (finally (stop-all! [waiter])))))

(deftest concurrent-users-never-lose-a-released-unit
  (let [queue (waiters/create)
        capacity 3
        free (AtomicInteger. capacity)
        attempt! (take-unit! free (AtomicBoolean.))
        n-threads 64
        rounds 500
        finished (CountDownLatch. n-threads)]
    (dotimes [_ n-threads]
      (Thread/startVirtualThread
       (fn []
         (dotimes [_ rounds]
           (waiters/await! queue attempt!)
           (.incrementAndGet free)
           (waiters/signal! queue 1))
         (.countDown finished))))
    (is (.await finished 20 TimeUnit/SECONDS) "every thread got a unit each round")
    (is (= capacity (.get free)))))
