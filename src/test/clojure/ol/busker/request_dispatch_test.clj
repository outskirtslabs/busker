(ns ol.busker.request-dispatch-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.request-dispatch :as request-dispatch])
  (:import
   [java.util.concurrent AbstractExecutorService ConcurrentHashMap CountDownLatch Executors
    ExecutorService RejectedExecutionException TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicIntegerArray AtomicLongArray]
   [java.util.concurrent.locks LockSupport]
   [ol.busker.request_dispatch ConnectionTable DrainState RequestDispatch]))

(defn- held-executor
  "Returns an executor that keeps submitted runnables in `submitted_` for the
  test to run. It rejects submissions after shutdown, and while `reject?_`
  holds true. A function in `intercept_` receives the next submission instead,
  once. `attempts_` counts every submission, rejected or not."
  [submitted_ shutdown?_ reject?_ intercept_ attempts_]
  (proxy [AbstractExecutorService] []
    (execute [task]
      (swap! attempts_ inc)
      (when (or @shutdown?_ @reject?_)
        (throw (RejectedExecutionException. "rejected")))
      (if-let [intercept (first (reset-vals! intercept_ nil))]
        (intercept task)
        (swap! submitted_ conj task)))
    (shutdown [] (reset! shutdown?_ true))
    (shutdownNow [] (reset! shutdown?_ true) [])
    (isShutdown [] @shutdown?_)
    (isTerminated [] @shutdown?_)
    (awaitTermination [_ _] true)))

(defn- held
  []
  (let [submitted_ (atom [])
        shutdown?_ (atom false)
        reject?_ (atom false)
        intercept_ (atom nil)
        attempts_ (atom 0)
        executor (held-executor submitted_ shutdown?_ reject?_ intercept_ attempts_)]
    {:submitted_ submitted_
     :attempts_ attempts_
     :reject?_ reject?_
     :intercept_ intercept_
     :executor executor
     :dispatch (request-dispatch/create executor)}))

(defn- await-active-cleared!
  "Waits until no drainer holds the active flag of connection `id`, then 10 ms
  more, time for a drainer that just cleared it to reach its last poll."
  [^RequestDispatch dispatch id]
  (let [^DrainState state (.lookup ^ConnectionTable (.-table dispatch) id)
        deadline (+ (System/nanoTime) (.toNanos TimeUnit/SECONDS 10))]
    (while (and (.get ^AtomicBoolean (.-active? state))
                (< (System/nanoTime) deadline))
      (Thread/sleep 1))
    (Thread/sleep 10)))

(defn- run-on-thread!
  "Runs `runnable` on a new virtual thread and waits for it. Returns the
  throwables its uncaught-exception handler received."
  [^Runnable runnable]
  (let [reported_ (atom [])
        thread (-> (Thread/ofVirtual)
                   (.uncaughtExceptionHandler
                    (reify Thread$UncaughtExceptionHandler
                      (uncaughtException [_ _ t] (swap! reported_ conj t))))
                   (.start runnable))]
    (is (.join thread (java.time.Duration/ofSeconds 10)))
    @reported_))

(defn- recording-task
  [log_ label]
  (fn []
    (swap! log_ conj [label (Thread/currentThread)])))

(deftest task-dispatched-alone-starts-a-thread-through-the-executor
  (let [{:keys [dispatch submitted_]} (held)
        log_ (atom [])]
    (request-dispatch/dispatch! dispatch 1 (recording-task log_ :a))
    (is (= 1 (count @submitted_)))
    (is (empty? @log_) "task waits for the executor thread")
    (run-on-thread! (first @submitted_))
    (is (= [:a] (map first @log_)))
    (is (.isVirtual ^Thread (second (first @log_))))))

(deftest tasks-of-one-connection-run-on-its-drainer
  (let [{:keys [dispatch submitted_]} (held)
        log_ (atom [])]
    (doseq [label [:a :b :c]]
      (request-dispatch/dispatch! dispatch 7 (recording-task log_ label)))
    (is (= 1 (count @submitted_)) "only the first task starts a thread")
    (run-on-thread! (first @submitted_))
    (testing "the drainer runs queued tasks in order on its thread"
      (is (= [:a :b :c] (map first @log_)))
      (is (= 1 (count (distinct (map second @log_))))))
    (testing "the drainer ends when the queue is empty and the next turn releases the connection"
      (is (false? (request-dispatch/sweep! dispatch)))
      (request-dispatch/dispatch! dispatch 8 (recording-task log_ :d))
      (is (= 2 (count @submitted_)))
      (is (identical? (first @submitted_) (second @submitted_))
          "the next connection's drainer reuses the released state"))))

(deftest tasks-of-different-connections-never-share-a-drainer
  (let [{:keys [dispatch submitted_]} (held)
        log_ (atom [])]
    (request-dispatch/dispatch! dispatch 1 (recording-task log_ :a1))
    (request-dispatch/dispatch! dispatch 2 (recording-task log_ :b1))
    (request-dispatch/dispatch! dispatch 1 (recording-task log_ :a2))
    (request-dispatch/dispatch! dispatch 2 (recording-task log_ :b2))
    (is (= 2 (count @submitted_)) "one drainer per connection")
    (run! run-on-thread! @submitted_)
    (let [thread-of (into {} @log_)]
      (is (= #{:a1 :a2 :b1 :b2} (set (keys thread-of))))
      (is (identical? (thread-of :a1) (thread-of :a2)))
      (is (identical? (thread-of :b1) (thread-of :b2)))
      (is (not (identical? (thread-of :a1) (thread-of :b1)))))))

(deftest released-state-serves-another-connection-alone
  (let [{:keys [dispatch submitted_]} (held)
        log_ (atom [])]
    (request-dispatch/dispatch! dispatch 1 (recording-task log_ :a))
    (run-on-thread! (first @submitted_))
    (request-dispatch/sweep! dispatch)
    (request-dispatch/dispatch! dispatch 2 (recording-task log_ :b))
    (testing "a later task of the released connection id starts a new drainer"
      (request-dispatch/dispatch! dispatch 1 (recording-task log_ :c)))
    (is (= 3 (count @submitted_)))
    (run! run-on-thread! (rest @submitted_))
    (is (= [:a :b :c] (map first @log_)))
    (is (= 3 (count (distinct (map second @log_)))))))

(defn- spin-micros
  [micros]
  (let [end (+ (System/nanoTime) (* 1000 micros))]
    (while (< (System/nanoTime) end)
      (Thread/onSpinWait))))

(defn- start-long-drainer!
  "Dispatches on connection `id` a task that blocks until `release`, then tasks
  `b` and `c`, runs the drainer and waits until it has run longer than the
  threshold. Returns the drainer thread."
  [dispatch submitted_ id release b c]
  (let [started (CountDownLatch. 1)
        before (count @submitted_)]
    (request-dispatch/dispatch! dispatch id (fn []
                                              (.countDown started)
                                              (.await ^CountDownLatch release 10 TimeUnit/SECONDS)))
    (request-dispatch/dispatch! dispatch id b)
    (request-dispatch/dispatch! dispatch id c)
    (let [drainer (Thread/startVirtualThread (nth @submitted_ before))]
      (is (.await started 10 TimeUnit/SECONDS))
      (Thread/sleep 5)
      drainer)))

(deftest queued-tasks-stay-while-the-drainer-has-not-started
  (let [{:keys [dispatch submitted_]} (held)]
    (request-dispatch/dispatch! dispatch 1 (fn []))
    (request-dispatch/dispatch! dispatch 1 (fn []))
    (is (true? (request-dispatch/sweep! dispatch)))
    (is (= 1 (count @submitted_)))))

(deftest long-handler-sends-new-and-queued-tasks-of-its-connection-to-new-threads
  (let [{:keys [dispatch submitted_]} (held)
        release (CountDownLatch. 1)
        log_ (atom [])
        queued-b (recording-task log_ :b)
        queued-c (recording-task log_ :c)
        drainer (start-long-drainer! dispatch submitted_ 1 release queued-b queued-c)]
    (testing "the loop-turn move hands queued tasks to new threads"
      (is (false? (request-dispatch/sweep! dispatch)))
      (is (= [queued-b queued-c] (subvec @submitted_ 1 3))))
    (testing "a new task of that connection gets a new thread"
      (let [direct (recording-task log_ :d)]
        (request-dispatch/dispatch! dispatch 1 direct)
        (is (identical? direct (peek @submitted_)))))
    (testing "another connection's task starts a separate drainer, not behind the slow one"
      (let [other (recording-task log_ :other)]
        (request-dispatch/dispatch! dispatch 2 other)
        (is (= 5 (count @submitted_)))
        (is (not (identical? other (peek @submitted_))))
        (run-on-thread! (peek @submitted_))
        (is (= [:other] (map first @log_)))))
    (.countDown release)
    (is (.join drainer (java.time.Duration/ofSeconds 10)))
    (is (= [:other] (map first @log_)) "moved tasks run only on their new threads")))

(deftest long-run-of-short-tasks-sends-new-and-queued-tasks-to-new-threads
  (testing "the drainer's whole run counts, not the task that runs now"
    (let [{:keys [dispatch submitted_]} (held)
          done_ (atom 0)
          short-task (fn []
                       (spin-micros 30)
                       (swap! done_ inc))]
      (dotimes [_ 200]
        (request-dispatch/dispatch! dispatch 3 short-task))
      (let [drainer (Thread/startVirtualThread (first @submitted_))]
        (while (< @done_ 10)
          (Thread/onSpinWait))
        (let [direct (fn [])]
          (request-dispatch/dispatch! dispatch 3 direct)
          (is (identical? direct (peek @submitted_)) "a new task gets a new thread"))
        (is (false? (request-dispatch/sweep! dispatch)) "the move empties the queue")
        (is (< 2 (count @submitted_)))
        (is (.join drainer (java.time.Duration/ofSeconds 10)))
        (is (< @done_ 200) "moved tasks do not run on the drainer")))))

(deftest move-leaves-queued-tasks-to-the-drainer-after-shutdown
  (let [{:keys [dispatch executor submitted_ attempts_]} (held)
        release (CountDownLatch. 1)
        log_ (atom [])
        drainer (start-long-drainer! dispatch submitted_ 1 release
                                     (recording-task log_ :b) (recording-task log_ :c))]
    (.shutdown ^ExecutorService executor)
    (is (true? (request-dispatch/sweep! dispatch)))
    (is (= 1 @attempts_) "the move does not hand tasks to a shut-down executor")
    (.countDown release)
    (is (.join drainer (java.time.Duration/ofSeconds 10)))
    (is (= [:b :c] (map first @log_)))
    (is (= #{drainer} (set (map second @log_))))))

(deftest move-puts-a-rejected-task-back-for-the-drainer
  (let [{:keys [dispatch submitted_ reject?_]} (held)
        release (CountDownLatch. 1)
        log_ (atom [])
        drainer (start-long-drainer! dispatch submitted_ 1 release
                                     (recording-task log_ :b) (recording-task log_ :c))]
    (reset! reject?_ true)
    (is (true? (request-dispatch/sweep! dispatch)))
    (is (= 1 (count @submitted_)))
    (testing "a new task the executor rejects is rejected, not queued"
      (is (thrown? RejectedExecutionException
                   (request-dispatch/dispatch! dispatch 1 (recording-task log_ :rejected)))))
    (.countDown release)
    (is (.join drainer (java.time.Duration/ofSeconds 10)))
    (is (= #{:b :c} (set (map first @log_))))
    (is (= #{drainer} (set (map second @log_))))
    (request-dispatch/sweep! dispatch)
    (reset! reject?_ false)
    (request-dispatch/dispatch! dispatch 9 (recording-task log_ :next))
    (is (identical? (first @submitted_) (peek @submitted_))
        "the state is released once its drainer ends")))

(deftest task-put-back-during-the-drainers-last-poll-runs-on-the-drainer
  (testing "the executor holds the move of :b until the drainer has run :c, found
  its queue empty and cleared its active flag, then rejects :b"
    (let [{:keys [dispatch submitted_ intercept_]} (held)
          release (CountDownLatch. 1)
          log_ (atom [])
          drainer (start-long-drainer! dispatch submitted_ 1 release
                                       (recording-task log_ :b) (recording-task log_ :c))]
      (reset! intercept_ (fn [_]
                           (.countDown release)
                           (await-active-cleared! dispatch 1)
                           (throw (RejectedExecutionException. "rejected"))))
      (request-dispatch/sweep! dispatch)
      (is (.join drainer (java.time.Duration/ofSeconds 10)))
      (is (= [:c :b] (map first @log_)))
      (is (= #{drainer} (set (map second @log_)))))))

(deftest state-stays-with-its-connection-while-a-drainer-that-lost-active-runs
  (testing "drainer A clears its active flag while the worker is held in a move;
  the worker then starts drainer B, the move puts :t back and A runs :t untimed.
  After B ends, a rejected start must not release the state while A runs, or A
  would poll the queue of the connection that gets the state next"
    (let [{:keys [dispatch submitted_ intercept_ reject?_]} (held)
          release (CountDownLatch. 1)
          t-started (CountDownLatch. 1)
          t-release (CountDownLatch. 1)
          log_ (atom [])
          drainer-a (start-long-drainer! dispatch submitted_ 1 release
                                         (fn []
                                           (swap! log_ conj [:t (Thread/currentThread)])
                                           (.countDown t-started)
                                           (.await t-release 10 TimeUnit/SECONDS))
                                         (fn []))]
      (reset! intercept_ (fn [_]
                           (.countDown release)
                           (await-active-cleared! dispatch 1)
                           (request-dispatch/dispatch! dispatch 1 (recording-task log_ :u))
                           (throw (RejectedExecutionException. "rejected"))))
      (request-dispatch/sweep! dispatch)
      (is (.await t-started 10 TimeUnit/SECONDS))
      (run-on-thread! (peek @submitted_))
      (request-dispatch/sweep! dispatch)
      (reset! reject?_ true)
      (is (thrown? RejectedExecutionException
                   (request-dispatch/dispatch! dispatch 1 (recording-task log_ :rejected))))
      (reset! reject?_ false)
      (request-dispatch/dispatch! dispatch 2 (recording-task log_ :v1))
      (request-dispatch/dispatch! dispatch 2 (recording-task log_ :v2))
      (.countDown t-release)
      (is (.join drainer-a (java.time.Duration/ofSeconds 10)))
      (run-on-thread! (peek @submitted_))
      (let [thread-of (into {} @log_)]
        (is (= #{:t :u :v1 :v2} (set (keys thread-of))))
        (is (identical? drainer-a (thread-of :t)))
        (is (identical? (thread-of :v1) (thread-of :v2)))
        (is (not (identical? drainer-a (thread-of :v2))))))))

(deftest drainer-that-lost-active-runs-tasks-queued-before-the-newer-one-starts
  (testing "drainer A runs :t untimed after drainer B took the active flag; :w,
  queued while B has not started, runs on A when :t ends"
    (let [{:keys [dispatch submitted_ intercept_]} (held)
          release (CountDownLatch. 1)
          t-started (CountDownLatch. 1)
          t-release (CountDownLatch. 1)
          log_ (atom [])
          drainer-a (start-long-drainer! dispatch submitted_ 1 release
                                         (fn []
                                           (.countDown t-started)
                                           (.await t-release 10 TimeUnit/SECONDS))
                                         (fn []))]
      (reset! intercept_ (fn [_]
                           (.countDown release)
                           (await-active-cleared! dispatch 1)
                           (request-dispatch/dispatch! dispatch 1 (recording-task log_ :u))
                           (throw (RejectedExecutionException. "rejected"))))
      (request-dispatch/sweep! dispatch)
      (is (.await t-started 10 TimeUnit/SECONDS))
      (request-dispatch/dispatch! dispatch 1 (recording-task log_ :w))
      (.countDown t-release)
      (is (.join drainer-a (java.time.Duration/ofSeconds 10)))
      (is (= [[:w drainer-a]] @log_))
      (run-on-thread! (peek @submitted_))
      (is (= [:w :u] (map first @log_))))))

(deftest throwing-task-does-not-stop-later-tasks
  (let [{:keys [dispatch submitted_]} (held)
        boom (ex-info "boom" {})
        interrupted_ (atom nil)
        ran_ (atom [])]
    (request-dispatch/dispatch! dispatch 1 (fn [] (throw boom)))
    (request-dispatch/dispatch! dispatch 1 (fn []
                                             (swap! ran_ conj :interrupts)
                                             (.interrupt (Thread/currentThread))))
    (request-dispatch/dispatch! dispatch 1 (fn []
                                             (swap! ran_ conj :checks)
                                             (reset! interrupted_
                                                     (.isInterrupted (Thread/currentThread)))))
    (let [reported (run-on-thread! (first @submitted_))]
      (is (= [boom] reported) "the thread's handler receives the throwable")
      (is (= [:interrupts :checks] @ran_))
      (is (false? @interrupted_) "the interrupt flag does not carry over"))))

(deftest shut-down-executor-rejects-new-tasks-and-runs-queued-ones
  (let [{:keys [dispatch executor submitted_]} (held)
        log_ (atom [])]
    (request-dispatch/dispatch! dispatch 1 (recording-task log_ :a))
    (request-dispatch/dispatch! dispatch 1 (recording-task log_ :b))
    (.shutdown ^ExecutorService executor)
    (is (thrown? RejectedExecutionException
                 (request-dispatch/dispatch! dispatch 1 (recording-task log_ :late))))
    (is (thrown? RejectedExecutionException
                 (request-dispatch/dispatch! dispatch 2 (recording-task log_ :late))))
    (run-on-thread! (first @submitted_))
    (is (= [:a :b] (map first @log_)))))

(deftest rejected-drainer-start-leaves-no-task-queued
  (let [{:keys [dispatch submitted_ intercept_]} (held)
        rejected_ (atom nil)
        log_ (atom [])]
    (reset! intercept_ (fn [state]
                         (reset! rejected_ state)
                         (throw (RejectedExecutionException. "rejected"))))
    (is (thrown? RejectedExecutionException
                 (request-dispatch/dispatch! dispatch 1 (recording-task log_ :rejected))))
    (request-dispatch/dispatch! dispatch 2 (recording-task log_ :other))
    (is (identical? @rejected_ (peek @submitted_)) "the connection's state is released")
    (request-dispatch/dispatch! dispatch 1 (recording-task log_ :a))
    (is (= 2 (count @submitted_)))
    (run! run-on-thread! @submitted_)
    (is (= [:other :a] (map first @log_)))))

(deftest error-starting-a-drainer-leaves-the-connection-usable
  (testing "an executor that throws an Error once, as thread creation can"
    (let [{:keys [dispatch submitted_ intercept_]} (held)
          log_ (atom [])
          oom (OutOfMemoryError. "unable to create thread")]
      (reset! intercept_ (fn [_] (throw oom)))
      (is (identical? oom (try
                            (request-dispatch/dispatch! dispatch 1 (recording-task log_ :failed))
                            nil
                            (catch Throwable t t))))
      (request-dispatch/dispatch! dispatch 1 (recording-task log_ :a))
      (request-dispatch/dispatch! dispatch 1 (recording-task log_ :b))
      (is (= 1 (count @submitted_)) "the next request starts a drainer")
      (run-on-thread! (first @submitted_))
      (is (= [:a :b] (map first @log_)))
      (is (false? (request-dispatch/sweep! dispatch))))))

(defn- holding-first
  "Returns an executor that keeps the first runnable submitted in `first_`
  without running it and runs the others on `executor`."
  [^ExecutorService executor first_]
  (proxy [AbstractExecutorService] []
    (execute [task]
      (when-not (compare-and-set! first_ nil task)
        (.execute executor task)))
    (shutdown [] (.shutdown executor))
    (shutdownNow [] (.shutdownNow executor))
    (isShutdown [] (.isShutdown executor))
    (isTerminated [] (.isTerminated executor))
    (awaitTermination [n unit] (.awaitTermination executor n unit))))

(defn- stop!
  "Does what the generation does when a stop begins, with this thread as the
  worker: [[request-dispatch/begin-stop!]], one sweep, then the shutdown."
  [dispatch ^ExecutorService executor]
  (request-dispatch/begin-stop! dispatch)
  (request-dispatch/sweep! dispatch)
  (is (request-dispatch/await-stop-moves! dispatch 1000))
  (.shutdown executor))

(deftest requests-queued-when-a-stop-begins-each-get-a-new-thread
  (let [real (Executors/newVirtualThreadPerTaskExecutor)]
    (try
      (testing "five 200 ms tasks of one connection queued behind a drainer that has not started"
        (let [first_ (atom nil)
              executor (holding-first real first_)
              dispatch (request-dispatch/create executor)
              done (CountDownLatch. 5)
              start (System/nanoTime)]
          (dotimes [_ 5]
            (request-dispatch/dispatch! dispatch 1 (fn []
                                                     (Thread/sleep 200)
                                                     (.countDown done))))
          (stop! dispatch executor)
          (Thread/startVirtualThread ^Runnable @first_)
          (is (.await done 10 TimeUnit/SECONDS))
          (let [ms (/ (- (System/nanoTime) start) 1e6)]
            (is (< ms 600) (str "five 200 ms tasks took " ms " ms")))))
      (testing "a request that waits for a later request of its connection"
        (let [first_ (atom nil)
              real (Executors/newVirtualThreadPerTaskExecutor)
              executor (holding-first real first_)
              dispatch (request-dispatch/create executor)
              provided (CountDownLatch. 1)
              done (CountDownLatch. 2)]
          (request-dispatch/dispatch! dispatch 2 (fn []))
          (request-dispatch/dispatch! dispatch 2 (fn []
                                                   (when (.await provided 5 TimeUnit/SECONDS)
                                                     (.countDown done))))
          (request-dispatch/dispatch! dispatch 2 (fn []
                                                   (.countDown provided)
                                                   (.countDown done)))
          (stop! dispatch executor)
          (Thread/startVirtualThread ^Runnable @first_)
          (is (.await done 3 TimeUnit/SECONDS))
          (.awaitTermination real 10 TimeUnit/SECONDS)))
      (testing "a task dispatched after the stop began gets a new thread"
        (let [{:keys [dispatch executor submitted_]} (held)
              late (fn [])]
          (request-dispatch/dispatch! dispatch 3 (fn []))
          (request-dispatch/begin-stop! dispatch)
          (request-dispatch/dispatch! dispatch 3 late)
          (is (identical? late (peek @submitted_)))
          (.shutdown ^ExecutorService executor)
          (is (thrown? RejectedExecutionException
                       (request-dispatch/dispatch! dispatch 3 (fn []))))))
      (finally
        (.shutdown real)
        (.awaitTermination real 10 TimeUnit/SECONDS)))))

(deftest connection-id-zero-is-refused
  (let [{:keys [dispatch]} (held)]
    (is (thrown? IllegalArgumentException (request-dispatch/dispatch! dispatch 0 (fn []))))))

(deftest stop-hook-interrupts-each-remaining-task
  (let [{:keys [dispatch executor submitted_]} (held)
        seen_ (atom [])
        task (fn [] (swap! seen_ conj (.isInterrupted (Thread/currentThread))))]
    (doseq [id [1 2 1 2 1 2]]
      (request-dispatch/dispatch! dispatch id task))
    (.shutdownNow ^ExecutorService executor)
    (request-dispatch/interrupt-remaining! dispatch)
    (run! run-on-thread! @submitted_)
    (is (= (repeat 6 true) @seen_))))

(defn- await-burst!
  "Waits up to 10 seconds for `done`, running the worker's loop turn meanwhile.
  Returns true when every task of the burst ran."
  [dispatch ^CountDownLatch done]
  (let [deadline (+ (System/nanoTime) (.toNanos TimeUnit/SECONDS 10))]
    (loop []
      (cond
        (.await done 200 TimeUnit/MICROSECONDS) true
        (> (System/nanoTime) deadline) false
        :else (do (request-dispatch/sweep! dispatch) (recur))))))

(defn- spin-nanos
  [nanos]
  (let [end (+ (System/nanoTime) nanos)]
    (while (< (System/nanoTime) end)
      (Thread/onSpinWait))))

(defn- await-flag!
  "Spins for up to 10 seconds until `flag` is true. Returns the flag."
  [^AtomicBoolean flag]
  (let [deadline (+ (System/nanoTime) (.toNanos TimeUnit/SECONDS 10))]
    (while (and (not (.get flag)) (< (System/nanoTime) deadline))
      (Thread/onSpinWait))
    (.get flag)))

(deftest task-dispatched-as-its-drainer-finds-the-queue-empty-runs
  (testing "the worker dispatches a second task of a connection as the drainer
  finishes the first, at offsets spread across the drainer's last poll, and
  then waits; a second task left queued without a drainer fails the test"
    (let [rounds 20000
          executor (Executors/newVirtualThreadPerTaskExecutor)
          dispatch (request-dispatch/create executor)]
      (try
        (is (= rounds
               (loop [r 0]
                 (if (= r rounds)
                   r
                   (let [finishing (AtomicBoolean.)
                         done (CountDownLatch. 2)
                         id (inc (quot r 50))]
                     (request-dispatch/dispatch! dispatch id (fn []
                                                               (.countDown done)
                                                               (.set finishing true)))
                     (if (await-flag! finishing)
                       (do
                         (spin-nanos (mod (* r 37) 400))
                         (request-dispatch/dispatch! dispatch id (fn [] (.countDown done)))
                         (if (await-burst! dispatch done)
                           (recur (inc r))
                           r))
                       r))))))
        (finally
          (.shutdown executor)
          (.awaitTermination executor 10 TimeUnit/SECONDS))))))

(deftest every-task-runs-exactly-once-on-a-thread-of-its-connection
  (testing "bursts from one dispatching thread over interleaved connections, each
  burst awaited, so a task left queued at the end of a burst fails the test;
  connection ids are retired and new ones come, as connections close and open"
    (let [n 300000
          real (Executors/newVirtualThreadPerTaskExecutor)
          states (java.util.Collections/newSetFromMap (java.util.IdentityHashMap.))
          executor (proxy [AbstractExecutorService] []
                     (execute [task]
                       (when-not (fn? task) (.add states task))
                       (.execute real task))
                     (shutdown [] (.shutdown real))
                     (shutdownNow [] (.shutdownNow real))
                     (isShutdown [] (.isShutdown real))
                     (isTerminated [] (.isTerminated real))
                     (awaitTermination [t unit] (.awaitTermination real t unit)))
          dispatch (request-dispatch/create executor)
          runs (AtomicIntegerArray. n)
          conn-of (long-array n)
          thread-of (AtomicLongArray. n)
          conns-of-thread (ConcurrentHashMap.)]
      (try
        (loop [start 0]
          (when (< start n)
            (let [end (min n (+ start 1 (mod (* start 31) 48)))
                  done (CountDownLatch. (- end start))
                  first-id (inc (quot start 2000))]
              (doseq [i (range start end)]
                (let [id (+ first-id (mod (* i 7) 5))]
                  (aset conn-of i id)
                  (request-dispatch/dispatch!
                   dispatch id
                   (fn []
                     (when (zero? (mod i 997))
                       (LockSupport/parkNanos 300000))
                     (let [tid (.threadId (Thread/currentThread))]
                       (.set thread-of i tid)
                       (.putIfAbsent conns-of-thread tid (ConcurrentHashMap/newKeySet))
                       (.add ^java.util.Set (.get conns-of-thread tid) id))
                     (.incrementAndGet runs i)
                     (.countDown done))))
                (when (zero? (mod i 64))
                  (request-dispatch/sweep! dispatch)))
              (if (await-burst! dispatch done)
                (recur end)
                (is false (str "tasks " start " to " end " did not all run"))))))
        (is (= n (count (filter #(= 1 (.get runs %)) (range n)))))
        (is (every? #(= 1 (count %)) (vals conns-of-thread))
            "no thread ran tasks of two connections")
        (is (< (count conns-of-thread) n) "threads are shared within connections")
        (is (< (count states) 40)
            (str "states of ended connections are released and reused: "
                 (count states) " states for " (aget conn-of (dec n)) " connection ids"))
        (finally
          (.shutdown executor)
          (.awaitTermination executor 10 TimeUnit/SECONDS))))))

(defn- interrupted-await
  "Calls [[request-dispatch/await-stop-moves!]] with a 1000 ms timeout from
  this thread with its interrupt flag set. Returns the result, the call's
  milliseconds and whether the flag was still set, and clears the flag."
  [dispatch]
  (.interrupt (Thread/currentThread))
  (try
    (let [start (System/nanoTime)
          moved? (request-dispatch/await-stop-moves! dispatch 1000)
          ms (/ (- (System/nanoTime) start) 1e6)
          interrupted? (Thread/interrupted)]
      {:moved? moved? :ms ms :interrupted? interrupted?})
    (finally
      (Thread/interrupted))))

(deftest stop-wait-ends-when-its-thread-is-interrupted
  (let [{:keys [dispatch]} (held)]
    (request-dispatch/begin-stop! dispatch)
    (testing "before the sweep ran, the wait ends at once and reports no sweep"
      (let [{:keys [moved? ms interrupted?]} (interrupted-await dispatch)]
        (is (false? moved?))
        (is (< ms 200) (str "an interrupted wait took " ms " ms"))
        (is interrupted? "the interrupt stays set for the caller")))
    (testing "after the sweep ran, an interrupted caller learns that it ran"
      (request-dispatch/sweep! dispatch)
      (let [{:keys [moved? ms interrupted?]} (interrupted-await dispatch)]
        (is (true? moved?))
        (is (< ms 200))
        (is interrupted?)))))

(deftest failed-start-for-a-queued-task-is-thrown-or-reported
  (let [fail-queued-start! #'request-dispatch/fail-queued-start!
        error (OutOfMemoryError. "unable to create thread")
        task (fn [])]
    (testing "a task still queued is taken back and the throwable rethrown"
      (let [tasks (doto (java.util.concurrent.ConcurrentLinkedQueue.) (.offer task))
            thrown_ (atom nil)]
        (is (empty? (run-on-thread! #(reset! thrown_ (try (fail-queued-start! tasks task error)
                                                          nil
                                                          (catch Throwable t t))))))
        (is (identical? error @thrown_))
        (is (.isEmpty tasks))))
    (testing "a task an older drainer already took runs there, and the throwable is reported"
      (is (= [error] (run-on-thread!
                      #(fail-queued-start! (java.util.concurrent.ConcurrentLinkedQueue.) task error)))))
    (testing "a rejection, as a stop can cause, of a task an older drainer already took is dropped"
      (is (empty? (run-on-thread!
                   #(fail-queued-start! (java.util.concurrent.ConcurrentLinkedQueue.) task
                                        (RejectedExecutionException. "shut down"))))))))

(deftest start-error-of-a-task-a-drainer-took-is-reported
  (testing "the worker's second dispatch of a connection races its drainer's
  last poll, and the executor throws an Error for that start, after giving
  the old drainer up to 10 microseconds to take the queued task; each Error is
  thrown from dispatch! or, when the drainer already took the task, reported"
    (let [rounds 20000
          real (Executors/newVirtualThreadPerTaskExecutor)
          fail? (AtomicBoolean.)
          second-ran_ (java.util.concurrent.atomic.AtomicReference.)
          thrown-by-executor (java.util.concurrent.atomic.AtomicInteger.)
          executor (proxy [AbstractExecutorService] []
                     (execute [task]
                       (if (.get fail?)
                         (let [^AtomicBoolean second-ran (.get second-ran_)
                               deadline (+ (System/nanoTime) 10000)]
                           (while (and (not (.get second-ran)) (< (System/nanoTime) deadline))
                             (Thread/onSpinWait))
                           (.incrementAndGet thrown-by-executor)
                           (throw (OutOfMemoryError. "unable to create thread")))
                         (.execute real task)))
                     (shutdown [] (.shutdown real))
                     (shutdownNow [] (.shutdownNow real))
                     (isShutdown [] (.isShutdown real))
                     (isTerminated [] (.isTerminated real))
                     (awaitTermination [t unit] (.awaitTermination real t unit)))
          dispatch (request-dispatch/create executor)
          thrown_ (atom 0)
          reported (run-on-thread!
                    (fn []
                      (dotimes [r rounds]
                        (let [finishing (AtomicBoolean.)
                              second-ran (AtomicBoolean.)
                              id (inc (quot r 50))]
                          (.set second-ran_ second-ran)
                          (request-dispatch/dispatch! dispatch id #(.set finishing true))
                          (when (await-flag! finishing)
                            (spin-nanos (mod (* r 37) 400))
                            (.set fail? true)
                            (try
                              (request-dispatch/dispatch! dispatch id #(.set second-ran true))
                              (catch OutOfMemoryError _
                                (swap! thrown_ inc)))
                            (.set fail? false)
                            (request-dispatch/sweep! dispatch))))))]
      (try
        (is (= (.get thrown-by-executor) (+ @thrown_ (count reported)))
            (str (.get thrown-by-executor) " errors from the executor, " @thrown_
                 " thrown from dispatch!, " (count reported) " reported"))
        (is (every? #(instance? OutOfMemoryError %) reported))
        (finally
          (.shutdown real)
          (.awaitTermination real 10 TimeUnit/SECONDS))))))
