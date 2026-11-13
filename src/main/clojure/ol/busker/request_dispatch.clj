(ns ^:no-doc ol.busker.request-dispatch
  "Runs the quick requests of one connection one after another on one virtual
  thread, for `:request-threads :reuse-within-connection`.

  A request that arrives while no drainer runs for its connection starts a
  virtual thread through the generation's request executor, as each request
  does with `:per-request`. That thread, the drainer, runs its request, then
  the requests of the same connection that the worker queued meanwhile, one
  after another, and ends when the queue is empty. Once a drainer has run for
  longer than 100 microseconds, whatever its handlers took each, a new request
  of its connection gets a new thread and [[sweep!]] starts a new thread for
  each queued request of that connection, until the drainer finds its queue
  empty and ends. The worker sweeps once per loop turn and waits at most 1 ms
  while requests are queued, so a request waits in the queue for at most about
  1 ms plus one worker loop turn. A drainer runs requests of one connection
  only. When a stop begins, [[begin-stop!]] makes the next sweep start a new
  thread for every queued request, before the executor shuts down, and later
  requests go to the executor directly.

  Each worker keeps a table from connection id to drain state, a free list of
  states and a list of the states with queued tasks. [[dispatch!]] and
  [[sweep!]] run on the worker thread only; a drainer touches only the state
  it drains and the worker's stack of states whose drainers ended.

  ## Related Namespaces

  - [[ol.busker.request]] dispatches each request.
  - [[ol.busker.generation]] creates one dispatch per worker, calls [[sweep!]]
    once per worker loop turn and from the hot poll, [[begin-stop!]] and
    [[await-stop-moves!]] before it shuts the executor down, and
    [[interrupt-remaining!]] before it interrupts the executor's threads."
  (:import
   [java.util ArrayDeque ArrayList]
   [java.util.concurrent ConcurrentLinkedQueue CountDownLatch ExecutorService
    RejectedExecutionException TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong AtomicReference]))

(set! *warn-on-reflection* true)

;; A request should not wait behind a handler for longer than waking a carrier thread costs.
(def ^:private ^:const slow-nanos 100000)

(definterface LongTable
  (lookup [^long k])
  (^void insert [^long k v])
  (^void delete [^long k])
  (^long size []))

(defn- home-slot
  ^long [^long k ^long mask]
  (bit-and (unsigned-bit-shift-right (unchecked-multiply k -7046029254386353131) 32) mask))

;; Open addressing with linear probing over positive keys; 0 marks a free slot.
;; Deletion shifts later entries of the probe run back, so there are no
;; tombstones. Used by the worker thread only.
(deftype ConnectionTable [^:unsynchronized-mutable ^longs ks
                          ^:unsynchronized-mutable ^objects vs
                          ^:unsynchronized-mutable ^long n]
  LongTable
  (lookup [_ k]
    (let [mask (dec (alength ks))]
      (loop [i (home-slot k mask)]
        (let [x (aget ks i)]
          (cond
            (== x k) (aget vs i)
            (zero? x) nil
            :else (recur (bit-and (inc i) mask)))))))
  (insert [_ k v]
    (when-not (pos? k)
      (throw (IllegalArgumentException. (str "Connection id must be positive: " k))))
    (when (> (* 2 (inc n)) (alength ks))
      (let [old-ks ks
            old-vs vs
            capacity (* 2 (alength old-ks))
            mask (dec capacity)
            new-ks (long-array capacity)
            new-vs (object-array capacity)]
        (dotimes [j (alength old-ks)]
          (let [x (aget old-ks j)]
            (when-not (zero? x)
              (loop [i (home-slot x mask)]
                (if (zero? (aget new-ks i))
                  (do (aset new-ks i x)
                      (aset new-vs i (aget old-vs j)))
                  (recur (bit-and (inc i) mask)))))))
        (set! ks new-ks)
        (set! vs new-vs)))
    (let [mask (dec (alength ks))]
      (loop [i (home-slot k mask)]
        (let [x (aget ks i)]
          (cond
            (== x k) (aset vs i v)
            (zero? x) (do (aset ks i k)
                          (aset vs i v)
                          (set! n (inc n)))
            :else (recur (bit-and (inc i) mask)))))))
  (delete [_ k]
    (let [mask (dec (alength ks))
          found (loop [i (home-slot k mask)]
                  (let [x (aget ks i)]
                    (cond
                      (== x k) i
                      (zero? x) -1
                      :else (recur (bit-and (inc i) mask)))))]
      (when-not (neg? found)
        (set! n (dec n))
        (loop [hole found
               j (bit-and (inc found) mask)]
          (let [x (aget ks j)]
            (if (zero? x)
              (do (aset ks hole 0)
                  (aset vs hole nil))
              (let [h (home-slot x mask)
                    stays? (if (<= hole j)
                             (and (< hole h) (<= h j))
                             (or (< hole h) (<= h j)))]
                (if stays?
                  (recur hole (bit-and (inc j) mask))
                  (do (aset ks hole x)
                      (aset vs hole (aget vs j))
                      (recur j (bit-and (inc j) mask)))))))))))
  (size [_] n))

(deftype RequestDispatch [^ExecutorService executor
                          ^AtomicBoolean interrupt-tasks?
                          ^AtomicBoolean stopping?
                          ;; Counted down by the first sweep after stopping? is set.
                          ^CountDownLatch stop-moved
                          ;; Head of the stack of states whose last drainer
                          ;; ended, linked through their next field.
                          ^AtomicReference ended
                          ^ConnectionTable table
                          ^ArrayDeque free
                          ^ArrayList queued])

(definterface Drain
  (takeFirstTask [])
  (^void setFirstTask [task])
  (nextEnded [])
  (^void setNextEnded [state])
  (^long connectionId [])
  (^void setConnectionId [^long id])
  (^boolean queuedListed [])
  (^void setQueuedListed [^boolean listed]))

(declare run-drainer!)

(deftype DrainState [^RequestDispatch dispatch
                     ^ConcurrentLinkedQueue tasks
                     ^AtomicBoolean active?
                     ;; System/nanoTime when the timed drainer started its
                     ;; current run, 0 when no timed drainer runs.
                     ^AtomicLong drain-start
                     ;; Odd while the worker moves queued tasks.
                     ^AtomicLong move-seq
                     ;; Drainer threads running on this state or starting.
                     ^AtomicInteger drainers
                     ;; True while the state is on the ended stack.
                     ^AtomicBoolean ended?
                     ^:volatile-mutable first-task
                     ^:volatile-mutable next-ended
                     ^:unsynchronized-mutable ^long connection-id
                     ^:unsynchronized-mutable ^boolean queued-listed?]
  Drain
  (takeFirstTask [_]
    (let [task first-task]
      (set! first-task nil)
      task))
  (setFirstTask [_ task] (set! first-task task))
  (nextEnded [_] next-ended)
  (setNextEnded [_ state] (set! next-ended state))
  (connectionId [_] connection-id)
  (setConnectionId [_ id] (set! connection-id id))
  (queuedListed [_] queued-listed?)
  (setQueuedListed [_ listed] (set! queued-listed? listed))

  Runnable
  (run [this] (run-drainer! this)))

(defn create
  "Returns the dispatch state of one worker. Drainers and handler threads start
  through `executor`."
  [^ExecutorService executor]
  (RequestDispatch. executor (AtomicBoolean.) (AtomicBoolean.) (CountDownLatch. 1)
                    (AtomicReference.)
                    (ConnectionTable. (long-array 16) (object-array 16) 0)
                    (ArrayDeque.) (ArrayList.)))

(defn- slow?
  [^DrainState state]
  (let [start (.get ^AtomicLong (.-drain-start state))]
    (and (not (zero? start))
         (> (- (System/nanoTime) start) slow-nanos))))

(defn- report!
  [^Throwable t]
  (let [thread (Thread/currentThread)]
    (try
      (.uncaughtException (.getUncaughtExceptionHandler thread) thread t)
      (catch Throwable _ nil))))

(defn- run-task!
  [^DrainState state timed? ^Runnable task]
  (let [^AtomicLong drain-start (.-drain-start state)]
    (when (.get ^AtomicBoolean (.-interrupt-tasks? ^RequestDispatch (.-dispatch state)))
      (.interrupt (Thread/currentThread)))
    (when (and timed? (zero? (.get drain-start)))
      (let [now (System/nanoTime)]
        (.set drain-start (if (zero? now) 1 now))))
    (try
      (.run task)
      (catch Throwable t
        (report! t)))
    (Thread/interrupted)))

(defn- final-poll
  "Polls once more after the drainer cleared `active?`. A task the worker took
  out of the queue to move it can come back when the executor rejects it, so an
  empty poll that overlapped a move waits for the move to end and polls again.
  The wait spins briefly, then yields the carrier on each check, so a worker
  preempted in the middle of a move does not keep a carrier busy."
  [^DrainState state]
  (let [^ConcurrentLinkedQueue tasks (.-tasks state)
        ^AtomicLong move-seq (.-move-seq state)]
    (loop []
      (let [before (.get move-seq)]
        (or (.poll tasks)
            (when-not (and (even? before) (= before (.get move-seq)))
              (loop [spins 0]
                (when (odd? (.get move-seq))
                  (if (< spins 100)
                    (Thread/onSpinWait)
                    (Thread/yield))
                  (recur (inc spins))))
              (recur)))))))

(defn- drain!
  "Runs queued tasks until the queue is empty. Only the drainer that holds
  `active?` records its start time; one that loses `active?` to a newer drainer
  keeps running tasks untimed so none are left queued."
  [^DrainState state]
  (let [^ConcurrentLinkedQueue tasks (.-tasks state)
        ^AtomicBoolean active? (.-active? state)]
    (loop [timed? true]
      (if-let [task (.poll tasks)]
        (do (run-task! state timed? task)
            (recur timed?))
        (when timed?
          (.set ^AtomicLong (.-drain-start state) 0)
          (.set active? false)
          (when-let [task (final-poll state)]
            (let [timed? (.compareAndSet active? false true)]
              (run-task! state timed? task)
              (recur timed?))))))))

(defn- push-ended!
  [^DrainState state]
  (when (.compareAndSet ^AtomicBoolean (.-ended? state) false true)
    (let [^AtomicReference head (.-ended ^RequestDispatch (.-dispatch state))]
      (loop []
        (let [top (.get head)]
          (.setNextEnded state top)
          (when-not (.compareAndSet head top state)
            (recur)))))))

(defn- run-drainer!
  [^DrainState state]
  (try
    (when-let [task (.takeFirstTask state)]
      (run-task! state true task))
    (drain! state)
    (finally
      (when (zero? (.decrementAndGet ^AtomicInteger (.-drainers state)))
        (push-ended! state)))))

;; No lost task: a state leaves the table only here, on the worker, when no
;; drainer of it runs or is starting and its queue is empty. Only the worker
;; queues tasks and starts drainers, so nothing can reach the state afterwards,
;; and a closed connection, whose id never comes back, holds no task. Counting
;; drainers rather than reading active? keeps a drainer that lost active? to a
;; newer one from polling the queue after the state serves another connection.
(defn- release-if-idle!
  [^RequestDispatch dispatch ^DrainState state]
  (let [id (.connectionId state)]
    (when (and (not (zero? id))
               (zero? (.get ^AtomicInteger (.-drainers state)))
               (not (.get ^AtomicBoolean (.-active? state)))
               (.isEmpty ^ConcurrentLinkedQueue (.-tasks state)))
      (.delete ^ConnectionTable (.-table dispatch) id)
      (.setConnectionId state 0)
      (.set ^AtomicLong (.-drain-start state) 0)
      (.push ^ArrayDeque (.-free dispatch) state))))

(defn- state-for!
  ^DrainState [^RequestDispatch dispatch ^long id]
  (let [^ConnectionTable table (.-table dispatch)]
    (or (.lookup table id)
        (let [^DrainState state
              (or (.poll ^ArrayDeque (.-free dispatch))
                  (DrainState. dispatch (ConcurrentLinkedQueue.) (AtomicBoolean.) (AtomicLong.)
                               (AtomicLong.) (AtomicInteger.) (AtomicBoolean.) nil nil 0 false))]
          (.setConnectionId state id)
          (.insert table id state)
          state))))

(defn- start-drainer!
  "Starts a drainer for `state`, which holds `active?`. When the executor throws,
  undoes the start, clears `active?` and rethrows."
  [^RequestDispatch dispatch ^DrainState state first-task]
  (let [^AtomicInteger drainers (.-drainers state)]
    (.incrementAndGet drainers)
    (.setFirstTask state first-task)
    (try
      (.execute ^ExecutorService (.-executor dispatch) state)
      (catch Throwable t
        (.setFirstTask state nil)
        (.decrementAndGet drainers)
        (.set ^AtomicBoolean (.-active? state) false)
        (throw t)))))

(defn- list-queued!
  [^RequestDispatch dispatch ^DrainState state]
  (when-not (.queuedListed state)
    (.setQueuedListed state true)
    (.add ^ArrayList (.-queued dispatch) state)))

(defn- fail-queued-start!
  "Handles throwable `t` from starting a drainer for the queued `task`: rethrows
  it when `task` can still be taken back from `tasks`. Otherwise an older
  drainer took `task` and runs it, so a rejection, which a stop can cause, is
  dropped and any other throwable is reported."
  [^ConcurrentLinkedQueue tasks task ^Throwable t]
  (cond
    (.remove tasks task) (throw t)
    (instance? RejectedExecutionException t) nil
    :else (report! t)))

(defn dispatch!
  "Runs `task` of the connection with H2O id `connection-id` (positive) on a
  new drainer, queues it for that connection's active drainer, or gives it a
  new thread once the drainer has run for longer than the threshold or a stop
  has begun. Throws RejectedExecutionException, with `task` not queued, once
  the executor is shut down, and rethrows whatever else the executor throws,
  with `task` not queued."
  [^RequestDispatch dispatch ^long connection-id ^Runnable task]
  (let [^ExecutorService executor (.-executor dispatch)]
    (when (.isShutdown executor)
      (throw (RejectedExecutionException. "Request executor is shut down")))
    (if (.get ^AtomicBoolean (.-stopping? dispatch))
      (.execute executor task)
      (let [^DrainState state (state-for! dispatch connection-id)
            ^ConcurrentLinkedQueue tasks (.-tasks state)
            ^AtomicBoolean active? (.-active? state)]
        (cond
          (and (not (.get active?)) (.compareAndSet active? false true))
          (try
            (start-drainer! dispatch state task)
            (catch Throwable t
              (release-if-idle! dispatch state)
              (throw t)))

          (slow? state)
          (.execute executor task)

          :else
          ;; No lost task: the worker offers, then reads active?; a drainer clears
          ;; active?, then polls. Either the read sees false and the worker starts a
          ;; drainer, or the clear comes after the read and the drainer's poll comes
          ;; after the offer. A worker compare-and-set that fails means a drainer
          ;; took active? back and drains until the queue is empty.
          (do
            (.offer tasks task)
            (list-queued! dispatch state)
            (when (and (not (.get active?)) (.compareAndSet active? false true))
              (try
                (start-drainer! dispatch state nil)
                (catch Throwable t
                  (fail-queued-start! tasks task t))))))))))

(defn- move-queued!
  "Hands queued tasks to the executor until the queue is empty or the executor
  rejects one, which goes back in the queue."
  [^ExecutorService executor ^ConcurrentLinkedQueue tasks]
  (loop []
    (when-let [^Runnable task (.poll tasks)]
      (when (try
              (.execute executor task)
              true
              (catch RejectedExecutionException _
                (.offer tasks task)
                false))
        (recur)))))

(defn- move-if-slow!
  "Moves each queued task of `state` to a new thread when its drainer
  is slow or, with `all?`, whatever its drainer does."
  [^RequestDispatch dispatch ^DrainState state all?]
  (let [^ExecutorService executor (.-executor dispatch)
        ^AtomicLong move-seq (.-move-seq state)]
    (when (and (or all? (slow? state)) (not (.isShutdown executor)))
      ;; Mark the move before checking that a timed drainer still runs: that
      ;; drainer's final poll then starts after the mark and sees any task
      ;; put back, so a task put back always has a drainer to run it.
      (.incrementAndGet move-seq)
      (try
        (when (or all? (not (zero? (.get ^AtomicLong (.-drain-start state)))))
          (move-queued! executor (.-tasks state)))
        (finally
          (.incrementAndGet move-seq))))))

(defn sweep!
  "Runs once per worker loop turn. Releases the states of connections whose
  drainers ended and are idle, and starts a new thread for every queued task of
  a connection whose drainer has run for longer than the threshold, or of every
  connection in the first sweep after [[begin-stop!]], unless the executor is
  shut down. Returns true when tasks remain queued."
  [^RequestDispatch dispatch]
  (loop [^DrainState state (.getAndSet ^AtomicReference (.-ended dispatch) nil)]
    (when state
      (let [next-state (.nextEnded state)]
        (.setNextEnded state nil)
        (.set ^AtomicBoolean (.-ended? state) false)
        (release-if-idle! dispatch state)
        (recur next-state))))
  (let [^ArrayList queued (.-queued dispatch)
        ^CountDownLatch stop-moved (.-stop-moved dispatch)
        all? (and (.get ^AtomicBoolean (.-stopping? dispatch))
                  (pos? (.getCount stop-moved)))
        queued? (loop [i 0
                       queued? false]
                  (if (< i (.size queued))
                    (let [^DrainState state (.get queued i)]
                      (move-if-slow! dispatch state all?)
                      (if (.isEmpty ^ConcurrentLinkedQueue (.-tasks state))
                        (let [last-index (dec (.size queued))]
                          (.set queued i (.get queued last-index))
                          (.remove queued (int last-index))
                          (.setQueuedListed state false)
                          (release-if-idle! dispatch state)
                          (recur i queued?))
                        (recur (inc i) true)))
                    queued?))]
    (when all?
      (.countDown stop-moved))
    queued?))

(defn queued?
  "Returns true when tasks of some connection of `dispatch` are queued. Worker
  thread only."
  [^RequestDispatch dispatch]
  (not (.isEmpty ^ArrayList (.-queued dispatch))))

(defn begin-stop!
  "Makes the next [[sweep!]] start a new thread for every queued task, and
  [[dispatch!]] hand every later task to the executor directly. Safe from any
  thread; call before the executor shuts down, then wake the worker."
  [^RequestDispatch dispatch]
  (.set ^AtomicBoolean (.-stopping? dispatch) true))

(defn await-stop-moves!
  "Waits up to `timeout-ms` for the first [[sweep!]] after [[begin-stop!]].
  Returns true when that sweep has run, also for an interrupted caller, and
  false when the wait timed out or was interrupted before it ran. An interrupt
  ends the wait and stays set."
  [^RequestDispatch dispatch ^long timeout-ms]
  (let [^CountDownLatch stop-moved (.-stop-moved dispatch)]
    (try
      (.await stop-moved timeout-ms TimeUnit/MILLISECONDS)
      (catch InterruptedException _
        (.interrupt (Thread/currentThread))
        (zero? (.getCount stop-moved))))))

(defn interrupt-remaining!
  "Makes drainers interrupt themselves before each remaining task, as
  `ExecutorService/shutdownNow` interrupts handler threads."
  [^RequestDispatch dispatch]
  (.set ^AtomicBoolean (.-interrupt-tasks? dispatch) true))
