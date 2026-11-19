(ns ^:no-doc ol.busker.capacity-waiters
  "Parks threads that wait for capacity and wakes one for each unit released.

  The queue takes no lock, so the event-loop thread can call [[signal!]] after
  each release. The caller keeps the capacity state: a wake tells a waiter to
  try again, it does not give it capacity."
  (:import
   [java.lang.invoke VarHandle]
   [java.util.concurrent ConcurrentLinkedDeque]
   [java.util.concurrent.atomic AtomicInteger]
   [java.util.concurrent.locks LockSupport]))

(set! *warn-on-reflection* true)

(def ^:private ^:const waiting 0)
(def ^:private ^:const woken 1)
(def ^:private ^:const left 2)

(deftype Waiter [^Thread thread ^AtomicInteger state])

(defn create
  "Returns an empty waiter queue."
  ^ConcurrentLinkedDeque []
  (ConcurrentLinkedDeque.))

(defn- wake!
  [^Waiter waiter]
  (when (.compareAndSet ^AtomicInteger (.-state waiter) waiting woken)
    (LockSupport/unpark (.-thread waiter))
    true))

(defn signal!
  "Wakes up to `n` threads that wait in [[await!]] on `waiters`, oldest first.
  Call it after the change that frees `n` units of capacity."
  [^ConcurrentLinkedDeque waiters ^long n]
  (when (pos? n)
    (VarHandle/fullFence)
    (loop [n n]
      (when (pos? n)
        (when-let [waiter (.pollFirst waiters)]
          (recur (if (wake! waiter) (dec n) n)))))))

(defn signal-all!
  "Wakes every thread that waits on `waiters`. Call it after the change that
  makes [[await!]] attempts stop waiting, such as closure."
  [^ConcurrentLinkedDeque waiters]
  (VarHandle/fullFence)
  (loop []
    (when-let [waiter (.pollFirst waiters)]
      (wake! waiter)
      (recur))))

(defn- join!
  [^ConcurrentLinkedDeque waiters thread]
  (let [waiter (Waiter. thread (AtomicInteger. waiting))]
    (.offerLast waiters waiter)
    (VarHandle/fullFence)
    waiter))

(defn- leave!
  "Removes `waiter` from `waiters` when no signal reached it, so the queue holds
  only live waiters, and passes one wake on when a signal picked it."
  [^ConcurrentLinkedDeque waiters ^Waiter waiter]
  (if (.compareAndSet ^AtomicInteger (.-state waiter) waiting left)
    (.removeLastOccurrence waiters waiter)
    (signal! waiters 1)))

(defn await!
  "Calls `attempt!` until it returns non-nil, and returns that value. Parks the
  thread on `waiters` between attempts.

  `attempt!` returns nil when no capacity is free, a keyword when the caller
  stops without capacity, for example after closure, and another value when
  it took capacity. A waiter rechecks after it joins the queue, so a release
  that comes before the park is not lost. An unpark of the thread makes it try
  again. Interruption throws InterruptedException and clears the interrupt
  status. An exception from `attempt!` ends the wait."
  [^ConcurrentLinkedDeque waiters attempt!]
  (or (attempt!)
      (let [thread (Thread/currentThread)]
        (loop [^Waiter waiter (join! waiters thread)]
          (let [result (try (attempt!)
                            (catch Throwable t
                              (leave! waiters waiter)
                              (throw t)))]
            (cond
              (some? result)
              (do (leave! waiters waiter)
                  result)

              (= woken (.get ^AtomicInteger (.-state waiter)))
              (recur (join! waiters thread))

              :else
              (do (LockSupport/park waiters)
                  (when (Thread/interrupted)
                    (leave! waiters waiter)
                    (throw (InterruptedException.)))
                  (recur waiter))))))))
