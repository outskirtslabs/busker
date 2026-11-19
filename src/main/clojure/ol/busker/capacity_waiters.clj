(ns ^:no-doc ol.busker.capacity-waiters
  "Parks threads that wait for capacity and wakes one for each unit released.

  The queue takes no lock, so the event-loop thread can call [[signal!]] after
  each release. The caller keeps the capacity state: a wake tells a waiter to
  try again, it does not give it capacity."
  (:import
   [java.lang.invoke VarHandle]
   [java.util.concurrent ConcurrentLinkedDeque]
   [java.util.concurrent.atomic AtomicReference]
   [java.util.concurrent.locks LockSupport]))

(set! *warn-on-reflection* true)

(defn create
  "Returns an empty waiter queue."
  ^ConcurrentLinkedDeque []
  (ConcurrentLinkedDeque.))

(defn- wake!
  [^AtomicReference waiter]
  (let [thread (.get waiter)]
    (when (and (instance? Thread thread)
               (.compareAndSet waiter thread :woken))
      (LockSupport/unpark thread)
      true)))

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
  (let [waiter (AtomicReference. thread)]
    (.offerLast waiters waiter)
    (VarHandle/fullFence)
    waiter))

(defn- leave!
  [waiters ^AtomicReference waiter thread]
  (when-not (.compareAndSet waiter thread :left)
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
        (loop [^AtomicReference waiter (join! waiters thread)]
          (let [result (try (attempt!)
                            (catch Throwable t
                              (leave! waiters waiter thread)
                              (throw t)))]
            (cond
              (some? result)
              (do (leave! waiters waiter thread)
                  result)

              (identical? :woken (.get waiter))
              (recur (join! waiters thread))

              :else
              (do (LockSupport/park waiters)
                  (when (Thread/interrupted)
                    (leave! waiters waiter thread)
                    (throw (InterruptedException.)))
                  (recur waiter))))))))
