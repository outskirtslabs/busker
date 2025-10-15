(ns ol.h2o.byte-bounded-queue
  (:import [java.util.concurrent LinkedTransferQueue Semaphore]))

(defprotocol Sized
  "Return the logical size of a value in bytes."
  (byte-size [x]
    "Return the size of `x` in bytes as a non-negative long."))

(defprotocol ByteBoundedQueue
  "Byte-bounded queue contract. Implementations must enforce back-pressure
   based solely on total enqueued bytes as reported by the `Sized` protocol.
   A single item whose `(byte-size item)` exceeds `capacity-bytes` MUST be
   rejected to avoid deadlock."

  (capacity-bytes [q]
    "Return the fixed maximum number of bytes allowed to be enqueued at once.")

  (queued-bytes [q]
    "Return the current sum of `(byte-size item)` across all enqueued items.")

  (remaining-bytes [q]
    "Return the remaining byte budget: capacity - queued.")

  (closed? [q]
    "Return true if the queue is closed for new puts/offers.")

  (close [q]
    "Close the queue for producers. Existing items remain available to consumers.
     Idempotent. Returns nil or the queue.")

  ;; --- Producer operations ---
  (put [q item]
    "Blocking put. Uses `(byte-size item)` to determine how many bytes to reserve
     from the byte budget, then enqueues `item`. Blocks until sufficient bytes are
     available. Throws IllegalArgumentException if `(byte-size item) > capacity`.
     Throws IllegalStateException if closed. Returns nil or the queue; MUST not
     drop the item silently.")

  ;; --- Consumer operations ---
  (drain [q max-bytes]
    "Non-blocking bulk take. Removes items whose combined `(byte-size item)` values
     do not exceed `max-bytes`, returning them as a vector. May return fewer items if the
     queue becomes empty."))

(defn- ensure-open [closed?_]
  (when @closed?_ (throw (IllegalStateException. "Queue closed for puts"))))

(deftype ^:private ByteBoundedLinkedTransferQueue
         [^LinkedTransferQueue q
          ^long bytes-capacity
          ^Semaphore permits
          closed?_]
  ByteBoundedQueue
  (capacity-bytes [_] bytes-capacity)
  (queued-bytes [_] (- bytes-capacity (.availablePermits permits)))
  (remaining-bytes [_] (.availablePermits permits))
  (closed? [_] @closed?_)
  (close [_] (reset! closed?_ true))
  (put [_ item]
    (ensure-open closed?_)
    (let [b (long (byte-size item))]
      (assert (not (neg? b)) "bytes must be >= 0")
      (when (> b bytes-capacity) (throw (IllegalArgumentException. (str "Chunk bytes " b " exceed capacity " bytes-capacity))))
      (when (pos? b) (.acquire permits (int b)))
      (try
        (.put q item)
        (catch Throwable t
          (when (pos? b) (.release permits (int b)))
          (throw t)))))
  (drain [_ max-bytes]
    (let [limit max-bytes]
      (loop [rem limit
             acc (transient [])]
        (if (<= rem 0)
          (persistent! acc)
          (if-some [head (.peek q)]
            (let [b (long (byte-size head))]
              (if (> b rem)
                (persistent! acc)
                (let [polled (.poll q)]
                  (assert (identical? polled head) "drain observed head change; multiple consumers are unsupported")
                  (when (pos? b) (.release permits (int b)))
                  (recur (- rem b) (conj! acc polled)))))
            (persistent! acc)))))))

(defn byte-bounded-spsc-queue
  "Create a single-producer single-consumer queue bounded by the (byte-size ..) of the items, not the total number of items."
  [capacity-bytes & _opts]
  (when (<= capacity-bytes 0)
    (throw (IllegalArgumentException. "capacity bytes must be > 0")))
  (->ByteBoundedLinkedTransferQueue (LinkedTransferQueue.)
                                    capacity-bytes
                                    (Semaphore. (int capacity-bytes) false)
                                    (atom false)))
