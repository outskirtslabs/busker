(ns ol.h2o.byte-bounded-queue
  "Byte-bounded queue for backpressure-aware streaming.

  This namespace provides a queue that enforces backpressure based on the total
  byte size of enqueued items rather than item count. This is essential for
  streaming scenarios where items might vary greatly in size (e.g., HTTP response chunks)
  and memory usage must be controlled precisely.

  ## How It Works

  Instead of limiting the number of items, the queue tracks the cumulative byte
  size of all enqueued items using the `Sized` protocol. When a producer tries
  to enqueue an item:

  1. The queue calculates the item's byte size via `(byte-size item)`
  2. If adding this item would exceed the byte capacity, the producer blocks
  3. Once a consumer drains items, their byte size is returned to the budget
  4. Blocked producers resume as soon as sufficient bytes become available

  This ensures predictable memory usage even with highly variable item sizes.

  ## Key Guarantees

  - No Oversized Items: Items larger than the total capacity are rejected
    immediately with `IllegalArgumentException` to prevent deadlock.

  - Single Producer/Single Consumer: Multiple consumers will trigger assertion failures.

  - Graceful Shutdown: After `close`, producers are rejected but existing
    items remain available for consumers to drain.

  - Atomic Byte Accounting: All byte budget operations are thread-safe and
    use semaphores to coordinate between producer and consumer threads.

  ## Usage Example

  ```clojure
  (require '[ol.h2o.byte-bounded-queue :as bbq])

  ;; Define how to measure your items
  (extend-protocol bbq/Sized
    String
    (byte-size [s] (.length s)))

  ;; Create a queue with 1KB capacity
  (def q (bbq/byte-bounded-spsc-queue 1024))

  ;; Producer thread blocks when capacity exceeded
  (bbq/put q \"small\")   ; 5 bytes, returns immediately
  (bbq/put q \"medium...\") ; blocks if would exceed 1024 total bytes

  ;; Consumer drains up to N bytes at once
  (let [items (bbq/drain q 512)] ; drain up to 512 bytes
    (doseq [item items]
      (println item)))

  (bbq/close q)
  ```

  ## Integration with Buffer Pool

  This queue works well with `ByteBuffer` instances from `buffer-pool` by
  implementing `Sized` to return buffer capacity:

  ```clojure
  (extend-protocol bbq/Sized
    java.nio.ByteBuffer
    (byte-size [buf] (.capacity buf)))
  ```"
  (:import [java.util.concurrent LinkedTransferQueue Semaphore]))

(defprotocol Sized
  "Protocol for types that can report their size in bytes.

  Implement this protocol for any type you want to enqueue in a `ByteBoundedQueue`.
  The byte size is used to enforce backpressure and memory limits."
  (byte-size [x]
    "Returns the logical size of `x` in bytes.

    This value determines how much of the queue's byte budget is consumed when
    `x` is enqueued. Must return a non-negative long.

    Arguments:
    - `x` - Value to measure

    Returns:
    - `long` - Size in bytes, must be >= 0

    Examples:
    ```clojure
    (extend-protocol Sized
      String
      (byte-size [s] (.length s))

      java.nio.ByteBuffer
      (byte-size [buf] (.capacity buf))

      (Class/forName \"[B\") ; byte array
      (byte-size [arr] (alength arr)))
    ```"))

(defprotocol ByteBoundedQueue
  "Protocol for queues that enforce backpressure based on byte size.

  Unlike traditional bounded queues that limit item count, byte-bounded queues
  track the cumulative byte size of enqueued items. This prevents memory exhaustion
  when streaming variable-sized data chunks."

  (capacity-bytes [q]
    "Returns the maximum total bytes that can be enqueued simultaneously.

    This is a fixed value set at queue creation time.

    Returns:
    - `long` - Total byte capacity")

  (queued-bytes [q]
    "Returns the current sum of byte sizes for all enqueued items.

    Calculated as: `(reduce + (map byte-size items))`

    Returns:
    - `long` - Currently occupied bytes")

  (remaining-bytes [q]
    "Returns the available byte budget for new items.

    Calculated as: `(- (capacity-bytes q) (queued-bytes q))`

    Returns:
    - `long` - Remaining bytes before producers block")

  (closed? [q]
    "Returns true if the queue is closed to new puts/offers.

    A closed queue rejects all producer operations but allows consumers to
    drain remaining items.

    Returns:
    - `boolean` - `true` if closed, `false` otherwise")

  (close [q]
    "Closes the queue to new producer operations.

    After closing:
    - All `put` operations throw `IllegalStateException`
    - Existing items remain available for `drain` operations
    - Idempotent - calling multiple times is safe

    Returns:
    - `nil` or the queue")

  ;; --- Producer operations ---
  (put [q item]
    "Enqueues an item, blocking until sufficient byte capacity is available.

    This operation:
    1. Calculates `(byte-size item)` to determine required capacity
    2. Blocks if `(+ (queued-bytes q) (byte-size item)) > (capacity-bytes q)`
    3. Enqueues the item once capacity becomes available
    4. Never drops items silently

    Arguments:
    - `item` - Value to enqueue (must satisfy `Sized` protocol)

    Returns:
    - `nil` or the queue

    Throws:
    - `IllegalArgumentException` if `(byte-size item) > (capacity-bytes q)`
    - `IllegalStateException` if queue is closed
    - `InterruptedException` if thread interrupted while blocking

    Example:
    ```clojure
    ;; Blocks until 100 bytes available
    (put q (byte-array 100))
    ```")

  ;; --- Consumer operations ---
  (drain [q max-bytes]
    "Removes and returns items whose cumulative size does not exceed `max-bytes`.

    This is a non-blocking bulk operation that:
    - Drains items in FIFO order
    - Stops when the next item would exceed `max-bytes`
    - Returns immediately even if fewer bytes are available
    - Releases byte capacity for blocked producers

    Arguments:
    - `max-bytes` (long) - Maximum cumulative byte size to drain

    Returns:
    - `vector` - Items drained (may be empty)

    Example:
    ```clojure
    ;; Drain up to 8KB of chunks
    (let [chunks (drain q 8192)]
      (doseq [chunk chunks]
        (process-chunk chunk)))
    ```

    Note: Current implementation assumes single consumer. Multiple concurrent
    consumers will trigger assertion failures."))

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
  (close [_]
    (reset! closed?_ true)
    (let [missing (- bytes-capacity (.availablePermits permits))]
      (when (pos? missing)
        (.release permits (int missing))))
    nil)
  (put [_ item]
    (ensure-open closed?_)
    (let [b (long (byte-size item))]
      (assert (not (neg? b)) "bytes must be >= 0")
      (when (> b bytes-capacity) (throw (IllegalArgumentException. (str "Chunk bytes " b " exceed capacity " bytes-capacity))))
      (when (pos? b) (.acquire permits (int b)))
      (try
        (ensure-open closed?_)
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
  "Creates a single-producer single-consumer queue bounded by byte size.

  This queue enforces backpressure based on the cumulative byte size of enqueued
  items rather than item count. It's designed for streaming scenarios where items
  have variable sizes and precise memory control is required.

  ## Concurrency Model

  The queue is optimized for single producer, single consumer (SPSC) usage:
  - One thread calls `put` to enqueue items
  - One thread calls `drain` to dequeue items
  - Multiple consumers are **not supported** and will cause assertion failures

  ## Memory Behavior

  The queue uses semaphores to coordinate byte capacity between producer and
  consumer threads. When the producer fills the capacity, it blocks until the
  consumer drains enough items to free up space.

  Arguments:
  - `capacity-bytes` (long) - Maximum total bytes that can be queued simultaneously.
    Must be greater than 0. Individual items larger than this value are rejected.

  Returns:
  - Queue instance satisfying `ByteBoundedQueue` protocol

  Throws:
  - `IllegalArgumentException` if `capacity-bytes <= 0`

  Example:
  ```clojure
  (require '[ol.h2o.byte-bounded-queue :as bbq])

  ;; Create queue with 64KB capacity
  (def q (bbq/byte-bounded-spsc-queue (* 64 1024)))

  ;; Producer thread
  (future
    (doseq [chunk large-data-chunks]
      (bbq/put q chunk))) ; blocks when 64KB filled

  ;; Consumer thread
  (loop []
    (when-some [chunks (seq (bbq/drain q 8192))]
      (process-chunks chunks)
      (recur)))
  ```"
  [capacity-bytes & _opts]
  (when (<= capacity-bytes 0)
    (throw (IllegalArgumentException. "capacity bytes must be > 0")))
  (->ByteBoundedLinkedTransferQueue (LinkedTransferQueue.)
                                    capacity-bytes
                                    (Semaphore. (int capacity-bytes) false)
                                    (atom false)))
