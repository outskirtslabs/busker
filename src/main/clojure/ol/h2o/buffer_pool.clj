(ns ol.h2o.buffer-pool
  "ByteBuffer pooling response buffers

  This namespace provides a thread-safe pool that reuses ByteBuffer instances
  to minimize garbage collection pressure during high-throughput I/O operations.
  Buffers are organized into size-aligned buckets using a factor-based scheme.

  ## How It Works

  The pool groups buffers into buckets by capacity. When you request a buffer
  of size N bytes, the pool rounds up to the nearest multiple of the `factor`
  (default 2048), then looks in the corresponding bucket. If a matching buffer
  is available, it's returned immediately. Otherwise, a fresh buffer is allocated.

  When you return a buffer, it's placed back into its bucket if there's room,
  subject to per-bucket and memory limits. Buffers that don't align with bucket
  sizes (e.g., unusual capacities) are rejected and left for GC.

  ## Configuration Options

  - `:factor` (int, default 2048) - Bucket alignment step. Buffer capacities are
    rounded to multiples of this value. Larger factors reduce bucket count but
    may waste space; smaller factors increase bucket count but improve fit.

  - `:min-capacity` (int, default 0) - Minimum pooled capacity. Requests below
    this are rounded up.

  - `:max-capacity` (int, default 65536) - Maximum pooled capacity. Buffers
    larger than this are allocated but never pooled on return.

  - `:max-bucket-size` (int, default ~2x CPU cores) - Maximum buffers per bucket.
    Once a bucket reaches this limit, returned buffers are discarded. Use `-1`
    or `nil` for unbounded buckets.

  - `:max-heap-memory` (long, default unlimited) - Total bytes of heap buffers
    the pool will retain. Additional returns are rejected.

  - `:max-direct-memory` (long, default unlimited) - Total bytes of direct buffers
    the pool will retain. Additional returns are rejected.

  ## Example

  ```clojure
  (require '[ol.h2o.buffer-pool :as pool])

  (def p (pool/make-bytebuffer-pool {:factor 2048
                                      :max-capacity 65536
                                      :max-bucket-size 16}))

  ;; Borrow a 3000-byte direct buffer (rounds up to 4096).
  (let [buf (pool/borrow p 3000 true)]
    ;; Use buf...
    (pool/return p buf)) ;; returns true if pooled

  (pool/dispose p)
  ```

  ## Thread Safety

  All operations are atomic and safe for concurrent access from multiple threads."
  (:import
   [java.nio ByteBuffer]))

(defprotocol BufferPool
  "Protocol for thread-safe ByteBuffer pooling."
  (borrow
    [pool size direct?]
    "Acquires a ByteBuffer with capacity at least `size` bytes.

    The actual capacity may be larger due to bucket alignment. If `size` exceeds
    the pool's `:max-capacity`, a fresh buffer is allocated but won't be pooled
    on return.

    Arguments:
    - `size` (long) - Minimum required capacity in bytes
    - `direct?` (boolean) - `true` for direct (off-heap) buffer, `false` for heap

    Returns:
    - `java.nio.ByteBuffer` with `capacity >= size` and `position` at 0")
  (return
    [pool buffer]
    "Returns a ByteBuffer to the pool for potential reuse.

    The buffer is only pooled if:
    - Its capacity aligns with a bucket size (multiple of `:factor`)
    - The bucket isn't full (under `:max-bucket-size`)
    - Pool memory limits aren't exceeded (`:max-heap-memory` / `:max-direct-memory`)

    Arguments:
    - `buffer` (ByteBuffer) - Buffer to return, or `nil` (ignored)

    Returns:
    - `boolean` - `true` if the buffer was accepted into the pool, `false` if rejected")
  (dispose
    [pool]
    "Clears all pooled buffers and releases resources.

    After calling `dispose`, `borrow` will still allocate fresh buffers, but
    `return` will reject all buffers. Call this during application shutdown.

    Returns:
    - `:disposed` keyword"))

(set! *warn-on-reflection* true)

(def ^:private default-factor 2048)
(def ^:private default-max-capacity 65536)

(defn- ceil-div ^long [^long a ^long b]
  (long (Math/ceil (double (/ a b)))))

(defn- normalize-opts
  [{:keys [min-capacity factor max-capacity max-bucket-size max-heap-memory max-direct-memory]
    :or   {min-capacity 0
           factor       default-factor
           max-capacity default-max-capacity}} n-cpu]
  {:min-capacity      (long (max 0 (long min-capacity)))
   :factor            (long (if (pos? (long factor)) (long factor) default-factor))
   :max-capacity      (long (if (pos? (long max-capacity)) (long max-capacity) default-max-capacity))
   ;; default to ~2x CPU per bucket (simple heuristic).
   :max-bucket-size   (let [m (or max-bucket-size (* 2 n-cpu))]
                        (when (not= m -1) (long m))) ; nil => unbounded
   ;; Treat 0 = heuristic, for now it means unlimited
   :max-heap-memory   (when (and max-heap-memory (pos? (long max-heap-memory)))
                        (long max-heap-memory)) ; nil => unlimited
   :max-direct-memory (when (and max-direct-memory (pos? (long max-direct-memory)))
                        (long max-direct-memory))}) ; nil => unlimited

(defn- bucket-index-for
  "Given a requested size and pool cfg, returns [bucket-index aligned-capacity].
   If size > max-capacity, caller should treat as non-pooled."
  [^long size {:keys [min-capacity factor max-capacity]}]
  (let [size (max size min-capacity)
        idx (ceil-div size factor)
        cap (min (* idx factor) max-capacity)]
    [(int (if (zero? cap) 0 (quot cap factor))) (int cap)]))

(defn- n-buckets
  "Number of buckets needed to cover [0..max-capacity] inclusive."
  [{:keys [factor max-capacity]}]
  ;; Include bucket 0 (capacity 0) and bucket for max-capacity.
  (inc (int (quot max-capacity factor))))

(defn- new-buckets
  "Create a vector of per-bucket atoms (each holds a vector of ByteBuffers)."
  [cfg]
  (vec (repeat (n-buckets cfg) (atom []))))

(defrecord BufferPoolImpl [cfg buckets totals disposed?]
  BufferPool
  (borrow [_ size direct?]
    (let [size (long (max 0 (long size)))]
      (if (> size (:max-capacity cfg))
        ;; Oversized: allocate but don't pool on return (unless aligned and within cap).
        (if direct?
          (ByteBuffer/allocateDirect (int size))
          (ByteBuffer/allocate (int size)))
        (let [[idx cap] (bucket-index-for size cfg)
              bucket (get (if direct? (:direct buckets) (:heap buckets)) idx)
              ;; CAS pop from the bucket (LIFO stack using vector).
              buf (loop []
                    (let [v @bucket]
                      (if-let [b (peek v)]
                        (if (compare-and-set! bucket v (pop v))
                          b
                          (recur))
                        nil)))]
          (if buf
            (do
              ;; Update totals; safe to subtract aligned cap.
              (swap! totals update (if direct? :direct :heap) #(- (long %) cap))
              (.clear ^ByteBuffer buf)
              buf)
            ;; None available: allocate aligned.
            (if direct?
              (ByteBuffer/allocateDirect cap)
              (ByteBuffer/allocate cap)))))))

  (return [_ buffer]
    (if (or (nil? buffer) @disposed?)
      false
      (let [cap (.capacity ^ByteBuffer buffer)
            direct? (.isDirect ^ByteBuffer buffer)
            cap-key (if direct? :direct :heap)
            bucket-vec (if direct? (:direct buckets) (:heap buckets))
            ;; Discard obviously un-poolable buffers.
            _ (when (<= cap 0) (throw (IllegalArgumentException. "Buffer capacity must be > 0")))]
        (if (> cap (:max-capacity cfg))
          false
          (let [[idx aligned] (bucket-index-for cap cfg)]
            (if (not= aligned cap)
                  ;; Only pool buffers that match bucket alignment (avoid weird sizes).
              false
              (let [bucket (get bucket-vec idx)
                        ;; Check bucket size cap.
                    under-bucket-cap?
                    (or (nil? (:max-bucket-size cfg))
                        (< (count @bucket) (:max-bucket-size cfg)))
                        ;; Check memory cap on pooled bytes.
                    next-total (+ (long (@totals cap-key)) cap)
                    mem-cap (if direct? (:max-direct-memory cfg) (:max-heap-memory cfg))
                    under-mem-cap? (or (nil? mem-cap) (<= next-total mem-cap))]
                (if (and under-bucket-cap? under-mem-cap?)
                  (do
                        ;; CAS push buffer onto bucket (vector as stack).
                    (loop []
                      (let [v @bucket]
                        (when-not (compare-and-set! bucket v (conj v buffer))
                          (recur))))
                    (swap! totals update cap-key (fnil + 0) cap)
                    true)
                  false))))))))

  (dispose [_]
    (reset! disposed? true)
    (doseq [bucket-vec [(:heap buckets) (:direct buckets)]
            bucket bucket-vec]
      (reset! bucket []))
    (reset! totals {:heap 0 :direct 0})
    :disposed))

(defn make-bytebuffer-pool
  "Create a new buffer pool instance.
   opts may include:
     :min-capacity      (int)  minimum pooled buffer capacity (default 0)
     :factor            (int)  capacity step/bucket factor (default 2048)
     :max-capacity      (int)  maximum pooled capacity (default 65536)
     :max-bucket-size   (int)  max buffers per bucket (-1 or nil for unbounded; default ~2x CPU)
     :max-heap-memory   (long) pooled heap-bytes cap (0 or nil for heuristic/unlimited)
     :max-direct-memory (long) pooled direct-bytes cap (0 or nil for heuristic/unlimited)
   Returns a value that satisfies BufferPool."
  [opts]
  (let [cfg       (normalize-opts opts (.availableProcessors (Runtime/getRuntime)))
        buckets   {:heap   (new-buckets cfg)
                   :direct (new-buckets cfg)}
        totals    (atom {:heap 0 :direct 0}) ;; bytes of pooled (available) buffers
        disposed? (atom false)]
    (BufferPoolImpl. cfg buckets totals disposed?)))
