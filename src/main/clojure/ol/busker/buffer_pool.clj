(ns ^{:no-doc true :clj-kondo/config '{:linters {:locking-suspicious-lock {:level :off}}}}
 ol.busker.buffer-pool
  "Provides bounded, reusable native payload storage for streaming responses.

  Each worker has 256 blocks of B bytes, where B is `:output-buffer-size`.
  A platform thread allocates the shared FFM arena once: 256 * B bytes, or
  8 MiB at the default B=32768, separate from fixed-response slots.

  [[try-borrow!]] returns a uniquely claimed block without waiting or allocating
  payload memory. [[release!]] returns it only after its producer or native send
  has finished using it. Private, queued and in-flight blocks all remain counted
  as checked out. [[retire!]] rejects new borrows and closes the arena only after
  every claim returns; a failed retirement must retain the pool and be retried.

  Application virtual threads fill blocks through [[fill-native-range!]]. Its
  confined view expires before publication; the worker reads the separately
  retained shared segment. Pool accounting is synchronized, but callers must
  coordinate segment access and release. This is internal response storage, not
  an application buffer API; applications use [[ol.busker.protocols/emit!]]."
  (:import
   [java.lang.foreign Arena MemorySegment]
   [java.util ArrayDeque HashMap]))

(set! *warn-on-reflection* true)

(defrecord StreamPool [^Arena arena ^long block-size ^Object lock
                       ^ArrayDeque free ^HashMap checked ^HashMap pressure state_])

(defn create-stream-pool
  "Returns a pool of 256 native blocks, each `size` bytes, in a shared FFM arena.
  Requires a platform thread and a positive int `size`; invalid inputs throw.
  The caller must retain the pool until [[retire!]] succeeds."
  [size]
  (when (.isVirtual (Thread/currentThread))
    (throw (IllegalStateException. "Stream storage must be allocated on a platform thread")))
  (when-not (and (integer? size) (pos? size) (<= size Integer/MAX_VALUE))
    (throw (IllegalArgumentException. "output-buffer-size must be a positive int")))
  (let [total (Math/multiplyExact (long 256) (long size))
        arena (Arena/ofShared)]
    (try
      (let [storage (.allocate arena total 8)
            free (ArrayDeque.)]
        (dotimes [i 256]
          (.add free {:index i :segment (.asSlice storage (long (* (long i) size)) (long size))}))
        (->StreamPool arena size (Object.) free (HashMap.) (HashMap.)
                      (atom {:retiring? false :closed? false :epoch 0})))
      (catch Throwable t
        (.close arena)
        (throw t)))))

(defn signal!
  "Wakes capacity waiters after a release or response closure."
  [^StreamPool pool]
  (locking (:lock pool)
    (swap! (:state_ pool) update :epoch inc)
    (.notifyAll ^Object (:lock pool))))

(defn epoch [^StreamPool pool]
  (locking (:lock pool) (:epoch @(:state_ pool))))

(defn await-change!
  "Waits once if `observed` still matches [[epoch]] and admission remains open.
  Callers must recheck capacity after waking and call [[signal!]] on cancellation.
  The Java monitor wait is interruptible and can park a virtual thread."
  [^StreamPool pool observed cancelled?]
  (locking (:lock pool)
    (when (and (= observed (:epoch @(:state_ pool)))
               (not (:retiring? @(:state_ pool)))
               (not (cancelled?)))
      (.wait ^Object (:lock pool)))))

(defn track-pressure!
  "Registers only responses with published, unflushed storage."
  [^StreamPool pool token flush-fn]
  (locking (:lock pool) (.put ^HashMap (:pressure pool) token flush-fn)))

(defn untrack-pressure! [^StreamPool pool token]
  (locking (:lock pool) (.remove ^HashMap (:pressure pool) token)))

(defn relieve-pressure!
  "Flushes the tracked subset without holding the pool monitor."
  [^StreamPool pool]
  (let [callbacks (locking (:lock pool) (vec (.values ^HashMap (:pressure pool))))]
    (doseq [f callbacks] (f))))

(defn try-borrow!
  "Returns a block claim, `:full` if exhausted, or `:closed` after retirement starts.
  Never waits or allocates payload storage. Return the exact claim to [[release!]]
  once all accesses finish; do not release a block still in native use."
  [^StreamPool pool]
  (locking (:lock pool)
    (if (:retiring? @(:state_ pool))
      :closed
      (if-let [block (.poll ^ArrayDeque (:free pool))]
        (let [claim (assoc block :claim (Object.))]
          (.put ^HashMap (:checked pool) (:index claim) (:claim claim))
          claim)
        :full))))

(defn release!
  "Returns a checked-out block and wakes capacity waiters; nil is ignored.
  Stale or duplicate claims throw. The caller must end all producer and native
  access first. This does not close the arena or finish a pending retirement."
  [^StreamPool pool block]
  (when block
    (locking (:lock pool)
      (when-not (identical? (:claim block) (.get ^HashMap (:checked pool) (:index block)))
        (throw (IllegalStateException. "Stale native stream block release")))
      (.remove ^HashMap (:checked pool) (:index block))
      (.add ^ArrayDeque (:free pool) (dissoc block :claim))
      (signal! pool))))

(defn checked-out [^StreamPool pool]
  (locking (:lock pool) (.size ^HashMap (:checked pool))))

(defn retire!
  "Rejects new borrows and wakes waiters. Requires a platform thread.
  Returns true after closing the arena, including repeated successful calls.
  Returns false while blocks remain checked out; retain the pool and retry after
  their release. A timeout or interrupted producer does not permit early closure."
  [^StreamPool pool]
  (when (.isVirtual (Thread/currentThread))
    (throw (IllegalStateException. "Stream storage retirement requires a platform thread")))
  (locking (:lock pool)
    (swap! (:state_ pool) assoc :retiring? true)
    (signal! pool)
    (if (.isEmpty ^HashMap (:checked pool))
      (do
        (when-not (:closed? @(:state_ pool))
          (.close ^Arena (:arena pool))
          (swap! (:state_ pool) assoc :closed? true)
          (.clear ^ArrayDeque (:free pool)))
        true)
      false)))

(defn fill-native-range!
  "Calls `producer` synchronously with a confined ByteBuffer of `capacity` bytes.
  Returns its final position as the byte count; the producer must not flip it.
  The view and its aliases reject access from other threads and expire on return
  or exception. The caller must keep `backing` valid throughout the call and any
  later native send; closing the view does not free that shared allocation."
  [^MemorySegment backing capacity producer]
  (with-open [view-arena (Arena/ofConfined)]
    (let [view (.reinterpret backing (long capacity) view-arena nil)
          buffer (.asByteBuffer view)]
      (producer buffer)
      (.position buffer))))
