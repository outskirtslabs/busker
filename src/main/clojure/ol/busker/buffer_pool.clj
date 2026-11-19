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
  coordinate segment access and release. Threads that wait for a block park on
  a lock-free queue: a release wakes one of them, and retirement wakes all.
  This is internal response storage, not an application buffer API;
  applications use [[ol.busker.protocols/emit!]]."
  (:require
   [ol.busker.capacity-waiters :as waiters])
  (:import
   [java.lang.foreign Arena MemorySegment]
   [java.util ArrayDeque HashMap]))

(set! *warn-on-reflection* true)

(defrecord StreamPool [^Arena arena ^long block-size ^Object lock
                       ^ArrayDeque free ^HashMap checked ^HashMap pressure state_
                       waiters])

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
                      (atom {:retiring? false :closed? false}) (waiters/create)))
      (catch Throwable t
        (.close arena)
        (throw t)))))

(defn await-block!
  "Calls [[ol.busker.capacity-waiters/await!]] with `attempt!` on the block
  waiters of `pool`. `attempt!` must return `:closed` once `pool` retires."
  [^StreamPool pool attempt!]
  (waiters/await! (:waiters pool) attempt!))

(defn wake-all!
  "Wakes every block waiter of `pool`, so that each rechecks its response."
  [^StreamPool pool]
  (waiters/signal-all! (:waiters pool)))

(defn track-pressure!
  "Registers only responses with published, unflushed storage.
  Wakes one block waiter, which then tries again."
  [^StreamPool pool token flush-fn]
  (locking (:lock pool) (.put ^HashMap (:pressure pool) token flush-fn))
  (waiters/signal! (:waiters pool) 1))

(defn untrack-pressure! [^StreamPool pool token]
  (locking (:lock pool) (.remove ^HashMap (:pressure pool) token)))

(defn relieve-pressure!
  "Removes all tracked responses, then calls each flush function once without
  holding the pool monitor. A response needs [[track-pressure!]] again to be
  flushed by a later call. A flush function that throws does not stop the
  others. Its response is tracked again. After the calls, if any flush function
  threw, every block waiter wakes and the first exception propagates."
  [^StreamPool pool]
  (let [^HashMap pressure (:pressure pool)
        entries (locking (:lock pool)
                  (let [taken (mapv (juxt key val) pressure)]
                    (.clear pressure)
                    taken))
        failed (into [] (keep (fn [[_ f :as entry]]
                                (try (f) nil
                                     (catch Throwable t (conj entry t)))))
                     entries)]
    (when-let [[[_ _ t]] (seq failed)]
      (locking (:lock pool)
        (doseq [[token f] failed] (.putIfAbsent pressure token f)))
      (wake-all! pool)
      (throw t))))

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
  "Returns a checked-out block and wakes one block waiter. Ignores nil.
  Stale or duplicate claims throw. The caller must end all producer and native
  access first. This does not close the arena or finish a pending retirement."
  [^StreamPool pool block]
  (when block
    (locking (:lock pool)
      (when-not (identical? (:claim block) (.get ^HashMap (:checked pool) (:index block)))
        (throw (IllegalStateException. "Stale native stream block release")))
      (.remove ^HashMap (:checked pool) (:index block))
      (.add ^ArrayDeque (:free pool) (dissoc block :claim)))
    (waiters/signal! (:waiters pool) 1)))

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
    (wake-all! pool)
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
