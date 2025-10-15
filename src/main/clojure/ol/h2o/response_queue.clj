(ns ol.h2o.response-queue
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.h2o.buffer-pool :as bp]
   [ol.h2o.byte-bounded-queue :as bbq]
   [ol.h2o.evloop :as evloop]
   [ol.h2o.native :as h2o]
   [ol.h2o.pool :as pool])
  (:import
   [java.lang.foreign Arena MemorySegment]
   [java.nio ByteBuffer]
   [java.nio.channels Channels WritableByteChannel]
   [java.util.concurrent.atomic AtomicBoolean AtomicReference]
   [ol.h2o.pool FixedPool]))

(set! *warn-on-reflection* true)

(defrecord
 ^{:doc "A sealed, immutable outbound unit to send in one h2o_sendvec call.
   - bufs: IPersistentVector of read-only ByteBuffer slices (position/limit fixed for send).
   - bytes: total payload bytes across bufs.
   - final?: true iff this chunk closes the response body (H2O_SEND_STATE_FINAL)."}
 Chunk
 [^clojure.lang.IPersistentVector bufs
  ^long bytes
  ^boolean final?])

(extend-protocol bbq/Sized
  Chunk
  (byte-size [c] (:bytes c)))

(defn release-chunk [p ^Chunk c]
  (doseq [b (:bufs c)]
    (pool/release p b)))

(defrecord
 ^{:doc "Per-request state for the queue-based sender.
   Fields:
   - req: the Request
   - bbq: SPSC ByteBoundedQueue of Chunk (writer enqueues, worker drains).
   - scheduled: AtomicBoolean; true iff a drain task is scheduled/on-going on the event loop.
   - in-flight: AtomicReference<Chunk>; the currently sent chunk awaiting proceed.
   - closing: AtomicBoolean; producer side. writer set when close() called (no more writes).
   - stopped: AtomicBoolean; consumer side. libh2o drives this when request is stopped/cancelled or consumer sets it when final chunk sent
   - final-enqueued: AtomicBoolean; tracks whether a final chunk has been enqueued.
   - buffer-pool: reference to pooled direct ByteBuffers (opaque here).
   - current-buffer: AtomicReference<ByteBuffer>; writer-owned aggregation buffer (rotated on seal).
   - evloop-system: the evloop handle
   - config: IPersistentMap of tuning knobs, e.g.:
       {:aggregation-size int
        :large-threshold  int
        :max-buffered-bytes long
        :max-vecs-per-send int}"}
 ResponseState
 [req bbq
  ^AtomicBoolean scheduled?_
  ^AtomicReference in-flight_
  ^AtomicBoolean closing?_
  ^AtomicBoolean stopped?_
  ^AtomicBoolean final-enqueued?_
  ^AtomicReference current-buffer_
  ^FixedPool buffer-pool
  evloop-system
  ^clojure.lang.IPersistentMap config])

(def default-output-buffer-size
  "How much body data in bytes accumulates before writing to the network"
  32768)

(defn new-response-state [req evloop-system]
  (->ResponseState req
                   (bbq/byte-bounded-spsc-queue default-output-buffer-size)
                   (AtomicBoolean. false)
                   (AtomicReference. nil)
                   (AtomicBoolean. false)
                   (AtomicBoolean. false)
                   (AtomicBoolean. false)
                   (AtomicReference. nil)
                   (bp/make-bytebuffer-pool {:buf-size 8192})
                   evloop-system
                   {}))

;; ----- Worker side functions
;; the worker thread is the only one allowed to call native functions
;; ref ol.h2o.evloop

(defn package-chunks
  "Build a contiguous array of h2o_sendvec_t descriptors from a vector of Chunks.

  Input:
  - chunks: vector of Chunk, where each Chunk has:
      :bufs   => vector of sealed (position/limit set) direct ByteBuffers
      :bytes  => total bytes across bufs (not used here except for sanity)
      :final? => whether this chunk closes the response
  - arena: java.lang.foreign.Arena to own the descriptor array.

  Output (map):
  - :seg     => MemorySegment of the descriptor array (length = veccnt * sizeof(sendvec))
  - :veccnt  => total number of vectors (sum of (count bufs) across chunks)
  - :final?  => true iff the LAST chunk is final

  Notes:
  - Keeps NO additional references; the caller must retain the drained `chunks`
  - If veccnt == 0 (e.g., empty-final), we return a 0-sized segment and veccnt 0."
  [chunks arena]
  (let [last-final? (boolean (when-let [c (peek chunks)] (:final? c)))
        bufs-flat (vec (mapcat :bufs chunks))
        veccnt (count bufs-flat)]
    (if (zero? veccnt)
      {:seg (mem/alloc 0 arena) :veccnt 0 :final? last-final?}
      (let [elem-size h2o/size-of-h2o-sendvec-t
            total-size (* veccnt elem-size)
            sendvec-array (mem/alloc total-size arena)]
        (dotimes [i veccnt]
          (let [^ByteBuffer b (nth bufs-flat i)]
            (when-not (.isDirect b) (throw (ex-info "Non-direct ByteBuffer in Chunk bufs; must be direct" {:index i})))
            (let [slot-off (* i elem-size)
                  slot (mem/slice sendvec-array slot-off elem-size)
                  seg (MemorySegment/ofBuffer b)
                  len (.remaining b)]
              (h2o/sendvec-init-raw slot seg len))))
        {:seg sendvec-array :veccnt veccnt :final? last-final?}))))

(defn drain-chunks
  "Worker thread. Called after proceed indicates we can send more chunks to native, returns true if final was sent"
  [req bbq preferred-chunk-size arena]
  ;; st must hold an :in-flight ref you set to `chunks` to keep ByteBuffers alive
  (let [chunks (bbq/drain bbq preferred-chunk-size)]
    (when (seq chunks)
      (let [{:keys [seg veccnt final?]} (package-chunks chunks arena)]
        (h2o/sendvec (-> req :req-ctx :req) seg veccnt (if final? h2o/H2O_SEND_STATE_FINAL h2o/H2O_SEND_STATE_IN_PROGRESS))
        [final? chunks]))))

(defn release-chunks
  "Worker thread. Release the Chunks that were in-flight back to the pool"
  [pool ^AtomicReference in-flight_]
  (assert pool)
  (assert in-flight_)
  (let [in-flight-val (.get in-flight_)]
    (println "release-chunks: in-flight-val=" (pr-str (when in-flight-val [(first in-flight-val) (System/identityHashCode (second in-flight-val))])))
    (when-some [[chunks ^Arena arena] in-flight-val]
      (let [arena-id (System/identityHashCode arena)]
        (println "release-chunks: closing arena" arena-id)
        (.close arena)
        (println "release-chunks: arena closed" arena-id)
        (when chunks
          (println "release-chunks: releasing" (count chunks) "chunks")
          (release-chunk pool chunks)))))
  (println "release-chunks: clearing in-flight_")
  (.set in-flight_ nil))

(defn send-vecs
  "Worker thread. Called in the evloop to drain chunks."
  [^ResponseState st]
  (when-not (.get ^AtomicBoolean (:stopped?_ st))
    (let [arena (Arena/ofConfined)
          arena-id (System/identityHashCode arena)]
      (println "send-vecs: created arena" arena-id)
      (try
        (println "send-vecs: draining chunks")
        (let [[final? chunks] (drain-chunks (:req st) (:bbq st) (get-in st [:config :preferred-chunk-size] Long/MAX_VALUE)
                                            arena)]
          (println "send-vecs: got chunks=" (count chunks) "final?=" final? "arena-id=" arena-id)
          (.set ^AtomicReference (:in-flight_ st) [chunks arena])
          (println "send-vecs: stored in-flight [chunks arena-id=" arena-id "]")
          (when final?
            (println "send-vecs: stopping (final chunk)")
            (.set ^AtomicBoolean (:stopped?_ st) true)))
        (finally
          (println "send-vecs: clearing scheduled?_")
          (.set ^AtomicBoolean (:scheduled?_ st) false))))))

(defn schedule-drain!
  "Try to schedule a drain task on the event loop.
   Uses CAS on scheduled?_ to ensure only one drain is posted at a time.
   Returns true if a new drain was scheduled, false if one was already pending."
  [^ResponseState st]
  (when (.compareAndSet ^AtomicBoolean (:scheduled?_ st) false true)
    (evloop/send-msg! (:evloop-system st) [:h2o/sendvec (fn [] (send-vecs st))])
    true))

(defn report-error [e]
  (println e))

(defn on-proceed
  "Worker thread. Called by libh2o to progress the response generator"
  [^ResponseState st]
  (println "on-proceed: called")
  (try
    (release-chunks (:buffer-pool st) (:in-flight_ st))
    (println "on-proceed: released chunks, stopped?=" (.get ^AtomicBoolean (:stopped?_ st)))
    (when-not (.get ^AtomicBoolean (:stopped?_ st))
      (println "on-proceed: scheduling next drain")
      (schedule-drain! st))
    (catch Exception e
      (println "on-proceed: ERROR" e)
      (report-error e))))

(defn on-stop
  "Worker thread. Called by libh2o to cancel the request (because client hung up etc)"
  [^ResponseState st reason]
  (try
    (println "response sending aborted, reason=" reason)
    (.set ^AtomicBoolean (:stopped?_ st) true)
    (catch Exception e
      (report-error e))))

;; ----- Writer/Producer side functions
;; the request thread is the virtualthread spawned to handle the request. It writes responses
;; ref: ol.h2o.request/enqueue-request

(defn make-chunk
  "Build a sealed Chunk from a seq of ByteBuffers and a final? flag.
   Each buffer should already have position/limit set for reading."
  ^Chunk [bufs final?]
  (let [vbufs (vec bufs)
        total (long (reduce (fn [^long acc ^ByteBuffer b] (+ acc (.remaining b))) 0 vbufs))]
    (->Chunk vbufs total (boolean final?))))

(defn output-stream [st]
  (reify
    WritableByteChannel
    (write [_ src]
      ;; If stopped? is true, the channel is closed regardless of closing?
      (when (or (.get ^AtomicBoolean (:stopped?_ st))
                (.get ^AtomicBoolean (:closing?_ st)))
        (throw (java.nio.channels.ClosedChannelException.)))
      (let [^AtomicReference cur-ref (:current-buffer_ st)
            ^FixedPool pool (:buffer-pool st)
            total (long (.remaining src))]
        (when (pos? total)
          (loop [left total]
            (if (zero? left)
              total
              (let [^ByteBuffer buf (or (.get cur-ref)
                                        (let [b (pool/borrow pool)]
                                          (when (nil? b) (throw (ex-info "Buffer pool exhausted" {})))
                                          (.clear ^ByteBuffer b)
                                          (.set cur-ref b)
                                          b))
                    can-copy (min left (.remaining buf))
                    pos (.position src)
                    lim (.limit src)]
                ;; Copy exactly can-copy bytes from src to buf
                (.limit src (+ pos can-copy))
                (.put buf src)
                (.limit src lim)
                ;; If aggregation buffer is full, seal it into a Chunk and enqueue
                (when (zero? (.remaining buf))
                  (.flip buf)
                  (let [read-view (.duplicate buf)
                        chunk (make-chunk [read-view] false)]
                    (.set cur-ref nil)
                    (bbq/put (:bbq st) chunk)
                    (schedule-drain! st)))
                (recur (- left can-copy))))))))
    (isOpen [_]
      (not (or (.get ^AtomicBoolean (:stopped?_ st))
               (.get ^AtomicBoolean (:closing?_ st)))))
    (close [_]
      (if (.get ^AtomicBoolean (:stopped?_ st))
        (do
          ;; Best-effort: release any borrowed current buffer back to the pool.
          (when-some [^ByteBuffer buf (.get ^AtomicReference (:current-buffer_ st))]
            (pool/release (:buffer-pool st) buf)
            (.set ^AtomicReference (:current-buffer_ st) nil))
          (.set ^AtomicBoolean (:closing?_ st) true)
          nil)
        (if (.compareAndSet ^AtomicBoolean (:closing?_ st) false true)
          (let [^AtomicReference cur-ref (:current-buffer_ st)
                ^ByteBuffer buf (.get cur-ref)
                final-chunk (if (and buf (pos? (.position buf)))
                              (do
                                (.flip buf)
                                (let [read-view (.duplicate buf)]
                                  (.set cur-ref nil)
                                  (make-chunk [read-view] true)))
                              (make-chunk [] true))]
            ;; Enqueue final (empty or with remaining data)
            (bbq/put (:bbq st) final-chunk)
            (.set ^AtomicBoolean (:final-enqueued?_ st) true)
            (schedule-drain! st)
            nil)
          nil)))))

(defn create-response-queue [req evloop-system _opts]
  (let [st (new-response-state req evloop-system)]
    {:state st
     :to-output-stream (fn [] (Channels/newOutputStream (output-stream st)))
     :on-proceed (mem/serialize (fn [_ctx-ptr] (on-proceed st)) [::ffi/fn [::mem/pointer] ::mem/void])
     :on-stop (mem/serialize (fn [_ctx-ptr reason] (on-stop st reason)) [::ffi/fn [::mem/pointer ::mem/int] ::mem/void])}))
