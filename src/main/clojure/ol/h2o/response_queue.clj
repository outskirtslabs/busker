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

;; the scheduled?_ flag:
;;
;; The scheduled?_ AtomicBoolean ensures only one drain task is pending/in-flight at a time,
;; enforcing h2o's strict "one sendvec in flight" contract: sendvec → on-proceed → sendvec.
;;
;; State transitions:
;;   false → true:  Writer calls schedule-drain! and successfully posts :h2o/sendvec message
;;   true → false:  Either (1) on-proceed clears it before scheduling next drain, OR
;;                        (2) send-vecs clears it when queue is empty (no work to do)
;;
;; Semantics:
;;   scheduled?_=true means one of:
;;     - A drain task is in the evloop mailbox (not yet executed)
;;     - send-vecs is currently executing on the worker thread
;;     - h2o_sendvec call is in-flight waiting for on-proceed callback
;;
;; Guarantees:
;;   - Prevents duplicate drain messages in the evloop mailbox (CAS gate in schedule-drain!)
;;   - Ensures on-proceed sees no concurrent drains (cleared before next schedule)
;;   - Writer can safely enqueue chunks and post drain without blocking on semaphores
;;
;; Key functions:
;;   schedule-drain!: CAS false→true, posts mailbox message if successful
;;   send-vecs:       Keeps true while sendvec in-flight; clears to false only if queue empty
;;   on-proceed:      Clears to false, releases buffers, schedules next drain if not stopped

(defn package-chunks
  "Build a contiguous array of h2o_sendvec_t descriptors from a vector of Chunks.
   Copies ByteBuffer data to arena-allocated stable segments for safe native access.
   Returns map with :seg (sendvec array), :veccnt, :final?, and :stable-segs (to prevent GC)."
  [chunks arena]
  (let [last-final? (boolean (when-let [c (peek chunks)] (:final? c)))
        bufs-flat   (vec (mapcat :bufs chunks))
        veccnt      (count bufs-flat)]
    (if (zero? veccnt)
      {:seg (mem/alloc 0 arena) :veccnt 0 :final? last-final? :stable-segs []}
      (let [elem-size     h2o/size-of-h2o-sendvec-t
            total-size    (* veccnt elem-size)
            sendvec-array (mem/alloc total-size arena)]
        (loop [i           0
               stable-segs (transient [])]
          (if (< i veccnt)
            (let [^ByteBuffer b (nth bufs-flat i)]
              (when-not (.isDirect b) (throw (ex-info "Non-direct ByteBuffer in Chunk bufs; must be direct" {:index i})))
              (let [slot-off   (* i elem-size)
                    slot       (mem/slice sendvec-array slot-off elem-size)
                    len        (.remaining b)
                    byte-arr   (byte-array len)
                    stable-seg (mem/alloc len arena)]
                (.get ^ByteBuffer b byte-arr)
                (MemorySegment/copy byte-arr 0 stable-seg java.lang.foreign.ValueLayout/JAVA_BYTE 0 len)
                (h2o/sendvec-init-raw slot stable-seg len)
                (recur (inc i) (conj! stable-segs stable-seg))))
            {:seg sendvec-array :veccnt veccnt :final? last-final? :stable-segs (persistent! stable-segs)}))))))

(defn drain-chunks
  "Worker thread. Called after proceed indicates we can send more chunks to native, returns true if final was sent"
  [req bbq preferred-chunk-size arena]
  ;; st must hold an :in-flight ref you set to `chunks` to keep ByteBuffers alive
  (let [chunks (bbq/drain bbq preferred-chunk-size)]
    (when (seq chunks)
      (let [{:keys [seg veccnt final? stable-segs]} (package-chunks chunks arena)]
        (h2o/sendvec (-> req :req-ctx :req) seg veccnt (if final? h2o/H2O_SEND_STATE_FINAL h2o/H2O_SEND_STATE_IN_PROGRESS))
        [final? chunks stable-segs]))))

(defn release-chunks
  "Worker thread. Release the Chunks that were in-flight back to the pool.
   Note: Arena/ofAuto arenas are GC-managed and should NOT be manually closed."
  [pool ^AtomicReference in-flight_]
  (let [in-flight-val (.get in-flight_)]
    (when-some [[chunks _arena _stable-segs] in-flight-val]
      (when (seq chunks)
        (doseq [chunk chunks]
          (release-chunk pool chunk)))))
  (.set in-flight_ nil))

(defn send-vecs
  "Evloop worker thread: drain chunks and send to native when ready."
  [^ResponseState st]
  (when-not (.get ^AtomicBoolean (:stopped?_ st))
    (let [arena  (Arena/ofAuto)
          result (drain-chunks (:req st) (:bbq st) (get-in st [:config :preferred-chunk-size] Long/MAX_VALUE)
                               arena)]
      (if result
        (let [[final? chunks stable-segs] result]
          (.set ^AtomicReference (:in-flight_ st) [chunks arena stable-segs])
          (when final?
            (.set ^AtomicBoolean (:stopped?_ st) true)))
        (.set ^AtomicBoolean (:scheduled?_ st) false)))))

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
  (try
    (.set ^AtomicBoolean (:scheduled?_ st) false)
    (release-chunks (:buffer-pool st) (:in-flight_ st))
    (when-not (.get ^AtomicBoolean (:stopped?_ st))
      (schedule-drain! st))
    (catch Exception e
      (report-error e))))

(defn on-stop
  "Worker thread. Called by libh2o to cancel the request (because client hung up etc)"
  [^ResponseState st _reason]
  (try
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
                  (let [chunk (make-chunk [buf] false)]
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
                                (.set cur-ref nil)
                                (make-chunk [buf] true))
                              (make-chunk [] true))]
            ;; Enqueue final (empty or with remaining data)
            (bbq/put (:bbq st) final-chunk)
            (.set ^AtomicBoolean (:final-enqueued?_ st) true)
            (schedule-drain! st)
            nil)
          nil)))))

(defn create-response-queue [req evloop-system _opts]
  (let [st (new-response-state req evloop-system)]
    {:state            st
     :to-output-stream (fn [] (Channels/newOutputStream (output-stream st)))
     :on-proceed       (mem/serialize (fn [_ctx-ptr] (on-proceed st)) [::ffi/fn [::mem/pointer] ::mem/void])
     :on-stop          (mem/serialize (fn [_ctx-ptr reason] (on-stop st reason)) [::ffi/fn [::mem/pointer ::mem/int] ::mem/void])}))
