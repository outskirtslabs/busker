(ns ^:no-doc ol.busker.response-queue
  (:require
   [coffi.mem :as mem]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.byte-bounded-queue :as bbq]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [taoensso.trove :as trove])
  (:import
   [java.io OutputStream]
   [java.lang.foreign Arena MemorySegment]
   [java.nio ByteBuffer]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicReference]))

(set! *warn-on-reflection* true)

(def ^:private ^:const drain-idle 0)
(def ^:private ^:const drain-active 1)
(def ^:private ^:const drain-signalled 2)

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
    (bp/return p b)))

;; ----- Worker side functions
;; the worker thread is the only one allowed to call native functions
;; ref ol.busker.evloop

;; The drain-state_ AtomicInteger permits one mailbox command or native send at a time:
;;
;;   0 idle: no command or send is active
;;   1 active: a command, send-vecs call, or native send is active
;;   2 signalled: active, with data enqueued since the last drain check
;;
;; A producer changes idle to active before posting a mailbox command. While active, it
;; records a signal instead. The worker consumes that signal and checks the queue again
;; before retiring to idle. This prevents an enqueue between an empty check and retirement
;; from becoming stranded.
;;
;; on-proceed keeps the drain active, releases completed buffers, and drains directly before
;; returning to h2o. Native sends therefore retain h2o's proceed → send call sequence.

(defn pending-work?
  "Returns true when a response writer has queued or in-flight data."
  [st]
  (or (pos? (.get ^AtomicInteger (:drain-state_ st)))
      (some? (.get ^AtomicReference (:in-flight_ st)))
      (pos? (bbq/queued-bytes (:bbq st)))))

(defn package-chunks
  "Build a contiguous array of h2o_sendvec_t descriptors from a vector of Chunks.
   Copies ByteBuffer data to arena-allocated stable segments for safe native access.
   Returns map with :seg (sendvec array), :veccnt, :final?, and :stable-segs (to prevent GC)."
  [chunks arena]
  (let [last-final? (boolean (when-let [c (peek chunks)] (:final? c)))
        bufs-flat (vec (mapcat :bufs chunks))
        veccnt (count bufs-flat)]
    (if (zero? veccnt)
      {:seg (mem/alloc 0 arena) :veccnt 0 :final? last-final? :stable-segs []}
      (let [elem-size h2o/size-of-h2o-sendvec-t
            total-size (* veccnt elem-size)
            sendvec-array (mem/alloc total-size arena)]
        (loop [i 0
               stable-segs (transient [])]
          (if (< i veccnt)
            (let [^ByteBuffer b (nth bufs-flat i)]
              (when-not (.isDirect b) (throw (ex-info "Non-direct ByteBuffer in Chunk bufs; must be direct" {:index i})))
              (let [slot-off (* i elem-size)
                    slot (mem/slice sendvec-array slot-off elem-size)
                    len (.remaining b)
                    byte-arr (byte-array len)
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

  (let [chunks (bbq/drain bbq preferred-chunk-size)
        #_#_total-size (reduce + (map :bytes chunks))]
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
  [st]
  (when-not (.get ^AtomicBoolean (:stopped?_ st))
    (when (nil? (.get ^AtomicReference (:in-flight_ st)))
      (let [^AtomicInteger drain-state_ (:drain-state_ st)]
        (loop []
          (let [arena (Arena/ofAuto)
                result (drain-chunks (:req st)
                                     (:bbq st)
                                     (get-in st [:config :preferred-chunk-size] Long/MAX_VALUE)
                                     arena)]
            (if result
              (let [[final? chunks stable-segs] result]
                (.set ^AtomicReference (:in-flight_ st) [chunks arena stable-segs])
                (.compareAndSet drain-state_ drain-signalled drain-active)
                (when final?
                  (.set ^AtomicBoolean (:stopped?_ st) true)))
              (case (.get drain-state_)
                0 nil
                1 (when-not (.compareAndSet drain-state_ drain-active drain-idle)
                    (recur))
                2 (do
                    (.compareAndSet drain-state_ drain-signalled drain-active)
                    (recur))
                (throw (IllegalStateException. "Invalid response drain state"))))))))))

(defn schedule-drain!
  "Record response work and wake or message the event-loop worker."
  [st]
  (let [worker (-> st :req :worker)
        ^AtomicInteger drain-state_ (:drain-state_ st)]
    (loop []
      (case (.get drain-state_)
        0 (if (.compareAndSet drain-state_ drain-idle drain-active)
            (do
              (pi/send-msg worker
                           [:h2o/sendvec
                            (:dispatch-module-id (:req st))
                            (:dispatch-request-seq (:req st))
                            (fn [] (send-vecs st))])
              :sent-msg)
            (recur))
        1 (if (.compareAndSet drain-state_ drain-active drain-signalled)
            (do
              (pi/wake worker)
              :signalled)
            (recur))
        2 (do
            (pi/wake worker)
            :signalled)
        (throw (IllegalStateException. "Invalid response drain state"))))))

(defn report-error [e]
  (trove/log! {:level :error :id :h2o/error :ex e}))

(defn on-proceed
  "Worker thread. Called by libh2o to progress the response generator"
  [st]
  (try
    (release-chunks (:buffer-pool st) (:in-flight_ st))
    (when-not (.get ^AtomicBoolean (:stopped?_ st)) (send-vecs st))
    (catch Exception e
      (report-error e))))

(defn on-stop
  "Worker thread. Called by libh2o to cancel the request (because client hung up etc).
  Must not throw."
  [st _reason]
  (try
    (.set ^AtomicBoolean (:stopped?_ st) true)
    (bbq/close (:bbq st))
    (when-some [^ByteBuffer buf (.getAndSet ^AtomicReference (:current-buffer_ st) nil)]
      (bp/return (:buffer-pool st) buf))
    (catch Exception e
      (report-error e))))

;; ----- Writer/Producer side functions
;; the request thread is the virtualthread spawned to handle the request. It writes responses
;; ref: ol.busker.request/on-request

(defn seal-chunk
  "Build a sealed Chunk from a seq of ByteBuffers and a final? flag.
   Each buffer should already have position/limit set for reading."
  ^Chunk [bufs final?]
  (let [vbufs (vec bufs)
        total (long (reduce (fn [^long acc ^ByteBuffer b] (+ acc (.remaining b))) 0 vbufs))]
    (->Chunk vbufs total (boolean final?))))

(defn- ->output-stream ^OutputStream [st]
  (proxy [OutputStream] []
    (write
      ([x]
       (if (number? x)
         (.write ^OutputStream this (byte-array [x]))
         (.write ^OutputStream this x 0 (alength ^bytes x))))
      ([^bytes ba off len]
       (when (or (.get ^AtomicBoolean (:stopped?_ st))
                 (.get ^AtomicBoolean (:closing?_ st)))
         (throw (java.io.IOException. "Stream closed")))
       (let [^AtomicReference cur-ref (:current-buffer_ st)
             pool                     (:buffer-pool st)]
         (loop [offset    (int off)
                remaining (int len)]
           (when (pos? remaining)
             (let [^ByteBuffer buf (or (.get cur-ref)
                                       (let [b (bp/borrow pool (:output-buffer-size st) true)]
                                         (when (nil? b) (throw (ex-info "Buffer pool exhausted" {})))
                                         (.clear ^ByteBuffer b)
                                         (.set cur-ref b)
                                         b))
                   can-copy        (min remaining (.remaining buf))]
               (.put buf ba offset can-copy)
               (when (zero? (.remaining buf))
                 (.flip buf)
                 (let [chunk (seal-chunk [buf] false)]
                   (.set cur-ref nil)
                   (bbq/put (:bbq st) chunk)
                   (schedule-drain! st)))
               (recur (+ offset can-copy) (- remaining can-copy))))))))
    (flush []
      (when (or (.get ^AtomicBoolean (:stopped?_ st))
                (.get ^AtomicBoolean (:closing?_ st)))
        (throw (java.io.IOException. "Stream closed")))
      (let [^AtomicReference cur-ref (:current-buffer_ st)
            ^ByteBuffer buf          (.get cur-ref)]
        (when (and buf (pos? (.position buf)))
          (.flip buf)
          (.set cur-ref nil)
          (let [chunk (seal-chunk [buf] false)]
            (bbq/put (:bbq st) chunk)
            (schedule-drain! st)))))
    (close []
      (if (.get ^AtomicBoolean (:stopped?_ st))
        (do
          (when-some [^ByteBuffer buf (.get ^AtomicReference (:current-buffer_ st))]
            (bp/return (:buffer-pool st) buf)
            (.set ^AtomicReference (:current-buffer_ st) nil))
          (.set ^AtomicBoolean (:closing?_ st) true)
          nil)
        (if (.compareAndSet ^AtomicBoolean (:closing?_ st) false true)
          (let [^AtomicReference cur-ref (:current-buffer_ st)
                ^ByteBuffer buf          (.get cur-ref)
                final-chunk              (if (and buf (pos? (.position buf)))
                                           (do
                                             (.flip buf)
                                             (.set cur-ref nil)
                                             (seal-chunk [buf] true))
                                           (seal-chunk [] true))]
            (bbq/put (:bbq st) final-chunk)
            (.set ^AtomicBoolean (:final-enqueued?_ st) true)
            (schedule-drain! st)
            nil)
          nil)))))

(defrecord
 ^{:doc "Per-request state for the queue-based sender.
   Fields:
   - req: the Request
   - bbq: SPSC ByteBoundedQueue of Chunk (writer enqueues, worker drains).
   - output-buffer-size: size of buffers to grab from the pool
   - drain-state: AtomicInteger; idle, active, or active with a producer signal.
   - in-flight: AtomicReference<Chunk>; the currently sent chunk awaiting proceed.
   - closing: AtomicBoolean; producer side. writer set when close() called (no more writes).
   - stopped: AtomicBoolean; consumer side. libh2o drives this when request is stopped/cancelled or consumer sets it when final chunk sent
   - final-enqueued: AtomicBoolean; tracks whether a final chunk has been enqueued.
   - buffer-pool: reference to pooled direct ByteBuffers (opaque here).
   - current-buffer: AtomicReference<ByteBuffer>; writer-owned aggregation buffer (rotated on seal).
   - config: IPersistentMap of tuning knobs, e.g.:
       {:aggregation-size int
        :large-threshold  int
        :max-buffered-bytes long
        :max-vecs-per-send int}"}
 H2OResponseWriter
 [req bbq output-buffer-size
  ^AtomicInteger drain-state_
  ^AtomicReference in-flight_
  ^AtomicBoolean closing?_
  ^AtomicBoolean stopped?_
  ^AtomicBoolean final-enqueued?_
  ^AtomicReference current-buffer_
  buffer-pool
  ^clojure.lang.IPersistentMap config]
  pi/Stopable
  (stop [this] (on-stop this nil)))

(defn new-response-state
  [req]
  (assert (-> req :config :buffer-pool))
  (assert (-> req :config :output-buffer-size))
  (->H2OResponseWriter req
                       (bbq/byte-bounded-spsc-queue (-> req :config :output-buffer-size))
                       (-> req :config :output-buffer-size)
                       (AtomicInteger. drain-idle)
                       (AtomicReference. nil)
                       (AtomicBoolean. false)
                       (AtomicBoolean. false)
                       (AtomicBoolean. false)
                       (AtomicReference. nil)
                       (-> req :config :buffer-pool)
                       {}))

(defn create-response-writer [req]
  (let [st (new-response-state req)
        {:keys [proceed stop]} (:callback-pointers req)]
    (assoc st
           :on-proceed-cb-ptr proceed
           :on-stop-cb-ptr stop
           :out-stream (->output-stream st))))
