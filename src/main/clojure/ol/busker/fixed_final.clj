(ns ^:no-doc ol.busker.fixed-final
  "Prepares and delivers complete final responses on the worker response paths.

  Direct plans write header descriptors and payload bytes into a claimed native
  slot. H2O retains that slot until request cleanup. [[publish-direct-response!]]
  parks for capacity; [[publish-direct-producer!]] parks or returns `:overloaded`.

  Each worker has 256 independently claimable slots. Eligibility allows at most
  64 header pairs, 16384 bytes of header staging and 32768 body bytes, subject to
  the response's output-byte limit. Streaming blocks are separate storage.
  Exhaustion does not allocate overflow payloads.

  A producer-use reference protects the receiver during packing without holding
  its admission lock across application code. Producer views are confined to the
  calling virtual thread and expire before publication. Private claims abort on
  failure; published claims stay retained until stale-request discard or native
  request cleanup. Only the platform worker drains ready slots and sends to H2O.

  ## Related Namespaces

  - [[ol.busker.response]] chooses fixed-slot or ordered streaming delivery.
  - [[ol.busker.response-serialization]] stages shared header descriptors.
  - [[ol.busker.response-head]] sends streaming response heads."
  (:require
   [babashka.ffi :as mem]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.response-serialization :as serialization])
  (:import
   [java.lang System$Logger$Level]
   [java.lang.foreign Arena MemorySegment]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent.atomic AtomicBoolean AtomicReference]
   [java.util.concurrent.locks Lock ReentrantReadWriteLock]))

(set! *warn-on-reflection* true)

(def ^:const max-header-pairs 64)
(def ^:const max-header-staging-bytes 16384)
(def ^:const response-ring-capacity 256)
(def ^:const response-ring-max-body-bytes 32768)
(def ^:private response-claim-index-mask 0xffff)

;; On supported 64-bit targets `clj_header_t` is 32 bytes. Each slot has
;; 64 descriptors and 2120 bytes of metadata before its payload.

(def ^:private response-slot-data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t))
(def ^:private response-slot-module-id (mem/place h2o/ffi-clj-fixed-response-slot-data-t :module-id))
(def ^:private response-slot-request-seq (mem/place h2o/ffi-clj-fixed-response-slot-data-t :request-seq))
(def ^:private response-slot-headers-len (mem/place h2o/ffi-clj-fixed-response-slot-data-t :headers-len))
(def ^:private response-slot-content-length (mem/place h2o/ffi-clj-fixed-response-slot-data-t :content-length))
(def ^:private response-slot-body-offset (mem/place h2o/ffi-clj-fixed-response-slot-data-t :body-offset))
(def ^:private response-slot-body-len (mem/place h2o/ffi-clj-fixed-response-slot-data-t :body-len))
(def ^:private response-slot-payload-len (mem/place h2o/ffi-clj-fixed-response-slot-data-t :payload-len))
(def ^:private response-slot-status (mem/place h2o/ffi-clj-fixed-response-slot-data-t :status))
(def ^:private response-slot-compress-hint (mem/place h2o/ffi-clj-fixed-response-slot-data-t :compress-hint))
;; The final field is the header array; its alignment equals the enclosing struct's.
(def ^:private response-slot-headers-offset
  (- response-slot-data-size (mem/sizeof [:array h2o/ffi-clj-header-t max-header-pairs])))

(deftype DirectResponsePlan [^long module-id
                             ^long request-seq
                             ^long status
                             headers
                             ^long header-bytes
                             body
                             ^long body-length
                             ^long content-length
                             ^long compress-hint])

(alter-meta! #'->DirectResponsePlan assoc :doc
             "Creates a direct response-ring plan. `module-id` and `request-seq` select the request; `status`, `headers`, and `header-bytes` describe the response head; `body`, `body-length`, and `content-length` describe the final payload; `compress-hint` selects native compression handling.")
(defn header-staging-bytes
  "Returns the established descriptor and C-string budget for `headers`."
  ^long [headers]
  (serialization/header-staging-bytes headers))

(defn ^:no-doc header-utf8-bytes
  ^long [headers]
  (serialization/header-bytes headers))

(defn direct-response-plan
  "Creates a fixed-final plan from normalized headers and a complete body.
  The caller must check eligibility and supply exact encoded byte lengths before
  publication; this constructor does not validate those limits."
  [module-id request-seq status headers header-bytes body body-length content-length compress-hint]
  (DirectResponsePlan. module-id request-seq status headers header-bytes body body-length
                       content-length compress-hint))

(def ^:private byte-array-class (Class/forName "[B"))

(defn- write-utf8!
  ^long [^MemorySegment segment ^long offset ^String value ^long length]
  (.setString segment offset value StandardCharsets/UTF_8)
  (+ offset length))

(defn ^:no-doc write-direct-response-slot!
  "Packs original response strings and body directly into claimed `slot` storage."
  [slot ^long payload-capacity ^DirectResponsePlan plan]
  (let [module-id (.-module-id plan)
        request-seq (.-request-seq plan)
        status (.-status plan)
        headers (.-headers plan)
        header-bytes (.-header-bytes plan)
        body (.-body plan)
        body-length (.-body-length plan)
        content-length (.-content-length plan)
        compress-hint (.-compress-hint plan)
        payload-length (+ header-bytes body-length)]
    (when (or (> (count headers) max-header-pairs)
              (> payload-length payload-capacity))
      (throw (ex-info "Fixed response does not fit its claimed slot"
                      {:header-count (count headers)
                       :payload-bytes payload-length
                       :payload-capacity payload-capacity})))
    (let [data slot
          payload (mem/slice data response-slot-data-size (inc payload-capacity))
          descriptors (mem/slice data response-slot-headers-offset
                                 (serialization/descriptor-bytes headers))
          body-offset (serialization/stage-headers! descriptors payload headers)]
      (when-not (= header-bytes body-offset)
        (throw (ex-info "Fixed response header size changed while packing"
                        {:expected header-bytes :actual body-offset})))
      (cond
        (zero? body-length) nil
        (string? body) (let [end (write-utf8! payload body-offset body body-length)]
                         (when-not (= payload-length end)
                           (throw (ex-info "Fixed response body size changed while packing"
                                           {:expected body-length
                                            :actual (- end body-offset)}))))
        (instance? byte-array-class body)
        (MemorySegment/copy (MemorySegment/ofArray ^bytes body) 0 ^MemorySegment payload body-offset body-length)
        :else
        (throw (ex-info "Unsupported direct fixed response body" {:type (class body)})))
      (mem/write data response-slot-module-id module-id)
      (mem/write data response-slot-request-seq request-seq)
      (mem/write data response-slot-headers-len (count headers))
      (mem/write data response-slot-content-length content-length)
      (mem/write data response-slot-body-offset body-offset)
      (mem/write data response-slot-body-len body-length)
      (mem/write data response-slot-payload-len payload-length)
      (mem/write data response-slot-status status)
      (mem/write data response-slot-compress-hint compress-hint)
      data)))

(defn notify-capacity!
  "Wakes fixed-slot waiters after release or admission closure."
  [worker]
  (let [monitor (:response-slot-monitor worker)]
    (locking monitor
      (.notifyAll ^Object monitor))))

(defn- wake-published!
  [worker]
  (try
    (pi/wake worker)
    (catch Throwable error
      (try
        (.log (System/getLogger "ol.busker.fixed-final") System$Logger$Level/ERROR
              "Fixed response wake failed" error)
        (catch Throwable _ nil)))))

(defn- claim-once!
  [worker]
  (let [^AtomicBoolean receiver-open?_ (:response-receiver-open?_ worker)
        ^ReentrantReadWriteLock receiver-lock (:response-receiver-lock worker)
        ^Lock read-lock (when receiver-lock (.readLock receiver-lock))]
    (cond
      (or (nil? receiver-open?_) (nil? read-lock) (not (.get receiver-open?_))) :closed
      (not (.tryLock read-lock)) :overloaded
      :else
      (try
        (let [receiver (.get ^AtomicReference (:response-receiver_ worker))]
          (if (or (not (.get receiver-open?_)) (nil? receiver) (mem/null? receiver))
            :closed
            (if (zero? (long (h2o/mt-response-use-acquire receiver)))
              :closed
              (let [claim (try
                            (h2o/mt-response-try-claim receiver)
                            (catch Throwable error
                              (h2o/mt-response-use-release receiver)
                              (throw error)))]
                (if (zero? claim)
                  (do (h2o/mt-response-use-release receiver) :overloaded)
                  [receiver claim])))))
        (finally (.unlock read-lock))))))

(defn- claim-response!
  [worker wait? cancelled?]
  (let [monitor (:response-slot-monitor worker)]
    (locking monitor
      (loop []
        (if (cancelled?)
          :closed
          (let [claim (claim-once! worker)]
            (if (and wait? (= :overloaded claim))
              (do (.wait ^Object monitor) (recur))
              claim)))))))

(defn- prepare-and-publish!
  [worker claim cancelled? prepare! accept!]
  (if (keyword? claim)
    claim
    (let [[receiver claim-handle] claim
          published?_ (volatile! false)]
      (try
        (if (cancelled?)
          :closed
          (let [slot-index (dec (bit-and (long claim-handle) response-claim-index-mask))
                slot (nth (:response-slots worker) slot-index)
                payload-capacity (long (:response-slot-payload-capacity worker))]
            (prepare! slot payload-capacity)
            (if (cancelled?)
              :closed
              (let [result (accept!
                            (fn []
                              (when-not (= 1 (h2o/mt-response-publish receiver claim-handle))
                                (throw (IllegalStateException. "Valid fixed response publication failed")))
                              (vreset! published?_ true)
                              :accepted))]
                (when @published?_
                  (wake-published! worker))
                result))))
        (catch Throwable error
          (if @published?_
            (do (wake-published! worker) :accepted)
            (throw error)))
        (finally
          (try
            (when-not @published?_
              (h2o/mt-response-abort receiver claim-handle))
            (finally
              (h2o/mt-response-use-release receiver)
              (when-not @published?_
                (notify-capacity! worker))
              (when-not (.get ^AtomicBoolean (:response-receiver-open?_ worker))
                (wake-published! worker)))))))))

(defn publish-direct-response!
  "Parks for a fixed slot, then publishes `plan`. Returns `:accepted` or `:closed`."
  ([worker plan]
   (publish-direct-response! worker plan (constantly false)))
  ([worker plan cancelled?]
   (publish-direct-response! worker plan cancelled? (fn [publish!] (publish!))))
  ([worker ^DirectResponsePlan plan cancelled? accept!]
   (prepare-and-publish! worker (claim-response! worker true cancelled?)
                         cancelled?
                         #(write-direct-response-slot! %1 %2 plan)
                         accept!)))

(defn- write-direct-producer-slot!
  [^MemorySegment slot payload-capacity ^DirectResponsePlan plan capacity producer]
  (write-direct-response-slot! slot payload-capacity plan)
  (let [body-offset (.-header-bytes plan)
        backing (.asSlice slot (long (+ response-slot-data-size body-offset)) (long capacity))
        length (with-open [view-arena (Arena/ofConfined)]
                 (let [view (.reinterpret backing (long capacity) view-arena nil)
                       buffer (.asByteBuffer view)]
                   (producer buffer)
                   (.position buffer)))]
    (mem/write slot response-slot-body-len length)
    (mem/write slot response-slot-payload-len (+ body-offset length))
    (when (= -1 (.-content-length plan))
      (mem/write slot response-slot-content-length length))))

(defn publish-direct-producer!
  "Fills a claimed fixed slot with a synchronous confined native producer.

  `plan` has an empty body; content length `-1` selects the produced length.
  `cancelled?` checks response closure. Closure must call [[notify-capacity!]].
  `accept!` atomically checks closure and registers the response before calling
  its publication function exactly once, or returns `:closed` without calling it.
  Returns `:accepted`, `:closed`, or `:overloaded` when `wait?` is false."
  [worker ^DirectResponsePlan plan capacity producer wait? cancelled? accept!]
  (when-not (and (integer? capacity) (pos? capacity)
                 (<= capacity response-ring-max-body-bytes)
                 (<= (+ (.-header-bytes plan) capacity)
                     (long (:response-slot-payload-capacity worker))))
    (throw (ex-info "Invalid fixed producer capacity" {:capacity capacity})))
  (prepare-and-publish!
   worker (claim-response! worker wait? cancelled?) cancelled?
   #(write-direct-producer-slot! %1 %2 plan capacity producer)
   accept!))