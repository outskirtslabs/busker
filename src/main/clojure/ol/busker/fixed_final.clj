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
   [java.lang.foreign Arena MemorySegment ValueLayout]
   [java.lang.invoke VarHandle]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicReference]
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
;; Byte offsets of the slot fields that write-direct-response-slot! writes,
;; following ffi-clj-fixed-response-slot-data-t; layout tests check them.
(def ^:no-doc ^:const slot-module-id-offset 8)
(def ^:no-doc ^:const slot-request-seq-offset 16)
(def ^:no-doc ^:const slot-headers-len-offset 24)
(def ^:no-doc ^:const slot-content-length-offset 32)
(def ^:no-doc ^:const slot-body-offset-offset 40)
(def ^:no-doc ^:const slot-body-len-offset 48)
(def ^:no-doc ^:const slot-payload-len-offset 56)
(def ^:no-doc ^:const slot-status-offset 64)
(def ^:no-doc ^:const slot-compress-hint-offset 68)
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

(defn- put-slot-long!
  [^MemorySegment slot ^long offset ^long value]
  (.set slot ValueLayout/JAVA_LONG_UNALIGNED offset value))

(defn- put-slot-int!
  [^MemorySegment slot ^long offset ^long value]
  (.set slot ValueLayout/JAVA_INT_UNALIGNED offset (unchecked-int value)))

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
    (let [^MemorySegment data slot
          ;; The payload view ends one byte after the capacity, for the zero
          ;; byte that follows the last string written.
          payload (.asSlice data (long response-slot-data-size) (inc payload-capacity))
          body-offset (serialization/stage-headers-at! data response-slot-headers-offset
                                                       payload headers)]
      (when-not (= header-bytes body-offset)
        (throw (ex-info "Fixed response header size changed while packing"
                        {:expected header-bytes :actual body-offset})))
      (cond
        (zero? body-length) nil
        (string? body) (let [end (serialization/write-utf8! payload body-offset body body-length)]
                         (when-not (= payload-length end)
                           (throw (ex-info "Fixed response body size changed while packing"
                                           {:expected body-length
                                            :actual (- end body-offset)}))))
        (instance? byte-array-class body)
        (MemorySegment/copy ^bytes body 0 payload ValueLayout/JAVA_BYTE body-offset (int body-length))
        :else
        (throw (ex-info "Unsupported direct fixed response body" {:type (class body)})))
      (put-slot-long! data slot-module-id-offset module-id)
      (put-slot-long! data slot-request-seq-offset request-seq)
      (put-slot-long! data slot-headers-len-offset (count headers))
      (put-slot-long! data slot-content-length-offset content-length)
      (put-slot-long! data slot-body-offset-offset body-offset)
      (put-slot-long! data slot-body-len-offset body-length)
      (put-slot-long! data slot-payload-len-offset payload-length)
      (put-slot-int! data slot-status-offset status)
      (put-slot-int! data slot-compress-hint-offset compress-hint)
      data)))

;; Fixed-slot waiters. A thread waits on `:response-slot-monitor` only in
;; claim-response!; under the monitor it increments `:response-slot-waiters`
;; before its first check and rechecks before each wait. A notifier makes its
;; change, issues a full fence, reads the count and takes the monitor for
;; notifyAll only when the count is above zero. Either the notifier sees the
;; count or the waiter's recheck sees the change; the monitor orders the first.
;; Closing receiver admission also wakes waiters, through notify-all-capacity!,
;; which does not read the count.

(defn notify-all-capacity!
  "Wakes every fixed-slot waiter of `worker` without reading the waiter count."
  [worker]
  (when-let [monitor (:response-slot-monitor worker)]
    (locking monitor
      (.notifyAll ^Object monitor))))

(defn notify-capacity!
  "Wakes fixed-slot waiters of `worker` after a slot is released or a response
  that may wait for a slot is closed. Call it after that change; it takes the
  monitor only when a thread is waiting or about to wait."
  [worker]
  (let [^AtomicInteger waiters (:response-slot-waiters worker)]
    (when (or (nil? waiters)
              (do (VarHandle/fullFence)
                  (pos? (.get waiters))))
      (notify-all-capacity! worker))))

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
  (let [monitor (:response-slot-monitor worker)
        ^AtomicInteger waiters (when wait? (:response-slot-waiters worker))]
    (locking monitor
      (when waiters (.incrementAndGet waiters))
      (try
        (loop []
          (if (cancelled?)
            :closed
            (let [claim (claim-once! worker)]
              (if (and wait? (= :overloaded claim))
                (do (.wait ^Object monitor) (recur))
                claim))))
        (finally
          (when waiters (.decrementAndGet waiters)))))))

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
    (put-slot-long! slot slot-body-len-offset length)
    (put-slot-long! slot slot-payload-len-offset (+ body-offset length))
    (when (= -1 (.-content-length plan))
      (put-slot-long! slot slot-content-length-offset length))))

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