(ns ^:no-doc ol.busker.fixed-final
  "Prepares and delivers complete final responses on the worker response paths.

  Direct plans write retained `clj_header_t` descriptors and payload bytes into a claimed
  response-ring slot. FIFO commands stage the same descriptor representation in worker
  scratch storage. [[try-publish-direct-response!]] never waits; [[execute!]] runs on
  the event-loop thread when response ordering requires the mailbox path.

  ## Related Namespaces

  - [[ol.busker.response]] chooses direct or FIFO final delivery.
  - [[ol.busker.response-serialization]] stages shared header descriptors.
  - [[ol.busker.response-head]] sends streaming response heads."
  (:require
   [babashka.ffi :as mem]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.response-serialization :as serialization]
   [ol.busker.worker-context :as worker-context])
  (:import
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

;; On supported 64-bit targets `clj_header_t` is 32 bytes. The 64 retained descriptors
;; add 1024 bytes to each response slot (2120 bytes of metadata total), or 256 KiB per
;; 256-slot worker compared with the former 16-byte packed-descriptor representation.

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

(defrecord FixedFinalCommand [^long module-id
                              ^long request-seq
                              ^long status
                              headers
                              ^long header-staging-bytes
                              ^long content-length
                              ^long compress-hint
                              ^bytes body])

(deftype DirectResponsePlan [^long module-id
                             ^long request-seq
                             ^long status
                             headers
                             ^long header-bytes
                             body
                             ^long body-length
                             ^long content-length
                             ^long compress-hint])
(deftype FixedFinalScratch [^Arena arena ^MemorySegment segment ^long capacity])

(alter-meta! #'->FixedFinalCommand assoc :doc
             "Creates a FIFO fixed-final command. `module-id` and `request-seq` select the request; `status`, `headers`, `content-length`, and `compress-hint` describe the final response; `header-staging-bytes` is the native staging requirement; `body` is the byte payload.")
(alter-meta! #'->DirectResponsePlan assoc :doc
             "Creates a direct response-ring plan. `module-id` and `request-seq` select the request; `status`, `headers`, and `header-bytes` describe the response head; `body`, `body-length`, and `content-length` describe the final payload; `compress-hint` selects native compression handling.")
(alter-meta! #'->FixedFinalScratch assoc :doc
             "Creates worker-local FIFO staging storage. `arena` retains native storage, `segment` holds serialized headers and body bytes, and `capacity` is its byte capacity.")

(defn command-module-id
  ^long [^FixedFinalCommand command]
  (.-module-id command))

(defn command-request-seq
  ^long [^FixedFinalCommand command]
  (.-request-seq command))

(defn header-staging-bytes
  "Returns the established descriptor and C-string budget for `headers`."
  ^long [headers]
  (serialization/header-staging-bytes headers))

(defn ^:no-doc header-utf8-bytes
  ^long [headers]
  (serialization/header-bytes headers))

(defn ^:no-doc prepared-command
  [module-id request-seq status headers header-staging-bytes content-length compress-hint body]
  (->FixedFinalCommand module-id request-seq status headers header-staging-bytes
                       content-length compress-hint body))

(defn direct-response-plan
  "Creates an eligible fixed-final plan without exposing its representation."
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

(defn ^:no-doc try-publish-direct-response!
  "Packs and publishes an eligible response without waiting, or returns a fallback status."
  [worker ^DirectResponsePlan plan]
  (let [^AtomicBoolean receiver-open?_ (:response-receiver-open?_ worker)
        ^ReentrantReadWriteLock receiver-lock (:response-receiver-lock worker)
        ^Lock read-lock (when receiver-lock (.readLock receiver-lock))]
    (if (or (nil? receiver-open?_)
            (nil? read-lock)
            (not (.get receiver-open?_))
            (not (.tryLock read-lock)))
      :closed
      (try
        (let [receiver_ (:response-receiver_ worker)
              receiver (when receiver_ (.get ^AtomicReference receiver_))]
          (if (or (not (.get receiver-open?_))
                  (nil? receiver)
                  (mem/null? receiver))
            :closed
            (let [claim-handle (h2o/mt-response-try-claim receiver)]
              (if (zero? claim-handle)
                :overloaded
                (let [slot-index (dec (bit-and claim-handle response-claim-index-mask))
                      slot (nth (:response-slots worker) slot-index)
                      payload-capacity (long (:response-slot-payload-capacity worker))
                      published?_ (volatile! false)]
                  (try
                    (write-direct-response-slot! slot payload-capacity plan)
                    (if (= 1 (h2o/mt-response-publish receiver claim-handle))
                      (do
                        (vreset! published?_ true)
                        (pi/wake worker)
                        :accepted)
                      :overloaded)
                    (finally
                      (when-not @published?_
                        (h2o/mt-response-abort receiver claim-handle)))))))))
        (finally
          (.unlock read-lock)
          (when-not (.get receiver-open?_)
            (pi/wake worker)))))))

(defn- worker-scratch-segment
  [worker ^long body-limit]
  (let [^AtomicReference scratch_ (:fixed-final-scratch_ worker)]
    (when-not scratch_
      (throw (ex-info "Worker has no fixed-final scratch state" {})))
    (let [capacity (+ max-header-staging-bytes (max 1 body-limit))]
      (if-let [^FixedFinalScratch scratch (.get scratch_)]
        (do
          (when-not (= capacity (.-capacity scratch))
            (throw (ex-info "Fixed-final scratch capacity changed"
                            {:expected (.-capacity scratch) :actual capacity})))
          (.-segment scratch))
        (let [arena (Arena/ofConfined)]
          (try
            (let [segment (mem/alloc arena capacity)
                  scratch (FixedFinalScratch. arena segment capacity)]
              (.set scratch_ scratch)
              segment)
            (catch Throwable error
              (.close arena)
              (throw error))))))))

(defn close-worker-scratch!
  "Releases fixed-final scratch on its event-loop worker."
  [worker]
  (when-let [^AtomicReference scratch_ (:fixed-final-scratch_ worker)]
    (when-let [^FixedFinalScratch scratch (.getAndSet scratch_ nil)]
      (let [^Arena arena (.-arena scratch)]
        (.close arena)))))

(defn- stage-segments
  [^MemorySegment segment headers ^bytes body]
  (let [header-count (long (count headers))
        descriptor-bytes (serialization/descriptor-bytes headers)
        body-length (long (alength body))
        headers-segment (if (zero? header-count)
                          mem/null
                          (mem/slice segment 0 descriptor-bytes))
        payload (mem/slice segment descriptor-bytes
                           (inc (+ (serialization/header-bytes headers) body-length)))
        body-offset (serialization/stage-headers! headers-segment payload headers)
        body-segment (mem/slice payload body-offset (max 1 body-length))]
    (when (pos? body-length)
      (MemorySegment/copy (MemorySegment/ofArray body) 0 ^MemorySegment body-segment 0 body-length))
    [headers-segment body-segment]))
(defn execute!
  "Stages `command` on its event-loop worker and sends it synchronously."
  [req ^FixedFinalCommand command]
  (when-not (identical? (:worker req) (worker-context/get-current-worker))
    (throw (ex-info "Fixed final command ran outside its event-loop worker" {})))
  (let [headers (.-headers command)
        ^bytes body (.-body command)
        body-limit-value (get-in req [:config :output-buffer-size])]
    (when-not (nat-int? body-limit-value)
      (throw (ex-info "Fixed final command exceeded its staging budget" {})))
    (let [body-limit (long body-limit-value)
          body-length (long (alength body))
          header-count (long (count headers))
          header-bytes (.-header-staging-bytes command)]
      (when (or (> body-length body-limit)
                (> header-count max-header-pairs)
                (> header-bytes max-header-staging-bytes))
        (throw (ex-info "Fixed final command exceeded its staging budget" {})))
      (let [scratch-segment (worker-scratch-segment (:worker req) body-limit)
            [headers-segment body-segment]
            (stage-segments scratch-segment headers body)]
        (h2o/send-fixed-final (:req-ctx-ptr req)
                              (.-status command)
                              headers-segment
                              header-count
                              (.-content-length command)
                              (.-compress-hint command)
                              body-segment
                              body-length)))))
