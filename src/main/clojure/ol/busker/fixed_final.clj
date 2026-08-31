(ns ^:no-doc ol.busker.fixed-final
  (:require
   [coffi.mem :as mem]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o])
  (:import
   [java.lang.foreign Arena MemorySegment]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent.atomic AtomicReference]))

(set! *warn-on-reflection* true)

(def ^:const max-header-pairs 64)
(def ^:const max-header-staging-bytes 16384)
(def ^:const response-ring-capacity 256)
(def ^:const response-ring-max-body-bytes 32768)

(def ^:private get-current-worker
  (delay (requiring-resolve 'ol.busker.evloop/get-current-worker)))

(def ^:private clj-header-size (mem/size-of ::h2o/clj-header-t))
(def ^:private clj-header-name-offset (mem/struct-field-offset ::h2o/clj-header-t :name))
(def ^:private clj-header-name-len-offset (mem/struct-field-offset ::h2o/clj-header-t :name_len))
(def ^:private clj-header-value-offset (mem/struct-field-offset ::h2o/clj-header-t :value))
(def ^:private clj-header-value-len-offset (mem/struct-field-offset ::h2o/clj-header-t :value_len))

(def ^:private response-slot-data-size
  (mem/size-of ::h2o/clj-fixed-response-slot-data-t))
(def ^:private response-slot-claim-token-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :claim-token))
(def ^:private response-slot-module-id-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :module-id))
(def ^:private response-slot-request-seq-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :request-seq))
(def ^:private response-slot-headers-len-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :headers-len))
(def ^:private response-slot-content-length-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :content-length))
(def ^:private response-slot-body-offset-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :body-offset))
(def ^:private response-slot-body-len-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :body-len))
(def ^:private response-slot-payload-len-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :payload-len))
(def ^:private response-slot-status-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :status))
(def ^:private response-slot-compress-hint-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :compress-hint))
(def ^:private response-slot-headers-offset
  (mem/struct-field-offset ::h2o/clj-fixed-response-slot-data-t :headers))
(def ^:private packed-header-size (mem/size-of ::h2o/clj-packed-header-t))
(def ^:private packed-name-offset
  (mem/struct-field-offset ::h2o/clj-packed-header-t :name-offset))
(def ^:private packed-name-len-offset
  (mem/struct-field-offset ::h2o/clj-packed-header-t :name-len))
(def ^:private packed-value-offset
  (mem/struct-field-offset ::h2o/clj-packed-header-t :value-offset))
(def ^:private packed-value-len-offset
  (mem/struct-field-offset ::h2o/clj-packed-header-t :value-len))

(defrecord FixedFinalCommand [^long module-id
                              ^long request-seq
                              ^long status
                              headers
                              ^long header-staging-bytes
                              ^long content-length
                              ^long compress-hint
                              ^bytes body])

(deftype FixedFinalScratch [^Arena arena ^MemorySegment segment ^long capacity])

(defn command-module-id
  ^long [^FixedFinalCommand command]
  (.-module-id command))

(defn command-request-seq
  ^long [^FixedFinalCommand command]
  (.-request-seq command))

(defn command-content-length
  ^long [^FixedFinalCommand command]
  (.-content-length command))

(defn encode-headers
  "Returns encoded header pairs and their native staging size."
  [headers]
  (reduce
   (fn [[result ^long total] [^String name ^String value]]
     (let [name-bytes (.getBytes name StandardCharsets/UTF_8)
           value-bytes (.getBytes value StandardCharsets/UTF_8)]
       [(conj result [name-bytes value-bytes])
        (+ total
           (long clj-header-size)
           (inc (alength name-bytes))
           (inc (alength value-bytes)))]))
   [[] 0]
   headers))

(defn header-staging-bytes
  "Returns the native descriptor and UTF-8 string space for `headers`."
  [headers]
  (second (encode-headers headers)))

(defn- command-data
  [module-id request-seq status headers header-bytes content-length compress-hint body-limit body]
  (let [string-headers (mapv (fn [[name value]] [(str name) (str value)]) headers)
        [headers calculated-header-bytes] (encode-headers string-headers)]
    (when (or (not (pos? module-id))
              (not (pos? request-seq))
              (not (>= status 200))
              (> (count headers) max-header-pairs)
              (> header-bytes max-header-staging-bytes)
              (not= header-bytes calculated-header-bytes)
              (not (nat-int? body-limit))
              (> (alength ^bytes body) body-limit)
              (some (fn [[name _]] (.equalsIgnoreCase ^String name "content-length"))
                    string-headers))
      (throw (ex-info "Invalid fixed final command"
                      {:module-id module-id
                       :request-seq request-seq
                       :status status
                       :header-count (count headers)
                       :header-staging-bytes header-bytes
                       :body-bytes (alength ^bytes body)
                       :body-limit body-limit})))
    (->FixedFinalCommand module-id request-seq status headers header-bytes
                         content-length compress-hint (aclone ^bytes body))))

(defn ^:no-doc prepared-command
  [module-id request-seq status headers header-bytes content-length compress-hint body]
  (->FixedFinalCommand module-id request-seq status headers header-bytes
                       content-length compress-hint body))

(defn command
  "Creates a fixed final command with copied JVM body bytes."
  [module-id request-seq status headers header-bytes content-length compress-hint body-limit body]
  (command-data module-id request-seq status headers header-bytes content-length compress-hint body-limit body))

(defn ^:no-doc write-response-slot!
  "Writes `command` into claimed `slot` storage and returns the slot segment."
  [slot ^long payload-capacity ^FixedFinalCommand command]
  (let [encoded-headers (.-headers command)
        header-bytes
        (reduce (fn [^long total [^bytes name ^bytes value]]
                  (+ total (alength name) (alength value)))
                0
                encoded-headers)
        ^bytes body (.-body command)
        body-length (long (alength body))
        payload-length (+ header-bytes body-length)]
    (when (or (> (count encoded-headers) max-header-pairs)
              (> payload-length payload-capacity))
      (throw (ex-info "Fixed response does not fit its claimed slot"
                      {:header-count (count encoded-headers)
                       :payload-bytes payload-length
                       :payload-capacity payload-capacity})))
    (let [data (mem/reinterpret slot (+ response-slot-data-size payload-capacity))
          payload (mem/slice data response-slot-data-size payload-capacity)
          body-offset
          (loop [index (long 0)
                 cursor (long 0)]
            (if (< index (count encoded-headers))
              (let [[^bytes name ^bytes value] (nth encoded-headers index)
                    name-length (long (alength name))
                    value-offset (+ cursor name-length)
                    value-length (long (alength value))
                    descriptor (+ response-slot-headers-offset
                                  (* index packed-header-size))]
                (mem/write-bytes payload name-length cursor name)
                (mem/write-bytes payload value-length value-offset value)
                (mem/write-int data (+ descriptor packed-name-offset) cursor)
                (mem/write-int data (+ descriptor packed-name-len-offset) name-length)
                (mem/write-int data (+ descriptor packed-value-offset) value-offset)
                (mem/write-int data (+ descriptor packed-value-len-offset) value-length)
                (recur (unchecked-inc index) (+ value-offset value-length)))
              cursor))]
      (when (pos? body-length)
        (mem/write-bytes payload body-length body-offset body))
      (mem/write-long data response-slot-module-id-offset (.-module-id command))
      (mem/write-long data response-slot-request-seq-offset (.-request-seq command))
      (mem/write-long data response-slot-headers-len-offset (count encoded-headers))
      (mem/write-long data response-slot-content-length-offset (.-content-length command))
      (mem/write-long data response-slot-body-offset-offset body-offset)
      (mem/write-long data response-slot-body-len-offset body-length)
      (mem/write-long data response-slot-payload-len-offset payload-length)
      (mem/write-int data response-slot-status-offset (.-status command))
      (mem/write-int data response-slot-compress-hint-offset (.-compress-hint command))
      data)))

(defn ^:no-doc try-publish-response!
  "Publishes `command` through `worker` without waiting, or returns a fallback status."
  [worker ^FixedFinalCommand command]
  (let [receiver_ (:response-receiver_ worker)
        receiver (when receiver_ (.get ^AtomicReference receiver_))]
    (if (or (nil? receiver) (mem/null? receiver))
      :closed
      (let [slot (h2o/mt-response-try-claim receiver)]
        (if (mem/null? slot)
          :overloaded
          (let [data (mem/reinterpret slot response-slot-data-size)
                claim-token (mem/read-long data (long response-slot-claim-token-offset))
                published?_ (volatile! false)]
            (try
              (write-response-slot! slot
                                    (h2o/mt-response-slot-payload-capacity receiver)
                                    command)
              (if (= 1 (h2o/mt-response-publish receiver slot claim-token))
                (do
                  (vreset! published?_ true)
                  (pi/wake worker)
                  :accepted)
                :overloaded)
              (finally
                (when-not @published?_
                  (h2o/mt-response-abort receiver slot claim-token))))))))))

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
            (let [segment (mem/alloc capacity arena)
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
  [^MemorySegment segment headers ^long header-bytes ^bytes body]
  (let [header-count (long (count headers))
        descriptors-size (* header-count (long clj-header-size))
        body-length (long (alength body))
        headers-segment (if (zero? header-count)
                          (mem/as-segment 0)
                          (mem/slice segment 0 descriptors-size))]
    (loop [idx (long 0)
           cursor descriptors-size]
      (when (< idx header-count)
        (let [[^bytes name-bytes ^bytes value-bytes] (nth headers idx)
              name-length (long (alength name-bytes))
              name-segment (mem/slice segment cursor (inc name-length))
              value-offset (+ cursor name-length 1)
              value-length (long (alength value-bytes))
              value-segment (mem/slice segment value-offset (inc value-length))
              descriptor-offset (* idx (long clj-header-size))]
          (mem/write-bytes name-segment name-length name-bytes)
          (mem/write-byte name-segment name-length (byte 0))
          (mem/write-bytes value-segment value-length value-bytes)
          (mem/write-byte value-segment value-length (byte 0))
          (mem/write-address headers-segment (+ descriptor-offset (long clj-header-name-offset)) name-segment)
          (mem/write-int headers-segment (+ descriptor-offset (long clj-header-name-len-offset)) name-length)
          (mem/write-address headers-segment (+ descriptor-offset (long clj-header-value-offset)) value-segment)
          (mem/write-int headers-segment (+ descriptor-offset (long clj-header-value-len-offset)) value-length)
          (recur (unchecked-inc idx) (+ value-offset value-length 1)))))
    (let [body-segment (mem/slice segment header-bytes (max 1 body-length))]
      (when (pos? body-length)
        (mem/write-bytes body-segment body-length body))
      [headers-segment body-segment])))

(defn execute!
  "Stages `command` on its event-loop worker and sends it synchronously."
  [req ^FixedFinalCommand command]
  (when-not (identical? (:worker req) (@get-current-worker))
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
            (stage-segments scratch-segment headers header-bytes body)]
        (h2o/send-fixed-final (:req-ctx-ptr req)
                              (.-status command)
                              headers-segment
                              header-count
                              (.-content-length command)
                              (.-compress-hint command)
                              body-segment
                              body-length)))))
