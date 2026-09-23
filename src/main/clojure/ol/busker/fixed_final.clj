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
(def ^:private response-claim-index-mask 0xffff)

(def ^:private get-current-worker
  (delay (requiring-resolve 'ol.busker.evloop/get-current-worker)))

(def ^:private clj-header-size (mem/size-of ::h2o/clj-header-t))
(def ^:private clj-header-name-offset (mem/struct-field-offset ::h2o/clj-header-t :name))
(def ^:private clj-header-name-len-offset (mem/struct-field-offset ::h2o/clj-header-t :name_len))
(def ^:private clj-header-value-offset (mem/struct-field-offset ::h2o/clj-header-t :value))
(def ^:private clj-header-value-len-offset (mem/struct-field-offset ::h2o/clj-header-t :value_len))

(def ^:private response-slot-data-size
  (mem/size-of ::h2o/clj-fixed-response-slot-data-t))
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

(defn command-module-id
  ^long [^FixedFinalCommand command]
  (.-module-id command))

(defn command-request-seq
  ^long [^FixedFinalCommand command]
  (.-request-seq command))

(declare header-sizes utf8-length)
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
  ^long [headers]
  (long (first (header-sizes headers))))

(defn ^:no-doc prepared-command
  [module-id request-seq status headers header-bytes content-length compress-hint body]
  (->FixedFinalCommand module-id request-seq status headers header-bytes
                       content-length compress-hint body))

(def ^:private byte-array-class (Class/forName "[B"))

(defn ^:no-doc utf8-length
  ^long [^String value]
  (loop [index (long 0)
         length (long 0)]
    (if (< index (.length value))
      (let [character (int (.charAt value index))]
        (cond
          (< character 0x80)
          (recur (unchecked-inc index) (unchecked-inc length))

          (< character 0x800)
          (recur (unchecked-inc index) (+ length 2))

          (Character/isHighSurrogate (char character))
          (if (and (< (unchecked-inc index) (.length value))
                   (Character/isLowSurrogate (.charAt value (unchecked-inc index))))
            (recur (+ index 2) (+ length 4))
            (recur (unchecked-inc index) (unchecked-inc length)))

          (Character/isLowSurrogate (char character))
          (recur (unchecked-inc index) (unchecked-inc length))

          :else
          (recur (unchecked-inc index) (+ length 3))))
      length)))

(defn ^:no-doc header-sizes
  "Returns `[native-staging-bytes packed-utf8-bytes]` for string header pairs."
  [headers]
  (loop [index (long 0)
         staging-bytes (long 0)
         packed-bytes (long 0)]
    (if (< index (count headers))
      (let [[^String name ^String value] (nth headers index)
            name-length (utf8-length name)
            value-length (utf8-length value)]
        (recur (unchecked-inc index)
               (long (+ staging-bytes (long clj-header-size)
                        name-length value-length 2))
               (long (+ packed-bytes name-length value-length))))
      [staging-bytes packed-bytes])))

(defn ^:no-doc header-utf8-bytes
  ^long [headers]
  (long (second (header-sizes headers))))

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
          body-offset
          (loop [index (long 0)
                 cursor (long 0)]
            (if (< index (count headers))
              (let [[^String name ^String value] (nth headers index)
                    name-offset cursor
                    name-length (utf8-length name)
                    value-offset (write-utf8! payload name-offset name name-length)
                    value-length (utf8-length value)
                    next-cursor (write-utf8! payload value-offset value value-length)
                    descriptor (+ response-slot-headers-offset
                                  (* index packed-header-size))]
                (mem/write-int data (+ descriptor packed-name-offset) name-offset)
                (mem/write-int data (+ descriptor packed-name-len-offset)
                               (- value-offset name-offset))
                (mem/write-int data (+ descriptor packed-value-offset) value-offset)
                (mem/write-int data (+ descriptor packed-value-len-offset)
                               (- next-cursor value-offset))
                (recur (unchecked-inc index) next-cursor))
              cursor))]
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
        (mem/write-bytes payload body-length body-offset ^bytes body)
        :else
        (throw (ex-info "Unsupported direct fixed response body" {:type (class body)})))
      (mem/write-long data response-slot-module-id-offset module-id)
      (mem/write-long data response-slot-request-seq-offset request-seq)
      (mem/write-long data response-slot-headers-len-offset (count headers))
      (mem/write-long data response-slot-content-length-offset content-length)
      (mem/write-long data response-slot-body-offset-offset body-offset)
      (mem/write-long data response-slot-body-len-offset body-length)
      (mem/write-long data response-slot-payload-len-offset payload-length)
      (mem/write-int data response-slot-status-offset status)
      (mem/write-int data response-slot-compress-hint-offset compress-hint)
      data)))

(defn ^:no-doc try-publish-direct-response!
  "Packs and publishes an eligible response without waiting, or returns a fallback status."
  [worker ^DirectResponsePlan plan]
  (let [receiver_ (:response-receiver_ worker)
        receiver (when receiver_ (.get ^AtomicReference receiver_))]
    (if (or (nil? receiver) (mem/null? receiver))
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
                  (h2o/mt-response-abort receiver claim-handle))))))))))

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
