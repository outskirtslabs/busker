(ns ^:no-doc ol.busker.fixed-final
  (:require
   [coffi.mem :as mem]
   [ol.busker.native :as h2o])
  (:import
   [java.lang.foreign MemorySegment]
   [java.nio.charset StandardCharsets]))

(set! *warn-on-reflection* true)

(def ^:const max-header-pairs 64)
(def ^:const max-header-staging-bytes 16384)

(def ^:private get-current-worker
  (delay (requiring-resolve 'ol.busker.evloop/get-current-worker)))

(def ^:private clj-header-size (mem/size-of ::h2o/clj-header-t))
(def ^:private clj-header-name-offset (mem/struct-field-offset ::h2o/clj-header-t :name))
(def ^:private clj-header-name-len-offset (mem/struct-field-offset ::h2o/clj-header-t :name_len))
(def ^:private clj-header-value-offset (mem/struct-field-offset ::h2o/clj-header-t :value))
(def ^:private clj-header-value-len-offset (mem/struct-field-offset ::h2o/clj-header-t :value_len))

(defrecord FixedFinalCommand [^long module-id
                              ^long request-seq
                              ^long status
                              headers
                              ^long header-staging-bytes
                              ^long content-length
                              ^long compress-hint
                              ^bytes body])

(defn command-module-id
  ^long [^FixedFinalCommand command]
  (.-module-id command))

(defn command-request-seq
  ^long [^FixedFinalCommand command]
  (.-request-seq command))

(defn command-content-length
  ^long [^FixedFinalCommand command]
  (.-content-length command))

(defn header-staging-bytes
  "Returns the native descriptor and UTF-8 string space for `headers`."
  [headers]
  (let [header-size (long clj-header-size)]
    (reduce
     (fn [^long total [name value]]
       (+ total
          header-size
          (inc (alength (.getBytes ^String name StandardCharsets/UTF_8)))
          (inc (alength (.getBytes ^String value StandardCharsets/UTF_8)))))
     0
     headers)))

(defn- command-data
  [module-id request-seq status headers header-bytes content-length compress-hint body-limit body]
  (let [headers (mapv (fn [[name value]] [(str name) (str value)]) headers)]
    (when (or (not (pos? module-id))
              (not (pos? request-seq))
              (not (>= status 200))
              (> (count headers) max-header-pairs)
              (> header-bytes max-header-staging-bytes)
              (not= header-bytes (header-staging-bytes headers))
              (not (nat-int? body-limit))
              (> (alength ^bytes body) body-limit)
              (some (fn [[name _]] (.equalsIgnoreCase ^String name "content-length")) headers))
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

(defn- headers-segment
  [headers arena]
  (let [header-count (long (count headers))
        header-size (long clj-header-size)
        name-offset (long clj-header-name-offset)
        name-len-offset (long clj-header-name-len-offset)
        value-offset (long clj-header-value-offset)
        value-len-offset (long clj-header-value-len-offset)]
    (if (zero? header-count)
      (mem/as-segment 0)
      (let [segment (mem/alloc (* header-count header-size) arena)]
        (loop [idx (long 0)]
          (when (< idx header-count)
            (let [[name value] (nth headers idx)
                  offset (* idx header-size)
                  name-ptr ^MemorySegment (mem/serialize name ::mem/c-string arena)
                  value-ptr ^MemorySegment (mem/serialize value ::mem/c-string arena)]
              (mem/write-address segment (+ offset name-offset) name-ptr)
              (mem/write-int segment (+ offset name-len-offset) (dec (.byteSize name-ptr)))
              (mem/write-address segment (+ offset value-offset) value-ptr)
              (mem/write-int segment (+ offset value-len-offset) (dec (.byteSize value-ptr)))
              (recur (unchecked-inc idx)))))
        segment))))

(defn- body-segment
  [^bytes body arena]
  (let [length (alength body)
        segment (mem/alloc (max 1 length) arena)]
    (when (pos? length)
      (mem/write-bytes segment length body))
    segment))

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
      (with-open [arena (mem/confined-arena)]
        (let [headers-segment (headers-segment headers arena)
              body-segment (body-segment body arena)]
          (h2o/send-fixed-final (:req-ctx-ptr req)
                                (.-status command)
                                headers-segment
                                header-count
                                (.-content-length command)
                                (.-compress-hint command)
                                body-segment
                                body-length))))))
