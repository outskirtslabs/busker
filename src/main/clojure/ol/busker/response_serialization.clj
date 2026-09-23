(ns ^:no-doc ol.busker.response-serialization
  "Native header staging shared by final and response-head delivery paths.

  This namespace calculates UTF-8 sizes and writes `clj_header_t` descriptors
  whose pointers refer to caller-provided native storage. Callers select storage
  with the lifetime required by their path: a claimed response slot, worker
  scratch, or a short-lived arena.

  ## Related Namespaces

  - [[ol.busker.fixed-final]] stages direct and mailbox final responses.
  - [[ol.busker.response-head]] stages start and informational response heads.
  - [[ol.busker.native]] defines `clj_header_t`."

  (:require
   [coffi.mem :as mem]
   [ol.busker.native :as h2o])
  (:import
   [java.lang.foreign MemorySegment]
   [java.nio.charset StandardCharsets]))

(set! *warn-on-reflection* true)

(def ^:private clj-header-size (mem/size-of ::h2o/clj-header-t))
(def ^:private clj-header-name-offset (mem/struct-field-offset ::h2o/clj-header-t :name))
(def ^:private clj-header-name-len-offset (mem/struct-field-offset ::h2o/clj-header-t :name_len))
(def ^:private clj-header-value-offset (mem/struct-field-offset ::h2o/clj-header-t :value))
(def ^:private clj-header-value-len-offset (mem/struct-field-offset ::h2o/clj-header-t :value_len))

(defn utf8-length
  "Returns the number of UTF-8 bytes required for `value`."
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

(defn header-bytes
  "Returns the UTF-8 bytes required by string header pairs in `headers`."
  ^long [headers]
  (reduce (fn [total [^String name ^String value]]
            (+ total (utf8-length name) (utf8-length value)))
          0
          headers))

(defn header-staging-bytes
  "Returns the established descriptor and C-string budget for `headers`."
  ^long [headers]
  (+ (* (count headers) clj-header-size)
     (header-bytes headers)
     (* 2 (count headers))))

(defn descriptor-bytes
  "Returns bytes occupied by `clj_header_t` descriptors for `headers`."
  ^long [headers]
  (* (count headers) clj-header-size))

(defn- write-utf8!
  ^long [^MemorySegment payload ^long offset ^String value ^long length]
  (.setString payload offset value StandardCharsets/UTF_8)
  (+ offset length))

(defn stage-headers!
  "Writes `headers` into `descriptors` and `payload`, returning UTF-8 byte use.

  Both segments must remain valid until native code has copied the descriptors."
  ^long [^MemorySegment descriptors ^MemorySegment payload headers]
  (loop [index (long 0)
         cursor (long 0)]
    (if (< index (count headers))
      (let [[^String name ^String value] (nth headers index)
            name-offset cursor
            name-length (utf8-length name)
            value-offset (write-utf8! payload name-offset name name-length)
            value-length (utf8-length value)
            next-cursor (write-utf8! payload value-offset value value-length)
            descriptor-offset (* index clj-header-size)
            name-segment (mem/slice payload name-offset (max 1 name-length))
            value-segment (mem/slice payload value-offset (max 1 value-length))]
        (mem/write-address descriptors (+ descriptor-offset clj-header-name-offset) name-segment)
        (mem/write-long descriptors (+ descriptor-offset clj-header-name-len-offset) name-length)
        (mem/write-address descriptors (+ descriptor-offset clj-header-value-offset) value-segment)
        (mem/write-long descriptors (+ descriptor-offset clj-header-value-len-offset) value-length)
        (recur (unchecked-inc index) next-cursor))
      cursor)))