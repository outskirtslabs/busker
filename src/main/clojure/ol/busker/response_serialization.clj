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
   [babashka.ffi :as ffi]
   [ol.busker.native :as h2o])
  (:import
   [java.lang.foreign MemorySegment]
   [java.nio.charset StandardCharsets]))

(set! *warn-on-reflection* true)

(def ^:private clj-header-size (ffi/sizeof h2o/ffi-clj-header-t))
(def ^:private clj-header-name (ffi/place h2o/ffi-clj-header-t :name))
(def ^:private clj-header-name-len (ffi/place h2o/ffi-clj-header-t :name_len))
(def ^:private clj-header-value (ffi/place h2o/ffi-clj-header-t :value))
(def ^:private clj-header-value-len (ffi/place h2o/ffi-clj-header-t :value_len))

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
            descriptor (ffi/slice descriptors (* index clj-header-size) clj-header-size)
            name-segment (ffi/slice payload name-offset (max 1 name-length))
            value-segment (ffi/slice payload value-offset (max 1 value-length))]
        (ffi/write descriptor clj-header-name name-segment)
        (ffi/write descriptor clj-header-name-len name-length)
        (ffi/write descriptor clj-header-value value-segment)
        (ffi/write descriptor clj-header-value-len value-length)
        (recur (unchecked-inc index) next-cursor))
      cursor)))