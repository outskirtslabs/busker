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
   [java.lang.foreign MemorySegment ValueLayout]
   [java.nio.charset StandardCharsets]))

(set! *warn-on-reflection* true)

(def ^:private clj-header-size (ffi/sizeof h2o/ffi-clj-header-t))

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

(defn staging-bytes
  "Returns the descriptor and C-string budget of `pair-count` header pairs whose
  names and values take `header-bytes` UTF-8 bytes."
  ^long [^long pair-count ^long header-bytes]
  (+ (* pair-count clj-header-size)
     header-bytes
     (* 2 pair-count)))

(defn header-staging-bytes
  "Returns the established descriptor and C-string budget for `headers`."
  ^long [headers]
  (staging-bytes (count headers) (header-bytes headers)))

(defn descriptor-bytes
  "Returns bytes occupied by `clj_header_t` descriptors for `headers`."
  ^long [headers]
  (* (count headers) clj-header-size))

(defn write-utf8!
  "Writes `value` as UTF-8 into `segment` at `offset`, followed by one zero byte,
  and returns `offset` plus `length`, the UTF-8 byte count of `value`."
  ^long [^MemorySegment segment ^long offset ^String value ^long length]
  (.setString segment offset value StandardCharsets/UTF_8)
  (+ offset length))

(defn stage-headers!
  "Writes `headers` into `segment`: `clj_header_t` descriptors from byte
  `descriptors-offset`, and their names and values from byte `payload-offset`.
  Returns the UTF-8 bytes the names and values take.

  `segment` must remain valid until native code has copied the descriptors."
  ^long [^MemorySegment segment ^long descriptors-offset ^long payload-offset headers]
  (let [base (.address segment)
        n (count headers)]
    (loop [index (long 0)
           cursor payload-offset]
      (if (< index n)
        (let [[^String name ^String value] (nth headers index)
              value-offset (write-utf8! segment cursor name (utf8-length name))
              next-cursor (write-utf8! segment value-offset value (utf8-length value))
              descriptor (+ descriptors-offset (* index h2o/header-size))]
          (.set segment ValueLayout/JAVA_LONG_UNALIGNED descriptor (+ base cursor))
          (.set segment ValueLayout/JAVA_LONG_UNALIGNED
                (+ descriptor h2o/header-length-offset) (- value-offset cursor))
          (.set segment ValueLayout/JAVA_LONG_UNALIGNED
                (+ descriptor h2o/header-value-offset) (+ base value-offset))
          (.set segment ValueLayout/JAVA_LONG_UNALIGNED
                (+ descriptor h2o/header-value-offset h2o/header-length-offset)
                (- next-cursor value-offset))
          (recur (unchecked-inc index) next-cursor))
        (- cursor payload-offset)))))
