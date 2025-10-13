(ns ol.h2o.bitfield
  "Helpers for working with C bitfields in Panama FFI.
  
  C bitfields have no standard ABI - layout is compiler and platform dependent.
  These helpers provide a way to define bitfield layouts and access individual bits
  safely, with runtime verification support."
  (:require [coffi.mem :as mem]))

(defn defbitfield
  "Define a bitfield layout specification.
  
  Returns a map with field definitions for bit manipulation.
  
  Args:
    fields - vector of [field-name bit-position] or [field-name bit-position bit-width]
             bit-width defaults to 1
  
  Example:
    (defbitfield handler-flags
      [[:supports-request-streaming 0]
       [:handles-expect 1]])
  
  Returns:
    {:fields {:supports-request-streaming {:pos 0 :width 1 :mask 0x01}
              :handles-expect {:pos 1 :width 1 :mask 0x02}}
     :storage-type ::mem/char  ; or ::mem/short, ::mem/int depending on total bits}"
  [fields]
  (let [field-defs (into {}
                         (map (fn [[name pos width]]
                                (let [w (or width 1)
                                      mask (bit-shift-left (dec (bit-shift-left 1 w)) pos)]
                                  [name {:pos pos :width w :mask mask}])))
                         fields)
        max-bits (apply max (map (fn [[_ pos width]]
                                   (+ pos (or width 1)))
                                 fields))
        storage-type (cond
                       (<= max-bits 8) ::mem/byte
                       (<= max-bits 16) ::mem/short
                       (<= max-bits 32) ::mem/int
                       :else ::mem/long)]
    {:fields field-defs
     :storage-type storage-type
     :max-bits max-bits}))

(defn get-bit
  "Extract a single bit field value from an integer.
  
  Args:
    value - the integer containing packed bitfields (may be boxed Number)
    field-spec - field spec from defbitfield (map with :pos, :width, :mask)
  
  Returns:
    The extracted field value as a long"
  [value {:keys [pos width mask]}]
  (let [v (long value)]
    (bit-shift-right (bit-and v mask) pos)))

(defn set-bit
  "Set a single bit field value in an integer.
  
  Args:
    value - the integer containing packed bitfields (may be boxed Number)
    field-spec - field spec from defbitfield
    field-value - the value to set (will be masked to width)
  
  Returns:
    The new integer with the field updated"
  [value {:keys [pos width mask]} field-value]
  (let [v (long value)
        fv (long field-value)
        cleared (bit-and v (bit-not mask))
        shifted (bit-shift-left (bit-and fv (dec (bit-shift-left 1 width))) pos)]
    (bit-or cleared shifted)))

(defn read-bitfield
  "Read all bitfield values from a memory segment at a given offset.
  
  Args:
    segment - Panama memory segment
    offset - byte offset into segment
    bitfield-spec - spec from defbitfield
  
  Returns:
    Map of field-name -> value"
  [segment offset {:keys [fields storage-type]}]
  (let [raw-value (case storage-type
                    ::mem/byte (mem/read-byte segment offset)
                    ::mem/short (mem/read-short segment offset)
                    ::mem/int (mem/read-int segment offset)
                    ::mem/long (mem/read-long segment offset))]
    (into {}
          (map (fn [[name field-spec]]
                 [name (get-bit raw-value field-spec)]))
          fields)))

(defn write-bitfield
  "Write bitfield values to a memory segment at a given offset.
  
  Args:
    segment - Panama memory segment
    offset - byte offset into segment
    bitfield-spec - spec from defbitfield
    values - map of field-name -> value to set
  
  Side effects:
    Writes to memory segment"
  [segment offset {:keys [fields storage-type]} values]
  (let [current (case storage-type
                  ::mem/byte (mem/read-byte segment offset)
                  ::mem/short (mem/read-short segment offset)
                  ::mem/int (mem/read-int segment offset)
                  ::mem/long (mem/read-long segment offset))
        new-value (reduce (fn [acc [name value]]
                            (if-let [field-spec (get fields name)]
                              (set-bit acc field-spec value)
                              acc))
                          current
                          values)]
    (case storage-type
      ::mem/byte (mem/write-byte segment offset new-value)
      ::mem/short (mem/write-short segment offset new-value)
      ::mem/int (mem/write-int segment offset new-value)
      ::mem/long (mem/write-long segment offset new-value))))

(defn test-bit?
  "Test if a single boolean bitfield is set (non-zero).
  
  Args:
    value - the integer containing packed bitfields (may be boxed Number)
    field-spec - field spec for a 1-bit field
  
  Returns:
    true if bit is set, false otherwise"
  [value {:keys [mask]}]
  (let [v (long value)]
    (not (zero? (bit-and v mask)))))

(defn make-bitfield-value
  "Create a packed bitfield value from a map of field names to values.
  
  Args:
    bitfield-spec - spec from defbitfield
    values - map of field-name -> value
  
  Returns:
    Integer with all fields packed"
  [{:keys [fields]} values]
  (reduce (fn [acc [name value]]
            (if-let [field-spec (get fields name)]
              (set-bit acc field-spec value)
              acc))
          0
          values))

(comment
  ;; Example usage:
  (def handler-flags
    (defbitfield [[:supports-request-streaming 0]
                  [:handles-expect 1]]))

  ;; Create a value with both flags set
  (def flags-value
    (make-bitfield-value handler-flags
                         {:supports-request-streaming 1
                          :handles-expect 1}))
  ;; => 3 (binary 11)

  ;; Test individual bits
  (test-bit? flags-value (get-in handler-flags [:fields :supports-request-streaming]))
  ;; => true

  ;; Read from memory
  (let [segment (mem/alloc 1)]
    (mem/serialize 3 ::mem/char segment)
    (read-bitfield segment 0 handler-flags))
  ;; => {:supports-request-streaming 1, :handles-expect 1}
  )
