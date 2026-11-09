(ns ^{:no-doc true :clj-kondo/config '{:linters {:locking-suspicious-lock {:level :off}}}}
 ol.busker.byte-bounded-queue
  "Accounts pending streaming bytes across private preparation and accepted records.

  A response uses its `:output-buffer-size` as the positive byte capacity and
  shares its state monitor with this budget. [[reserve!]] never waits and rejects
  reservations larger than the capacity. The response controller handles waiting
  and closure; [[release!]] wakes monitor waiters when byte credit returns.

  Credit returns on discard or installation of a native send, not on mailbox
  consumption. Native in-flight bytes and physical block retention have separate
  limits in [[ol.busker.response-queue]] and [[ol.busker.buffer-pool]].")

(defrecord ByteBudget [capacity lock used_])

(defn byte-budget
  "Creates a byte budget synchronized with the response's `lock`."
  [capacity lock]
  (when-not (and (integer? capacity) (pos? capacity))
    (throw (IllegalArgumentException. "Byte capacity must be positive")))
  (->ByteBudget capacity lock (atom 0)))

(defn queued-bytes [budget]
  (locking (:lock budget) @(:used_ budget)))

(defn remaining-bytes [budget]
  (- (:capacity budget) (queued-bytes budget)))

(defn reserve!
  "Reserves `n` pending bytes without waiting. Returns true on success or nil if full."
  [budget n]
  (when-not (and (integer? n) (<= 0 n (:capacity budget)))
    (throw (IllegalArgumentException. "Invalid response byte reservation")))
  (locking (:lock budget)
    (when (<= (+ @(:used_ budget) n) (:capacity budget))
      (swap! (:used_ budget) + n)
      true)))

(defn release!
  "Releases bytes on discard or native in-flight installation, not mailbox receipt."
  [budget n]
  (locking (:lock budget)
    (when-not (<= 0 n @(:used_ budget))
      (throw (IllegalStateException. "Response byte credit released twice")))
    (swap! (:used_ budget) - n)
    (.notifyAll ^Object (:lock budget))))
