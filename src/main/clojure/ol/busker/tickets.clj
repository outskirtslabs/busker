(ns ol.busker.tickets
  "Session ticket key management for TLS 1.3 resumption and 0-RTT."
  (:require
   [clojure.edn :as edn]
   [coffi.mem :as mem]
   [ol.busker.native :as h2o]
   [ol.clave.storage :as storage])
  (:import
   java.nio.file.NoSuchFileException
   [java.security SecureRandom]))

(set! *warn-on-reflection* true)

(def ^:const default-ticket-lifetime-seconds
  86400)

(def ^:const default-max-ticket-keys
  4)

(def ^:const rotation-check-interval-ms
  120000)

(def ^:const ticket-storage-key
  (storage/storage-key "busker" "session_tickets" "keys.edn"))

(def ^:const ticket-rotation-lock-name
  "busker-session-ticket-rotation")

(def ^:dynamic *ticket-rotation-jitter-seconds*
  "Jitter seconds for rotation checks.
   Bind to 0 in tests to disable."
  6)

(defprotocol TicketKeyStore
  "Backend for session ticket key persistence."

  (load-snapshot [store]
    "Load the current ticket snapshot from the backing store.

    Returns a snapshot map or nil if no snapshot is stored.")

  (store-snapshot! [store snapshot]
    "Atomically persist the complete ticket snapshot.")

  (with-store-lock [store f]
    "Run `f` while holding any store-specific coordination lock."))

(defn hex->bytes
  "Convert hex string to byte array."
  ^bytes [^String s]
  (let [len (/ (count s) 2)
        arr (byte-array len)]
    (dotimes [i len]
      (aset arr i (unchecked-byte
                   (Integer/parseInt (subs s (* i 2) (+ (* i 2) 2)) 16))))
    arr))

(defn bytes->hex
  "Convert byte array to lowercase hex string."
  ^String [^bytes arr]
  (let [sb (StringBuilder.)]
    (doseq [b arr]
      (.append sb (format "%02x" (bit-and (long b) 0xff))))
    (str sb)))

(defn generate-random-bytes
  "Generate cryptographically secure random bytes."
  ^bytes [n]
  (let [arr (byte-array n)
        rng (SecureRandom.)]
    (.nextBytes rng arr)
    arr))

(defn generate-key
  "Generate a new session ticket key with specified validity window."
  [not-before-ms lifetime-ms]
  {:name (generate-random-bytes 16)
   :aes-key (generate-random-bytes 32)
   :hmac-key (generate-random-bytes 64)
   :not-before not-before-ms
   :not-after (+ not-before-ms lifetime-ms -1)})

(defn key->edn
  "Convert a key map to EDN-safe format."
  [{:keys [name aes-key hmac-key not-before not-after]}]
  {:name (bytes->hex name)
   :aes-key (bytes->hex aes-key)
   :hmac-key (bytes->hex hmac-key)
   :not-before not-before
   :not-after not-after})

(defn edn->key
  "Convert EDN format back to a key map with byte arrays."
  [{:keys [name aes-key hmac-key not-before not-after]}]
  {:name (hex->bytes name)
   :aes-key (hex->bytes aes-key)
   :hmac-key (hex->bytes hmac-key)
   :not-before not-before
   :not-after not-after})

(defn- key-set->edn
  [keys]
  (mapv key->edn (or keys [])))

(defn- snapshot->edn
  [snapshot]
  (when snapshot
    {:keys (key-set->edn (:keys snapshot))
     :last-rotation-ms (:last-rotation-ms snapshot)
     :next-rotation-ms (:next-rotation-ms snapshot)}))

(defn- same-key-set?
  [a b]
  (= (key-set->edn a)
     (key-set->edn b)))

(defn- same-snapshot?
  [a b]
  (= (snapshot->edn a)
     (snapshot->edn b)))

(defn- parse-stored-snapshot
  [content]
  (let [data (edn/read-string content)]
    (when-not (map? data)
      (throw (ex-info "Stored session ticket snapshot must be a map." {})))
    (let [keys (:keys data)
          last-rotation-ms (:last-rotation-ms data)
          next-rotation-ms (:next-rotation-ms data)]
      (when-not (vector? keys)
        (throw (ex-info "Stored session ticket snapshot must contain :keys." {})))
      (when-not (or (nil? last-rotation-ms) (integer? last-rotation-ms))
        (throw (ex-info "Stored session ticket snapshot has invalid :last-rotation-ms." {})))
      (when-not (or (nil? next-rotation-ms) (integer? next-rotation-ms))
        (throw (ex-info "Stored session ticket snapshot has invalid :next-rotation-ms." {})))
      {:keys (mapv edn->key keys)
       :last-rotation-ms last-rotation-ms
       :next-rotation-ms next-rotation-ms})))

(defn- corrupt-storage-error
  [cause]
  (ex-info "Stored session ticket data is corrupt."
           {::load-status :corrupt}
           cause))

(defn- storage-load-error
  [cause]
  (ex-info "Failed to load stored session ticket data."
           {::load-status :storage-error}
           cause))

(defn- load-storage-snapshot
  [storage-impl]
  (try
    (let [content (storage/load-string storage-impl
                                       nil
                                       ticket-storage-key)]
      (try
        (parse-stored-snapshot content)
        (catch Exception e
          (throw (corrupt-storage-error e)))))
    (catch NoSuchFileException _
      nil)
    (catch clojure.lang.ExceptionInfo e
      (throw e))
    (catch Exception e
      (throw (storage-load-error e)))))

(defn- store-storage-snapshot!
  [storage-impl snapshot]
  (storage/store-string! storage-impl
                         nil
                         ticket-storage-key
                         (pr-str (snapshot->edn snapshot))))

(deftype MemoryTicketStore [snapshot-atom]
  TicketKeyStore
  (load-snapshot [_]
    @snapshot-atom)
  (store-snapshot! [_ snapshot]
    (reset! snapshot-atom snapshot)
    nil)
  (with-store-lock [_ f]
    (f)))

(defn memory-ticket-store
  "Create an in-memory ticket key store."
  ([] (memory-ticket-store nil))
  ([snapshot]
   (->MemoryTicketStore (atom snapshot))))

(deftype StorageTicketStore [storage-impl]
  TicketKeyStore
  (load-snapshot [_]
    (load-storage-snapshot storage-impl))
  (store-snapshot! [_ snapshot]
    (store-storage-snapshot! storage-impl snapshot))
  (with-store-lock [_ f]
    (storage/with-lock storage-impl
      nil
      ticket-rotation-lock-name
      f)))

(defn storage-ticket-store
  "Create a Clave-backed ticket key store."
  [storage-impl]
  (->StorageTicketStore storage-impl))

(defn- load-keys-result
  [store]
  (try
    (let [snapshot (load-snapshot store)]
      (if snapshot
        {:status :ok
         :snapshot snapshot
         :keys (:keys snapshot)}
        {:status :missing}))
    (catch clojure.lang.ExceptionInfo e
      {:status (or (::load-status (ex-data e))
                   :storage-error)
       :exception e})
    (catch Exception e
      {:status :storage-error
       :exception e})))

(defn- prune-expired-keys
  [keys now-ms]
  (filterv #(>= (:not-after %) now-ms) keys))

(defn- retain-max-keys
  [keys max-keys]
  (->> keys
       (take max-keys)
       vec))

(defn- current-time-ms
  []
  (System/currentTimeMillis))

(defn- rotation-interval-ms
  [lifetime-ms]
  (max 1 (quot lifetime-ms 4)))

(defn- active-key?
  [key now-ms]
  (and (<= (:not-before key) now-ms)
       (< now-ms (:not-after key))))

(defn- needs-new-key?
  [keys now-ms lifetime-ms]
  (if (empty? keys)
    true
    (let [newest (first keys)
          rotation-threshold (/ lifetime-ms 4)
          age (- now-ms (:not-before newest))]
      (>= age rotation-threshold))))

(defn- snapshot-needs-rotation?
  [snapshot now-ms lifetime-ms]
  (let [keys (-> snapshot :keys (or []) vec)
        valid-keys (prune-expired-keys keys now-ms)]
    (cond
      (not-any? #(active-key? % now-ms) valid-keys)
      true

      (integer? (:next-rotation-ms snapshot))
      (>= now-ms (:next-rotation-ms snapshot))

      :else
      (needs-new-key? valid-keys now-ms lifetime-ms))))

(defn rotate-keys
  "Perform key rotation if needed.

  Returns updated keys ordered newest-first."
  [keys now-ms lifetime-ms max-keys]
  ;; Rotation keeps all non-expired keys and prepends one fresh key when the
  ;; newest active key reaches 1/4 of its lifetime. The retained set is capped
  ;; at `max-keys`. New keys become active immediately so encryption always has
  ;; a currently valid key even when operators configure `:max-keys 1`.
  (let [valid-keys (-> keys
                       (prune-expired-keys now-ms)
                       (retain-max-keys max-keys))]
    (if (needs-new-key? valid-keys now-ms lifetime-ms)
      (let [new-key (generate-key now-ms lifetime-ms)
            next-keys (into [new-key] valid-keys)]
        (retain-max-keys next-keys max-keys))
      valid-keys)))

(defn- snapshot-with-keys
  [keys now-ms lifetime-ms]
  {:keys (vec keys)
   :last-rotation-ms now-ms
   :next-rotation-ms (+ now-ms (rotation-interval-ms lifetime-ms))})

(defn- rotate-snapshot
  [snapshot now-ms lifetime-ms max-keys]
  (let [keys (rotate-keys (:keys snapshot) now-ms lifetime-ms max-keys)]
    (if (and snapshot
             (same-key-set? (:keys snapshot) keys))
      snapshot
      (snapshot-with-keys keys now-ms lifetime-ms))))

(defn sync-keys-to-native!
  "Push current keys to the native ticket manager."
  [native-mgr keys]
  (if (empty? keys)
    (h2o/ticket-manager-set-keys native-mgr mem/null 0)
    (with-open [arena (mem/confined-arena)]
      (let [arr (h2o/session-ticket-keys->native-array keys arena)]
        (h2o/ticket-manager-set-keys native-mgr arr (count keys))))))

(defn- sync-service-to-native!
  [{:keys [native-mgrs-atom snapshot-atom]}]
  (let [keys (:keys @snapshot-atom)]
    (doseq [native-mgr @native-mgrs-atom]
      (sync-keys-to-native! native-mgr keys))))

(defn- replace-current-snapshot!
  [{:keys [snapshot-atom] :as service} snapshot]
  (reset! snapshot-atom snapshot)
  (sync-service-to-native! service))

(defn- initialize-snapshot
  [store lifetime-ms max-keys]
  (let [now-ms (current-time-ms)]
    (with-store-lock
      store
      (fn []
        (let [{:keys [status snapshot exception]} (load-keys-result store)]
          (case status
            :ok
            (let [winner (if (snapshot-needs-rotation? snapshot now-ms lifetime-ms)
                           (rotate-snapshot snapshot now-ms lifetime-ms max-keys)
                           snapshot)]
              (when-not (same-snapshot? snapshot winner)
                (store-snapshot! store winner))
              winner)

            :missing
            (let [winner (rotate-snapshot nil now-ms lifetime-ms max-keys)]
              (store-snapshot! store winner)
              winner)

            (throw exception)))))))

(defn- refresh-snapshot!
  [store current-snapshot lifetime-ms max-keys]
  (let [now-ms (current-time-ms)
        load-result (load-keys-result store)]
    (cond
      (= :storage-error (:status load-result))
      current-snapshot

      (and (= :ok (:status load-result))
           (not (snapshot-needs-rotation? (:snapshot load-result)
                                          now-ms
                                          lifetime-ms)))
      (:snapshot load-result)

      :else
      (try
        (with-store-lock
          store
          (fn []
            (let [{:keys [status snapshot]} (load-keys-result store)
                  base-snapshot (case status
                                  :ok snapshot
                                  :missing current-snapshot
                                  :corrupt current-snapshot
                                  :storage-error current-snapshot)
                  winner (if (snapshot-needs-rotation? base-snapshot
                                                       now-ms
                                                       lifetime-ms)
                           (rotate-snapshot base-snapshot
                                            now-ms
                                            lifetime-ms
                                            max-keys)
                           (or base-snapshot
                               (rotate-snapshot nil
                                                now-ms
                                                lifetime-ms
                                                max-keys)))]
              (when (or (not= status :ok)
                        (not (same-snapshot? base-snapshot winner)))
                (store-snapshot! store winner))
              winner)))
        (catch Exception _
          current-snapshot)))))

(defn- rotation-check-jitter-ms
  []
  (if (zero? *ticket-rotation-jitter-seconds*)
    0
    (rand-int (* *ticket-rotation-jitter-seconds* 1000))))

(defn- run-rotation-loop
  [{:keys [store lifetime-ms max-keys snapshot-atom running?-atom] :as service}]
  (while @running?-atom
    (try
      (Thread/sleep (long (+ rotation-check-interval-ms
                             (rotation-check-jitter-ms))))
      (when @running?-atom
        (let [current-snapshot @snapshot-atom
              next-snapshot (refresh-snapshot! store
                                               current-snapshot
                                               lifetime-ms
                                               max-keys)]
          (when-not (same-snapshot? next-snapshot current-snapshot)
            (replace-current-snapshot! service next-snapshot))))
      (catch InterruptedException _
        nil)
      (catch Exception _
        nil))))

(defn create-key-service
  "Create a key service map with store, rotation, and native sync."
  ([store session-ticket-config]
   (create-key-service store session-ticket-config {}))
  ([store session-ticket-config {:keys [shared?]
                                 :or {shared? false}}]
   (let [lifetime-s (get session-ticket-config
                         :lifetime-seconds
                         default-ticket-lifetime-seconds)
         max-keys (get session-ticket-config
                       :max-keys
                       default-max-ticket-keys)
         lifetime-ms (* lifetime-s 1000)
         initial-snapshot (initialize-snapshot store lifetime-ms max-keys)]
     {:shared? shared?
      :store store
      :lifetime-ms lifetime-ms
      :max-keys max-keys
      :snapshot-atom (atom initial-snapshot)
      :native-mgrs-atom (atom [])
      :running?-atom (atom false)
      :rotation-thread (atom nil)})))

(defn- reseed-memory-snapshot
  [snapshot lifetime-ms]
  (when snapshot
    (let [now-ms (current-time-ms)
          last-rotation-ms (or (:last-rotation-ms snapshot) now-ms)]
      {:keys (vec (or (:keys snapshot) []))
       :last-rotation-ms last-rotation-ms
       :next-rotation-ms (+ last-rotation-ms
                            (rotation-interval-ms lifetime-ms))})))

(defn create-memory-ticket-service
  ([session-ticket-config]
   (create-memory-ticket-service session-ticket-config nil))
  ([session-ticket-config snapshot]
   (let [lifetime-s (get session-ticket-config
                         :lifetime-seconds
                         default-ticket-lifetime-seconds)
         lifetime-ms (* lifetime-s 1000)]
     (create-key-service (memory-ticket-store
                          (reseed-memory-snapshot snapshot lifetime-ms))
                         session-ticket-config
                         {:shared? true}))))

(defn recreate-memory-ticket-service
  "Create a new shared in-memory key service from `service`'s current snapshot,
  adopting `session-ticket-config` for future rotation."
  [service session-ticket-config]
  (create-memory-ticket-service session-ticket-config
                                @(:snapshot-atom service)))

(defn create-key-manager
  "Create a key manager wrapper that owns its own key service."
  [store native-mgr session-ticket-config]
  {:service (create-key-service store session-ticket-config)
   :native-mgr native-mgr
   :owns-service? true})

(defn attach-key-manager
  "Attach `native-mgr` to an already-running shared key service."
  [service native-mgr]
  {:service service
   :native-mgr native-mgr
   :owns-service? false})

(defn start-key-service!
  [{:keys [running?-atom rotation-thread] :as service}]
  (when (compare-and-set! running?-atom false true)
    (let [t (Thread. #(run-rotation-loop service) "busker-ticket-rotation")]
      (.setDaemon t true)
      (reset! rotation-thread t)
      (.start t)))
  service)

(defn stop-key-service!
  [{:keys [running?-atom rotation-thread]}]
  (reset! running?-atom false)
  (when-let [t @rotation-thread]
    (.interrupt ^Thread t)
    (.join ^Thread t 5000)))

(defn- attach-native-manager!
  [{:keys [native-mgrs-atom snapshot-atom]} native-mgr]
  (swap! native-mgrs-atom
         (fn [native-mgrs]
           (if (some #(identical? native-mgr %) native-mgrs)
             native-mgrs
             (conj native-mgrs native-mgr))))
  (sync-keys-to-native! native-mgr (:keys @snapshot-atom)))

(defn- detach-native-manager!
  [{:keys [native-mgrs-atom]} native-mgr]
  (swap! native-mgrs-atom
         (fn [native-mgrs]
           (->> native-mgrs
                (remove #(identical? native-mgr %))
                vec))))

(defn shared-service?
  [service]
  (true? (:shared? service)))

(defn- service-of
  [manager-or-service]
  (or (:service manager-or-service)
      manager-or-service))

(defn start-key-manager!
  "Start the backing key service and attach the native ticket manager."
  [{:keys [service native-mgr] :as mgr}]
  (start-key-service! service)
  (attach-native-manager! service native-mgr)
  mgr)

(defn stop-key-manager!
  "Detach the native ticket manager and stop any service it owns."
  [{:keys [service native-mgr owns-service?]}]
  (when (and service native-mgr)
    (detach-native-manager! service native-mgr))
  (when owns-service?
    (stop-key-service! service)))

(defn current-keys
  "Get the current keys from the key service."
  [manager-or-service]
  (-> manager-or-service
      service-of
      :snapshot-atom
      deref
      :keys))
