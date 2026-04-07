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

  (load-keys [store]
    "Load all keys from the backing store.

    Returns a seq of key maps ordered newest-first, or nil if no keys are
    stored.")

  (store-keys! [store keys]
    "Atomically persist the complete set of keys.")

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

(defn- same-key-set?
  [a b]
  (= (key-set->edn a)
     (key-set->edn b)))

(defn- parse-stored-keys
  [content]
  (->> content
       edn/read-string
       (mapv edn->key)))

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

(defn- load-storage-keys
  [storage-impl]
  (try
    (let [content (storage/load-string storage-impl
                                       nil
                                       ticket-storage-key)]
      (try
        (parse-stored-keys content)
        (catch Exception e
          (throw (corrupt-storage-error e)))))
    (catch NoSuchFileException _
      nil)
    (catch clojure.lang.ExceptionInfo e
      (throw e))
    (catch Exception e
      (throw (storage-load-error e)))))

(defn- store-storage-keys!
  [storage-impl keys]
  (storage/store-string! storage-impl
                         nil
                         ticket-storage-key
                         (pr-str (key-set->edn keys))))

(deftype MemoryTicketStore [keys-atom]
  TicketKeyStore
  (load-keys [_]
    @keys-atom)
  (store-keys! [_ keys]
    (reset! keys-atom (vec keys))
    nil)
  (with-store-lock [_ f]
    (f)))

(defn memory-ticket-store
  "Create an in-memory ticket key store."
  []
  (->MemoryTicketStore (atom nil)))

(deftype StorageTicketStore [storage-impl]
  TicketKeyStore
  (load-keys [_]
    (load-storage-keys storage-impl))
  (store-keys! [_ keys]
    (store-storage-keys! storage-impl keys))
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
    (let [keys (some-> (load-keys store) vec)]
      (if keys
        {:status :ok
         :keys keys}
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

(defn- needs-new-key?
  [keys now-ms lifetime-ms]
  (if (empty? keys)
    true
    (let [newest (first keys)
          rotation-threshold (/ lifetime-ms 4)
          age (- now-ms (:not-before newest))]
      (>= age rotation-threshold))))

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

(defn- rotation-needed?
  [keys now-ms lifetime-ms max-keys]
  (not (same-key-set? keys
                      (rotate-keys keys now-ms lifetime-ms max-keys))))

(defn sync-keys-to-native!
  "Push current keys to the native ticket manager."
  [native-mgr keys]
  (if (empty? keys)
    (h2o/ticket-manager-set-keys native-mgr mem/null 0)
    (with-open [arena (mem/confined-arena)]
      (let [arr (h2o/session-ticket-keys->native-array keys arena)]
        (h2o/ticket-manager-set-keys native-mgr arr (count keys))))))

(defn- replace-current-keys!
  [{:keys [native-mgr keys-atom]} keys]
  (reset! keys-atom (vec keys))
  (sync-keys-to-native! native-mgr keys))

(defn- initialize-keys
  [store lifetime-ms max-keys]
  (let [now-ms (System/currentTimeMillis)]
    (with-store-lock
      store
      (fn []
        (let [{:keys [status keys]} (load-keys-result store)
              base-keys (if (= status :ok) keys nil)
              winner (rotate-keys base-keys now-ms lifetime-ms max-keys)]
          (store-keys! store winner)
          winner)))))

(defn- refresh-keys!
  [store current-keys lifetime-ms max-keys]
  (let [now-ms (System/currentTimeMillis)
        load-result (load-keys-result store)]
    (cond
      (= :storage-error (:status load-result))
      current-keys

      (and (= :ok (:status load-result))
           (not (rotation-needed? (:keys load-result)
                                  now-ms
                                  lifetime-ms
                                  max-keys)))
      (:keys load-result)

      :else
      (try
        (with-store-lock
          store
          (fn []
            (let [{:keys [status keys]} (load-keys-result store)
                  base-keys (case status
                              :ok keys
                              :missing nil
                              :corrupt nil
                              :storage-error current-keys)
                  next-keys (rotate-keys base-keys now-ms lifetime-ms max-keys)]
              (when (or (not= status :ok)
                        (not (same-key-set? base-keys next-keys)))
                (store-keys! store next-keys))
              next-keys)))
        (catch Exception _
          current-keys)))))

(defn- rotation-check-jitter-ms
  []
  (if (zero? *ticket-rotation-jitter-seconds*)
    0
    (rand-int (* *ticket-rotation-jitter-seconds* 1000))))

(defn- run-rotation-loop
  [{:keys [store lifetime-ms max-keys keys-atom running?-atom] :as mgr}]
  (while @running?-atom
    (try
      (Thread/sleep (long (+ rotation-check-interval-ms
                             (rotation-check-jitter-ms))))
      (when @running?-atom
        (let [current-keys @keys-atom
              next-keys (refresh-keys! store
                                       current-keys
                                       lifetime-ms
                                       max-keys)]
          (when-not (same-key-set? next-keys current-keys)
            (replace-current-keys! mgr next-keys))))
      (catch InterruptedException _
        nil)
      (catch Exception _
        nil))))

(defn create-key-manager
  "Create a key manager map with store, rotation, and native sync."
  [store native-mgr session-ticket-config]
  (let [lifetime-s (get session-ticket-config
                        :lifetime-seconds
                        default-ticket-lifetime-seconds)
        max-keys (get session-ticket-config
                      :max-keys
                      default-max-ticket-keys)
        lifetime-ms (* lifetime-s 1000)
        initial-keys (initialize-keys store lifetime-ms max-keys)]
    {:store store
     :native-mgr native-mgr
     :lifetime-ms lifetime-ms
     :max-keys max-keys
     :keys-atom (atom initial-keys)
     :running?-atom (atom false)
     :rotation-thread (atom nil)}))

(defn start-key-manager!
  "Start the background rotation thread for a key manager.
   Returns the key manager."
  [{:keys [native-mgr keys-atom running?-atom rotation-thread] :as mgr}]
  (reset! running?-atom true)
  (sync-keys-to-native! native-mgr @keys-atom)
  (let [t (Thread. #(run-rotation-loop mgr) "busker-ticket-rotation")]
    (.setDaemon t true)
    (reset! rotation-thread t)
    (.start t))
  mgr)

(defn stop-key-manager!
  "Stop the background rotation thread."
  [{:keys [running?-atom rotation-thread]}]
  (reset! running?-atom false)
  (when-let [t @rotation-thread]
    (.interrupt ^Thread t)
    (.join ^Thread t 5000)))

(defn current-keys
  "Get the current keys from the key manager."
  [{:keys [keys-atom]}]
  @keys-atom)
