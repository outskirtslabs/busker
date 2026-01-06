(ns ol.busker.tickets
  "Session ticket key management for TLS 1.3 resumption and 0-RTT.

  Provides key storage backends and rotation logic for session tickets.
  Keys are used by the native layer for ticket encryption/decryption.

  Configuration layers:
  1. Zero config (default) - in-memory keys, lost on restart
  2. File persistence - EDN file storage, survives restart
  3. Custom store - implement [[TicketKeyStore]] protocol"
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [coffi.mem :as mem]
   [ol.busker.native :as h2o])
  (:import
   [java.io File]
   [java.nio.file Files]
   [java.nio.file.attribute PosixFilePermission]
   [java.security SecureRandom]
   [java.util HashSet]))

(set! *warn-on-reflection* true)

(def ^:const default-ticket-lifetime-seconds 86400)  ; 24 hours
(def ^:const rotation-check-interval-ms 120000)      ; 120 seconds
(def ^:const max-rotation-jitter-ms 6000)            ; 6 seconds jitter
(def ^:const file-poll-interval-ms 10000)            ; 10 seconds

(def ^:dynamic *ticket-rotation-jitter-seconds*
  "Jitter seconds for rotation checks. Bind to 0 in tests to disable."
  6)

(defprotocol TicketKeyStore
  "Backend for session ticket key persistence.

  Busker runs rotation checks every 120 seconds with jitter.
  A new key is created when the newest key is older than ticket-lifetime/4.
  This protocol handles storage only.

  Keys are maps with:

  | key           | type   | description                           |
  |---------------|--------|---------------------------------------|
  | `:name`       | bytes  | 16-byte unique key identifier         |
  | `:aes-key`    | bytes  | 32-byte AES-256 encryption key        |
  | `:hmac-key`   | bytes  | 64-byte HMAC-SHA256 key               |
  | `:not-before` | long   | activation time (epoch milliseconds)  |
  | `:not-after`  | long   | expiration time (epoch milliseconds)  |

  Thread Safety: Implementations must be thread-safe. store-keys! may be
  called while load-keys is in progress on another thread."

  (load-keys [store]
    "Load all keys from the backing store.

    Returns: seq of key maps (newest first), or nil if no keys stored.

    Called at server startup and after rotation to verify persistence.

    On error: Throw exception. Server logs error and uses in-memory
    fallback (losing persistence until next successful store).")

  (store-keys! [store keys]
    "Atomically persist the complete set of keys.

    keys: seq of key maps, ordered newest-first

    Atomicity: Readers must see either old or new state, never partial.
    File stores: use write-to-temp + rename pattern.

    Called after each key rotation cycle.

    On error: Throw exception. Server logs and retries next cycle.
    In-memory keys remain valid even if persistence fails."))

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
  "Convert a key map to EDN-safe format (hex-encoded bytes)."
  [{:keys [name aes-key hmac-key not-before not-after]}]
  {:name (bytes->hex name)
   :aes-key (bytes->hex aes-key)
   :hmac-key (bytes->hex hmac-key)
   :not-before not-before
   :not-after not-after})

(defn edn->key
  "Convert EDN format back to key map with byte arrays."
  [{:keys [name aes-key hmac-key not-before not-after]}]
  {:name (hex->bytes name)
   :aes-key (hex->bytes aes-key)
   :hmac-key (hex->bytes hmac-key)
   :not-before not-before
   :not-after not-after})

;; In-memory store (Layer 1)

(deftype MemoryTicketStore [keys-atom]
  TicketKeyStore
  (load-keys [_]
    @keys-atom)
  (store-keys! [_ keys]
    (reset! keys-atom (vec keys))
    nil))

(defn memory-ticket-store
  "Create an in-memory ticket key store.
   Keys are lost on restart."
  []
  (->MemoryTicketStore (atom nil)))

;; File-backed store (Layer 2)

(defn- set-file-permissions-0600
  "Set file permissions to owner read/write only (0600)."
  [^File file]
  (let [perms (HashSet.)]
    (.add perms PosixFilePermission/OWNER_READ)
    (.add perms PosixFilePermission/OWNER_WRITE)
    (Files/setPosixFilePermissions (.toPath file) perms)))

(deftype FileTicketStore [^String path cache-atom last-mtime-atom]
  TicketKeyStore
  (load-keys [_]
    (let [file (io/file path)]
      (when (.exists file)
        (let [mtime (.lastModified file)]
          (if (= mtime @last-mtime-atom)
            @cache-atom
            (let [content (slurp file)
                  keys (mapv edn->key (edn/read-string content))]
              (reset! cache-atom keys)
              (reset! last-mtime-atom mtime)
              keys))))))

  (store-keys! [_ keys]
    (let [file (io/file path)
          parent (.getParentFile file)]
      (when (and parent (not (.exists parent)))
        (throw (ex-info "Parent directory does not exist" {:path path})))
      (let [temp-file (File/createTempFile "tickets" ".edn.tmp" parent)
            edn-keys (mapv key->edn keys)
            content (pr-str edn-keys)]
        (try
          (spit temp-file content)
          (set-file-permissions-0600 temp-file)
          (Files/move (.toPath temp-file) (.toPath file)
                      (into-array java.nio.file.CopyOption
                                  [java.nio.file.StandardCopyOption/REPLACE_EXISTING
                                   java.nio.file.StandardCopyOption/ATOMIC_MOVE]))
          (reset! cache-atom (vec keys))
          (reset! last-mtime-atom (.lastModified file))
          nil
          (catch Exception e
            (when (.exists temp-file)
              (.delete temp-file))
            (throw e)))))))

(defn file-ticket-store
  "Create a file-backed ticket key store.

  path: File path for EDN key storage

  Atomic writes via temp file + rename.
  File permissions set to 0600 (owner read/write only).
  Parent directory must exist.
  Polls the file every 10 seconds for external changes.

  Thread safe for concurrent load-keys/store-keys! calls."
  [path]
  (->FileTicketStore path (atom nil) (atom 0)))

;; Key rotation logic

(defn- prune-expired-keys
  "Remove keys where not-after < now."
  [keys now-ms]
  (filterv #(>= (:not-after %) now-ms) keys))

(defn- unique-1byte-ids
  "Ensure keys have unique 1-byte identifiers (first byte of name).
   Returns keys with unique IDs, preferring newer keys."
  [keys]
  (let [seen (volatile! #{})]
    (filterv (fn [k]
               (let [id (aget ^bytes (:name k) 0)]
                 (if (contains? @seen id)
                   false
                   (do (vswap! seen conj id)
                       true))))
             keys)))

(defn- needs-new-key?
  "Check if a new key should be generated.
   Returns true if no valid keys or newest key is past 1/4 of its lifetime."
  [keys now-ms lifetime-ms]
  (if (empty? keys)
    true
    (let [newest (first keys)
          rotation-threshold (/ lifetime-ms 4)
          age (- now-ms (:not-before newest))]
      (>= age rotation-threshold))))

(defn rotate-keys
  "Perform key rotation if needed.

  Returns updated keys seq (newest first).

  Algorithm:
  1. Prune expired keys
  2. Remove entries with colliding 1-byte identifiers
  3. Generate new key if needed (no valid key or newest past 1/4 lifetime)
  4. Abort if would need 256+ unique identifiers"
  [keys now-ms lifetime-ms]
  (let [valid-keys (-> keys
                       (prune-expired-keys now-ms)
                       unique-1byte-ids)]
    (when (>= (count valid-keys) 256)
      (throw (ex-info "Too many ticket keys - would require 256+ unique identifiers"
                      {:key-count (count valid-keys)})))
    (if (needs-new-key? valid-keys now-ms lifetime-ms)
      (let [not-before (if (empty? valid-keys)
                         now-ms
                         (+ now-ms 60000))  ; 60s grace period for clock skew
            new-key (generate-key not-before lifetime-ms)]
        (into [new-key] valid-keys))
      valid-keys)))

;; Native key synchronization

(defn keys->native-array
  "Convert seq of Clojure key maps to native clj_session_ticket_t array."
  [keys arena]
  (let [struct-size h2o/size-of-session-ticket-t
        n (count keys)
        segment (mem/alloc (* n struct-size) arena)]
    (doseq [[i k] (map-indexed vector keys)]
      (let [offset (* i struct-size)
            name-bytes ^bytes (:name k)
            aes-bytes ^bytes (:aes-key k)
            hmac-bytes ^bytes (:hmac-key k)]
        ;; Copy byte arrays into native memory
        (doseq [j (range 16)]
          (mem/write-byte segment (+ offset j) (aget name-bytes j)))
        (doseq [j (range 32)]
          (mem/write-byte segment (+ offset 16 j) (aget aes-bytes j)))
        (doseq [j (range 64)]
          (mem/write-byte segment (+ offset 48 j) (aget hmac-bytes j)))
        ;; Write timestamps
        (mem/write-long segment (+ offset 112) (:not-before k))
        (mem/write-long segment (+ offset 120) (:not-after k))))
    segment))

(defn sync-keys-to-native!
  "Push current keys to native ticket manager."
  [native-mgr keys]
  (if (empty? keys)
    (h2o/ticket-manager-set-keys native-mgr mem/null 0)
    (with-open [arena (mem/confined-arena)]
      (let [arr (keys->native-array keys arena)]
        (h2o/ticket-manager-set-keys native-mgr arr (count keys))))))

;; Key manager (combines store + rotation + native sync)

(defrecord KeyManager [store native-mgr lifetime-ms keys-atom running?-atom rotation-thread])

(defn- rotation-check-jitter-ms
  "Return random jitter in milliseconds for rotation checks."
  []
  (if (zero? *ticket-rotation-jitter-seconds*)
    0
    (* (rand-int (* *ticket-rotation-jitter-seconds* 1000)) 1)))

(defn- run-rotation-loop
  "Background rotation loop. Checks every 120s + jitter."
  [{:keys [store native-mgr lifetime-ms keys-atom running?-atom] :as _mgr}]
  (while @running?-atom
    (try
      (Thread/sleep (long (+ rotation-check-interval-ms (rotation-check-jitter-ms))))
      (when @running?-atom
        (let [now-ms (System/currentTimeMillis)
              current-keys @keys-atom
              rotated-keys (rotate-keys current-keys now-ms lifetime-ms)]
          (when (not= (count rotated-keys) (count current-keys))
            (reset! keys-atom rotated-keys)
            (sync-keys-to-native! native-mgr rotated-keys)
            (try
              (store-keys! store rotated-keys)
              (catch Exception _e
                nil)))))  ; Log and continue on store failure
      (catch InterruptedException _
        nil)  ; Expected during shutdown
      (catch Exception _e
        nil))))  ; Log and continue

(defn create-key-manager
  "Create a key manager with store, rotation, and native sync.

  Options:
  - :session-ticket-lifetime-seconds - key lifetime (default 24h)

  Returns a KeyManager record. Call start-key-manager! to begin rotation."
  [store native-mgr opts]
  (let [lifetime-s (get opts :session-ticket-lifetime-seconds default-ticket-lifetime-seconds)
        lifetime-ms (* lifetime-s 1000)
        initial-keys (or (try (load-keys store) (catch Exception _ nil))
                         (let [now (System/currentTimeMillis)]
                           [(generate-key now lifetime-ms)]))]
    (->KeyManager store native-mgr lifetime-ms (atom initial-keys) (atom false) (atom nil))))

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
