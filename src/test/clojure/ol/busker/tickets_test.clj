(ns ol.busker.tickets-test
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [ol.busker.tickets :as tickets]
   [ol.clave.storage :as storage]
   [ol.clave.storage.file :as file-storage])
  (:import
   java.nio.file.Files))

(defn- temp-storage-root
  []
  (str (Files/createTempDirectory "busker-tickets-test"
                                  (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- key-set->edn
  [keys]
  (mapv tickets/key->edn keys))

(defn- key-with-name
  [name-byte-values not-before not-after]
  {:name (byte-array (map byte name-byte-values))
   :aes-key (byte-array 32)
   :hmac-key (byte-array 64)
   :not-before not-before
   :not-after not-after})

(deftest rotate-keys-preserves-distinct-full-ticket-names-test
  (testing "rotation keeps keys whose 16-byte names differ even if the first byte matches"
    (let [now-ms 1000000
          lifetime-ms 86400000
          key-a (key-with-name
                 (concat [42] (repeat 15 1))
                 (- now-ms 10000)
                 (+ now-ms 10000))
          key-b (key-with-name
                 (concat [42] (repeat 15 2))
                 (- now-ms 20000)
                 (+ now-ms 20000))
          rotated (tickets/rotate-keys [key-a key-b] now-ms lifetime-ms 4)]
      (is (= (key-set->edn [key-a key-b])
             (key-set->edn rotated))))))

(deftest rotate-keys-trims-retained-set-to-max-keys-test
  (testing "rotation keeps only the newest max-keys valid entries"
    (let [now-ms 1000000
          lifetime-ms 86400000
          key-1 (key-with-name (concat [1] (repeat 15 1)) 999700 1100000)
          key-2 (key-with-name (concat [2] (repeat 15 2)) 999600 1100000)
          key-3 (key-with-name (concat [3] (repeat 15 3)) 999500 1100000)
          rotated (tickets/rotate-keys [key-1 key-2 key-3] now-ms lifetime-ms 2)]
      (is (= (key-set->edn [key-1 key-2])
             (key-set->edn rotated))))))

(deftest rotate-keys-max-keys-one-activates-replacement-immediately-test
  (testing "rotation with max-keys 1 keeps an immediately valid encryption key"
    (let [now-ms 1000000
          lifetime-ms 400000
          current-key (key-with-name (concat [9] (repeat 15 9))
                                     800000
                                     1200000)
          rotated (tickets/rotate-keys [current-key] now-ms lifetime-ms 1)]
      (is (= 1 (count rotated)))
      (is (= now-ms (:not-before (first rotated))))
      (is (< now-ms (:not-after (first rotated))))
      (is (not= (key-set->edn [current-key])
                (key-set->edn rotated))))))

(deftest storage-ticket-store-startup-persists-immediately-test
  (testing "storage-backed startup persists keys under the fixed storage key and lock"
    (let [root (temp-storage-root)
          impl (file-storage/file-storage {:root root})
          original-with-lock storage/with-lock
          lock-name_ (atom nil)
          store (tickets/storage-ticket-store impl)
          manager (with-redefs [tickets/sync-keys-to-native! (fn [_ _] nil)
                                storage/with-lock (fn [storage-impl lease lock-name f]
                                                    (reset! lock-name_ lock-name)
                                                    (original-with-lock storage-impl lease lock-name f))]
                    (-> (tickets/create-key-manager store
                                                    ::native
                                                    {:lifetime-seconds 86400})
                        (tickets/start-key-manager!)))]
      (try
        (is (= "busker-session-ticket-rotation" @lock-name_))
        (is (.exists (io/file root "busker/session_tickets/keys.edn")))
        (let [snapshot (-> impl
                           (storage/load-string nil "busker/session_tickets/keys.edn")
                           edn/read-string)]
          (is (map? snapshot))
          (is (= (key-set->edn (tickets/current-keys manager))
                 (:keys snapshot))))
        (finally
          (tickets/stop-key-manager! manager))))))

(deftest storage-ticket-store-startup-fails-when-storage-is-unavailable-test
  (testing "storage-backed startup treats storage lock failures as fatal"
    (let [root (temp-storage-root)
          impl (file-storage/file-storage {:root root})
          store (tickets/storage-ticket-store impl)]
      (with-redefs [storage/with-lock (fn [& _]
                                        (throw (ex-info "lock failed" {})))]
        (is (thrown? clojure.lang.ExceptionInfo
                     (tickets/create-key-manager store
                                                 ::native
                                                 {:lifetime-seconds 86400})))))))

(deftest storage-ticket-store-startup-fails-on-corrupt-persisted-data-test
  (testing "storage-backed startup fails when persisted ticket data is corrupt"
    (let [root (temp-storage-root)
          impl (file-storage/file-storage {:root root})
          _ (storage/store-string! impl
                                   nil
                                   "busker/session_tickets/keys.edn"
                                   "{this-is-not-edn")
          store (tickets/storage-ticket-store impl)]
      (with-redefs [tickets/sync-keys-to-native! (fn [_ _] nil)]
        (is (thrown? clojure.lang.ExceptionInfo
                     (tickets/create-key-manager store
                                                 ::native
                                                 {:lifetime-seconds 86400})))))))

(deftest storage-ticket-store-startup-fails-on-storage-read-error-test
  (testing "storage-backed startup fails when persisted ticket data cannot be read"
    (let [root (temp-storage-root)
          impl (file-storage/file-storage {:root root})
          store (tickets/storage-ticket-store impl)]
      (with-redefs [tickets/sync-keys-to-native! (fn [_ _] nil)
                    storage/load-string (fn [& _]
                                          (throw (ex-info "read failed" {})))]
        (is (thrown? clojure.lang.ExceptionInfo
                     (tickets/create-key-manager store
                                                 ::native
                                                 {:lifetime-seconds 86400})))))))

(deftest storage-ticket-store-startup-reuses-existing-keys-test
  (testing "storage-backed startup reloads the stored snapshot instead of generating a new one"
    (let [root (temp-storage-root)
          impl (file-storage/file-storage {:root root})
          now-ms (System/currentTimeMillis)
          existing-keys [(tickets/generate-key (System/currentTimeMillis)
                                               (* 86400 1000))]
          _ (storage/store-string! impl
                                   nil
                                   "busker/session_tickets/keys.edn"
                                   (pr-str {:keys (key-set->edn existing-keys)
                                            :last-rotation-ms now-ms
                                            :next-rotation-ms (+ now-ms 3600000)}))
          store (tickets/storage-ticket-store impl)
          manager (with-redefs [tickets/sync-keys-to-native! (fn [_ _] nil)]
                    (-> (tickets/create-key-manager store
                                                    ::native
                                                    {:lifetime-seconds 60})
                        (tickets/start-key-manager!)))]
      (try
        (is (= (key-set->edn existing-keys)
               (key-set->edn (tickets/current-keys manager))))
        (finally
          (tickets/stop-key-manager! manager))))))

(deftest storage-ticket-store-persists-rotation-schedule-metadata-test
  (testing "storage-backed snapshots persist explicit rotation schedule metadata"
    (let [root (temp-storage-root)
          impl (file-storage/file-storage {:root root})
          store (tickets/storage-ticket-store impl)
          manager (with-redefs [tickets/sync-keys-to-native! (fn [_ _] nil)]
                    (-> (tickets/create-key-manager store
                                                    ::native
                                                    {:lifetime-seconds 86400})
                        (tickets/start-key-manager!)))]
      (try
        (let [snapshot (-> impl
                           (storage/load-string nil "busker/session_tickets/keys.edn")
                           edn/read-string)]
          (is (map? snapshot))
          (is (= (key-set->edn (tickets/current-keys manager))
                 (:keys snapshot)))
          (is (integer? (:last-rotation-ms snapshot)))
          (is (integer? (:next-rotation-ms snapshot)))
          (when (and (integer? (:last-rotation-ms snapshot))
                     (integer? (:next-rotation-ms snapshot)))
            (is (< (:last-rotation-ms snapshot)
                   (:next-rotation-ms snapshot)))))
        (finally
          (tickets/stop-key-manager! manager))))))

(deftest storage-ticket-store-startup-honors-stored-next-rotation-ms-test
  (testing "storage-backed startup trusts persisted next-rotation timing instead of deriving it from key age"
    (let [root (temp-storage-root)
          impl (file-storage/file-storage {:root root})
          now-ms (System/currentTimeMillis)
          old-key (tickets/generate-key (- now-ms (* 12 60 60 1000))
                                        (* 24 60 60 1000))
          _ (storage/store-string! impl
                                   nil
                                   "busker/session_tickets/keys.edn"
                                   (pr-str {:keys (key-set->edn [old-key])
                                            :last-rotation-ms (- now-ms 1000)
                                            :next-rotation-ms (+ now-ms 3600000)}))
          store (tickets/storage-ticket-store impl)
          manager (with-redefs [tickets/sync-keys-to-native! (fn [_ _] nil)]
                    (-> (tickets/create-key-manager store
                                                    ::native
                                                    {:lifetime-seconds 86400})
                        (tickets/start-key-manager!)))]
      (try
        (is (= (key-set->edn [old-key])
               (key-set->edn (tickets/current-keys manager))))
        (finally
          (tickets/stop-key-manager! manager))))))

(deftest storage-ticket-store-reuses-persisted-keys-across-managers-test
  (testing "two managers using the same storage root converge on one stored key set"
    (let [root (temp-storage-root)
          impl (file-storage/file-storage {:root root})
          store (tickets/storage-ticket-store impl)
          manager-a (with-redefs [tickets/sync-keys-to-native! (fn [_ _] nil)]
                      (-> (tickets/create-key-manager store
                                                      ::native-a
                                                      {:lifetime-seconds 86400})
                          (tickets/start-key-manager!)))
          keys-a (key-set->edn (tickets/current-keys manager-a))]
      (tickets/stop-key-manager! manager-a)
      (let [manager-b (with-redefs [tickets/sync-keys-to-native! (fn [_ _] nil)]
                        (-> (tickets/create-key-manager store
                                                        ::native-b
                                                        {:lifetime-seconds 86400})
                            (tickets/start-key-manager!)))]
        (try
          (is (= keys-a
                 (key-set->edn (tickets/current-keys manager-b))))
          (finally
            (tickets/stop-key-manager! manager-b)))))))

(deftest memory-ticket-store-does-not-persist-across-managers-test
  (testing "memory-only stores lose keys across restart"
    (let [manager-a (with-redefs [tickets/sync-keys-to-native! (fn [_ _] nil)]
                      (-> (tickets/create-key-manager (tickets/memory-ticket-store)
                                                      ::native-a
                                                      {:lifetime-seconds 86400})
                          (tickets/start-key-manager!)))
          keys-a (key-set->edn (tickets/current-keys manager-a))]
      (tickets/stop-key-manager! manager-a)
      (let [manager-b (with-redefs [tickets/sync-keys-to-native! (fn [_ _] nil)]
                        (-> (tickets/create-key-manager (tickets/memory-ticket-store)
                                                        ::native-b
                                                        {:lifetime-seconds 86400})
                            (tickets/start-key-manager!)))]
        (try
          (is (not= keys-a
                    (key-set->edn (tickets/current-keys manager-b))))
          (finally
            (tickets/stop-key-manager! manager-b)))))))
