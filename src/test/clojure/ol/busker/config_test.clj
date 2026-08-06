(ns ol.busker.config-test
  (:require
   [clojure.test :refer [are deftest is testing]]
   [ol.busker.config :as cfg]
   [ol.busker.specs :as specs]
   [ol.busker.test-utils :as util]
   [ol.clave.storage :as storage]))

(defn- load-errors
  [config]
  (try
    (cfg/load! config)
    nil
    (catch clojure.lang.ExceptionInfo e
      (:errors (ex-data e)))))

(deftest bind-address-valid-test
  (testing "accepts supported bind address forms"
    (doseq [addr [":8080"
                  "0.0.0.0:443"
                  "example.com:80"
                  "[::]:443"
                  "[2001:db8::1]:8443"
                  "unix:/tmp/busker.sock"
                  "unix:@busker"]]
      (is (cfg/bind-address? addr) (str "expected valid: " addr)))))

(deftest bind-address-invalid-test
  (testing "rejects unsupported bind address forms"
    (doseq [addr [""
                  "localhost"
                  "localhost:"
                  ":0"
                  ":65536"
                  "[::]"
                  "[::]:"
                  "unix:"
                  "unix:/"
                  "unix:@"]]
      (is (not (cfg/bind-address? addr)) (str "expected invalid: " addr)))))

(deftest parse-bind-test
  (testing "parses tcp binds"
    (is (= [{:address nil :port 8080}]
           (cfg/parse-bind ":8080")))
    (is (= [{:address "0.0.0.0" :port 443}]
           (cfg/parse-bind "0.0.0.0:443")))
    (is (= [{:address "::" :port 443}]
           (cfg/parse-bind "[::]:443")))
    (is (= [{:address "0.0.0.0" :port 443}
            {:address "::" :port 443}]
           (cfg/parse-bind ["0.0.0.0:443" "[::]:443"]))))
  (testing "parses unix binds"
    (is (= [{:unix "/tmp/busker.sock"}]
           (cfg/parse-bind "unix:/tmp/busker.sock")))
    (is (= [{:unix "@busker"}]
           (cfg/parse-bind "unix:@busker"))))
  (testing "rejects invalid bind values"
    (is (thrown? clojure.lang.ExceptionInfo
                 (cfg/parse-bind 123)))
    (is (thrown? clojure.lang.ExceptionInfo
                 (cfg/parse-bind [])))
    (is (thrown? clojure.lang.ExceptionInfo
                 (cfg/parse-bind ["bad"])))))

(deftest load-new-config-shape-test
  (testing "loads top-level tls, map entrypoints, and dispatch defaults"
    (let [config (cfg/load!
                  {:tls {:storage {:factory 'ol.clave.storage.file/file-storage
                                   :root "target/config-test-storage"}
                         :certificates {:load [{:type :pem
                                                :cert-file (util/fixture-cert-path)
                                                :key-file (util/fixture-key-path)}]
                                        :manage ["example.com"
                                                 "www.example.com"]}}
                   :entrypoints {:http {:bind ":8080"
                                        :tls false}
                                 :https {:bind ":8443"
                                         :tls {:tls-compatibility-mode
                                               :modern}}}
                   :dispatch [{:entrypoints #{:https}}]})]
      (is (satisfies? storage/Storage (get-in config [:tls :storage])))
      (is (= ["example.com" "www.example.com"]
             (get-in config [:tls :certificates :manage])))
      (is (= [{:directory-url
               "https://acme-v02.api.letsencrypt.org/directory"}]
             (get-in config [:tls :issuers])))
      (is (= true (get-in config [:entrypoints :http :http1?])))
      (is (= true (get-in config [:entrypoints :https :http2?])))
      (is (= true (get-in config [:entrypoints :https :http3?])))
      (is (= false (get-in config [:entrypoints :http :http3?])))))

  (testing "exposes config defaults used by the loader"
    (is (= 1 (:n-workers specs/default-config)))
    (is (= 1024 (:max-connections specs/default-config)))
    (is (= 4
           (get-in specs/default-config
                   [:tls :session-tickets :max-keys])))
    (is (= 86400
           (get-in specs/default-config
                   [:tls :session-tickets :lifetime-seconds])))
    (is (= [{:directory-url
             "https://acme-v02.api.letsencrypt.org/directory"}]
           (get-in specs/default-config [:tls :issuers])))
    (is (= true
           (:http3? (:default specs/entrypoint))))))

(deftest entrypoint-http3-defaults-test
  (testing "defaults HTTP/3 from effective entrypoint TLS"
    (let [snapshot (cfg/normalized-snapshot
                    {:entrypoints {:clear {:bind ":8080"
                                           :tls false}
                                   :secure {:bind ":8443"}
                                   :secure-http3-disabled {:bind ":9443"
                                                           :http3? false}}
                     :dispatch [{}]})]
      (is (= {:clear {:tls false
                      :http3? false}
              :secure {:tls {:tls-compatibility-mode :modern}
                       :http3? true}
              :secure-http3-disabled {:tls {:tls-compatibility-mode :modern}
                                      :http3? false}}
             (into {}
                   (map (fn [[entrypoint-id entrypoint]]
                          [entrypoint-id
                           (select-keys entrypoint [:tls :http3?])]))
                   (:entrypoints snapshot)))))))

(deftest session-ticket-config-contract-test
  (testing "accepts the minimal active session ticket config"
    (let [config (cfg/load!
                  {:tls {:session-tickets {:disabled? true
                                           :persistence :memory
                                           :max-keys 7
                                           :lifetime-seconds 42}}
                   :entrypoints {:http {:bind ":8080"
                                        :tls false}}
                   :dispatch [{}]})]
      (is (= true (get-in config [:tls :session-tickets :disabled?])))
      (is (= :memory (get-in config [:tls :session-tickets :persistence])))
      (is (= 7 (get-in config [:tls :session-tickets :max-keys])))
      (is (= 42 (get-in config [:tls :session-tickets :lifetime-seconds])))))

  (testing "rejects storage persistence without top-level tls storage"
    (let [errors (load-errors
                  {:tls {:session-tickets {:persistence :storage}}
                   :entrypoints {:https {:bind ":8443"
                                         :tls {:tls-compatibility-mode :modern}}}
                   :dispatch [{}]})]
      (is (some #(= ::cfg/session-ticket-storage-missing (:error %)) errors))))

  (testing "rejects unknown session ticket knobs"
    (let [errors (load-errors
                  {:tls {:session-tickets {:bogus true}}
                   :entrypoints {:http {:bind ":8080"
                                        :tls false}}
                   :dispatch [{}]})]
      (is (some #(= ::cfg/session-ticket-config-invalid (:error %)) errors))))

  (testing "malformed session ticket values stay on the normal spec validation path"
    (let [errors (load-errors
                  {:tls {:session-tickets true}
                   :entrypoints {:http {:bind ":8080"
                                        :tls false}}
                   :dispatch [{}]})]
      (is (some #(= ::cfg/config-spec-invalid (:error %)) errors)))))

(deftest validate-entrypoint-selection-test
  (testing "rejects dispatch references to unknown entrypoints"
    (let [errors (load-errors
                  {:entrypoints {:http {:bind ":8080" :tls false}}
                   :dispatch [{:entrypoints #{:missing}}]})]
      (is (some #(= ::cfg/dispatch-entrypoint-missing (:error %)) errors))))

  (testing "rejects HTTP/3 on explicit cleartext entrypoints"
    (let [errors (load-errors
                  {:entrypoints {:http {:bind ":8080"
                                        :tls false
                                        :http3? true}}
                   :dispatch [{}]})]
      (is (some #(= ::cfg/entrypoint-http3-requires-tls (:error %)) errors))))

  (testing "rejects conflicting bind usage across entrypoints"
    (let [errors (load-errors
                  {:entrypoints {:http {:bind ":8080" :tls false}
                                 :https {:bind ":8080"
                                         :tls {:tls-compatibility-mode
                                               :modern}}}
                   :dispatch [{}]})]
      (is (some #(= ::cfg/entrypoint-bind-conflict (:error %)) errors)))))

(deftest validate-entrypoint-bind-conflicts-test
  (are [binds expected-conflict?]
       (= expected-conflict?
          (boolean
           (some #(= ::cfg/entrypoint-bind-conflict (:error %))
                 (load-errors
                  {:entrypoints {:http {:bind binds :tls false}}
                   :dispatch [{}]}))))
    ["0.0.0.0:8080" "[::]:8080"] false
    ["0.0.0.0:8080" "127.0.0.1:8080"] true
    ["[::]:8080" "[::1]:8080"] true
    ["127.0.0.1:8080" "127.0.0.1:8080"] true
    ["unix:/tmp/busker.sock" "unix:/tmp/busker.sock"] true
    ["unix:/tmp/busker.sock" ":8080"] false
    [":8080" "0.0.0.0:8080"] true
    [":8080" "[::]:8080"] true
    [":8080" "127.0.0.1:8080"] true
    [":8080" "[::1]:8080"] true
    [":8080" "example.invalid:8080"] true
    ["example.invalid:8080" "0.0.0.0:8080"] true
    ["example.invalid:8080" "[::]:8080"] true))

(deftest validate-top-level-tls-test
  (testing "rejects tls listeners when no global certificates are available"
    (let [errors (load-errors
                  {:entrypoints {:https {:bind ":8443"
                                         :tls {:tls-compatibility-mode
                                               :modern}}}
                   :dispatch [{}]})]
      (is (some #(= ::cfg/tls-missing-certificates (:error %)) errors))))

  (testing "rejects pem load entries with missing files"
    (let [errors (load-errors
                  {:tls {:certificates {:load [{:type :pem
                                                :cert-file "/nonexistent/cert.pem"
                                                :key-file "/nonexistent/key.pem"}]}}
                   :entrypoints {:https {:bind ":8443"
                                         :tls {:tls-compatibility-mode
                                               :modern}}}
                   :dispatch [{}]})]
      (is (some #(= ::cfg/tls-cert-file-not-found (:error %)) errors))
      (is (some #(= ::cfg/tls-key-file-not-found (:error %)) errors)))))

(deftest config->listeners-test
  (testing "expands unspecified TCP hosts into IPv4 and IPv6 wildcards"
    (let [listeners (-> {:entrypoints {:http {:bind [":8080" ":8081"]
                                              :tls false}
                                       :https {:bind ":8443"
                                               :tls {:tls-compatibility-mode
                                                     :modern}
                                               :http3? false}}}
                        cfg/apply-config-defaults
                        cfg/config->listeners
                        :listeners)]
      (is (= [{:entrypoint :http
               :host "0.0.0.0"
               :port 8080}
              {:entrypoint :http
               :host "::"
               :port 8080}
              {:entrypoint :http
               :host "0.0.0.0"
               :port 8081}
              {:entrypoint :http
               :host "::"
               :port 8081}
              {:entrypoint :https
               :host "0.0.0.0"
               :port 8443
               :tls {:tls-compatibility-mode :modern
                     :http3? false}}
              {:entrypoint :https
               :host "::"
               :port 8443
               :tls {:tls-compatibility-mode :modern
                     :http3? false}}]
             listeners)))))