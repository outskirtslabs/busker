(ns ol.busker.config-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.config :as cfg]
   [ol.busker.specs :as specs]))

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

(deftest validate-entrypoint-test
  (testing "rejects http3 on unix entrypoint"
    ;; Unix sockets are internal, so no TLS cert errors - just the http3-unix error
    (let [entrypoint {:name  :webs
                      :bind  "unix:/tmp/busker.sock"
                      :http1? true
                      :http2? true
                      :http3? true
                      :tls   {}}
          errors (cfg/validate-entrypoint [] entrypoint)]
      (is (= 1 (count errors)))
      (is (= ::cfg/entrypoint-http3-unix (:error (first errors))))))
  (testing "rejects http3 without tls on non-internal address"
    ;; ":443" binds to all interfaces, which is not internal
    (let [entrypoint {:name  :webs
                      :bind  ":443"
                      :http1? true
                      :http2? true
                      :http3? true}]
      (is (= [{:msg "HTTP/3 requires TLS configuration for non-internal addresses."
               :error ::cfg/entrypoint-http3-requires-tls
               :data {:entrypoint :webs}}]
             (cfg/validate-entrypoint [] entrypoint)))))
  (testing "allows http3 without explicit tls on internal address"
    ;; Internal addresses get auto-provisioned self-signed certs
    (let [entrypoint {:name  :webs
                      :bind  "127.0.0.1:443"
                      :http1? true
                      :http2? true
                      :http3? true}]
      (is (= [] (cfg/validate-entrypoint [] entrypoint)))))
  (testing "rejects entrypoint with no enabled protocols"
    (let [entrypoint {:name  :web
                      :bind  ":8080"
                      :http1? false
                      :http2? false
                      :http3? false}]
      (is (= [{:msg "Entrypoint must enable at least one HTTP protocol."
               :error ::cfg/entrypoint-no-protocols
               :data {:entrypoint :web
                      :http1? false
                      :http2? false
                      :http3? false}}]
             (cfg/validate-entrypoint [] entrypoint)))))
  (testing "accumulates errors on existing vector"
    ;; Unix socket is internal, so only http3-unix error (not http3-requires-tls)
    (let [entrypoint {:name  :webs
                      :bind  "unix:/tmp/busker.sock"
                      :http1? false
                      :http2? false
                      :http3? true}
          errors [{:msg "existing"
                   :error :test/existing
                   :data {}}]]
      (is (= [{:msg "existing"
               :error :test/existing
               :data {}}
              {:msg "HTTP/3 is not supported for unix domain sockets."
               :error ::cfg/entrypoint-http3-unix
               :data {:entrypoint :webs
                      :bind ["unix:/tmp/busker.sock"]}}]
             (cfg/validate-entrypoint errors entrypoint))))))

(deftest validate-entrypoint-bind-test
  (testing "rejects invalid bind type"
    (let [entrypoint {:name  :web
                      :bind  10
                      :http1? true
                      :http2? false
                      :http3? false}]
      (is (= [{:msg "Bind must be a string or vector of strings."
               :error ::cfg/entrypoint-bind-type
               :data {:entrypoint :web
                      :bind 10}}]
             (cfg/validate-entrypoint [] entrypoint)))))
  (testing "rejects empty bind list"
    (let [entrypoint {:name  :web
                      :bind  []
                      :http1? true
                      :http2? false
                      :http3? false}]
      (is (= [{:msg "Bind must include at least one address."
               :error ::cfg/entrypoint-bind-empty
               :data {:entrypoint :web
                      :bind []}}]
             (cfg/validate-entrypoint [] entrypoint)))))
  (testing "rejects invalid bind addresses"
    (let [entrypoint {:name  :web
                      :bind  ["bad" ":0"]
                      :http1? true
                      :http2? false
                      :http3? false}]
      (is (= [{:msg "Bind address must be a valid address string."
               :error ::cfg/entrypoint-bind-address-invalid
               :data {:entrypoint :web
                      :bind "bad"
                      :index 0}}
              {:msg "Bind address must be a valid address string."
               :error ::cfg/entrypoint-bind-address-invalid
               :data {:entrypoint :web
                      :bind ":0"
                      :index 1}}]
             (cfg/validate-entrypoint [] entrypoint)))))
  (testing "rejects non-string or blank bind entries"
    (let [entrypoint {:name  :web
                      :bind  [":8080" 10 ""]
                      :http1? true
                      :http2? false
                      :http3? false}]
      (is (= [{:msg "Bind address must be a string."
               :error ::cfg/entrypoint-bind-address-type
               :data {:entrypoint :web
                      :bind 10
                      :index 1
                      :type "java.lang.Long"}}
              {:msg "Bind address must not be blank."
               :error ::cfg/entrypoint-bind-address-blank
               :data {:entrypoint :web
                      :bind ""
                      :index 2}}]
             (cfg/validate-entrypoint [] entrypoint))))))

(deftest apply-entrypoint-defaults-test
  (testing "applies defaults to non-tls entrypoint"
    (let [entrypoint {:name :web :bind ":8080"}
          result (cfg/apply-entrypoint-defaults entrypoint)]
      (is (= true (:http1? result)))
      (is (= true (:http2? result)))
      (is (= true (:http3? result)))))
  (testing "defaults http3 to true for tls entrypoint"
    (let [entrypoint {:name :web
                      :bind ":8443"
                      :tls {:cert-file "server.crt"
                            :key-file "server.key"}}
          result (cfg/apply-entrypoint-defaults entrypoint)]
      (is (= true (:http1? result)))
      (is (= true (:http2? result)))
      (is (= true (:http3? result)))))
  (testing "preserves user-provided values"
    (let [entrypoint {:name :web :bind ":8080" :http1? false :http3? false}
          result (cfg/apply-entrypoint-defaults entrypoint)]
      (is (= false (:http1? result)))
      (is (= true (:http2? result)))
      (is (= false (:http3? result))))))

(deftest tls-false-entrypoint-test
  (testing "accepts explicit tls disable and defaults http3 to false"
    (let [config (cfg/load! {:entrypoints [{:name :web
                                            :bind ":8080"
                                            :tls false}]})
          ep (first (:entrypoints config))]
      (is (= false (:tls ep)))
      (is (= false (:http3? ep)))
      (is (= true (:http1? ep)))
      (is (= true (:http2? ep))))))

(deftest apply-config-defaults-test
  (testing "applies defaults to config"
    (let [config {:entrypoints [{:name :web :bind ":8080"}]}
          result (cfg/apply-config-defaults config)]
      (is (= 1024 (:max-connections result)))
      (is (= 32768 (:output-buffer-size result)))
      (is (= "ol.busker/dev" (:server-name result)))
      (is (= true (:compress? result)))
      (is (= 100 (:compress-min-size result)))
      (is (= 1 (:compress-gzip-level result)))
      (is (= 1 (:compress-brotli-level result)))
      (is (= 3 (:compress-zstd-level result)))
      (is (some? (:executor result)))
      (is (some? (:buffer-pool result)))))
  (testing "applies entrypoint defaults"
    (let [config {:entrypoints [{:name :web :bind ":8080"}]}
          result (cfg/apply-config-defaults config)
          ep (first (:entrypoints result))]
      (is (= true (:http1? ep)))
      (is (= true (:http2? ep)))
      (is (= true (:http3? ep)))))
  (testing "preserves user-provided values"
    (let [config {:entrypoints [{:name :web :bind ":8080"}]
                  :max-connections 512
                  :server-name "custom"}
          result (cfg/apply-config-defaults config)]
      (is (= 512 (:max-connections result)))
      (is (= "custom" (:server-name result))))))

(deftest validate-tls-test
  (testing "returns errors unchanged when no tls config"
    (let [entrypoint {:name :web :bind ":8080"}]
      (is (= [] (cfg/validate-tls [] entrypoint)))))
  (testing "rejects tls config missing cert-file"
    (let [entrypoint {:name :web :bind ":443" :tls {:key-file "/tmp/key.pem"}}
          errors (cfg/validate-tls [] entrypoint)]
      (is (= 1 (count (filter #(= ::cfg/tls-missing-cert-file (:error %)) errors))))))
  (testing "rejects tls config missing key-file"
    (let [entrypoint {:name :web :bind ":443" :tls {:cert-file "/tmp/cert.pem"}}
          errors (cfg/validate-tls [] entrypoint)]
      (is (= 1 (count (filter #(= ::cfg/tls-missing-key-file (:error %)) errors))))))
  (testing "rejects non-existent cert-file"
    (let [entrypoint {:name :web :bind ":443"
                      :tls {:cert-file "/nonexistent/cert.pem"
                            :key-file "/nonexistent/key.pem"}}
          errors (cfg/validate-tls [] entrypoint)]
      (is (some #(= ::cfg/tls-cert-file-not-found (:error %)) errors))
      (is (some #(= ::cfg/tls-key-file-not-found (:error %)) errors)))))

(deftest validate-config-test
  (testing "rejects config with no entrypoints"
    (let [errors (cfg/validate-config {})]
      (is (= 1 (count errors)))
      (is (= ::cfg/config-no-entrypoints (:error (first errors))))))
  (testing "rejects config with empty entrypoints"
    (let [errors (cfg/validate-config {:entrypoints []})]
      (is (= 1 (count errors)))
      (is (= ::cfg/config-no-entrypoints (:error (first errors))))))
  (testing "collects errors from all entrypoints"
    (let [config {:entrypoints [{:name :web1 :bind "bad1" :http1? true :http2? false :http3? false}
                                {:name :web2 :bind "bad2" :http1? true :http2? false :http3? false}]}
          errors (cfg/validate-config config)]
      (is (= 2 (count errors)))
      (is (every? #(= ::cfg/entrypoint-bind-address-invalid (:error %)) errors)))))

(deftest load!-test
  (testing "returns config with defaults on valid input"
    (let [config (cfg/load! {:entrypoints [{:name :web :bind ":8080" :http3? false}]})]
      (is (= 1024 (:max-connections config)))
      (is (some? (:executor config)))
      (is (some? (:buffer-pool config)))
      (is (= true (-> config :entrypoints first :http1?)))))
  (testing "uses default internal entrypoints when none provided"
    ;; Default entrypoints are internal (localhost), so no TLS errors
    (let [config (cfg/load! {})]
      (is (seq (:entrypoints config)))
      (is (some? (:executor config)))))
  (testing "throws on explicit empty entrypoints"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Invalid configuration"
                          (cfg/load! {:entrypoints []}))))
  (testing "throws with all errors collected"
    (try
      (cfg/load! {:entrypoints [{:name :web1 :bind "bad1" :http1? true :http2? false :http3? false}
                                {:name :web2 :bind "bad2" :http1? true :http2? false :http3? false}]})
      (is false "expected exception")
      (catch clojure.lang.ExceptionInfo e
        (let [errors (:errors (ex-data e))]
          (is (= 2 (count errors))))))))

(deftest load!-spec-validation-test
  (testing "rejects invalid n-workers type"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid configuration"
         (cfg/load! {:entrypoints [{:name :web :bind ":8080" :http3? false}]
                     :n-workers "two"}))))
  (testing "rejects invalid entrypoint name type"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid configuration"
         (cfg/load! {:entrypoints [{:name "web" :bind ":8080" :http3? false}]}))))
  (testing "rejects invalid tls compatibility mode"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid configuration"
         (cfg/load! {:entrypoints [{:name :web
                                    :bind "127.0.0.1:8443"
                                    :http3? false
                                    :tls {:tls-compatibility-mode :legacy}}]})))))

(deftest specs-defaults-test
  (testing "spec namespace exposes entrypoint defaults"
    (is (= {:http1? true
            :http2? true
            :tls {}}
           (:default specs/entrypoint))))
  (testing "spec namespace exposes config defaults used by config loader"
    (is (= 1 (:n-workers specs/default-config)))
    (is (= 1024 (:max-connections specs/default-config)))
    (is (= 86400 (:session-ticket-lifetime-seconds specs/default-config)))))
