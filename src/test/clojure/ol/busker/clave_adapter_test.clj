(ns ol.busker.clave-adapter-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :refer [deftest is testing]]
   [ol.busker.specs :as specs]
   [ol.busker.clave-adapter :as adapter]
   [ol.clave.acme.solver.http :as http-solver]
   [ol.clave.automation :as automation]
   [ol.clave.storage.file :as file-storage])
  (:import
   [java.util.concurrent LinkedBlockingQueue]))

(deftest build-managed-plan-test
  (testing "returns nil when no managed tls entrypoints exist"
    (is (nil? (adapter/build-managed-plan
               {:domains ["example.com"]
                :entrypoints [{:name :http
                               :bind ":8080"
                               :http3? false
                               :tls false}
                              {:name :https
                               :bind ":8443"
                               :http3? false
                               :tls {:cert-file "cert.pem"
                                     :key-file "key.pem"}}]}))))

  (testing "extracts managed domains and clave config from tls"
    (let [storage (file-storage/file-storage "target/clave-adapter-test")
          config-fn (fn [_] nil)
          plan (adapter/build-managed-plan
                {:domains ["example.com" "www.example.com" "example.com"]
                 :entrypoints [{:name :https
                                :bind ":443"
                                :http3? false
                                :tls {:issuers [{:directory-url "https://acme.example/directory"}]
                                      :storage storage
                                      :issuer-selection :shuffle
                                      :key-type :p384
                                      :key-reuse true
                                      :cache-capacity 12
                                      :solvers {:tls-alpn-01 :existing}
                                      :ocsp {:enabled false}
                                      :config-fn config-fn
                                      :http-client {:connect-timeout 1000}}}]})]
      (is (= {:domains ["example.com" "www.example.com"]
              :managed-entrypoints [{:name :https
                                     :bind ":443"
                                     :http3? false
                                     :tls {:issuers [{:directory-url "https://acme.example/directory"}]
                                           :storage storage
                                           :issuer-selection :shuffle
                                           :key-type :p384
                                           :key-reuse true
                                           :cache-capacity 12
                                           :solvers {:tls-alpn-01 :existing}
                                           :ocsp {:enabled false}
                                           :config-fn config-fn
                                           :http-client {:connect-timeout 1000}}}]
              :clave-config {:issuers [{:directory-url "https://acme.example/directory"}]
                             :storage storage
                             :issuer-selection :shuffle
                             :key-type :p384
                             :key-reuse true
                             :cache-capacity 12
                             :solvers {:tls-alpn-01 :existing}
                             :ocsp {:enabled false}
                             :config-fn config-fn
                             :http-client {:connect-timeout 1000}}}
             plan))
      (is (s/valid? ::specs/managed-plan plan)))))

(deftest start-managed-test
  (testing "starts clave, manages domains, and waits for initial cert readiness"
    (let [create-config (atom nil)
          calls (atom [])
          queue (doto (LinkedBlockingQueue.)
                  (.offer {:type :certificate-obtained
                           :data {:domain "example.com"}}))
          lookup-count (atom 0)
          fake-solver {:registry (atom {})}
          plan {:domains ["example.com"]
                :managed-entrypoints [{:name :https
                                       :bind ":443"
                                       :tls {:issuers [{:directory-url "https://acme.example/directory"}]}}]
                :clave-config {:issuers [{:directory-url "https://acme.example/directory"}]
                               :solvers {:tls-alpn-01 :existing}}}
          system {:id ::system}]
      (with-redefs [adapter/initial-cert-wait-timeout-ms 500
                    adapter/event-poll-timeout-ms 10
                    http-solver/solver (fn [] fake-solver)
                    automation/create (fn [config]
                                        (reset! create-config config)
                                        (swap! calls conj :create)
                                        system)
                    automation/start! (fn [s]
                                        (swap! calls conj :start)
                                        s)
                    automation/manage-domains (fn [s domains]
                                                (swap! calls conj [:manage s domains])
                                                nil)
                    automation/get-event-queue (fn [_]
                                                 queue)
                    automation/lookup-cert (fn [_ _]
                                             (if (> (swap! lookup-count inc) 1)
                                               {:names ["example.com"]}
                                               nil))]
        (let [runtime (adapter/start! plan)]
          (is (= [:create
                  :start
                  [:manage system ["example.com"]]]
                 @calls))
          (is (= {:system system
                  :domains ["example.com"]
                  :http-solver fake-solver}
                 (select-keys runtime [:system :domains :http-solver])))
          (is (ifn? (:lookup-fn runtime)))
          (is (= {:issuers [{:directory-url "https://acme.example/directory"}]
                  :solvers {:tls-alpn-01 :existing
                            :http-01 fake-solver}}
                 @create-config)
              "existing solvers are preserved")))))

  (testing "raises on certificate-failed events while waiting"
    (let [queue (doto (LinkedBlockingQueue.)
                  (.offer {:type :certificate-failed
                           :data {:domain "example.com"
                                  :reason :acme-error}}))
          plan {:domains ["example.com"]
                :managed-entrypoints [{:name :https
                                       :bind ":443"
                                       :tls {:issuers [{:directory-url "https://acme.example/directory"}]}}]
                :clave-config {:issuers [{:directory-url "https://acme.example/directory"}]}}
          system {:id ::system}]
      (with-redefs [adapter/initial-cert-wait-timeout-ms 200
                    adapter/event-poll-timeout-ms 10
                    http-solver/solver (fn [] {:registry (atom {})})
                    automation/create (fn [_] system)
                    automation/start! identity
                    automation/manage-domains (fn [_ _] nil)
                    automation/get-event-queue (fn [_] queue)
                    automation/lookup-cert (fn [_ _] nil)
                    automation/stop (fn [_] nil)]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"failed"
             (adapter/start! plan)))))))

(testing "rejects invalid managed-plan shape"
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Invalid managed plan"
       (adapter/start! {:domains ["example.com"]
                        :clave-config {:issuers [{:directory-url "https://acme.example/directory"}]}}))))

(deftest wrap-handler-test
  (testing "returns original handler when runtime is nil"
    (let [handler (fn [_] {:status 200 :body "ok"})]
      (is (= {:status 200 :body "ok"}
             ((adapter/wrap-handler handler nil) {:uri "/any"}))))

    (let [handler (fn [_] {:status 200 :body "ok"})]
      (is (= {:status 200 :body "ok"}
             ((adapter/wrap-handler handler {}) {:uri "/any"})))))

  (testing "composes acme middleware for challenge routes"
    (let [solver {:registry (atom {"abc123" "token-value"})}
          wrapped (adapter/wrap-handler (fn [_] {:status 200 :body "app"})
                                        {:http-solver solver})]
      (is (= {:status 200
              :headers {"content-type" "text/plain"}
              :body "token-value"}
             (wrapped {:uri "/.well-known/acme-challenge/abc123"})))
      (is (= {:status 200 :body "app"}
             (wrapped {:uri "/not-a-challenge"}))))))

(deftest lookup-certificate-test
  (testing "returns nil when runtime is missing"
    (is (nil? (adapter/lookup-certificate nil "example.com")))
    (is (nil? (adapter/lookup-certificate {} "example.com"))))

  (testing "delegates to clave lookup"
    (let [runtime {:system {:id ::system}}]
      (with-redefs [automation/lookup-cert (fn [_ hostname]
                                             (when (= hostname "hit.example")
                                               {:names [hostname]}))]
        (is (= {:names ["hit.example"]}
               (adapter/lookup-certificate runtime "hit.example")))
        (is (nil? (adapter/lookup-certificate runtime "miss.example")))))))

(deftest stop-managed-test
  (testing "delegates stop to clave automation"
    (let [stopped (atom nil)
          runtime {:system {:id ::system}}]
      (with-redefs [automation/stop (fn [system]
                                      (reset! stopped system)
                                      nil)]
        (is (nil? (adapter/stop! runtime)))
        (is (= {:id ::system} @stopped)))))

  (testing "returns nil when runtime is nil"
    (is (nil? (adapter/stop! nil)))
    (is (nil? (adapter/stop! {})))))
