(ns ol.busker.runtime-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.config :as config]
   [ol.busker.runtime :as runtime]
   [ol.busker.test-utils :as util]))

(deftest runtime-starts-and-serves-one-generation-test
  (let [port (util/free-port)
        server (runtime/start!
                {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                      :tls false}}
                 :dispatch [{:handler (fn [_]
                                        {:status 200
                                         :body "runtime-ok"})}]})]
    (try
      (testing "state reports the running generation"
        (is (= :running (:phase (runtime/state server)))))
      (testing "requests are served through the runtime handle"
        (let [result (util/curl :http nil port "/" :max-time 5)]
          (is (= 0 (:exit result))
              (str "HTTP request should succeed. stderr: " (:err result)))
          (is (= "runtime-ok" (:out result)))))
      (finally
        (runtime/stop! server))))
  (let [port (util/free-port)
        entered (promise)
        release (promise)
        server (runtime/start!
                {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                      :tls false}}
                 :dispatch [{:handler (fn [_]
                                        (deliver entered true)
                                        (deref release 5000 true)
                                        {:status 200
                                         :body "released"})}]})]
    (try
      (testing "stop! waits for the in-flight request to finish"
        (let [request-fut (future
                            (util/curl :http nil port "/" :max-time 10))]
          (is (deref entered 5000 false)
              "The request should enter the handler before stop begins")
          (let [stop-fut (future
                           (runtime/stop! server))]
            (is (= ::timeout (deref stop-fut 200 ::timeout))
                "stop! should wait for the in-flight request to finish")
            (deliver release true)
            (let [result (deref request-fut 10000 nil)]
              (is (some? result))
              (when result
                (is (= 0 (:exit result))
                    (str "Request should complete during stop. stderr: "
                         (:err result)))
                (is (= "released" (:out result)))))
            (is (not= ::timeout (deref stop-fut 10000 ::timeout))
                "stop! should complete after the request finishes"))))
      (finally
        (when (= :running (:phase (runtime/state server)))
          (runtime/stop! server))))
    (is (= :stopped (:phase (runtime/state server))))))

(deftest cert-automation-reuse-and-replacement-test
  (let [started (atom [])
        current-runtime {:managed-plan {:subject-names ["a.example"]
                                        :clave-config {:issuer :acme}}
                         :system ::current}
        acquire! #'ol.busker.runtime/acquire-cert-automation!]
    (with-redefs [clave-adapter/start! (fn [managed-plan]
                                         (swap! started conj managed-plan)
                                         {:managed-plan managed-plan
                                          :system ::started})]
      (let [reused (acquire! current-runtime
                             {:subject-names ["a.example"]
                              :clave-config {:issuer :acme}})
            replaced (acquire! current-runtime
                               {:subject-names ["b.example"]
                                :clave-config {:issuer :acme}})]
        (is (= current-runtime (:runtime reused)))
        (is (true? (:reused? reused)))
        (is (nil? (:superseded reused)))
        (is (= {:managed-plan {:subject-names ["b.example"]
                               :clave-config {:issuer :acme}}
                :system ::started}
               (:runtime replaced)))
        (is (false? (:reused? replaced)))
        (is (= current-runtime (:superseded replaced)))
        (is (= [{:subject-names ["b.example"]
                 :clave-config {:issuer :acme}}]
               @started))))))

(deftest runtime-start-fails-when-automation-startup-fails-test
  (with-redefs [clave-adapter/build-managed-plan
                (fn [_]
                  {:subject-names ["managed.example"]
                   :clave-config {:issuer :acme}})
                clave-adapter/start!
                (fn [_]
                  (throw (ex-info "automation failed"
                                  {:stage :automation-startup})))]
    (let [config {:entrypoints {:http {:bind (str "127.0.0.1:" (util/free-port))
                                       :tls false}}
                  :dispatch [{:handler (fn [_]
                                         {:status 200
                                          :body "unused"})}]}]
      (try
        (runtime/start! config)
        (is false "start! should throw when automation startup fails")
        (catch clojure.lang.ExceptionInfo e
          (is (= {:stage :automation-startup}
                 (ex-data e))))))))

(deftest candidate-plan-skips-unchanged-snapshots-unless-forced-test
  (let [port (util/free-port)
        user-config {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                          :tls false}}
                     :dispatch [{:handler (fn [_]
                                            {:status 200
                                             :body "unchanged"})}]}
        snapshot (config/normalized-snapshot user-config)
        plan! #'ol.busker.runtime/candidate-plan]
    (is (= {:action :unchanged
            :snapshot snapshot}
           (plan! snapshot nil user-config {})))
    (is (= :activate
           (:action (plan! snapshot nil user-config {:force? true}))))
    (is (true? (:force? (plan! snapshot nil user-config {:force? true}))))))

(deftest candidate-plan-computes-listener-and-automation-deltas-test
  (let [current-port (util/free-port)
        next-port (util/free-port)
        current-config {:tls {:certificates {:manage ["a.example"]}
                              :issuers [{:directory-url
                                         "https://acme.example/directory"}]}
                        :entrypoints {:https {:bind (str "127.0.0.1:" current-port)
                                              :tls {:tls-compatibility-mode
                                                    :modern}}}
                        :dispatch [{:handler (fn [_]
                                               {:status 200
                                                :body "current"})}]}
        next-config {:tls {:certificates {:manage ["b.example"]}
                           :issuers [{:directory-url
                                      "https://acme.example/directory"}]}
                     :entrypoints {:https {:bind (str "127.0.0.1:" next-port)
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}
                     :dispatch [{:handler (fn [_]
                                            {:status 200
                                             :body "next"})}]}
        current-snapshot (config/normalized-snapshot current-config)
        current-managed-plan (clave-adapter/build-managed-plan current-snapshot)
        plan! #'ol.busker.runtime/candidate-plan
        plan (plan! current-snapshot current-managed-plan next-config {})]
    (is (= :activate (:action plan)))
    (is (= {:action :replace
            :managed-plan {:subject-names ["b.example"]
                           :clave-config {:issuers [{:directory-url
                                                     "https://acme.example/directory"}]}}}
           (:automation-plan plan)))
    (is (= {:reuse []
            :acquire [{:transport :tcp
                       :host "127.0.0.1"
                       :port next-port}
                      {:transport :udp
                       :host "127.0.0.1"
                       :port next-port}]
            :release [{:transport :tcp
                       :host "127.0.0.1"
                       :port current-port}
                      {:transport :udp
                       :host "127.0.0.1"
                       :port current-port}]}
           (:listener-plan plan)))))
