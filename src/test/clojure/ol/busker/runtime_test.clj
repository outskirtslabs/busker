(ns ol.busker.runtime-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.runtime :as runtime]
   [ol.busker.test-utils :as util]))

(deftest runtime-starts-and-serves-one-generation-test
  (let [port 18571
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
  (let [port 18572
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
    (let [config {:entrypoints {:http {:bind "127.0.0.1:18575"
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
