(ns ol.busker.dispatch-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.config :as cfg]
   [ol.busker.test-utils :as util]))

(defn api-match?
  [req]
  (= "/api" (:uri req)))

(defn admin-match?
  [req]
  (= "/admin" (:uri req)))

(defn trace-step
  [req label]
  (update req :trace (fnil conj []) label))

(defn wrap-trace
  [handler {:keys [label]}]
  (fn [req]
    (-> req
        (trace-step [:mw-in label])
        handler
        (update :trace (fnil conj []) [:mw-out label]))))

(deftest dispatch-handler-test
  (testing "routes by entrypoint and resolves qualified symbols"
    (let [config (cfg/load!
                  {:tls {:certificates {:load [{:type :pem
                                                :cert-file (util/fixture-cert-path)
                                                :key-file (util/fixture-key-path)}]}}
                   :entrypoints {:http {:bind ":8080" :tls false}
                                 :https {:bind ":8443"
                                         :tls {:tls-compatibility-mode
                                               :modern}}}
                   :dispatch [{:entrypoints #{:https}
                               :terminal? true
                               :match 'ol.busker.dispatch-test/api-match?
                               :handler (fn [req]
                                          {:status 200
                                           :trace (:trace req)
                                           :body :https-api})}
                              {:handler (fn [_]
                                          {:status 200
                                           :body :fallback})}]})
          handler (cfg/dispatch-handler config)]
      (is (= {:status 200
              :trace nil
              :body :https-api}
             (handler {:scheme :https
                       :server-port 8443
                       :uri "/api"})))
      (is (= {:status 200
              :body :fallback}
             (handler {:scheme :http
                       :server-port 8080
                       :uri "/api"})))))

  (testing "honors mutually exclusive groups and terminal dispatchers"
    (let [config (cfg/load!
                  {:entrypoints {:http {:bind ":8080" :tls false}}
                   :dispatch [{:group :site
                               :match (constantly true)
                               :handler (fn [req]
                                          (trace-step req :group-a))}
                              {:group :site
                               :match (constantly true)
                               :handler (fn [req]
                                          (trace-step req :group-b))}
                              {:terminal? true
                               :match 'ol.busker.dispatch-test/admin-match?
                               :handler (fn [req]
                                          {:status 200
                                           :trace (:trace req)
                                           :body :admin})}
                              {:handler (fn [req]
                                          {:status 200
                                           :trace (:trace req)
                                           :body :fallback})}]})
          handler (cfg/dispatch-handler config)]
      (is (= {:status 200
              :trace [:group-a]
              :body :admin}
             (handler {:scheme :http
                       :server-port 8080
                       :uri "/admin"})))
      (is (= {:status 200
              :trace [:group-a]
              :body :fallback}
             (handler {:scheme :http
                       :server-port 8080
                       :uri "/other"})))))

  (testing "wraps middleware around dispatcher handlers"
    (let [config (cfg/load!
                  {:entrypoints {:http {:bind ":8080" :tls false}}
                   :dispatch [{:middleware [['ol.busker.dispatch-test/wrap-trace
                                             {:label :audit}]]
                               :handler (fn [req]
                                          {:status 200
                                           :trace (:trace req)
                                           :body :ok})}]})
          handler (cfg/dispatch-handler config)]
      (is (= {:status 200
              :trace [[:mw-in :audit] [:mw-out :audit]]
              :body :ok}
             (handler {:scheme :http
                       :server-port 8080
                       :uri "/"}))))))
