(ns ol.busker-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.config :as config]
   [ol.busker.test-utils :as util]))

(defn- callable-value?
  [v]
  (and (ifn? v)
       (not (vector? v))
       (not (map? v))
       (not (set? v))
       (not (symbol? v))
       (not (keyword? v))
       (not (string? v))))

(defn- eventually-curl
  [scheme proto port path]
  (loop [attempt 0]
    (let [result (try
                   (util/curl scheme proto port path :max-time 5)
                   (catch Throwable t
                     t))]
      (if (or (map? result)
              (>= attempt 4))
        result
        (do
          (Thread/sleep 100)
          (recur (inc attempt)))))))

(deftest public-api-start-stop-and-state-test
  (testing "The public API starts the server and exposes pure state data"
    (let [port 18573
          server (busker/start!
                  {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                        :tls false}}
                   :dispatch [{:handler (fn [_]
                                          {:status 200
                                           :body "busker-ok"})}]})
          state (busker/state server)]
      (try
        (is (= :running (:phase state)))
        (is (= [{:entrypoint :http
                 :host "127.0.0.1"
                 :port port}]
               (:listeners (:config state))))
        (is (not (contains? (:config state) :executor)))
        (is (not (contains? (:config state) :buffer-pool)))
        (is (not-any? callable-value?
                      (tree-seq coll? seq (:config state)))
            "Public state should not expose handler functions or other callables")
        (let [result (util/curl :http nil port "/" :max-time 5)]
          (is (= 0 (:exit result))
              (str "HTTP request should succeed. stderr: " (:err result)))
          (is (= "busker-ok" (:out result))))
        (finally
          (busker/stop! server)))
      (is (= :stopped (:phase (busker/state server)))))))

(deftest public-api-reload-test
  (let [port 18587
        server (busker/start!
                {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                      :tls false}}
                 :dispatch [{:handler (fn [_]
                                        {:status 200
                                         :body "before-reload"})}]})]
    (try
      (is (= :activated
             (busker/reload! server
                             {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                                   :tls false}}
                              :dispatch [{:handler (fn [_]
                                                     {:status 200
                                                      :body "after-reload"})}]}
                             {:force? true})))
      (let [result (eventually-curl :http nil port "/")]
        (is (= 0 (:exit result)))
        (is (= "after-reload" (:out result))))
      (finally
        (busker/stop! server)))))

(deftest public-api-unchanged-and-failed-reload-test
  (let [port 18588
        config {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                     :tls false}}
                :dispatch [{:handler (fn [_]
                                       {:status 200
                                        :body "steady"})}]}
        bad-config {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                         :tls false}}
                    :dispatch [{:handler (fn [_]
                                           {:status 200
                                            :body "bad"})}]}
        real-load! config/load!
        server (busker/start! config)]
    (try
      (is (= :unchanged
             (busker/reload! server config)))
      (with-redefs [config/load!
                    (fn [user-config]
                      (if (= bad-config user-config)
                        (throw (ex-info "bad reload"
                                        {:stage :validation}))
                        (real-load! user-config)))]
        (try
          (busker/reload! server bad-config {:force? true})
          (is false)
          (catch clojure.lang.ExceptionInfo e
            (is (= {:reason :reload-failed
                    :stage :validation}
                   (select-keys (ex-data e) [:reason :stage]))))))
      (let [result (util/curl :http nil port "/" :max-time 5)]
        (is (= 0 (:exit result)))
        (is (= "steady" (:out result))))
      (finally
        (busker/stop! server)))))
