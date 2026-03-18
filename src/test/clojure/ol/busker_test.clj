(ns ol.busker-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
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
