(ns ol.busker-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.config :as config]
   [ol.busker.test-utils :as util]
   [ol.clave.acme.solver.http :as http-solver]
   [ol.clave.automation :as automation])
  (:import
   [java.util.concurrent LinkedBlockingQueue]))

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
                     t))
          retryable?
          (or (instance? Throwable result)
              (and (map? result)
                   (not= 0 (:exit result))
                   (re-find #"Failed to connect|Could not connect|Connection refused|timed out|Timeout"
                            (str (:err result)))))]
      (if (and retryable?
               (< attempt 4))
        (do
          (Thread/sleep 100)
          (recur (inc attempt)))
        result))))

(defn- managed-http-config
  [port body]
  {:tls {:certificates {:manage ["managed.example"]}
         :issuers [{:directory-url "https://acme.example/directory"}]}
   :entrypoints {:http {:bind (str "127.0.0.1:" port)
                        :tls false}}
   :dispatch [{:handler (fn [_]
                          {:status 200
                           :body body})}]})

(deftest public-api-start-stop-and-state-test
  (testing "The public API starts the server and exposes pure state data"
    (let [port (util/free-port)
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
  (let [port (util/free-port)
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

(deftest public-api-managed-reload-stays-healthy-after-delayed-certificate-failure-test
  (let [port (util/free-port)
        queue (LinkedBlockingQueue.)
        failed-event {:type :certificate-failed
                      :data {:domain "managed.example"
                             :reason :acme-error}}
        queued (promise)
        server (busker/start!
                {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                      :tls false}}
                 :dispatch [{:handler (fn [_]
                                        {:status 200
                                         :body "before-managed"})}]})]
    (try
      (with-redefs [clave-adapter/initial-cert-wait-timeout-ms 200
                    clave-adapter/event-poll-timeout-ms 10
                    http-solver/solver (fn [] {:registry (atom {})})
                    automation/create (fn [_] {:id ::system})
                    automation/start identity
                    automation/manage-domains (fn [_ _] nil)
                    automation/get-event-queue (fn [_] queue)
                    automation/lookup-cert (fn [_ _] nil)
                    automation/stop (fn [_] nil)]
        (future
          (Thread/sleep 25)
          (.offer queue failed-event)
          (deliver queued true))
        (is (= :activated
               (busker/reload! server
                               (managed-http-config port "managed-active")
                               {:force? true})))
        (is (deref queued 1000 false))
        (is (true?
             (loop [attempt 0]
               (cond
                 (zero? (.size queue))
                 true

                 (>= attempt 20)
                 false

                 :else
                 (do
                   (Thread/sleep 25)
                   (recur (inc attempt))))))
            "The delayed failure event should be consumed after activation")
        (is (= :activated
               (busker/reload! server
                               {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                                     :tls false}}
                                :dispatch [{:handler (fn [_]
                                                       {:status 200
                                                        :body "after-failure"})}]}
                               {:force? true})))
        (let [result (eventually-curl :http nil port "/")]
          (is (= 0 (:exit result)))
          (is (= "after-failure" (:out result)))))
      (finally
        (with-redefs [automation/stop (fn [_] nil)]
          (busker/stop! server))))))

(deftest public-api-unchanged-and-failed-reload-test
  (let [port (util/free-port)
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
      (let [result (eventually-curl :http nil port "/")]
        (is (= 0 (:exit result)))
        (is (= "steady" (:out result))))
      (finally
        (busker/stop! server)))))
