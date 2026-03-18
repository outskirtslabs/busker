(ns ol.busker.reload-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.config :as config]
   [ol.busker.runtime :as runtime]
   [ol.busker.test-utils :as util]))

(defn- response-config
  [port handler]
  {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                        :tls false}}
   :dispatch [{:handler handler}]})

(deftest reload-activates-new-generation-and-drains-old-test
  (let [port 18580
        old-entered (promise)
        old-release (promise)
        server (runtime/start!
                (response-config port
                                 (fn [_]
                                   (deliver old-entered true)
                                   (deref old-release 5000 true)
                                   {:status 200
                                    :body "old-generation"})))]
    (try
      (let [old-request (future
                          (util/curl :http nil port "/" :max-time 10))]
        (is (deref old-entered 5000 false))
        (is (= :activated
               (runtime/reload! server
                                (response-config port
                                                 (fn [_]
                                                   {:status 200
                                                    :body "new-generation"}))
                                {:force? true})))
        (let [new-request (util/curl :http nil port "/" :max-time 5)]
          (is (= 0 (:exit new-request)))
          (is (= "new-generation" (:out new-request))))
        (deliver old-release true)
        (let [result (deref old-request 10000 nil)]
          (is (some? result))
          (when result
            (is (= 0 (:exit result)))
            (is (= "old-generation" (:out result))))))
      (finally
        (runtime/stop! server)))))

(deftest reload-failure-keeps-current-generation-active-test
  (let [port 18581
        good-config (response-config port
                                     (fn [_]
                                       {:status 200
                                        :body "still-active"}))
        bad-config (response-config port
                                    (fn [_]
                                      {:status 200
                                       :body "never-starts"}))
        real-load! config/load!
        server (runtime/start! good-config)]
    (try
      (with-redefs [config/load!
                    (fn [user-config]
                      (if (= bad-config user-config)
                        (throw (ex-info "candidate build failed"
                                        {:stage :validation}))
                        (real-load! user-config)))]
        (try
          (runtime/reload! server bad-config {:force? true})
          (is false)
          (catch clojure.lang.ExceptionInfo e
            (is (= {:reason :reload-failed
                    :stage :validation}
                   (select-keys (ex-data e) [:reason :stage]))))))
      (let [result (util/curl :http nil port "/" :max-time 5)]
        (is (= 0 (:exit result)))
        (is (= "still-active" (:out result))))
      (finally
        (runtime/stop! server)))))

(deftest reload-calls-serialize-through-lifecycle-gate-test
  (let [port 18582
        server (runtime/start!
                (response-config port
                                 (fn [_]
                                   {:status 200
                                    :body "base"})))
        reload-a-config (response-config port
                                         (fn [_]
                                           {:status 200
                                            :body "reload-a"}))
        reload-b-config (response-config port
                                         (fn [_]
                                           {:status 200
                                            :body "reload-b"}))
        real-load! config/load!
        first-entered (promise)
        release-first (promise)
        events (atom [])]
    (try
      (with-redefs [config/load!
                    (fn [user-config]
                      (cond
                        (= reload-a-config user-config)
                        (do
                          (swap! events conj :reload-a-enter)
                          (deliver first-entered true)
                          (deref release-first 5000 true)
                          (swap! events conj :reload-a-exit)
                          (real-load! user-config))

                        (= reload-b-config user-config)
                        (do
                          (swap! events conj :reload-b-enter)
                          (real-load! user-config))

                        :else
                        (real-load! user-config)))]
        (let [reload-a (future (runtime/reload! server reload-a-config {:force? true}))
              _ (is (deref first-entered 5000 false))
              reload-b (future (runtime/reload! server reload-b-config {:force? true}))]
          (Thread/sleep 200)
          (is (= [:reload-a-enter] @events))
          (deliver release-first true)
          (is (= :activated (deref reload-a 10000 nil)))
          (is (= :activated (deref reload-b 10000 nil)))
          (is (= [:reload-a-enter :reload-a-exit :reload-b-enter]
                 @events))))
      (finally
        (runtime/stop! server)))))

(deftest stop-waits-for-active-and-draining-generations-test
  (let [port 18583
        old-entered (promise)
        old-completed (promise)
        old-release (promise)
        new-entered (promise)
        new-completed (promise)
        new-release (promise)
        server (runtime/start!
                (response-config port
                                 (fn [_]
                                   (deliver old-entered true)
                                   (deref old-release 5000 true)
                                   (deliver old-completed true)
                                   {:status 200
                                    :body "old"})))]
    (try
      (let [old-request (future
                          (try
                            (util/curl :http nil port "/" :max-time 10)
                            (catch Throwable t
                              t)))]
        (is (deref old-entered 5000 false))
        (is (= :activated
               (runtime/reload! server
                                (response-config port
                                                 (fn [_]
                                                   (deliver new-entered true)
                                                   (deref new-release 5000 true)
                                                   (deliver new-completed true)
                                                   {:status 200
                                                    :body "new"}))
                                {:force? true})))
        (let [new-request (future
                            (try
                              (util/curl :http nil port "/" :max-time 10)
                              (catch Throwable t
                                t)))
              _ (is (deref new-entered 5000 false))
              stop-fut (future (runtime/stop! server))]
          (is (= ::timeout (deref stop-fut 200 ::timeout)))
          (deliver old-release true)
          (is (= ::timeout (deref stop-fut 200 ::timeout)))
          (deliver new-release true)
          (is (deref old-completed 10000 false))
          (is (deref new-completed 10000 false))
          (deref old-request 10000 nil)
          (deref new-request 10000 nil)
          (is (not= ::timeout (deref stop-fut 10000 ::timeout)))))
      (finally
        (when (not= :stopped (:phase (runtime/state server)))
          (runtime/stop! server))))
    (is (= :stopped (:phase (runtime/state server))))))
