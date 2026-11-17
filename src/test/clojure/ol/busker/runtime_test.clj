(ns ol.busker.runtime-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.generation :as generation]
   [ol.busker.runtime :as runtime]
   [ol.busker.test-utils :as util]))

(defn- invoke-on-virtual-thread
  [f]
  (let [result_ (promise)]
    (Thread/startVirtualThread
     #(try
        (deliver result_ {:value (f)})
        (catch Throwable t
          (deliver result_ {:error t}))))
    (let [{:keys [value error]} @result_]
      (when error
        (throw error))
      value)))
(defn- lifecycle-thread-count
  []
  (->> (.keySet (Thread/getAllStackTraces))
       (filter #(= "busker-lifecycle" (.getName ^Thread %)))
       (filter #(.isAlive ^Thread %))
       count))

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
                           (runtime/stop! server))
                second-stop-fut (future
                                  (runtime/stop! server))]
            (is (= ::timeout (deref stop-fut 200 ::timeout))
                "stop! should wait for the in-flight request to finish")
            (is (= ::timeout (deref second-stop-fut 200 ::timeout))
                "concurrent stop! should wait for the same in-flight request")
            (deliver release true)
            (let [result (deref request-fut 10000 nil)]
              (is (some? result))
              (when result
                (is (= 0 (:exit result))
                    (str "Request should complete during stop. stderr: "
                         (:err result)))
                (is (= "released" (:out result)))))
            (is (not= ::timeout (deref stop-fut 10000 ::timeout))
                "stop! should complete after the request finishes")
            (is (not= ::timeout (deref second-stop-fut 10000 ::timeout))
                "concurrent stop! should complete with the same result"))))
      (finally
        (when (= :running (:phase (runtime/state server)))
          (runtime/stop! server))))
    (is (= :stopped (:phase (runtime/state server))))))

(deftest virtual-thread-lifecycle-calls-run-generation-work-on-platform-threads-test
  (let [port (util/free-port)
        user-config {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                          :tls false}}
                     :dispatch [{:handler (fn [_]
                                            {:status 200
                                             :body "lifecycle-platform"})}]}
        calls_ (atom [])
        start-generation generation/start!
        begin-stop generation/begin-stop!
        record! (fn [operation]
                  (swap! calls_ conj {:operation operation
                                      :virtual? (.isVirtual (Thread/currentThread))}))]
    (with-redefs [generation/start!
                  (fn [& args]
                    (record! :start)
                    (apply start-generation args))
                  generation/begin-stop!
                  (fn [instance]
                    (record! :begin-stop)
                    (begin-stop instance))]
      (let [server (invoke-on-virtual-thread #(runtime/start! user-config))]
        (try
          (is (= :activated
                 (invoke-on-virtual-thread
                  #(runtime/reload! server user-config {:force? true}))))
          (finally
            (invoke-on-virtual-thread #(runtime/stop! server)))))
      (is (seq @calls_))
      (is (every? (comp false? :virtual?) @calls_)))))
(deftest repeated-stop-reuses-lifecycle-result-test
  (let [port (util/free-port)
        server (runtime/start!
                {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                      :tls false}}
                 :dispatch [{:handler (fn [_]
                                        {:status 200
                                         :body "stop-twice"})}]})]
    (is (nil? (invoke-on-virtual-thread #(runtime/stop! server))))
    (is (nil? (invoke-on-virtual-thread #(runtime/stop! server))))
    (is (.isShutdown ^java.util.concurrent.ExecutorService
         (:busker/lifecycle-executor server)))))

(deftest runtime-start-fails-when-automation-startup-fails-test
  (let [lifecycle-threads-before (lifecycle-thread-count)]
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
                   (ex-data e)))))
        (is (loop [remaining 100]
              (if (<= (lifecycle-thread-count) lifecycle-threads-before)
                true
                (when (pos? remaining)
                  (Thread/sleep 10)
                  (recur (dec remaining))))))))))

(deftest start-of-a-config-that-is-not-a-map-reports-the-validation-stage-test
  (let [e (try (runtime/start! "not-a-map") nil (catch Throwable t t))]
    (is (= "Failed to compile generation config" (ex-message e)))
    (is (= :validation (:stage (ex-data e))))))

(deftest a-start-error-that-its-cleanup-throws-again-is-not-suppressed-in-itself-test
  (let [e (ex-info "simulated" {})
        stop! generation/stop!
        stopped_ (atom [])
        error (with-redefs [generation/register-shared-ticket-manager! (fn [_] (throw e))
                            generation/stop! (fn [g & _] (swap! stopped_ conj g) (throw e))]
                (try
                  (runtime/start! {:entrypoints {:http {:bind (str "127.0.0.1:" (util/free-port))
                                                        :tls false}}
                                   :dispatch [{:handler (constantly {:status 200})}]})
                  nil
                  (catch Throwable t t)))]
    (run! stop! @stopped_)
    (is (identical? e error))
    (is (not-any? #(identical? e %) (.getSuppressed ^Throwable e)))))
