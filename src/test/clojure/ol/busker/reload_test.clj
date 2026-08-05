(ns ol.busker.reload-test
  (:require
   [clojure.test :refer [deftest is]]
   [coffi.mem :as mem]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.native :as native]
   [ol.busker.config :as config]
   [ol.busker.generation :as generation]
   [ol.busker.listen :as listen]
   [ol.busker.protocols :as proto]
   [ol.busker.runtime :as runtime]
   [ol.busker.test-utils :as util]
   [ol.busker.tickets :as tickets]
   [ol.clave.acme.solver.http :as http-solver]
   [ol.clave.automation :as automation])
  (:import
   [java.util.concurrent ExecutorService LinkedBlockingQueue]))

(defn- response-config
  [port handler]
  {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                        :tls false}}
   :dispatch [{:handler handler}]})

(defn- tls-response-config
  [port handler]
  (util/with-static-tls
    {:entrypoints {:https {:bind (str "127.0.0.1:" port)
                           :http3? true
                           :tls {:tls-compatibility-mode :modern}}}
     :dispatch [{:handler handler}]}))

(defn- managed-http-config
  [port handler]
  {:tls {:certificates {:manage ["managed.example"]}
         :issuers [{:directory-url "https://acme.example/directory"}]}
   :entrypoints {:http {:bind (str "127.0.0.1:" port)
                        :tls false}}
   :dispatch [{:handler handler}]})

(defn- eventually-curl
  [scheme proto port path & {:keys [max-time]
                             :or {max-time 10}}]
  (loop [attempt 0]
    (let [result (try
                   (util/curl scheme proto port path :max-time max-time)
                   (catch Throwable t
                     t))
          connect-failed?
          (and (map? result)
               (not= 0 (:exit result))
               (re-find #"Failed to connect|Could not connect|Connection refused"
                        (str (:err result))))]
      (if (and (or connect-failed?
                   (instance? Throwable result))
               (< attempt 5))
        (do
          (Thread/sleep 100)
          (recur (inc attempt)))
        (if (instance? Throwable result)
          (throw result)
          result)))))

(defn- udp-transport-resource
  [generation-instance]
  (->> (::generation/listener-claims generation-instance)
       (keep (fn [[claim-key claim]]
               (when (= :udp (:transport claim-key))
                 (listen/resource claim))))
       first))

(defn- active-generation-instance
  [server]
  (:instance (:active @(:busker/state server))))

(defn- active-generation-keys-edn
  [server]
  (some-> server
          active-generation-instance
          ::generation/key-manager
          :service
          :snapshot-atom
          deref
          :keys
          (->> (mapv tickets/key->edn))))

(defn- generation-shared-ticket-service
  [generation-state]
  (some-> generation-state
          :instance
          ::generation/key-manager
          :service))

(defn- shared-ticket-manager-count
  [service]
  (count @(:native-mgrs-atom service)))

(deftest reload-publishes-candidate-before-old-generation-begins-drain-test
  (let [old-generation {:generation-id 1
                        :config {:version :old}
                        :managed-plan nil
                        :instance ::old-instance}
        candidate {:generation-id 2
                   :config {:version :new}
                   :managed-plan nil
                   :instance ::new-instance}
        state-atom (atom {:phase :running
                          :config (:config old-generation)
                          :next-generation-id 2
                          :listener-pool ::listener-pool
                          :active old-generation
                          :draining []})
        server {:busker/lifecycle-gate (Object.)
                :busker/state state-atom}
        events (atom [])]
    (with-redefs-fn {#'ol.busker.runtime/candidate-plan
                     (fn [_ _ _ _]
                       {:action :activate})
                     #'ol.busker.runtime/build-generation!
                     (fn [_ generation-id listener-pool active memory-ticket-service]
                       (swap! events conj :candidate-built)
                       (is (= 2 generation-id))
                       (is (= ::listener-pool listener-pool))
                       (is (= old-generation active))
                       (is (nil? memory-ticket-service))
                       candidate)
                     #'ol.busker.runtime/begin-drain!
                     (fn [server-handle generation]
                       (swap! events conj :begin-drain)
                       (is (= candidate (:active @(:busker/state server-handle))))
                       (is (= (:config candidate) (:config @(:busker/state server-handle))))
                       (is (= old-generation generation))
                       (assoc generation
                              :drain-future
                              (future :ok)))}
      (fn []
        (is (= :activated
               (runtime/reload! server ::next-config {:force? true})))
        (is (= [:candidate-built :begin-drain] @events))
        (is (= candidate (:active @state-atom)))
        (is (= [1] (mapv :generation-id (:draining @state-atom))))))))

(deftest begin-drain-waits-for-stop-accepting-before-background-stop-test
  (let [events (atom [])
        stop-accepting-release (promise)
        server {:busker/lifecycle-gate (Object.)
                :busker/state (atom {:phase :running
                                     :draining []})}
        generation {:generation-id 1
                    :cert-automation nil
                    :instance ::old-instance}]
    (with-redefs-fn {#'ol.busker.generation/begin-stop!
                     (fn [instance]
                       (is (= ::old-instance instance))
                       (swap! events conj :begin-stop)
                       instance)
                     #'ol.busker.generation/await-stop-accepting!
                     (fn [instance]
                       (is (= ::old-instance instance))
                       (swap! events conj :await-stop-accepting)
                       (deref stop-accepting-release 5000 true)
                       (swap! events conj :stopped-accepting)
                       nil)
                     #'ol.busker.generation/stop!
                     (fn [instance]
                       (is (= ::old-instance instance))
                       (swap! events conj :stop)
                       nil)
                     #'ol.busker.runtime/finalize-draining-generation!
                     (fn [_ _]
                       (swap! events conj :finalize)
                       nil)}
      (fn []
        (let [drain-fut (future (#'runtime/begin-drain! server generation))]
          (is (= ::timeout (deref drain-fut 200 ::timeout)))
          (is (= [:begin-stop :await-stop-accepting]
                 @events))
          (deliver stop-accepting-release true)
          (let [draining-generation (deref drain-fut 1000 ::timeout)]
            (is (map? draining-generation))
            (is (contains? draining-generation :drain-future))
            @(:drain-future draining-generation)
            (is (= [:begin-stop
                    :await-stop-accepting
                    :stopped-accepting
                    :stop
                    :finalize]
                   @events))))))))

(deftest reload-activates-new-generation-and-drains-old-test
  (let [port (util/free-port)
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
                          (eventually-curl :http nil port "/" :max-time 10))]
        (is (deref old-entered 5000 false))
        (is (= :activated
               (runtime/reload! server
                                (response-config port
                                                 (fn [_]
                                                   {:status 200
                                                    :body "new-generation"}))
                                {:force? true})))
        (let [state @(:busker/state server)
              active-executor (::generation/executor (:instance (:active state)))
              draining-executor (::generation/executor
                                 (:instance (first (:draining state))))]
          (is (= {:distinct? true
                  :draining-shutdown? true
                  :active-shutdown? false}
                 {:distinct? (not (identical? active-executor draining-executor))
                  :draining-shutdown? (.isShutdown ^ExecutorService draining-executor)
                  :active-shutdown? (.isShutdown ^ExecutorService active-executor)}))
          (let [new-request (util/curl :http nil port "/" :max-time 5)]
            (is (= 0 (:exit new-request)))
            (is (= "new-generation" (:out new-request)))))
        (deliver old-release true)
        (let [result (deref old-request 10000 nil)]
          (is (some? result))
          (when result
            (is (= 0 (:exit result)))
            (is (= "old-generation" (:out result))))))
      (finally
        (runtime/stop! server)))))

(deftest memory-session-tickets-survive-reload-test
  (let [port (util/free-port)
        config-a (util/with-static-tls
                   (assoc (response-config port
                                           (fn [_]
                                             {:status 200
                                              :body "generation-a"}))
                          :tls {:session-tickets {:persistence :memory}}
                          :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                                :http3? false
                                                :tls {:tls-compatibility-mode
                                                      :modern}}}))
        config-b (util/with-static-tls
                   (assoc (response-config port
                                           (fn [_]
                                             {:status 200
                                              :body "generation-b"}))
                          :tls {:session-tickets {:persistence :memory}}
                          :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                                :http3? false
                                                :tls {:tls-compatibility-mode
                                                      :modern}}}))
        server (runtime/start! config-a)]
    (try
      (let [ready (util/wait-for-curl-ready! :https :h1 port "/" :max-time 2)]
        (is (= 0 (:exit ready))))
      (let [keys-a (active-generation-keys-edn server)]
        (is (seq keys-a))
        (is (= :activated
               (runtime/reload! server config-b {:force? true})))
        (let [ready (util/wait-for-curl-ready! :https :h1 port "/" :max-time 2)]
          (is (= 0 (:exit ready))))
        (is (= keys-a
               (active-generation-keys-edn server))))
      (finally
        (runtime/stop! server)))))

(deftest memory-session-tickets-not-reused-after-disabled-reload-test
  (let [port (util/free-port)
        memory-config (util/with-static-tls
                        (assoc (response-config port
                                                (fn [_]
                                                  {:status 200
                                                   :body "memory"}))
                               :tls {:session-tickets {:persistence :memory}}
                               :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                                     :http3? false
                                                     :tls {:tls-compatibility-mode
                                                           :modern}}}))
        disabled-config (util/with-static-tls
                          (assoc (response-config port
                                                  (fn [_]
                                                    {:status 200
                                                     :body "disabled"}))
                                 :tls {:session-tickets {:disabled? true}}
                                 :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                                       :http3? false
                                                       :tls {:tls-compatibility-mode
                                                             :modern}}}))
        server (runtime/start! memory-config)]
    (try
      (let [ready (util/wait-for-curl-ready! :https :h1 port "/" :max-time 2)]
        (is (= 0 (:exit ready))))
      (is (seq (active-generation-keys-edn server)))
      (is (= :activated
             (runtime/reload! server disabled-config {:force? true})))
      (let [ready (util/wait-for-curl-ready! :https :h1 port "/" :max-time 2)]
        (is (= 0 (:exit ready))))
      (is (nil? (::generation/key-manager (active-generation-instance server))))
      (finally
        (runtime/stop! server)))))

(deftest memory-session-ticket-service-is-shared-during-reload-overlap-test
  (let [port (util/free-port)
        old-entered (promise)
        old-release (promise)
        config-a (util/with-static-tls
                   {:tls {:session-tickets {:persistence :memory}}
                    :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                          :http3? false
                                          :tls {:tls-compatibility-mode :modern}}}
                    :dispatch [{:handler (fn [_]
                                           (deliver old-entered true)
                                           (deref old-release 5000 true)
                                           {:status 200
                                            :body "old"})}]})
        config-b (util/with-static-tls
                   {:tls {:session-tickets {:persistence :memory}}
                    :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                          :http3? false
                                          :tls {:tls-compatibility-mode :modern}}}
                    :dispatch [{:handler (fn [_]
                                           {:status 200
                                            :body "new"})}]})
        server (runtime/start! config-a)]
    (try
      (let [old-request (future
                          (util/curl :https :h1 port "/" :max-time 10))]
        (is (deref old-entered 5000 false))
        (is (= :activated
               (runtime/reload! server config-b {:force? true})))
        (let [state @(:busker/state server)
              active-service (generation-shared-ticket-service (:active state))
              draining-service (generation-shared-ticket-service
                                (first (:draining state)))]
          (is (= 1 (count (:draining state))))
          (is (some? active-service))
          (is (identical? active-service draining-service)))
        (deliver old-release true)
        (let [result (deref old-request 10000 nil)]
          (is (some? result))))
      (finally
        (runtime/stop! server)))))

(deftest memory-session-ticket-policy-change-replaces-service-but-preserves-keys-test
  (let [port (util/free-port)
        config-a (util/with-static-tls
                   (assoc (response-config port
                                           (fn [_]
                                             {:status 200
                                              :body "generation-a"}))
                          :tls {:session-tickets {:persistence :memory
                                                  :lifetime-seconds 86400
                                                  :max-keys 4}}
                          :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                                :http3? false
                                                :tls {:tls-compatibility-mode
                                                      :modern}}}))
        config-b (util/with-static-tls
                   (assoc (response-config port
                                           (fn [_]
                                             {:status 200
                                              :body "generation-b"}))
                          :tls {:session-tickets {:persistence :memory
                                                  :lifetime-seconds 120
                                                  :max-keys 2}}
                          :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                                :http3? false
                                                :tls {:tls-compatibility-mode
                                                      :modern}}}))
        server (runtime/start! config-a)]
    (try
      (let [ready (util/wait-for-curl-ready! :https :h1 port "/" :max-time 2)]
        (is (= 0 (:exit ready))))
      (let [service-a (generation-shared-ticket-service (:active @(:busker/state server)))
            keys-a (active-generation-keys-edn server)]
        (is (some? service-a))
        (is (= 86400000 (:lifetime-ms service-a)))
        (is (= 4 (:max-keys service-a)))
        (is (seq keys-a))
        (is (= :activated
               (runtime/reload! server config-b {:force? true})))
        (let [ready (util/wait-for-curl-ready! :https :h1 port "/" :max-time 2)
              service-b (generation-shared-ticket-service (:active @(:busker/state server)))
              snapshot-b @(:snapshot-atom service-b)]
          (is (= 0 (:exit ready)))
          (is (some? service-b))
          (is (not (identical? service-a service-b)))
          (is (= keys-a
                 (active-generation-keys-edn server)))
          (is (= 120000 (:lifetime-ms service-b)))
          (is (= 2 (:max-keys service-b)))
          (is (= (+ (:last-rotation-ms snapshot-b)
                    (quot 120000 4))
                 (:next-rotation-ms snapshot-b)))))
      (finally
        (runtime/stop! server)))))

(deftest failed-reload-does-not-leak-shared-ticket-native-manager-test
  (let [port (util/free-port)
        config-a (util/with-static-tls
                   (assoc (response-config port
                                           (fn [_]
                                             {:status 200
                                              :body "generation-a"}))
                          :tls {:session-tickets {:persistence :memory}}
                          :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                                :http3? false
                                                :tls {:tls-compatibility-mode
                                                      :modern}}}))
        config-b (util/with-static-tls
                   (assoc (response-config port
                                           (fn [_]
                                             {:status 200
                                              :body "generation-b"}))
                          :tls {:session-tickets {:persistence :memory}}
                          :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                                :http3? false
                                                :tls {:tls-compatibility-mode
                                                      :modern}}}))
        server (runtime/start! config-a)]
    (try
      (let [ready (util/wait-for-curl-ready! :https :h1 port "/" :max-time 2)]
        (is (= 0 (:exit ready))))
      (let [service (generation-shared-ticket-service (:active @(:busker/state server)))]
        (is (some? service))
        (is (= 1 (shared-ticket-manager-count service)))
        (with-redefs [ol.busker.generation/init-worker-state
                      (fn [_]
                        (throw (ex-info "simulated worker init failure" {})))]
          (try
            (runtime/reload! server config-b {:force? true})
            (is false "Reload should fail")
            (catch clojure.lang.ExceptionInfo e
              (is (= :reload-failed (:reason (ex-data e)))))))
        (is (identical? service
                        (generation-shared-ticket-service
                         (:active @(:busker/state server)))))
        (is (= 1 (shared-ticket-manager-count service)))
        (let [request (util/curl :https :h1 port "/" :max-time 5)]
          (is (= 0 (:exit request)))
          (is (= "generation-a" (:out request)))))
      (finally
        (runtime/stop! server)))))

(deftest memory-service-is-not-reused-after-mode-change-during-overlap-test
  (let [port (util/free-port)
        old-entered (promise)
        old-release (promise)
        config-a (util/with-static-tls
                   {:tls {:session-tickets {:persistence :memory}}
                    :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                          :http3? false
                                          :tls {:tls-compatibility-mode :modern}}}
                    :dispatch [{:handler (fn [_]
                                           (deliver old-entered true)
                                           (deref old-release 5000 true)
                                           {:status 200
                                            :body "memory-a"})}]})
        config-b (util/with-static-tls
                   {:tls {:session-tickets {:disabled? true}}
                    :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                          :http3? false
                                          :tls {:tls-compatibility-mode :modern}}}
                    :dispatch [{:handler (fn [_]
                                           {:status 200
                                            :body "disabled"})}]})
        config-c (util/with-static-tls
                   {:tls {:session-tickets {:persistence :memory}}
                    :entrypoints {:https {:bind (str "127.0.0.1:" port)
                                          :http3? false
                                          :tls {:tls-compatibility-mode :modern}}}
                    :dispatch [{:handler (fn [_]
                                           {:status 200
                                            :body "memory-c"})}]})
        server (runtime/start! config-a)]
    (try
      (let [old-request (future
                          (util/curl :https :h1 port "/" :max-time 10))]
        (is (deref old-entered 5000 false))
        (let [original-service (generation-shared-ticket-service
                                (:active @(:busker/state server)))]
          (is (some? original-service))
          (is (= :activated
                 (runtime/reload! server config-b {:force? true})))
          (is (= :activated
                 (runtime/reload! server config-c {:force? true})))
          (let [state @(:busker/state server)
                active-service (generation-shared-ticket-service (:active state))]
            (is (some? active-service))
            (is (not (identical? original-service active-service)))))
        (deliver old-release true)
        (let [result (deref old-request 10000 nil)]
          (is (some? result))
          (when result
            (is (= 0 (:exit result)))
            (is (= "memory-a" (:out result))))))
      (finally
        (runtime/stop! server)))))

(deftest http3-reload-activates-new-generation-and-drains-old-connections-test
  (let [port (util/free-port)
        old-first-chunk (promise)
        old-release (promise)
        old-body "old-generation-stream-complete"
        server (runtime/start!
                (assoc
                 (tls-response-config port
                                      (fn [{emitter :ol.busker.request/emitter}]
                                        (future
                                          (proto/emit! emitter
                                                       {:status 200
                                                        :headers {"content-type" "text/plain"}})
                                          (proto/emit! emitter "old-generation-")
                                          (proto/flush emitter)
                                          (deliver old-first-chunk true)
                                          (deref old-release 5000 true)
                                          (proto/emit! emitter "stream-complete")
                                          (proto/close emitter))
                                        {:body emitter}))
                 :n-workers 2))]
    (try
      (let [old-request (future
                          (util/curl :https :h3 port "/" :max-time 10))]
        (is (deref old-first-chunk 5000 false))
        (is (= :activated
               (runtime/reload! server
                                (assoc
                                 (tls-response-config port
                                                      (fn [_]
                                                        {:status 200
                                                         :body "new-generation"}))
                                 :n-workers 2)
                                {:force? true})))
        (let [new-requests (mapv (fn [_]
                                   (util/curl :https :h3 port "/" :max-time 5))
                                 (range 20))]
          (doseq [[idx new-request] (map-indexed vector new-requests)]
            (is (= 0 (:exit new-request))
                (str "HTTP/3 request after reload should succeed for request "
                     idx ". stderr: " (:err new-request)))
            (is (= "new-generation" (:out new-request)))))
        (deliver old-release true)
        (let [result (deref old-request 10000 nil)]
          (is (some? result))
          (when result
            (is (= 0 (:exit result))
                (str "Draining HTTP/3 request should succeed. stderr: " (:err result)))
            (is (= old-body (:out result))))))
      (finally
        (runtime/stop! server)))))

(deftest http3-reload-with-worker-topology-change-reuses-transport-and-routes-to-active-generation-test
  (let [port (util/free-port)
        old-first-chunk (promise)
        old-release (promise)
        old-body "old-generation-stream-complete"
        server (runtime/start!
                (assoc
                 (tls-response-config port
                                      (fn [{emitter :ol.busker.request/emitter}]
                                        (future
                                          (proto/emit! emitter
                                                       {:status 200
                                                        :headers {"content-type" "text/plain"}})
                                          (proto/emit! emitter "old-generation-")
                                          (proto/flush emitter)
                                          (deliver old-first-chunk true)
                                          (deref old-release 5000 true)
                                          (proto/emit! emitter "stream-complete")
                                          (proto/close emitter))
                                        {:body emitter}))
                 :n-workers 2))]
    (try
      (let [old-request (future
                          (util/curl :https :h3 port "/" :max-time 10))]
        (is (deref old-first-chunk 5000 false))
        (is (= :activated
               (runtime/reload! server
                                (assoc
                                 (tls-response-config port
                                                      (fn [_]
                                                        {:status 200
                                                         :body "new-generation"}))
                                 :n-workers 1)
                                {:force? true})))
        (let [state @(:busker/state server)
              active-transport (udp-transport-resource (:instance (:active state)))
              draining-transport
              (udp-transport-resource (:instance (first (:draining state))))]
          (is (some? active-transport))
          (is (some? draining-transport))
          (is (identical? active-transport draining-transport))
          (is (= 1 (count (:draining state)))))
        (let [new-requests (mapv (fn [_]
                                   (util/curl :https :h3 port "/" :max-time 5))
                                 (range 20))]
          (doseq [[idx new-request] (map-indexed vector new-requests)]
            (is (= 0 (:exit new-request))
                (str "HTTP/3 request after topology-changing reload should succeed for request "
                     idx ". stderr: " (:err new-request)))
            (is (= "new-generation" (:out new-request)))))
        (deliver old-release true)
        (let [result (deref old-request 10000 nil)]
          (is (some? result))
          (when result
            (is (= 0 (:exit result))
                (str "Draining HTTP/3 request should succeed. stderr: " (:err result)))
            (is (= old-body (:out result))))))
      (finally
        (runtime/stop! server)))))

(deftest http3-reload-with-worker-topology-increase-reuses-transport-and-routes-to-active-generation-test
  (let [port (util/free-port)
        old-first-chunk (promise)
        old-release (promise)
        old-body "old-generation-stream-complete"
        server (runtime/start!
                (assoc
                 (tls-response-config port
                                      (fn [{emitter :ol.busker.request/emitter}]
                                        (future
                                          (proto/emit! emitter
                                                       {:status 200
                                                        :headers {"content-type" "text/plain"}})
                                          (proto/emit! emitter "old-generation-")
                                          (proto/flush emitter)
                                          (deliver old-first-chunk true)
                                          (deref old-release 5000 true)
                                          (proto/emit! emitter "stream-complete")
                                          (proto/close emitter))
                                        {:body emitter}))
                 :n-workers 1))]
    (try
      (let [old-request (future
                          (util/curl :https :h3 port "/" :max-time 10))]
        (is (deref old-first-chunk 5000 false))
        (is (= :activated
               (runtime/reload! server
                                (assoc
                                 (tls-response-config port
                                                      (fn [_]
                                                        {:status 200
                                                         :body "new-generation"}))
                                 :n-workers 2)
                                {:force? true})))
        (let [state @(:busker/state server)
              active-transport (udp-transport-resource (:instance (:active state)))
              draining-transport
              (udp-transport-resource (:instance (first (:draining state))))]
          (is (some? active-transport))
          (is (some? draining-transport))
          (is (identical? active-transport draining-transport))
          (is (= 1 (count (:draining state)))))
        (let [new-requests (mapv (fn [_]
                                   (util/curl :https :h3 port "/" :max-time 5))
                                 (range 20))]
          (doseq [[idx new-request] (map-indexed vector new-requests)]
            (is (= 0 (:exit new-request))
                (str "HTTP/3 request after topology-increasing reload should succeed for request "
                     idx ". stderr: " (:err new-request)))
            (is (= "new-generation" (:out new-request)))))
        (deliver old-release true)
        (let [result (deref old-request 10000 nil)]
          (is (some? result))
          (when result
            (is (= 0 (:exit result))
                (str "Draining HTTP/3 request should succeed. stderr: " (:err result)))
            (is (= old-body (:out result))))))
      (finally
        (runtime/stop! server)))))

(deftest reload-failure-keeps-current-generation-active-test
  (let [port (util/free-port)
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
      (let [result (eventually-curl :http nil port "/" :max-time 5)]
        (is (= 0 (:exit result)))
        (is (= "still-active" (:out result))))
      (finally
        (runtime/stop! server)))))

(deftest reload-reports-listener-acquisition-failure-stage-test
  (let [port (util/free-port)
        server (runtime/start!
                (response-config port
                                 (fn [_]
                                   {:status 200
                                    :body "still-listening"})))]
    (try
      (with-redefs [listen/acquire-claim
                    (fn [_ listener]
                      (throw (ex-info "claim failed"
                                      {:listener-key (listen/listener-key listener)})))]
        (try
          (runtime/reload! server
                           (response-config port
                                            (fn [_]
                                              {:status 200
                                               :body "never-starts"}))
                           {:force? true})
          (is false)
          (catch clojure.lang.ExceptionInfo e
            (is (= {:reason :reload-failed
                    :stage :listener-acquisition
                    :listener-key {:transport :tcp
                                   :host "127.0.0.1"
                                   :port port}}
                   (select-keys (ex-data e)
                                [:reason :stage :listener-key]))))))
      (let [result (util/curl :http nil port "/" :max-time 5)]
        (is (= 0 (:exit result)))
        (is (= "still-listening" (:out result))))
      (finally
        (runtime/stop! server)))))

(deftest reload-reports-tls-startup-failure-stage-test
  (let [port (util/free-port)
        server (runtime/start!
                (response-config port
                                 (fn [_]
                                   {:status 200
                                    :body "tls-old"})))]
    (try
      (with-redefs-fn {#'ol.busker.native/create-ssl-ctx
                       (fn [& _]
                         mem/null)}
        (fn []
          (try
            (runtime/reload! server
                             (tls-response-config port
                                                  (fn [_]
                                                    {:status 200
                                                     :body "tls-new"}))
                             {:force? true})
            (is false)
            (catch clojure.lang.ExceptionInfo e
              (is (= {:reason :reload-failed
                      :stage :tls-startup
                      :listener {:entrypoint :https
                                 :host "127.0.0.1"
                                 :port port}}
                     (select-keys (ex-data e)
                                  [:reason :stage :listener])))))))
      (let [result (util/curl :http nil port "/" :max-time 5)]
        (is (= 0 (:exit result)))
        (is (= "tls-old" (:out result))))
      (finally
        (runtime/stop! server)))))

(deftest config-validation-precedes-request-executor-creation-test
  (let [port (util/free-port)
        executor-created? (atom false)]
    (with-redefs-fn {#'ol.busker.generation/new-request-executor
                     (fn []
                       (reset! executor-created? true)
                       (throw (ex-info "request executor should not be created" {})))}
      (fn []
        (try
          (runtime/start!
           (assoc (response-config port (constantly {:status 200}))
                  :n-workers 0))
          (is false)
          (catch clojure.lang.ExceptionInfo e
            (is (= [::config/config-spec-invalid]
                   (mapv :error (:errors (ex-data e)))))))
        (is (false? @executor-created?))))))

(deftest failed-startup-shuts-down-request-executor-test
  (let [port (util/free-port)
        request-executor_ (atom nil)
        new-request-executor @#'ol.busker.generation/new-request-executor]
    (with-redefs-fn {#'ol.busker.native/create-ssl-ctx
                     (fn [& _]
                       mem/null)
                     #'ol.busker.generation/new-request-executor
                     (fn []
                       (let [executor (new-request-executor)]
                         (reset! request-executor_ executor)
                         executor))}
      (fn []
        (try
          (runtime/start!
           (tls-response-config port (constantly {:status 200})))
          (is false)
          (catch clojure.lang.ExceptionInfo e
            (is (= :tls-startup (:stage (ex-data e))))))
        (is (.isShutdown ^ExecutorService @request-executor_))))))

(deftest reload-activates-while-managed-certificates-converge-test
  (let [port (util/free-port)
        queue (LinkedBlockingQueue.)
        server (runtime/start!
                (response-config port
                                 (fn [_]
                                   {:status 200
                                    :body "before-managed"})))]
    (try
      (with-redefs [clave-adapter/initial-cert-wait-timeout-ms 50
                    clave-adapter/event-poll-timeout-ms 10
                    http-solver/solver (fn [] {:registry (atom {})})
                    automation/create (fn [_] {:id ::system})
                    automation/start identity
                    automation/manage-domains (fn [_ _] nil)
                    automation/subscribe-events (fn [_] queue)
                    automation/unsubscribe-events (fn [_ _] nil)
                    automation/lookup-cert (fn [_ _] nil)
                    automation/stop (fn [_] nil)]
        (is (= :activated
               (runtime/reload! server
                                (managed-http-config port
                                                     (fn [_]
                                                       {:status 200
                                                        :body "managed-active"}))
                                {:force? true})))
        (let [result (eventually-curl :http nil port "/" :max-time 5)]
          (is (= 0 (:exit result)))
          (is (= "managed-active" (:out result)))))
      (finally
        (with-redefs [automation/unsubscribe-events (fn [_ _] nil)
                      automation/stop (fn [_] nil)]
          (runtime/stop! server))))))

(deftest reload-calls-serialize-through-lifecycle-gate-test
  (let [port (util/free-port)
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
  (let [port (util/free-port)
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
                            (eventually-curl :http nil port "/" :max-time 10)
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
                              (eventually-curl :http nil port "/" :max-time 10)
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

(deftest repeated-reloads-retain-multiple-draining-generations-test
  (let [port (util/free-port)
        old-entered (promise)
        old-release (promise)
        mid-entered (promise)
        mid-release (promise)
        server (runtime/start!
                (response-config port
                                 (fn [_]
                                   (deliver old-entered true)
                                   (deref old-release 5000 true)
                                   {:status 200
                                    :body "old"})))]
    (try
      (let [old-request (future
                          (eventually-curl :http nil port "/" :max-time 10))]
        (is (deref old-entered 5000 false))
        (is (= :activated
               (runtime/reload! server
                                (response-config port
                                                 (fn [_]
                                                   (deliver mid-entered true)
                                                   (deref mid-release 5000 true)
                                                   {:status 200
                                                    :body "mid"}))
                                {:force? true})))
        (let [mid-request (future
                            (eventually-curl :http nil port "/" :max-time 10))]
          (is (deref mid-entered 10000 false))
          (is (= :activated
                 (runtime/reload! server
                                  (response-config port
                                                   (fn [_]
                                                     {:status 200
                                                      :body "new"}))
                                  {:force? true})))
          (is (= 2 (count (:draining @(:busker/state server)))))
          (let [new-request (eventually-curl :http nil port "/" :max-time 5)]
            (is (= 0 (:exit new-request)))
            (is (= "new" (:out new-request))))
          (deliver old-release true)
          (deliver mid-release true)
          (let [old-result (deref old-request 10000 nil)
                mid-result (deref mid-request 10000 nil)]
            (is (= 0 (:exit old-result)))
            (is (= 0 (:exit mid-result)))
            (is (= "old" (:out old-result)))
            (is (= "mid" (:out mid-result))))))
      (finally
        (runtime/stop! server)))))
