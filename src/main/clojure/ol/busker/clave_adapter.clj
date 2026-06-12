(ns ol.busker.clave-adapter
  (:require
   [clojure.spec.alpha :as s]
   [ol.busker.specs :as specs]
   [ol.clave.acme.solver.http :as http-solver]
   [ol.clave.automation :as automation]
   [taoensso.trove :as trove])
  (:import
   [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(def initial-cert-wait-timeout-ms
  120000)

(def event-poll-timeout-ms
  250)

(def ^:private clave-config-keys
  [:storage
   :issuers
   :issuer-selection
   :key-type
   :key-reuse
   :cache-capacity
   :solvers
   :ocsp
   :config-fn
   :http-client])

(defn- distinct-subject-names
  [subject-names]
  (->> subject-names
       (filter string?)
       distinct
       vec))

(defn- assert-managed-plan!
  [managed-plan]
  (if (s/valid? ::specs/managed-plan managed-plan)
    managed-plan
    (throw
     (ex-info "Invalid managed plan"
              {:managed-plan managed-plan
               :spec ::specs/managed-plan
               :explain (s/explain-data ::specs/managed-plan managed-plan)}))))

(defn build-managed-plan
  [config]
  (let [tls-config (:tls config)
        subject-names (distinct-subject-names
                       (get-in tls-config [:certificates :manage]))]
    (when (seq subject-names)
      (assert-managed-plan!
       {:subject-names subject-names
        :clave-config (select-keys tls-config clave-config-keys)}))))

(defn- certificate-failed-event?
  [event pending]
  (and (= :certificate-failed (:type event))
       (contains? pending (get-in event [:data :domain]))))

(defn- start-event-watcher!
  [system subject-names]
  (when (seq subject-names)
    (let [pending (set subject-names)]
      (try
        (let [queue (automation/get-event-queue system)
              running? (atom true)
              worker
              (future
                (try
                  (loop []
                    (when @running?
                      (when-let [event (.poll queue event-poll-timeout-ms
                                              TimeUnit/MILLISECONDS)]
                        (when (certificate-failed-event? event pending)
                          (trove/log! {:level :error
                                       :id ::certificate-obtain-failed
                                       :data {:event event
                                              :subject-name
                                              (get-in event [:data :domain])}})))
                      (recur)))
                  (catch InterruptedException _
                    nil)
                  (catch Throwable t
                    (trove/log! {:level :error
                                 :id ::certificate-event-watcher-failed
                                 :ex t}))))]
          {:running? running?
           :worker worker})
        (catch Throwable t
          (trove/log! {:level :error
                       :id ::certificate-event-watcher-start-failed
                       :ex t})
          nil)))))

(defn- stop-event-watcher!
  [watcher]
  (when watcher
    (reset! (:running? watcher) false)
    (future-cancel (:worker watcher)))
  nil)

(defn lookup-certificate
  [runtime hostname]
  (if-let [system (:system runtime)]
    (let [bundle (automation/lookup-cert system hostname)]
      (trove/log! {:level :debug
                   :id (if bundle ::lookup-hit ::lookup-miss)
                   :data {:hostname hostname}})
      bundle)
    nil))

(defn start!
  [managed-plan]
  (when managed-plan
    (let [{:keys [subject-names clave-config]}
          (assert-managed-plan! managed-plan)
          http01-solver (http-solver/solver)
          clave-config (update clave-config :solvers
                               (fn [solvers]
                                 (assoc (or solvers {})
                                        :http-01 http01-solver)))
          system (automation/create clave-config)]
      (try
        (automation/start system)
        (automation/manage-domains system subject-names)
        (let [runtime {:system system
                       :managed-plan managed-plan
                       :subject-names subject-names
                       :http-solver http01-solver
                       :event-watcher (start-event-watcher! system
                                                            subject-names)}]
          (assoc runtime :lookup-fn
                 (fn [hostname]
                   (lookup-certificate runtime hostname))))
        (catch Throwable t
          (stop-event-watcher! nil)
          (automation/stop system)
          (throw t))))))

(defn wrap-handler
  [handler runtime]
  (if-let [solver (:http-solver runtime)]
    (http-solver/wrap-acme-challenge handler solver)
    handler))

(defn stop!
  [runtime]
  (stop-event-watcher! (:event-watcher runtime))
  (when-let [system (:system runtime)]
    (automation/stop system))
  nil)
