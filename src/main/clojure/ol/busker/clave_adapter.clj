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

(declare lookup-certificate)

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

(defn- pending-subject-names
  [system subject-names]
  (reduce (fn [pending subject-name]
            (if (automation/lookup-cert system subject-name)
              pending
              (conj pending subject-name)))
          #{}
          subject-names))

(defn- certificate-failed-event?
  [event pending]
  (and (= :certificate-failed (:type event))
       (contains? pending (get-in event [:data :domain]))))

(defn- wait-for-initial-certificates!
  [system subject-names]
  (when (seq subject-names)
    (let [queue (automation/get-event-queue system)
          deadline-ms (+ (System/currentTimeMillis) initial-cert-wait-timeout-ms)]
      (loop [pending (pending-subject-names system subject-names)]
        (when (seq pending)
          (let [now-ms (System/currentTimeMillis)
                remaining-ms (- deadline-ms now-ms)]
            (when (<= remaining-ms 0)
              (throw
               (ex-info "Timed out waiting for initial certificates"
                        {:pending-subject-names (vec (sort pending))
                         :timeout-ms initial-cert-wait-timeout-ms})))
            (let [poll-ms (long (max 1 (min event-poll-timeout-ms remaining-ms)))
                  event (.poll queue poll-ms TimeUnit/MILLISECONDS)]
              (when (certificate-failed-event? event pending)
                (throw
                 (ex-info "Initial certificate obtain failed"
                          {:event event
                           :pending-subject-names (vec (sort pending))}))))
            (recur (pending-subject-names system pending))))))))

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
        (automation/start! system)
        (automation/manage-domains system subject-names)
        (wait-for-initial-certificates! system subject-names)
        (let [runtime {:system system
                       :subject-names subject-names
                       :http-solver http01-solver}]
          (assoc runtime :lookup-fn
                 (fn [hostname]
                   (lookup-certificate runtime hostname))))
        (catch Throwable t
          (automation/stop system)
          (throw t))))))

(defn wrap-handler
  [handler runtime]
  (if-let [solver (:http-solver runtime)]
    (http-solver/wrap-acme-challenge handler solver)
    handler))

(defn lookup-certificate
  [runtime hostname]
  (if-let [system (:system runtime)]
    (let [bundle (automation/lookup-cert system hostname)]
      (trove/log! {:level :debug
                   :id (if bundle ::lookup-hit ::lookup-miss)
                   :data {:hostname hostname}})
      bundle)
    nil))

(defn stop!
  [runtime]
  (when-let [system (:system runtime)]
    (automation/stop system))
  nil)
