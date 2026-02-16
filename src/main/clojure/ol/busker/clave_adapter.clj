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

(defn- managed-tls-entrypoint?
  [entrypoint]
  (let [tls (:tls entrypoint)]
    (and (map? tls)
         (seq (:issuers tls)))))

(defn- distinct-domains
  [domains]
  (->> domains
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
  (let [managed-entrypoints (->> (:entrypoints config)
                                 (filter managed-tls-entrypoint?)
                                 vec)]
    (when (seq managed-entrypoints)
      (let [tls-config (-> managed-entrypoints first :tls)]
        (assert-managed-plan!
         {:domains (distinct-domains (:domains config))
          :managed-entrypoints managed-entrypoints
          :clave-config (select-keys tls-config clave-config-keys)})))))

(defn- pending-domains
  [system domains]
  (reduce (fn [pending domain]
            (if (automation/lookup-cert system domain)
              pending
              (conj pending domain)))
          #{}
          domains))

(defn- certificate-failed-event?
  [event pending]
  (and (= :certificate-failed (:type event))
       (contains? pending (get-in event [:data :domain]))))

(defn- wait-for-initial-certificates!
  [system domains]
  (when (seq domains)
    (let [queue (automation/get-event-queue system)
          deadline-ms (+ (System/currentTimeMillis) initial-cert-wait-timeout-ms)]
      (loop [pending (pending-domains system domains)]
        (when (seq pending)
          (let [now-ms (System/currentTimeMillis)
                remaining-ms (- deadline-ms now-ms)]
            (when (<= remaining-ms 0)
              (throw
               (ex-info "Timed out waiting for initial certificates"
                        {:pending-domains (vec (sort pending))
                         :timeout-ms initial-cert-wait-timeout-ms})))
            (let [poll-ms (long (max 1 (min event-poll-timeout-ms remaining-ms)))
                  event (.poll queue poll-ms TimeUnit/MILLISECONDS)]
              (when (certificate-failed-event? event pending)
                (throw
                 (ex-info "Initial certificate obtain failed"
                          {:event event
                           :pending-domains (vec (sort pending))}))))
            (recur (pending-domains system pending))))))))

(defn start!
  [managed-plan]
  (when managed-plan
    (let [{:keys [domains clave-config]} (assert-managed-plan! managed-plan)
          http01-solver (http-solver/solver)
          clave-config (update clave-config :solvers
                               (fn [solvers]
                                 (assoc (or solvers {}) :http-01 http01-solver)))
          system (automation/create clave-config)]
      (try
        (automation/start! system)
        (automation/manage-domains system domains)
        (wait-for-initial-certificates! system domains)
        (let [runtime {:system system
                       :domains domains
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
