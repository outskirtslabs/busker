(ns ^:no-doc ol.busker.runtime
  "Multi-generation runtime lifecycle for Busker."
  (:require
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.config :as config]
   [ol.busker.generation :as generation]
   [ol.busker.listen :as listen]
   [ol.busker.tickets :as tickets]))

(set! *warn-on-reflection* true)

(defn- memory-session-tickets?
  [compiled-config]
  (let [session-ticket-config (get-in compiled-config [:tls :session-tickets])]
    (and (not (:disabled? session-ticket-config))
         (= :memory (:persistence session-ticket-config))
         (some (comp :tls val) (:entrypoints compiled-config)))))

(defn- memory-ticket-policy
  [compiled-config]
  (let [session-ticket-config (get-in compiled-config [:tls :session-tickets])]
    {:session-ticket-config session-ticket-config
     :lifetime-ms (* (get session-ticket-config
                          :lifetime-seconds
                          tickets/default-ticket-lifetime-seconds)
                     1000)
     :max-keys (get session-ticket-config
                    :max-keys
                    tickets/default-max-ticket-keys)}))

(defn- acquire-memory-ticket-service!
  [current-service compiled-config]
  (let [{:keys [session-ticket-config lifetime-ms max-keys]}
        (memory-ticket-policy compiled-config)]
    (cond
      (not (memory-session-tickets? compiled-config))
      {:service nil
       :reused? false}

      (and current-service
           (= lifetime-ms (:lifetime-ms current-service))
           (= max-keys (:max-keys current-service)))
      {:service current-service
       :reused? true}

      :else
      {:service (-> (if current-service
                      (tickets/recreate-memory-ticket-service current-service
                                                              session-ticket-config)
                      (tickets/create-memory-ticket-service session-ticket-config))
                    (tickets/start-key-service!))
       :reused? false})))

(defn- snapshot-listener-keys
  [snapshot]
  (->> (:listeners snapshot)
       (mapcat (fn [listener]
                 (cond-> [(listen/listener-key listener)]
                   (get-in listener [:tls :http3?])
                   (conj (listen/listener-key
                          (assoc listener :transport :udp))))))
       distinct
       (sort-by pr-str)
       vec))

(defn- listener-reuse-plan
  [current-snapshot next-snapshot]
  (let [current-listeners (set (snapshot-listener-keys current-snapshot))
        next-listeners (set (snapshot-listener-keys next-snapshot))]
    {:reuse (->> current-listeners
                 (filter next-listeners)
                 (sort-by pr-str)
                 vec)
     :acquire (->> next-listeners
                   (remove current-listeners)
                   (sort-by pr-str)
                   vec)
     :release (->> current-listeners
                   (remove next-listeners)
                   (sort-by pr-str)
                   vec)}))

(defn- automation-reuse-plan
  [current-managed-plan next-managed-plan]
  (cond
    (and (nil? current-managed-plan) (nil? next-managed-plan))
    {:action :none}

    (nil? current-managed-plan)
    {:action :start
     :managed-plan next-managed-plan}

    (nil? next-managed-plan)
    {:action :stop}

    (= current-managed-plan next-managed-plan)
    {:action :reuse
     :managed-plan next-managed-plan}

    :else
    {:action :replace
     :managed-plan next-managed-plan}))

(defn- candidate-plan
  [current-snapshot current-managed-plan user-config opts]
  (let [next-snapshot (config/normalized-snapshot user-config)
        next-managed-plan (clave-adapter/build-managed-plan next-snapshot)
        force? (true? (:force? opts))]
    (if (and (not force?)
             (= current-snapshot next-snapshot))
      {:action :unchanged
       :snapshot current-snapshot}
      {:action :activate
       :current-snapshot current-snapshot
       :next-snapshot next-snapshot
       :listener-plan (listener-reuse-plan current-snapshot next-snapshot)
       :automation-plan (automation-reuse-plan current-managed-plan
                                               next-managed-plan)
       :force? force?})))

(defn- acquire-cert-automation!
  [current-runtime managed-plan]
  (cond
    (nil? managed-plan)
    {:runtime nil
     :reused? false
     :superseded current-runtime}

    (= (:managed-plan current-runtime) managed-plan)
    {:runtime current-runtime
     :reused? true
     :superseded nil}

    :else
    {:runtime (clave-adapter/start! managed-plan)
     :reused? false
     :superseded current-runtime}))

(defn- wrap-stage-error
  [message stage t]
  (let [data (ex-data t)]
    (throw (ex-info message
                    (merge {:stage (or (:stage data) stage)}
                           data)
                    t))))

(defn- build-generation!
  [user-config generation-id listener-pool current-generation current-memory-ticket-service]
  (let [compiled-config
        (try
          (config/load! user-config)
          (catch Throwable t
            (wrap-stage-error "Failed to compile generation config"
                              :validation
                              t)))
        snapshot (config/normalized-snapshot user-config)
        managed-plan (clave-adapter/build-managed-plan snapshot)
        current-cert-runtime (:cert-automation current-generation)
        {:keys [runtime] :as cert-runtime-result}
        (try
          (acquire-cert-automation! current-cert-runtime managed-plan)
          (catch Throwable t
            (wrap-stage-error "Failed to start certificate automation"
                              :automation-startup
                              t)))
        {:keys [service] :as memory-ticket-service-result}
        (try
          (acquire-memory-ticket-service! current-memory-ticket-service
                                          compiled-config)
          (catch Throwable t
            (when (and runtime (not (:reused? cert-runtime-result)))
              (clave-adapter/stop! runtime))
            (wrap-stage-error "Failed to start in-memory session tickets"
                              :activation
                              t)))]
    (try
      {:generation-id generation-id
       :config snapshot
       :managed-plan managed-plan
       :cert-automation runtime
       :memory-ticket-service service
       :instance (generation/start! compiled-config
                                    runtime
                                    {:activate-http3-transports? (nil? current-generation)
                                     :generation-id generation-id
                                     :listener-pool listener-pool
                                     :memory-ticket-service service})}
      (catch Throwable t
        (when (and service (not (:reused? memory-ticket-service-result)))
          (tickets/stop-key-service! service))
        (when (and runtime (not (:reused? cert-runtime-result)))
          (clave-adapter/stop! runtime))
        (wrap-stage-error "Failed to start generation"
                          :activation
                          t)))))

(defn- current-generations
  [state]
  (cond-> []
    (:active state) (conj (:active state))
    (seq (:draining state)) (into (:draining state))))

(defn- cert-runtime-in-use?
  [state cert-runtime]
  (boolean
   (some #(identical? cert-runtime (:cert-automation %))
         (current-generations state))))

(defn- ticket-service-in-use?
  [state service]
  (boolean
   (some #(identical? service (:memory-ticket-service %))
         (current-generations state))))

(defn- finalize-draining-generation!
  [server-handle generation]
  (let [state-atom (:busker/state server-handle)
        gate (:busker/lifecycle-gate server-handle)
        cert-runtime (:cert-automation generation)
        ticket-service (:memory-ticket-service generation)
        [stop-cert-runtime? stop-ticket-service?]
        (locking gate
          (let [state @state-atom
                next-draining (->> (:draining state)
                                   (remove #(= (:generation-id generation)
                                               (:generation-id %)))
                                   vec)
                next-state (assoc state :draining next-draining)
                stop-ticket-service?
                (and ticket-service
                     (not (ticket-service-in-use? next-state ticket-service)))
                next-state (cond-> next-state
                             (and stop-ticket-service?
                                  (identical? ticket-service
                                              (:memory-ticket-service state)))
                             (assoc :memory-ticket-service nil))]
            (reset! state-atom next-state)
            [(and (= :running (:phase state))
                  cert-runtime
                  (not (cert-runtime-in-use? next-state cert-runtime)))
             stop-ticket-service?]))]
    (when stop-cert-runtime?
      (clave-adapter/stop! cert-runtime))
    (when stop-ticket-service?
      (tickets/stop-key-service! ticket-service))
    nil))

(defn- begin-drain!
  [server-handle generation]
  (generation/begin-stop! (:instance generation))
  (generation/await-stop-accepting! (:instance generation))
  (assoc generation
         :drain-future
         (future
           (let [result
                 (try
                   (generation/stop! (:instance generation))
                   :ok
                   (catch Throwable t
                     t)
                   (finally
                     (finalize-draining-generation! server-handle generation)))]
             result))))

(defn- await-drain!
  [generation]
  (let [result @(or (:drain-future generation)
                    (future
                      (generation/stop! (:instance generation))
                      :ok))]
    (when (instance? Throwable result)
      (throw result))
    nil))

(defn- cleanup-unpublished-generation!
  [generation current-cert-runtime current-memory-ticket-service]
  (when-let [instance (:instance generation)]
    (try
      (generation/stop! instance)
      (catch Throwable _
        nil)))
  (when-let [cert-runtime (:cert-automation generation)]
    (when (not (identical? cert-runtime current-cert-runtime))
      (try
        (clave-adapter/stop! cert-runtime)
        (catch Throwable _
          nil))))
  (when-let [ticket-service (:memory-ticket-service generation)]
    (when (not (identical? ticket-service current-memory-ticket-service))
      (try
        (tickets/stop-key-service! ticket-service)
        (catch Throwable _
          nil))))
  nil)

(defn start!
  "Start a new Busker server from `user-config`."
  [user-config]
  (let [listener-pool (listen/open-pool)
        generation (build-generation! user-config 1 listener-pool nil nil)]
    (try
      (let [generation (update generation :instance generation/register-shared-ticket-manager!)]
        {:busker/lifecycle-gate (Object.)
         :busker/state
         (atom {:phase :running
                :config (:config generation)
                :next-generation-id 2
                :listener-pool listener-pool
                :memory-ticket-service (:memory-ticket-service generation)
                :active generation
                :draining []})})
      (catch Throwable t
        (cleanup-unpublished-generation! generation nil nil)
        (throw t)))))

(defn reload!
  "Compile and activate a new runtime snapshot for `server-handle`."
  ([server-handle user-config]
   (reload! server-handle user-config {}))
  ([{:keys [busker/lifecycle-gate busker/state] :as server-handle} user-config opts]
   (locking lifecycle-gate
     (let [{:keys [phase active next-generation-id listener-pool draining
                   memory-ticket-service]} @state]
       (when (not= :running phase)
         (throw (ex-info "Server is stopping"
                         {:reason :server-stopping})))
       (let [plan (candidate-plan (:config active)
                                  (:managed-plan active)
                                  user-config
                                  opts)]
         (if (= :unchanged (:action plan))
           :unchanged
           (try
             (let [candidate (build-generation! user-config
                                                next-generation-id
                                                listener-pool
                                                active
                                                memory-ticket-service)]
               (try
                 (let [candidate (update candidate :instance generation/activate-http3-transports!)
                       candidate (update candidate :instance generation/register-shared-ticket-manager!)
                       _ (reset! state
                                 {:phase :running
                                  :config (:config candidate)
                                  :next-generation-id (inc next-generation-id)
                                  :listener-pool listener-pool
                                  :memory-ticket-service (:memory-ticket-service candidate)
                                  :active candidate
                                  :draining draining})
                       draining-generation (begin-drain! server-handle active)]
                   (swap! state update :draining conj draining-generation)
                   :activated)
                 (catch Throwable t
                   (cleanup-unpublished-generation! candidate
                                                    (:cert-automation active)
                                                    memory-ticket-service)
                   (throw t))))
             (catch Throwable t
               (let [data (ex-data t)]
                 (throw (ex-info "Reload failed"
                                 (merge {:reason :reload-failed
                                         :stage (or (:stage data) :activation)}
                                        data)
                                 t)))))))))))

(defn stop!
  "Synchronously stop `server-handle`, waiting for active and draining
  generations to quiesce."
  [{:keys [busker/lifecycle-gate busker/state] :as server-handle}]
  (when server-handle
    (let [{:keys [config generations-to-stop memory-ticket-service]}
          (locking lifecycle-gate
            (let [{:keys [phase config active draining next-generation-id listener-pool]} @state]
              (cond
                (= :stopped phase)
                {:config                config
                 :generations-to-stop   nil
                 :memory-ticket-service nil}

                (= :stopping phase)
                {:config                config
                 :generations-to-stop   draining
                 :memory-ticket-service (:memory-ticket-service @state)}

                :else
                (let [draining (cond-> draining
                                 active (conj (begin-drain! server-handle active)))]
                  (reset! state
                          {:phase                 :stopping
                           :config                config
                           :next-generation-id    next-generation-id
                           :listener-pool         listener-pool
                           :memory-ticket-service (:memory-ticket-service @state)
                           :active                nil
                           :draining              draining})
                  {:config                config
                   :generations-to-stop   draining
                   :memory-ticket-service (:memory-ticket-service @state)}))))]
      (when generations-to-stop
        (doseq [generation generations-to-stop]
          (await-drain! generation))
        (doseq [cert-runtime (->> generations-to-stop
                                  (map :cert-automation)
                                  (remove nil?)
                                  distinct)]
          (clave-adapter/stop! cert-runtime))
        (when memory-ticket-service
          (tickets/stop-key-service! memory-ticket-service))
        (locking lifecycle-gate
          (let [{:keys [next-generation-id listener-pool]} @state]
            (reset! state
                    {:phase                 :stopped
                     :config                config
                     :next-generation-id    next-generation-id
                     :listener-pool         listener-pool
                     :memory-ticket-service nil
                     :active                nil
                     :draining              []}))))))
  nil)

(defn state
  "Return pure data describing the current runtime state."
  [server-handle]
  (let [{:keys [phase config]} @(:busker/state server-handle)]
    {:phase phase
     :config config}))
