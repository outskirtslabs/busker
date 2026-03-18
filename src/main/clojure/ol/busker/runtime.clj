(ns ol.busker.runtime
  "Multi-generation runtime lifecycle for Busker."
  (:require
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.config :as config]
   [ol.busker.generation :as generation]
   [ol.busker.listen :as listen]))

(set! *warn-on-reflection* true)

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
  [user-config generation-id listener-pool current-generation]
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
        {:keys [runtime reused?]}
        (try
          (acquire-cert-automation! current-cert-runtime managed-plan)
          (catch Throwable t
            (wrap-stage-error "Failed to start certificate automation"
                              :automation-startup
                              t)))]
    (try
      {:generation-id generation-id
       :config snapshot
       :managed-plan managed-plan
       :cert-automation runtime
       :instance (generation/start! compiled-config
                                    runtime
                                    {:listener-pool listener-pool})}
      (catch Throwable t
        (when (and runtime (not reused?))
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

(defn- finalize-draining-generation!
  [server-handle generation]
  (let [state-atom (:busker/state server-handle)
        gate (:busker/lifecycle-gate server-handle)
        cert-runtime (:cert-automation generation)
        stop-cert-runtime?
        (locking gate
          (let [state @state-atom
                next-draining (->> (:draining state)
                                   (remove #(= (:generation-id generation)
                                               (:generation-id %)))
                                   vec)
                next-state (assoc state :draining next-draining)]
            (reset! state-atom next-state)
            (and (= :running (:phase state))
                 cert-runtime
                 (not (cert-runtime-in-use? next-state cert-runtime)))))]
    (when stop-cert-runtime?
      (clave-adapter/stop! cert-runtime))
    nil))

(defn- begin-drain!
  [server-handle generation]
  (generation/begin-stop! (:instance generation))
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

(defn start!
  "Start a new Busker server from `user-config`."
  [user-config]
  (let [listener-pool (listen/open-pool)
        generation (build-generation! user-config 1 listener-pool nil)]
    {:busker/lifecycle-gate (Object.)
     :busker/state
     (atom {:phase :running
            :config (:config generation)
            :next-generation-id 2
            :listener-pool listener-pool
            :active generation
            :draining []})}))

(defn reload!
  "Compile and activate a new runtime snapshot for `server-handle`."
  ([server-handle user-config]
   (reload! server-handle user-config {}))
  ([server-handle user-config opts]
   (locking (:busker/lifecycle-gate server-handle)
     (let [state-atom (:busker/state server-handle)
           {:keys [phase active next-generation-id listener-pool draining]}
           @state-atom]
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
                                                active)
                   draining-generation (begin-drain! server-handle active)]
               (reset! state-atom
                       {:phase :running
                        :config (:config candidate)
                        :next-generation-id (inc next-generation-id)
                        :listener-pool listener-pool
                        :active candidate
                        :draining (conj draining draining-generation)})
               :activated)
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
  [server-handle]
  (let [{:keys [config generations-to-stop]}
        (locking (:busker/lifecycle-gate server-handle)
          (let [state-atom (:busker/state server-handle)
                {:keys [phase config active draining next-generation-id listener-pool]}
                @state-atom]
            (cond
              (= :stopped phase)
              {:config config
               :generations-to-stop nil}

              (= :stopping phase)
              {:config config
               :generations-to-stop draining}

              :else
              (let [draining (cond-> draining
                               active (conj (begin-drain! server-handle active)))]
                (reset! state-atom
                        {:phase :stopping
                         :config config
                         :next-generation-id next-generation-id
                         :listener-pool listener-pool
                         :active nil
                         :draining draining})
                {:config config
                 :generations-to-stop draining}))))]
    (when generations-to-stop
      (doseq [generation generations-to-stop]
        (await-drain! generation))
      (doseq [cert-runtime (->> generations-to-stop
                                (map :cert-automation)
                                (remove nil?)
                                distinct)]
        (clave-adapter/stop! cert-runtime))
      (locking (:busker/lifecycle-gate server-handle)
        (let [state-atom (:busker/state server-handle)
              {:keys [next-generation-id listener-pool]} @state-atom]
          (reset! state-atom
                  {:phase :stopped
                   :config config
                   :next-generation-id next-generation-id
                   :listener-pool listener-pool
                   :active nil
                   :draining []})))))
  nil)

(defn state
  "Return pure data describing the current runtime state."
  [server-handle]
  (let [{:keys [phase config]} @(:busker/state server-handle)]
    {:phase phase
     :config config}))
