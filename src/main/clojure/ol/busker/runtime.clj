(ns ol.busker.runtime
  "One-generation runtime lifecycle for Busker.

  This namespace owns the stable server handle used by the Phase 3 API.
  It starts exactly one runtime generation, exposes pure-data state, and
  synchronizes `stop!` through a lifecycle gate."
  (:require
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.config :as config]
   [ol.busker.listen :as listen]
   [ol.busker.server :as server]))

(set! *warn-on-reflection* true)

(defn- server-step
  [sym]
  (or (ns-resolve 'ol.busker.server sym)
      (throw (ex-info "Missing server startup step"
                      {:symbol sym}))))

(def ^:private init-tls-lookup-state-var
  (delay (server-step 'init-tls-lookup-state)))

(def ^:private init-core-state-var
  (delay (server-step 'init-core-state)))

(def ^:private init-listener-state-var
  (delay (server-step 'init-listener-state)))

(def ^:private init-tls-http3-state-var
  (delay (server-step 'init-tls-http3-state)))

(def ^:private init-worker-state-var
  (delay (server-step 'init-worker-state)))

(def ^:private finalize-server-state-var
  (delay (server-step 'finalize-server-state)))

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

(defn- prepare-generation-state
  [compiled-config cert-automation]
  (let [{:keys [n-workers max-connections executor]} compiled-config
        ring-handler (-> compiled-config
                         config/dispatch-handler
                         (clave-adapter/wrap-handler cert-automation))]
    {::server/ring-handler ring-handler
     ::server/config compiled-config
     ::server/clave-runtime cert-automation
     ::server/message-handler server/evloop-msg-processor
     ::server/shutting-down? (java.util.concurrent.atomic.AtomicBoolean. false)
     ::server/n-workers n-workers
     ::server/max-connections max-connections
     ::server/executor executor}))

(defn- start-generation!
  [user-config current-cert-automation]
  (let [compiled-config (config/load! user-config)
        snapshot (config/normalized-snapshot user-config)
        managed-plan (clave-adapter/build-managed-plan compiled-config)
        {:keys [runtime superseded]}
        (acquire-cert-automation! current-cert-automation managed-plan)
        init-tls-lookup-state @init-tls-lookup-state-var
        init-core-state @init-core-state-var
        init-listener-state @init-listener-state-var
        init-tls-http3-state @init-tls-http3-state-var
        init-worker-state @init-worker-state-var
        finalize-server-state @finalize-server-state-var]
    (try
      (let [prepared-state (prepare-generation-state compiled-config runtime)
            server-state (-> prepared-state
                             init-tls-lookup-state
                             init-core-state
                             init-listener-state
                             init-tls-http3-state
                             init-worker-state
                             finalize-server-state)]
        {:config snapshot
         :runtime {:generation-id 0
                   :server (dissoc server-state ::server/clave-runtime)
                   :cert-automation runtime}
         :superseded-cert-automation superseded})
      (catch Throwable t
        (when (and runtime
                   (not= runtime current-cert-automation))
          (clave-adapter/stop! runtime))
        (throw t)))))

(defn start!
  "Start one Busker runtime generation from `user-config`.

  Returns a stable opaque server handle with a lifecycle gate and published
  runtime state."
  [user-config]
  (let [{:keys [config runtime superseded-cert-automation]}
        (start-generation! user-config nil)]
    (when superseded-cert-automation
      (clave-adapter/stop! superseded-cert-automation))
    {:busker/lifecycle-gate (Object.)
     :busker/state
     (atom {:phase :running
            :config config
            :runtime runtime})}))

(defn stop!
  "Synchronously stop the runtime represented by `server-handle`.

  This function is idempotent.
  After it returns, [[state]] reports `:phase :stopped`."
  [server-handle]
  (locking (:busker/lifecycle-gate server-handle)
    (let [state-atom (:busker/state server-handle)
          {:keys [phase runtime config]} @state-atom]
      (when (= :running phase)
        (swap! state-atom assoc :phase :stopping)
        (server/stop-server (:server runtime))
        (clave-adapter/stop! (:cert-automation runtime))
        (reset! state-atom {:phase :stopped
                            :config config
                            :runtime nil}))))
  nil)

(defn state
  "Return pure data describing the current runtime state.

  The returned map is safe for callers to retain.
  It contains the current lifecycle `:phase` and the normalized config snapshot
  under `:config`."
  [server-handle]
  (let [{:keys [phase config]} @(:busker/state server-handle)]
    {:phase phase
     :config config}))
