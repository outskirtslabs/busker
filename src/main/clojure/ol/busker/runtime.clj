(ns ol.busker.runtime
  "One-generation runtime lifecycle for Busker.

  This namespace owns the stable server handle used by the Phase 3 API.
  It starts exactly one runtime generation, exposes pure-data state, and
  synchronizes `stop!` through a lifecycle gate."
  (:require
   [ol.busker.config :as config]
   [ol.busker.server :as server]))

(set! *warn-on-reflection* true)

(defn start!
  "Start one Busker runtime generation from `user-config`.

  Returns a stable opaque server handle with a lifecycle gate and published
  runtime state."
  [user-config]
  (let [generation-config (config/normalized-snapshot user-config)
        running-server (server/run-server user-config)]
    {:busker/lifecycle-gate (Object.)
     :busker/state
     (atom {:phase :running
            :config generation-config
            :runtime {:generation-id 0
                      :server running-server}})}))

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
