(ns ^:no-doc ol.busker.runtime
  "Coordinates the active Busker generation and process lifecycle.

  Key functions start a runtime, publish a new generation, drain the prior generation, and
  stop lifecycle services such as listeners and certificates.

  Start, reload and server stop run the pure core [[ol.busker.lifecycle.runtime]]:
  [[drive!]] performs each action of the core with a function of [[effects]]
  and gives the outcome back to the core.

  ## Related Namespaces

  - [[ol.busker.generation]] manages one native generation.
  - [[ol.busker.listen]] manages listener resources."
  (:require
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.config :as config]
   [ol.busker.generation :as generation]
   [ol.busker.lifecycle.runtime :as rt]
   [ol.busker.listen :as listen]
   [ol.busker.tickets :as tickets]
   [taoensso.trove :as trove])
  (:import
   [java.util.concurrent Callable ExecutionException ExecutorService Executors Future
    RejectedExecutionException ThreadFactory TimeUnit]
   [java.util.concurrent.atomic AtomicReference]))

(set! *warn-on-reflection* true)

(defn- new-lifecycle-executor
  []
  (Executors/newSingleThreadExecutor
   (reify ThreadFactory
     (newThread [_ task]
       (doto (Thread. task "busker-lifecycle")
         (.setDaemon true))))))

(defn- await-lifecycle
  [^Future result]
  (try
    (.get result)
    (catch InterruptedException e
      (.interrupt (Thread/currentThread))
      (throw e))
    (catch ExecutionException e
      (throw (.getCause e)))))

(defn- call-on-lifecycle
  [^ExecutorService executor f]
  (await-lifecycle
   (.submit executor
            ^Callable
            (reify Callable
              (call [_]
                (f))))))

(defn- memory-ticket-policy
  "Returns the policy of the in-memory ticket service that `compiled-config`
  needs, or nil when it needs none."
  [compiled-config]
  (let [{:keys [disabled? persistence] :as session-ticket-config}
        (get-in compiled-config [:tls :session-tickets])]
    (when (and (not disabled?)
               (= :memory persistence)
               (some (comp :tls val) (:entrypoints compiled-config)))
      {:lifetime-ms (* (get session-ticket-config
                            :lifetime-seconds
                            tickets/default-ticket-lifetime-seconds)
                       1000)
       :max-keys (get session-ticket-config :max-keys tickets/default-max-ticket-keys)})))

(defn- result!
  "Returns the value of the core result, or throws its error with the
  other errors added as suppressed."
  [{:keys [value error suppressed]}]
  (when error
    (doseq [t suppressed :when (not (identical? error t))]
      (.addSuppressed ^Throwable error ^Throwable t))
    (throw error))
  value)

;; The :retire effect starts a thread that drains the generation through the
;; same runner.
(declare drive!)

(def ^:private effects
  "Maps each action kind of the core to the function that performs it. Each
  function takes the context `{:state state-atom :user-config c
  :listener-pool p}` and the arguments of the action. It makes no lifecycle
  decision."
  {:compile-config
   (fn [{:keys [user-config]}]
     (let [compiled-config (config/load! user-config)
           snapshot (config/normalized-snapshot user-config)]
       {:compiled-config compiled-config
        :snapshot snapshot
        :cert-plan (clave-adapter/build-managed-plan snapshot)
        :ticket-policy (memory-ticket-policy compiled-config)}))

   :start-cert-runtime
   (fn [_ plan]
     (clave-adapter/start! plan))

   :start-ticket-service
   (fn [_ current compiled-config]
     (let [session-ticket-config (get-in compiled-config [:tls :session-tickets])]
       (tickets/start-key-service!
        (if current
          (tickets/recreate-memory-ticket-service current session-ticket-config)
          (tickets/create-memory-ticket-service session-ticket-config)))))

   :start-generation
   (fn [{:keys [listener-pool]}
        {:keys [generation-id cert-automation memory-ticket-service]}
        compiled-config]
     (generation/start! compiled-config
                        cert-automation
                        {:generation-id generation-id
                         :listener-pool listener-pool
                         :memory-ticket-service memory-ticket-service}))

   ;; HTTP/3 activation takes accept from every other generation and cannot be
   ;; undone, so it comes last.
   :activate
   (fn [_ {:keys [instance]}]
     (generation/register-shared-ticket-manager! instance)
     (generation/activate-http3-transports! instance))

   ;; The lifecycle thread and the drain threads write the state atom with the
   ;; pure functions of the core. Only the lifecycle thread changes :phase,
   ;; :active and :next-generation-id. `drained` and `attach-drain` change the
   ;; entry of one id, `services-stopped` removes the entry of one id, and
   ;; `end-stopped` removes entries. Each swap reads the result of the swaps
   ;; before it, so a drain that ends during a reload or a stop is not lost or
   ;; counted twice.
   :swap
   (fn [{:keys [state]} f & args]
     (apply swap! state f args))

   :stop-accepting
   (fn [_ {:keys [instance]}]
     (generation/begin-stop! instance)
     (generation/await-stop-accepting! instance))

   :log-failure
   (fn [_ id {:keys [generation-id]} t]
     (trove/log! {:level :error :id id :error t :data {:generation-id generation-id}}))

   :retire
   (fn [ctx generation]
     (future (drive! ctx (rt/begin-drain generation))))

   :await-drain
   (fn [_ {:keys [drain]}]
     (result! @drain))

   :stop-generation
   (fn [_ {:keys [instance]}]
     (generation/stop! instance))

   :stop-cert-runtime
   (fn [_ cert-runtime]
     (clave-adapter/stop! cert-runtime))

   :stop-ticket-service
   (fn [_ service]
     (tickets/stop-key-service! service))})

(defn- drive!
  "Performs the action of `progress` and gives its outcome to [[rt/step]] until
  the core is done. Returns the result of the core. Each action starts with
  the interrupt flag clear, so that an interrupt does not end the waits of the
  run. The flag is set again when the run is done."
  [ctx progress]
  (loop [{[kind & args] :action :as progress} progress
         interrupted? false]
    (if (= :done kind)
      (do
        (when interrupted?
          (.interrupt (Thread/currentThread)))
        (first args))
      (let [cleared? (Thread/interrupted)
            outcome (try
                      {:value (apply (effects kind) ctx args)}
                      (catch Throwable t
                        {:error t}))]
        (recur (rt/step progress outcome)
               (or interrupted?
                   cleared?
                   (instance? InterruptedException (:error outcome))))))))

(defn- start-on-lifecycle!
  [user-config lifecycle-executor]
  (let [listener-pool (listen/open-pool)
        generation (result! (drive! {:user-config user-config :listener-pool listener-pool}
                                    (rt/begin-start)))]
    {:busker/lifecycle-executor lifecycle-executor
     :busker/stop-result_ (AtomicReference.)
     :busker/state (atom (rt/initial-state generation listener-pool))}))

(defn start!
  "Start a new Busker server from `user-config`."
  [user-config]
  (let [executor (new-lifecycle-executor)]
    (try
      (call-on-lifecycle executor #(start-on-lifecycle! user-config executor))
      (catch Throwable t
        (.shutdownNow ^ExecutorService executor)
        (throw t)))))

(defn- reload-on-lifecycle!
  [{:keys [busker/state]} user-config opts]
  (let [{:keys [active] :as observed} @state
        snapshot (try
                   {:snapshot (config/normalized-snapshot user-config)}
                   (catch Throwable t
                     {:snapshot-error t}))]
    (result!
     (drive! {:state state
              :user-config user-config
              :listener-pool (:listener-pool observed)}
             (rt/begin-reload
              (assoc snapshot
                     :state observed
                     :force? (true? (:force? opts))
                     :failed-worker? (boolean (some-> (:instance active)
                                                      generation/failed-worker?))))))))

(defn reload!
  "Compile and activate a new runtime snapshot for `server-handle`."
  ([server-handle user-config]
   (reload! server-handle user-config {}))
  ([{:keys [busker/lifecycle-executor] :as server-handle} user-config opts]
   (try
     (call-on-lifecycle lifecycle-executor
                        #(reload-on-lifecycle! server-handle user-config opts))
     (catch RejectedExecutionException _
       (throw (ex-info "Server is stopping"
                       {:reason :server-stopping}))))))

(defn- stop-on-lifecycle!
  [{:keys [busker/state]}]
  (result! (drive! {:state state} (rt/begin-stop @state))))

(defn stop!
  "Synchronously stop `server-handle`, waiting for active and draining
   generations to quiesce."
  [{:keys [busker/lifecycle-executor busker/stop-result_] :as server-handle}]
  (when server-handle
    (let [candidate (promise)
          leader? (.compareAndSet ^AtomicReference stop-result_ nil candidate)
          result_ ^clojure.lang.IDeref (.get ^AtomicReference stop-result_)]
      (when leader?
        (try
          (.execute
           ^ExecutorService lifecycle-executor
           ^Runnable
           (reify Runnable
             (run [_]
               (let [outcome (try
                               {:value (stop-on-lifecycle! server-handle)}
                               (catch Throwable t
                                 {:error t}))]
                 (.shutdown ^ExecutorService lifecycle-executor)
                 (deliver candidate outcome)))))
          (catch Throwable t
            (deliver candidate {:error t})
            (.shutdownNow ^ExecutorService lifecycle-executor))))
      (let [{:keys [value error]} @result_]
        (when-not (.awaitTermination ^ExecutorService lifecycle-executor
                                     10 TimeUnit/SECONDS)
          (throw (ex-info "Lifecycle executor did not stop" {})))
        (when error
          (throw error))
        value))))

(defn state
  "Return pure data describing the current runtime state."
  [server-handle]
  (let [{:keys [phase config]} @(:busker/state server-handle)]
    {:phase phase
     :config config}))
