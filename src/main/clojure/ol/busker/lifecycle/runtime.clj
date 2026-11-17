(ns ^:no-doc ol.busker.lifecycle.runtime
  "Pure transitions for runtime reload and server stop.

  [[begin-reload]] and [[begin-stop]] return a progress value with the first
  action under `:action`. The shell does that action and gives its outcome to
  [[step]], which returns the progress with the next action, until the action
  is `[:done result]`. An outcome or a result is `{:value v}` or
  `{:error throwable}`. A stop result can also have `:suppressed`.

  A generation entry, a certificate runtime and a ticket service are opaque
  values. The core compares entries by `:generation-id` and services by
  identity. `[:swap f & args]` asks the shell to `swap!` the state atom with
  `f`. Its outcome value is the new state.")

(set! *warn-on-reflection* true)

(defn initial-state
  "Returns the state of a runtime that serves `generation`."
  [generation listener-pool]
  {:phase :running
   :config (:config generation)
   :next-generation-id (inc (:generation-id generation))
   :listener-pool listener-pool
   :active generation
   :draining []})

(defn publish
  "Makes `candidate` active and puts the active generation on the draining list
  in the same step."
  [{:keys [active] :as state} candidate]
  (-> state
      (assoc :config (:config candidate) :active candidate)
      (update :next-generation-id inc)
      (update :draining conj active)))

(defn begin-stopping
  "Sets the phase to `:stopping` and puts the active generation on the draining list."
  [{:keys [active] :as state}]
  (cond-> (assoc state :phase :stopping :active nil)
    active (update :draining conj active)))

(defn end-stopped
  "Sets the phase to `:stopped` and keeps `result` for a later stop."
  [state result]
  (assoc state :phase :stopped :draining [] :stop-result result))

(defn attach-drain
  "Puts the drain `handle` on draining generation `id` if it is still listed."
  [state id handle]
  (update state :draining
          (partial mapv #(cond-> % (= id (:generation-id %)) (assoc :drain handle)))))

(defn- service-stops
  "Returns stop actions for the services of `generations` that no generation in
  `users` has."
  [generations users]
  (vec
   (distinct
    (for [[k action] [[:cert-automation :stop-cert-runtime]
                      [:memory-ticket-service :stop-ticket-service]]
          service (keep k generations)
          :when (not-any? #(identical? service (k %)) users)]
      [action service]))))

(defn drained
  "Takes draining generation `id` off the list. Returns `{:state s :stop actions}`.
  While the phase is `:running`, `:stop` has a stop action for each service of
  that generation that no listed generation has. During a server stop it is
  empty, because the stop stops the services of every generation it drains."
  [state id]
  (let [generation (first (filter #(= id (:generation-id %)) (:draining state)))
        next-state (update state :draining
                           (partial filterv #(not= id (:generation-id %))))]
    {:state next-state
     :stop (when (and generation (= :running (:phase state)))
             (service-stops [generation]
                            (cons (:active next-state) (:draining next-state))))}))

(defn- go
  [progress stage action]
  (assoc progress :stage stage :action action))

(defn- done
  [progress result]
  (go progress :done [:done result]))

(defn- reload-failure
  [t]
  (let [data (ex-data t)]
    {:error (ex-info "Reload failed"
                     (merge {:reason :reload-failed
                             :stage (or (:stage data) :activation)}
                            data)
                     t)}))

(defn begin-reload
  "Returns the progress of a reload from the observation `{:state s :snapshot c
  :force? b :failed-worker? b}`. `:snapshot` is the normalized next
  configuration. `:failed-worker?` tells if a worker of the active generation
  has failed."
  [{:keys [state snapshot force? failed-worker?]}]
  (let [{:keys [phase active next-generation-id]} state
        progress {:op :reload :old active}]
    (cond
      (not= :running phase)
      (done progress {:error (ex-info "Server is stopping" {:reason :server-stopping})})

      (and (not force?) (not failed-worker?) (= snapshot (:config active)))
      (done progress {:value :unchanged})

      :else
      (go progress :build [:build next-generation-id]))))

(defn begin-stop
  "Returns the progress of a server stop of `state`. A stop of a stopped
  runtime gives the result of the first stop."
  [state]
  (if (= :stopped (:phase state))
    (done {:op :stop} (:stop-result state))
    (go {:op :stop :old (:active state) :failures []}
        :begin-stopping [:swap begin-stopping])))

(defn- next-discard
  [{:keys [to-discard error] :as progress}]
  (if-let [action (first to-discard)]
    (go (update progress :to-discard rest) :discard action)
    (done progress (reload-failure error))))

(defn- next-stop-step
  "Awaits each generation, then stops each service, then ends the stop with the
  first failure and the others as `:suppressed`."
  [{:keys [to-await to-stop failures] :as progress}]
  (cond
    (seq to-await)
    (go (update progress :to-await rest) :await [:await-drain (first to-await)])

    (seq to-stop)
    (go (update progress :to-stop rest) :stop-service (first to-stop))

    :else
    (let [result (if (seq failures)
                   {:error (first failures) :suppressed (vec (rest failures))}
                   {:value nil})]
      (go (assoc progress :result result) :end-stopped [:swap end-stopped result]))))

(defn- drain-begun
  "Continues after the old generation began to drain: a reload is done, a
  server stop awaits it after the generations that were draining already."
  [{:keys [op old] :as progress}]
  (if (= :reload op)
    (done progress {:value :activated})
    (next-stop-step (update progress :to-await conj old))))

(defn step
  "Applies `outcome` of the action under `:action` in `progress`. Returns the
  progress with the next action under `:action`. Nothing is undone after
  `:activate` succeeds."
  [{:keys [stage old candidate] :as progress} {:keys [value error]}]
  (case stage
    :build
    (if error
      (done progress (reload-failure error))
      (go (assoc progress :candidate value) :activate [:activate value]))

    :activate
    (if error
      (next-discard (assoc progress
                           :error error
                           :to-discard (into [[:stop-generation candidate]]
                                             (service-stops [candidate] [old]))))
      (go (assoc progress :candidate value) :publish [:swap publish value]))

    :discard
    (next-discard progress)

    :publish
    (go progress :stop-accepting [:stop-accepting old])

    :stop-accepting
    (if error
      (go progress :log [:log-stop-accepting-failed old error])
      (go progress :retire [:retire old]))

    :log
    (go progress :retire [:retire old])

    :retire
    (if error
      (drain-begun progress)
      (go (assoc-in progress [:old :drain] value)
          :attach [:swap attach-drain (:generation-id old) value]))

    :attach
    (drain-begun progress)

    :begin-stopping
    (let [generations (:draining value)
          progress (assoc progress
                          :to-await (filterv #(not= (:generation-id old) (:generation-id %))
                                             generations)
                          :to-stop (service-stops generations []))]
      (if old
        (go progress :stop-accepting [:stop-accepting old])
        (next-stop-step progress)))

    (:await :stop-service)
    (next-stop-step (cond-> progress error (update :failures conj error)))

    :end-stopped
    (done progress (:result progress))))
