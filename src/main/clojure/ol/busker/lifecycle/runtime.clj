(ns ^:no-doc ol.busker.lifecycle.runtime
  "Pure transitions for server start, runtime reload, server stop and the drain
  of one generation.

  [[begin-start]], [[begin-reload]], [[begin-stop]] and [[begin-drain]] return
  a progress value with the first action under `:action`. The shell does that
  action and gives its outcome to [[step]], which returns the progress with
  the next action, until the action is `[:done result]`. An outcome or a
  result is `{:value v}` or `{:error throwable}`. A failed result can also
  have `:suppressed`.

  A generation entry, a certificate runtime and a ticket service are opaque
  values. The core compares entries by `:generation-id` and services by
  identity. A new entry reuses a service of the old one when it has the same
  `:cert-plan` or `:ticket-policy`. `[:swap f & args]` asks the shell to
  `swap!` the state atom with `f`. Its outcome value is the new state.")

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
  "Sets the phase to `:stopped`. Of the draining list, it keeps the generations
  whose id is in `kept`, because they did not drain."
  [state kept]
  (-> state
      (assoc :phase :stopped)
      (update :draining (partial filterv #(contains? kept (:generation-id %))))))

(defn attach-drain
  "Puts the drain `handle` on draining generation `id` if it is still listed."
  [state id handle]
  (update state :draining
          (partial mapv #(cond-> % (= id (:generation-id %)) (assoc :drain handle)))))

(defn drained
  "Marks draining generation `id` as stopped. It stays listed until its drain
  thread has stopped the services that [[drained-stops]] gives."
  [state id]
  (update state :draining
          (partial mapv #(cond-> % (= id (:generation-id %)) (assoc :drained? true)))))

(defn drain-failed
  "Marks draining generation `id` as failed to stop. It stays listed."
  [state id]
  (update state :draining
          (partial mapv #(cond-> % (= id (:generation-id %)) (assoc :drain-failed? true)))))

(defn services-stopped
  "Takes draining generation `id` off the list."
  [state id]
  (update state :draining (partial filterv #(not= id (:generation-id %)))))

(defn unawaited
  "Records that the server stop does not await the drain of generation `id`,
  so the drain logs its errors."
  [state id]
  (update state :unawaited (fnil conj #{}) id))

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

(defn drained-stops
  "Returns the stop actions that follow the drain of `generation`, from
  `state`, the result of [[drained]]. While the phase is `:running`, that is a
  stop action for each service of `generation` that no listed generation has,
  except generations that [[drained]] marked. During a server stop it is
  empty, because the stop stops the services of each generation that was not
  marked when it began."
  [state generation]
  (when (= :running (:phase state))
    (service-stops [generation] (cons (:active state) (remove :drained? (:draining state))))))

(defn- go
  [progress stage action]
  (assoc progress :stage stage :action action))

(defn- done
  [progress result]
  (go progress :done [:done result]))

(defn- wrap-error
  "Returns an error with `message` and `data` that wraps `t`. It has the data of
  `t`, and the stage of `t` or else `stage`."
  [message data stage t]
  (let [t-data (ex-data t)]
    (ex-info message (merge data {:stage (or (:stage t-data) stage)} t-data) t)))

(defn- reload-failure
  ([t] (reload-failure t :activation))
  ([t stage]
   {:error (wrap-error "Reload failed" {:reason :reload-failed} stage t)}))

(defn- reused
  "Returns the service under `k` that the candidate gets without a start: none
  when it needs none, the service of the old generation when that one has the
  same need under `need-k`, or else `::start`."
  [{:keys [old candidate]} k need-k]
  (let [need (need-k candidate)]
    (cond
      (nil? need) nil
      (= need (need-k old)) (k old)
      :else ::start)))

(defn- start-generation
  [{:keys [candidate compiled-config] :as progress}]
  (go progress :start-generation [:start-generation candidate compiled-config]))

(defn- start-ticket-service
  [{:keys [old compiled-config] :as progress}]
  (let [service (reused progress :memory-ticket-service :ticket-policy)]
    (if (= ::start service)
      (go progress :start-tickets
          [:start-ticket-service (:memory-ticket-service old) compiled-config])
      (start-generation (assoc-in progress [:candidate :memory-ticket-service] service)))))

(defn- start-cert-runtime
  [progress]
  (let [runtime (reused progress :cert-automation :cert-plan)]
    (if (= ::start runtime)
      (go progress :start-cert [:start-cert-runtime (get-in progress [:candidate :cert-plan])])
      (start-ticket-service (assoc-in progress [:candidate :cert-automation] runtime)))))

(defn- build
  "Builds the candidate, generation `id`: compiles the configuration and makes
  its normalized snapshot, starts or reuses the certificate runtime and the
  ticket service, starts the generation and activates it."
  [progress id]
  (go (assoc progress :candidate {:generation-id id})
      :compile [:compile-config]))

(defn begin-start
  "Returns the progress of the start of a server. The result value is the
  generation entry for [[initial-state]]."
  []
  (build {:op :start} 1))

(defn begin-reload
  "Returns the progress of a reload from the observation `{:state s :snapshot c
  :snapshot-error t :force? b :failed-worker? b}`. `:snapshot` is the
  normalized next configuration, or `:snapshot-error` is the error that its
  normalization threw. `:failed-worker?` tells if a worker of the active
  generation has failed."
  [{:keys [state snapshot snapshot-error force? failed-worker?]}]
  (let [{:keys [phase active next-generation-id]} state
        progress {:op :reload :old active}]
    (cond
      (not= :running phase)
      (done progress {:error (ex-info "Server is stopping" {:reason :server-stopping})})

      snapshot-error
      (done progress (reload-failure snapshot-error :validation))

      (and (not force?) (not failed-worker?) (= snapshot (:config active)))
      (done progress {:value :unchanged})

      :else
      (build progress next-generation-id))))

(defn begin-stop
  "Returns the progress of a server stop of `state`."
  [state]
  (go {:op :stop :old (:active state) :failures [] :kept #{}}
      :begin-stopping [:swap begin-stopping]))

(defn begin-drain
  "Returns the progress of the drain of `generation` after it stopped
  accepting. The drain stops it, marks it with [[drained]], stops the services
  that [[drained-stops]] gives and takes it off the list. A failed stop marks
  it with [[drain-failed]]."
  [generation]
  (go {:op :drain :generation generation :failures []}
      :drain-stop [:stop-generation generation]))

(defn- next-discard
  [{:keys [op to-discard error failures] :as progress}]
  (if-let [action (first to-discard)]
    (go (update progress :to-discard rest) :discard action)
    (done progress (assoc (if (= :start op) {:error error} (reload-failure error))
                          :suppressed failures))))

(defn- discard
  "Stops what the candidate started and does not share with the old generation,
  then fails with `error` and the errors of these stops as `:suppressed`. A
  reload wraps `error` in its reload error."
  [{:keys [old candidate] :as progress} error]
  (next-discard (assoc progress
                       :error error
                       :failures []
                       :to-discard (into (if (:instance candidate)
                                           [[:stop-generation candidate]]
                                           [])
                                         (service-stops [candidate] [old])))))

(defn- failures-result
  [[failure & more]]
  (if failure
    {:error failure :suppressed (vec more)}
    {:value nil}))

(defn- next-stop-step
  "Awaits each generation, then stops each service, then ends the stop with the
  first failure and the others as `:suppressed`. A generation without a drain
  thread is stopped in place of the await. An interrupt ends the await of a
  drain, and [[unawaited]] records it."
  [{:keys [to-await to-stop kept] :as progress}]
  (cond
    (seq to-await)
    (let [generation (first to-await)]
      (go (assoc progress :to-await (rest to-await) :awaiting generation)
          :await
          (if (:drain generation)
            [:await-drain generation]
            [:stop-generation generation])))

    (seq to-stop)
    (go (update progress :to-stop rest) :stop-service (first to-stop))

    :else
    (go progress :end-stopped [:swap end-stopped kept])))

(defn- awaited
  "Continues the stop after the await of a generation. An error keeps the
  generation listed."
  [{:keys [awaiting] :as progress} error]
  (next-stop-step (cond-> progress
                    error (-> (update :failures conj error)
                              (update :kept conj (:generation-id awaiting))))))

(defn- next-drain-stop
  "Stops each service, then takes the generation off the list."
  [{:keys [to-stop generation] :as progress}]
  (if (seq to-stop)
    (go (update progress :to-stop rest) :drain-service (first to-stop))
    (go progress :services-stopped [:swap services-stopped (:generation-id generation)])))

(defn- next-drain-log
  "Logs each failed service stop, then ends the drain."
  [{:keys [failures generation] :as progress}]
  (if-let [failure (first failures)]
    (go (update progress :failures rest) :log-service
        [:log-failure :ol.busker.runtime/service-stop-failed generation failure])
    (done progress {:value nil})))

(defn- drain-begun
  "Continues after the old generation began to drain: a reload is done, a
  server stop awaits it after the generations that were draining already."
  [{:keys [op old] :as progress}]
  (if (= :reload op)
    (done progress {:value :activated})
    (next-stop-step (update progress :to-await conj old))))

(defn step
  "Applies `outcome` of the action under `:action` in `progress`. Returns the
  progress with the next action under `:action`. Nothing is undone after the
  candidate is published."
  [{:keys [stage old generation] :as progress} {:keys [value error]}]
  (case stage
    :compile
    (if error
      (discard progress (wrap-error "Failed to compile generation config" {} :validation error))
      (start-cert-runtime (-> progress
                              (assoc :compiled-config (:compiled-config value))
                              (update :candidate assoc
                                      :config (:snapshot value)
                                      :cert-plan (:cert-plan value)
                                      :ticket-policy (:ticket-policy value)))))

    :start-cert
    (if error
      (discard progress (wrap-error "Failed to start certificate automation" {}
                                    :automation-startup error))
      (start-ticket-service (assoc-in progress [:candidate :cert-automation] value)))

    :start-tickets
    (if error
      (discard progress (wrap-error "Failed to start in-memory session tickets" {}
                                    :activation error))
      (start-generation (assoc-in progress [:candidate :memory-ticket-service] value)))

    :start-generation
    (if error
      (discard progress (wrap-error "Failed to start generation" {} :activation error))
      (let [candidate (assoc (:candidate progress) :instance value)]
        (go (assoc progress :candidate candidate) :activate [:activate candidate])))

    :activate
    (cond
      error (discard progress error)
      (= :start (:op progress)) (done progress {:value (:candidate progress)})
      :else (go progress :publish [:swap publish (:candidate progress)]))

    :discard
    (next-discard (cond-> progress error (update :failures conj error)))

    :publish
    (if error
      (discard progress error)
      (go progress :stop-accepting [:stop-accepting old]))

    :stop-accepting
    (if error
      (go progress :log-stop-accepting
          [:log-failure :ol.busker.runtime/stop-accepting-failed old error])
      (go progress :retire [:retire old]))

    :log-stop-accepting
    (go progress :retire [:retire old])

    :retire
    (if error
      (go progress :log-retire
          [:log-failure :ol.busker.runtime/retire-failed old error])
      (go (assoc-in progress [:old :drain] value)
          :attach [:swap attach-drain (:generation-id old) value]))

    :log-retire
    (drain-begun progress)

    ;; An error leaves the list entry without its drain handle. The drain
    ;; still runs, and a stop has the handle in `:old`.
    :attach
    (drain-begun progress)

    ;; An error leaves the state as it was, so nothing is drained or stopped.
    :begin-stopping
    (if error
      (done progress {:error error})
      (let [generations (:draining value)
            progress (assoc progress
                            :to-await (filterv #(not= (:generation-id old) (:generation-id %))
                                               generations)
                            :to-stop (service-stops (remove :drained? generations) []))]
        (if old
          (go progress :stop-accepting [:stop-accepting old])
          (next-stop-step progress))))

    :await
    (if (and (instance? InterruptedException error) (:drain (:awaiting progress)))
      (go (assoc progress :interrupt error)
          :unawaited [:swap unawaited (:generation-id (:awaiting progress))])
      (awaited progress error))

    ;; A drain that took its generation off the list or marked it failed
    ;; before this swap gives its failures to this stop. Its result is ready,
    ;; so the stop reads it.
    :unawaited
    (let [{:keys [awaiting interrupt]} progress]
      (cond
        error (awaited (update progress :failures conj interrupt) error)
        (some #(and (= (:generation-id awaiting) (:generation-id %)) (not (:drain-failed? %)))
              (:draining value))
        (awaited progress interrupt)
        :else (go (update progress :failures conj interrupt)
                  :reawait [:await-drain awaiting])))

    :reawait
    (awaited progress error)

    :stop-service
    (next-stop-step (cond-> progress error (update :failures conj error)))

    :end-stopped
    (done progress (failures-result (cond-> (:failures progress) error (conj error))))

    :drain-stop
    (if error
      (go (assoc progress :error error)
          :drain-failed [:swap drain-failed (:generation-id generation)])
      (go progress :drained [:swap drained (:generation-id generation)]))

    ;; The generation stays listed, so a later stop awaits this drain and
    ;; reads the error. When an interrupt ended the await, it is logged.
    :drain-failed
    (cond
      error (done progress {:error (:error progress) :suppressed [error]})
      (contains? (:unawaited value) (:generation-id generation))
      (go progress :log-drain
          [:log-failure :ol.busker.runtime/generation-stop-failed generation (:error progress)])
      :else (done progress {:error (:error progress)}))

    :log-drain
    (done progress {:value nil})

    :drained
    (if error
      (done progress {:error error})
      (next-drain-stop (assoc progress :to-stop (drained-stops value generation))))

    :drain-service
    (next-drain-stop (cond-> progress error (update :failures conj error)))

    ;; A server stop that began before this swap awaits this drain, so the
    ;; failures go to it. With no stop running, or when an interrupt ended the
    ;; await, they are logged.
    :services-stopped
    (cond
      error (done progress {:error error :suppressed (:failures progress)})
      (and (= :stopping (:phase value))
           (not (contains? (:unawaited value) (:generation-id generation))))
      (done progress (failures-result (:failures progress)))
      :else (next-drain-log progress))

    :log-service
    (next-drain-log progress)))
