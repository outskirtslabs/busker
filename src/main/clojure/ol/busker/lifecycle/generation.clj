(ns ^:no-doc ol.busker.lifecycle.generation
  "Pure rules for a worker and a generation: the worker turn, the native
  resource table, generation start and generation retirement.

  Functions take and return plain values. A node id is a kind keyword for a
  generation node and `[kind worker-idx]` for a worker node. An observation
  is `{:workers [status ...]}`, one status per worker index:
  `{:thread :none|:alive|:ended, :stage ..., :drained? ..., :nodes {kind state}}`.")

(set! *warn-on-reflection* true)

(defn worker-turn
  "Returns the lifecycle action of one loop turn. Allocates nothing."
  [stage stop-requested? drained?]
  (case stage
    :serving (if stop-requested? :begin-drain :serve)
    :draining (if drained? :release :wait)
    :released :idle))

(defn stopped-accepting?
  "Returns true when the worker with `status` accepts no new connection."
  [status]
  (or (some? (:failure status)) (not= :serving (:stage status))))

(defn all-stopped-accepting?
  "Returns true when every worker status in `statuses` stopped accepting."
  [statuses]
  (every? stopped-accepting? statuses))

(def resources
  "The native resource nodes of a generation.

  - `:needs`: the nodes that must stay acquired while this node is acquired.
    A worker node needs the node of the same worker.
  - `:release-at`: for a worker node, when its worker may release it:
    `:drain` (stop accepting), `:drained` (no connection left) or
    `:thread-end` (only the stopping thread, after the worker thread ended).
  - `:when-kept`: what the stopping thread does to a node it cannot release."
  {:socket-claims {}
   :udp-claims {}
   :arena {}
   :close-executor {}
   :config {:needs #{:arena}}
   :loops {}
   :ssl-ctxs {}
   :ticket-manager {}
   :h3-contexts {:needs #{:config :ticket-manager}}
   :callback-dispatch {:release-at :thread-end :needs #{:close-executor :arena}}
   :context {:release-at :drained
             :needs #{:config :loops :ssl-ctxs :arena :callback-dispatch}}
   :response-receiver {:release-at :drained :needs #{:context}}
   :pool {:release-at :drained}
   :listeners {:release-at :drain :needs #{:socket-claims :loops :context}}
   :h3 {:release-at :drained :needs #{:context :h3-contexts :udp-claims}
        :when-kept :close-udp-sockets}})

(def worker-release-order
  "The release order of the nodes of one worker."
  [:listeners :response-receiver :pool :h3 :context :callback-dispatch])

(def generation-release-order
  "The release order of the generation nodes, after every worker node."
  [:close-executor :h3-contexts :ticket-manager :loops :config :ssl-ctxs
   :socket-claims :udp-claims :arena])

(defn release-order
  "Returns the release order of the stopping thread for `n-workers` workers."
  [n-workers]
  (concat (for [i (range n-workers) kind worker-release-order] [kind i])
          generation-release-order))

(defn- kind-of [id] (if (vector? id) (first id) id))

(defn- worker-of [id] (when (vector? id) (second id)))

(defn needs
  "Returns the ids of the nodes that node `id` needs."
  [id]
  (for [kind (:needs (resources (kind-of id)))]
    (if (:release-at (resources kind)) [kind (worker-of id)] kind)))

(defn order-violations
  "Returns the `[id dependent]` pairs where `order` releases `id` while
  `dependent`, a node in `acquired` that needs `id`, is not released yet.
  An empty result means that `order` respects [[resources]]."
  [acquired order]
  (first
   (reduce (fn [[bad released] id]
             [(into bad (for [d acquired
                              :when (and (not (released d)) (some #{id} (needs d)))]
                          [id d]))
              (conj released id)])
           [[] #{}]
           order)))

(defn quiet?
  "Returns true when `releaser`, `:stopping` or a worker index, may release
  node `id` by observation `obs`. The worker thread alone releases its nodes
  while it runs. The stopping thread releases a worker's nodes after that
  thread ended or when it never started, and generation nodes only while no
  worker thread runs. An ended worker that is not drained keeps every node
  except its listeners."
  [obs releaser id]
  (let [idx (worker-of id)
        at (:release-at (resources (kind-of id)))
        status (get-in obs [:workers idx])]
    (cond
      (and (= :stopping releaser) idx)
      (case (:thread status)
        :alive false
        :none true
        (or (= :drain at) (boolean (:drained? status))))

      (= :stopping releaser)
      (not-any? #(= :alive (:thread %)) (:workers obs))

      :else
      (and (= releaser idx)
           (case at
             :drain (not= :serving (:stage status))
             :drained (and (= :draining (:stage status)) (boolean (:drained? status)))
             false)))))

(defn- releasable?
  [nodes obs releaser not-yet id]
  (and (= :acquired (nodes id))
       (not (contains? not-yet (or (worker-of id) id)))
       (quiet? obs releaser id)
       (every? #(= :released (nodes %))
               (filter #(some #{id} (needs %)) (keys nodes)))))

(defn next-release
  "Returns the first node in release order that `releaser` may release now,
  or nil. `nodes` maps node ids to states. `not-yet` holds the worker indexes
  and generation ids whose release returned not yet in this run. It stops the
  rest of that worker's nodes."
  [nodes obs releaser not-yet]
  (let [order (if (= :stopping releaser)
                (release-order (count (:workers obs)))
                (map #(vector % releaser) worker-release-order))]
    (first (filter #(releasable? nodes obs releaser not-yet %) order))))

(defn release-state
  "Returns the node state after a release with `outcome`: `:ok`, `:not-yet`
  or `{:error t}`. A release that throws leaks the node."
  [outcome]
  (case outcome
    :ok :released
    :not-yet :acquired
    :leaked))

(defn acquire-steps
  "Returns the start steps for `n-workers` workers in order, as
  `[step node-ids]` pairs. Each step acquires its nodes or none of them."
  [n-workers]
  (let [each (fn [& kinds] (for [kind kinds i (range n-workers)] [kind i]))]
    (-> [[:claims [:socket-claims :udp-claims]]
         [:executors [:close-executor]]
         [:tls-lookup []]
         [:core (into [:arena :config :loops] (each :context :response-receiver))]
         [:pools (vec (each :pool))]
         [:listeners (into [:ssl-ctxs] (each :listeners))]
         [:http3 (into [:ticket-manager :h3-contexts] (each :h3))]]
        (into (map (fn [i] [[:worker i] [[:callback-dispatch i]]])) (range n-workers))
        (conj [:activate-http3 []]))))

(defn start
  "Returns the progress value of a generation that starts `n-workers` workers."
  [n-workers]
  {:steps (acquire-steps n-workers) :nodes {}})

(defn retire
  "Returns `progress` ready for a new retirement run. Node states, the stop
  signal, the handler wait and the kept actions done carry over. A stopped
  progress stays stopped."
  [progress]
  (if (= :stopped (:outcome (:result progress)))
    (dissoc progress :action)
    (dissoc progress :result :start-error :joined? :join-error :not-yet :errors :action)))

(defn- alive-workers [obs]
  (vec (keep-indexed (fn [i status] (when (= :alive (:thread status)) i)) (:workers obs))))

(defn- later-state [a b]
  (let [rank {nil -1 :acquired 0 :releasing 1 :released 2 :leaked 2}]
    (if (>= (rank a) (rank b)) a b)))

(defn- merge-ended-workers
  "Takes the node states that ended worker threads published."
  [nodes obs]
  (reduce-kv (fn [nodes idx status]
               (if (= :ended (:thread status))
                 (reduce-kv (fn [nodes kind state] (update nodes [kind idx] later-state state))
                            nodes (:nodes status))
                 nodes))
             nodes (vec (:workers obs))))

(defn- add-error [progress outcome]
  (if-let [e (:error outcome)]
    (update progress :errors (fnil conj []) e)
    progress))

(defn- record [progress obs outcome]
  (let [[op arg] (:action progress)
        p (dissoc progress :action)
        e (:error outcome)]
    (case op
      (nil :done) p
      :acquire (if e
                 (-> p (dissoc :steps) (assoc :start-error e))
                 (let [p (-> p
                             (update :nodes into (map #(vector % :acquired)) (second (first (:steps p))))
                             (update :steps rest))]
                   (if (seq (:steps p))
                     p
                     (-> p (dissoc :steps) (assoc :result {:outcome :started})))))
      :signal-stop (if e (assoc p :result {:outcome :failed :error e}) (assoc p :signalled? true))
      :await-handlers (cond
                        e (assoc p :result {:outcome :failed :error e})
                        (= :timed-out outcome) (assoc p :handlers :timed-out)
                        :else (assoc p :handlers :done))
      :interrupt-handlers (assoc p :handlers :done)
      :join-workers (if e
                      (assoc p :joined? true :join-error e)
                      (-> p (assoc :joined? true) (update :nodes merge-ended-workers obs)))
      :force-fail-workers (assoc p :result (cond-> {:outcome :not-joined :alive-workers arg}
                                             (:join-error p) (assoc :error (:join-error p))))
      :release (-> p
                   (assoc-in [:nodes arg] (release-state outcome))
                   (add-error outcome)
                   (cond-> (= :not-yet outcome)
                     (update :not-yet (fnil conj #{}) (or (worker-of arg) arg))))
      :when-kept (-> p (update :kept-done (fnil conj #{}) arg) (add-error outcome)))))

(defn- sweep-result [{:keys [nodes errors]}]
  (if (every? #{:released} (vals nodes))
    {:outcome :stopped}
    {:outcome :kept
     :unfinished-workers (->> nodes
                              (keep (fn [[id state]] (when (not= :released state) (worker-of id))))
                              distinct sort vec)
     :errors (or errors [])}))

(defn- next-action [p obs]
  (let [act #(assoc p :action %)
        alive (alive-workers obs)]
    (cond
      (:result p) (act [:done (cond-> (:result p)
                                (:start-error p) (assoc :start-error (:start-error p)))])
      (:steps p) (act [:acquire (ffirst (:steps p))])
      (not (:signalled? p)) (act [:signal-stop])
      (nil? (:handlers p)) (act [:await-handlers])
      (= :timed-out (:handlers p)) (act [:interrupt-handlers])
      (not (:joined? p)) (act [:join-workers])
      (or (seq alive) (:join-error p)) (act [:force-fail-workers alive])
      :else
      (let [nodes (:nodes p)
            n (count (:workers obs))]
        (if-let [id (next-release nodes obs :stopping (:not-yet p #{}))]
          (-> p (assoc-in [:nodes id] :releasing) (assoc :action [:release id]))
          (if-let [id (first (filter #(and (= :acquired (nodes %))
                                           (:when-kept (resources (kind-of %)))
                                           (not (contains? (:kept-done p) %)))
                                     (release-order n)))]
            (act [:when-kept id])
            (recur (assoc p :result (sweep-result p)) obs)))))))

(defn step
  "Applies `outcome` of the action under `:action` in `progress`, then returns
  the progress with the next action under `:action`, chosen by observation
  `obs`. A node that the next action releases is `:releasing` already.
  Outcomes are `:ok`, `:timed-out`, `:not-yet` or `{:error t}`."
  [progress obs outcome]
  (next-action (record progress obs outcome) obs))

(defn result
  "Returns the result of the last finished run of `progress`, or nil."
  [progress]
  (when (= :done (first (:action progress)))
    (second (:action progress))))

(defn phase
  "Returns the phase of `progress`: `:starting`, `:running`, `:stopping`,
  `:retiring`, `:kept` or `:stopped`."
  [progress]
  (let [outcome (:outcome (:result progress))]
    (cond
      (= :stopped outcome) :stopped
      (#{:kept :not-joined} outcome) :kept
      (:steps progress) :starting
      (not (:signalled? progress)) :running
      (not= :done (:handlers progress)) :stopping
      :else :retiring)))
