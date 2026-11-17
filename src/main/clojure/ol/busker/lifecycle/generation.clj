(ns ^:no-doc ol.busker.lifecycle.generation
  "Pure rules for a worker and a generation: the worker turn, the native
  resource table, generation start and generation retirement.

  Functions take and return plain values. A node id is a kind keyword for a
  generation node and `[kind worker-idx]` for a worker node. A node state is
  `:acquired`, `:released` or `:unfinished`, a release that started and did
  not end. An observation is `{:workers [status ...]}`, one status per worker
  index: `{:thread :none|:alive|:ended, :stage ..., :drained? ..., :nodes {kind state}}`.")

(set! *warn-on-reflection* true)

(defn worker-turn
  "Returns the lifecycle action of one loop turn. Allocates nothing."
  [stage stop-requested? drained?]
  (case stage
    :serving (if stop-requested? :begin-drain :serve)
    :draining (if drained? :release :wait)
    :released :idle))

(def resources
  "The native resource nodes of a generation.

  - `:needs`: the nodes that must stay acquired while this node is acquired.
    A worker node needs the node of the same worker.
  - `:release-at`: for a worker node, when its worker may release it:
    `:drain` (stop accepting), `:drained` (no connection left) or
    `:thread-end` (only the stopping thread, after the worker thread ended).

  `:thread` is the worker thread. It needs no node, because the worker
  thread releases the nodes of that worker while it runs, and its release does nothing,
  because the join ends the thread."
  {:socket-claims {}
   :udp-claims {}
   :arena {}
   :close-executor {}
   :config {:needs #{:arena :close-executor}}
   :loops {}
   :ticket-manager {}
   :ssl-ctxs {:needs #{:ticket-manager}}
   :h3-contexts {:needs #{:config :ticket-manager :ssl-ctxs}}
   :callback-dispatch {:release-at :thread-end :needs #{:close-executor :arena}}
   :context {:release-at :drained
             :needs #{:config :loops :ssl-ctxs :arena :callback-dispatch}}
   :response-receiver {:release-at :drained :needs #{:context}}
   :pool {:release-at :drained}
   :listeners {:release-at :drain :needs #{:socket-claims :loops :context :ssl-ctxs}}
   :h3 {:release-at :drained :needs #{:context :h3-contexts :udp-claims}}
   :thread {:release-at :thread-end}})

(def worker-release-order
  "The release order of the nodes of one worker. [[resources]] decides first,
  and the order breaks ties. The order also keeps the drain check of the
  worker safe. That check reads the HTTP/3 and context counts with no
  [[readable?]] check. The releases that can return not yet,
  `:response-receiver` and `:pool`, come before `:h3` and `:context`. So while
  the worker waits for them, nothing has freed the nodes that the check reads."
  [:listeners :response-receiver :pool :h3 :context :callback-dispatch :thread])

(def generation-release-order
  "The release order of the generation nodes, after every worker node."
  [:h3-contexts :ssl-ctxs :ticket-manager :loops :config :close-executor
   :socket-claims :udp-claims :arena])

(defn release-order
  "Returns the release order of the stopping thread for `n-workers` workers."
  [n-workers]
  (concat (for [i (range n-workers) kind worker-release-order] [kind i])
          generation-release-order))

(defn acquire-order
  "Returns the node ids that a start of `n-workers` workers acquires, in order."
  [n-workers]
  (let [each (fn [kind] (map #(vector kind %) (range n-workers)))]
    (concat [:socket-claims :udp-claims :close-executor :arena :config :loops :ticket-manager
             :ssl-ctxs]
            (mapcat each [:callback-dispatch :context :response-receiver :pool :listeners])
            [:h3-contexts]
            (mapcat each [:h3 :thread]))))

(defn kind-of
  "Returns the kind of node `id`."
  [id]
  (if (vector? id) (first id) id))

(defn worker-of
  "Returns the worker index of node `id`, or nil for a generation node."
  [id]
  (when (vector? id) (second id)))

(defn needs
  "Returns the ids of the nodes that node `id` needs."
  [id]
  (for [kind (:needs (resources (kind-of id)))]
    (if (:release-at (resources kind)) [kind (worker-of id)] kind)))

(defn readable?
  "Returns true when the stopping thread may read the native counts of node
  `kind` of the ended worker at `i`: the node is acquired in the progress
  `nodes` and in the worker's `status`, so nothing freed it."
  [nodes status kind i]
  (and (= :acquired (nodes [kind i]))
       (contains? #{nil :acquired} (get-in status [:nodes kind]))))

(defn ended-drained?
  "Returns true when the ended worker with `status` left nothing that a loop
  turn must finish. `:facts` of the status holds what the stopping thread
  read: `:commands?`, `:responses?` and `:blocks?` pending, and the open
  connections `:generation-connections?`, `:context-connections?` and
  `:h3-connections?`. A worker that ended without failure closed its
  connections itself, so only a failed worker is checked for them."
  [{:keys [failure facts]}]
  (not (or (:commands? facts)
           (:responses? facts)
           (:blocks? facts)
           (and (some? failure)
                (or (:generation-connections? facts)
                    (:context-connections? facts)
                    (:h3-connections? facts))))))

(defn quiet?
  "Returns true when `releaser`, `:stopping` or a worker index, may release
  node `id` by observation `obs`. The worker thread alone releases its nodes
  while it runs. The stopping thread releases a worker's nodes after that
  thread ended or when it never started, and generation nodes only while no
  worker thread runs. An ended worker that is not drained keeps every node
  except its listeners. See [[ended-drained?]]."
  [obs releaser id]
  (let [idx (worker-of id)
        at (:release-at (resources (kind-of id)))
        status (get-in obs [:workers idx])]
    (cond
      (and (= :stopping releaser) idx)
      (case (:thread status)
        :alive false
        :none true
        (or (= :drain at) (ended-drained? status)))

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

(defn worker-release
  "Returns the kind of the next node that the worker thread may release, by
  the `status` of that worker and `drained?`, or nil. A node that is not in the
  `:nodes` of `status` is acquired."
  [status drained?]
  (let [nodes (into {} (map (fn [kind] [[kind 0] (get (:nodes status) kind :acquired)]))
                    worker-release-order)]
    (kind-of (next-release nodes {:workers [(assoc status :drained? drained?)]} 0 #{}))))

(defn worker-released?
  "Returns true when the worker with `status` released every node it
  releases itself, so that its loop may end."
  [status]
  (= :released (get-in status [:nodes :context])))

(defn release-state
  "Returns the node state after a release with `outcome`: `:ok`, `:not-yet`
  or `{:error t}`. A release that throws is not tried again."
  [outcome]
  (case outcome
    :ok :released
    :not-yet :acquired
    :unfinished))

(defn start
  "Returns the progress value of a generation that starts `n-workers`
  workers."
  [n-workers]
  {:steps (mapv #(vector :acquire %) (acquire-order n-workers))
   :nodes {}})

(defn retire
  "Returns `progress` ready for a new retirement run. Node states, the stop
  signal, the handler wait, the kept actions done and the release errors
  carry over. A stopped progress stays stopped."
  [progress]
  (if (= :stopped (:outcome (:result progress)))
    (dissoc progress :action)
    (select-keys progress [:nodes :signalled? :handlers :kept-done :errors])))

(defn fail
  "Returns `progress` ended with the result `{:outcome :failed :error t}`, for
  a throw outside an action. The progress keeps `:start-error` and
  `:errors`."
  [progress t]
  (let [result {:outcome :failed :error t}]
    (assoc progress :result result :action [:done result])))

(defn- alive-workers [obs]
  (vec (keep-indexed (fn [i status] (when (= :alive (:thread status)) i)) (:workers obs))))

(defn- merge-ended-workers
  "Takes the node states that ended worker threads published, for each node
  that the progress has as acquired."
  [nodes obs]
  (reduce-kv (fn [nodes idx status]
               (if (= :ended (:thread status))
                 (reduce-kv (fn [nodes kind state]
                              (cond-> nodes
                                (= :acquired (nodes [kind idx])) (assoc [kind idx] state)))
                            nodes (:nodes status))
                 nodes))
             nodes (vec (:workers obs))))

(defn- add-error [progress outcome]
  (if-let [e (:error outcome)]
    (update progress :errors (fnil conj []) e)
    progress))

(defn record
  "Applies `outcome` of the action under `:action` in `progress`. Outcomes
  are `:ok`, `:timed-out`, `:not-yet` or `{:error t}`. An acquire that could
  not free its partial work has the outcome `{:error t :unfinished? true}`,
  and its node stays as `:unfinished`. Its error counts as a release error."
  [progress outcome]
  (let [[op arg] (:action progress)
        p (dissoc progress :action)
        e (:error outcome)]
    (case op
      (nil :done) p
      :acquire
      (if e
        (cond-> (-> p (dissoc :steps) (assoc :start-error e))
          (:unfinished? outcome) (-> (assoc-in [:nodes arg] :unfinished) (add-error outcome)))
        (let [p (-> p (update :steps subvec 1) (assoc-in [:nodes arg] :acquired))]
          (if (seq (:steps p))
            p
            (-> p (dissoc :steps) (assoc :result {:outcome :started})))))
      :signal-stop (if e (assoc p :result {:outcome :failed :error e}) (assoc p :signalled? true))
      :await-handlers (cond
                        e (assoc p :result {:outcome :failed :error e})
                        (= :timed-out outcome) (assoc p :handlers :timed-out)
                        :else (assoc p :handlers :done))
      :interrupt-handlers (if e (assoc p :result {:outcome :failed :error e}) (assoc p :handlers :done))
      :join-workers (cond-> (assoc p :joined? true) e (assoc :join-error e))
      :force-fail-workers (assoc p :result arg)
      :release (-> p
                   (assoc-in [:nodes arg] (release-state outcome))
                   (add-error outcome)
                   (cond-> (= :not-yet outcome)
                     (update :not-yet (fnil conj #{}) (or (worker-of arg) arg))))
      :close-kept-udp (-> p (update :kept-done (fnil conj #{}) arg) (add-error outcome)))))

(defn- needed-by-unfinished
  "Returns the ids of the nodes that the `:unfinished` nodes in `nodes` need,
  directly or through other nodes."
  [nodes]
  (loop [todo (keep (fn [[id state]] (when (= :unfinished state) id)) nodes)
         needed #{}]
    (if-let [[id & more] (seq todo)]
      (let [new (remove needed (needs id))]
        (recur (concat more new) (into needed new)))
      needed)))

(defn- sweep-result
  "Returns the result of a run that released all it could. The unfinished
  workers hold an acquired node that no `:unfinished` node needs. The errors
  are of the releases that threw and the acquires that could not free their
  work."
  [{:keys [nodes errors]}]
  (if (every? #{:released} (vals nodes))
    {:outcome :stopped}
    (let [needed (needed-by-unfinished nodes)]
      {:outcome :kept
       :unfinished-workers (->> nodes
                                (keep (fn [[id state]]
                                        (when (and (= :acquired state) (not (needed id)))
                                          (worker-of id))))
                                distinct sort vec)
       :errors (or errors [])})))

(defn step
  "Returns `progress` with the next action under `:action`, chosen by
  observation `obs`, after it took the node states that ended worker threads
  published. A node that the next action releases is `:unfinished` already."
  [progress obs]
  (let [p (update progress :nodes merge-ended-workers obs)
        act #(assoc p :action %)
        alive (alive-workers obs)]
    (cond
      (:result p) (act [:done (:result p)])
      (:steps p) (act (first (:steps p)))
      (not (:signalled? p)) (act [:signal-stop])
      (nil? (:handlers p)) (act [:await-handlers])
      (= :timed-out (:handlers p)) (act [:interrupt-handlers])
      (not (:joined? p)) (act [:join-workers])
      (or (seq alive) (:join-error p))
      (act [:force-fail-workers (cond-> {:outcome :not-joined :alive-workers alive}
                                  (:join-error p) (assoc :error (:join-error p)))])
      :else
      (let [nodes (:nodes p)]
        (if-let [id (next-release nodes obs :stopping (:not-yet p #{}))]
          (-> p (assoc-in [:nodes id] :unfinished) (assoc :action [:release id]))
          ;; The UDP sockets of a kept HTTP/3 node close, so that the port can
          ;; be bound again. Its contexts stay.
          (if-let [id (first (filter #(and (= :h3 (kind-of %))
                                           (= :acquired (nodes %))
                                           (not (contains? (:kept-done p) %)))
                                     (release-order (count (:workers obs)))))]
            (act [:close-kept-udp id])
            (recur (assoc p :result (sweep-result p)) obs)))))))

(defn result
  "Returns the result of the last finished run of `progress`, or nil."
  [progress]
  (when (= :done (first (:action progress)))
    (second (:action progress))))

(defn kept?
  "Returns true when `result` keeps native resources of the generation. A run
  that failed keeps them until a later run releases them."
  [result]
  (contains? #{:kept :not-joined :failed} (:outcome result)))

(defn phase
  "Returns the phase of `progress` after a run: `:running`, `:stopping` (the
  stop signal went out and the stop did not end), `:kept` or `:stopped`."
  [progress]
  (let [outcome (:outcome (:result progress))]
    (cond
      (= :stopped outcome) :stopped
      (kept? (:result progress)) :kept
      (not (:signalled? progress)) :running
      :else :stopping)))
