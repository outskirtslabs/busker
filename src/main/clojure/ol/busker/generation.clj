(ns ^:no-doc ol.busker.generation
  "Starts, drains, reloads, and stops one native server generation.

  [[ol.busker.lifecycle.generation]] decides each lifecycle step, and
  [[ol.busker.generation.effects]] acquires and releases each native
  resource. This namespace runs the steps: [[start!]] and [[stop!]] are the
  same loop. It also runs the worker loop, which releases the resources of a
  worker when it drains. Lifecycle allocation and disposal run on platform
  threads. Handlers and close callbacks use virtual executors.

  Each worker receives `:fixed-response-slots` fixed-response slots and a separate shared streaming
  arena of 256 * `:output-buffer-size` bytes (8 MiB at the default size).
  Graceful draining leaves admitted responses usable. Receiver admission closes
  before retirement, but entered producers and native sends keep storage valid
  until publication, discard or cleanup. A worker thread still alive at the
  timeout, or a failed worker that left connections, blocks or responses,
  makes retirement report failure and keep the generation rather than free
  live data. A failed worker whose thread has ended and left none of these is
  released by the stopping thread.

  A start that fails releases what it acquired by the rules of a stop, and
  throws its first error.

  ## Related Namespaces

  - [[ol.busker.runtime]] selects active generations.
  - [[ol.busker.evloop]] runs worker threads."
  (:require
   [babashka.ffi :as mem]
   [clojure.string :as str]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.config :as config]
   [ol.busker.evloop :as evloop]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.generation.accept :as accept]
   [ol.busker.generation.effects :as effects]
   [ol.busker.internal.protocols :as p]
   [ol.busker.lifecycle.generation :as gen]
   [ol.busker.listen :as listen]
   [ol.busker.native :as h2o]
   [ol.busker.request-dispatch :as request-dispatch]
   [ol.busker.response-queue :as rq]
   [ol.busker.tickets :as tickets]
   [ol.busker.worker-context :as worker-context]
   [ol.busker.worker-mailbox :as mailbox]
   [taoensso.trove :as trove])
  (:import
   [java.util.concurrent ExecutorService Executors TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong AtomicLongArray AtomicReference
    AtomicReferenceArray]))

(set! *warn-on-reflection* true)

(defn- new-request-executor ^ExecutorService []
  (Executors/newVirtualThreadPerTaskExecutor))

(defonce ^:private failed-retirements_ (atom []))

(defn- retain-failed-retirement!
  [generation error]
  (swap! failed-retirements_ (fn [retirements]
                               (cond-> retirements
                                 (not-any? #(identical? generation %) retirements)
                                 (conj generation))))
  (try
    (trove/log! {:level :error
                 :id ::callback-retirement-failed
                 :error error
                 :data {:generation-id (::generation-id generation)}})
    (catch Throwable _ nil)))

(defn- remove-failed-retirement!
  [generation]
  (swap! failed-retirements_
         (fn [retirements]
           (vec (remove #(identical? generation %) retirements)))))

(defn- with-live-request
  [module-id request-seq f]
  (when-let [worker (worker-context/get-current-worker)]
    (when-let [[req _] (callback-dispatch/entry (:callback-dispatch worker)
                                                module-id request-seq)]
      (f req))))

(defn evloop-msg-processor
  [op args]
  (case op
    :h2o/proceed-request
    (let [[module-id request-seq] args]
      (with-live-request module-id request-seq
        (fn [req]
          (h2o/proceed-req (:req (:req-ctx req))))))

    :h2o/output
    (let [[module-id request-seq st record] args
          worker (worker-context/get-current-worker)]
      (if (and worker (callback-dispatch/entry (:callback-dispatch worker)
                                               module-id request-seq))
        (rq/consume-record! st record)
        (rq/discard-record! st record)))
    nil))

;;; Worker

(defn- has-active-http3-connections?
  [http3-ctx]
  (and http3-ctx
       (not (mem/null? http3-ctx))
       (pos? (h2o/http3-num-connections http3-ctx))))

(defn- all-connections-drained?
  "Returns true when the worker at `worker-idx` holds no connection. It does not
  wait for the connections of other workers, which a failed worker never closes."
  [ctx-ptr http3-ctxs ^AtomicLongArray counts worker-idx]
  (and (zero? (h2o/context-get-active-conns ctx-ptr))
       (zero? (h2o/context-get-shutdown-conns ctx-ptr))
       (zero? (.get counts (int worker-idx)))
       (not-any? has-active-http3-connections? http3-ctxs)))

(defn- response-exchange-drained?
  [worker]
  (let [receiver_ (:response-receiver_ worker)
        receiver (when receiver_ (.get ^AtomicReference receiver_))]
    (or (nil? receiver)
        (mem/null? receiver)
        (zero? (h2o/mt-response-pending receiver)))))

(defn- worker-native-failure! [worker cause]
  (when (and (.compareAndSet ^AtomicReference (:native-failure_ worker) nil cause)
             (not (identical? cause (.get ^AtomicReference (:termination-cause_ worker)))))
    (accept/log-callback-failure! ::worker-failed cause))
  (.set ^AtomicBoolean (:accepting?_ worker) false)
  (.set ^AtomicBoolean (:running?_ worker) false)
  (mailbox/close! (:mailbox worker))
  (effects/close-response-receiver-admission! worker)
  (callback-dispatch/fail-responses! (:callback-dispatch worker) cause)
  (when-let [pool (:stream-pool worker)]
    (bp/wake-all! pool)))

(defn- worker-drained?
  "Returns true when the worker of `state` holds no connection, response,
  command, block or callback entry. Call it on the worker thread."
  [worker {:keys [ctx-ptr http3-ctxs worker-connection-counts worker-idx]}]
  (and (all-connections-drained? ctx-ptr http3-ctxs worker-connection-counts worker-idx)
       (response-exchange-drained? worker)
       (not (mailbox/pending? (:mailbox worker)))
       (zero? (bp/checked-out (:stream-pool worker)))
       (callback-dispatch/no-live-entries? (:callback-dispatch worker))))

(def ^:private serving-status {:stage :serving})

(defn- publish-status!
  "Sets the status of the worker of `state` and delivers the stopped-accepting
  promise of the generation once every worker failed or closed its
  listeners. Only the worker thread calls it."
  [{:keys [^AtomicReferenceArray statuses worker-idx stopped-accepting_]} status]
  (.set statuses (int worker-idx) status)
  (when (every? #(let [status (.get statuses (int %))]
                   (or (some? (:failure status))
                       (= :released (get-in status [:nodes :listeners]))))
                (range (.length statuses)))
    (deliver stopped-accepting_ true)))

(defn- withdraw-http3-worker!
  "Stops other workers from sending new HTTP/3 connections to this worker,
  unless its `status` shows its HTTP/3 contexts released. Call it on the
  worker thread."
  [status http3-ctxs]
  (when (contains? #{nil :acquired} (get-in status [:nodes :h3]))
    (run! h2o/http3-withdraw-worker http3-ctxs)))

(declare release-node)

(defn- release-worker-nodes!
  "Releases, on the worker thread, each node of `worker` that the core lets it
  release now. Publishes the node as `:unfinished` before each release and its
  new state after it. The release itself cannot skip the second publish,
  because its throws are caught. The first publish covers an `Error`, for
  example a `StackOverflowError`, between the end of the release and the
  second publish: the thread then ends with the node `:unfinished`, and the
  stopping thread does not release it again. Returns the last status. Stops
  at a release that returns not yet, and throws the error of a release that
  threw, which fails the worker."
  [worker state status drained?]
  (let [handles (assoc state :worker worker)]
    (loop [status status]
      (if-let [kind (gen/worker-release status drained?)]
        (let [_ (publish-status! state (assoc-in status [:nodes kind] :unfinished))
              outcome (try
                        (release-node handles kind 0)
                        (catch Throwable t {:error t}))
              status (assoc-in status [:nodes kind] (gen/release-state outcome))]
          (publish-status! state status)
          (cond
            (:error outcome) (throw (:error outcome))
            (= :not-yet outcome) status
            :else (recur status)))
        status))))

(defn- begin-drain!
  "Stops the worker reading its listeners, closes them and asks H2O to close
  its connections. Returns the stage `:draining`."
  [worker {:keys [listener-socks http3-ctxs loop-ptr ctx-ptr ^AtomicReferenceArray statuses
                  worker-idx]
           :as state}]
  (doseq [sock-ptr listener-socks]
    (when (and (not (mem/null? sock-ptr))
               (not (zero? (h2o/socket-reading? sock-ptr))))
      (h2o/socket-read-stop sock-ptr)))
  (run! h2o/http3-stop-accepting http3-ctxs)
  (h2o/evloop-run loop-ptr 0)
  (let [status (assoc (.get statuses (int worker-idx)) :stage :draining)]
    (publish-status! state status)
    (release-worker-nodes! worker state status false))
  (h2o/context-request-shutdown ctx-ptr)
  :draining)

(defn- finish-drain!
  "Releases the nodes of a drained worker. Returns `:released` once the core
  sees the worker released, else `:draining`."
  [worker {:keys [^AtomicReferenceArray statuses worker-idx] :as state}]
  (let [status (release-worker-nodes! worker state (.get statuses (int worker-idx)) true)]
    (if-not (gen/worker-released? status)
      :draining
      (do
        (publish-status! state (assoc status :stage :released))
        (when (= :overloaded (p/send-msg worker evloop/stop-msg))
          (throw (IllegalStateException. "Stop request cannot overload")))
        :released))))

(defn- lifecycle-turn!
  "Performs the lifecycle action that [[gen/worker-turn]] picks for `worker` in
  `stage`. Returns the next stage."
  [worker stage {:keys [^AtomicBoolean shutting-down?] :as state}]
  (loop [stage stage]
    (case (gen/worker-turn stage
                           (.get shutting-down?)
                           (and (identical? :draining stage) (worker-drained? worker state)))
      :begin-drain (recur (begin-drain! worker state))
      :release (finish-drain! worker state)
      stage)))

(defn- worker-loop
  [worker stage mailbox-work? {:keys [loop-ptr ctx-ptr] :as state}]
  (when-let [failure (.get ^AtomicReference (:native-failure_ worker))]
    (throw failure))
  (let [stage (lifecycle-turn! worker stage state)
        disposed? (identical? :released stage)
        receiver_ (:response-receiver_ worker)
        receiver (when receiver_ (.get ^AtomicReference receiver_))
        ring-work? (and (not disposed?)
                        receiver
                        (not (mem/null? receiver))
                        (pos? (h2o/mt-response-ring-drain receiver)))
        sent? (when-not disposed?
                (fixed-final/notify-freed! worker)
                (callback-dispatch/drain-responses! (:callback-dispatch worker)))
        shutting? (not (identical? :serving stage))
        tasks-queued? (when-let [reuse (:request-dispatch worker)]
                        (request-dispatch/sweep! reuse))]
    (if disposed?
      (h2o/evloop-run loop-ptr 0)
      (let [now (h2o/evloop-now loop-ptr)
            base-wait (h2o/cleanup-thread now ctx-ptr)
            wait-ms (cond
                      ring-work? 0
                      mailbox-work? 0
                      ;; A write that completes at once leaves its callback on
                      ;; H2O's pending list, and the poll's wait ignores that list.
                      sent? 0
                      (callback-dispatch/pending-response-work?
                       (:callback-dispatch worker)) 0
                      shutting? 10
                      :else base-wait)]
        (when-not shutting?
          (accept/update-listener-state! state))
        (h2o/evloop-run loop-ptr (cond
                                   (pos? (p/count-msgs worker)) 0
                                   tasks-queued? (min 1 wait-ms)
                                   :else wait-ms))))
    stage))

(defn- zero-wait-turn-fn
  "Returns the hot poll's native turn. With a request dispatch, the turn also
  sweeps it while tasks are queued, so a request behind a slow handler gets
  its thread during the poll."
  [loop-ptr request-dispatch]
  (if request-dispatch
    (fn []
      (h2o/evloop-run loop-ptr 0)
      (when (request-dispatch/queued? request-dispatch)
        (request-dispatch/sweep! request-dispatch)))
    (fn [] (h2o/evloop-run loop-ptr 0))))

(defn- acquire-thread
  "Starts the thread of worker `i` of generation map `g`. Every other node of
  the worker is acquired. Returns `g` with the worker."
  [{::keys [config executor loops max-connections shutting-down? wakeup-receivers
            response-receivers response-slots stream-pools callback-dispatches
            large-head-warnings
            worker-statuses stopped-accepting_ worker-connection-counts accepting-workers
            limit-waiting]
    :as g}
   i]
  (let [loop-ptr (nth loops i)
        request-dispatch (when (= :reuse-within-connection (:request-threads config))
                           (request-dispatch/create executor))
        {:keys [fixed-body-limit payload-capacity]} (effects/fixed-response-sizes config)
        state (merge (effects/worker-handles g i)
                     {:loop-ptr loop-ptr
                      :max-connections max-connections
                      :shutting-down? shutting-down?
                      :worker-connection-counts worker-connection-counts
                      :accepting-workers accepting-workers
                      :limit-waiting limit-waiting
                      :statuses worker-statuses
                      :stopped-accepting_ stopped-accepting_})
        worker (evloop/start-worker!
                (fn [worker stage mailbox-work?]
                  (worker-loop worker stage mailbox-work? state))
                evloop-msg-processor
                (nth wakeup-receivers i)
                :loop-state :serving
                :stream-pool (nth stream-pools i)
                :on-native-failure!
                (fn [worker cause]
                  (try
                    (worker-native-failure! worker cause)
                    (finally
                      (.set ^AtomicLongArray worker-connection-counts (int i) Long/MAX_VALUE)
                      (let [status (.get ^AtomicReferenceArray worker-statuses (int i))]
                        (publish-status! state (assoc status :failure cause))
                        (withdraw-http3-worker! status (:http3-ctxs state))
                        (accept/wake-other-workers! accepting-workers i)))))
                :discard-command! (fn [[op _ _ st record]]
                                    (when (= :h2o/output op)
                                      (rq/discard-record! st record)))
                :response-receiver (nth response-receivers i)
                :response-slots (nth response-slots i)
                :response-slot-payload-capacity payload-capacity
                :fixed-body-limit fixed-body-limit
                :large-head-warnings large-head-warnings
                :callback-dispatch (nth callback-dispatches i)
                :request-dispatch request-dispatch
                :zero-wait-turn! (zero-wait-turn-fn loop-ptr request-dispatch)
                :hot-poll-ns (* 1000 (long (:worker-hot-poll-micros config))))]
    (.set ^AtomicReferenceArray accepting-workers (int i) worker)
    (assoc-in g [::workers i] worker)))

(def node-effects
  "The acquire and the release effect of each resource kind, see
  [[effects/table]]. The worker thread is here, because its loop is in this
  namespace. Its release does nothing: the join ends the thread."
  (assoc effects/table :thread {:acquire acquire-thread
                                :release (fn [_ _])}))

(defn- release-node
  "Releases the node of `kind` with `x`, see [[effects/table]]. Returns
  `:not-yet` while the node is in use, else `:ok`."
  [x kind timeout-ns]
  (if (identical? :not-yet ((:release (node-effects kind)) x timeout-ns))
    :not-yet
    :ok))

;;; Generation

(defn- prepare-generation-state
  [compiled-config cert-runtime listener-pool generation-id memory-ticket-service]
  (let [{:keys [n-workers max-connections]} compiled-config]
    {::ring-handler (-> compiled-config
                        config/dispatch-handler
                        (clave-adapter/wrap-handler cert-runtime))
     ::generation-id generation-id
     ::config compiled-config
     ::cert-runtime cert-runtime
     ::memory-ticket-service memory-ticket-service
     ::listener-pool listener-pool
     ::listener-claims {}
     ::executor (new-request-executor)
     ::callback-dispatches (vec (repeat n-workers nil))
     ::contexts (vec (repeat n-workers nil))
     ::wakeup-receivers (vec (repeat n-workers nil))
     ::response-receivers (vec (repeat n-workers nil))
     ::response-slots (vec (repeat n-workers nil))
     ::stream-pools (vec (repeat n-workers nil))
     ::listener-sockets (vec (repeat n-workers nil))
     ::http3-worker-contexts (vec (repeat n-workers nil))
     ::workers (vec (repeat n-workers nil))
     ::progress_ (atom {})
     ::stop-lock (Object.)
     ::shutting-down? (AtomicBoolean. false)
     ::active-connection-count_ (AtomicLong. 0)
     ::worker-connection-counts (AtomicLongArray. (int n-workers))
     ::accepting-workers (AtomicReferenceArray. (int n-workers))
     ::limit-waiting (AtomicBoolean. false)
     ::large-head-warnings (when (:large-response-head-warnings? compiled-config)
                             (doto (AtomicLongArray. 2) (.set 0 (System/nanoTime))))
     ::worker-statuses (AtomicReferenceArray. ^objects (object-array (repeat n-workers serving-status)))
     ::stopped-accepting_ (promise)
     ::n-workers n-workers
     ::max-connections max-connections}))

(defn- http3-transport-resources
  [claims]
  (->> claims
       (filter (fn [[claim-key _]]
                 (= :udp (:transport claim-key))))
       (map (fn [[_ claim]] (listen/resource claim)))
       distinct
       vec))

(defn activate-http3-transports!
  [{::keys [generation-id listener-claims] :as generation}]
  (doseq [transport (http3-transport-resources listener-claims)]
    (when (and transport (not (mem/null? transport)))
      (h2o/http3-activate-udp-transport-generation transport generation-id)))
  generation)

(defn register-shared-ticket-manager!
  [{::keys [memory-ticket-service key-manager] :as generation}]
  (when (and memory-ticket-service key-manager)
    (tickets/start-key-manager! key-manager))
  generation)

(defn failed-worker?
  "Returns true when a worker of `generation` has failed."
  [generation]
  (boolean (some #(some-> ^AtomicReference (:native-failure_ %) .get) (::workers generation))))

(defn- ended-worker-facts
  "Returns what the ended thread of the worker at `i` left, for
  [[gen/ended-drained?]]. It reads native counts only from nodes that
  [[gen/readable?]] allows."
  [{::keys [^AtomicLong active-connection-count_] :as g} nodes i worker status]
  (let [readable? #(gen/readable? nodes status % i)
        {:keys [ctx-ptr http3-ctxs]} (effects/worker-handles g i)]
    {:commands? (mailbox/pending? (:mailbox worker))
     :responses? (and (readable? :response-receiver) (not (response-exchange-drained? worker)))
     :blocks? (pos? (bp/checked-out (:stream-pool worker)))
     :generation-connections? (pos? (.get active-connection-count_))
     :context-connections? (and (readable? :context)
                                (or (pos? (h2o/context-get-active-conns ctx-ptr))
                                    (pos? (h2o/context-get-shutdown-conns ctx-ptr))))
     :h3-connections? (boolean (and (readable? :h3)
                                    (some has-active-http3-connections? http3-ctxs)))}))

(defn- observe
  "Returns the observation of generation `g` for [[gen/step]], with the node
  states `nodes` of its progress."
  [{::keys [workers ^AtomicReferenceArray worker-statuses n-workers] :as g} nodes]
  {:workers (mapv (fn [i]
                    (let [worker (get workers i)
                          ^Thread thread (:thread worker)]
                      (cond
                        (nil? thread) {:thread :none}
                        (.isAlive thread) {:thread :alive}
                        :else (let [status (.get worker-statuses (int i))]
                                (assoc status
                                       :thread :ended
                                       :facts (ended-worker-facts g nodes i worker status))))))
                  (range n-workers))})

(defn- retirement-error
  "Returns the error of retirement `result` that is not stopped."
  [{::keys [workers]} {:keys [outcome error alive-workers unfinished-workers errors]}]
  (let [ids (fn [idxs] (mapv #(:id (get workers %) %) idxs))]
    (case outcome
      :failed error
      :not-joined (or error
                      (ex-info "Generation retirement timed out while worker threads still run. Native resources kept."
                               {:alive-workers (ids alive-workers)}))
      :kept (let [^Throwable e (ex-info
                                (str/join " " (cond-> []
                                                (seq unfinished-workers)
                                                (conj "Worker threads ended with connections, responses or blocks nothing can finish. TCP and Unix listeners and HTTP/3 UDP sockets closed. Loops, contexts and memory kept.")
                                                (or (seq errors) (empty? unfinished-workers))
                                                (conj "A native release threw or an acquire could not free its work. Its resources and the resources that they need are kept.")))
                                {:unfinished-workers (ids unfinished-workers)})]
              (run! #(.addSuppressed e ^Throwable %) errors)
              e))))

(def ^:private action-effects
  "Performs each action of [[gen/step]]: `(fn [g arg timeout-ns])` returns
  `[g outcome]`, with the new handles after an acquire."
  {:acquire (fn [g id _]
              [((:acquire (node-effects (gen/kind-of id))) g (gen/worker-of id)) :ok])
   :signal-stop (fn [g _ _] (effects/signal-stop! g) [g :ok])
   :await-handlers (fn [{::keys [^ExecutorService executor] :as g} _ ^long timeout-ns]
                     [g (if (.awaitTermination executor timeout-ns TimeUnit/NANOSECONDS)
                          :ok
                          :timed-out)])
   :interrupt-handlers (fn [g _ timeout-ns] (effects/interrupt-handlers! g timeout-ns) [g :ok])
   :join-workers (fn [g _ timeout-ns] (effects/join-workers! g timeout-ns) [g :ok])
   :force-fail-workers (fn [{::keys [workers] :as g} result _]
                         (let [error (retirement-error g result)]
                           (doseq [i (:alive-workers result) :let [worker (get workers i)]]
                             (.compareAndSet ^AtomicReference (:termination-cause_ worker) nil error)
                             (p/wake worker))
                           [g :ok]))
   :release (fn [g id timeout-ns]
              [g (release-node (if-let [i (gen/worker-of id)] (effects/worker-handles g i) g)
                               (gen/kind-of id) timeout-ns)])
   :close-kept-udp (fn [g id _] (effects/close-kept-udp! g (gen/worker-of id)) [g :ok])})

(defn- run-lifecycle!
  "Performs the actions that [[gen/step]] picks for generation map `g` from
  `progress`, while `more?` holds for the next action. Returns `[g p]`, with
  `p` the last progress. [[gen/record]] takes each outcome before the next
  observation, so a throw outside an action ends the run with every outcome
  recorded, see [[gen/fail]]. Each action starts with the interrupt flag
  clear, so that an interrupt does not end the waits of the run. The flag is
  set again when the run ends."
  [g progress ^long timeout-ns more?]
  (loop [g g
         p progress
         interrupted? false]
    (let [p (try
              (gen/step p (observe g (:nodes p)))
              (catch Throwable t
                (gen/fail p t)))
          [op arg] (:action p)]
      (if (more? op)
        (let [cleared? (Thread/interrupted)
              [g outcome] (try
                            ((action-effects op) g arg timeout-ns)
                            (catch Throwable t
                              [g (if-let [first-error (effects/partial-acquire t)]
                                   {:error first-error :unfinished? true}
                                   {:error t})]))]
          (recur g
                 (try
                   (gen/record p outcome)
                   (catch Throwable t
                     (gen/fail p t)))
                 (or interrupted?
                     cleared?
                     (instance? InterruptedException (:error outcome)))))
        (do
          (when interrupted?
            (.interrupt (Thread/currentThread)))
          [g p])))))

(defn- report-retirement!
  "Records the retirement `result` of `generation` in the list of kept
  generations, and throws its error when it is not stopped."
  [generation result]
  (case (:outcome result)
    nil nil
    :stopped (remove-failed-retirement! generation)
    (let [error (retirement-error generation result)]
      (retain-failed-retirement! generation error)
      (throw error))))

(defn- retire!
  "Runs the retirement of `generation` while `more?` holds for the next
  action, writes its progress and reports its result."
  [{::keys [stop-lock progress_] :as generation} ^long timeout-ns more?]
  (locking stop-lock
    (let [[_ p] (run-lifecycle! generation (gen/retire @progress_) timeout-ns more?)]
      (reset! progress_ p)
      (report-retirement! generation (gen/result p)))))

(defn phase
  "Returns the lifecycle phase of `generation`: `:running`, `:stopping`,
  `:kept` or `:stopped`."
  [generation]
  (gen/phase @(::progress_ generation)))

(defn begin-stop!
  "Stops `generation` accepting new work, once. Returns `generation`."
  [generation]
  (retire! generation 0 #{:signal-stop})
  generation)

(def ^:private stop-accepting-wait-ms 60000)

(defn await-stop-accepting!
  "Waits up to 60 s for every worker of `generation` to stop accepting after
  [[begin-stop!]] and throws when one has not. It leaves the generation in
  `:stopping`, so [[stop!]] still waits for its request handlers and workers."
  [{::keys [stopped-accepting_] :as generation}]
  (when (= ::timeout (deref stopped-accepting_ stop-accepting-wait-ms ::timeout))
    (throw (ex-info "Workers did not stop accepting"
                    {:generation-id (::generation-id generation)})))
  nil)

(defn stop!
  "Retires `generation`: stops it accepting, waits up to `timeout` for its
  request handlers and worker threads, and releases its native resources.
  Throws when it must keep the generation. A later call goes on from where
  the last one ended."
  ([generation]
   (stop! generation 60 TimeUnit/SECONDS))
  ([generation ^long timeout ^TimeUnit timeunit]
   (assert generation)
   (retire! generation (.toNanos timeunit timeout) #(not= :done %))
   nil))

(defn start!
  "Starts a generation for `compiled-config`. A start that fails releases what
  it acquired, and throws the first error with the errors of the release
  suppressed. When the release must keep resources, the generation is
  recorded as kept."
  ([compiled-config cert-runtime]
   (start! compiled-config cert-runtime {}))
  ([compiled-config cert-runtime {:keys [generation-id listener-pool memory-ticket-service]
                                  :or {generation-id 1
                                       listener-pool (listen/open-pool)}}]
   (let [g (prepare-generation-state (config/config->listeners compiled-config) cert-runtime
                                     listener-pool generation-id memory-ticket-service)
         [g p] (run-lifecycle! g (gen/start (::n-workers g))
                               (.toNanos TimeUnit/SECONDS 60) #(not= :done %))
         result (gen/result p)]
     (reset! (::progress_ g) p)
     (if (= :started (:outcome result))
       g
       (let [^Throwable start-error (or (:start-error p) (:error result))
             kept-error (when (gen/kept? result)
                          (retirement-error g result))]
         (some->> kept-error (retain-failed-retirement! g))
         (doseq [^Throwable e (if (= :not-joined (:outcome result))
                                [kept-error]
                                (cons (:error result) (:errors p)))
                 :when (and e (not (identical? e start-error)))]
           (.addSuppressed start-error e))
         (throw start-error))))))
