(ns ol.busker.lifecycle.generation-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.lifecycle.generation :as gen]))

(set! *warn-on-reflection* true)

;;; Simulation

(def ^:private boom (ex-info "release failed" {}))

(def ^:private serving
  {:thread :alive :stage :serving :drained? true
   :nodes (zipmap (butlast gen/worker-release-order) (repeat :acquired))})

(def ^:private failed-idle
  (assoc serving :thread :ended :failure (ex-info "loop threw" {})))

(def ^:private failed-holding
  (assoc failed-idle :facts {:context-connections? true}))

(def ^:private never-started {:thread :none})

(defn- world [& statuses]
  {:workers (vec statuses) :releases [] :kept-actions [] :signals 0 :interrupts 0})

(defn- worker-releases
  "Releases, on worker `idx`, each node that the worker may release now."
  [world idx]
  (loop [world world]
    (let [status (get-in world [:workers idx])]
      (if-let [kind (gen/worker-release status (:drained? status))]
        (recur (-> world
                   (assoc-in [:workers idx :nodes kind] :released)
                   (update :releases conj [idx [kind idx]])))
        (cond-> world
          (gen/worker-released? status) (assoc-in [:workers idx :stage] :released))))))

(defn- run-worker
  "Runs the turns of worker `idx` until it waits or its thread ends."
  [world idx]
  (loop [world world]
    (let [{:keys [thread stage stop-requested? drained?]} (get-in world [:workers idx])]
      (if (not= :alive thread)
        world
        (case (gen/worker-turn stage stop-requested? drained?)
          :begin-drain (recur (-> world
                                  (assoc-in [:workers idx :stage] :draining)
                                  (worker-releases idx)))
          :release (recur (worker-releases world idx))
          :idle (assoc-in world [:workers idx :thread] :ended)
          (:serve :wait) world)))))

(defn- start-worker [world progress idx]
  (assoc-in world [:workers idx]
            (assoc serving :nodes (into {:thread :acquired}
                                        (keep (fn [[[kind i] _]] (when (= i idx) [kind :acquired])))
                                        (filter (comp vector? key) (:nodes progress))))))

(defn- perform
  "Performs `action` on `world`. `outcomes` maps an action to an outcome
  other than :ok."
  [world progress [op arg :as action] outcomes]
  (let [outcome (get outcomes action :ok)]
    [(case op
       :acquire (cond-> world
                  (and (= :thread (gen/kind-of arg)) (= :ok outcome)) (start-worker progress (second arg)))
       :signal-stop (cond-> (update world :workers
                                    (partial mapv #(cond-> % (= :alive (:thread %))
                                                           (assoc :stop-requested? true))))
                      (= :ok outcome) (update :signals inc))
       :interrupt-handlers (cond-> world (= :ok outcome) (update :interrupts inc))
       :join-workers (reduce run-worker world (range (count (:workers world))))
       :release (cond-> world (= :ok outcome) (update :releases conj [:stopping arg]))
       :close-kept-udp (update world :kept-actions conj arg)
       world)
     outcome]))

(defn- run
  "Steps `progress` against `world` until a :done action. Returns the final
  progress, world and the actions in order."
  ([world progress] (run world progress {}))
  ([world progress outcomes]
   (loop [p (gen/step progress world) world world actions []]
     (let [action (:action p)]
       (if (or (= :done (first action)) (< 300 (count actions)))
         {:progress p :world world :actions (conj actions action)}
         (let [[world outcome] (perform world p action outcomes)]
           (recur (gen/step (gen/record p outcome) world) world (conj actions action))))))))

(defn- started
  "Returns the progress of a started generation of `n` workers, ready to retire."
  [n]
  (gen/retire (:progress (run (apply world (repeat n never-started)) (gen/start n)))))

(defn- released-by [releaser {:keys [world]}]
  (set (keep (fn [[r id]] (when (= releaser r) id)) (:releases world))))

(defn- states [{:keys [progress]} state]
  (set (keep (fn [[id s]] (when (= state s) id)) (:nodes progress))))

(defn- all-nodes [n]
  (set (gen/release-order n)))

(defn- needed-closure
  "Returns the nodes that node `id` needs, directly or through other nodes."
  [id]
  (loop [todo (vec (gen/needs id)) seen #{}]
    (if-let [[x & more] (seq todo)]
      (if (seen x)
        (recur (vec more) seen)
        (recur (into (vec more) (gen/needs x)) (conj seen x)))
      seen)))

;;; Retirement

(deftest a-stop-of-two-healthy-workers-releases-every-node-once-test
  (let [r (run (world serving serving) (started 2))
        releases (map second (:releases (:world r)))]
    (is (= {:outcome :stopped} (gen/result (:progress r))))
    (is (= :stopped (gen/phase (:progress r))))
    (is (= (all-nodes 2) (set releases)))
    (is (= (count releases) (count (set releases))))
    (testing "each worker releases its nodes that it may release before its thread ends"
      (doseq [i [0 1]
              [kind {:keys [release-at]}] gen/resources
              :when (#{:drain :drained} release-at)]
        (is (contains? (released-by i r) [kind i]) (str [kind i]))))))

(deftest stop-releases-a-failed-worker-that-left-nothing-test
  (let [r (run (world failed-idle serving) (started 2))]
    (is (= {:outcome :stopped} (gen/result (:progress r))))
    (is (= (set (map #(vector % 0) gen/worker-release-order))
           (set (filter #(= [0] (rest %)) (filter vector? (released-by :stopping r))))))
    (is (empty? (:kept-actions (:world r))))))

(deftest stop-keeps-a-generation-whose-failed-worker-holds-a-connection-test
  (let [r (run (world failed-holding serving) (started 2))
        result (gen/result (:progress r))
        worker-0 (filter #(= 0 (gen/worker-of %)) (all-nodes 2))]
    (testing "TCP and Unix listeners and their claims are released"
      (is (every? (states r :released) [[:listeners 0] [:listeners 1] :socket-claims])))
    (testing "the UDP sockets of the kept HTTP/3 node close"
      (is (= [[:h3 0]] (:kept-actions (:world r)))))
    (testing "only what the failed worker holds and what it needs stays"
      (is (contains? (states r :acquired) [:context 0]))
      (is (every? (into (set worker-0) (mapcat needed-closure) worker-0)
                  (states r :acquired))))
    (is (= {:outcome :kept :unfinished-workers [0] :errors []} result))
    (is (= :kept (gen/phase (:progress r))))
    (testing "a later stop releases nothing again and closes no socket again"
      (let [r2 (run (:world r) (gen/retire (:progress r)))]
        (is (= (:releases (:world r)) (:releases (:world r2))))
        (is (= [[:h3 0]] (:kept-actions (:world r2))))
        (is (= result (gen/result (:progress r2))))))))

(deftest stop-keeps-a-generation-whose-failed-worker-pool-cannot-retire-test
  (let [r (run (world failed-idle serving) (started 2) {[:release [:pool 0]] :not-yet})]
    (is (every? (states r :released) [[:listeners 0] [:listeners 1] :socket-claims]))
    (is (= :acquired (get-in r [:progress :nodes [:pool 0]])))
    (is (= [[:h3 0]] (:kept-actions (:world r))))
    (is (= :kept (:outcome (gen/result (:progress r)))))
    (testing "a later stop releases the rest once the pool retires"
      (let [r2 (run (:world r) (gen/retire (:progress r)))
            releases (map second (:releases (:world r2)))]
        (is (= {:outcome :stopped} (gen/result (:progress r2))))
        (is (= (all-nodes 2) (set releases)))
        (is (= (count releases) (count (set releases))))))))

(deftest a-claim-release-that-throws-still-keeps-the-generation-test
  (let [r (run (world failed-holding serving) (started 2) {[:release :socket-claims] {:error boom}})]
    (is (= {:outcome :kept :unfinished-workers [0] :errors [boom]} (gen/result (:progress r))))
    (is (= :unfinished (get-in r [:progress :nodes :socket-claims])))
    (is (= [[:h3 0]] (:kept-actions (:world r))))
    (testing "a later stop still reports the release error"
      (is (= {:outcome :kept :unfinished-workers [0] :errors [boom]}
             (gen/result (:progress (run (:world r) (gen/retire (:progress r))))))))))

(deftest a-worker-alive-at-the-deadline-keeps-everything-until-a-later-stop-test
  (let [stuck (assoc serving :drained? false)
        r (run (world serving stuck) (started 2))]
    (is (= {:outcome :not-joined :alive-workers [1]} (gen/result (:progress r))))
    (is (empty? (released-by :stopping r)))
    (is (= :kept (gen/phase (:progress r))))
    (testing "a later stop releases everything once and signals no second stop"
      (let [w (assoc-in (:world r) [:workers 1 :drained?] true)
            r2 (run w (gen/retire (:progress r)))
            releases (map second (:releases (:world r2)))]
        (is (= {:outcome :stopped} (gen/result (:progress r2))))
        (is (= (all-nodes 2) (set releases)))
        (is (= (count releases) (count (set releases))))
        (is (= 1 (:signals (:world r2))))))))

(deftest an-interrupted-join-keeps-everything-test
  (let [r (run (world failed-idle serving) (started 2) {[:join-workers] {:error boom}})]
    (is (= {:outcome :not-joined :alive-workers [] :error boom} (gen/result (:progress r))))
    (is (empty? (released-by :stopping r)))))

(deftest handlers-that-do-not-end-in-time-are-interrupted-test
  (let [r (run (world serving serving) (started 2) {[:await-handlers] :timed-out})]
    (is (= 1 (:interrupts (:world r))))
    (is (= {:outcome :stopped} (gen/result (:progress r))))))

(deftest a-handler-interrupt-that-throws-ends-the-run-with-its-error-test
  (let [r (run (world serving serving) (started 2)
               {[:await-handlers] :timed-out [:interrupt-handlers] {:error boom}})]
    (is (= {:outcome :failed :error boom} (gen/result (:progress r))))
    (is (= :kept (gen/phase (:progress r))))
    (testing "a later run without the throw stops the generation"
      (is (= {:outcome :stopped}
             (gen/result (:progress (run (:world r) (gen/retire (:progress r))))))))))

(deftest a-worker-release-that-did-not-finish-keeps-the-node-test
  (let [w (world (assoc-in failed-idle [:nodes :pool] :unfinished) serving)
        r (run w (started 2))]
    (is (= :unfinished (get-in r [:progress :nodes [:pool 0]])))
    (is (not (contains? (released-by :stopping r) [:pool 0])))
    (is (= {:outcome :kept :unfinished-workers [] :errors []} (gen/result (:progress r))))))

(deftest a-stop-signal-that-throws-ends-the-run-before-any-release-test
  (let [r (run (world serving serving) (started 2) {[:signal-stop] {:error boom}})]
    (is (= {:outcome :failed :error boom} (gen/result (:progress r))))
    (is (empty? (:releases (:world r))))
    (is (= :kept (gen/phase (:progress r))))
    (testing "a later run without the throw stops the generation"
      (is (= {:outcome :stopped}
             (gen/result (:progress (run (:world r) (gen/retire (:progress r))))))))))

(deftest a-release-that-throws-at-any-node-test
  (doseq [id (gen/release-order 2)]
    (testing (str id)
      (let [r (run (world failed-idle failed-idle) (started 2) {[:release id] {:error boom}})]
        (is (= #{id} (states r :unfinished)))
        (is (= (needed-closure id) (states r :acquired)))
        (is (= (disj (all-nodes 2) id) (into (states r :released) (needed-closure id))))
        (is (= [boom] (:errors (gen/result (:progress r)))))
        (testing "a second retirement releases nothing again"
          (let [r2 (run (:world r) (gen/retire (:progress r)))]
            (is (= (:releases (:world r)) (:releases (:world r2))))
            (is (= :kept (:outcome (gen/result (:progress r2)))))))))))

;;; Ended workers

(deftest an-ended-worker-is-released-only-when-it-left-nothing-to-finish-test
  (let [outcome (fn [status]
                  (:outcome (gen/result (:progress (run (world status serving) (started 2))))))
        ended (assoc serving :thread :ended)]
    (is (= :stopped (outcome ended)))
    (doseq [fact [:commands? :responses? :blocks?]]
      (is (= :kept (outcome (assoc ended :facts {fact true}))) (str fact))
      (is (= :kept (outcome (assoc ended :failure boom :facts {fact true}))) (str fact)))
    (testing "connections count only for a failed worker, whose loop cannot close them"
      (doseq [fact [:generation-connections? :context-connections? :h3-connections?]]
        (is (= :stopped (outcome (assoc ended :facts {fact true}))) (str fact))
        (is (= :kept (outcome (assoc ended :failure boom :facts {fact true}))) (str fact))))))

(deftest native-counts-are-read-only-from-nodes-nothing-freed-test
  (is (gen/readable? {[:context 0] :acquired} {} :context 0))
  (is (gen/readable? {[:context 0] :acquired} {:nodes {:context :acquired}} :context 0))
  (is (not (gen/readable? {[:context 0] :acquired} {:nodes {:context :unfinished}} :context 0)))
  (is (not (gen/readable? {[:context 0] :acquired} {:nodes {:context :released}} :context 0)))
  (is (not (gen/readable? {[:context 0] :unfinished} {} :context 0)))
  (is (not (gen/readable? {} {} :context 0))))

(deftest the-worker-releases-its-nodes-in-stage-order-test
  (is (= :listeners (gen/worker-release {:stage :draining} false)))
  (is (nil? (gen/worker-release {:stage :serving} true)))
  (is (nil? (gen/worker-release {:stage :draining :nodes {:listeners :released}} false)))
  (is (= :response-receiver (gen/worker-release {:stage :draining :nodes {:listeners :released}} true)))
  (testing "a node that did not finish keeps what it needs"
    (is (= :pool (gen/worker-release {:stage :draining
                                      :nodes {:listeners :released :response-receiver :unfinished}}
                                     true))))
  (let [statuses (for [stage [:serving :draining :released]
                       states (reduce (fn [acc kind]
                                        (for [m acc state [:acquired :released :unfinished]]
                                          (assoc m kind state)))
                                      [{}]
                                      gen/worker-release-order)]
                   {:stage stage :nodes states})]
    (testing "while the receiver or the pool waits, the worker frees no node the drain check reads"
      (is (empty? (for [status statuses
                        :when (some #(= :acquired (get-in status [:nodes %])) [:response-receiver :pool])
                        drained? [false true]
                        :when (#{:h3 :context} (gen/worker-release status drained?))]
                    status))))
    (testing "the worker never releases its callback dispatch or its thread"
      (is (empty? (for [status statuses
                        drained? [false true]
                        :when (#{:callback-dispatch :thread} (gen/worker-release status drained?))]
                    status)))))
  (is (not (gen/worker-released? {:nodes {:listeners :released}})))
  (is (gen/worker-released? {:nodes {:context :released}})))

(deftest an-acquire-that-could-not-free-its-partial-work-keeps-the-generation-test
  (let [steps (:steps (gen/start 2))
        k (.indexOf ^java.util.List steps [:acquire [:context 1]])
        r (run (world never-started never-started) (gen/start 2)
               {[:acquire [:context 1]] {:error boom :unfinished? true}})
        acquired (set (keep (fn [[op id]] (when (= :acquire op) id)) (take k steps)))]
    (is (= :unfinished (get-in r [:progress :nodes [:context 1]])))
    (testing "what the node needs stays, everything else is released"
      (is (= (needed-closure [:context 1]) (states r :acquired)))
      (is (= (apply disj acquired (needed-closure [:context 1])) (states r :released))))
    (testing "no worker counts as unfinished, and the error counts as a release error"
      (is (= {:outcome :kept :unfinished-workers [] :errors [boom]} (gen/result (:progress r))))
      (is (= boom (get-in r [:progress :start-error]))))
    (is (= :kept (gen/phase (:progress r))))))

(deftest a-throw-outside-an-action-fails-the-run-and-a-later-run-goes-on-test
  (let [p (-> (started 2) (assoc-in [:nodes [:pool 0]] :unfinished) (gen/fail boom))]
    (is (= {:outcome :failed :error boom} (gen/result p)))
    (is (= :kept (gen/phase p)))
    (testing "the node that the throw left unfinished stays acquired"
      (let [r2 (run (world failed-idle serving) (gen/retire p))]
        (is (= :unfinished (get-in r2 [:progress :nodes [:pool 0]])))
        (is (not (contains? (released-by :stopping r2) [:pool 0])))))))

;;; Start

(deftest every-node-needs-only-nodes-acquired-before-it-test
  (let [order (vec (gen/acquire-order 3))
        before (fn [id] (set (subvec order 0 (.indexOf ^java.util.List order id))))]
    (doseq [id order
            needed (gen/needs id)]
      (is (contains? (before id) needed) (str id " needs " needed)))))

(deftest every-node-is-in-release-order-before-the-nodes-it-needs-test
  (let [order (vec (gen/release-order 3))
        after (fn [id] (set (subvec order (inc (.indexOf ^java.util.List order id)))))]
    (doseq [id order
            needed (gen/needs id)]
      (is (contains? (after id) needed) (str id " needs " needed)))))
