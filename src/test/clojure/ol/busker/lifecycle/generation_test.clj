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
  (assoc failed-idle :drained? false))

(def ^:private never-started {:thread :none})

(defn- world [& statuses]
  {:workers (vec statuses) :releases [] :kept-actions []})

(defn- worker-releases
  "Releases, on worker `idx`, each node that the worker may release now."
  [world idx]
  (loop [world world]
    (let [nodes (update-keys (get-in world [:workers idx :nodes]) #(vector % idx))]
      (if-let [id (gen/next-release nodes world idx #{})]
        (recur (-> world
                   (assoc-in [:workers idx :nodes (first id)] :released)
                   (update :releases conj [idx id])))
        (cond-> world
          (= :released (get-in world [:workers idx :nodes :context]))
          (assoc-in [:workers idx :stage] :released))))))

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
            (assoc serving :nodes (into {:callback-dispatch :acquired}
                                        (keep (fn [[[kind i] _]] (when (= i idx) [kind :acquired])))
                                        (filter (comp vector? key) (:nodes progress))))))

(defn- perform
  "Performs `action` on `world`. `outcomes` maps an action to an outcome
  other than :ok."
  [world progress [op arg :as action] outcomes]
  (let [outcome (get outcomes action :ok)]
    [(case op
       :acquire (cond-> world
                  (and (vector? arg) (= :ok outcome)) (start-worker progress (second arg)))
       :signal-stop (update world :workers
                            (partial mapv #(cond-> % (= :alive (:thread %))
                                                   (assoc :stop-requested? true))))
       :join-workers (reduce run-worker world (range (count (:workers world))))
       :release (cond-> world (= :ok outcome) (update :releases conj [:stopping arg]))
       :when-kept (update world :kept-actions conj arg)
       world)
     outcome]))

(defn- run
  "Steps `progress` against `world` until a :done action. Returns the final
  progress, world and the actions in order."
  ([world progress] (run world progress {}))
  ([world progress outcomes]
   (loop [p (gen/step progress world nil) world world actions []]
     (let [action (:action p)]
       (if (or (= :done (first action)) (< 300 (count actions)))
         {:progress p :world world :actions (conj actions action)}
         (let [[world outcome] (perform world p action outcomes)]
           (recur (gen/step p world outcome) world (conj actions action))))))))

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

(defn- needed-closure [id]
  (loop [todo (vec (gen/needs id)) seen #{}]
    (if-let [[x & more] (seq todo)]
      (if (seen x)
        (recur (vec more) seen)
        (recur (into (vec more) (gen/needs x)) (conj seen x)))
      seen)))

;;; Worker

(deftest worker-turn-covers-every-input-test
  (doseq [stage [:serving :draining :released]
          stop? [false true]
          drained? [false true]]
    (is (= (case stage
             :serving (if stop? :begin-drain :serve)
             :draining (if drained? :release :wait)
             :released :idle)
           (gen/worker-turn stage stop? drained?)))))

(deftest a-failed-worker-stops-accepting-at-once-test
  (is (gen/stopped-accepting? {:stage :serving :failure boom}))
  (is (gen/stopped-accepting? {:stage :draining}))
  (is (not (gen/stopped-accepting? {:stage :serving})))
  (is (not (gen/all-stopped-accepting? [{:stage :serving :failure boom} {:stage :serving}])))
  (is (gen/all-stopped-accepting? [{:stage :serving :failure boom} {:stage :draining}])))

(deftest a-worker-releases-nothing-of-the-generation-test
  (let [w (world (assoc serving :stage :draining) serving)]
    (doseq [id (gen/release-order 2)
            :when (not= 0 (and (vector? id) (second id)))]
      (is (not (gen/quiet? w 0 id)) (str id)))
    (is (not (gen/quiet? w 0 [:callback-dispatch 0])))))

;;; Resource table

(def ^:private todays-stop-order
  "Today's release order in ol.busker.generation for worker 0 that stops
  normally (dispose-context-if-ready), failed worker 1 (release-failed-worker!)
  and context 2 that has no worker thread (the release branch of stop!)."
  (concat [[:listeners 0] [:response-receiver 0] [:pool 0] [:h3 0] [:context 0]]
          [[:listeners 1] [:response-receiver 1] [:pool 1] [:h3 1] [:context 1]]
          [[:callback-dispatch 0] [:callback-dispatch 1] :close-executor
           [:listeners 2] [:h3 2] [:response-receiver 2] [:context 2]
           :h3-contexts :ticket-manager :loops :config :ssl-ctxs [:pool 2]
           :socket-claims :udp-claims :arena]))

(deftest todays-release-orders-respect-the-resource-table-test
  (let [acquired (disj (all-nodes 3) [:callback-dispatch 2])]
    (testing "a stop that releases everything"
      (is (= acquired (set todays-stop-order)))
      (is (= (count acquired) (count todays-stop-order)))
      (is (empty? (gen/order-violations acquired todays-stop-order))))
    (testing "a stop that keeps failed worker 1 with a connection"
      (let [kept-order (concat (take 5 todays-stop-order) [[:listeners 1] :socket-claims])]
        (is (empty? (gen/order-violations (all-nodes 2) kept-order)))
        (testing "releases the socket claims while a context without a thread holds its listener"
          (is (= [[:socket-claims [:listeners 2]]] (gen/order-violations acquired kept-order))))))))

(deftest the-core-release-order-respects-the-resource-table-test
  (doseq [n [1 2 3]]
    (is (empty? (gen/order-violations (all-nodes n) (gen/release-order n))))))

(deftest the-order-check-finds-a-wrong-order-test
  (is (= [[:arena :config]] (gen/order-violations #{:arena :config} [:arena :config]))))

;;; Retirement

(deftest a-stop-of-two-healthy-workers-releases-every-node-once-test
  (let [r (run (world serving serving) (started 2))
        releases (map second (:releases (:world r)))]
    (is (= {:outcome :stopped} (gen/result (:progress r))))
    (is (= :stopped (gen/phase (:progress r))))
    (is (= (all-nodes 2) (set releases)))
    (is (= (count releases) (count (set releases))))
    (is (= #{[:callback-dispatch 0] [:callback-dispatch 1]
             :close-executor :h3-contexts :ticket-manager :loops :config :ssl-ctxs
             :socket-claims :udp-claims :arena}
           (released-by :stopping r)))
    (is (= [[:signal-stop] [:await-handlers] [:join-workers]] (take 3 (:actions r))))))

(deftest stop-releases-a-failed-worker-that-left-nothing-test
  (let [r (run (world failed-idle serving) (started 2))]
    (is (= {:outcome :stopped} (gen/result (:progress r))))
    (is (= (set (map #(vector % 0) gen/worker-release-order))
           (set (filter #(= [0] (rest %)) (filter vector? (released-by :stopping r))))))
    (is (empty? (:kept-actions (:world r))))))

(deftest stop-keeps-a-generation-whose-failed-worker-holds-a-connection-test
  (let [r (run (world failed-holding serving) (started 2))
        result (gen/result (:progress r))]
    (testing "TCP and Unix listeners and their claims are released"
      (is (= #{[:listeners 0] [:callback-dispatch 1] :socket-claims} (released-by :stopping r))))
    (testing "UDP claims stay, and the UDP sockets of the kept HTTP/3 node close"
      (is (contains? (states r :acquired) :udp-claims))
      (is (= [[:h3 0]] (:kept-actions (:world r))))
      (is (some #{[:when-kept [:h3 0]]} (:actions r))))
    (testing "what the failed worker holds and what it needs stays"
      (is (= (conj (set (map #(vector % 0) (rest gen/worker-release-order)))
                   :close-executor :h3-contexts :ticket-manager :loops :config :ssl-ctxs
                   :udp-claims :arena)
             (states r :acquired))))
    (is (= {:outcome :kept :unfinished-workers [0] :errors []} result))
    (is (= :kept (gen/phase (:progress r))))
    (testing "a later stop releases nothing again and closes no socket again"
      (let [r2 (run (:world r) (gen/retire (:progress r)))]
        (is (= (:releases (:world r)) (:releases (:world r2))))
        (is (= [[:h3 0]] (:kept-actions (:world r2))))
        (is (= result (gen/result (:progress r2))))
        (is (= [:join-workers] (first (:actions r2))))))))

(deftest stop-keeps-a-generation-whose-failed-worker-pool-cannot-retire-test
  (let [r (run (world failed-idle serving) (started 2) {[:release [:pool 0]] :not-yet})]
    (is (= #{[:listeners 0] [:response-receiver 0] [:callback-dispatch 1] :socket-claims}
           (released-by :stopping r)))
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
    (is (= :leaked (get-in r [:progress :nodes :socket-claims])))
    (is (= [[:h3 0]] (:kept-actions (:world r))))))

(deftest a-worker-alive-at-the-deadline-keeps-everything-until-a-later-stop-test
  (let [stuck (assoc serving :drained? false)
        r (run (world serving stuck) (started 2))]
    (is (= [[:signal-stop] [:await-handlers] [:join-workers] [:force-fail-workers [1]]
            [:done {:outcome :not-joined :alive-workers [1]}]]
           (:actions r)))
    (is (empty? (released-by :stopping r)))
    (is (= :kept (gen/phase (:progress r))))
    (testing "a later stop starts at the join and releases everything once"
      (let [w (assoc-in (:world r) [:workers 1 :drained?] true)
            r2 (run w (gen/retire (:progress r)))
            releases (map second (:releases (:world r2)))]
        (is (= [:join-workers] (first (:actions r2))))
        (is (= {:outcome :stopped} (gen/result (:progress r2))))
        (is (= (all-nodes 2) (set releases)))
        (is (= (count releases) (count (set releases))))))))

(deftest an-interrupted-join-keeps-everything-test
  (let [r (run (world failed-idle serving) (started 2) {[:join-workers] {:error boom}})]
    (is (= {:outcome :not-joined :alive-workers [] :error boom} (gen/result (:progress r))))
    (is (empty? (released-by :stopping r)))))

(deftest handlers-that-do-not-end-in-time-are-interrupted-test
  (let [r (run (world serving serving) (started 2) {[:await-handlers] :timed-out})]
    (is (= [[:signal-stop] [:await-handlers] [:interrupt-handlers] [:join-workers]]
           (take 4 (:actions r))))
    (is (= {:outcome :stopped} (gen/result (:progress r))))))

(deftest a-stop-signal-that-throws-ends-the-run-before-any-release-test
  (let [r (run (world serving serving) (started 2) {[:signal-stop] {:error boom}})]
    (is (= [[:signal-stop] [:done {:outcome :failed :error boom}]] (:actions r)))
    (is (= :running (gen/phase (:progress r))))
    (is (= [:signal-stop] (first (:actions (run (:world r) (gen/retire (:progress r)))))))))

(deftest a-release-that-throws-at-any-node-test
  (doseq [id (gen/release-order 2)]
    (testing (str id)
      (let [r (run (world failed-idle failed-idle) (started 2) {[:release id] {:error boom}})]
        (is (= #{id} (states r :leaked)))
        (is (= (needed-closure id) (states r :acquired)))
        (is (= (disj (all-nodes 2) id) (into (states r :released) (needed-closure id))))
        (is (= [boom] (:errors (gen/result (:progress r)))))
        (testing "a second retirement releases nothing again"
          (let [r2 (run (:world r) (gen/retire (:progress r)))]
            (is (= (:releases (:world r)) (:releases (:world r2))))
            (is (= :kept (:outcome (gen/result (:progress r2)))))))))))

;;; Start

(deftest a-start-acquires-every-node-in-order-test
  (let [r (run (world never-started never-started) (gen/start 2))]
    (is (= {:outcome :started} (gen/result (:progress r))))
    (is (= :running (gen/phase (:progress r))))
    (is (= (all-nodes 2) (set (keys (:nodes (:progress r))))))
    (is (= (map #(vector :acquire (first %)) (gen/acquire-steps 2)) (butlast (:actions r))))))

(deftest a-failed-start-at-any-step-releases-exactly-what-it-acquired-test
  (let [steps (gen/acquire-steps 2)]
    (doseq [k (range (count steps))]
      (testing (str (first (nth steps k)))
        (let [acquired (set (mapcat second (take k steps)))
              r (run (world never-started never-started) (gen/start 2)
                     {[:acquire (first (nth steps k))] {:error boom}})
              releases (map second (:releases (:world r)))]
          (is (= {:outcome :stopped :start-error boom} (gen/result (:progress r))))
          (is (= acquired (set releases)))
          (is (= (count releases) (count (set releases))))
          (testing "the stopping thread releases contexts whose worker thread never started"
            (doseq [i [0 1]
                    :when (and (acquired [:context i]) (not (acquired [:callback-dispatch i])))]
              (is (contains? (released-by :stopping r) [:context i])))))))))
