(ns ol.busker.lifecycle.runtime-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.lifecycle.runtime :as rt]))

(set! *warn-on-reflection* true)

(defn- generation
  "A fake generation entry. Keywords are interned, so services compare by identity."
  [id & {:as more}]
  (merge {:generation-id id
          :config {:version id}
          :instance (keyword (str "instance-" id))
          :cert-automation (keyword (str "cert-" id))
          :memory-ticket-service (keyword (str "tickets-" id))}
         more))

(defn- services
  [generation]
  (keep generation [:cert-automation :memory-ticket-service]))

(defn- err
  ([] (err {}))
  ([data] (ex-info "simulated" data)))

(defn- world
  "A fake shell. `:script` maps an action type to its outcome. `:candidate` is
  what `[:build id]` gives. `:drain` maps a generation id to the outcome of its
  `generation/stop!`. `:stop-errors` is a set of services whose stop throws.
  `:drain-at` maps a generation id to the action count after which its
  background drain runs."
  [state & {:as opts}]
  (merge {:state state
          :script {}
          :drain {}
          :stop-errors #{}
          :drain-at {}
          :trace []
          :steps 0
          :stops []
          :started (set (mapcat services (cons (:active state) (:draining state))))
          :background (set (keep #(when (:drain %) (:generation-id %)) (:draining state)))
          :drain-results {}
          :seen (set (map :generation-id (remove nil? (cons (:active state) (:draining state)))))
          :drained #{}
          :violations []}
         opts))

(defn- violations
  [{:keys [state seen drained]}]
  (let [{:keys [phase active draining]} state
        listed (map :generation-id (remove nil? (cons active draining)))]
    (cond-> []
      (and (= :running phase) (nil? active))
      (conj :running-without-active)

      (not= (count listed) (count (set listed)))
      (conj :listed-twice)

      (and (not= :stopped phase)
           (seq (remove (set listed) (remove drained seen))))
      (conj :in-no-list))))

(defn- set-state
  [w state]
  (let [w (-> w
              (assoc :state state)
              (update :seen into (map :generation-id (remove nil? (cons (:active state) (:draining state))))))]
    (update w :violations into (violations w))))

(defn- stop-service
  "Records the stop of `service` and returns `[world outcome]`."
  [w service]
  [(update w :stops conj service)
   (if ((:stop-errors w) service) {:error (err {:service service})} {:value nil})])

(defn- finish-drain
  "Runs the background drain of generation `id` once, as the drain thread does."
  [w id]
  (if (contains? (:drain-results w) id)
    w
    (let [stopped (get-in w [:drain id] {:value nil})]
      (if (:error stopped)
        (assoc-in w [:drain-results id] stopped)
        (let [{:keys [state stop]} (rt/drained (:state w) id)
              w (-> w (update :drained conj id) (set-state state))
              [w errors] (reduce (fn [[w errors] [_ service]]
                                   (let [[w o] (stop-service w service)]
                                     [w (cond-> errors (:error o) (conj (:error o)))]))
                                 [w []]
                                 stop)]
          (assoc-in w [:drain-results id]
                    (if (seq errors) {:error (first errors)} {:value nil})))))))

(defn- perform
  "Does `action` in the fake world. Returns `[world outcome]`."
  [w [type a :as action]]
  (let [given (get-in w [:script type])]
    (case type
      :swap (let [[_ f & args] action
                  state (apply f (:state w) args)]
              [(set-state w state) {:value state}])
      :build (let [candidate (or (:candidate w) (generation a))]
               (if given
                 [w given]
                 [(update w :started into (services candidate)) {:value candidate}]))
      :activate [w (or given {:value a})]
      :retire (if given
                [w given]
                [(update w :background conj (:generation-id a))
                 {:value [:drain (:generation-id a)]}])
      :await-drain (if (:drain a)
                     (let [w (finish-drain w (:generation-id a))]
                       [w (get-in w [:drain-results (:generation-id a)])])
                     [w (get-in w [:drain (:generation-id a)] {:value nil})])
      (:stop-cert-runtime :stop-ticket-service) (stop-service w a)
      [w (or given {:value nil})])))

(defn- run-due-drains
  [w]
  (reduce (fn [w id]
            (if (and (contains? (:background w) id)
                     (<= (get-in w [:drain-at id] Long/MAX_VALUE) (:steps w)))
              (finish-drain w id)
              w))
          w
          (keys (:drain-at w))))

(defn- drive
  "Does the action of `progress` in the fake world and steps, as the shell
  does, until `[:done result]`. Returns the world with `:result`."
  [w progress]
  (loop [w w
         progress progress]
    (let [action (:action progress)]
      (if (= :done (first action))
        (assoc w :result (second action))
        (let [[w o] (perform (update w :trace conj action) action)
              w (run-due-drains (update w :steps inc))]
          (assert (< (:steps w) 100) "the core loops")
          (recur w (rt/step progress o)))))))

(defn- reload
  [w & {:as observed}]
  (drive w (rt/begin-reload (merge {:state (:state w) :snapshot {:version 1}} observed))))

(defn- stop
  [w]
  (drive w (rt/begin-stop (:state w))))

(defn- types
  [w]
  (mapv (fn [[type a]] (if (= :swap type) [type a] type)) (:trace w)))

(def ^:private one
  (generation 1))

(defn- running
  [& draining]
  (update (rt/initial-state one :pool) :draining into draining))

;; Reload rows

(deftest unchanged-configuration-does-nothing-test
  (let [w (reload (world (running)))]
    (is (= {:value :unchanged} (:result w)))
    (is (= [] (:trace w)))))

(deftest forced-reload-activates-and-retires-the-old-generation-test
  (let [w (reload (world (running)) :force? true)
        two (generation 2)]
    (is (= {:value :activated} (:result w)))
    (is (= [[:build 2]
            [:activate two]
            [:swap rt/publish two]
            [:stop-accepting one]
            [:retire one]
            [:swap rt/attach-drain 1 [:drain 1]]]
           (:trace w)))
    (is (= two (get-in w [:state :active])))
    (is (= [(assoc one :drain [:drain 1])] (get-in w [:state :draining])))
    (is (= 3 (get-in w [:state :next-generation-id])))))

(deftest changed-configuration-activates-test
  (is (= {:value :activated}
         (:result (reload (world (running)) :snapshot {:version 2})))))

(deftest same-configuration-with-a-failed-worker-activates-test
  (let [w (reload (world (running)) :failed-worker? true)]
    (is (= {:value :activated} (:result w)))
    (is (= 1 (:generation-id (first (get-in w [:state :draining])))))))

(deftest retry-is-unchanged-only-when-the-active-generation-serves-it-test
  (let [w (reload (world (running)) :failed-worker? true)
        retry (reload (assoc w :trace []) :snapshot {:version 2})]
    (is (= {:value :unchanged} (:result retry)))
    (is (= [] (:trace retry)))))

(deftest reload-of-a-stopping-server-fails-test
  (doseq [phase [:stopping :stopped]]
    (let [w (reload (world (assoc (running) :phase phase)) :force? true)
          e (:error (:result w))]
      (is (= "Server is stopping" (ex-message e)))
      (is (= {:reason :server-stopping} (ex-data e)))
      (is (= [] (:trace w))))))

(deftest build-failure-reports-its-stage-and-changes-nothing-test
  (doseq [[data expected]
          [[{:stage :validation} {:reason :reload-failed :stage :validation}]
           [{:stage :automation-startup} {:reason :reload-failed :stage :automation-startup}]
           [{} {:reason :reload-failed :stage :activation}]
           [{:stage :listener-acquisition :listener-key :k}
            {:reason :reload-failed :stage :listener-acquisition :listener-key :k}]]]
    (testing (pr-str data)
      (let [cause (err data)
            w (reload (world (running) :script {:build {:error cause}}) :force? true)
            e (:error (:result w))]
        (is (= "Reload failed" (ex-message e)))
        (is (= expected (ex-data e)))
        (is (identical? cause (ex-cause e)))
        (is (= [[:build 2]] (:trace w)))
        (is (= (running) (:state w)))))))

(deftest activation-failure-discards-what-the-candidate-started-test
  (let [two (generation 2 :cert-automation :cert-1)
        w (reload (world (running)
                         :candidate two
                         :script {:activate {:error (err {:stage :tls-startup})}})
                  :force? true)]
    (is (= {:reason :reload-failed :stage :tls-startup}
           (ex-data (:error (:result w)))))
    (is (= [[:build 2]
            [:activate two]
            [:stop-generation two]
            [:stop-ticket-service :tickets-2]]
           (:trace w))
        "the reused certificate runtime is not stopped")
    (is (= (running) (:state w)))))

(deftest discard-errors-do-not-hide-the-activation-error-test
  (let [w (reload (world (running)
                         :stop-errors #{:cert-2 :tickets-2}
                         :script {:activate {:error (err)}
                                  :stop-generation {:error (err)}})
                  :force? true)]
    (is (= :activation (:stage (ex-data (:error (:result w))))))
    (is (= [:cert-2 :tickets-2] (:stops w)))))

(deftest old-generation-that-does-not-stop-accepting-is-still-retired-test
  (let [cause (err {:generation-id 1})
        w (reload (world (running) :script {:stop-accepting {:error cause}}) :force? true)]
    (is (= {:value :activated} (:result w)))
    (is (= [:build :activate [:swap rt/publish] :stop-accepting
            :log-stop-accepting-failed :retire [:swap rt/attach-drain]]
           (types w)))
    (is (= [:log-stop-accepting-failed one cause] (nth (:trace w) 4)))
    (is (= 2 (get-in w [:state :active :generation-id])))
    (is (= [1] (map :generation-id (get-in w [:state :draining]))))))

(deftest old-generation-without-a-drain-thread-stays-listed-test
  (let [w (reload (world (running) :script {:retire {:error (err)}}) :force? true)]
    (is (= {:value :activated} (:result w)))
    (is (= [one] (get-in w [:state :draining])))))

;; Generation drained

(deftest drained-while-running-stops-services-no-listed-generation-has-test
  (let [two (generation 2 :memory-ticket-service :tickets-1)
        state (rt/publish (running) two)]
    (is (= {:state (assoc state :draining [])
            :stop [[:stop-cert-runtime :cert-1]]}
           (rt/drained state 1)))))

(deftest drained-while-stopping-stops-nothing-test
  (is (= [] (get-in (rt/drained (rt/begin-stopping (running)) 1) [:state :draining])))
  (is (empty? (:stop (rt/drained (rt/begin-stopping (running)) 1)))))

(deftest drained-of-an-unlisted-generation-changes-nothing-test
  (is (= {:state (running) :stop nil} (rt/drained (running) 7))))

;; Server stop rows

(deftest stop-drains-every-generation-and-stops-each-service-once-test
  (let [zero (generation 0 :drain [:drain 0] :cert-automation :cert-1)
        w (stop (world (running zero)))]
    (is (= {:value nil} (:result w)))
    (is (= [[:swap rt/begin-stopping]
            [:stop-accepting one]
            [:retire one]
            [:swap rt/attach-drain 1 [:drain 1]]
            [:await-drain zero]
            [:await-drain (assoc one :drain [:drain 1])]
            [:stop-cert-runtime :cert-1]
            [:stop-ticket-service :tickets-0]
            [:stop-ticket-service :tickets-1]
            [:swap rt/end-stopped {:value nil}]]
           (:trace w)))
    (is (= [:cert-1 :tickets-0 :tickets-1] (:stops w)))
    (is (= :stopped (get-in w [:state :phase])))))

(deftest stop-collects-every-failure-test
  (let [e0 (err {:generation-id 0})
        e1 (err {:generation-id 1})
        zero (generation 0 :drain [:drain 0])
        w (stop (world (running zero)
                       :drain {0 {:error e0} 1 {:error e1}}
                       :stop-errors #{:cert-0}))
        {:keys [error suppressed]} (:result w)]
    (is (identical? e0 error))
    (is (= [e1 {:service :cert-0}] [(first suppressed) (ex-data (second suppressed))]))
    (is (= [:cert-0 :cert-1 :tickets-0 :tickets-1] (:stops w)))
    (is (= :stopped (get-in w [:state :phase])))
    (is (= [] (get-in w [:state :draining])))))

(deftest second-stop-gives-the-same-result-test
  (let [w (stop (world (running) :drain {1 {:error (err)}}))
        again (stop (assoc w :trace []))]
    (is (= (:result w) (:result again)))
    (is (= [] (:trace again)))))

;; Bugs of the old code: each test fails on the old behaviour.

(deftest failed-activation-lists-the-candidate-nowhere-test
  (let [w (reload (world (running) :script {:activate {:error (err)}}) :force? true)]
    (is (= one (get-in w [:state :active])))
    (is (not-any? #{2} (map :generation-id (get-in w [:state :draining]))))))

(deftest old-generation-is-listed-before-it-stops-accepting-test
  (let [w (reload (world (running)) :force? true)
        published (rt/publish (running) (generation 2))]
    (is (= [1] (map :generation-id (:draining published))))
    (is (< (.indexOf ^java.util.List (types w) [:swap rt/publish])
           (.indexOf ^java.util.List (types w) :stop-accepting)))
    (is (empty? (:violations w)))))

(deftest failed-activation-then-stop-stops-the-new-cert-runtime-once-test
  (let [w (reload (world (running) :script {:activate {:error (err)}}) :force? true)
        w (stop (assoc w :script {}))]
    (is (= 1 (count (filter #{:cert-2} (:stops w)))))
    (is (= 1 (count (filter #{:cert-1} (:stops w)))))))

(deftest stop-continues-after-the-first-drain-failure-test
  (let [zero (generation 0 :drain [:drain 0])
        w (stop (world (running zero) :drain {0 {:error (err)}}))]
    (is (= [:await-drain :await-drain] (filterv #{:await-drain} (types w))))
    (is (= :stopped (get-in w [:state :phase])))))

;; Exhaustive

(defn- outcome-space
  []
  (for [mode [:unchanged :changed :failed-worker]
        build [:ok :validation :no-stage]
        activate [:ok :error]
        stop-accepting [:ok :error]
        retire [:ok :error]
        cert-reused? [false true]
        tickets-reused? [false true]
        zero? [false true]
        drain-at-0 [nil 1 3 5]
        drain-at-1 [nil 2 5]
        drain-fails (if zero? [#{} #{0} #{1} #{0 1}] [#{} #{1}])
        stop-error [nil :cert-1 :tickets-2]]
    {:mode mode :build build :activate activate :stop-accepting stop-accepting
     :retire retire :cert-reused? cert-reused? :tickets-reused? tickets-reused?
     :zero? zero? :drain-at {0 drain-at-0 1 drain-at-1} :drain-fails drain-fails
     :stop-error stop-error}))

(defn- start-world
  [{:keys [build activate stop-accepting retire cert-reused? tickets-reused?
           zero? drain-at drain-fails stop-error]}]
  (world (if zero? (running (generation 0 :drain [:drain 0])) (running))
         :candidate (cond-> (generation 2)
                      cert-reused? (assoc :cert-automation :cert-1)
                      tickets-reused? (assoc :memory-ticket-service :tickets-1))
         :script (cond-> {}
                   (= :validation build) (assoc :build {:error (err {:stage :validation})})
                   (= :no-stage build) (assoc :build {:error (err)})
                   (= :error activate) (assoc :activate {:error (err)})
                   (= :error stop-accepting) (assoc :stop-accepting {:error (err)})
                   (= :error retire) (assoc :retire {:error (err)}))
         :drain (into {} (map (fn [id] [id {:error (err {:generation-id id})}])) drain-fails)
         :drain-at (into {} (remove (comp nil? val)) drain-at)
         :stop-errors (if stop-error #{stop-error} #{})))

(deftest every-outcome-sequence-keeps-the-invariants-test
  (let [runs (outcome-space)
        failures
        (for [{:keys [mode] :as space} runs
              :let [after-reload (reload (start-world space)
                                         :snapshot {:version (if (= :changed mode) 2 1)}
                                         :failed-worker? (= :failed-worker mode))
                    after-stop (stop after-reload)
                    again (stop after-stop)
                    stop-counts (frequencies (:stops after-stop))
                    problems
                    (cond-> (:violations after-stop)
                      (not= :stopped (get-in after-stop [:state :phase]))
                      (conj :not-stopped)

                      (not= (:result after-stop) (:result again))
                      (conj :second-stop-differs)

                      (not= (:trace after-stop) (:trace again))
                      (conj :second-stop-acts)

                      (not-every? #(= 1 (stop-counts %)) (:started after-stop))
                      (conj [:stops stop-counts (:started after-stop)])

                      (not-every? (:started after-stop) (keys stop-counts))
                      (conj :stopped-a-service-never-started))]
              :when (seq problems)]
          [space problems])]
    (is (< 10000 (count runs)))
    (is (empty? (take 3 failures)))))
