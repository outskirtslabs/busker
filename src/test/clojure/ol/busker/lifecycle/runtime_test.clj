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
          :cert-plan [:plan id]
          :ticket-policy [:policy id]
          :instance (keyword (str "instance-" id))
          :cert-automation (keyword (str "cert-" id))
          :memory-ticket-service (keyword (str "tickets-" id))}
         more))

(defn- services
  [generation]
  (keep #(% generation) [:cert-automation :memory-ticket-service]))

(defn- err
  ([] (err {}))
  ([data] (ex-info "simulated" data)))

(defn- world
  "A fake shell. `:script` maps an action type, or `[:swap f]` for a swap, to
  its outcome. `:candidate` is the generation that the build starts: its
  plan, policy, services and instance. `:drain` maps a
  generation id to the outcome of its `generation/stop!`. `:stop-errors` is a
  set of services whose stop throws. `:drain-at` maps a generation id to the
  action count after which its background drain runs. `:background` maps the
  id of each generation with a drain thread to its entry. `:starts` lists
  the services that the core started. `:logs` lists the id and the cause of
  each logged failure. `:active-at-stop-accepting` lists the active
  generation id at each stop-accepting."
  [state & {:as opts}]
  (merge {:state state
          :script {}
          :drain {}
          :stop-errors #{}
          :drain-at {}
          :trace []
          :steps 0
          :stops []
          :starts []
          :logs []
          :active-at-stop-accepting []
          :started (set (mapcat services (cons (:active state) (:draining state))))
          :background (into {} (keep #(when (:drain %) [(:generation-id %) %])) (:draining state))
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

      (seq (remove (set listed) (remove drained seen)))
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

(defn- candidate
  [w]
  (or (:candidate w) (generation (get-in w [:state :next-generation-id] 1))))

(defn- start-service
  "Records the start of `service` and returns `[world outcome]`."
  [w given service]
  (if given
    [w given]
    [(-> w (update :started conj service) (update :starts conj service)) {:value service}]))

(declare drive)

(defn- finish-drain
  "Runs the background drain of generation `id` once, as the drain thread
  does. Its actions are not in the trace of the operation."
  [w id]
  (if (contains? (:drain-results w) id)
    w
    (let [{:keys [trace result]} w
          w (drive (assoc-in w [:drain-results id] :running)
                   (rt/begin-drain (get-in w [:background id])))]
      (assoc w :drain-results (assoc (:drain-results w) id (:result w))
             :trace trace
             :result result))))

(defn- perform
  "Does `action` in the fake world. Returns `[world outcome]`."
  [w [type a :as action]]
  (let [given (get-in w [:script type])]
    (case type
      :swap (let [[_ f & args] action]
              (if-let [given (get-in w [:script [:swap f]])]
                [w given]
                (let [state (apply f (:state w) args)]
                  [(set-state w state) {:value state}])))
      :compile-config [w (or given {:value (assoc (select-keys (candidate w) [:cert-plan :ticket-policy])
                                                  :snapshot (:config (candidate w))
                                                  :compiled-config :compiled)})]
      :start-cert-runtime (start-service w given (:cert-automation (candidate w)))
      :start-ticket-service (start-service w given (:memory-ticket-service (candidate w)))
      :start-generation [w (or given {:value (:instance (candidate w))})]
      :activate [w (or given {:value a})]
      :retire (if given
                [w given]
                [(assoc-in w [:background (:generation-id a)] a)
                 {:value [:drain (:generation-id a)]}])
      :await-drain (let [w (finish-drain w (:generation-id a))]
                     [w (get-in w [:drain-results (:generation-id a)])])
      :stop-generation (let [o (or given (get-in w [:drain (:generation-id a)]) {:value nil})]
                         [(cond-> w (not (:error o)) (update :drained conj (:generation-id a))) o])
      (:stop-cert-runtime :stop-ticket-service) (stop-service w a)
      :stop-accepting [(update w :active-at-stop-accepting conj
                               (get-in w [:state :active :generation-id]))
                       (or given {:value nil})]
      :log-failure (let [[_ id _ cause] action]
                     [(update w :logs conj [id cause]) (or given {:value nil})])
      [w (or given {:value nil})])))

(defn- run-due-drains
  [w]
  (reduce (fn [w id]
            (if (and (contains? (:background w) id)
                     (not (contains? (:drain-results w) id))
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

(defn- start
  [w]
  (drive w (rt/begin-start)))

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
  (let [w (reload (world (running)) :force? true)]
    (is (= {:value :activated} (:result w)))
    (is (= (generation 2) (get-in w [:state :active])))
    (is (= [(assoc one :drain [:drain 1])] (get-in w [:state :draining])))
    (is (= 3 (get-in w [:state :next-generation-id])))
    (is (= [:cert-2 :tickets-2] (:starts w)))
    (is (= [] (:stops w)))))

(deftest changed-configuration-activates-test
  (is (= {:value :activated}
         (:result (reload (world (running)) :snapshot {:version 2})))))

(deftest same-configuration-with-a-failed-worker-activates-test
  (let [w (reload (world (running)) :failed-worker? true)]
    (is (= {:value :activated} (:result w)))
    (is (= 1 (:generation-id (first (get-in w [:state :draining])))))))

(deftest retry-is-unchanged-only-when-the-active-generation-serves-it-test
  (let [w (reload (world (running) :candidate (generation 2 :config {:version 1})) :failed-worker? true)
        retry (reload (assoc w :trace []))]
    (is (= {:value :unchanged} (:result retry)))
    (is (= [] (:trace retry)))))

(deftest reload-of-a-stopping-server-fails-test
  (doseq [phase [:stopping :stopped]]
    (let [w (reload (world (assoc (running) :phase phase)) :force? true)
          e (:error (:result w))]
      (is (= "Server is stopping" (ex-message e)))
      (is (= {:reason :server-stopping} (ex-data e)))
      (is (= [] (:trace w))))))

(def ^:private build-failures
  "Each build action, the error that wraps its failure, and the services that
  the discard stops."
  [[:compile-config "Failed to compile generation config" :validation []]
   [:start-cert-runtime "Failed to start certificate automation" :automation-startup []]
   [:start-ticket-service "Failed to start in-memory session tickets" :activation
    [:cert-automation]]
   [:start-generation "Failed to start generation" :activation
    [:cert-automation :memory-ticket-service]]])

(deftest build-failure-reports-its-stage-and-stops-what-it-started-test
  (doseq [[action message stage stops] build-failures
          [data expected] [[{} {:stage stage}]
                           [{:stage :listener-acquisition :listener-key :k}
                            {:stage :listener-acquisition :listener-key :k}]]]
    (testing (pr-str action data)
      (let [cause (err data)
            w (reload (world (running) :script {action {:error cause}}) :force? true)
            e (:error (:result w))]
        (is (= "Reload failed" (ex-message e)))
        (is (= (assoc expected :reason :reload-failed) (ex-data e)))
        (is (= message (ex-message (ex-cause e))))
        (is (= expected (ex-data (ex-cause e))))
        (is (identical? cause (ex-cause (ex-cause e))))
        (is (= (map (generation 2) stops) (:starts w) (:stops w))
            "the build starts nothing after the failed action")
        (is (= (running) (:state w)))))))

(deftest build-reuses-a-service-only-for-the-same-need-test
  (testing "the same need reuses the service of the old generation"
    (let [w (reload (world (running)
                           :candidate (generation 2 :config {:version 1} :cert-plan [:plan 1]
                                                  :ticket-policy [:policy 1]))
                    :force? true)]
      (is (= [] (:starts w)))
      (is (= [:cert-1 :tickets-1]
             (map (get-in w [:state :active]) [:cert-automation :memory-ticket-service])))
      (is (= {:cert-1 1 :tickets-1 1} (frequencies (:stops (stop w)))))))
  (testing "no need starts no service"
    (let [w (reload (world (running)
                           :candidate (generation 2 :config {:version 1} :cert-plan nil
                                                  :ticket-policy nil))
                    :force? true)]
      (is (= [] (:starts w)))
      (is (= [nil nil]
             (map (get-in w [:state :active]) [:cert-automation :memory-ticket-service]))))))

(deftest activation-failure-discards-what-the-candidate-started-test
  (let [two (generation 2 :config {:version 1} :cert-plan [:plan 1] :cert-automation :cert-1)
        w (reload (world (running)
                         :candidate two
                         :script {:activate {:error (err {:stage :tls-startup})}})
                  :force? true)]
    (is (= {:reason :reload-failed :stage :tls-startup}
           (ex-data (:error (:result w)))))
    (is (= [:tickets-2] (:stops w)) "the reused certificate runtime is not stopped")
    (is (contains? (:drained w) 2))
    (is (= (running) (:state w)))))

(deftest discard-errors-are-suppressed-on-the-activation-error-test
  (let [stop-error (err {:generation-id 2})
        w (reload (world (running)
                         :stop-errors #{:cert-2 :tickets-2}
                         :script {:activate {:error (err)}
                                  :stop-generation {:error stop-error}})
                  :force? true)
        {:keys [error suppressed]} (:result w)]
    (is (= :activation (:stage (ex-data error))))
    (is (= [:cert-2 :tickets-2] (:stops w)))
    (is (= [stop-error {:service :cert-2} {:service :tickets-2}]
           (cons (first suppressed) (map ex-data (rest suppressed)))))))

(deftest old-generation-that-does-not-stop-accepting-is-still-retired-test
  (let [cause (err {:generation-id 1})
        w (reload (world (running) :script {:stop-accepting {:error cause}}) :force? true)]
    (is (= {:value :activated} (:result w)))
    (is (= [[:ol.busker.runtime/stop-accepting-failed cause]] (:logs w)))
    (is (= 2 (get-in w [:state :active :generation-id])))
    (is (= [(assoc one :drain [:drain 1])] (get-in w [:state :draining])))))

(deftest old-generation-without-a-drain-thread-is-logged-and-stays-listed-test
  (let [cause (err)
        w (reload (world (running) :script {:retire {:error cause}}) :force? true)]
    (is (= {:value :activated} (:result w)))
    (is (= [[:ol.busker.runtime/retire-failed cause]] (:logs w)))
    (is (= [one] (get-in w [:state :draining])))))

(deftest publish-failure-discards-the-candidate-and-keeps-the-old-generation-test
  (let [cause (Error. "simulated")
        w (reload (world (running) :script {[:swap rt/publish] {:error cause}}) :force? true)]
    (is (identical? cause (ex-cause (:error (:result w)))))
    (is (= {:reason :reload-failed :stage :activation} (ex-data (:error (:result w)))))
    (is (= [:cert-2 :tickets-2] (:stops w)))
    (is (contains? (:drained w) 2))
    (is (= (running) (:state w)))))

(deftest attach-failure-still-awaits-the-drain-in-a-stop-test
  (let [w (stop (world (running) :script {[:swap rt/attach-drain] {:error (Error.)}}))]
    (is (= {:value nil} (:result w)))
    (is (= {:value nil} (get-in w [:drain-results 1])) "the stop awaits the drain")
    (is (= [:cert-1 :tickets-1] (:stops w)))))

(deftest begin-stopping-failure-ends-the-stop-and-changes-nothing-test
  (let [cause (Error. "simulated")
        w (stop (world (running) :script {[:swap rt/begin-stopping] {:error cause}}))]
    (is (= {:error cause} (:result w)))
    (is (= [] (:stops w)))
    (is (empty? (:drained w)))
    (is (= (running) (:state w)))))

(deftest end-stopped-failure-is-added-to-the-stop-failures-test
  (let [e1 (err)
        cause (Error. "simulated")
        w (stop (world (running) :drain {1 {:error e1}}
                       :script {[:swap rt/end-stopped] {:error cause}}))]
    (is (= {:error e1 :suppressed [cause]} (:result w)))))

;; Generation drained

(deftest drained-while-running-stops-services-no-listed-generation-has-test
  (let [w (drive (world (rt/publish (running) (generation 2 :memory-ticket-service :tickets-1)))
                 (rt/begin-drain one))]
    (is (= {:value nil} (:result w)))
    (is (= [:cert-1] (:stops w)))))

(deftest drain-stops-the-generation-then-takes-it-off-the-list-test
  (let [w (drive (world (rt/publish (running) (generation 2))) (rt/begin-drain one))]
    (is (= {:value nil} (:result w)))
    (is (= [] (get-in w [:state :draining])))
    (is (= [:cert-1 :tickets-1] (:stops w)))
    (is (empty? (:violations w)) "the generation is off the list only after it stopped")))

(deftest drain-logs-a-service-stop-that-fails-and-goes-on-test
  (let [w (drive (world (rt/publish (running) (generation 2)) :stop-errors #{:cert-1})
                 (rt/begin-drain one))]
    (is (= {:value nil} (:result w)))
    (is (= [:ol.busker.runtime/service-stop-failed] (map first (:logs w))))
    (is (= {:service :cert-1} (ex-data (second (first (:logs w))))))
    (is (= [:cert-1 :tickets-1] (:stops w)))))

(deftest drain-that-cannot-stop-the-generation-keeps-it-listed-test
  (let [cause (err)
        w (drive (world (rt/publish (running) (generation 2)) :drain {1 {:error cause}})
                 (rt/begin-drain one))]
    (is (= {:error cause} (:result w)))
    (is (= [] (:stops w)))
    (is (= [one] (get-in w [:state :draining])))))

;; Server stop rows

(deftest stop-drains-every-generation-and-stops-each-service-once-test
  (let [zero (generation 0 :drain [:drain 0] :cert-automation :cert-1)
        w (stop (world (running zero)))]
    (is (= {:value nil} (:result w)))
    (is (= {:cert-1 1 :tickets-0 1 :tickets-1 1} (frequencies (:stops w))))
    (is (= #{0 1} (:drained w)))
    (is (= :stopped (get-in w [:state :phase])))
    (is (= [] (get-in w [:state :draining])))))

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
    (is (= #{:cert-0 :cert-1 :tickets-0 :tickets-1} (set (:stops w))))
    (is (= :stopped (get-in w [:state :phase])))
    (is (= [0 1] (map :generation-id (get-in w [:state :draining])))
        "generations that did not drain stay listed")))

;; Bugs of the old code: each test fails on the old behaviour.

(deftest old-generation-stops-accepting-only-after-the-new-one-is-active-test
  (let [w (reload (world (running)) :force? true)]
    (is (= [2] (:active-at-stop-accepting w)))
    (is (empty? (:violations w)))))

;; Start rows

(deftest start-failure-reports-its-stage-and-stops-what-it-started-test
  (doseq [[action message stage stops] build-failures]
    (testing (pr-str action)
      (let [cause (err)
            w (start (world nil :stop-errors #{:cert-1} :script {action {:error cause}}))
            {:keys [error suppressed]} (:result w)]
        (is (= message (ex-message error)))
        (is (= {:stage stage} (ex-data error)))
        (is (identical? cause (ex-cause error)))
        (is (= (map one stops) (:stops w)))
        (is (= (filter #{:cert-1} (:stops w)) (map (comp :service ex-data) suppressed)))))))

(deftest start-activation-failure-stops-the-generation-and-its-services-test
  (let [cause (err)
        w (start (world nil :script {:activate {:error cause}}))]
    (is (= {:error cause :suppressed []} (:result w)))
    (is (= #{1} (:drained w)))
    (is (= [:cert-1 :tickets-1] (:stops w)))))

;; Exhaustive

(deftest every-start-outcome-stops-each-service-it-started-once-test
  (doseq [failure [nil :compile-config :start-cert-runtime :start-ticket-service
                   :start-generation :activate]
          stop-errors [#{} #{:cert-1} #{:cert-1 :tickets-1}]
          stop-generation [nil {:error (err)}]]
    (let [w (start (world nil
                          :stop-errors stop-errors
                          :script (cond-> {}
                                    failure (assoc failure {:error (err)})
                                    stop-generation (assoc :stop-generation stop-generation))))
          {:keys [value error]} (:result w)]
      (is (= (nil? failure) (some? value) (nil? error)))
      (is (= (if failure (sort (:started w)) [])
             (sort (:stops w)))
          (pr-str failure stop-errors stop-generation)))))

(defn- outcome-space
  []
  (for [mode [:unchanged :changed :failed-worker]
        build [:ok :compile-config :start-cert-runtime :start-ticket-service :start-generation]
        :let [later (if (= :ok build) [:ok :error] [:ok])]
        activate later
        stop-accepting later
        retire later
        cert-reused? [false true]
        tickets-reused? [false true]
        zero? [false true]
        drain-at-0 [nil 1 6 9]
        drain-at-1 [nil 9 12]
        drain-fails (if zero? [#{} #{0} #{1} #{0 1}] [#{} #{1}])
        stop-error [nil :cert-1 :tickets-2]]
    {:mode mode :build build :activate activate :stop-accepting stop-accepting
     :retire retire :cert-reused? cert-reused? :tickets-reused? tickets-reused?
     :zero? zero? :drain-at {0 drain-at-0 1 drain-at-1} :drain-fails drain-fails
     :stop-error stop-error}))

(defn- outcome-world
  [{:keys [build activate stop-accepting retire cert-reused? tickets-reused?
           zero? drain-at drain-fails stop-error]}]
  (world (if zero? (running (generation 0 :drain [:drain 0])) (running))
         :candidate (cond-> (generation 2)
                      cert-reused? (assoc :cert-plan [:plan 1])
                      tickets-reused? (assoc :ticket-policy [:policy 1]))
         :script (cond-> {}
                   (not= :ok build) (assoc build {:error (err)})
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
              :let [after-reload (reload (outcome-world space)
                                         :snapshot {:version (if (= :changed mode) 2 1)}
                                         :failed-worker? (= :failed-worker mode))
                    after-stop (stop after-reload)
                    stop-counts (frequencies (:stops after-stop))
                    problems
                    (cond-> (:violations after-stop)
                      (not= :stopped (get-in after-stop [:state :phase]))
                      (conj :not-stopped)

                      (not-every? #(= 1 (stop-counts %)) (:started after-stop))
                      (conj [:stops stop-counts (:started after-stop)])

                      (not-every? (:started after-stop) (keys stop-counts))
                      (conj :stopped-a-service-never-started))]
              :when (seq problems)]
          [space problems])]
    (is (< 10000 (count runs)))
    (is (empty? (take 3 failures)))))
