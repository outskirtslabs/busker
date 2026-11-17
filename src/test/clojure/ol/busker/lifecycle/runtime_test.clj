(ns ol.busker.lifecycle.runtime-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.results :as results]
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

(defn- parts
  "Returns the instance and the services of `generation`."
  [generation]
  (keep #(% generation) [:instance :cert-automation :memory-ticket-service]))

(defn- err
  ([] (err {}))
  ([data] (ex-info "simulated" data)))

(defn- world
  "A fake shell over the real core, with a lifecycle thread, a drain thread for
  each retired generation, and interrupts. `:ops` lists the lifecycle
  operations, each `[kind observation]`. `:script` maps an action type, `[type
  generation-id]` or `[:swap f]` to its outcome. `:stop-errors` is a set of
  services whose stop throws. Else an action other than a swap takes the next
  item of `:fails`, and true fails it. `:candidate` is the generation that a
  build starts, or else the next item of `:builds` gives its `:cert` and
  `:tickets` needs: `:new`, `:same` as the active generation, or nil.
  `:interrupts` is the number of interrupts that can end an await of a drain.
  With `:ops-only?` the run ends with the last operation.

  It records the `:results` of the operations, the last `:result`, the
  `:stop-result`, the `:starts` and `:stops` of services, the instances of
  `:generation-stops`, the `:logs`, the `:raised` errors and the `:broken`
  invariants."
  [state & {:as opts}]
  (merge {:state state :ops [] :script {} :stop-errors #{} :fails [] :builds []
          :interrupts 0 :results [] :starts [] :stops [] :generation-stops [] :logs []
          :raised [] :broken #{} :drain-results {}
          :started (set (mapcat parts (cons (:active state) (:draining state))))
          :drains (into {} (keep #(when (:drain %) [(:generation-id %) (rt/begin-drain %)]))
                        (:draining state))}
         opts))

(defn- set-state
  [w {:keys [phase active draining] :as state}]
  (let [listed (map :generation-id (remove nil? (cons active draining)))]
    (cond-> (assoc w :state state)
      (and (= :running phase) (nil? active))
      (update :broken conj :a-running-server-has-an-active-generation)

      (not= (count listed) (count (set listed)))
      (update :broken conj :no-generation-is-listed-twice))))

(defn- candidate
  [{:keys [state builds] :as w}]
  (or (:candidate w)
      (let [{:keys [cert tickets] :or {cert :new tickets :new}} (first builds)
            need (fn [g k need] (assoc g k (case need :new (k g) :same (k (:active state)) nil)))]
        (-> (generation (:next-generation-id state 1))
            (need :cert-plan cert)
            (need :ticket-policy tickets)))))

(defn- outcome
  "Returns `[world outcome]`, with a nil outcome when `action` does not fail."
  [{:keys [script stop-errors fails] :as w} [type a]]
  (if-let [given (or (script [type a]) (script [type (:generation-id a)]) (script type))]
    [w given]
    (let [e (cond (stop-errors a) (err {:service a}) (and (first fails) (not= :swap type)) (err))]
      [(cond-> (update w :fails rest) e (update :raised conj e)) (when e {:error e})])))

(defn- await-drain
  "Gives the drain result as the shell does: its error carries the other
  errors as suppressed. An interrupt ends the await first."
  [w {id :generation-id}]
  (if (:interrupted? w)
    (let [e (InterruptedException.)]
      [(-> w (dissoc :interrupted?) (update :raised conj e)) {:error e}])
    (let [{:keys [error suppressed] :as result} (get-in w [:drain-results id])]
      (doseq [t suppressed] (.addSuppressed ^Throwable error ^Throwable t))
      [w (if error {:error error} result)])))

(def ^:private stop-actions
  #{:stop-generation :stop-cert-runtime :stop-ticket-service})

(defn- serves?
  "Tells if `action` stops the active generation, or a service of it, while
  the server runs."
  [{{:keys [phase active]} :state} [type a]]
  (and (= :running phase)
       (contains? (conj stop-actions :stop-accepting) type)
       (some #(identical? (if (map? a) (:instance a) a) %) (parts active))))

(defn- perform
  "Does `action` in the fake world. Returns `[world outcome]`."
  [w [type a & more :as action]]
  (let [w (cond-> w
            (serves? w action) (update :broken conj :a-running-server-stops-nothing-it-serves)
            (and (stop-actions type) (:stop-returned? w))
            (update :broken conj :nothing-stops-after-a-stop-returns)
            (= :stop-generation type) (update :generation-stops conj (:instance a))
            (#{:stop-cert-runtime :stop-ticket-service} type) (update :stops conj a))
        c (candidate w)
        start (fn [w x]
                ;; A reload after a failed one builds the same generation id, so
                ;; a service that starts again gets a new name.
                (let [x (cond-> x ((:started w) x) (-> name (str "'" (count (:starts w))) keyword))]
                  [(-> w (update :started conj x) (update :starts conj x)) {:value x}]))]
    (case type
      :log-failure [(update w :logs conj [a (last action)]) {:value nil}]
      :await-drain (await-drain w a)
      (let [[w failed] (outcome w action)]
        (if failed
          [w failed]
          (case type
            :swap (let [state (apply a (:state w) more)] [(set-state w state) {:value state}])
            :compile-config [w {:value (assoc (select-keys c [:cert-plan :ticket-policy])
                                              :snapshot (:config c) :compiled-config :compiled)}]
            :start-cert-runtime (start w (:cert-automation c))
            :start-ticket-service (start w (:memory-ticket-service c))
            :start-generation (start w (:instance c))
            :retire [(assoc-in w [:drains (:generation-id a)] (rt/begin-drain a))
                     {:value [:drain (:generation-id a)]}]
            [w {:value nil}]))))))

(defn- advance
  "Does the action of `progress` and steps the core. Returns `[world progress
  result]`, with the result once the core is done."
  [w progress]
  (let [[w o] (perform w (:action progress))
        {[kind result] :action :as progress} (rt/step progress o)]
    [w progress (when (= :done kind) result)]))

(defn- errors-of
  "Returns each error of `result`: its error, its suppressed errors, and their
  causes and suppressed errors."
  [{:keys [error suppressed]}]
  (letfn [(walk [^Throwable t]
            (cons t (mapcat walk (remove nil? (cons (.getCause t) (.getSuppressed t))))))]
    (mapcat walk (cond->> suppressed error (cons error)))))

(defn- interrupted?
  [result]
  (some #(instance? InterruptedException %) (errors-of result)))

(defn- end-op
  [{:keys [op] :as w} progress result]
  (if-not result
    (assoc w :lifecycle progress)
    (cond-> (-> (dissoc w :lifecycle)
                (update :builds rest)
                (update :results conj result)
                (assoc :result result))
      (= :start op) (as-> w (if-let [g (:value result)]
                              (set-state w (rt/initial-state g :pool))
                              (assoc w :ops [])))
      (= :stop op) (assoc :stop-result result :stop-returned? (not (interrupted? result))))))

(defn- step-lifecycle
  [{:keys [state] :as w}]
  (if-let [progress (:lifecycle w)]
    (apply end-op (advance w progress))
    (let [[[op observation] & more] (:ops w)
          {[kind result] :action :as progress}
          (case op
            :start (rt/begin-start)
            :reload (rt/begin-reload (merge {:state state :snapshot (:config (:active state))}
                                            observation))
            :stop (rt/begin-stop state))]
      (end-op (assoc w :ops more :op op) progress (when (= :done kind) result)))))

(defn- step-drain
  [w id]
  (let [[w progress result] (advance w (get-in w [:drains id]))]
    (if result
      (-> w (update :drains dissoc id) (assoc-in [:drain-results id] result))
      (assoc-in w [:drains id] progress))))

(defn- actors
  "Returns the actors that can do a step: the lifecycle thread unless it awaits
  a drain that has no result, each drain thread from the newest, and an
  interrupt that can end that await."
  [{:keys [lifecycle ops drains drain-results interrupts interrupted?]}]
  (let [[type {id :generation-id}] (:action lifecycle)
        waits? (and (= :await-drain type) (not (contains? drain-results id)) (not interrupted?))]
    (concat (when (and (or lifecycle (seq ops)) (not waits?)) [:lifecycle])
            (sort > (keys drains))
            (when (and waits? (pos? interrupts)) [:interrupt]))))

(defn- simulate
  "Runs the actors of `w` to their end. When two or more actors can step, the
  next item of `schedule` picks one. When the schedule ends, the lifecycle
  thread steps, or else the newest drain."
  [w schedule]
  (loop [w w
         schedule schedule
         n 0]
    (assert (< n 1000) "the core loops")
    (let [actors (vec (actors w))
          choice? (< 1 (count actors))
          actor (when (seq actors) (actors (mod (if choice? (or (first schedule) 0) 0) (count actors))))]
      (if (or (nil? actor) (and (:ops-only? w) (nil? (:lifecycle w)) (empty? (:ops w))))
        w
        (recur (case actor
                 :lifecycle (step-lifecycle w)
                 :interrupt (-> w (update :interrupts dec) (assoc :interrupted? true))
                 (step-drain w actor))
               (cond-> schedule choice? rest)
               (inc n))))))

(defn- run-op
  [w op & {:as observation}]
  (simulate (assoc w :ops [[op observation]] :ops-only? true) []))

(defn- reload
  [w & observation]
  (apply run-op w :reload :snapshot {:version 1} observation))

(defn- stop
  [w]
  (run-op w :stop))

(defn- start
  [w]
  (run-op w :start))

(def ^:private one
  (generation 1))

(defn- running
  [& draining]
  (update (rt/initial-state one :pool) :draining into draining))

(defn- reloaded
  "The state after a reload from generation 1 to generation 2, with the drain
  of generation 1 attached."
  []
  (-> (rt/publish (running) (generation 2))
      (rt/attach-drain 1 [:drain 1])))

(defn- broken
  "Returns the invariants that the run `w` broke."
  [{:keys [state started stops generation-stops stop-result results logs raised] :as w}]
  (let [stopped (frequencies (concat stops generation-stops))
        reported (frequencies (concat (mapcat errors-of results) (map second logs)))]
    (cond-> (:broken w)
      (or (:lifecycle w) (seq (:ops w)))
      (conj :every-operation-returns)

      (or (some #(< 1 %) (vals stopped))
          (not-every? started (keys stopped))
          (and (or (nil? state) stop-result) (not-every? stopped started)))
      (conj :each-service-and-generation-stops-once)

      (not-every? #(= 1 (reported %)) raised)
      (conj :every-error-is-reported-once)

      (and stop-result (not= :stopped (:phase state)))
      (conj :a-stop-leaves-the-server-stopped))))

;; Schedules

(def ^:private run-gen
  (gen/hash-map
   :reloads (gen/vector gen/boolean 0 3)
   :builds (gen/vector (gen/hash-map :cert (gen/elements [:new :same nil])
                                     :tickets (gen/elements [:new :same nil]))
                       4)
   :fails (gen/vector (gen/frequency [[6 (gen/return false)] [1 (gen/return true)]]) 0 40)
   :interrupts (gen/choose 0 2)
   :schedule (gen/vector (gen/choose 0 4) 0 80)))

(defn- schedule-world
  "A world that starts a server, does a reload for each item of `reloads`, a
  forced one for true, and stops the server."
  [{:keys [reloads] :as run}]
  (world nil
         (assoc (select-keys run [:builds :fails :interrupts])
                :ops (concat [[:start]] (map #(vector :reload {:force? %}) reloads) [[:stop]]))))

(defspec every-schedule-keeps-the-invariants 5000
  (prop/for-all [run run-gen]
                (let [broken (broken (simulate (schedule-world run) (:schedule run)))]
                  (reify results/Result
                    (pass? [_] (empty? broken))
                    (result-data [_] {:broken broken})))))

(deftest stop-that-begins-during-a-drain-returns-its-errors-test
  (doseq [n (range 5)]
    (testing (str "after " n " drain actions")
      (let [w (simulate (world (reloaded) :ops [[:stop]] :stop-errors #{:cert-1 :tickets-1})
                        (concat (repeat n 1) (repeat 0)))]
        (is (= #{{:service :cert-1} {:service :tickets-1}}
               (set (keep ex-data (errors-of (:stop-result w))))))
        (is (= #{} (broken w)))))))

(deftest interrupt-that-ends-the-await-of-a-drain-logs-its-errors-test
  ;; Drain 1 marks its generation drained, then the stop begins, awaits that
  ;; drain, and an interrupt ends the await.
  (let [w (simulate (world (reloaded) :ops [[:stop]] :stop-errors #{:cert-1 :tickets-1}
                           :interrupts 1)
                    (concat [1 1] (repeat 5 0) [2] (repeat 0)))]
    (is (instance? InterruptedException (:error (:stop-result w))))
    (is (= #{{:service :cert-1} {:service :tickets-1}} (set (map (comp ex-data second) (:logs w)))))
    (is (= #{} (broken w)))))

;; Reload rows

(deftest unchanged-configuration-does-nothing-test
  (let [w (reload (world (running)))]
    (is (= {:value :unchanged} (:result w)))
    (is (= [] (:starts w)))
    (is (= (running) (:state w)))))

(deftest forced-reload-activates-and-retires-the-old-generation-test
  (let [w (reload (world (running)) :force? true)]
    (is (= {:value :activated} (:result w)))
    (is (= (generation 2) (get-in w [:state :active])))
    (is (= [(assoc one :drain [:drain 1])] (get-in w [:state :draining])))
    (is (= 3 (get-in w [:state :next-generation-id])))
    (is (= #{:cert-2 :tickets-2 :instance-2} (set (:starts w))))
    (is (= 3 (count (:starts w))))
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
        retry (reload w)]
    (is (= {:value :unchanged} (:result retry)))
    (is (= (:starts w) (:starts retry)))))

(deftest reload-of-a-stopping-server-fails-test
  (doseq [phase [:stopping :stopped]]
    (let [w (reload (world (assoc (running) :phase phase)) :force? true)
          e (:error (:result w))]
      (is (= "Server is stopping" (ex-message e)))
      (is (= {:reason :server-stopping} (ex-data e)))
      (is (= [] (:starts w))))))

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
        (is (= (frequencies (map (generation 2) stops)) (frequencies (:starts w)) (frequencies (:stops w)))
            "the build starts nothing after the failed action")
        (is (= (running) (:state w)))))))

(deftest build-reuses-a-service-only-for-the-same-need-test
  (testing "the same need reuses the service of the old generation"
    (let [w (reload (world (running)
                           :candidate (generation 2 :config {:version 1} :cert-plan [:plan 1]
                                                  :ticket-policy [:policy 1]))
                    :force? true)]
      (is (= [:instance-2] (:starts w)))
      (is (= [:cert-1 :tickets-1]
             (map (get-in w [:state :active]) [:cert-automation :memory-ticket-service])))
      (is (= {:cert-1 1 :tickets-1 1} (frequencies (:stops (stop w)))))))
  (testing "no need starts no service"
    (let [w (reload (world (running)
                           :candidate (generation 2 :config {:version 1} :cert-plan nil
                                                  :ticket-policy nil))
                    :force? true)]
      (is (= [:instance-2] (:starts w)))
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
    (is (= [:instance-2] (:generation-stops w)))
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
    (is (= {:cert-2 1 :tickets-2 1} (frequencies (:stops w))))
    (is (identical? stop-error (first suppressed)))
    (is (= #{{:service :cert-2} {:service :tickets-2}} (set (map ex-data (rest suppressed)))))))

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
    (is (= {:cert-2 1 :tickets-2 1} (frequencies (:stops w))))
    (is (= [:instance-2] (:generation-stops w)))
    (is (= (running) (:state w)))))

(deftest attach-failure-still-awaits-the-drain-in-a-stop-test
  (let [w (stop (world (running) :script {[:swap rt/attach-drain] {:error (Error.)}}))]
    (is (= {:value nil} (:result w)))
    (is (= [:instance-1] (:generation-stops w)) "the stop awaits the drain")
    (is (= {:cert-1 1 :tickets-1 1} (frequencies (:stops w))))))

(deftest begin-stopping-failure-ends-the-stop-and-changes-nothing-test
  (let [cause (Error. "simulated")
        w (stop (world (running) :script {[:swap rt/begin-stopping] {:error cause}}))]
    (is (= {:error cause} (:result w)))
    (is (= [] (:stops w)))
    (is (= [] (:generation-stops w)))
    (is (= (running) (:state w)))))

(deftest end-stopped-failure-is-added-to-the-stop-failures-test
  (let [e1 (err)
        cause (Error. "simulated")
        w (stop (world (running) :script {[:stop-generation 1] {:error e1}
                                          [:swap rt/end-stopped] {:error cause}}))]
    (is (= {:error e1 :suppressed [cause]} (:result w)))))

(deftest drain-whose-last-swap-fails-gives-its-errors-to-the-next-stop-test
  (let [cause (Error. "simulated")
        drained (simulate (world (reloaded) :stop-errors #{:cert-1}
                                 :script {[:swap rt/services-stopped] {:error cause}})
                          [])
        w (stop (assoc drained :script {}))
        [e & more] (errors-of (:result w))]
    (is (identical? cause e))
    (is (= [{:service :cert-1}] (map ex-data more)))
    (is (= [] (:logs w)))
    (is (= [1] (map :generation-id (get-in w [:state :draining]))))))

(deftest drain-whose-failure-swap-fails-gives-both-errors-to-the-next-stop-test
  (let [e1 (err {:generation-id 1})
        cause (Error. "simulated")
        failed (simulate (world (reloaded) :script {[:stop-generation 1] {:error e1}
                                                    [:swap rt/drain-failed] {:error cause}})
                         [])
        w (stop (assoc failed :script {}))]
    (is (= [e1 cause] (errors-of (:result w))))
    (is (= [] (:logs w)))
    (is (= [1] (map :generation-id (get-in w [:state :draining]))))))

(deftest stop-collects-every-failure-test
  (let [e0 (err {:generation-id 0})
        e1 (err {:generation-id 1})
        w (stop (world (running (generation 0 :drain [:drain 0]))
                       :script {[:stop-generation 0] {:error e0} [:stop-generation 1] {:error e1}}
                       :stop-errors #{:cert-0}))
        {:keys [error suppressed]} (:result w)]
    (is (identical? e0 error))
    (is (= [e1 {:service :cert-0}] [(first suppressed) (ex-data (second suppressed))]))
    (is (= [0 1] (map :generation-id (get-in w [:state :draining])))
        "generations that did not drain stay listed")))

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
        (is (= (frequencies (map one stops)) (frequencies (:stops w))))
        (is (= (filter #{:cert-1} (:stops w)) (map (comp :service ex-data) suppressed)))))))

(deftest start-activation-failure-stops-the-generation-and-its-services-test
  (let [cause (err)
        w (start (world nil :script {:activate {:error cause}}))]
    (is (= {:error cause :suppressed []} (:result w)))
    (is (= [:instance-1] (:generation-stops w)))
    (is (= {:cert-1 1 :tickets-1 1} (frequencies (:stops w))))))
