(ns ol.busker.callback-errors-test
  "Failures inside the Clojure functions behind native upcall stubs.

  A Throwable that leaves an upcall into native code ends the JVM, so these
  tests call the Clojure function an upcall stub wraps, on the test thread, or
  make Clojure code under an upcall fail in a way the callback contains."
  (:require
   [babashka.ffi :as ffi]
   [clojure.test :refer [deftest is]]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.config :as config]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.generation :as generation]
   [ol.busker.native :as h2o]
   [ol.busker.request :as request]
   [ol.busker.response :as response]
   [ol.busker.test-utils :as util]
   [ol.busker.worker-context :as worker-context]
   [taoensso.trove :as trove])
  (:import
   [java.util.concurrent.atomic AtomicLong AtomicLongArray AtomicReferenceArray]))

(defn- capture-logs
  "Calls `f` with trove bound to a backend that records each event as
  `{:id .. :error ..}` and returns `[result events]`."
  [f]
  (let [events (atom [])]
    [(binding [trove/*log-fn* (fn [_ns _coords _level id lazy_]
                                (swap! events conj {:id id :error (:error (force lazy_))}))]
       (f))
     @events]))

(defn- with-worker-context
  "Calls `f` with `worker` bound as the current event-loop worker."
  [worker f]
  (.set worker-context/worker-context worker)
  (try (f) (finally (.remove worker-context/worker-context))))

(defn- bound-dispatch
  [arena]
  (doto (callback-dispatch/create arena)
    (callback-dispatch/bind-thread! (Thread/currentThread))))

(defn- faults [dispatch]
  (:callback-fault (callback-dispatch/diagnostics dispatch)))

;; Request and cleanup callbacks

(deftest request-callback-answers-503-when-copying-the-request-fails
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (bound-dispatch arena)
          failure (OutOfMemoryError. "request copy failed")
          [result events]
          (with-redefs [h2o/copy-request-context (fn [_] (throw failure))]
            (with-worker-context {:callback-dispatch dispatch}
              #(capture-logs (fn [] (request/on-request nil nil {} nil ffi/null)))))]
      (is (= h2o/CLJ_HANDLER_OVERLOADED result))
      (is (= [{:id ::request/request-callback-failed :error failure}] events))
      (is (= 1 (faults dispatch))))))

(defn- failing-log-backend
  "Calls `f` with a trove backend that throws OutOfMemoryError."
  [f]
  (binding [trove/*log-fn* (fn [& _] (throw (OutOfMemoryError. "log failed")))]
    (f)))

(deftest request-callback-contains-a-failing-report
  (with-redefs [h2o/copy-request-context (fn [_] (throw (OutOfMemoryError. "request copy failed")))]
    (is (= h2o/CLJ_HANDLER_OVERLOADED
           (failing-log-backend #(request/on-request nil nil {} nil ffi/null))))))

(deftest request-callback-stub-reports-what-on-request-throws
  (let [port (util/free-port)
        instance (generation/start!
                  (config/load!
                   {:entrypoints {:http {:bind (str "127.0.0.1:" port) :tls false}}
                    :dispatch [{:handler (constantly {:status 200 :body "ok"})}]})
                  nil)
        failure (StackOverflowError.)]
    (try
      (let [on-request-cb (-> instance ::generation/handler ::h2o/on-request-cb)
            [result events]
            (with-redefs [request/on-request (fn [& _] (throw failure))]
              (capture-logs #(on-request-cb ffi/null)))]
        (is (= h2o/CLJ_HANDLER_OVERLOADED result))
        (is (= [{:id ::request/request-callback-failed :error failure}] events)))
      (finally
        (generation/stop! instance)))))

(deftest request-cleanup-callback-contains-errors
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (bound-dispatch arena)
          failure (AssertionError. "retire failed")
          capacity-notified (atom 0)
          [result events]
          (with-redefs [callback-dispatch/retire! (fn [_ _ _] (throw failure))
                        fixed-final/notify-capacity! (fn [_] (swap! capacity-notified inc))]
            (with-worker-context {:callback-dispatch dispatch}
              #(capture-logs (fn [] (request/on-request-cleanup 1 1)))))]
      (is (nil? result))
      (is (= 1 @capacity-notified))
      (is (= [{:id ::request/cleanup-callback-failed :error failure}] events))
      (is (= 1 (faults dispatch))))))

(deftest request-cleanup-callback-contains-a-failing-report
  (with-redefs [callback-dispatch/retire! (fn [_ _ _] (throw (AssertionError. "retire failed")))
                fixed-final/notify-capacity! (fn [_] nil)]
    (with-worker-context {:callback-dispatch nil}
      #(is (nil? (failing-log-backend (fn [] (request/on-request-cleanup 1 1))))))))

(deftest cleanup-callback-keeps-the-interrupt-status
  (with-redefs [callback-dispatch/retire! (fn [_ _ _] (throw (InterruptedException.)))
                fixed-final/notify-capacity! (fn [_] nil)]
    (try
      (with-worker-context {:callback-dispatch nil}
        #(request/on-request-cleanup 1 1))
      (is (Thread/interrupted))
      (finally (Thread/interrupted)))))

;; Accept and connection-close callbacks

(defn- run-accepts
  "Runs one accept on worker 0 of one for each `[outcome millis]` of `steps`,
  with the clock at `millis`. An outcome is `:ok`, `:limit` for an accept at the
  connection limit, or a Throwable for `h2o-accept` to throw; `natives` replaces
  further H2O calls. Closing a socket whose close callback is installed runs
  that callback, as H2O does. Returns what the accepts took, gave back and
  reported."
  [steps natives]
  (let [active (AtomicLong.)
        counts (AtomicLongArray. 1)
        worker-failures (atom 0)
        worker {:on-native-failure! (fn [_ _] (swap! worker-failures inc))}
        workers (doto (AtomicReferenceArray. 1) (.set 0 worker))
        calls (atom [])
        record (fn [k] (fn [& _] (swap! calls conj k) nil))
        close-installed? (atom false)
        per-worker (#'generation/create-connection-close-callback active counts 0)
        sock (ffi/alloc (ffi/auto-arena) 8)
        outcome_ (atom nil)
        clock_ (atom 0)
        {::generation/keys [accept-cb]}
        (#'generation/create-accept-callback ::accept-ctx per-worker active counts workers 0)
        defaults {#'generation/now-nanos (fn [] (* 1000000 @clock_))
                  #'h2o/socket-read-stop (record :read-stop)
                  #'h2o/evloop-socket-accept (fn [_] sock)
                  #'h2o/conn-limit-try-acquire (fn []
                                                 (swap! calls conj :acquire)
                                                 (if (= :limit @outcome_) 0 1))
                  #'h2o/conn-limit-release (record :release)
                  #'h2o/socket-set-on-close (fn [& _] (reset! close-installed? true))
                  #'h2o/h2o-accept (fn [& _]
                                     (when (instance? Throwable @outcome_) (throw @outcome_))
                                     (reset! close-installed? false))
                  #'h2o/socket-close (fn [_]
                                       (swap! calls conj :socket-close)
                                       (when @close-installed?
                                         (reset! close-installed? false)
                                         ((::generation/connection-close-cb per-worker) sock)))}
        [_ events]
        (with-redefs-fn (merge defaults natives)
          #(capture-logs (fn []
                           (doseq [[outcome millis] steps]
                             (reset! outcome_ outcome)
                             (reset! clock_ millis)
                             (accept-cb ::listener ffi/null)))))]
    {:events (mapv :id events)
     :worker-failures @worker-failures
     :in-run? (not= Long/MIN_VALUE
                    (.get ^AtomicLong (:started (::generation/accept-failure-run per-worker))))
     :calls (frequencies @calls)
     :active (.get active)
     :count (.get counts 0)}))

(def ^:private accept-failure (OutOfMemoryError. "accept failed"))

(deftest failed-accept-before-h2o-gives-back-the-slot-counts-and-socket
  (let [{:keys [events worker-failures calls active count]}
        (run-accepts [[:ok 0] [:ok 1]]
                     {#'h2o/socket-set-on-close
                      (fn [& _] (throw (IllegalStateException. "set-on-close failed")))})]
    (is (= 0 active))
    (is (= 0 count))
    (is (= {:acquire 2 :release 2 :socket-close 2 :read-stop 2} calls))
    (is (= 0 worker-failures))
    (is (= [::generation/accept-callback-failed] events)
        "Only the first failure of a run is logged.")))

(deftest failed-h2o-accept-closes-the-socket-and-its-close-callback-gives-back
  (let [{:keys [calls active count]} (run-accepts [[accept-failure 0] [accept-failure 1]] {})]
    (is (= 0 active))
    (is (= 0 count))
    (is (= {:acquire 2 :release 2 :socket-close 2 :read-stop 2} calls)
        "The close callback releases each slot once.")))

(deftest failed-accept-before-a-socket-stops-the-listener-for-the-turn
  (let [{:keys [events calls active count]}
        (run-accepts [[:ok 0]]
                     {#'h2o/evloop-socket-accept
                      (fn [_] (throw (IllegalStateException. "accept failed")))})]
    (is (= {:read-stop 1} calls))
    (is (= [0 0] [active count]))
    (is (= [::generation/accept-callback-failed] events))))

(deftest failed-accepts-that-clear-leave-the-worker-serving
  (let [{:keys [events worker-failures in-run? calls active count]}
        (run-accepts (concat (map (fn [ms] [accept-failure ms]) (range 0 20 2)) [[:ok 21]]) {})]
    (is (= 0 worker-failures) "The worker keeps serving.")
    (is (false? in-run?) "The completed accept ends the run.")
    (is (= [1 1] [active count]) "Only the completed connection is counted.")
    (is (= 1 (- (:acquire calls) (:release calls))) "Only its slot is taken.")
    (is (= [::generation/accept-callback-failed] events) "One log line for the run.")))

(deftest failed-accepts-that-last-a-second-fail-the-worker-once
  (let [{:keys [events worker-failures active count]}
        (run-accepts (map (fn [ms] [accept-failure ms]) [0 400 999 1000 1500 2000]) {})]
    (is (= 1 worker-failures))
    (is (= [0 0] [active count]))
    (is (= [::generation/accept-callback-failed ::generation/accept-failures-failed-worker]
           events)
        "The run is logged once, and failing the worker once more.")))

(deftest an-accept-at-the-connection-limit-does-not-end-a-run
  (let [{:keys [worker-failures in-run?]}
        (run-accepts [[accept-failure 0] [:limit 500] [accept-failure 1000]] {})]
    (is (true? in-run?))
    (is (= 1 worker-failures))))

(deftest connection-close-callback-contains-and-logs-errors
  (let [failure (IllegalStateException. "release failed")
        {::generation/keys [connection-close-cb]}
        (#'generation/create-connection-close-callback (AtomicLong. 1)
                                                       (AtomicLongArray. 1) 0)
        [result events]
        (with-redefs [h2o/conn-limit-release (fn [] (throw failure))]
          (capture-logs #(connection-close-cb ffi/null)))]
    (is (nil? result))
    (is (= [{:id ::generation/connection-close-callback-failed :error failure}]
           events))))

;; Body, proceed and stop callbacks

(defn- register!
  "Registers `req` and `emitter` in `dispatch` and returns the request sequence."
  [dispatch req emitter]
  (let [request-seq (callback-dispatch/allocate-identity! dispatch)]
    (callback-dispatch/register! dispatch (:module-id dispatch) request-seq req emitter)
    request-seq))

(deftest body-callback-logs-the-failure-as-its-error
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (bound-dispatch arena)
          failure (IllegalStateException. "chunk failed")
          request-seq (register! dispatch {:write-req {:write-chunk (fn [_ _] (throw failure))}} nil)
          [_ events] (capture-logs
                      #((:body-callback dispatch) (:module-id dispatch) request-seq ffi/null 0 1))]
      (is (= [{:id ::callback-dispatch/body-callback-failed :error failure}] events))
      (is (= 1 (faults dispatch))))))

(deftest body-callback-contains-a-failing-log-backend
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (bound-dispatch arena)
          request-seq (register! dispatch
                                 {:write-req {:write-chunk (fn [_ _] (throw (IllegalStateException. "chunk failed")))}}
                                 nil)]
      (binding [trove/*log-fn* (fn [& _] (throw (RuntimeException. "backend failed")))]
        (is (nil? ((:body-callback dispatch) (:module-id dispatch) request-seq ffi/null 0 1))))
      (is (= 1 (faults dispatch))))))

(deftest proceed-and-stop-callbacks-contain-and-log-errors
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (bound-dispatch arena)
          request-seq (register! dispatch {} ::emitter)
          proceed-failure (IllegalStateException. "proceed failed")
          stop-failure (IllegalStateException. "stop failed")
          [results events]
          (capture-logs
           #(vector
             (with-redefs [response/writer-if-created (fn [_] (throw proceed-failure))]
               ((:proceed-callback dispatch) (:module-id dispatch) request-seq))
             (with-redefs [response/writer-if-created (fn [_] nil)
                           response/stop-emitter (fn [_] (throw stop-failure))]
               ((:stop-callback dispatch) (:module-id dispatch) request-seq 0))))]
      (is (= [nil nil] results))
      (is (= [{:id ::callback-dispatch/proceed-callback-failed :error proceed-failure}
              {:id ::callback-dispatch/stop-callback-failed :error stop-failure}]
             events))
      (is (= 2 (faults dispatch))))))

(deftest tls-lookup-callback-contains-and-logs-errors
  (with-open [arena (ffi/shared-arena)]
    (let [failure (IllegalStateException. "lookup failed")
          {:keys [callback]} (h2o/build-tls-lookup-callback (fn [_] (throw failure)))
          outputs (vec (repeatedly 4 #(ffi/alloc arena 8)))
          _ (doseq [out outputs] (ffi/write out :long 7))
          [result events] (capture-logs
                           #(apply callback ffi/null 0 (conj outputs ffi/null)))]
      (is (= -1 result))
      (is (= [{:id ::h2o/tls-lookup-callback-exception :error failure}] events))
      (is (= [0 0 0 0] (mapv #(ffi/read % :long) outputs))))))
