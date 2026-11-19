(ns ol.busker.callback-errors-test
  "Failures inside the Clojure functions behind native upcall stubs.

  A Throwable that leaves an upcall into native code ends the JVM, so these
  tests call the Clojure function an upcall stub wraps, on the test thread, or
  make Clojure code under an upcall fail in a way the callback contains."
  (:require
   [babashka.ffi :as ffi]
   [babashka.process :as p]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.config :as config]
   [ol.busker.generation :as generation]
   [ol.busker.generation.accept :as accept]
   [ol.busker.native :as h2o]
   [ol.busker.request :as request]
   [ol.busker.response :as response]
   [ol.busker.test-utils :as util]
   [ol.busker.worker-context :as worker-context]
   [taoensso.trove :as trove])
  (:import
   [java.io IOException InputStream]
   [java.net InetSocketAddress Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong AtomicLongArray AtomicReferenceArray]))

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

(defn- request-seqs
  "A native array of `seqs` in `arena`, as the shim passes it to the cleanup
  callback."
  [arena seqs]
  (let [^java.lang.foreign.MemorySegment segment (ffi/alloc arena (* 8 (count seqs)))]
    (dotimes [i (count seqs)]
      (.setAtIndex segment java.lang.foreign.ValueLayout/JAVA_LONG (long i) (long (nth seqs i))))
    segment))

(deftest request-cleanup-callback-contains-errors
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (bound-dispatch arena)
          failure (AssertionError. "retire failed")
          [result events]
          (with-redefs [callback-dispatch/retire! (fn [_ _ _] (throw failure))]
            (with-worker-context {:callback-dispatch dispatch}
              #(capture-logs (fn [] (request/on-request-cleanup 1 (request-seqs arena [1]) 1)))))]
      (is (nil? result))
      (is (= [{:id ::request/cleanup-callback-failed :error failure}] events))
      (is (= 1 (faults dispatch))))))

(deftest request-cleanup-callback-retires-the-rest-of-a-batch-after-a-failure
  (with-open [arena (ffi/shared-arena)]
    (let [failure (AssertionError. "retire failed")
          retired (atom [])
          [_ events]
          (with-redefs [callback-dispatch/retire! (fn [_ _ request-seq]
                                                    (swap! retired conj request-seq)
                                                    (when (= 2 request-seq) (throw failure)))]
            (with-worker-context {:callback-dispatch nil}
              #(capture-logs (fn [] (request/on-request-cleanup 1 (request-seqs arena [1 2 3]) 3)))))]
      (is (= [1 2 3] @retired))
      (is (= [{:id ::request/cleanup-callback-failed :error failure}] events)))))

(deftest request-cleanup-callback-contains-a-failing-report
  (with-open [arena (ffi/shared-arena)]
    (with-redefs [callback-dispatch/retire! (fn [_ _ _] (throw (AssertionError. "retire failed")))]
      (with-worker-context {:callback-dispatch nil}
        #(is (nil? (failing-log-backend
                    (fn [] (request/on-request-cleanup 1 (request-seqs arena [1]) 1)))))))))

(deftest cleanup-callback-keeps-the-interrupt-status
  (with-open [arena (ffi/shared-arena)]
    (with-redefs [callback-dispatch/retire! (fn [_ _ _] (throw (InterruptedException.)))]
      (try
        (with-worker-context {:callback-dispatch nil}
          #(request/on-request-cleanup 1 (request-seqs arena [1]) 1))
        (is (Thread/interrupted))
        (finally (Thread/interrupted))))))

;; Accept and connection-close callbacks

(defn- run-accepts
  "Runs one accept on worker 0 of one for each `[outcome millis]` of `steps`,
  with the clock at `millis` and `queued` sockets queued, 1 by default. An
  outcome is `:ok`, `:limit` for an accept at the connection limit, or a
  Throwable for `h2o-accept` to throw. `natives` replaces further H2O calls.
  `shutting-down?` is the generation's stop flag. Closing a socket whose
  close callback is installed runs that callback, as H2O does. Returns what
  the accepts took, gave back and reported, and how many sockets stay queued."
  [steps natives & {:keys [queued shutting-down?]
                    :or {queued 1 shutting-down? (AtomicBoolean.)}}]
  (let [active (AtomicLong.)
        counts (AtomicLongArray. 1)
        worker-failures (atom 0)
        worker {:on-native-failure! (fn [_ _] (swap! worker-failures inc))}
        workers (doto (AtomicReferenceArray. 1) (.set 0 worker))
        calls (atom [])
        record (fn [k] (fn [& _] (swap! calls conj k) nil))
        close-installed? (atom false)
        per-worker (#'accept/create-connection-close-callback active counts 0)
        sock (ffi/alloc (ffi/auto-arena) 8)
        queued_ (atom 0)
        outcome_ (atom nil)
        clock_ (atom 0)
        {::accept/keys [accept-cb]}
        (#'accept/create-accept-callback ::accept-ctx per-worker active counts workers 0 0
                                         shutting-down?)
        defaults {#'accept/now-nanos (fn [] (* 1000000 @clock_))
                  #'h2o/socket-read-stop (record :read-stop)
                  #'h2o/evloop-socket-accept (fn [_]
                                               (if (pos? @queued_)
                                                 (do (swap! queued_ dec) sock)
                                                 ffi/null))
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
                                         ((::accept/connection-close-cb per-worker) sock)))}
        [_ events]
        (with-redefs-fn (merge defaults natives)
          #(capture-logs (fn []
                           (doseq [[outcome millis] steps]
                             (reset! queued_ queued)
                             (reset! outcome_ outcome)
                             (reset! clock_ millis)
                             (accept-cb ::listener ffi/null)))))]
    {:events (mapv :id events)
     :worker-failures @worker-failures
     :in-run? (not= Long/MIN_VALUE
                    (.get ^AtomicLong (:started (::accept/accept-failure-run per-worker))))
     :calls (frequencies @calls)
     :active (.get active)
     :count (.get counts 0)
     :queued @queued_}))

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
    (is (= [::accept/accept-callback-failed] events)
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
    (is (= [::accept/accept-callback-failed] events))))

(deftest failed-accepts-that-clear-leave-the-worker-serving
  (let [{:keys [events worker-failures in-run? calls active count]}
        (run-accepts (concat (map (fn [ms] [accept-failure ms]) (range 0 20 2)) [[:ok 21]]) {})]
    (is (= 0 worker-failures) "The worker keeps serving.")
    (is (false? in-run?) "The completed accept ends the run.")
    (is (= [1 1] [active count]) "Only the completed connection is counted.")
    (is (= 1 (- (:acquire calls) (:release calls))) "Only its slot is taken.")
    (is (= [::accept/accept-callback-failed] events) "One log line for the run.")))

(deftest failed-accepts-that-last-a-second-fail-the-worker-once
  (let [{:keys [events worker-failures active count]}
        (run-accepts (map (fn [ms] [accept-failure ms]) [0 400 999 1000 1500 2000]) {})]
    (is (= 1 worker-failures))
    (is (= [0 0] [active count]))
    (is (= [::accept/accept-callback-failed ::accept/accept-failures-failed-worker]
           events)
        "The run is logged once, and failing the worker once more.")))

(deftest an-accept-at-the-connection-limit-does-not-end-a-run
  (let [{:keys [worker-failures in-run?]}
        (run-accepts [[accept-failure 0] [:limit 500] [accept-failure 1000]] {})]
    (is (true? in-run?))
    (is (= 1 worker-failures))))

(deftest a-stop-during-an-accept-callback-leaves-later-sockets-queued
  (let [stopping (AtomicBoolean.)
        {:keys [queued active count]}
        (run-accepts [[:ok 0]]
                     {#'h2o/h2o-accept (fn [& _] (.set stopping true))}
                     :queued 2 :shutting-down? stopping)]
    (is (= [1 1] [active count]) "The socket accepted before the stop is served.")
    (is (= 1 queued) "The next generation accepts the second socket.")))

(deftest connection-close-callback-contains-and-logs-errors
  (let [failure (IllegalStateException. "release failed")
        {::accept/keys [connection-close-cb]}
        (#'accept/create-connection-close-callback (AtomicLong. 1)
                                                   (AtomicLongArray. 1) 0)
        [result events]
        (with-redefs [h2o/conn-limit-release (fn [] (throw failure))]
          (capture-logs #(connection-close-cb ffi/null)))]
    (is (nil? result))
    (is (= [{:id ::accept/connection-close-callback-failed :error failure}]
           events))))

;; Body, proceed and stop callbacks

(defn- register!
  "Registers `req` and `emitter` in `dispatch` and returns the request sequence."
  [dispatch req emitter]
  (let [request-seq (callback-dispatch/allocate-identity! dispatch)]
    (callback-dispatch/register! dispatch (:module-id dispatch) request-seq req emitter)
    request-seq))

(deftest body-callback-reports-delivery-and-fails-the-body-on-error
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (bound-dispatch arena)
          failure (IllegalStateException. "chunk failed")
          failed (atom [])
          throw? (atom false)
          request-seq (register! dispatch
                                 {:write-req {:write-chunk (fn [_ _] (when @throw? (throw failure)))
                                              :fail! #(swap! failed conj %)}}
                                 nil)
          body (fn [] ((:body-callback dispatch) (:module-id dispatch) request-seq ffi/null 0 1))
          ok (body)
          _ (reset! throw? true)
          [failed-result events] (capture-logs body)]
      (is (= 0 ok))
      (is (= 1 failed-result))
      (is (= [failure] @failed))
      (is (= [{:id ::callback-dispatch/body-callback-failed :error failure}] events))
      (is (= 1 (faults dispatch)))
      (is (= 1 ((:body-callback dispatch) (:module-id dispatch) (inc request-seq) ffi/null 0 1))
          "A chunk for no live request is not delivered."))))

(deftest body-callback-contains-a-failing-log-backend
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (bound-dispatch arena)
          request-seq (register! dispatch
                                 {:write-req {:write-chunk (fn [_ _] (throw (IllegalStateException. "chunk failed")))}}
                                 nil)]
      (binding [trove/*log-fn* (fn [& _] (throw (RuntimeException. "backend failed")))]
        (is (= 1 ((:body-callback dispatch) (:module-id dispatch) request-seq ffi/null 0 1))))
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

;; Request body channel

(deftest failed-request-body-wakes-the-reader-with-an-ioexception
  (let [{:keys [write-chunk fail! ^InputStream input-stream]}
        (request/create-write-req-channel (fn [] nil))
        cause (IllegalStateException. "chunk failed")
        reader (future (try (.read input-stream) (catch IOException e e)))]
    (Thread/sleep 100)
    (fail! cause)
    (let [result (deref reader 2000 ::timeout)]
      (is (instance? IOException result))
      (is (identical? cause (ex-cause result))))
    (write-chunk (.getBytes "late" StandardCharsets/US_ASCII) true)
    (is (thrown? IOException (.read input-stream)) "Later reads fail too.")
    (.close input-stream)
    (is (thrown? IOException (.read input-stream)))))

;; Failed request bodies against a real server

(defn- throwing-body-channel
  "Returns a replacement for `request/create-write-req-channel` whose
  `:write-chunk` throws on its `n`th call, after recording that call's chunk
  length and end flag in `failed-chunk_`."
  [n failed-chunk_]
  (let [create request/create-write-req-channel]
    (fn [proceed-callback]
      (let [calls (AtomicLong.)]
        (update (create proceed-callback) :write-chunk
                (fn [write-chunk]
                  (fn [chunk is-last]
                    (if (= n (.incrementAndGet calls))
                      (do (reset! failed-chunk_ [(some-> ^bytes chunk alength) is-last])
                          (throw (IllegalStateException. "chunk handling failed")))
                      (write-chunk chunk is-last)))))))))

(defn- reading-handler
  "Returns a handler that reads the whole request body and delivers what the
  read returned or threw to `read_`."
  [read_]
  (fn [{:keys [^InputStream body]}]
    (deliver read_ (try (count (.readAllBytes body)) (catch Throwable t t)))
    {:status 200 :body "read"}))

(defn- start-server
  [config handler]
  (generation/activate-http3-transports!
   (generation/start! (config/load! (util/with-handler handler config)) nil)))

(defn- dispatch-of [instance]
  (:callback-dispatch (first (::generation/workers instance))))

(defn- await-released
  "Waits until the request entry is gone and every stream block is back, and
  returns the dispatch diagnostics."
  [instance]
  (let [dispatch (dispatch-of instance)
        pool (:stream-pool (first (::generation/workers instance)))
        deadline (+ (System/nanoTime) (.toNanos TimeUnit/SECONDS 5))]
    (loop []
      (if (or (and (callback-dispatch/no-live-entries? dispatch)
                   (zero? (bp/checked-out pool)))
              (> (System/nanoTime) deadline))
        (do (Thread/sleep 200) (callback-dispatch/diagnostics dispatch))
        (do (Thread/sleep 20) (recur))))))

(defn- assert-request-ended
  [instance read_]
  (let [read (deref read_ 5000 ::timeout)]
    (is (instance? IOException read) (str "The handler's read failed: " read)))
  (is (= {:retired 1 :duplicate 0 :callback-fault 1}
         (select-keys (await-released instance) [:retired :duplicate :callback-fault]))
      "The request is released once.")
  (is (callback-dispatch/no-live-entries? (dispatch-of instance))))

(deftest failed-later-body-chunk-ends-an-http1-request
  (let [port (util/free-port)
        read_ (promise)
        failed-chunk_ (atom nil)]
    (with-redefs [request/create-write-req-channel (throwing-body-channel 2 failed-chunk_)]
      (let [instance (start-server {:n-workers 1
                                    :entrypoints {:http {:bind (str "127.0.0.1:" port)
                                                         :tls false}}}
                                   (reading-handler read_))]
        (try
          (with-open [socket (doto (Socket.)
                               (.connect (InetSocketAddress. "127.0.0.1" (int port)) 2000)
                               (.setSoTimeout 5000))]
            (let [out (.getOutputStream socket)]
              (.write out (.getBytes (str "POST / HTTP/1.1\r\nHost: localhost\r\n"
                                          "Content-Length: 10\r\n\r\nhello")
                                     StandardCharsets/US_ASCII))
              (.flush out)
              (Thread/sleep 300)
              (.write out (.getBytes "world" StandardCharsets/US_ASCII))
              (.flush out)
              (let [response (String. (.readAllBytes (.getInputStream socket))
                                      StandardCharsets/ISO_8859_1)]
                (is (str/starts-with? response "HTTP/1.1 502 ") response))))
          (is (= [5 true] @failed-chunk_))
          (assert-request-ended instance read_)
          (finally
            (generation/stop! instance)))))))

(def ^:private protocols
  {:h1 {:scheme :http :entrypoint {:http {:tls false}}}
   :h2 {:scheme :https :entrypoint {:tls {:http3? false :tls {:tls-compatibility-mode :modern}}}}
   :h3 {:scheme :https :entrypoint {:tls {:http3? true :tls {:tls-compatibility-mode :modern}}}}})

(defn- with-failing-first-chunk
  "Starts a one-worker server for `proto` whose first body chunk fails, calls
  `send` with the port, and returns `[send-result failed-chunk]` once the request
  has ended."
  [proto send]
  (let [port (util/free-port)
        read_ (promise)
        failed-chunk_ (atom nil)
        entrypoints (update-vals (get-in protocols [proto :entrypoint])
                                 #(assoc % :bind (str "127.0.0.1:" port)))]
    (with-redefs [request/create-write-req-channel (throwing-body-channel 1 failed-chunk_)]
      (let [instance (start-server (util/with-static-tls {:n-workers 1 :entrypoints entrypoints})
                                   (reading-handler read_))]
        (try
          (Thread/sleep 200)
          (let [result (send port)]
            (assert-request-ended instance read_)
            [result @failed-chunk_])
          (finally
            (generation/stop! instance)))))))

(defn- assert-answered
  [{:keys [exit out]}]
  (is (not= 28 exit) "The client is answered before its time limit.")
  (is (not (str/ends-with? out "\n200")) (str "exit " exit ": " out)))

;; HTTP/3 is covered only for bodies that arrive complete: a streamed HTTP/3
;; upload currently aborts the process inside H2O's receive path even when no
;; failure is injected.
(deftest failed-first-chunk-of-a-complete-body-ends-the-request
  (doseq [proto [:h1 :h2 :h3]]
    (testing proto
      (let [[result failed-chunk]
            (with-failing-first-chunk
              proto
              #(util/curl (get-in protocols [proto :scheme]) proto % "/"
                          :max-time 5
                          :args ["-o" "-" "-w" "\n%{http_code}"
                                 "--data-binary" "0123456789"]))]
        (is (= [10 true] failed-chunk) "The whole body arrived in the failing chunk.")
        (assert-answered result)
        (is (str/ends-with? (:out result) "\n502"))))))

(defn- curl-streamed
  "Uploads `hello`, then a second later `world`, with curl over `proto`."
  [proto port]
  (let [url (if (= :h1 proto)
              (str "http://127.0.0.1:" port "/")
              (str "https://localhost.examp1e.net:" port "/"))
        args (cond-> ["curl" "-k" "-s" "--max-time" "5" "-T" "-" "-o" "-" "-w" "'\n%{http_code}'"]
               (= :h1 proto) (into ["--http1.1" "-H" "'Expect:'"])
               (= :h2 proto) (into ["--http2" "--resolve"
                                    (str "localhost.examp1e.net:" port ":127.0.0.1")]))]
    (p/shell {:out :string :err :string :continue true}
             "bash" "-c"
             (str "(printf hello; sleep 1; printf world) | " (str/join " " (conj args url))))))

(deftest failed-first-chunk-of-a-streamed-body-ends-the-request
  (doseq [proto [:h1 :h2]]
    (testing proto
      (let [[result failed-chunk] (with-failing-first-chunk proto #(curl-streamed proto %))]
        (is (= [5 false] failed-chunk) "The failing chunk was the first of a streamed body.")
        (assert-answered result)
        (when (= :h1 proto)
          (is (str/ends-with? (:out result) "\n502")))))))
