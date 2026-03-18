(ns ol.busker.benchmark.server
  "Server benchmark harness mirroring the http-kit benchmark suite, extended
  with our ol.busker server."
  (:require
   [clj-async-profiler.core :as prof]
   [ol.busker :as busker]
   [ol.busker.benchmark.utils :as u]
   [org.httpkit.server :as http-kit]
   [ring.adapter.jetty :as jetty]
   [ring.adapter.jetty9 :as sunng-jetty])
  (:import
   [java.lang Thread]
   [java.util.concurrent
    ExecutorService
    Executors
    LinkedBlockingQueue
    TimeUnit]
   [org.eclipse.jetty.util.component LifeCycle]
   [org.eclipse.jetty.util.thread QueuedThreadPool]))

(set! *warn-on-reflection* true)

;;;; CSV formatting helpers ---------------------------------------------------

(defn- as-csv-row
  ([] ; headers
   (u/join->csv
    [(u/standard-csv-rows)
     ["Server.name" "Server.pool-type" "Server.min-threads" "Server.max-threads" "Server.queue-size"]
     ["wrk.version" "wrk.warm-up" "wrk.duration" "wrk.timeout" "wrk.threads" "wrk.conns" "wrk.keep-alive?"]
     ["wrk.error" "wrk.duration (µsecs)" "Reqs.total" "Reqs.per-sec" "Bytes.total" "Bytes.per-sec"]
     ["Latency.mean (µsecs)" "Latency.stdev (µsecs)" "Latency.min (µsecs)"
      "Latency.p50 (µsecs)" "Latency.p75 (µsecs)" "Latency.p80 (µsecs)"
      "Latency.p90 (µsecs)" "Latency.p98 (µsecs)" "Latency.p99 (µsecs)"
      "Latency.p99-9 (µsecs)" "Latency.p99-99 (µsecs)" "Latency.p99-999 (µsecs)"
      "Latency.p100 (µsecs)"]
     ["Errors.rate" "Errors.total" "Errors.connect" "Errors.read" "Errors.write" "Errors.status" "Errors.timeout"]]))

  ([{:as row :keys [worker wrk-result]}]
   (u/join->csv
    [(u/standard-csv-rows row)
     (u/quoted (:server-name row))
     (let [{:keys [type n-min-threads n-max-threads queue-size]} worker]
       [(some-> type name) n-min-threads n-max-threads queue-size])

     (let [{:keys [opts measurements error]} wrk-result
           {:keys [version warm-up duration timeout n-threads n-conns keep-alive?]} opts
           {:keys [usecs reqs reqs-per-sec bytes bytes-per-sec latency errors]} measurements]
       [version warm-up duration timeout n-threads n-conns keep-alive?
        error
        [usecs reqs reqs-per-sec bytes bytes-per-sec]
        (let [{:keys [mean stdev min p50 p75 p80 p90 p98 p99 p99-9 p99-99 p99-999 max]} latency]
          [mean stdev min p50 p75 p80 p90 p98 p99 p99-9 p99-99 p99-999 max])
        (let [{:keys [rate total connect read write status timeout]} errors]
          [(u/format-round4 rate) total connect read write status timeout])])])))

;;;; Server abstraction -------------------------------------------------------

(defn shutdown-pool [^ExecutorService pool timeout-msecs]
  (when pool
    (.shutdown pool)
    (.awaitTermination pool timeout-msecs TimeUnit/MILLISECONDS)
    (.shutdownNow pool)))

(defprotocol IServer
  (^:private server-start [_ handler port worker-opts])
  (^:private server-stop [_ timeout-msecs]))

;;;; Worker factories ---------------------------------------------------------

#_(defn- new-thread-factory [prefix]
    (.factory (.name (Thread/ofPlatform) prefix 0)))

(defn- new-h2o-worker [_opts]
  ;; ol.busker always executes Ring handlers on virtual threads; the native workers
  ;; remain fixed platform threads that drive libh2o.
  (let [executor (Executors/newVirtualThreadPerTaskExecutor)]
    {:pool executor
     :type :virtual
     :allow-virtual? true
     :queue-size nil
     :n-min-threads nil
     :n-max-threads nil}))

;;;; Server implementations ----------------------------------------------------

(deftype ServerH2O [state_]
  clojure.lang.IDeref
  (deref [_]
    (let [{:keys [worker port]} @state_]
      {:server-name u/dep-busker
       :running? (boolean port)
       :worker worker
       :port port}))

  IServer
  (server-start [_ handler port worker-opts]
    (when (nil? @state_)
      (let [{:keys [pool] :as worker} (new-h2o-worker worker-opts)
            #_#_n-workers (max 1 (or (:n-workers worker-opts)
                                     (:n-threads worker-opts)
                                     u/num-cores))
            n-workers (min (or (:n-workers worker-opts)
                               (:n-threads worker-opts)
                               u/num-cores) u/num-cores)
            _ (prof/start)
            server (busker/start!
                    {:entrypoints {:bench {:bind (str "127.0.0.1:" port)
                                           :http3? false
                                           :tls false}}
                     :dispatch [{:handler handler}]
                     :n-workers n-workers
                     :server-name "ol.busker/bench"
                     :executor pool})]
        (reset! state_
                {:server server
                 :pool pool
                 :worker (dissoc worker :pool)
                 :port port})
        true)))

  (server-stop [_ timeout-msecs]
    (when-let [{:keys [server pool]} @state_]
      (prof/stop)
      (busker/stop! server)
      (shutdown-pool pool timeout-msecs)
      (reset! state_ nil)
      true)))

(deftype ServerHttpKit [state_]
  clojure.lang.IDeref
  (deref [_]
    (let [{:keys [worker port]} @state_]
      {:server-name u/dep-http-kit
       :running? (boolean port)
       :worker worker
       :port port}))

  IServer
  (server-start [_ handler port worker-opts]
    (when (nil? @state_)
      (let [worker (http-kit/new-worker worker-opts)
            server (http-kit/run-server handler {:port port :worker-pool (:pool worker)})]
        (reset! state_
                {:server server
                 :worker (dissoc worker :pool :queue :n-cores)
                 :pool (:pool worker)
                 :port port})
        true)))

  (server-stop [_ timeout-msecs]
    (when-let [{:keys [server pool]} @state_]
      (server timeout-msecs)
      (shutdown-pool pool timeout-msecs)
      (reset! state_ nil)
      true)))

(deftype ServerSunngJetty [state_]
  clojure.lang.IDeref
  (deref [_]
    (let [{:keys [worker port]} @state_]
      {:server-name u/dep-sunng-jetty
       :running? (boolean port)
       :worker worker
       :port port}))

  IServer
  (server-start [_ handler port worker-opts]
    (when (nil? @state_)
      (let [max-threads (long (or (:n-threads worker-opts)
                                  (* 2 u/num-cores)))
            min-threads (long (max 1 (min max-threads (or (:n-min-threads worker-opts)
                                                          (quot max-threads 2)))))
            queue-size (some-> (:queue-size worker-opts) long)
            idle-timeout (int 60000)
            queue (if (and queue-size (pos? queue-size))
                    (LinkedBlockingQueue. (int queue-size))
                    (LinkedBlockingQueue.))
            thread-pool (QueuedThreadPool. (int max-threads)
                                           (int min-threads)
                                           idle-timeout
                                           queue)
            server (sunng-jetty/run-jetty handler {:port port :thread-pool thread-pool :join? false})
            worker {:type :queued
                    :n-min-threads (int min-threads)
                    :n-max-threads (int max-threads)
                    :queue-size (when queue-size (int queue-size))}]
        (reset! state_
                {:server server
                 :worker worker
                 :thread-pool thread-pool
                 :port port})
        true)))

  (server-stop [_ timeout-msecs]
    (when-let [{:keys [^org.eclipse.jetty.server.Server server thread-pool]} @state_]
      (.setStopTimeout server timeout-msecs)
      (.stop server)
      (when (instance? LifeCycle thread-pool)
        (.stop ^LifeCycle thread-pool))
      (reset! state_ nil)
      true)))

(deftype ServerJetty [state_]
  clojure.lang.IDeref
  (deref [_]
    (let [{:keys [worker port]} @state_]
      {:server-name u/dep-jetty
       :running? (boolean port)
       :worker worker
       :port port}))

  IServer
  (server-start [_ handler port worker-opts]
    (when (nil? @state_)
      (let [max-threads (long (or (:n-threads worker-opts)
                                  (* 2 u/num-cores)))
            min-threads (long (max 1 (min max-threads (or (:n-min-threads worker-opts)
                                                          (quot max-threads 2)))))
            queue-size (some-> (:queue-size worker-opts) long)
            idle-timeout (int 60000)
            queue (if (and queue-size (pos? queue-size))
                    (LinkedBlockingQueue. (int queue-size))
                    (LinkedBlockingQueue.))
            thread-pool (QueuedThreadPool. (int max-threads)
                                           (int min-threads)
                                           idle-timeout
                                           queue)
            server (jetty/run-jetty handler {:port port :thread-pool thread-pool :join? false})
            worker {:type :queued
                    :n-min-threads (int min-threads)
                    :n-max-threads (int max-threads)
                    :queue-size (when queue-size (int queue-size))}]
        (reset! state_
                {:server server
                 :worker worker
                 :thread-pool thread-pool
                 :port port})
        true)))

  (server-stop [_ timeout-msecs]
    (when-let [{:keys [^org.eclipse.jetty.server.Server server thread-pool]} @state_]
      (.setStopTimeout server timeout-msecs)
      (.stop server)
      (when (instance? LifeCycle thread-pool)
        (.stop ^LifeCycle thread-pool))
      (reset! state_ nil)
      true)))

(defn- new-server [server-id]
  (case server-id
    :ol.busker (ServerH2O. (atom nil))
    :http-kit (ServerHttpKit. (atom nil))
    :jetty (ServerJetty. (atom nil))
    :sunng-jetty (ServerSunngJetty. (atom nil))
    (throw (ex-info "[new-server] unexpected server id"
                    {:server-id server-id
                     :expected #{:ol.busker :http-kit :jetty :sunng-jetty}}))))

;;;; Request handler ----------------------------------------------------------

(defn- rand-msecs ^long [^long min-ms ^long max-ms]
  (+ min-ms (long (* (Math/random) (- max-ms min-ms)))))

(defn hot-work [^long msecs]
  (let [t0 (System/currentTimeMillis)]
    (loop [n 0.0]
      (when (< (- (System/currentTimeMillis) t0) msecs)
        (u/throw-if-aborted)
        (recur (+ n (Math/random)))))))

(defn new-handler [{:keys [resp-len resp-work]}]
  (let [body (if resp-len
               (reduce str (repeatedly resp-len #(rand-int 10)))
               "")
        response {:status 200
                  :body body
                  :headers {"content-type" "text/plain"
                            "content-length" (str (count body))}}
        {:keys [sleep hot]} resp-work]
    (fn [_request]
      (try
        (when-let [[min max] (not-empty sleep)]
          (u/throw-if-aborted)
          (Thread/sleep (int (rand-msecs min max))))
        (when-let [[min max] (not-empty hot)]
          (hot-work (rand-msecs min max)))
        response
        (catch InterruptedException _
          ;; Jetty will interrupt to cancel connections; return nil to close.
          nil)))))

;;;; Lifecycle ---------------------------------------------------------------

(defn with-server [{:keys [handler-fn server-opts worker-opts timeplan]} f]
  (let [{:keys [server-id port]
         :or {server-id :ol.busker
              port (u/rand-free-port)}}
        server-opts
        {:keys [shutdown-msecs sleep-msecs]
         :or {shutdown-msecs 20000
              sleep-msecs 2000}} timeplan
        handler (or handler-fn (new-handler server-opts))
        server (new-server server-id)]
    (u/log "[with-server] starting " (name server-id) " on port " port)
    (server-start server handler port worker-opts)
    (try
      (f server)
      (finally
        (let [t0 (System/currentTimeMillis)]
          (u/log "[with-server] shutting down " (name server-id) " on port " port)
          (server-stop server shutdown-msecs)
          (u/log "[with-server] shutdown finished in " (u/secs-since t0) " seconds")
          (when (pos? sleep-msecs)
            (u/log "[with-server] sleeping " (u/msecs->secs sleep-msecs) " seconds for cooldown")
            (Thread/sleep (int sleep-msecs))))))))

;;;; Bench execution ----------------------------------------------------------

(defn bench-by-spec
  [{:keys [server-opts worker-opts wrk-opts] :as spec}]
  (u/throw-if-aborted)
  (let [wrk-opts-vec (if (vector? wrk-opts) wrk-opts [wrk-opts])
        total (count wrk-opts-vec)
        server-opts (u/or-defaults server-opts {:server-id :ol.busker
                                                :port (u/rand-free-port)})
        worker-opts (u/or-defaults worker-opts {:n-threads u/num-cores
                                                :queue-size 8192})
        wrk-opts-vec
        (mapv
         #(u/or-defaults %
                         {:n-conns (max 1 (u/round0 (* u/num-cores 0.5)))
                          :n-threads (max 1 (u/round0 (* u/num-cores 0.5)))
                          :keep-alive? true
                          :warm-up "5s"
                          :duration "5s"
                          :timeout "2s"})
         wrk-opts-vec)]
    (with-server (assoc spec :server-opts server-opts :worker-opts worker-opts)
      (fn [server]
        (mapv
         (fn [idx wrk-opts]
           (u/log "[bench-by-spec] running wrk opts " (inc idx) "/" total ": " wrk-opts)
           (let [{:keys [worker server-name]} @server
                 wrk-result
                 (try
                   (u/run-wrk (assoc wrk-opts :port (:port server-opts)))
                   (catch Throwable t
                     (u/error! [:bench (:server-id server-opts)] (u/ex->str t))
                     {:error (str t)}))
                 result (assoc spec
                               :server-name server-name
                               :worker worker
                               :wrk-result wrk-result)
                 csv (as-csv-row result)]
             (when-let [m (:measurements wrk-result)]
               (u/log "[bench-by-spec] wrk snippet "
                      {:reqs-per-sec (:reqs-per-sec m)
                       :latency-p98 (str (get-in m [:latency :p98]) " µsecs")
                       :error-rate (-> (get-in m [:errors :rate]) u/format-round4)}))
             (u/append! csv)
             (assoc result :csv csv)))
         (range total) wrk-opts-vec)))))

;;;; Profiles -----------------------------------------------------------------

(def profiles
  (let [nc          u/num-cores
        queue-size  65536
        wrk-threads (max 1 (u/round0 (* nc 0.333)))
        wrk-conns   (cond
                      #_#_(>= nc 16) [128 256]
                      (>= nc 8) [64 128]
                      (>= nc 4) [32 64]
                      :else     [8 16])]
    {:quick
     {:comments    "Quick comparison across servers"
      :server-opts {:server-id [:ol.busker :http-kit :jetty :sunng-jetty]
                    :resp-len  [128]
                    :resp-work [{:sleep [10 70] :hot [0 20]}]}
      :worker-opts {:queue-size [queue-size]
                    :n-threads  [(* nc 2)]}
      :wrk-opts    {:timeout     ["2s"]
                    :n-threads   [wrk-threads]
                    :keep-alive? [true]
                    :n-conns     wrk-conns}}
     :single
     {:comments    "single run"
      :server-opts {:server-id [:ol.busker]
                    :resp-len  [128]
                    :resp-work [{:sleep [10 70] :hot [0 20]}]}
      :worker-opts {:queue-size [queue-size]
                    :n-threads  [(* nc 2)]}
      :wrk-opts    {:timeout     ["2s"]
                    :n-threads   [wrk-threads]
                    :keep-alive? [true]
                    :n-conns     wrk-conns}}}))

(defn bench-by-profile
  [{:keys [metadata system-info port profile runtime dry-run? skip?]
    :or {system-info (u/get-system-info)
         port (u/rand-free-port)
         profile :quick}}]
  (if skip?
    [nil nil]
    (let [{:keys [server-opts worker-opts wrk-opts]} (u/get-profile profiles profile)
          have-vts? (u/have-virtual-threads?)
          nat-idx_ (atom 0)
          specs
          (distinct
           (for [server-id (:server-id server-opts)
                 resp-len (:resp-len server-opts)
                 resp-work (:resp-work server-opts)
                 worker-n (:n-threads worker-opts)
                 queue-size (:queue-size worker-opts)
                 timeout (:timeout wrk-opts)
                 wrk-n (:n-threads wrk-opts)
                 conns (:n-conns wrk-opts)
                 keep-alive? (:keep-alive? wrk-opts)]
             (let [allow-virtual? (and have-vts? (nil? worker-n))]
               {:nat-idx (swap! nat-idx_ inc)
                :metadata metadata
                :system-info system-info
                :server-opts {:server-id server-id
                              :port port
                              :resp-len resp-len
                              :resp-work resp-work}
                :worker-opts {:n-threads worker-n
                              :queue-size queue-size
                              :allow-virtual? allow-virtual?
                              :n-workers worker-n}
                :wrk-opts {:timeout timeout
                           :n-threads wrk-n
                           :n-conns conns
                           :keep-alive? keep-alive?}})))
          n-specs (count specs)
          timeplan (u/get-wrk-timeplan {:n-runs n-specs :runtime (or runtime (str (* n-specs 15) "s"))})
          specs
          (mapv
           (fn [spec]
             (-> spec
                 (assoc :timeplan timeplan)
                 (update :wrk-opts merge {:warm-up (:warm-up-tstr timeplan)
                                          :duration (:duration-tstr timeplan)})))
           specs)]
      (u/with-appender (:instant system-info) "server.csv" (delay (as-csv-row))
        (fn []
          (if dry-run?
            {:dry-run? true :n-specs n-specs :specs specs}
            (do
              (u/log "[bench-by-profile] executing " n-specs " specs (~" (:total-tstr timeplan) ")")
              {:dry-run? false
               :n-specs  n-specs
               :rows
               (u/with-os-tuning
                 (fn []
                   (reduce
                    (fn [acc spec]
                      (u/log "[bench-by-profile] spec " (:nat-idx spec) "/" n-specs ": "
                             (select-keys spec [:server-opts :worker-opts]))
                      (into acc (bench-by-spec spec)))
                    []
                    (shuffle specs))))})))))))
