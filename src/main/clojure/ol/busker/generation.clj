(ns ^:no-doc ol.busker.generation
  "Starts, drains, reloads, and stops one native server generation.

  Key functions create workers and native contexts, coordinate graceful draining, and release
  receivers and arenas after active work ends. Lifecycle allocation and disposal
  run on platform threads; handlers and close callbacks use virtual executors.

  Each worker receives `:fixed-response-slots` fixed-response slots and a separate shared streaming
  arena of 256 * `:output-buffer-size` bytes (8 MiB at the default size).
  Graceful draining leaves admitted responses usable. Receiver admission closes
  before retirement, but entered producers and native sends keep storage valid
  until publication, discard or cleanup. A worker thread still alive at the
  timeout, or a failed worker that left connections, blocks or responses,
  makes retirement report failure and keep the generation rather than free
  live data. A failed worker whose thread has ended and left none of these is
  released by the stopping thread.

  Partial-startup cleanup unregisters receivers before disposing their H2O
  contexts. Dependent resources must remain allocated if safe cleanup cannot be
  established; startup still throws its original error.

  ## Related Namespaces

  - [[ol.busker.runtime]] selects active generations.
  - [[ol.busker.evloop]] runs worker threads."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [babashka.ffi :as mem]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.config :as config]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.evloop :as evloop]
   [ol.busker.worker-context :as worker-context]
   [ol.busker.internal.protocols :as p]
   [ol.busker.listen :as listen]
   [ol.busker.native :as h2o]
   [ol.busker.native.socket :as socket]
   [ol.busker.request :as request]
   [ol.busker.request-dispatch :as request-dispatch]
   [ol.busker.response-queue :as rq]
   [ol.busker.worker-mailbox :as mailbox]
   [ol.busker.tickets :as tickets]
   [ol.busker.wake-endpoint :as wake-endpoint]
   [ol.clave.certificate :as clave-certificate]
   [taoensso.trove :as trove])
  (:import
   [java.io File]
   [java.security.cert CertificateFactory X509Certificate]
   [java.util Base64]
   [java.util.concurrent ExecutorService Executors TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong AtomicLongArray AtomicReference
    AtomicReferenceArray]
   [java.util.concurrent.locks Lock ReentrantReadWriteLock]))

(set! *warn-on-reflection* true)

(defn- new-request-executor ^ExecutorService []
  (Executors/newVirtualThreadPerTaskExecutor))

(defn- new-close-callback-executor ^ExecutorService []
  (Executors/newVirtualThreadPerTaskExecutor))

(defonce ^:private failed-retirements_ (atom []))

(defn- retain-failed-retirement!
  [generation phase-atom error]
  (swap! failed-retirements_ conj generation)
  (reset! phase-atom :retirement-failed)
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

(defn- close-callback-dispatcher
  [^ExecutorService executor]
  (fn [^Runnable task]
    (try
      (.execute executor task)
      (catch java.util.concurrent.RejectedExecutionException _
        (Thread/startVirtualThread task)))))

(defn- wrap-stage-error
  [message stage data t]
  (let [error-data (ex-data t)]
    (throw (ex-info message
                    (merge {:stage (or (:stage error-data) stage)}
                           data
                           error-data)
                    t))))

(defn- with-live-request
  [module-id request-seq f]
  (when-let [worker (worker-context/get-current-worker)]
    (when-let [[req _] (callback-dispatch/entry (:callback-dispatch worker)
                                                module-id request-seq)]
      (f req))))

(defn- retire-wakeup-receiver!
  [worker]
  (when-let [endpoint (:wake-endpoint worker)]
    (wake-endpoint/quiesce-endpoint! endpoint))
  (let [^AtomicReference receiver_ (:wakeup-receiver_ worker)
        receiver (.get receiver_)]
    (when (and receiver (not (mem/null? receiver)))
      (h2o/mt-destroy-wakeup-receiver receiver))
    (.compareAndSet receiver_ receiver nil)))

(defn- retire-current-wakeup-receiver!
  []
  (when-let [worker (worker-context/get-current-worker)]
    (retire-wakeup-receiver! worker)))

(defn evloop-msg-processor
  [op args]
  (case op
    :h2o/retire-wakeup-receiver
    (retire-current-wakeup-receiver!)

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

;; Connection spread. `counts` holds each worker's TCP and Unix socket
;; connections; only that worker writes its slot, from its accept and close
;; callbacks. A worker reads its listeners only while [[at-minimum?]] holds.
;; It decides in every loop turn before it blocks, and an accept that raises the
;; minimum wakes the others, so a worker that became eligible decides again.
;; A worker stopping for the global limit sets `limit-waiting`, which a release
;; clears with a wake. A failed worker's slot is Long/MAX_VALUE, and during
;; shutdown no worker starts reading.

(defn- at-minimum?
  "Returns true when `own` is not above any count in `counts`."
  [^long own ^AtomicLongArray counts]
  (let [n (.length counts)]
    (loop [i 0]
      (cond
        (== i n) true
        (> own (.get counts (int i))) false
        :else (recur (inc i))))))

(defn- wake-other-workers!
  "Wakes every started worker in `workers` except the one at `worker-idx`."
  [^AtomicReferenceArray workers ^long worker-idx]
  (let [n (.length workers)]
    (loop [i 0]
      (when (< i n)
        (when-not (== i worker-idx)
          (when-let [worker (.get workers i)]
            (p/wake worker)))
        (recur (inc i))))))

(defn- log-callback-failure!
  "Logs a failure of a native callback. Never throws, since the callback
  returns to native code."
  [id ^Throwable t]
  (try
    (when (instance? InterruptedException t)
      (.interrupt (Thread/currentThread)))
    (trove/log! {:level :error :id id :error t})
    (catch Throwable _ nil)))

(defn- create-connection-close-callback
  [active-connection-count_ ^AtomicLongArray counts worker-idx]
  (let [worker-idx (long worker-idx)
        cb (fn [_]
             (try
               (.decrementAndGet ^AtomicLong active-connection-count_)
               (.decrementAndGet counts (int worker-idx))
               (h2o/conn-limit-release)
               (catch Throwable t
                 (log-callback-failure! ::connection-close-callback-failed t))))]
    {::connection-close-cb cb
     ::connection-close-cb-ptr (mem/callback (mem/auto-arena) cb [:pointer] :void)
     ::accept-failure-run {:started (AtomicLong. Long/MIN_VALUE)
                           :failed-worker? (AtomicBoolean.)}}))

(def ^:const accept-failure-run-limit-nanos
  "How long a run of failed accepts, from its first failure to the next
  completed accept, may last before the worker is failed."
  1000000000)

(defn- now-nanos
  []
  (System/nanoTime))

(defn- attempt!
  [f]
  (try (f) (catch Throwable _ nil)))

(defn- undo-failed-accept!
  "Gives back what an accept took before it failed at `stage`. From `:closable`
  on, closing the socket runs its close callback, which releases the slot and
  the counts; at `:handed` H2O has the socket and nothing is given back."
  [stage sock-ptr ^AtomicLong active-connection-count_ ^AtomicLongArray counts worker-idx]
  (when (= :counted stage)
    (attempt! #(.decrementAndGet active-connection-count_))
    (attempt! #(.decrementAndGet counts (int worker-idx))))
  (when (#{:limited :counted} stage)
    (attempt! h2o/conn-limit-release))
  (when (#{:accepted :limited :counted :closable} stage)
    (attempt! #(h2o/socket-close sock-ptr))))

(defn- end-accept-failure-run!
  [{:keys [^AtomicLong started ^AtomicBoolean failed-worker?]}]
  (.set started Long/MIN_VALUE)
  (.set failed-worker? false))

(defn- accept-failed!
  "Stops `listener-ptr` for this event-loop turn after a failed accept of the
  worker at `worker-idx`. The first failure of a run starts it and is logged;
  when the run has lasted [[accept-failure-run-limit-nanos]], the worker is
  failed once."
  [^AtomicReferenceArray workers worker-idx
   {:keys [^AtomicLong started ^AtomicBoolean failed-worker?]} listener-ptr ^Throwable t]
  (attempt! #(h2o/socket-read-stop listener-ptr))
  (attempt!
   #(let [now (now-nanos)]
      (if (= Long/MIN_VALUE (.get started))
        (do (.set started now)
            (log-callback-failure! ::accept-callback-failed t))
        (when (and (>= (- now (.get started)) accept-failure-run-limit-nanos)
                   (.compareAndSet failed-worker? false true))
          (let [worker (.get workers (int worker-idx))]
            (log-callback-failure! ::accept-failures-failed-worker t)
            (when-let [fail (:on-native-failure! worker)]
              (fail worker t))))))))

(defn- create-accept-callback
  [accept-ctx-ptr {::keys [connection-close-cb-ptr accept-failure-run]}
   active-connection-count_ ^AtomicLongArray counts workers worker-idx]
  (let [worker-idx (long worker-idx)
        accept-cb (fn [listener-ptr _err-ptr]
                    (let [stage_ (volatile! nil)
                          sock_ (volatile! nil)]
                      (try
                        (if-not (at-minimum? (.get counts (int worker-idx)) counts)
                          (h2o/socket-read-stop listener-ptr)
                          (let [sock-ptr (h2o/evloop-socket-accept listener-ptr)]
                            (when-not (mem/null? sock-ptr)
                              (vreset! sock_ sock-ptr)
                              (vreset! stage_ :accepted)
                              (if (pos? (h2o/conn-limit-try-acquire))
                                (do
                                  (vreset! stage_ :limited)
                                  (.incrementAndGet ^AtomicLong active-connection-count_)
                                  (let [own (.incrementAndGet counts (int worker-idx))]
                                    (vreset! stage_ :counted)
                                    (h2o/socket-set-on-close sock-ptr connection-close-cb-ptr mem/null)
                                    (vreset! stage_ :closable)
                                    (h2o/h2o-accept accept-ctx-ptr sock-ptr)
                                    (vreset! stage_ :handed)
                                    (end-accept-failure-run! accept-failure-run)
                                    (when (at-minimum? own counts)
                                      (wake-other-workers! workers worker-idx))))
                                (do
                                  (vreset! stage_ nil)
                                  (h2o/socket-close sock-ptr))))))
                        (catch Throwable t
                          (undo-failed-accept! @stage_ @sock_ active-connection-count_
                                               counts worker-idx)
                          (accept-failed! workers worker-idx accept-failure-run listener-ptr t)))))]
    {::accept-cb accept-cb
     ::accept-cb-ptr (mem/callback (mem/auto-arena) accept-cb [:pointer :pointer] :void)}))

(defn- config->flat-globalconf-t
  [config]
  (let [has? (fn [k] (if (contains? config k) 1 0))
        num-val (fn [k] (get config k 0))
        bool->int (fn [k] (if (get config k) 1 0))]
    {:has_server_name (has? :server-name)
     :server_name (get config :server-name "")
     :has_proxy_status_identity (has? :proxy-status-identity)
     :proxy_status_identity (get config :proxy-status-identity "")
     :has_max_request_entity_size (has? :max-request-entity-size)
     :max_request_entity_size (num-val :max-request-entity-size)
     :has_max_delegations (has? :max-delegations)
     :max_delegations (num-val :max-delegations)
     :has_max_reprocesses (has? :max-reprocesses)
     :max_reprocesses (num-val :max-reprocesses)
     :has_handshake_timeout (has? :handshake-timeout)
     :handshake_timeout (num-val :handshake-timeout)
     :has_max_spare_pipes (has? :max-spare-pipes)
     :max_spare_pipes (num-val :max-spare-pipes)
     :has_http1__req_timeout (has? :http1-req-timeout)
     :http1__req_timeout (num-val :http1-req-timeout)
     :has_http1__req_io_timeout (has? :http1-req-io-timeout)
     :http1__req_io_timeout (num-val :http1-req-io-timeout)
     :has_http1__upgrade_to_http2 (has? :http1-upgrade?)
     :http1__upgrade_to_http2 (bool->int :http1-upgrade?)
     :has_http1__req_io_timeout_while_generating (has? :http1-req-io-timeout-while-generating?)
     :http1__req_io_timeout_while_generating (bool->int :http1-req-io-timeout-while-generating?)
     :has_http2__idle_timeout (has? :http2-idle-timeout)
     :http2__idle_timeout (num-val :http2-idle-timeout)
     :has_http2__graceful_shutdown_timeout (has? :http2-graceful-shutdown-timeout)
     :http2__graceful_shutdown_timeout (num-val :http2-graceful-shutdown-timeout)
     :has_http2__max_streams (has? :http2-max-streams)
     :http2__max_streams (num-val :http2-max-streams)
     :has_http2__max_concurrent_requests_per_connection (has? :http2-max-requests)
     :http2__max_concurrent_requests_per_connection (num-val :http2-max-requests)
     :has_http2__max_concurrent_streaming_requests_per_connection (has? :http2-max-streaming-requests)
     :http2__max_concurrent_streaming_requests_per_connection (num-val :http2-max-streaming-requests)
     :has_http2__max_streams_for_priority (has? :http2-max-priority-streams)
     :http2__max_streams_for_priority (num-val :http2-max-priority-streams)
     :has_http2__active_stream_window_size (has? :http2-stream-window-size)
     :http2__active_stream_window_size (num-val :http2-stream-window-size)
     :has_http2__dos_delay (has? :http2-dos-delay)
     :http2__dos_delay (num-val :http2-dos-delay)
     :has_http3__idle_timeout (has? :http3-idle-timeout)
     :http3__idle_timeout (num-val :http3-idle-timeout)
     :has_http3__graceful_shutdown_timeout (has? :http3-graceful-shutdown-timeout)
     :http3__graceful_shutdown_timeout (num-val :http3-graceful-shutdown-timeout)
     :has_http3__active_stream_window_size (has? :http3-stream-window-size)
     :http3__active_stream_window_size (num-val :http3-stream-window-size)
     :has_http3__ack_frequency (has? :http3-ack-frequency)
     :http3__ack_frequency (num-val :http3-ack-frequency)
     :has_compress_args (if (:compress? config false) 1 0)
     :compress_args_mine_size (num-val :compress-min-size)
     :compress_args_gzip_quality (num-val :compress-gzip-level)
     :compress_args_brotli_quality (num-val :compress-brotli-level)
     :compress_args_zstd_quality (num-val :compress-zstd-level)}))

(defn- create-entrypoint-hosts
  "Allocates the native hosts record of the entrypoint at `index` and returns the
  hosts array for the accept contexts of its listeners. Connections keep that
  pointer, and the request callback reads the entrypoint index back from it."
  [arena config-ptr index]
  (let [record (mem/alloc arena (h2o/entrypoint-hosts-size))
        hosts (h2o/entrypoint-hosts-init record config-ptr (int index))]
    (when (or (nil? hosts) (mem/null? hosts))
      (throw (ex-info "Could not create the native hosts array of an entrypoint"
                      {:index index})))
    hosts))

(defn- create-server-config
  [arena executor close-callback-executor ring-handler config]
  (let [config-ptr (mem/alloc arena (h2o/globalconf-size))
        flat-config-ptr (mem/alloc arena h2o/ffi-clj-h2o-flat-globalconf-t)
        flat-config (-> (config->flat-globalconf-t config)
                        (update :server_name #(mem/string->ptr arena %))
                        (update :proxy_status_identity #(mem/string->ptr arena %)))
        _ (mem/write flat-config-ptr h2o/ffi-clj-h2o-flat-globalconf-t flat-config)
        config-ptr (do
                     (h2o/create-global-conf config-ptr flat-config-ptr)
                     config-ptr)
        hostconf-ptr (with-open [arena2 (mem/confined-arena)]
                       (h2o/config-register-host config-ptr
                                                 (h2o/str->iovec "default" arena2)
                                                 65535))
        entrypoint-hosts (into {}
                               (map-indexed (fn [index id]
                                              [id (create-entrypoint-hosts
                                                   arena config-ptr index)]))
                               (config/entrypoint-ids config))
        close-callback-dispatch (close-callback-dispatcher close-callback-executor)
        on-request-cb (fn [req-ctx-ptr]
                        (try
                          (request/on-request executor
                                              close-callback-dispatch
                                              config
                                              ring-handler
                                              req-ctx-ptr)
                          (catch Throwable t
                            (request/request-callback-failed! ::request/request-callback-failed t)
                            h2o/CLJ_HANDLER_OVERLOADED)))
        on-request-cleanup-cb request/on-request-cleanup
        handler (h2o/create-handler hostconf-ptr
                                    on-request-cb
                                    on-request-cleanup-cb
                                    flat-config-ptr
                                    arena)]
    {::config-ptr config-ptr
     ::hostconf-ptr hostconf-ptr
     ::entrypoint-hosts entrypoint-hosts
     ::on-request-cb on-request-cb
     ::on-request-cleanup-cb on-request-cleanup-cb
     ::handler handler}))

(defn- update-listener-state!
  "Starts or stops reading this worker's listener sockets by the global
  connection limit and the connection spread rule."
  [{:keys [listener-socks accept-callbacks max-connections worker-connection-counts
           worker-idx accepting-workers limit-waiting]}]
  (let [^AtomicLongArray counts worker-connection-counts
        ^AtomicBoolean limit-waiting limit-waiting
        worker-idx (long worker-idx)
        limit-allows? (or (zero? max-connections)
                          (< (h2o/conn-limit-current) max-connections)
                          (do (.set limit-waiting true)
                              (< (h2o/conn-limit-current) max-connections)))
        _ (when (and limit-allows? (.get limit-waiting) (.compareAndSet limit-waiting true false))
            (wake-other-workers! accepting-workers worker-idx))
        should-accept? (and limit-allows?
                            (at-minimum? (.get counts (int worker-idx)) counts))]
    (doseq [listener-idx (range (count listener-socks))]
      (let [sock-ptr (nth listener-socks listener-idx)
            accept-callback (nth accept-callbacks listener-idx)]
        (when-not (mem/null? sock-ptr)
          (if should-accept?
            (when (zero? (h2o/socket-reading? sock-ptr))
              (h2o/socket-read-start sock-ptr accept-callback))
            (when-not (zero? (h2o/socket-reading? sock-ptr))
              (h2o/socket-read-stop sock-ptr))))))))

(defn- initiate-worker-shutdown!
  [{:keys [listener-socks loop-ptr ctx-ptr http3-ctxs] ::keys [listeners-closed?_]}]
  (doseq [listener-idx (range (count listener-socks))]
    (let [sock-ptr (nth listener-socks listener-idx)]
      (when (and (not (mem/null? sock-ptr))
                 (not (zero? (h2o/socket-reading? sock-ptr))))
        (h2o/socket-read-stop sock-ptr))))
  (doseq [http3-ctx http3-ctxs]
    (when (and http3-ctx (not (mem/null? http3-ctx)))
      (h2o/http3-stop-accepting http3-ctx)))
  (h2o/evloop-run loop-ptr 0)
  (.set ^AtomicBoolean listeners-closed?_ true)
  (doseq [listener-idx (range (count listener-socks))]
    (let [sock-ptr (nth listener-socks listener-idx)]
      (when-not (mem/null? sock-ptr)
        (h2o/socket-close sock-ptr))))
  (h2o/context-request-shutdown ctx-ptr))

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

(defn- close-response-receiver-admission!
  [worker]
  (when-let [^AtomicBoolean receiver-open?_ (:response-receiver-open?_ worker)]
    (.set receiver-open?_ false))
  (fixed-final/notify-all-capacity! worker))

(defn- worker-native-failure! [worker cause]
  (when (and (.compareAndSet ^AtomicReference (:native-failure_ worker) nil cause)
             (not (identical? cause (.get ^AtomicReference (:termination-cause_ worker)))))
    (log-callback-failure! ::worker-failed cause))
  (.set ^AtomicBoolean (:accepting?_ worker) false)
  (.set ^AtomicBoolean (:running?_ worker) false)
  (mailbox/close! (:mailbox worker))
  (close-response-receiver-admission! worker)
  (callback-dispatch/fail-responses! (:callback-dispatch worker) cause))

(defn- destroy-response-receiver!
  [worker]
  (close-response-receiver-admission! worker)
  (let [^AtomicReference receiver_ (:response-receiver_ worker)
        ^ReentrantReadWriteLock receiver-lock (:response-receiver-lock worker)
        ^Lock write-lock (when receiver-lock (.writeLock receiver-lock))]
    (if (or (nil? receiver_) (nil? write-lock) (not (.tryLock write-lock)))
      false
      (try
        (let [receiver (.get receiver_)]
          (if (or (nil? receiver) (mem/null? receiver))
            true
            (if (zero? (h2o/mt-response-pending receiver))
              (do
                (h2o/mt-destroy-response-receiver receiver)
                (.set receiver_ nil)
                true)
              false)))
        (finally
          (.unlock write-lock))))))

(defn- ready-to-dispose?
  [{:keys [context-disposed? shutdown-initiated?]}
   {:keys [ctx-ptr http3-ctxs worker-connection-counts worker-idx]} worker]
  (and (not context-disposed?)
       shutdown-initiated?
       (all-connections-drained? ctx-ptr http3-ctxs worker-connection-counts worker-idx)
       (response-exchange-drained? worker)
       (not (mailbox/pending? (:mailbox worker)))
       (zero? (bp/checked-out (:stream-pool worker)))
       (callback-dispatch/no-live-entries? (:callback-dispatch worker))))

(defn- count-stopped-accepting!
  "Counts the worker of `worker-state` as stopped accepting, at most once, and
  delivers the generation's stopped-accepting promise for the last worker."
  [{::keys [^AtomicBoolean stopped-accepting?_ ^AtomicLong stop-accepting-remaining_
            stopped-accepting_]}]
  (when (and (.compareAndSet stopped-accepting?_ false true)
             (zero? (.decrementAndGet stop-accepting-remaining_)))
    (deliver stopped-accepting_ true)))

(defn- check-and-initiate-shutdown!
  [loop-state {:keys [shutting-down?] :as state}]
  (let [should-shutdown? (.get ^AtomicBoolean shutting-down?)
        already? (true? (:shutdown-initiated? loop-state))]
    (when (and should-shutdown? (not already?))
      (initiate-worker-shutdown! (assoc state :shutdown-initiated? true))
      (count-stopped-accepting! state))
    (assoc loop-state :shutdown-initiated? (or already? should-shutdown?))))

(defn- update-receiver-destruction
  [loop-state worker]
  (cond
    (:receiver-destroyed? loop-state)
    loop-state

    (nil? (.get ^AtomicReference (:wakeup-receiver_ worker)))
    (assoc loop-state :receiver-destroyed? true)

    :else
    loop-state))

(defn- http3-ctxs-guard
  "The lock and flag that keep [[withdraw-http3-worker!]] from reaching a
  worker's HTTP/3 contexts after [[dispose-context-if-ready]] freed them."
  []
  {:lock (Object.) :disposed? (AtomicBoolean. false)})

(defn- withdraw-http3-worker!
  "Stops other workers from sending new HTTP/3 connections to this worker,
  unless its contexts are already freed. Safe from any thread."
  [{:keys [lock ^AtomicBoolean disposed?]} http3-ctxs]
  (locking lock
    (when-not (.get disposed?)
      (doseq [http3-ctx http3-ctxs]
        (when (and http3-ctx (not (mem/null? http3-ctx)))
          (h2o/http3-withdraw-worker http3-ctx))))))

(defn- dispose-context-if-ready
  [loop-state worker {:keys [ctx-ptr http3-ctxs http3-ctxs-guard] :as state}]
  (if (and (ready-to-dispose? loop-state state worker)
           (destroy-response-receiver! worker)
           (bp/retire! (:stream-pool worker)))
    (do
      (retire-current-wakeup-receiver!)
      (let [{:keys [lock ^AtomicBoolean disposed?]} http3-ctxs-guard]
        (locking lock
          (.set disposed? true)
          (doseq [http3-ctx http3-ctxs]
            (when (and http3-ctx (not (mem/null? http3-ctx)))
              (h2o/http3-free-worker-ctx http3-ctx)))))
      (h2o/context-dispose ctx-ptr)
      (case (p/send-msg worker evloop/stop-msg)
        :accepted nil
        :closed nil
        :overloaded (throw (IllegalStateException. "Stop request cannot overload")))
      (assoc loop-state :context-disposed? true))
    loop-state))

(defn- release-failed-worker!
  "Releases, on the stopping thread, what the loop of `worker` releases when it
  stops normally. Call it only after the thread of the failed worker has ended.
  It always closes the worker's listener duplicates. The loop no longer runs,
  so nothing can close what it still holds. With `sockets-left?`, connections,
  responses or blocks left, or a stream pool that cannot retire, it releases
  nothing else and returns false. Returns true once the state is released,
  also when the worker released it itself before it failed."
  [worker {:keys [listener-socks ctx-ptr http3-ctxs http3-ctxs-guard]
           ::keys [^AtomicBoolean listeners-closed?_]}
   sockets-left?]
  (let [{:keys [lock ^AtomicBoolean disposed?]} http3-ctxs-guard]
    (locking lock
      (when (.compareAndSet listeners-closed?_ false true)
        (doseq [sock-ptr listener-socks]
          (when-not (mem/null? sock-ptr)
            (h2o/socket-close sock-ptr))))
      (cond
        (.get disposed?)
        true

        (or sockets-left?
            (pos? (bp/checked-out (:stream-pool worker)))
            (pos? (h2o/context-get-active-conns ctx-ptr))
            (pos? (h2o/context-get-shutdown-conns ctx-ptr))
            (some has-active-http3-connections? http3-ctxs)
            (not (destroy-response-receiver! worker))
            (not (bp/retire! (:stream-pool worker))))
        false

        :else
        (do
          (.set disposed? true)
          (doseq [http3-ctx http3-ctxs]
            (when (and http3-ctx (not (mem/null? http3-ctx)))
              (h2o/http3-free-worker-ctx http3-ctx)))
          (retire-wakeup-receiver! worker)
          (h2o/context-dispose ctx-ptr)
          true)))))

(defn- retire-worker!
  "Returns `:alive` while the thread of `worker` runs, `:released` when its
  native state is released and `:unfinished` when it ended with native work
  left."
  [worker worker-state sockets-left?]
  (cond
    (.isAlive ^Thread (:thread worker))
    :alive

    (.get ^AtomicReference (:native-failure_ worker))
    (if (release-failed-worker! worker worker-state sockets-left?) :released :unfinished)

    (or (mailbox/pending? (:mailbox worker))
        (not (response-exchange-drained? worker))
        (pos? (bp/checked-out (:stream-pool worker))))
    :unfinished

    :else
    :released))

(defn- worker-loop
  [worker loop-state
   {:keys [loop-ptr ctx-ptr] :as state}]
  (when-let [failure (.get ^AtomicReference (:native-failure_ worker))]
    (throw failure))
  (let [mailbox-work? (true? (::evloop/mailbox-work? loop-state))
        loop-state (-> (dissoc loop-state ::evloop/mailbox-work?)
                       (check-and-initiate-shutdown! state)
                       (update-receiver-destruction worker)
                       (dispose-context-if-ready worker state))
        disposed? (true? (:context-disposed? loop-state))
        receiver_ (:response-receiver_ worker)
        receiver (when receiver_ (.get ^AtomicReference receiver_))
        ring-work? (and (not disposed?)
                        receiver
                        (not (mem/null? receiver))
                        (pos? (h2o/mt-response-ring-drain receiver)))
        sent? (when-not disposed?
                (when ring-work? (fixed-final/notify-capacity! worker))
                (callback-dispatch/drain-responses! (:callback-dispatch worker)))
        shutting? (true? (:shutdown-initiated? loop-state))
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
          (update-listener-state! state))
        (h2o/evloop-run loop-ptr (cond
                                   (pos? (p/count-msgs worker)) 0
                                   tasks-queued? (min 1 wait-ms)
                                   :else wait-ms))))
    loop-state))

(defn http3-enabled?
  [listener]
  (boolean
   (when-let [tls (:tls listener)]
     (and (map? tls)
          (not= false (:http3? tls))))))

(defn- x509-certificate->pem
  [^X509Certificate cert]
  (let [line-separator (byte-array [(byte 10)])
        encoder (Base64/getMimeEncoder 64 line-separator)
        cert-b64 (.encodeToString encoder (.getEncoded cert))]
    (str "-----BEGIN CERTIFICATE-----\n"
         cert-b64
         "\n-----END CERTIFICATE-----\n")))

(defn- certificate->pem
  [certificate]
  (cond
    (string? certificate)
    certificate

    (instance? X509Certificate certificate)
    (x509-certificate->pem certificate)

    :else
    (throw (ex-info "Unsupported certificate value in TLS lookup bundle."
                    {:certificate-type (some-> certificate class .getName)}))))

(defn- private-key->pem
  [private-key]
  (cond
    (nil? private-key)
    nil

    (string? private-key)
    private-key

    :else
    (clave-certificate/private-key->pem private-key)))

(defn- clave-bundle->tls-material
  [bundle]
  (let [certificates (:certificate bundle)
        cert-chain (cond
                     (nil? certificates) nil
                     (sequential? certificates) certificates
                     :else [certificates])
        private-key-pem (private-key->pem (:private-key bundle))]
    (when (and (seq cert-chain) private-key-pem)
      {:cert-chain-pem (apply str (map certificate->pem cert-chain))
       :private-key-pem private-key-pem})))

(defn- read-certificates
  [cert-file]
  ;; Parse from in-memory bytes so certificate loading does not depend on
  ;; FileInputStream close semantics during startup or reload.
  (with-open [in (java.io.ByteArrayInputStream.
                  (java.nio.file.Files/readAllBytes
                   (.toPath (io/file cert-file))))]
    (vec (.generateCertificates (CertificateFactory/getInstance "X.509") in))))

(defn- certificate-subject-names
  [^X509Certificate certificate]
  (let [sans (or (.getSubjectAlternativeNames certificate) [])
        san-names (->> sans
                       (keep (fn [entry]
                               (let [entry-type (.get ^java.util.List entry 0)
                                     value (.get ^java.util.List entry 1)]
                                 (when (or (= entry-type 2)
                                           (= entry-type 7))
                                   (str value)))))
                       distinct
                       vec)]
    (if (seq san-names)
      san-names
      (when-let [[_ cn] (re-find #"CN=([^,]+)"
                                 (.getName (.getSubjectX500Principal certificate)))]
        [cn]))))

(defn- wildcard-subject-name?
  [subject-name]
  (and (string? subject-name)
       (str/starts-with? subject-name "*.")))

(defn- subject-name-matches-hostname?
  [subject-name hostname]
  (cond
    (or (nil? subject-name) (nil? hostname))
    false

    (= subject-name hostname)
    true

    (wildcard-subject-name? subject-name)
    (let [suffix (subs subject-name 1)
          hostname-parts (str/split hostname #"\.")
          suffix-parts (str/split (subs subject-name 2) #"\.")]
      (and (str/ends-with? hostname suffix)
           (= (count hostname-parts)
              (inc (count suffix-parts)))))

    :else
    false))

(defn- tls-material-from-files
  [cert-file key-file]
  (let [certificates (read-certificates cert-file)]
    {:names (or (some-> certificates first certificate-subject-names)
                [])
     :material {:cert-chain-pem (slurp cert-file)
                :private-key-pem (slurp key-file)}}))

(defn- directory-pem-pair
  [^File dir]
  (let [cert-pem (io/file dir "cert.pem")
        key-pem (io/file dir "key.pem")
        fullchain-pem (io/file dir "fullchain.pem")
        privkey-pem (io/file dir "privkey.pem")]
    (cond
      (and (.isFile cert-pem) (.isFile key-pem))
      {:cert-file (.getPath cert-pem)
       :key-file (.getPath key-pem)}

      (and (.isFile fullchain-pem) (.isFile privkey-pem))
      {:cert-file (.getPath fullchain-pem)
       :key-file (.getPath privkey-pem)}

      :else
      nil)))

(defn- load-folder-tls-materials
  [path]
  (->> (file-seq (io/file path))
       (filter #(.isDirectory ^File %))
       (keep directory-pem-pair)
       (mapv (fn [{:keys [cert-file key-file]}]
               (tls-material-from-files cert-file key-file)))))

(defn- load-static-tls-materials
  [config]
  (->> (get-in config [:tls :certificates :load])
       (mapcat (fn [{:keys [type path cert-file key-file]}]
                 (case type
                   :folder (load-folder-tls-materials path)
                   :pem [(tls-material-from-files cert-file key-file)]
                   [])))
       vec))

(defn- first-static-tls-identity
  [config]
  (some (fn [{:keys [type path cert-file key-file]}]
          (case type
            :pem
            (when (and cert-file key-file)
              {:cert-file cert-file
               :key-file key-file})

            :folder
            (some->> (file-seq (io/file path))
                     (filter #(.isDirectory ^File %))
                     (keep directory-pem-pair)
                     first)

            nil))
        (get-in config [:tls :certificates :load])))

(defn- tls-callback-required?
  [config]
  (boolean
   (some (fn [[_ entrypoint]]
           (map? (:tls entrypoint)))
         (:entrypoints config))))

(defn- build-tls-lookup-fn
  [config cert-runtime]
  (let [static-materials (load-static-tls-materials config)
        fallback-material (some-> static-materials first :material)
        fallback-subject-name (first (:subject-names cert-runtime))
        cert-lookup-fn
        (or (:lookup-fn cert-runtime)
            (let [lookup-certificate clave-adapter/lookup-certificate]
              (fn [hostname]
                (lookup-certificate cert-runtime hostname))))]
    (fn [hostname]
      (let [sni-hostname (when (and (string? hostname)
                                    (not (.isEmpty ^String hostname)))
                           hostname)
            static-match (when sni-hostname
                           (some (fn [{:keys [names material]}]
                                   (when (some #(subject-name-matches-hostname? % sni-hostname)
                                               names)
                                     material))
                                 static-materials))]
        (cond
          static-match
          static-match

          sni-hostname
          (some-> (cert-lookup-fn sni-hostname)
                  clave-bundle->tls-material)

          fallback-material
          fallback-material

          fallback-subject-name
          (some-> (cert-lookup-fn fallback-subject-name)
                  clave-bundle->tls-material)

          :else
          (do
            (trove/log! {:level :trace
                         :id ::tls-lookup-no-sni-miss
                         :data {}})
            nil))))))

(defn- init-tls-lookup-state
  [{::keys [config cert-runtime] :as state}]
  (try
    (if (tls-callback-required? config)
      (let [lookup-fn (build-tls-lookup-fn config cert-runtime)
            callback-refs (h2o/build-tls-lookup-callback lookup-fn)]
        (assoc state
               ::tls-lookup-fn lookup-fn
               ::tls-lookup-callback callback-refs))
      state)
    (catch Throwable t
      (wrap-stage-error "Failed to initialize TLS lookup state"
                        :tls-startup
                        {}
                        t))))

(defn- create-ssl-context
  [listener tls-lookup-callback-ptr]
  (try
    (when-let [tls (:tls listener)]
      (when (map? tls)
        (let [ssl-ctx (h2o/create-ssl-ctx nil nil
                                          1
                                          (or tls-lookup-callback-ptr mem/null)
                                          mem/null)]
          (when (mem/null? ssl-ctx)
            (throw (ex-info "Failed to create SSL_CTX (check OpenSSL errors in stderr)"
                            {:listener (dissoc listener :tls)
                             :listener-tls (:tls listener)})))
          ssl-ctx)))
    (catch Throwable t
      (wrap-stage-error "Failed to create TLS listener context"
                        :tls-startup
                        {:listener (dissoc listener :tls)}
                        t))))

(defn- create-http3-contexts
  [listeners config config-ptr ticket-mgr-ptr ticket-lifetime-seconds tls-lookup-callback-ptr
   cleanup-state_]
  (let [{:keys [cert-file key-file]} (first-static-tls-identity config)]
    (try
      (into {}
            (keep-indexed
             (fn [idx listener]
               (when (http3-enabled? listener)
                 (let [ptls-ctx (h2o/http3-create-ptls-ctx cert-file
                                                           key-file
                                                           (or tls-lookup-callback-ptr mem/null)
                                                           mem/null)]
                   (when (or (nil? ptls-ctx) (mem/null? ptls-ctx))
                     (throw (ex-info "Failed to create ptls context for HTTP/3"
                                     {:listener-index idx
                                      :cert-file cert-file
                                      :key-file key-file})))
                   (let [encrypt-ticket
                         (when (and ticket-mgr-ptr (not (mem/null? ticket-mgr-ptr)))
                           (h2o/ticket-manager-create-encrypt-ticket ticket-mgr-ptr 1))
                         _ (when (and encrypt-ticket
                                      (not (zero? (h2o/ptls-ctx-set-tickets ptls-ctx
                                                                            encrypt-ticket
                                                                            ticket-lifetime-seconds
                                                                            8192))))
                             (h2o/http3-free-ptls-ctx ptls-ctx)
                             (throw (ex-info "Failed to set session tickets for HTTP/3"
                                             {:listener-index idx})))
                         quicly-ctx (h2o/http3-create-quicly-ctx ptls-ctx config-ptr)]
                     (when (or (nil? quicly-ctx) (mem/null? quicly-ctx))
                       (h2o/http3-free-ptls-ctx ptls-ctx)
                       (throw (ex-info "Failed to create quicly context for HTTP/3"
                                       {:listener-index idx})))
                     (when (and ticket-mgr-ptr (not (mem/null? ticket-mgr-ptr)))
                       (h2o/ticket-manager-set-quic-tag ticket-mgr-ptr quicly-ctx))
                     (let [entry {:ptls-ctx ptls-ctx
                                  :quicly-ctx quicly-ctx
                                  :encrypt-ticket encrypt-ticket}]
                       (vswap! cleanup-state_ assoc-in [::http3-contexts idx] entry)
                       [idx entry])))))
             listeners))
      (catch Throwable t
        (wrap-stage-error "Failed to initialize HTTP/3 TLS state"
                          :tls-startup
                          {}
                          t)))))

(defn- create-http3-worker-contexts
  [generation-id n-workers listeners loops contexts entrypoint-hosts http3-contexts listener-claims
   ticket-mgr-ptr cleanup-state_]
  (vswap! cleanup-state_ assoc ::http3-worker-contexts (vec (repeat n-workers [])))
  (vec
   (for [thread-idx (range n-workers)]
     (vec
      (for [listener-idx (range (count listeners))]
        (let [listener (nth listeners listener-idx)]
          (when (http3-enabled? listener)
            (let [{:keys [quicly-ctx]} (get http3-contexts listener-idx)
                  listener-claim (or (get listener-claims
                                          (listen/listener-key
                                           (assoc listener :transport :udp)))
                                     (throw (ex-info "Missing UDP listener claim"
                                                     {:listener listener})))
                  loop-ptr (nth loops thread-idx)
                  ctx-ptr (nth contexts thread-idx)
                  http3-ctx (h2o/http3-attach-udp-transport
                             ctx-ptr
                             loop-ptr
                             quicly-ctx
                             (get entrypoint-hosts (:entrypoint listener))
                             (listen/resource listener-claim)
                             (or ticket-mgr-ptr mem/null)
                             generation-id
                             (int thread-idx))]
              (when (or (nil? http3-ctx) (mem/null? http3-ctx))
                (throw (ex-info "Failed to create HTTP/3 worker context"
                                {:listener-index listener-idx
                                 :thread-idx thread-idx
                                 :port (:port listener)})))
              (vswap! cleanup-state_ update-in [::http3-worker-contexts thread-idx] conj http3-ctx)
              http3-ctx))))))))

(defn- free-http3-contexts
  [http3-contexts]
  (doseq [[_ {:keys [quicly-ctx ptls-ctx]}] http3-contexts]
    (when quicly-ctx
      (h2o/http3-free-quicly-ctx quicly-ctx))
    (when ptls-ctx
      (h2o/http3-free-ptls-ctx ptls-ctx))))

(defn- claim-keys
  [listeners]
  (->> listeners
       (mapcat (fn [listener]
                 (cond-> [(listen/listener-key listener)]
                   (http3-enabled? listener)
                   (conj (listen/listener-key
                          (assoc listener :transport :udp))))))
       distinct
       vec))

(defn- http3-transport-resources
  [claims]
  (->> claims
       (filter (fn [[claim-key _]]
                 (= :udp (:transport claim-key))))
       (map (fn [[_ claim]] (listen/resource claim)))
       distinct
       vec))

(defn- release-listener-claims!
  [claims]
  (doseq [claim (vals claims)]
    (listen/release! claim))
  nil)

(defn- release-socket-listener-claims!
  "Releases the TCP and Unix socket claims among `claims`, and keeps the UDP
  claims that HTTP/3 worker contexts still reference. Each failed release is
  added to `error` as a suppressed exception, and the other claims are still
  released."
  [claims ^Throwable error]
  (doseq [[claim-key claim] claims
          :when (not= :udp (:transport claim-key))]
    (try
      (listen/release! claim)
      (catch Throwable t
        (.addSuppressed error t)))))

(defn- close-kept-http3-sockets!
  "Closes the HTTP/3 UDP sockets of each worker of `generation` whose HTTP/3
  worker contexts are kept, once, so that the UDP ports can be bound again and
  get no datagrams for those sockets. Call it only after every worker thread
  has ended. The contexts and the UDP claims stay, because kept connections
  still point at the contexts and the contexts at their transport. Each socket
  that stays open is added to `error` as a suppressed exception."
  [generation ^Throwable error]
  (doseq [{:keys [http3-ctxs http3-ctxs-guard worker-idx]
           ::keys [^AtomicBoolean http3-sockets-closed?_]} (::worker-states generation)]
    (let [{:keys [lock ^AtomicBoolean disposed?]} http3-ctxs-guard]
      (locking lock
        (when (and (not (.get disposed?))
                   (.compareAndSet http3-sockets-closed?_ false true))
          (doseq [http3-ctx http3-ctxs
                  :when (and http3-ctx (not (mem/null? http3-ctx)))]
            (when-not (zero? (h2o/http3-close-worker-sockets http3-ctx))
              (.addSuppressed error (ex-info "HTTP/3 UDP socket of a kept worker stays open"
                                             {:worker-idx worker-idx})))))))))

(defn- acquire-listener-claims!
  [pool listeners]
  (reduce (fn [claims claim-key]
            (try
              (assoc claims claim-key
                     (listen/acquire-claim pool claim-key))
              (catch Throwable t
                (release-listener-claims! claims)
                (wrap-stage-error "Failed to acquire listener claims"
                                  :listener-acquisition
                                  {:listener-key claim-key}
                                  t))))
          {}
          (claim-keys listeners)))

(defn- prepare-generation-state
  [compiled-config cert-runtime listener-pool listener-claims generation-id
   memory-ticket-service]
  (let [{:keys [n-workers max-connections]} compiled-config
        ring-handler (-> compiled-config
                         config/dispatch-handler
                         (clave-adapter/wrap-handler cert-runtime))]
    {::ring-handler ring-handler
     ::generation-id generation-id
     ::config compiled-config
     ::cert-runtime cert-runtime
     ::memory-ticket-service memory-ticket-service
     ::listener-pool listener-pool
     ::listener-claims listener-claims
     ::phase (atom :running)
     ::stop-lock (Object.)
     ::message-handler evloop-msg-processor
     ::shutting-down? (AtomicBoolean. false)
     ::active-connection-count_ (AtomicLong. 0)
     ::worker-connection-counts (AtomicLongArray. (int n-workers))
     ::accepting-workers (AtomicReferenceArray. (int n-workers))
     ::limit-waiting (AtomicBoolean. false)
     ::stop-accepting-remaining_ (AtomicLong. n-workers)
     ::stopped-accepting_ (promise)
     ::n-workers n-workers
     ::max-connections max-connections}))

(defn- attempt-core-unwind!
  [^Throwable failure f]
  (try
    (f)
    (catch Throwable error
      (.addSuppressed failure error))))

(defn- unwind-core-state!
  [^Throwable failure arena config-ptr loops contexts wakeup-receivers response-receivers]
  (doseq [receiver (rseq response-receivers)]
    (attempt-core-unwind! failure
                          #(when (and receiver (not (mem/null? receiver)))
                             (h2o/mt-destroy-response-receiver receiver))))
  (doseq [receiver (rseq wakeup-receivers)]
    (attempt-core-unwind! failure
                          #(when (and receiver (not (mem/null? receiver)))
                             (h2o/mt-destroy-wakeup-receiver receiver))))
  (doseq [context (rseq contexts)]
    (attempt-core-unwind! failure #(h2o/context-dispose context)))
  (doseq [loop (rseq loops)]
    (attempt-core-unwind! failure #(h2o/evloop-destroy loop)))
  (attempt-core-unwind! failure #(h2o/config-dispose config-ptr))
  (attempt-core-unwind! failure #(.close ^java.lang.AutoCloseable arena)))

(defn- init-core-state
  [{::keys [ring-handler config executor close-callback-executor n-workers max-connections
            active-connection-count_ worker-connection-counts]
    :as state}]
  (let [arena (mem/shared-arena)
        _ (h2o/conn-limit-set-max max-connections)
        server-config (create-server-config arena executor close-callback-executor ring-handler config)
        config-ptr (::config-ptr server-config)
        loops (h2o/create-loops n-workers)
        contexts (h2o/create-contexts arena loops config-ptr)
        wakeup-receivers (mapv h2o/mt-create-wakeup-receiver contexts)
        slot-count (long (:fixed-response-slots config))
        fixed-body-limit (min (long (:output-buffer-size config))
                              (long (:fixed-response-max-body-size config)))
        response-slot-payload-capacity (+ fixed-final/max-header-staging-bytes fixed-body-limit)
        ;; MemorySegment.setString writes a trailing NUL outside the logical payload.
        response-slot-storage-capacity (inc response-slot-payload-capacity)
        response-receivers
        (mapv #(h2o/mt-create-response-receiver
                %
                slot-count
                response-slot-storage-capacity)
              contexts)
        response-slot-size (+ (h2o/mt-response-slot-data-size)
                              response-slot-storage-capacity)
        _ (when (some #(or (nil? %) (mem/null? %)) response-receivers)
            (let [failure (ex-info (format "Could not create native response receiver of %d slots of %d bytes"
                                           slot-count response-slot-size)
                                   {:fixed-response-slots slot-count
                                    :slot-bytes response-slot-size})]
              (unwind-core-state! failure
                                  arena
                                  config-ptr
                                  loops
                                  contexts
                                  wakeup-receivers
                                  response-receivers)
              (throw failure)))
        response-slots
        (mapv (fn [receiver]
                (mapv (fn [index]
                        (mem/reinterpret
                         (h2o/mt-response-slot-data receiver index)
                         response-slot-size
                         arena))
                      (range slot-count)))
              response-receivers)
        on-close-callbacks (mapv #(create-connection-close-callback
                                   active-connection-count_ worker-connection-counts %)
                                 (range n-workers))]
    (assoc state
           ::arena arena
           ::server-config server-config
           ::config-ptr config-ptr
           ::entrypoint-hosts (::entrypoint-hosts server-config)
           ::handler (::handler server-config)
           ::loops loops
           ::contexts contexts
           ::wakeup-receivers wakeup-receivers
           ::response-receivers response-receivers
           ::response-slots response-slots
           ::response-slot-payload-capacity response-slot-payload-capacity
           ::fixed-body-limit fixed-body-limit
           ::on-close-callbacks on-close-callbacks)))

(defn- init-tls-http3-state
  [{::keys [generation-id listener-runtimes config config-ptr entrypoint-hosts n-workers
            loops contexts tls-lookup-callback listener-claims memory-ticket-service]
    :as state} cleanup-state_]
  (let [listeners (mapv :listener listener-runtimes)
        ssl-ctx-ptrs (mapv :ssl-ctx-ptr listener-runtimes)
        has-tls-listeners? (some some? ssl-ctx-ptrs)
        tls-lookup-callback-ptr (:callback-ptr tls-lookup-callback)
        session-ticket-config (get-in config [:tls :session-tickets])
        session-ticket-lifetime-seconds (:lifetime-seconds session-ticket-config)
        session-tickets-disabled? (:disabled? session-ticket-config)
        session-ticket-persistence (:persistence session-ticket-config)
        standalone-ticket-store
        (when (and has-tls-listeners?
                   (not session-tickets-disabled?)
                   (= :storage session-ticket-persistence))
          (tickets/storage-ticket-store (get-in config [:tls :storage])))
        native-ticket-mgr (when (and has-tls-listeners?
                                     (not session-tickets-disabled?))
                            (h2o/ticket-manager-create
                             session-ticket-lifetime-seconds))
        _ (vswap! cleanup-state_ assoc ::native-ticket-mgr native-ticket-mgr)
        key-manager
        (when native-ticket-mgr
          (cond
            (= :storage session-ticket-persistence)
            (-> (tickets/create-key-manager standalone-ticket-store
                                            native-ticket-mgr
                                            session-ticket-config)
                (tickets/start-key-manager!))

            memory-ticket-service
            (tickets/attach-key-manager memory-ticket-service
                                        native-ticket-mgr)

            :else
            (-> (tickets/create-key-manager (tickets/memory-ticket-store)
                                            native-ticket-mgr
                                            session-ticket-config)
                (tickets/start-key-manager!))))
        _ (vswap! cleanup-state_ assoc ::key-manager key-manager)]
    (when native-ticket-mgr
      (doseq [ssl-ctx-ptr ssl-ctx-ptrs]
        (when ssl-ctx-ptr
          (h2o/ssl-ctx-set-tickets ssl-ctx-ptr native-ticket-mgr))))
    (let [http3-contexts (create-http3-contexts listeners
                                                config
                                                config-ptr
                                                native-ticket-mgr
                                                session-ticket-lifetime-seconds
                                                tls-lookup-callback-ptr
                                                cleanup-state_)
          http3-worker-contexts (create-http3-worker-contexts generation-id
                                                              n-workers
                                                              listeners
                                                              loops
                                                              contexts
                                                              entrypoint-hosts
                                                              http3-contexts
                                                              listener-claims
                                                              native-ticket-mgr
                                                              cleanup-state_)]
      (assoc state
             ::ticket-store standalone-ticket-store
             ::native-ticket-mgr native-ticket-mgr
             ::key-manager key-manager
             ::http3-contexts http3-contexts
             ::http3-worker-contexts http3-worker-contexts))))

(defn- init-single-listener-state
  [listener listener-claims n-workers loops contexts arena entrypoint-hosts on-close-callbacks
   tls-lookup-callback-ptr active-connection-count_ worker-connection-counts accepting-workers]
  (let [ssl-ctx-ptr (create-ssl-context listener tls-lookup-callback-ptr)
        listener-claim (or (get listener-claims (listen/listener-key listener))
                           (throw (ex-info "Missing TCP listener claim"
                                           {:listener listener})))
        listener-fd (listen/resource listener-claim)
        dup-fds (try
                  (socket/dup-for-threads listener-fd n-workers)
                  (catch Throwable t
                    (when ssl-ctx-ptr
                      (h2o/free-ssl-ctx ssl-ctx-ptr))
                    (throw t)))
        thread-states
        (vec
         (for [thread-idx (range n-workers)]
           (let [accept-ctx (h2o/create-accept-ctx arena
                                                   (nth contexts thread-idx)
                                                   (get entrypoint-hosts
                                                        (:entrypoint listener))
                                                   ssl-ctx-ptr)
                 accept-callback (create-accept-callback
                                  accept-ctx
                                  (nth on-close-callbacks thread-idx)
                                  active-connection-count_
                                  worker-connection-counts
                                  accepting-workers
                                  thread-idx)
                 accept-callback-ptr (::accept-cb-ptr accept-callback)
                 sock-ptr (h2o/create-socket-for-loop
                           (nth loops thread-idx)
                           (nth dup-fds thread-idx)
                           h2o/H2O_SOCKET_FLAG_DONT_READ)]
             (h2o/socket-read-start sock-ptr accept-callback-ptr)
             {:accept-ctx accept-ctx
              :accept-callback accept-callback
              :accept-callback-ptr accept-callback-ptr
              :socket sock-ptr})))]
    {:listener listener
     :listener-claim listener-claim
     :ssl-ctx-ptr ssl-ctx-ptr
     :dup-fds dup-fds
     :thread-states thread-states}))

(defn- init-listener-state
  [{::keys [n-workers loops contexts arena entrypoint-hosts on-close-callbacks
            listener-claims
            tls-lookup-callback
            active-connection-count_ worker-connection-counts accepting-workers]
    :as state} cleanup-state_]
  (let [tls-lookup-callback-ptr (:callback-ptr tls-lookup-callback)]
    (-> state
        ::config
        :entrypoints
        (->> (into [] (mapcat config/entrypoint->listeners))
             (mapv (fn [listener]
                     (let [runtime (init-single-listener-state listener
                                                               listener-claims
                                                               n-workers
                                                               loops
                                                               contexts
                                                               arena
                                                               entrypoint-hosts
                                                               on-close-callbacks
                                                               tls-lookup-callback-ptr
                                                               active-connection-count_
                                                               worker-connection-counts
                                                               accepting-workers)]
                       (vswap! cleanup-state_ update ::listener-runtimes (fnil conj []) runtime)
                       runtime))))
        (->> (assoc state ::listener-runtimes)))))

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

(defn- init-worker-state
  [{::keys [config executor n-workers loops contexts listener-runtimes http3-worker-contexts
            max-connections shutting-down? message-handler wakeup-receivers response-receivers
            response-slots response-slot-payload-capacity fixed-body-limit stream-pools
            arena stop-accepting-remaining_ stopped-accepting_
            worker-connection-counts accepting-workers limit-waiting]
    :as state} cleanup-state_]
  (let [hot-poll-ns (* 1000 (long (:worker-hot-poll-micros config)))
        reuse? (= :reuse-within-connection (:request-threads config))
        workers-and-states
        (vec
         (for [thread-idx (range n-workers)]
           (let [loop-ptr (nth loops thread-idx)
                 ctx-ptr (nth contexts thread-idx)
                 callback-dispatch (callback-dispatch/create arena)
                 thread-listener-states
                 (mapv #(nth (:thread-states %) thread-idx) listener-runtimes)
                 listener-socks-for-thread (mapv :socket thread-listener-states)
                 accept-callbacks-for-thread (mapv :accept-callback-ptr thread-listener-states)
                 http3-ctxs-for-thread (nth http3-worker-contexts thread-idx)
                 http3-ctxs-guard (http3-ctxs-guard)
                 reuse (when reuse? (request-dispatch/create executor))
                 worker-state {:listener-socks listener-socks-for-thread
                               :accept-callbacks accept-callbacks-for-thread
                               :loop-ptr loop-ptr
                               :ctx-ptr ctx-ptr
                               :http3-ctxs http3-ctxs-for-thread
                               :http3-ctxs-guard http3-ctxs-guard
                               :max-connections max-connections
                               :shutting-down? shutting-down?
                               :worker-connection-counts worker-connection-counts
                               :worker-idx thread-idx
                               :accepting-workers accepting-workers
                               :limit-waiting limit-waiting
                               ::stop-accepting-remaining_ stop-accepting-remaining_
                               ::stopped-accepting_ stopped-accepting_
                               ::stopped-accepting?_ (AtomicBoolean. false)
                               ::listeners-closed?_ (AtomicBoolean. false)
                               ::http3-sockets-closed?_ (AtomicBoolean. false)}
                 _ (vswap! cleanup-state_ update ::worker-states (fnil conj []) worker-state)
                 worker (evloop/start-worker!
                         (fn [worker loop-state]
                           (worker-loop worker loop-state worker-state))
                         message-handler
                         (nth wakeup-receivers thread-idx)
                         :stream-pool (nth stream-pools thread-idx)
                         :on-native-failure!
                         (fn [worker cause]
                           (try
                             (worker-native-failure! worker cause)
                             (finally
                               (.set ^AtomicLongArray worker-connection-counts
                                     (int thread-idx) Long/MAX_VALUE)
                               (withdraw-http3-worker! http3-ctxs-guard
                                                       http3-ctxs-for-thread)
                               (wake-other-workers! accepting-workers thread-idx)
                               (count-stopped-accepting! worker-state))))
                         :discard-command! (fn [[op _ _ st record]]
                                             (when (= :h2o/output op)
                                               (rq/discard-record! st record)))
                         :response-receiver (nth response-receivers thread-idx)
                         :response-slots (nth response-slots thread-idx)
                         :response-slot-payload-capacity response-slot-payload-capacity
                         :fixed-body-limit fixed-body-limit
                         :callback-dispatch callback-dispatch
                         :request-dispatch reuse
                         :zero-wait-turn! (zero-wait-turn-fn loop-ptr reuse)
                         :hot-poll-ns hot-poll-ns)]
             (vswap! cleanup-state_ update ::workers (fnil conj []) worker)
             (.set ^AtomicReferenceArray accepting-workers (int thread-idx) worker)
             [worker worker-state])))]
    (assoc state
           ::workers (mapv first workers-and-states)
           ::worker-states (mapv second workers-and-states))))

(defn- init-stream-pools
  [{::keys [config n-workers] :as state}]
  (let [pools_ (atom [])]
    (try
      (dotimes [_ n-workers]
        (swap! pools_ conj (bp/create-stream-pool (:output-buffer-size config))))
      (assoc state ::stream-pools @pools_)
      (catch Throwable t
        (doseq [pool @pools_] (bp/retire! pool))
        (throw t)))))

(defn- finalize-generation-state
  [state]
  (dissoc state ::message-handler))

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
  (boolean (some #(.get ^AtomicReference (:native-failure_ %)) (::workers generation))))

(defn begin-stop!
  [{::keys [stop-lock phase] :as generation}]
  (locking stop-lock
    (when (= :running @phase)
      (when-let [handler-ptr (some-> generation ::handler ::h2o/handler-ptr)]
        (h2o/handler-set-shutting-down handler-ptr 1))
      (when-let [shutting-down? (::shutting-down? generation)]
        (.set ^AtomicBoolean shutting-down? true))
      (when-let [key-mgr (::key-manager generation)]
        (tickets/stop-key-manager! key-mgr))
      (when (seq (::workers generation))
        (doseq [worker (::workers generation)]
          (some-> (:request-dispatch worker) request-dispatch/begin-stop!)
          (callback-dispatch/begin-drain! (:callback-dispatch worker)))
        (evloop/broadcast-wake! (::workers generation))
        (let [deadline (+ (System/nanoTime) 1000000000)]
          (doseq [worker (::workers generation)]
            (some-> (:request-dispatch worker)
                    (request-dispatch/await-stop-moves!
                     (max 1 (quot (- deadline (System/nanoTime)) 1000000)))))))
      (when-let [^ExecutorService executor (::executor generation)]
        (.shutdown executor))
      (reset! phase :stopping)))
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
  ([generation]
   (stop! generation 60 TimeUnit/SECONDS))
  ([{::keys [^ExecutorService executor ^ExecutorService close-callback-executor stop-lock]
     :as generation} ^long timeout ^TimeUnit timeunit]
   (assert generation)
   (begin-stop! generation)
   (locking stop-lock
     (let [phase-atom (::phase generation)]
       (when (not= :stopped @phase-atom)
         (when (contains? #{:stopping :retiring :retirement-failed} @phase-atom)
           (when (= :stopping @phase-atom)
             (when executor
               (when-not (.awaitTermination executor timeout timeunit)
                 (doseq [worker (::workers generation)]
                   (some-> (:request-dispatch worker) request-dispatch/interrupt-remaining!))
                 (.shutdownNow executor)
                 (when-not (.awaitTermination executor timeout timeunit)
                   (println "Virtual thread request executor pool did not shutdown cleanly"))))
             (when (seq (::workers generation))
               (evloop/broadcast-wake! (::workers generation)))
             (reset! phase-atom :retiring))
           (let [{:keys [joined? error listeners-closed?]}
                 (try
                   (let [deadline (+ (System/nanoTime) (.toNanos timeunit timeout))]
                     (doseq [worker (::workers generation)]
                       (let [remaining (- deadline (System/nanoTime))]
                         (when (pos? remaining)
                           (.join ^Thread (:thread worker)
                                  (max 1 (quot remaining 1000000)))))))
                   (let [sockets-left? (some-> ^AtomicLong (::active-connection-count_ generation) .get pos?)
                         workers (::workers generation)
                         states (vec (map-indexed
                                      (fn [idx worker]
                                        (retire-worker! worker
                                                        (get (::worker-states generation) idx)
                                                        sockets-left?))
                                      workers))
                         ids-in (fn [status]
                                  (vec (keep (fn [[worker s]] (when (= status s) (:id worker)))
                                             (map vector workers states))))]
                     (cond
                       (some #{:alive} states)
                       {:joined? false
                        :error (ex-info "Generation retirement timed out while worker threads still run. Native resources kept."
                                        {:alive-workers (ids-in :alive)})}

                       (or (some #{:unfinished} states)
                           (not-every? #(zero? (bp/checked-out %)) (::stream-pools generation)))
                       {:joined? false
                        :listeners-closed? true
                        :error (ex-info "Worker threads ended with connections, responses or blocks nothing can finish. TCP and Unix listeners and HTTP/3 UDP sockets closed. Loops, contexts and memory kept."
                                        {:unfinished-workers (ids-in :unfinished)})}

                       :else
                       {:joined? true}))
                   (catch InterruptedException t
                     (.interrupt (Thread/currentThread))
                     {:joined? false :error t})
                   (catch Throwable t
                     {:joined? false :error t}))]
             (if-not joined?
               (do
                 (doseq [worker (::workers generation)]
                   (.compareAndSet ^AtomicReference (:termination-cause_ worker) nil error)
                   (p/wake worker))
                 (retain-failed-retirement! generation phase-atom error)
                 (when listeners-closed?
                   (close-kept-http3-sockets! generation error)
                   (release-socket-listener-claims! (::listener-claims generation) error))
                 (throw error))
               (do
                 (doseq [worker (::workers generation)]
                   (callback-dispatch/finish! (:callback-dispatch worker)))
                 (when close-callback-executor
                   (.shutdown close-callback-executor)
                   (when-not (.awaitTermination close-callback-executor timeout timeunit)
                     (.shutdownNow close-callback-executor)
                     (when-not (.awaitTermination close-callback-executor timeout timeunit)
                       (println "Virtual thread close callback executor did not shutdown cleanly"))))
                 (doseq [idx (range (count (::workers generation))
                                    (count (::contexts generation)))]
                   (doseq [listener (::listener-runtimes generation)]
                     (when-let [sock (get-in listener [:thread-states idx :socket])]
                       (h2o/socket-close sock)))
                   (doseq [ctx (get (::http3-worker-contexts generation) idx)]
                     (when (and ctx (not (mem/null? ctx)))
                       (h2o/http3-free-worker-ctx ctx)))
                   (h2o/mt-destroy-wakeup-receiver (nth (::wakeup-receivers generation) idx))
                   (h2o/mt-destroy-response-receiver (nth (::response-receivers generation) idx))
                   (h2o/context-dispose (nth (::contexts generation) idx)))
                 (when-let [http3-contexts (::http3-contexts generation)]
                   (free-http3-contexts http3-contexts))
                 (when-let [ticket-mgr (::native-ticket-mgr generation)]
                   (h2o/ticket-manager-destroy ticket-mgr))
                 (when (seq (::loops generation))
                   (h2o/destroy-loops (::loops generation)))
                 (when-let [config-ptr (::config-ptr generation)]
                   (h2o/config-dispose config-ptr))
                 (doseq [{:keys [ssl-ctx-ptr]} (::listener-runtimes generation)]
                   (when ssl-ctx-ptr
                     (h2o/free-ssl-ctx ssl-ctx-ptr)))
                 (doseq [pool (::stream-pools generation)]
                   (when-not (bp/retire! pool)
                     (throw (ex-info "Streaming storage remains checked out" {}))))
                 (release-listener-claims! (::listener-claims generation))
                 (when-let [arena (::arena generation)]
                   (.close ^java.lang.AutoCloseable arena))
                 (remove-failed-retirement! generation)
                 (reset! phase-atom :stopped))))))
       nil))))

(defn start!
  ([compiled-config cert-runtime]
   (start! compiled-config cert-runtime {}))
  ([compiled-config cert-runtime {:keys [activate-http3-transports? generation-id listener-pool
                                         memory-ticket-service]
                                  :or {activate-http3-transports? true
                                       generation-id 1
                                       listener-pool (listen/open-pool)}}]
   (let [compiled-config (config/config->listeners compiled-config)
         listeners (:listeners compiled-config)
         listener-claims (acquire-listener-claims! listener-pool listeners)
         cleanup-state_ (volatile! nil)]
     (try
       (let [state (prepare-generation-state compiled-config
                                             cert-runtime
                                             listener-pool
                                             listener-claims
                                             generation-id
                                             memory-ticket-service)
             _ (vreset! cleanup-state_ state)
             state (assoc state
                          ::executor (new-request-executor)
                          ::close-callback-executor (new-close-callback-executor))
             _ (vreset! cleanup-state_ state)]
         (cond-> (reduce (fn [state initialize]
                           (let [next-state (initialize state)]
                             (vreset! cleanup-state_ next-state)
                             next-state))
                         state
                         [init-tls-lookup-state init-core-state init-stream-pools
                          #(init-listener-state % cleanup-state_)
                          #(init-tls-http3-state % cleanup-state_)
                          #(init-worker-state % cleanup-state_) finalize-generation-state])
           activate-http3-transports? activate-http3-transports!))
       (catch Throwable t
         (try
           (if-let [state @cleanup-state_]
             (stop! state)
             (release-listener-claims! listener-claims))
           (catch Throwable cleanup-error
             (when-not (identical? t cleanup-error)
               (.addSuppressed t cleanup-error))))
         (throw t))))))