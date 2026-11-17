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
   [babashka.ffi :as mem]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.config :as config]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.generation.accept :as accept]
   [ol.busker.generation.tls :as tls]
   [ol.busker.evloop :as evloop]
   [ol.busker.lifecycle.generation :as gen]
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
   [taoensso.trove :as trove])
  (:import
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
  [generation error]
  (swap! failed-retirements_ conj generation)
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
    (accept/log-callback-failure! ::worker-failed cause))
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
  promise of the generation once every worker stopped accepting. Only the
  worker thread calls it."
  [{:keys [^AtomicReferenceArray statuses worker-idx stopped-accepting_]} status]
  (.set statuses (int worker-idx) status)
  (when (gen/all-stopped-accepting? (map #(.get statuses (int %)) (range (.length statuses))))
    (deliver stopped-accepting_ true)))

(defn- withdraw-http3-worker!
  "Stops other workers from sending new HTTP/3 connections to this worker,
  unless its `status` shows its HTTP/3 contexts released. Call it on the
  worker thread."
  [status http3-ctxs]
  (when (contains? #{nil :acquired} (get-in status [:nodes :h3]))
    (run! h2o/http3-withdraw-worker http3-ctxs)))

(defn- worker-handles
  "Returns the native handles of the worker at index `i` of generation map `g`.
  A context that has no worker thread has no `:worker`."
  [{::keys [workers listener-runtimes http3-worker-contexts contexts wakeup-receivers
            response-receivers stream-pools]} i]
  (let [worker (get workers i)]
    {:worker worker
     :listener-socks (into [] (keep #(get-in % [:thread-states i :socket])) listener-runtimes)
     :http3-ctxs (into [] (remove #(or (nil? %) (mem/null? %))) (get http3-worker-contexts i))
     :ctx-ptr (get contexts i)
     :wakeup-receiver (get wakeup-receivers i)
     :response-receiver (get response-receivers i)
     :pool (or (:stream-pool worker) (get stream-pools i))}))

(defn- worker-kinds
  "Returns the kinds of the nodes that worker handles `h` hold."
  [{:keys [worker listener-socks http3-ctxs ctx-ptr response-receiver pool]}]
  (cond-> []
    (seq listener-socks) (conj :listeners)
    (if worker (:response-receiver_ worker) response-receiver) (conj :response-receiver)
    pool (conj :pool)
    (seq http3-ctxs) (conj :h3)
    ctx-ptr (conj :context)
    (:callback-dispatch worker) (conj :callback-dispatch)))

(defn- release-worker-node!
  "Releases node `kind` of the worker with handles `h`. Returns `:not-yet`
  while its response receiver or stream pool is in use, else `:ok`."
  [{:keys [worker listener-socks http3-ctxs ctx-ptr wakeup-receiver response-receiver pool]}
   kind]
  (if (identical?
       :not-yet
       (case kind
         :listeners (run! #(when-not (mem/null? %) (h2o/socket-close %)) listener-socks)
         :response-receiver (if worker
                              (when-not (destroy-response-receiver! worker) :not-yet)
                              (h2o/mt-destroy-response-receiver response-receiver))
         :pool (when-not (bp/retire! pool) :not-yet)
         :h3 (run! h2o/http3-free-worker-ctx http3-ctxs)
         :context (do (if worker
                        (retire-wakeup-receiver! worker)
                        (h2o/mt-destroy-wakeup-receiver wakeup-receiver))
                      (h2o/context-dispose ctx-ptr))
         :callback-dispatch (callback-dispatch/finish! (:callback-dispatch worker))))
    :not-yet
    :ok))

(defn- release-worker-nodes!
  "Releases, on the worker thread, each node of the worker that the core lets
  it release now, and publishes each state change. Returns the last status."
  [worker {:keys [^AtomicReferenceArray statuses worker-idx] :as state} drained?]
  (let [h (assoc state :worker worker)
        acquired (into {} (map (fn [kind] [[kind worker-idx] :acquired])) (worker-kinds h))]
    (loop [status (.get statuses (int worker-idx))]
      (let [nodes (into acquired (map (fn [[kind s]] [[kind worker-idx] s])) (:nodes status))
            obs {:workers {worker-idx (assoc status :drained? drained?)}}]
        (if-let [[kind] (gen/next-release nodes obs worker-idx #{})]
          (do
            (publish-status! state (assoc-in status [:nodes kind] :releasing))
            (let [outcome (release-worker-node! h kind)
                  status (assoc-in status [:nodes kind] (gen/release-state outcome))]
              (publish-status! state status)
              (if (= :not-yet outcome) status (recur status))))
          status)))))

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
  (publish-status! state (assoc (.get statuses (int worker-idx)) :stage :draining))
  (release-worker-nodes! worker state false)
  (h2o/context-request-shutdown ctx-ptr)
  :draining)

(defn- finish-drain!
  "Releases the nodes of a drained worker. Returns `:released` once its context
  is released, else `:draining`."
  [worker state]
  (let [status (release-worker-nodes! worker state true)]
    (if (not= :released (get-in status [:nodes :context]))
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
                (when ring-work? (fixed-final/notify-capacity! worker))
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

(defn- init-tls-lookup-state
  [{::keys [config cert-runtime] :as state}]
  (try
    (if (tls/tls-callback-required? config)
      (let [lookup-fn (tls/build-tls-lookup-fn config cert-runtime)
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
  (let [{:keys [cert-file key-file]} (tls/first-static-tls-identity config)]
    (try
      (into {}
            (keep-indexed
             (fn [idx listener]
               (when (tls/http3-enabled? listener)
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
          (when (tls/http3-enabled? listener)
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
                   (tls/http3-enabled? listener)
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
     ::progress_ (atom {})
     ::stop-lock (Object.)
     ::message-handler evloop-msg-processor
     ::shutting-down? (AtomicBoolean. false)
     ::active-connection-count_ (AtomicLong. 0)
     ::worker-connection-counts (AtomicLongArray. (int n-workers))
     ::accepting-workers (AtomicReferenceArray. (int n-workers))
     ::limit-waiting (AtomicBoolean. false)
     ::worker-statuses (AtomicReferenceArray. ^objects (object-array (repeat n-workers serving-status)))
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
        large-head-warnings (when (:large-response-head-warnings? config)
                              (doto (AtomicLongArray. 2) (.set 0 (System/nanoTime))))
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
        on-close-callbacks (mapv #(accept/create-connection-close-callback
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
           ::large-head-warnings large-head-warnings
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
                 accept-callback (accept/create-accept-callback
                                  accept-ctx
                                  (nth on-close-callbacks thread-idx)
                                  active-connection-count_
                                  worker-connection-counts
                                  accepting-workers
                                  thread-idx)
                 accept-callback-ptr (::accept/accept-cb-ptr accept-callback)
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
  [{::keys [config executor n-workers loops listener-runtimes
            max-connections shutting-down? message-handler wakeup-receivers response-receivers
            response-slots response-slot-payload-capacity fixed-body-limit large-head-warnings stream-pools
            arena worker-statuses stopped-accepting_
            worker-connection-counts accepting-workers limit-waiting]
    :as state} cleanup-state_]
  (let [hot-poll-ns (* 1000 (long (:worker-hot-poll-micros config)))
        reuse? (= :reuse-within-connection (:request-threads config))
        workers
        (vec
         (for [thread-idx (range n-workers)]
           (let [loop-ptr (nth loops thread-idx)
                 callback-dispatch (callback-dispatch/create arena)
                 reuse (when reuse? (request-dispatch/create executor))
                 worker-state (merge (worker-handles state thread-idx)
                                     {:accept-callbacks
                                      (mapv #(get-in % [:thread-states thread-idx :accept-callback-ptr])
                                            listener-runtimes)
                                      :loop-ptr loop-ptr
                                      :max-connections max-connections
                                      :shutting-down? shutting-down?
                                      :worker-connection-counts worker-connection-counts
                                      :worker-idx thread-idx
                                      :accepting-workers accepting-workers
                                      :limit-waiting limit-waiting
                                      :statuses worker-statuses
                                      :stopped-accepting_ stopped-accepting_})
                 worker (evloop/start-worker!
                         (fn [worker stage mailbox-work?]
                           (worker-loop worker stage mailbox-work? worker-state))
                         message-handler
                         (nth wakeup-receivers thread-idx)
                         :loop-state :serving
                         :stream-pool (nth stream-pools thread-idx)
                         :on-native-failure!
                         (fn [worker cause]
                           (try
                             (worker-native-failure! worker cause)
                             (finally
                               (.set ^AtomicLongArray worker-connection-counts
                                     (int thread-idx) Long/MAX_VALUE)
                               (let [status (.get ^AtomicReferenceArray worker-statuses (int thread-idx))]
                                 (withdraw-http3-worker! status (:http3-ctxs worker-state))
                                 (accept/wake-other-workers! accepting-workers thread-idx)
                                 (publish-status! worker-state (assoc status :failure cause))))))
                         :discard-command! (fn [[op _ _ st record]]
                                             (when (= :h2o/output op)
                                               (rq/discard-record! st record)))
                         :response-receiver (nth response-receivers thread-idx)
                         :response-slots (nth response-slots thread-idx)
                         :response-slot-payload-capacity response-slot-payload-capacity
                         :fixed-body-limit fixed-body-limit
                         :large-head-warnings large-head-warnings
                         :callback-dispatch callback-dispatch
                         :request-dispatch reuse
                         :zero-wait-turn! (zero-wait-turn-fn loop-ptr reuse)
                         :hot-poll-ns hot-poll-ns)]
             (vswap! cleanup-state_ update ::workers (fnil conj []) worker)
             (.set ^AtomicReferenceArray accepting-workers (int thread-idx) worker)
             worker)))]
    (assoc state ::workers workers)))

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

(defn- worker-count [{::keys [workers contexts]}]
  (max (count workers) (count contexts)))

(defn- held-nodes
  "Returns the nodes that generation map `g` holds, each `:acquired`. `g` may
  be a generation that started only in part."
  [{::keys [listener-claims close-callback-executor arena config-ptr loops listener-runtimes
            native-ticket-mgr http3-contexts]
    :as g}]
  (let [udp? #(= :udp (:transport %))]
    (into (cond-> {}
            (some (complement udp?) (keys listener-claims)) (assoc :socket-claims :acquired)
            (some udp? (keys listener-claims)) (assoc :udp-claims :acquired)
            close-callback-executor (assoc :close-executor :acquired)
            arena (assoc :arena :acquired)
            config-ptr (assoc :config :acquired)
            (seq loops) (assoc :loops :acquired)
            (some :ssl-ctx-ptr listener-runtimes) (assoc :ssl-ctxs :acquired)
            native-ticket-mgr (assoc :ticket-manager :acquired)
            (seq http3-contexts) (assoc :h3-contexts :acquired))
          (for [i (range (worker-count g))
                kind (worker-kinds (worker-handles g i))]
            [[kind i] :acquired]))))

(defn- ended-worker-drained?
  "Returns true when the ended thread of the worker at `i` left nothing that a
  loop turn must finish. It reads native counts only from nodes that are
  acquired in `nodes` and in the worker's `status`."
  [{::keys [^AtomicLong active-connection-count_] :as g} nodes i worker status]
  (let [held? (fn [kind] (and (= :acquired (nodes [kind i]))
                              (contains? #{nil :acquired} (get-in status [:nodes kind]))))
        {:keys [ctx-ptr http3-ctxs]} (worker-handles g i)]
    (and (not (mailbox/pending? (:mailbox worker)))
         (response-exchange-drained? worker)
         (zero? (bp/checked-out (:stream-pool worker)))
         (or (nil? (.get ^AtomicReference (:native-failure_ worker)))
             (and (not (some-> active-connection-count_ .get pos?))
                  (not (and (held? :context)
                            (or (pos? (h2o/context-get-active-conns ctx-ptr))
                                (pos? (h2o/context-get-shutdown-conns ctx-ptr)))))
                  (not (and (held? :h3) (some has-active-http3-connections? http3-ctxs))))))))

(defn- observe
  "Returns the observation of generation `g` for [[gen/step]], with the node
  states `nodes` of its progress."
  [{::keys [workers ^AtomicReferenceArray worker-statuses] :as g} nodes]
  {:workers (mapv (fn [i]
                    (let [worker (get workers i)
                          ^Thread thread (:thread worker)]
                      (cond
                        (nil? thread) {:thread :none}
                        (.isAlive thread) {:thread :alive}
                        :else (let [status (.get worker-statuses (int i))]
                                (assoc status
                                       :thread :ended
                                       :drained? (ended-worker-drained? g nodes i worker status))))))
                  (range (worker-count g)))})

(defn- shutdown-executor!
  [^ExecutorService executor ^long timeout-ns message]
  (.shutdown executor)
  (when-not (.awaitTermination executor timeout-ns TimeUnit/NANOSECONDS)
    (.shutdownNow executor)
    (when-not (.awaitTermination executor timeout-ns TimeUnit/NANOSECONDS)
      (println message))))

(defn- release-generation-node!
  [{::keys [close-callback-executor http3-contexts native-ticket-mgr loops config-ptr
            listener-runtimes listener-claims ^java.lang.AutoCloseable arena]}
   kind timeout-ns]
  (case kind
    :close-executor (shutdown-executor! close-callback-executor timeout-ns
                                        "Virtual thread close callback executor did not shutdown cleanly")
    :h3-contexts (free-http3-contexts http3-contexts)
    :ticket-manager (h2o/ticket-manager-destroy native-ticket-mgr)
    :loops (h2o/destroy-loops loops)
    :config (h2o/config-dispose config-ptr)
    :ssl-ctxs (doseq [{:keys [ssl-ctx-ptr]} listener-runtimes :when ssl-ctx-ptr]
                (h2o/free-ssl-ctx ssl-ctx-ptr))
    :socket-claims (doseq [[claim-key claim] listener-claims :when (not= :udp (:transport claim-key))]
                     (listen/release! claim))
    :udp-claims (doseq [[claim-key claim] listener-claims :when (= :udp (:transport claim-key))]
                  (listen/release! claim))
    :arena (.close arena))
  :ok)

(defn- close-kept-http3-sockets!
  "Closes the HTTP/3 UDP sockets of the kept worker at index `i`, so that the
  UDP ports can be bound again and get no datagrams for those sockets. The
  contexts and the UDP claims stay, because kept connections still point at
  the contexts and the contexts at their transport."
  [g i]
  (when-not (every? zero? (mapv h2o/http3-close-worker-sockets (:http3-ctxs (worker-handles g i))))
    (throw (ex-info "HTTP/3 UDP socket of a kept worker stays open" {:worker-idx i})))
  :ok)

(defn- signal-stop!
  [{::keys [handler ^AtomicBoolean shutting-down? key-manager workers ^ExecutorService executor]}]
  (when-let [handler-ptr (::h2o/handler-ptr handler)]
    (h2o/handler-set-shutting-down handler-ptr 1))
  (some-> shutting-down? (.set true))
  (some-> key-manager tickets/stop-key-manager!)
  (when (seq workers)
    (doseq [worker workers]
      (some-> (:request-dispatch worker) request-dispatch/begin-stop!)
      (callback-dispatch/begin-drain! (:callback-dispatch worker)))
    (evloop/broadcast-wake! workers)
    (let [deadline (+ (System/nanoTime) 1000000000)]
      (doseq [worker workers]
        (some-> (:request-dispatch worker)
                (request-dispatch/await-stop-moves!
                 (max 1 (quot (- deadline (System/nanoTime)) 1000000)))))))
  (some-> executor .shutdown)
  :ok)

(defn- interrupt-handlers!
  [{::keys [^ExecutorService executor workers]} ^long timeout-ns]
  (doseq [worker workers]
    (some-> (:request-dispatch worker) request-dispatch/interrupt-remaining!))
  (.shutdownNow executor)
  (when-not (.awaitTermination executor timeout-ns TimeUnit/NANOSECONDS)
    (println "Virtual thread request executor pool did not shutdown cleanly"))
  :ok)

(defn- join-workers!
  [{::keys [workers]} ^long timeout-ns]
  (when (seq workers)
    (evloop/broadcast-wake! workers))
  (try
    (let [deadline (+ (System/nanoTime) timeout-ns)]
      (doseq [worker workers
              :let [remaining (- deadline (System/nanoTime))]
              :when (pos? remaining)]
        (.join ^Thread (:thread worker) (max 1 (quot remaining 1000000)))))
    :ok
    (catch InterruptedException e
      (.interrupt (Thread/currentThread))
      {:error e})))

(defn- retirement-error
  "Returns the error of retirement `result` that is not stopped."
  [{::keys [workers]} {:keys [outcome error alive-workers unfinished-workers errors]}]
  (let [ids (fn [idxs] (mapv #(:id (get workers %) %) idxs))]
    (case outcome
      :failed error
      :not-joined (or error
                      (ex-info "Generation retirement timed out while worker threads still run. Native resources kept."
                               {:alive-workers (ids alive-workers)}))
      :kept (let [e (ex-info "Worker threads ended with connections, responses or blocks nothing can finish. TCP and Unix listeners and HTTP/3 UDP sockets closed. Loops, contexts and memory kept."
                             {:unfinished-workers (ids unfinished-workers)})]
              (run! #(.addSuppressed ^Throwable e ^Throwable %) errors)
              e))))

(defn- perform!
  "Performs the action of retirement progress `p` on generation `g`. Returns
  its outcome for [[gen/step]]."
  [{::keys [workers ^ExecutorService executor] :as g} {[op arg] :action :as p} ^long timeout-ns]
  (case op
    :signal-stop (signal-stop! g)
    :await-handlers (if (or (nil? executor)
                            (.awaitTermination executor timeout-ns TimeUnit/NANOSECONDS))
                      :ok
                      :timed-out)
    :interrupt-handlers (interrupt-handlers! g timeout-ns)
    :join-workers (join-workers! g timeout-ns)
    :force-fail-workers (let [error (retirement-error g {:outcome :not-joined :alive-workers arg
                                                         :error (:join-error p)})]
                          (doseq [i arg :let [worker (get workers i)]]
                            (.compareAndSet ^AtomicReference (:termination-cause_ worker) nil error)
                            (p/wake worker))
                          :ok)
    :release (let [[kind i] (if (vector? arg) arg [arg])]
               (if i
                 (release-worker-node! (worker-handles g i) kind)
                 (release-generation-node! g kind timeout-ns)))
    :when-kept (close-kept-http3-sockets! g (second arg))))

(defn- run-retirement!
  "Performs the retirement actions of generation `g` and steps the core with
  each outcome while `more?` holds for the next action. Returns the result
  of the run, or nil when the run stopped before its end."
  [{::keys [progress_] :as g} ^long timeout-ns more?]
  (let [start (update (gen/retire @progress_) :nodes #(or % (held-nodes g)))]
    (loop [p (gen/step start (observe g (:nodes start)) nil)]
      (reset! progress_ p)
      (if (more? (first (:action p)))
        (let [outcome (try (perform! g p timeout-ns) (catch Throwable t {:error t}))]
          (recur (gen/step p (observe g (:nodes p)) outcome)))
        (gen/result p)))))

(defn- report-retirement!
  "Records the retirement `result` of `generation` in the list of kept
  generations, and throws its error when it is not stopped."
  [generation result]
  (case (:outcome result)
    nil nil
    :stopped (remove-failed-retirement! generation)
    :failed (throw (:error result))
    (let [error (retirement-error generation result)]
      (retain-failed-retirement! generation error)
      (throw error))))

(defn phase
  "Returns the lifecycle phase of `generation`: `:running`, `:stopping`,
  `:retiring`, `:kept` or `:stopped`."
  [generation]
  (gen/phase @(::progress_ generation)))

(defn begin-stop!
  "Stops `generation` accepting new work, once. Returns `generation`."
  [{::keys [stop-lock] :as generation}]
  (locking stop-lock
    (report-retirement! generation (run-retirement! generation 0 #{:signal-stop})))
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
  ([{::keys [stop-lock] :as generation} ^long timeout ^TimeUnit timeunit]
   (assert generation)
   (locking stop-lock
     (report-retirement! generation
                         (run-retirement! generation (.toNanos timeunit timeout) #(not= :done %))))
   nil))

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