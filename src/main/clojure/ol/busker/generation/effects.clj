(ns ^:no-doc ol.busker.generation.effects
  "The acquire and the release of each native resource kind of a generation,
  in [[table]], keyed by the kinds of [[ol.busker.lifecycle.generation/resources]].

  An effect makes no lifecycle decision. It does one native or JVM action on
  the handles of the generation map `g` and returns the new map, or throws.
  An acquire that throws has released what it made, or throws as partial
  work, see [[partial-acquire]]. The lifecycle core
  decides when each effect runs. Effects run on the platform lifecycle
  thread, except the worker node releases that the worker thread runs.

  ## Related Namespaces

  - [[ol.busker.lifecycle.generation]] decides the order.
  - [[ol.busker.generation]] runs the effects."
  (:require
   [babashka.ffi :as mem]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.config :as config]
   [ol.busker.evloop :as evloop]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.generation :as-alias g]
   [ol.busker.generation.accept :as accept]
   [ol.busker.generation.tls :as tls]
   [ol.busker.listen :as listen]
   [ol.busker.native :as h2o]
   [ol.busker.native.socket :as socket]
   [ol.busker.request :as request]
   [ol.busker.request-dispatch :as request-dispatch]
   [ol.busker.tickets :as tickets]
   [ol.busker.wake-endpoint :as wake-endpoint])
  (:import
   [java.lang AutoCloseable]
   [java.util.concurrent ExecutorService Executors RejectedExecutionException TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicReference]
   [java.util.concurrent.locks Lock ReentrantReadWriteLock]))

(set! *warn-on-reflection* true)

(defn- stage-error
  [message stage data t]
  (let [error-data (ex-data t)]
    (ex-info message (merge {:stage (or (:stage error-data) stage)} data error-data) t)))

(defn partial-acquire
  "Returns the first error of an acquire that could not free its partial
  work, or nil when `t` is not such a throw."
  [t]
  (::partial (ex-data t)))

(defn- wrap-stage-error
  [message stage data t]
  (throw (if (partial-acquire t) t (stage-error message stage data t))))

(defn- free-and-throw
  "Runs `free`, then throws `t`. When `free` throws, its error is suppressed
  in the first error, and `t` throws as partial work, see [[partial-acquire]],
  so that the core keeps the node and what it needs."
  [^Throwable t free]
  (let [^Throwable first-error (or (partial-acquire t) t)]
    (try
      (free)
      (catch Throwable e
        (.addSuppressed first-error e)
        (throw (if (partial-acquire t)
                 t
                 (ex-info "An acquire could not free its partial work" {::partial t} t)))))
    (throw t)))

(defn- undo-and-throw
  "Runs `undo`, the cleanup of an acquire that failed with `t`, then throws
  `t`, see [[free-and-throw]]. When `t` is partial work already, it throws
  `t` and does not run `undo`, because the work that an inner cleanup could
  not free may still need what `undo` frees."
  [^Throwable t undo]
  (if (partial-acquire t)
    (throw t)
    (free-and-throw t undo)))

(defn- acquire-each
  "Returns the vector of `(acquire x)` for each of `xs`. When one throws, it
  calls `free-all` on the items made before and throws. It does this also
  for partial work, because no item needs another."
  [acquire free-all xs]
  (reduce (fn [items x]
            (try
              (conj items (acquire x))
              (catch Throwable t
                (free-and-throw t #(free-all items)))))
          []
          xs))

(defn- run-all!
  "Calls `f` on each of `xs`, also after one throws. Then throws the first
  error with the others suppressed."
  [f xs]
  (when-let [[^Throwable e & more] (seq (keep #(try (f %) nil (catch Throwable t t)) xs))]
    (run! #(.addSuppressed e %) more)
    (throw e)))

(defn worker-handles
  "Returns the native handles of the worker at index `i` of generation map `g`,
  and `i` as `:worker-idx`. A context that has no worker thread has no
  `:worker`."
  [{::g/keys [workers callback-dispatches listener-sockets http3-worker-contexts contexts
              wakeup-receivers response-receivers stream-pools]} i]
  (let [worker (get workers i)
        socks (get listener-sockets i)]
    {:worker-idx i
     :worker worker
     :callback-dispatch (get callback-dispatches i)
     :listener-socks (mapv :socket socks)
     :accept-callbacks (mapv :accept-callback-ptr socks)
     :http3-ctxs (into [] (remove #(or (nil? %) (mem/null? %))) (get http3-worker-contexts i))
     :ctx-ptr (get contexts i)
     :wakeup-receiver (get wakeup-receivers i)
     :response-receiver (get response-receivers i)
     :pool (get stream-pools i)}))

;;; Listener claims

(defn- udp-key? [claim-key] (= :udp (:transport claim-key)))

(defn- claim-keys
  [listeners]
  (->> listeners
       (mapcat (fn [listener]
                 (cond-> [(listen/listener-key listener)]
                   (tls/http3-enabled? listener)
                   (conj (listen/listener-key (assoc listener :transport :udp))))))
       distinct))

(defn- release-claims! [claims udp?]
  (run-all! listen/release! (for [[claim-key claim] claims :when (= udp? (udp-key? claim-key))]
                              claim)))

(defn- acquire-claims
  "Claims the listener keys of `g` whose transport is UDP or not, by `udp?`."
  [{::g/keys [listener-pool config] :as g} udp?]
  (let [claim-keys (filterv #(= udp? (udp-key? %)) (claim-keys (:listeners config)))
        claims (acquire-each (fn [claim-key]
                               (try
                                 (listen/acquire-claim listener-pool claim-key)
                                 (catch Throwable t
                                   (throw (stage-error "Failed to acquire listener claims"
                                                       :listener-acquisition {:listener-key claim-key} t)))))
                             #(run-all! listen/release! %)
                             claim-keys)]
    (update g ::g/listener-claims merge (zipmap claim-keys claims))))

;;; Executor, arena, server configuration, loops

(defn- shutdown-executor!
  [^ExecutorService executor ^long timeout-ns message]
  (.shutdown executor)
  (when-not (.awaitTermination executor timeout-ns TimeUnit/NANOSECONDS)
    (.shutdownNow executor)
    (when-not (.awaitTermination executor timeout-ns TimeUnit/NANOSECONDS)
      (println message))))

(defn- close-callback-dispatcher
  [^ExecutorService executor]
  (fn [^Runnable task]
    (try
      (.execute executor task)
      (catch RejectedExecutionException _
        (Thread/startVirtualThread task)))))

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

(defn- acquire-config
  [{::g/keys [arena executor close-callback-executor ring-handler config max-connections] :as g} _]
  (h2o/conn-limit-set-max max-connections)
  (let [config-ptr (mem/alloc arena (h2o/globalconf-size))
        flat-config-ptr (mem/alloc arena h2o/ffi-clj-h2o-flat-globalconf-t)
        flat-config (-> (config->flat-globalconf-t config)
                        (update :server_name #(mem/string->ptr arena %))
                        (update :proxy_status_identity #(mem/string->ptr arena %)))]
    (mem/write flat-config-ptr h2o/ffi-clj-h2o-flat-globalconf-t flat-config)
    (h2o/create-global-conf config-ptr flat-config-ptr)
    (try
      (let [hostconf-ptr (with-open [arena2 (mem/confined-arena)]
                           (h2o/config-register-host config-ptr (h2o/str->iovec "default" arena2) 65535))
            entrypoint-hosts (into {}
                                   (map-indexed (fn [index id]
                                                  [id (create-entrypoint-hosts arena config-ptr index)]))
                                   (config/entrypoint-ids config))
            close-callback-dispatch (close-callback-dispatcher close-callback-executor)
            on-request-cb (fn [req-ctx-ptr]
                            (try
                              (request/on-request executor close-callback-dispatch config
                                                  ring-handler req-ctx-ptr)
                              (catch Throwable t
                                (request/request-callback-failed! ::request/request-callback-failed t)
                                h2o/CLJ_HANDLER_OVERLOADED)))]
        (assoc g
               ::g/config-ptr config-ptr
               ::g/entrypoint-hosts entrypoint-hosts
               ::g/handler (h2o/create-handler hostconf-ptr on-request-cb request/on-request-cleanup
                                               flat-config-ptr arena)))
      (catch Throwable t
        (undo-and-throw t #(h2o/config-dispose config-ptr))))))

(defn- acquire-loops
  [{::g/keys [n-workers] :as g} _]
  (assoc g ::g/loops (acquire-each (fn [_] (h2o/evloop-create)) #(run-all! h2o/evloop-destroy %)
                                   (range n-workers))))

;;; TLS

(defn- create-ssl-context
  "Returns the SSL_CTX of a TLS `listener`, with the session tickets of
  `native-ticket-mgr` when there is one, or nil for a listener without TLS."
  [listener tls-lookup-callback-ptr native-ticket-mgr]
  (try
    (when-let [tls (:tls listener)]
      (when (map? tls)
        (let [ssl-ctx (h2o/create-ssl-ctx nil nil 1 (or tls-lookup-callback-ptr mem/null) mem/null)]
          (when (mem/null? ssl-ctx)
            (throw (ex-info "Failed to create SSL_CTX (check OpenSSL errors in stderr)"
                            {:listener (dissoc listener :tls)
                             :listener-tls (:tls listener)})))
          (when native-ticket-mgr
            (try
              (h2o/ssl-ctx-set-tickets ssl-ctx native-ticket-mgr)
              (catch Throwable t
                (undo-and-throw t #(h2o/free-ssl-ctx ssl-ctx)))))
          ssl-ctx)))
    (catch Throwable t
      (wrap-stage-error "Failed to create TLS listener context" :tls-startup
                        {:listener (dissoc listener :tls)} t))))

(defn- free-ssl-ctxs! [ssl-ctxs]
  (run-all! h2o/free-ssl-ctx (remove nil? ssl-ctxs)))

(defn- tls-lookup-callback
  [{::g/keys [config cert-runtime]}]
  (try
    (when (tls/tls-callback-required? config)
      (h2o/build-tls-lookup-callback (tls/build-tls-lookup-fn config cert-runtime)))
    (catch Throwable t
      (wrap-stage-error "Failed to initialize TLS lookup state" :tls-startup {} t))))

(defn- acquire-ssl-ctxs
  "Builds the TLS lookup callback and one SSL_CTX per TLS listener, nil for
  the others. The callback stub is in an automatic arena and needs no release."
  [{::g/keys [config native-ticket-mgr] :as g} _]
  (let [lookup (tls-lookup-callback g)]
    (assoc g
           ::g/tls-lookup-callback lookup
           ::g/ssl-ctxs (acquire-each #(create-ssl-context % (:callback-ptr lookup) native-ticket-mgr)
                                      free-ssl-ctxs!
                                      (:listeners config)))))

(defn- start-key-manager
  [{::g/keys [config memory-ticket-service]} store native-ticket-mgr]
  (let [session-ticket-config (get-in config [:tls :session-tickets])]
    (cond
      (= :storage (:persistence session-ticket-config))
      (-> (tickets/create-key-manager store native-ticket-mgr session-ticket-config)
          (tickets/start-key-manager!))

      memory-ticket-service
      (tickets/attach-key-manager memory-ticket-service native-ticket-mgr)

      :else
      (-> (tickets/create-key-manager (tickets/memory-ticket-store) native-ticket-mgr
                                      session-ticket-config)
          (tickets/start-key-manager!)))))

(defn- acquire-ticket-manager
  "Creates the native ticket manager and its key manager. Does nothing with no
  TLS listener or with session tickets off."
  [{::g/keys [config] :as g} _]
  (let [{:keys [disabled? persistence lifetime-seconds]} (get-in config [:tls :session-tickets])]
    (if (or disabled? (not-any? #(map? (:tls %)) (:listeners config)))
      g
      (let [store (when (= :storage persistence)
                    (tickets/storage-ticket-store (get-in config [:tls :storage])))
            native-ticket-mgr (h2o/ticket-manager-create lifetime-seconds)]
        (when (or (nil? native-ticket-mgr) (mem/null? native-ticket-mgr))
          (throw (ex-info "Could not create native ticket manager"
                          {:lifetime-seconds lifetime-seconds})))
        (try
          (assoc g
                 ::g/ticket-store store
                 ::g/native-ticket-mgr native-ticket-mgr
                 ::g/key-manager (start-key-manager g store native-ticket-mgr))
          (catch Throwable t
            (undo-and-throw t #(h2o/ticket-manager-destroy native-ticket-mgr))))))))

;;; HTTP/3

(defn- free-http3-contexts!
  [http3-contexts]
  (run-all! (fn [{:keys [quicly-ctx ptls-ctx]}]
              (when quicly-ctx
                (h2o/http3-free-quicly-ctx quicly-ctx))
              (when ptls-ctx
                (h2o/http3-free-ptls-ctx ptls-ctx)))
            (remove nil? http3-contexts)))

(defn- create-http3-context
  "Returns the ptls and quicly contexts of the HTTP/3 listener at `idx`, or
  throws after it freed what it made."
  [{::g/keys [config config-ptr native-ticket-mgr tls-lookup-callback]} idx]
  (let [{:keys [cert-file key-file]} (tls/first-static-tls-identity config)
        ticket-lifetime-seconds (get-in config [:tls :session-tickets :lifetime-seconds])
        ptls-ctx (h2o/http3-create-ptls-ctx cert-file key-file
                                            (or (:callback-ptr tls-lookup-callback) mem/null)
                                            mem/null)]
    (when (or (nil? ptls-ctx) (mem/null? ptls-ctx))
      (throw (ex-info "Failed to create ptls context for HTTP/3"
                      {:listener-index idx :cert-file cert-file :key-file key-file})))
    (try
      (let [encrypt-ticket (when native-ticket-mgr
                             (h2o/ticket-manager-create-encrypt-ticket native-ticket-mgr 1))
            _ (when (and native-ticket-mgr (or (nil? encrypt-ticket) (mem/null? encrypt-ticket)))
                (throw (ex-info "Failed to create session ticket callback for HTTP/3"
                                {:listener-index idx})))
            _ (when (and encrypt-ticket
                         (not (zero? (h2o/ptls-ctx-set-tickets ptls-ctx encrypt-ticket
                                                               ticket-lifetime-seconds 8192))))
                (h2o/free-encrypt-ticket encrypt-ticket)
                (throw (ex-info "Failed to set session tickets for HTTP/3" {:listener-index idx})))
            quicly-ctx (h2o/http3-create-quicly-ctx ptls-ctx config-ptr)]
        (when (or (nil? quicly-ctx) (mem/null? quicly-ctx))
          (throw (ex-info "Failed to create quicly context for HTTP/3" {:listener-index idx})))
        (try
          (when native-ticket-mgr
            (h2o/ticket-manager-set-quic-tag native-ticket-mgr quicly-ctx))
          {:ptls-ctx ptls-ctx :quicly-ctx quicly-ctx :encrypt-ticket encrypt-ticket}
          (catch Throwable t
            (undo-and-throw t #(h2o/http3-free-quicly-ctx quicly-ctx)))))
      (catch Throwable t
        (undo-and-throw t #(h2o/http3-free-ptls-ctx ptls-ctx))))))

(defn- acquire-h3-contexts
  "Makes the HTTP/3 contexts of each HTTP/3 listener. The vector has one entry
  per listener, nil for a listener without HTTP/3."
  [{::g/keys [config] :as g} _]
  (try
    (assoc g ::g/http3-contexts
           (acquire-each (fn [[idx listener]]
                           (when (tls/http3-enabled? listener)
                             (create-http3-context g idx)))
                         free-http3-contexts!
                         (map-indexed vector (:listeners config))))
    (catch Throwable t
      (wrap-stage-error "Failed to initialize HTTP/3 TLS state" :tls-startup {} t))))

(defn- attach-http3
  [{::g/keys [generation-id loops contexts entrypoint-hosts http3-contexts listener-claims
              native-ticket-mgr]}
   i listener-idx listener]
  (let [claim (or (get listener-claims (listen/listener-key (assoc listener :transport :udp)))
                  (throw (ex-info "Missing UDP listener claim" {:listener listener})))
        http3-ctx (h2o/http3-attach-udp-transport (nth contexts i)
                                                  (nth loops i)
                                                  (:quicly-ctx (get http3-contexts listener-idx))
                                                  (get entrypoint-hosts (:entrypoint listener))
                                                  (listen/resource claim)
                                                  (or native-ticket-mgr mem/null)
                                                  generation-id
                                                  (int i))]
    (when (or (nil? http3-ctx) (mem/null? http3-ctx))
      (throw (ex-info "Failed to create HTTP/3 worker context"
                      {:listener-index listener-idx :thread-idx i :port (:port listener)})))
    http3-ctx))

(defn- acquire-h3
  "Attaches worker `i` to the UDP transport of each HTTP/3 listener. The
  vector has one entry per listener, nil for a listener without HTTP/3."
  [{::g/keys [config] :as g} i]
  (assoc-in g [::g/http3-worker-contexts i]
            (acquire-each (fn [[listener-idx listener]]
                            (when (tls/http3-enabled? listener)
                              (attach-http3 g i listener-idx listener)))
                          #(run-all! h2o/http3-free-worker-ctx (remove nil? %))
                          (map-indexed vector (:listeners config)))))

(defn close-kept-udp!
  "Closes the HTTP/3 UDP sockets of the kept worker at index `i`, so that the
  UDP ports can be bound again and get no datagrams for those sockets. The
  contexts and the UDP claims stay, because kept connections still point at
  the contexts and the contexts at their transport."
  [g i]
  (run-all! #(when-not (zero? (h2o/http3-close-worker-sockets %))
               (throw (ex-info "HTTP/3 UDP socket of a kept worker stays open" {:worker-idx i})))
            (:http3-ctxs (worker-handles g i))))

;;; Worker contexts and receivers

(defn retire-wakeup-receiver!
  "Quiesces the wake endpoint of `worker`, then unregisters and frees its wake
  receiver once."
  [worker]
  (when-let [endpoint (:wake-endpoint worker)]
    (wake-endpoint/quiesce-endpoint! endpoint))
  (let [receiver (.getAndSet ^AtomicReference (:wakeup-receiver_ worker) nil)]
    (when (and receiver (not (mem/null? receiver)))
      (h2o/mt-destroy-wakeup-receiver receiver))))

(defn close-response-receiver-admission!
  "Stops producers entering the response receiver of `worker`."
  [worker]
  (when-let [^AtomicBoolean receiver-open?_ (:response-receiver-open?_ worker)]
    (.set receiver-open?_ false))
  (fixed-final/notify-all-capacity! worker))

(defn destroy-response-receiver!
  "Frees the response receiver of `worker` when no producer holds it and no
  response is pending. Returns false when it cannot free it now."
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
                (.set receiver_ nil)
                (h2o/mt-destroy-response-receiver receiver)
                true)
              false)))
        (finally
          (.unlock write-lock))))))

(defn- acquire-context
  [{::g/keys [arena loops config-ptr] :as g} i]
  (let [ctx-ptr (h2o/create-context arena (nth loops i) config-ptr)
        wakeup-receiver (try
                          (let [receiver (h2o/mt-create-wakeup-receiver ctx-ptr)]
                            (when (or (nil? receiver) (mem/null? receiver))
                              (throw (ex-info "Could not create native wake receiver"
                                              {:worker-idx i})))
                            receiver)
                          (catch Throwable t
                            (undo-and-throw t #(h2o/context-dispose ctx-ptr))))]
    (-> g
        (assoc-in [::g/contexts i] ctx-ptr)
        (assoc-in [::g/wakeup-receivers i] wakeup-receiver))))

(defn- release-context
  [{:keys [worker ctx-ptr wakeup-receiver]} _]
  (if worker
    (retire-wakeup-receiver! worker)
    (h2o/mt-destroy-wakeup-receiver wakeup-receiver))
  (h2o/context-dispose ctx-ptr))

(defn fixed-response-sizes
  "Returns the largest body of a fixed response, `:fixed-body-limit`, and the
  writable bytes of one response slot, `:payload-capacity`, for `config`."
  [config]
  (let [body-limit (min (long (:output-buffer-size config))
                        (long (:fixed-response-max-body-size config)))]
    {:fixed-body-limit body-limit
     :payload-capacity (+ fixed-final/max-header-staging-bytes body-limit)}))

(defn- acquire-response-receiver
  [{::g/keys [arena config contexts] :as g} i]
  (let [slot-count (long (:fixed-response-slots config))
        ;; MemorySegment.setString writes a trailing NUL outside the logical payload.
        storage-capacity (inc (long (:payload-capacity (fixed-response-sizes config))))
        slot-size (+ (h2o/mt-response-slot-data-size) storage-capacity)
        receiver (h2o/mt-create-response-receiver (nth contexts i) slot-count storage-capacity)]
    (when (or (nil? receiver) (mem/null? receiver))
      (throw (ex-info (format "Could not create native response receiver of %d slots of %d bytes"
                              slot-count slot-size)
                      {:fixed-response-slots slot-count :slot-bytes slot-size})))
    (try
      (-> g
          (assoc-in [::g/response-receivers i] receiver)
          (assoc-in [::g/response-slots i]
                    (mapv #(mem/reinterpret (h2o/mt-response-slot-data receiver %) slot-size arena)
                          (range slot-count))))
      (catch Throwable t
        (undo-and-throw t #(h2o/mt-destroy-response-receiver receiver))))))

(defn- release-response-receiver
  [{:keys [worker response-receiver]} _]
  (if worker
    (when-not (destroy-response-receiver! worker) :not-yet)
    (h2o/mt-destroy-response-receiver response-receiver)))

;;; Listener sockets

(defn- close-listener-sockets! [socks]
  (run-all! h2o/socket-close (remove mem/null? socks)))

(defn- open-listener-socket
  "Opens the socket of worker `i` for `listener` and starts its accepts."
  [{::g/keys [listener-claims loops contexts arena entrypoint-hosts active-connection-count_
              worker-connection-counts accepting-workers]}
   i listener ssl-ctx close-callback]
  (let [claim (or (get listener-claims (listen/listener-key listener))
                  (throw (ex-info "Missing TCP listener claim" {:listener listener})))
        fd (socket/dup-fd (listen/resource claim))
        sock-ptr (try
                   (h2o/create-socket-for-loop (nth loops i) fd h2o/H2O_SOCKET_FLAG_DONT_READ)
                   (catch Throwable t
                     (undo-and-throw t #(socket/close-fd! fd))))]
    (try
      (let [accept-ctx (h2o/create-accept-ctx arena (nth contexts i)
                                              (get entrypoint-hosts (:entrypoint listener)) ssl-ctx)
            callback (accept/create-accept-callback accept-ctx close-callback active-connection-count_
                                                    worker-connection-counts accepting-workers i)
            callback-ptr (::accept/accept-cb-ptr callback)]
        (h2o/socket-read-start sock-ptr callback-ptr)
        {:socket sock-ptr :accept-callback callback :accept-callback-ptr callback-ptr})
      (catch Throwable t
        (undo-and-throw t #(h2o/socket-close sock-ptr))))))

(defn- acquire-listeners
  [{::g/keys [config ssl-ctxs active-connection-count_ worker-connection-counts] :as g} i]
  (let [close-callback (accept/create-connection-close-callback active-connection-count_
                                                                worker-connection-counts i)]
    (assoc-in g [::g/listener-sockets i]
              (acquire-each (fn [[listener ssl-ctx]]
                              (open-listener-socket g i listener ssl-ctx close-callback))
                            #(close-listener-sockets! (map :socket %))
                            (map vector (:listeners config) ssl-ctxs)))))

;;; Stop

(defn signal-stop!
  "Stops new work: marks the handler and the workers as stopping, stops the
  key manager and shuts down the request executor."
  [{::g/keys [handler ^AtomicBoolean shutting-down? key-manager workers ^ExecutorService executor]}]
  (when-let [handler-ptr (::h2o/handler-ptr handler)]
    (h2o/handler-set-shutting-down handler-ptr 1))
  (.set shutting-down? true)
  (some-> key-manager tickets/stop-key-manager!)
  (when-let [workers (seq (remove nil? workers))]
    (doseq [worker workers]
      (some-> (:request-dispatch worker) request-dispatch/begin-stop!)
      (callback-dispatch/begin-drain! (:callback-dispatch worker)))
    (evloop/broadcast-wake! workers)
    (let [deadline (+ (System/nanoTime) 1000000000)]
      (doseq [worker workers]
        (some-> (:request-dispatch worker)
                (request-dispatch/await-stop-moves!
                 (max 1 (quot (- deadline (System/nanoTime)) 1000000)))))))
  (.shutdown executor))

(defn interrupt-handlers!
  "Interrupts the request handlers that still run."
  [{::g/keys [^ExecutorService executor workers]} ^long timeout-ns]
  (doseq [worker workers]
    (some-> (:request-dispatch worker) request-dispatch/interrupt-remaining!))
  (.shutdownNow executor)
  (when-not (.awaitTermination executor timeout-ns TimeUnit/NANOSECONDS)
    (println "Virtual thread request executor pool did not shutdown cleanly")))

(defn join-workers!
  "Wakes the worker threads and waits up to `timeout-ns` for them to end."
  [{::g/keys [workers]} ^long timeout-ns]
  (let [workers (remove nil? workers)
        deadline (+ (System/nanoTime) timeout-ns)]
    (evloop/broadcast-wake! workers)
    (doseq [worker workers
            :let [remaining (- deadline (System/nanoTime))]
            :when (pos? remaining)]
      (.join ^Thread (:thread worker) (max 1 (quot remaining 1000000))))))

;;; The table

(def table
  "The acquire and the release effect of each resource kind except
  `:thread`, which [[ol.busker.generation/node-effects]] adds, because the
  worker loop is in that namespace.

  - `:acquire`: `(fn [g worker-idx])`, returns `g` with the new handles.
    `worker-idx` is nil for a generation node.
  - `:release`: `(fn [x timeout-ns])`, returns `:not-yet` while the resource
    is in use, else anything. `x` is the generation map `g`, or for a worker
    node the [[worker-handles]] of that worker."
  {:socket-claims {:acquire (fn [g _] (acquire-claims g false))
                   :release (fn [g _] (release-claims! (::g/listener-claims g) false))}
   :udp-claims {:acquire (fn [g _] (acquire-claims g true))
                :release (fn [g _] (release-claims! (::g/listener-claims g) true))}
   :close-executor {:acquire (fn [g _]
                               (assoc g ::g/close-callback-executor
                                      (Executors/newVirtualThreadPerTaskExecutor)))
                    :release (fn [g timeout-ns]
                               (shutdown-executor! (::g/close-callback-executor g) timeout-ns
                                                   "Virtual thread close callback executor did not shutdown cleanly"))}
   :arena {:acquire (fn [g _] (assoc g ::g/arena (mem/shared-arena)))
           :release (fn [g _] (.close ^AutoCloseable (::g/arena g)))}
   :config {:acquire acquire-config
            :release (fn [g _] (h2o/config-dispose (::g/config-ptr g)))}
   :loops {:acquire acquire-loops
           :release (fn [g _] (run-all! h2o/evloop-destroy (::g/loops g)))}
   :ssl-ctxs {:acquire acquire-ssl-ctxs
              :release (fn [g _] (free-ssl-ctxs! (::g/ssl-ctxs g)))}
   :ticket-manager {:acquire acquire-ticket-manager
                    :release (fn [g _] (some-> (::g/native-ticket-mgr g) h2o/ticket-manager-destroy))}
   :h3-contexts {:acquire acquire-h3-contexts
                 :release (fn [g _] (free-http3-contexts! (::g/http3-contexts g)))}
   :context {:acquire acquire-context
             :release release-context}
   :response-receiver {:acquire acquire-response-receiver
                       :release release-response-receiver}
   :pool {:acquire (fn [g i]
                     (assoc-in g [::g/stream-pools i]
                               (bp/create-stream-pool (:output-buffer-size (::g/config g)))))
          :release (fn [{:keys [pool]} _] (when-not (bp/retire! pool) :not-yet))}
   :listeners {:acquire acquire-listeners
               :release (fn [{:keys [listener-socks]} _] (close-listener-sockets! listener-socks))}
   :h3 {:acquire acquire-h3
        :release (fn [{:keys [http3-ctxs]} _] (run-all! h2o/http3-free-worker-ctx http3-ctxs))}
   :callback-dispatch {:acquire (fn [g i]
                                  (assoc-in g [::g/callback-dispatches i]
                                            (callback-dispatch/create (::g/arena g))))
                       :release (fn [{:keys [callback-dispatch]} _]
                                  (callback-dispatch/finish! callback-dispatch))}})
