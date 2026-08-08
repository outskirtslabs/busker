(ns ^:no-doc ol.busker.generation
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.config :as config]
   [ol.busker.evloop :as evloop]
   [ol.busker.internal.protocols :as p]
   [ol.busker.listen :as listen]
   [ol.busker.native :as h2o]
   [ol.busker.native.socket :as socket]
   [ol.busker.request :as request]
   [ol.busker.tickets :as tickets]
   [ol.clave.certificate :as clave-certificate]
   [taoensso.trove :as trove])
  (:import
   [java.io File]
   [java.security.cert CertificateFactory X509Certificate]
   [java.util Base64]
   [java.util.concurrent ExecutorService Executors TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong AtomicReference]))

(set! *warn-on-reflection* true)

(defn- new-request-executor ^ExecutorService []
  (Executors/newVirtualThreadPerTaskExecutor))

(defn- new-close-callback-executor ^ExecutorService []
  (Executors/newVirtualThreadPerTaskExecutor))

(defonce ^:private failed-retirements_ (atom []))

(defn- retain-failed-retirement!
  [generation phase-atom error]
  (trove/log! {:level :error
               :id    ::callback-retirement-failed
               :ex    error
               :data  {:generation-id (::generation-id generation)}})
  (swap! failed-retirements_ conj generation)
  (reset! phase-atom :retirement-failed))

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
  (when-let [worker (evloop/get-current-worker)]
    (when-let [[req _] (callback-dispatch/entry (:callback-dispatch worker)
                                                module-id request-seq)]
      (f req))))

(defn evloop-msg-processor
  [op args]
  (case op
    :h2o/proceed-request
    (let [[module-id request-seq] args]
      (with-live-request module-id request-seq
        (fn [req]
          (h2o/proceed-req (:req (:req-ctx req))))))

    :h2o/sendvec
    (let [[module-id request-seq send-vecs] args]
      (with-live-request module-id request-seq
        (fn [_]
          (send-vecs))))

    :h2o/send-informational
    (let [[module-id request-seq send-fn] args]
      (with-live-request module-id request-seq send-fn))

    :h2o/start-response
    (let [[module-id request-seq start-fn] args]
      (with-live-request module-id request-seq start-fn))
    nil))

(defn- create-connection-close-callback
  [active-connection-count_]
  (let [cb (fn [_]
             (.decrementAndGet ^AtomicLong active-connection-count_)
             (h2o/conn-limit-release))]
    {::connection-close-cb cb
     ::connection-close-cb-ptr
     (mem/serialize cb [::ffi/fn [::mem/pointer] ::mem/void])}))

(defn- create-accept-callback
  [accept-ctx-ptr on-close-callback active-connection-count_]
  (let [accept-cb (fn [listener-ptr err-ptr]
                    (when-not (mem/null? err-ptr)
                      nil)
                    (let [sock-ptr (h2o/evloop-socket-accept listener-ptr)]
                      (when-not (mem/null? sock-ptr)
                        #_{:clj-kondo/ignore [:type-mismatch]}
                        (if (pos? (h2o/conn-limit-try-acquire))
                          (do
                            (.incrementAndGet ^AtomicLong active-connection-count_)
                            (h2o/socket-set-on-close sock-ptr on-close-callback (mem/as-segment 0))
                            (h2o/h2o-accept accept-ctx-ptr sock-ptr))
                          (h2o/socket-close sock-ptr)))))]
    {::accept-cb accept-cb
     ::accept-cb-ptr
     (mem/serialize accept-cb
                    [::ffi/fn [::mem/pointer ::mem/c-string] ::mem/void])}))

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

(defn- create-server-config
  [arena executor close-callback-executor ring-handler config]
  (let [config-ptr (mem/alloc (h2o/globalconf-size) arena)
        flat-config-ptr (mem/serialize (config->flat-globalconf-t config)
                                       ::h2o/clj-h2o-flat-globalconf-t
                                       arena)
        config-ptr (do
                     (h2o/create-global-conf config-ptr flat-config-ptr)
                     config-ptr)
        hostconf-ptr (with-open [arena2 (mem/confined-arena)]
                       (h2o/config-register-host config-ptr
                                                 (h2o/str->iovec "default" arena2)
                                                 65535))
        on-request-cb (partial request/on-request executor
                               (close-callback-dispatcher close-callback-executor)
                               config
                               ring-handler)
        on-request-cleanup-cb request/on-request-cleanup
        handler (h2o/create-handler hostconf-ptr
                                    on-request-cb
                                    on-request-cleanup-cb
                                    flat-config-ptr
                                    arena)]
    {::config-ptr config-ptr
     ::hostconf-ptr hostconf-ptr
     ::on-request-cb on-request-cb
     ::on-request-cleanup-cb on-request-cleanup-cb
     ::handler handler}))

(defn- update-listener-state!
  [listener-socks accept-callbacks max-connections]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (let [current-conns (h2o/conn-limit-current)
        should-accept? (or (zero? max-connections)
                           (< current-conns max-connections))]
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
  [{:keys [listener-socks loop-ptr ctx-ptr http3-ctxs]}]
  (doseq [listener-idx (range (count listener-socks))]
    (let [sock-ptr (nth listener-socks listener-idx)]
      (when (and (not (mem/null? sock-ptr))
                 #_{:clj-kondo/ignore [:type-mismatch]}
                 (not (zero? (h2o/socket-reading? sock-ptr))))
        (h2o/socket-read-stop sock-ptr))))
  (doseq [http3-ctx http3-ctxs]
    (when (and http3-ctx (not (mem/null? http3-ctx)))
      (h2o/http3-stop-accepting http3-ctx)))
  (h2o/evloop-run loop-ptr 0)
  (doseq [listener-idx (range (count listener-socks))]
    (let [sock-ptr (nth listener-socks listener-idx)]
      (when-not (mem/null? sock-ptr)
        (h2o/socket-close sock-ptr))))
  (h2o/context-request-shutdown ctx-ptr))

(defn- has-active-http3-connections?
  [http3-ctx]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (and http3-ctx
       (not (mem/null? http3-ctx))
       (pos? (h2o/http3-num-connections http3-ctx))))

(defn- all-connections-drained?
  [ctx-ptr http3-ctxs active-connection-count_]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (and (zero? (h2o/context-get-active-conns ctx-ptr))
       (zero? (h2o/context-get-shutdown-conns ctx-ptr))
       (zero? (.get ^AtomicLong active-connection-count_))
       (not-any? has-active-http3-connections? http3-ctxs)))

(defn- ready-to-dispose?
  [{:keys [context-disposed? shutdown-initiated? receiver-destroyed?]}
   {:keys [ctx-ptr http3-ctxs active-connection-count_]} worker]
  (and (not context-disposed?)
       shutdown-initiated?
       receiver-destroyed?
       (all-connections-drained? ctx-ptr http3-ctxs active-connection-count_)
       (callback-dispatch/no-live-entries? (:callback-dispatch worker))))

(defn- check-and-initiate-shutdown!
  [loop-state {:keys [shutting-down?] :as state}]
  (let [should-shutdown? (.get ^AtomicBoolean shutting-down?)
        already? (true? (:shutdown-initiated? loop-state))]
    (when (and should-shutdown? (not already?))
      (initiate-worker-shutdown! (assoc state :shutdown-initiated? true))
      (when-let [remaining (::stop-accepting-remaining_ state)]
        (when (zero? (.decrementAndGet ^AtomicLong remaining))
          (deliver (::stopped-accepting_ state) true))))
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

(defn- dispose-context-if-ready
  [loop-state worker {:keys [ctx-ptr http3-ctxs] :as state}]
  (if (ready-to-dispose? loop-state state worker)
    (do
      (doseq [http3-ctx http3-ctxs]
        (when (and http3-ctx (not (mem/null? http3-ctx)))
          (h2o/http3-free-worker-ctx http3-ctx)))
      (h2o/context-dispose ctx-ptr)
      (p/send-msg worker evloop/stop-msg)
      (assoc loop-state :context-disposed? true))
    loop-state))

(defn- worker-loop
  [worker loop-state
   {:keys [loop-ptr ctx-ptr listener-socks accept-callbacks max-connections] :as state}]
  (let [loop-state (-> loop-state
                       (check-and-initiate-shutdown! state)
                       (update-receiver-destruction worker)
                       (dispose-context-if-ready worker state))
        disposed? (true? (:context-disposed? loop-state))
        shutting? (true? (:shutdown-initiated? loop-state))]
    (if disposed?
      (h2o/evloop-run loop-ptr 0)
      (let [now (h2o/evloop-now loop-ptr)
            base-wait (h2o/cleanup-thread now ctx-ptr)
            wait-ms (cond
                      (callback-dispatch/pending-response-work?
                       (:callback-dispatch worker)) 5
                      shutting? 10
                      :else base-wait)]
        (when-not shutting?
          (update-listener-state! listener-socks accept-callbacks max-connections))
        (h2o/evloop-run loop-ptr (if (pos? (p/count-msgs worker)) 0 wait-ms))))
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
  [listeners config config-ptr ticket-mgr-ptr ticket-lifetime-seconds tls-lookup-callback-ptr]
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
                         _ (when encrypt-ticket
                             (h2o/ptls-ctx-set-tickets ptls-ctx
                                                       encrypt-ticket
                                                       ticket-lifetime-seconds
                                                       8192))
                         quicly-ctx (h2o/http3-create-quicly-ctx ptls-ctx config-ptr)]
                     (when (or (nil? quicly-ctx) (mem/null? quicly-ctx))
                       (h2o/http3-free-ptls-ctx ptls-ctx)
                       (throw (ex-info "Failed to create quicly context for HTTP/3"
                                       {:listener-index idx})))
                     (when (and ticket-mgr-ptr (not (mem/null? ticket-mgr-ptr)))
                       (h2o/ticket-manager-set-quic-tag ticket-mgr-ptr quicly-ctx))
                     [idx {:ptls-ctx ptls-ctx
                           :quicly-ctx quicly-ctx
                           :encrypt-ticket encrypt-ticket}]))))
             listeners))
      (catch Throwable t
        (wrap-stage-error "Failed to initialize HTTP/3 TLS state"
                          :tls-startup
                          {}
                          t)))))

(defn- create-http3-worker-contexts
  [generation-id n-workers listeners loops contexts config-ptr http3-contexts listener-claims]
  (let [hosts-ptr (h2o/globalconf-get-hosts config-ptr)]
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
                               hosts-ptr
                               (listen/resource listener-claim)
                               generation-id
                               (int thread-idx))]
                (when (or (nil? http3-ctx) (mem/null? http3-ctx))
                  (throw (ex-info "Failed to create HTTP/3 worker context"
                                  {:listener-index listener-idx
                                   :thread-idx thread-idx
                                   :port (:port listener)})))
                http3-ctx)))))))))

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
     ::stop-accepting-remaining_ (AtomicLong. n-workers)
     ::stopped-accepting_ (promise)
     ::n-workers n-workers
     ::max-connections max-connections}))

(defn- init-core-state
  [{::keys [ring-handler config executor close-callback-executor n-workers max-connections
            active-connection-count_]
    :as state}]
  (let [arena (mem/shared-arena)
        _ (h2o/conn-limit-set-max max-connections)
        server-config (create-server-config arena executor close-callback-executor ring-handler config)
        config-ptr (::config-ptr server-config)
        loops (h2o/create-loops n-workers)
        contexts (h2o/create-contexts arena loops config-ptr)
        wakeup-receivers (vec (for [ctx-ptr contexts]
                                (h2o/mt-create-wakeup-receiver ctx-ptr)))
        on-close-callback (create-connection-close-callback active-connection-count_)]
    (assoc state
           ::arena arena
           ::server-config server-config
           ::config-ptr config-ptr
           ::handler (::handler server-config)
           ::loops loops
           ::contexts contexts
           ::wakeup-receivers wakeup-receivers
           ::on-close-callback on-close-callback)))

(defn- init-tls-http3-state
  [{::keys [generation-id listener-runtimes config config-ptr n-workers loops contexts
            tls-lookup-callback listener-claims memory-ticket-service]
    :as state}]
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
                (tickets/start-key-manager!))))]
    (when native-ticket-mgr
      (doseq [ssl-ctx-ptr ssl-ctx-ptrs]
        (when ssl-ctx-ptr
          (h2o/ssl-ctx-set-tickets ssl-ctx-ptr native-ticket-mgr 8192))))
    (let [http3-contexts (create-http3-contexts listeners
                                                config
                                                config-ptr
                                                native-ticket-mgr
                                                session-ticket-lifetime-seconds
                                                tls-lookup-callback-ptr)
          http3-worker-contexts (create-http3-worker-contexts generation-id
                                                              n-workers
                                                              listeners
                                                              loops
                                                              contexts
                                                              config-ptr
                                                              http3-contexts
                                                              listener-claims)]
      (assoc state
             ::ticket-store standalone-ticket-store
             ::native-ticket-mgr native-ticket-mgr
             ::key-manager key-manager
             ::http3-contexts http3-contexts
             ::http3-worker-contexts http3-worker-contexts))))

(defn- init-single-listener-state
  [listener listener-claims n-workers loops contexts arena config-ptr on-close-callback
   tls-lookup-callback-ptr active-connection-count_]
  (let [ssl-ctx-ptr (create-ssl-context listener tls-lookup-callback-ptr)
        listener-claim (or (get listener-claims (listen/listener-key listener))
                           (throw (ex-info "Missing TCP listener claim"
                                           {:listener listener})))
        listener-fd (listen/resource listener-claim)
        dup-fds (socket/dup-for-threads listener-fd n-workers)
        thread-states
        (vec
         (for [thread-idx (range n-workers)]
           (let [accept-ctx (h2o/create-accept-ctx arena
                                                   (nth contexts thread-idx)
                                                   config-ptr
                                                   ssl-ctx-ptr)
                 accept-callback (create-accept-callback
                                  accept-ctx
                                  (::connection-close-cb-ptr on-close-callback)
                                  active-connection-count_)
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
  [{::keys [n-workers loops contexts arena config-ptr on-close-callback
            listener-claims
            tls-lookup-callback
            active-connection-count_]
    :as state}]
  (let [tls-lookup-callback-ptr (:callback-ptr tls-lookup-callback)]
    (-> state
        ::config
        :entrypoints
        (->> (into [] (mapcat config/entrypoint->listeners))
             (mapv #(init-single-listener-state %
                                                listener-claims
                                                n-workers
                                                loops
                                                contexts
                                                arena
                                                config-ptr
                                                on-close-callback
                                                tls-lookup-callback-ptr
                                                active-connection-count_)))
        (->> (assoc state ::listener-runtimes)))))

(defn- init-worker-state
  [{::keys [n-workers loops contexts listener-runtimes http3-worker-contexts
            max-connections shutting-down? message-handler wakeup-receivers arena
            stop-accepting-remaining_ stopped-accepting_ active-connection-count_]
    :as state}]
  (let [workers
        (vec
         (for [thread-idx (range n-workers)]
           (let [loop-ptr (nth loops thread-idx)
                 ctx-ptr (nth contexts thread-idx)
                 callback-dispatch (callback-dispatch/create arena)
                 thread-listener-states
                 (mapv #(nth (:thread-states %) thread-idx) listener-runtimes)
                 listener-socks-for-thread (mapv :socket thread-listener-states)
                 accept-callbacks-for-thread (mapv :accept-callback-ptr thread-listener-states)
                 http3-ctxs-for-thread (nth http3-worker-contexts thread-idx)]
             (evloop/start-worker!
              (fn [worker loop-state]
                (worker-loop worker
                             loop-state
                             {:listener-socks listener-socks-for-thread
                              :accept-callbacks accept-callbacks-for-thread
                              :loop-ptr loop-ptr
                              :ctx-ptr ctx-ptr
                              :http3-ctxs http3-ctxs-for-thread
                              :max-connections max-connections
                              :shutting-down? shutting-down?
                              :active-connection-count_ active-connection-count_
                              ::stop-accepting-remaining_ stop-accepting-remaining_
                              ::stopped-accepting_ stopped-accepting_}))
              message-handler
              (nth wakeup-receivers thread-idx)
              :callback-dispatch callback-dispatch))))]
    (assoc state ::workers workers)))

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
          (callback-dispatch/begin-drain! (:callback-dispatch worker)))
        (evloop/broadcast-wake! (::workers generation)))
      (when-let [^ExecutorService executor (::executor generation)]
        (.shutdown executor))
      (reset! phase :stopping)))
  generation)

(defn await-stop-accepting!
  [{::keys [stopped-accepting_] :as _generation}]
  @stopped-accepting_
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
         (when (= :stopping @phase-atom)
           (when executor
             (when-not (.awaitTermination executor timeout timeunit)
               (.shutdownNow executor)
               (when-not (.awaitTermination executor timeout timeunit)
                 (println "Virtual thread request executor pool did not shutdown cleanly"))))
           (when (seq (::workers generation))
             (evloop/broadcast-wake! (::workers generation)))
           (doseq [[worker wr] (map vector (::workers generation) (::wakeup-receivers generation))]
             (when (and wr (not (mem/null? wr)))
               (h2o/mt-destroy-wakeup-receiver wr))
             (.set ^AtomicReference (:wakeup-receiver_ worker) nil)))
         (reset! phase-atom :retiring)
         (let [{:keys [joined? error]}
               (try
                 (when (seq (::workers generation))
                   (evloop/join-all! (::workers generation)))
                 {:joined? (not-any? (fn [worker]
                                       (.isAlive ^Thread (:thread worker)))
                                     (::workers generation))}
                 (catch InterruptedException t
                   (.interrupt (Thread/currentThread))
                   {:joined? false :error t})
                 (catch Throwable t
                   {:joined? false :error t}))]
           (if-not joined?
             (retain-failed-retirement!
              generation phase-atom
              (or error
                  (ex-info "Callback retirement could not establish worker join" {})))
             (do
               (doseq [worker (::workers generation)]
                 (callback-dispatch/finish! (:callback-dispatch worker)))
               (when close-callback-executor
                 (.shutdown close-callback-executor)
                 (when-not (.awaitTermination close-callback-executor timeout timeunit)
                   (.shutdownNow close-callback-executor)
                   (when-not (.awaitTermination close-callback-executor timeout timeunit)
                     (println "Virtual thread close callback executor did not shutdown cleanly"))))
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
               (when-let [buffer-pool (-> generation ::config :buffer-pool)]
                 (bp/dispose buffer-pool))
               (release-listener-claims! (::listener-claims generation))
               (when-let [arena (::arena generation)]
                 (.close ^java.lang.AutoCloseable arena))
               (remove-failed-retirement! generation)
               (reset! phase-atom :stopped))))))
     nil)))

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
         (cond-> (-> state
                     init-tls-lookup-state
                     init-core-state
                     init-listener-state
                     init-tls-http3-state
                     init-worker-state
                     finalize-generation-state)
           activate-http3-transports? activate-http3-transports!))
       (catch Throwable t
         (try
           (if-let [state @cleanup-state_]
             (stop! state)
             (release-listener-claims! listener-claims))
           (catch Throwable _
             (release-listener-claims! listener-claims)))
         (throw t))))))