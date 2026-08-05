(ns ^:no-doc ol.busker.config
  "Internal configuration parsing, validation, normalization, and compilation."
  (:require
   [clojure.walk :as walk]
   [clojure.string :as str]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.specs :as specs]
   [ol.clave.storage :as storage])
  (:import
   [java.io File]))

(defn port-string?
  [s]
  (when (string? s)
    (when (re-matches #"\d+" s)
      (let [port (Long/parseLong s)]
        (<= 1 port 65535)))))

(defn parse-port
  [s]
  (when (port-string? s)
    (Long/parseLong s)))

(defn parse-tcp-bind-address
  [s]
  (when (string? s)
    (cond
      (str/starts-with? s ":")
      (when-let [port (parse-port (subs s 1))]
        {:address nil :port port})

      (str/starts-with? s "[")
      (when-let [[_ host port] (re-matches #"\[([^\]]+)\]:(\d+)" s)]
        (when-let [port (parse-port port)]
          {:address host :port port}))

      :else
      (let [idx (.lastIndexOf ^String s ":")]
        (when (pos? idx)
          (let [host (subs s 0 idx)
                port (subs s (inc idx))]
            (when (and (not (str/blank? host))
                       (not (str/includes? host ":")))
              (when-let [port (parse-port port)]
                {:address host :port port}))))))))

(defn parse-unix-bind-address
  [s]
  (when (string? s)
    (when (or (re-matches #"unix:/.+" s)
              (re-matches #"unix:@.+" s))
      {:unix (subs s 5)})))

(defn unix-bind-address?
  [s]
  (boolean (parse-unix-bind-address s)))

(defn parse-bind-address
  [s]
  (or (parse-tcp-bind-address s)
      (parse-unix-bind-address s)))

(defn bind-address?
  [s]
  (some? (parse-bind-address s)))

(defn parse-bind
  [bind]
  (let [binds (cond
                (string? bind) [bind]
                (vector? bind) bind
                :else (throw (ex-info "Invalid :bind value."
                                      {:bind bind})))]
    (when (empty? binds)
      (throw (ex-info "Bind must include at least one address."
                      {:bind bind})))
    (mapv (fn [addr]
            (or (parse-bind-address addr)
                (throw (ex-info "Invalid bind address."
                                {:bind addr}))))
          binds)))

(defn- qualified-symbol-ref?
  [v]
  (and (symbol? v)
       (namespace v)
       (name v)))

(defn- callable-value?
  [v]
  (and (ifn? v)
       (not (vector? v))
       (not (map? v))
       (not (set? v))
       (not (symbol? v))
       (not (keyword? v))
       (not (string? v))))

(defn- deep-merge
  [& values]
  (if (every? map? values)
    (apply merge-with deep-merge values)
    (last values)))

(defn- bind-values
  [bind]
  (cond
    (string? bind) [bind]
    (vector? bind) bind
    :else nil))

(defn- config-error
  [error msg data]
  {:msg msg
   :error error
   :data data})

(def ^:private supported-session-ticket-keys
  #{:disabled?
    :persistence
    :max-keys
    :lifetime-seconds})

(defn- entrypoint-error
  [entrypoint-id error msg data]
  {:msg msg
   :error error
   :data (merge {:entrypoint entrypoint-id} data)})

(defn- maybe-add-error
  [errors pred error-fn]
  (if pred
    (conj errors (error-fn))
    errors))

(defn validate-bind-address
  [errors entrypoint-id idx addr]
  (cond
    (not (string? addr))
    (conj errors (entrypoint-error entrypoint-id
                                   ::entrypoint-bind-address-type
                                   "Bind address must be a string."
                                   {:bind addr
                                    :index idx
                                    :type (.getName (class addr))}))

    (str/blank? addr)
    (conj errors (entrypoint-error entrypoint-id
                                   ::entrypoint-bind-address-blank
                                   "Bind address must not be blank."
                                   {:bind addr
                                    :index idx}))

    (bind-address? addr)
    errors

    :else
    (conj errors (entrypoint-error entrypoint-id
                                   ::entrypoint-bind-address-invalid
                                   "Bind address must be a valid address string."
                                   {:bind addr
                                    :index idx}))))

(defn validate-bind
  [errors entrypoint-id entrypoint]
  (let [bind (:bind entrypoint)
        binds (bind-values bind)]
    (cond
      (nil? binds)
      (conj errors (entrypoint-error entrypoint-id
                                     ::entrypoint-bind-type
                                     "Bind must be a string or vector of strings."
                                     {:bind bind}))

      (empty? binds)
      (conj errors (entrypoint-error entrypoint-id
                                     ::entrypoint-bind-empty
                                     "Bind must include at least one address."
                                     {:bind bind}))

      :else
      (reduce-kv (fn [errs idx addr]
                   (validate-bind-address errs entrypoint-id idx addr))
                 errors
                 (vec binds)))))

(defn- wildcard-host?
  [host]
  (or (nil? host)
      (= host "0.0.0.0")
      (= host "::")))

(defn- bind-conflict?
  [a b]
  (cond
    (and (:unix a) (:unix b))
    (= (:unix a) (:unix b))

    (or (:unix a) (:unix b))
    false

    :else
    (and (= (:port a) (:port b))
         (or (= (:address a) (:address b))
             (wildcard-host? (:address a))
             (wildcard-host? (:address b))))))

(defn- validate-entrypoint-conflicts
  [entrypoints]
  (let [parsed (mapcat (fn [[entrypoint-id entrypoint]]
                         (map (fn [bind]
                                {:entrypoint entrypoint-id
                                 :bind bind
                                 :parsed (parse-bind-address bind)})
                              (or (bind-values (:bind entrypoint)) [])))
                       entrypoints)]
    (loop [errors []
           [current & remaining] parsed]
      (if-not current
        errors
        (let [conflicts (filter #(bind-conflict? (:parsed current)
                                                 (:parsed %))
                                remaining)
              errors (into errors
                           (map (fn [conflict]
                                  (entrypoint-error
                                   (:entrypoint current)
                                   ::entrypoint-bind-conflict
                                   "Entrypoints must not bind conflicting addresses."
                                   {:bind (:bind current)
                                    :conflict-entrypoint (:entrypoint conflict)
                                    :conflict-bind (:bind conflict)}))
                                conflicts))]
          (recur errors remaining))))))

(defn- validate-tls-load-entry
  [errors entry]
  (let [{:keys [type path cert-file key-file]} entry]
    (case type
      :folder
      (-> errors
          (maybe-add-error
           (not (string? path))
           #(config-error ::tls-folder-path-missing
                          "TLS folder entries require :path."
                          {:entry entry}))
          (maybe-add-error
           (and (string? path)
                (not (.isDirectory (File. ^String path))))
           #(config-error ::tls-folder-not-found
                          "TLS certificate folder was not found."
                          {:path path})))

      :pem
      (-> errors
          (maybe-add-error
           (not (string? cert-file))
           #(config-error ::tls-missing-cert-file
                          "TLS PEM entries require :cert-file."
                          {:entry entry}))
          (maybe-add-error
           (not (string? key-file))
           #(config-error ::tls-missing-key-file
                          "TLS PEM entries require :key-file."
                          {:entry entry}))
          (maybe-add-error
           (and (string? cert-file)
                (not (.exists (File. ^String cert-file))))
           #(config-error ::tls-cert-file-not-found
                          "TLS certificate file not found."
                          {:cert-file cert-file}))
          (maybe-add-error
           (and (string? key-file)
                (not (.exists (File. ^String key-file))))
           #(config-error ::tls-key-file-not-found
                          "TLS private key file not found."
                          {:key-file key-file})))

      (conj errors
            (config-error ::tls-load-type-invalid
                          "TLS certificate load entries must use :type :folder or :pem."
                          {:entry entry})))))

(defn- global-certificates-present?
  [config]
  (let [tls (:tls config)]
    (or (seq (get-in tls [:certificates :load]))
        (seq (get-in tls [:certificates :manage])))))

(defn validate-entrypoint
  [errors entrypoint-id entrypoint config]
  (let [errors (or errors [])
        errors (validate-bind errors entrypoint-id entrypoint)
        binds (bind-values (:bind entrypoint))
        http1? (:http1? entrypoint)
        http2? (:http2? entrypoint)
        http3? (:http3? entrypoint)
        tls-enabled? (map? (:tls entrypoint))
        unix? (boolean (some unix-bind-address? binds))]
    (-> errors
        (maybe-add-error
         (and http3? unix?)
         #(entrypoint-error entrypoint-id
                            ::entrypoint-http3-unix
                            "HTTP/3 is not supported for unix domain sockets."
                            {:bind (vec binds)}))
        (maybe-add-error
         (and http3? (not tls-enabled?))
         #(entrypoint-error entrypoint-id
                            ::entrypoint-http3-requires-tls
                            "HTTP/3 requires TLS on the entrypoint."
                            {}))
        (maybe-add-error
         (not (or http1? http2? http3?))
         #(entrypoint-error entrypoint-id
                            ::entrypoint-no-protocols
                            "Entrypoint must enable at least one HTTP protocol."
                            {:http1? http1?
                             :http2? http2?
                             :http3? http3?}))
        (maybe-add-error
         (and tls-enabled?
              (not (global-certificates-present? config)))
         #(entrypoint-error entrypoint-id
                            ::tls-missing-certificates
                            "TLS entrypoints require top-level :tls :certificates configuration."
                            {})))))

(defn apply-entrypoint-defaults
  [entrypoint]
  (let [http3-overridden? (contains? entrypoint :http3?)
        entrypoint (merge (:default specs/entrypoint) entrypoint)]
    (if http3-overridden?
      entrypoint
      (assoc entrypoint :http3? (map? (:tls entrypoint))))))

(defn- base-config-with-defaults
  [user-config]
  (let [config (merge specs/default-config (dissoc user-config :tls))
        config (assoc config :tls (deep-merge (:tls specs/default-config)
                                              (or (:tls user-config) {})))
        config (update config :entrypoints
                       (fn [entrypoints]
                         (when entrypoints
                           (into (empty entrypoints)
                                 (map (fn [[entrypoint-id entrypoint]]
                                        [entrypoint-id
                                         (apply-entrypoint-defaults entrypoint)]))
                                 entrypoints))))]
    config))

(defn apply-config-defaults
  [user-config]
  (let [config (base-config-with-defaults user-config)]
    (cond-> config
      (not (contains? user-config :buffer-pool))
      (assoc :buffer-pool (bp/make-bytebuffer-pool {})))))

(def ^:private callable-placeholder
  ::callable)

(def ^:private storage-placeholder
  ::storage)

(defn validate-config
  [{:keys [entrypoints dispatch tls] :as config}]
  (let [missing-entrypoints? (or (nil? entrypoints) (empty? entrypoints))
        missing-dispatch? (or (nil? dispatch) (empty? dispatch))
        session-ticket-config (:session-tickets tls)
        session-ticket-keys (if (map? session-ticket-config)
                              (-> session-ticket-config keys set)
                              #{})
        unsupported-session-ticket-keys
        (->> session-ticket-keys
             (remove supported-session-ticket-keys)
             sort
             vec)
        session-ticket-persistence (when (map? session-ticket-config)
                                     (:persistence session-ticket-config))
        spec-errors (when (and (not missing-entrypoints?)
                               (not missing-dispatch?)
                               (not (specs/valid-config? config)))
                      [(config-error
                        ::config-spec-invalid
                        "Config failed spec validation."
                        {:problems (-> config
                                       specs/explain-config
                                       :clojure.spec.alpha/problems)})])
        dispatch-errors
        (mapcat (fn [{:keys [entrypoints] :as dispatcher}]
                  (keep (fn [entrypoint-id]
                          (when-not (contains? (:entrypoints config) entrypoint-id)
                            (config-error
                             ::dispatch-entrypoint-missing
                             "Dispatcher references an unknown entrypoint."
                             {:dispatcher dispatcher
                              :entrypoint entrypoint-id})))
                        entrypoints))
                (or dispatch []))
        tls-errors
        (-> []
            (maybe-add-error
             (seq unsupported-session-ticket-keys)
             #(config-error
               ::session-ticket-config-invalid
               "Session ticket config contains unsupported keys."
               {:keys unsupported-session-ticket-keys}))
            (maybe-add-error
             (and (= :storage session-ticket-persistence)
                  (nil? (:storage tls)))
             #(config-error
               ::session-ticket-storage-missing
               "Session ticket storage persistence requires top-level :tls :storage."
               {}))
            (into (mapcat (partial validate-tls-load-entry [])
                          (get-in tls [:certificates :load])))
            (maybe-add-error
             (and (seq (get-in tls [:certificates :manage]))
                  (not (seq (:issuers tls))))
             #(config-error
               ::tls-manage-requires-issuers
               "Managed certificates require top-level :tls :issuers."
               {:manage (vec (get-in tls [:certificates :manage]))})))]
    (cond-> (vec spec-errors)
      missing-entrypoints?
      (conj (config-error
             ::config-no-entrypoints
             "Config must include at least one entrypoint."
             {}))

      missing-dispatch?
      (conj (config-error
             ::config-no-dispatch
             "Config must include at least one dispatcher."
             {}))

      (and (map? entrypoints) (seq entrypoints))
      (into (mapcat (fn [[entrypoint-id entrypoint]]
                      (validate-entrypoint [] entrypoint-id entrypoint config))
                    entrypoints))

      (and (map? entrypoints) (seq entrypoints))
      (into (validate-entrypoint-conflicts entrypoints))

      (seq dispatch-errors)
      (into dispatch-errors)

      (seq tls-errors)
      (into tls-errors))))

(defn- resolve-function-ref
  [value kind]
  (cond
    (nil? value)
    nil

    (callable-value? value)
    value

    (qualified-symbol-ref? value)
    (let [resolved (requiring-resolve value)]
      (when-not (ifn? resolved)
        (throw (ex-info "Resolved value is not callable."
                        {:kind kind
                         :value value})))
      resolved)

    :else
    (throw (ex-info "Config value must be a function or qualified symbol."
                    {:kind kind
                     :value value}))))

(defn- resolve-storage
  [value]
  (cond
    (nil? value)
    nil

    (satisfies? storage/Storage value)
    value

    (map? value)
    (let [factory (resolve-function-ref (:factory value) :storage-factory)
          initial-instance (try
                             (factory value)
                             (catch Throwable _
                               ::retry))
          instance (if (and (= ::retry initial-instance)
                            (contains? value :root))
                     (factory (:root value))
                     (if (and (nil? initial-instance)
                              (contains? value :root))
                       (factory (:root value))
                       initial-instance))]
      (when-not (satisfies? storage/Storage instance)
        (throw (ex-info "Storage factory must return a Storage implementation."
                        {:value value
                         :instance instance})))
      instance)

    :else
    (throw (ex-info "Invalid :tls :storage value."
                    {:value value}))))

(defn- compile-middleware-entry
  [entry]
  (cond
    (vector? entry)
    (let [[wrap-ref opts] entry
          wrap-fn (resolve-function-ref wrap-ref :middleware)]
      (fn [handler]
        (if (some? opts)
          (wrap-fn handler opts)
          (wrap-fn handler))))

    (or (callable-value? entry) (qualified-symbol-ref? entry))
    (let [wrap-fn (resolve-function-ref entry :middleware)]
      (fn [handler]
        (wrap-fn handler)))

    :else
    (throw (ex-info "Invalid middleware entry."
                    {:entry entry}))))

(defn- compile-dispatcher
  [dispatcher]
  (let [match-fn (resolve-function-ref (:match dispatcher) :match)
        base-handler (or (resolve-function-ref (:handler dispatcher) :handler)
                         (fn [_] {:status 200}))
        middleware (map compile-middleware-entry (:middleware dispatcher))
        handler (reduce (fn [handler middleware-fn]
                          (middleware-fn handler))
                        base-handler
                        middleware)]
    (assoc dispatcher
           :match-fn match-fn
           :compiled-handler handler)))

(defn entrypoint->listeners
  [[entrypoint-id entrypoint]]
  (let [{:keys [bind http3? tls]} entrypoint
        binds (bind-values bind)]
    (mapv (fn [addr]
            (let [parsed (parse-bind-address addr)
                  base (cond
                         (:unix parsed)
                         {:entrypoint entrypoint-id
                          :unix (:unix parsed)}

                         (:address parsed)
                         {:entrypoint entrypoint-id
                          :host (:address parsed)
                          :port (:port parsed)}

                         :else
                         {:entrypoint entrypoint-id
                          :port (:port parsed)})]
              (if (map? tls)
                (assoc base :tls (assoc tls :http3? http3?))
                base)))
          binds)))

(defn config->listeners
  [config]
  (assoc config
         :listeners
         (into []
               (mapcat entrypoint->listeners)
               (:entrypoints config))))

(defn normalized-snapshot
  "Return the normalized pure-data config snapshot for `user-config`.

  The snapshot applies Busker defaults, expands listeners, and strips
  runtime-owned objects such as buffer pools, storage instances, and direct
  callable values."
  [user-config]
  (let [config (-> (base-config-with-defaults user-config)
                   config->listeners
                   (dissoc :buffer-pool))]
    (walk/postwalk
     (fn [value]
       (cond
         (callable-value? value)
         callable-placeholder

         (satisfies? storage/Storage value)
         storage-placeholder

         :else
         value))
     config)))

(defn- entrypoint-scheme
  [[_ entrypoint]]
  (if (map? (:tls entrypoint))
    :https
    :http))

(defn- build-entrypoint-resolver
  [entrypoints]
  (let [listeners (into [] (mapcat entrypoint->listeners) entrypoints)
        by-port (into {}
                      (keep (fn [{:keys [entrypoint port]}]
                              (when port
                                [port entrypoint])))
                      listeners)
        by-scheme (reduce (fn [acc pair]
                            (update acc (entrypoint-scheme pair)
                                    (fnil conj [])
                                    (first pair)))
                          {}
                          entrypoints)
        singletons (into {}
                         (keep (fn [[scheme ids]]
                                 (when (= 1 (count ids))
                                   [scheme (first ids)])))
                         by-scheme)
        fallback (ffirst entrypoints)]
    (fn [req]
      (or (:ol.busker/entrypoint req)
          (get by-port (:server-port req))
          (get singletons (:scheme req))
          fallback))))

(defn- build-dispatch-handler
  [config]
  (let [resolve-entrypoint (build-entrypoint-resolver (:entrypoints config))
        dispatchers (mapv compile-dispatcher (:dispatch config))]
    (fn [req]
      (loop [current (assoc req :ol.busker/entrypoint
                            (resolve-entrypoint req))
             matched-groups #{}
             matched? false
             [dispatcher & more] dispatchers]
        (if-not dispatcher
          (if matched?
            current
            {:status 200})
          (let [{:keys [entrypoints group match-fn compiled-handler terminal?]}
                dispatcher
                entrypoint-id (:ol.busker/entrypoint current)
                bound? (if (contains? dispatcher :entrypoints)
                         (contains? entrypoints entrypoint-id)
                         true)
                group-available? (or (nil? group)
                                     (not (contains? matched-groups group)))
                matches? (or (nil? match-fn)
                             (match-fn current))]
            (if (and bound? group-available? matches?)
              (let [result (compiled-handler current)
                    matched-groups (cond-> matched-groups
                                     group (conj group))]
                (if terminal?
                  result
                  (recur result matched-groups true more)))
              (recur current matched-groups matched? more))))))))

(def ^:private dispatch-handler-key
  ::dispatch-handler)

(defn dispatch-handler
  [config]
  (or (get config dispatch-handler-key)
      (throw (ex-info "Config has not been compiled."
                      {:config-keys (keys config)}))))

(defn- compile-config
  [config]
  (let [config (update config :tls
                       (fn [tls]
                         (if (map? tls)
                           (-> tls
                               (update :storage resolve-storage)
                               (update :config-fn
                                       #(when % (resolve-function-ref % :config-fn))))
                           tls)))
        handler (build-dispatch-handler config)]
    (assoc config dispatch-handler-key handler)))

(defn load!
  [user-config]
  (let [config (apply-config-defaults user-config)
        errors (validate-config config)]
    (when (seq errors)
      (throw (ex-info "Invalid configuration"
                      {:errors errors
                       :config config})))
    (try
      (compile-config config)
      (catch clojure.lang.ExceptionInfo e
        (throw (ex-info "Invalid configuration"
                        {:errors [(config-error
                                   ::config-compile-invalid
                                   (.getMessage e)
                                   (ex-data e))]
                         :config config}
                        e))))))

(comment
  (load! {})
  (config->listeners (apply-config-defaults {})))
