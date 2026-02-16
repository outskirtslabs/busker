(ns ol.busker.config
  "Configuration parsing, defaults, and validation for Busker."
  (:require
   [clojure.string :as str]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.specs :as specs]
   [ol.clave.crypto.impl.parse-ip :as parse-ip])
  (:import
   [java.io File]
   [java.net InetAddress]
   [java.util.concurrent Executors]))

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

(def ^:private internal-suffixes
  "Domain suffixes that are internal and cannot get public certs.
  Matches certmagic's SubjectIsInternal logic."
  #{".localhost" ".local" ".internal" ".home.arpa"})

(defn- internal-ip?
  "Returns true if the IP address is internal (loopback, private, link-local, ULA).
  Matches certmagic's isInternalIP logic."
  [^InetAddress addr]
  (or (.isLoopbackAddress addr)      ;; 127.0.0.0/8, ::1
      (.isLinkLocalAddress addr)     ;; 169.254.0.0/16, fe80::/10
      (.isSiteLocalAddress addr)     ;; 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
      (.isAnyLocalAddress addr)      ;; 0.0.0.0, ::
      ;; IPv6 ULA (fc00::/7) - not covered by isSiteLocalAddress
      (let [bytes (.getAddress addr)]
        (and (= 16 (alength bytes))
             (let [first-byte (bit-and (aget bytes 0) 0xfe)]
               (= first-byte 0xfc))))))

(defn internal-host?
  "Returns true if the hostname/IP is internal per certmagic's SubjectIsInternal.
  Internal subjects cannot get public certs and use self-signed instead."
  [host]
  (when (and (string? host) (not (str/blank? host)))
    (let [host-lower (-> host str/lower-case (str/replace #"\.$" ""))]
      (or (= "localhost" host-lower)
          (some #(str/ends-with? host-lower %) internal-suffixes)
          (when-let [inet-addr (parse-ip/from-string host)]
            (internal-ip? inet-addr))))))

(defn internal-address?
  "Returns true if the bind address is internal.
  Internal addresses qualify for auto-generated self-signed certificates."
  [addr]
  (when-let [parsed (parse-bind-address addr)]
    (cond
      ;; Unix sockets are internal
      (:unix parsed)
      true

      ;; nil host means all interfaces (e.g., \":8080\") - not internal
      (nil? (:address parsed))
      false

      ;; Check if the host is internal
      :else
      (internal-host? (:address parsed)))))

(defn parse-bind
  [bind]
  (let [binds (cond
                (string? bind) [bind]
                (vector? bind) bind
                :else (throw (ex-info "Invalid :bind value." {:bind bind})))]
    (when (empty? binds)
      (throw (ex-info "Bind must include at least one address." {:bind bind})))
    (mapv (fn [addr]
            (or (parse-bind-address addr)
                (throw (ex-info "Invalid bind address." {:bind addr}))))
          binds)))

(defn entrypoint-error
  [entrypoint error msg data]
  {:msg msg
   :error error
   :data (merge {:entrypoint (:name entrypoint)} data)})

(defn maybe-add-entrypoint-error
  [errors pred entrypoint error msg data]
  (if pred
    (conj errors (entrypoint-error entrypoint error msg data))
    errors))

(defn bind-values
  [bind]
  (cond
    (string? bind) [bind]
    (vector? bind) bind
    :else nil))

(defn all-binds-internal?
  "Returns true if all bind addresses in the entrypoint are internal."
  [entrypoint]
  (let [binds (bind-values (:bind entrypoint))]
    (and (seq binds)
         (every? internal-address? binds))))

(defn validate-bind-address
  [errors entrypoint idx addr]
  (cond
    (not (string? addr))
    (conj errors (entrypoint-error entrypoint
                                   ::entrypoint-bind-address-type
                                   "Bind address must be a string."
                                   {:bind addr
                                    :index idx
                                    :type (.getName (class addr))}))
    (str/blank? addr)
    (conj errors (entrypoint-error entrypoint
                                   ::entrypoint-bind-address-blank
                                   "Bind address must not be blank."
                                   {:bind addr
                                    :index idx}))
    (bind-address? addr)
    errors
    :else
    (conj errors (entrypoint-error entrypoint
                                   ::entrypoint-bind-address-invalid
                                   "Bind address must be a valid address string."
                                   {:bind addr
                                    :index idx}))))

(defn validate-bind
  [errors entrypoint]
  (let [bind (:bind entrypoint)
        binds (bind-values bind)]
    (cond
      (nil? binds)
      (conj errors (entrypoint-error entrypoint
                                     ::entrypoint-bind-type
                                     "Bind must be a string or vector of strings."
                                     {:bind bind}))
      (empty? binds)
      (conj errors (entrypoint-error entrypoint
                                     ::entrypoint-bind-empty
                                     "Bind must include at least one address."
                                     {:bind bind}))
      :else
      (reduce-kv (fn [errs idx addr]
                   (validate-bind-address errs entrypoint idx addr))
                 errors
                 (vec binds)))))

(defn validate-tls
  "Validate TLS config for an entrypoint, returning errors vector.
  Skips cert-file/key-file validation when:
  - All bindings are internal (will use auto-generated self-signed certs)
  - ACME issuers are configured (certs will be obtained automatically)
  - No explicit cert/key files are configured (automatic provisioning mode)"
  [errors entrypoint]
  (let [tls (:tls entrypoint)]
    (if-not (map? tls)
      errors
      (let [cert-file (:cert-file tls)
            key-file (:key-file tls)
            has-issuers? (seq (:issuers tls))
            internal? (all-binds-internal? entrypoint)
            manual-cert-mode? (or cert-file key-file)
            ;; Manual mode requires cert files unless internal or ACME
            needs-cert-files? (and manual-cert-mode? (not internal?) (not has-issuers?))]
        (-> errors
            (maybe-add-entrypoint-error
             (and needs-cert-files? (nil? cert-file))
             entrypoint ::tls-missing-cert-file
             "TLS config requires :cert-file for non-internal addresses without ACME issuers." {})
            (maybe-add-entrypoint-error
             (and needs-cert-files? (nil? key-file))
             entrypoint ::tls-missing-key-file
             "TLS config requires :key-file for non-internal addresses without ACME issuers." {})
            (maybe-add-entrypoint-error
             (and cert-file (not (.exists (File. ^String cert-file))))
             entrypoint ::tls-cert-file-not-found
             "TLS certificate file not found." {:cert-file cert-file})
            (maybe-add-entrypoint-error
             (and key-file (not (.exists (File. ^String key-file))))
             entrypoint ::tls-key-file-not-found
             "TLS private key file not found." {:key-file key-file}))))))

(defn validate-entrypoint
  [errors entrypoint]
  (let [errors (or errors [])
        errors (validate-bind errors entrypoint)
        binds (bind-values (:bind entrypoint))
        http1? (:http1? entrypoint)
        http2? (:http2? entrypoint)
        http3? (:http3? entrypoint)
        unix? (boolean (some unix-bind-address? binds))]
    (-> errors
        (maybe-add-entrypoint-error
         (and http3? unix?)
         entrypoint
         ::entrypoint-http3-unix
         "HTTP/3 is not supported for unix domain sockets."
         {:bind (vec binds)})
        (maybe-add-entrypoint-error
         (and http3?
              (not (map? (:tls entrypoint)))
              (not (all-binds-internal? entrypoint)))
         entrypoint
         ::entrypoint-http3-requires-tls
         "HTTP/3 requires TLS configuration for non-internal addresses."
         {})
        (maybe-add-entrypoint-error
         (not (or http1? http2? http3?))
         entrypoint
         ::entrypoint-no-protocols
         "Entrypoint must enable at least one HTTP protocol."
         {:http1? http1?
          :http2? http2?
          :http3? http3?})
        (validate-tls entrypoint))))

(defn apply-entrypoint-defaults
  "Apply default values to an entrypoint map."
  [entrypoint]
  (let [tls-overridden? (contains? entrypoint :tls)
        http3-overridden? (contains? entrypoint :http3?)
        entrypoint (merge (:default specs/entrypoint) entrypoint)
        entrypoint (if tls-overridden?
                     entrypoint
                     (assoc entrypoint :tls (:default specs/tls)))]
    (if http3-overridden?
      entrypoint
      (assoc entrypoint :http3? (map? (:tls entrypoint))))))

(defn apply-config-defaults
  "Apply default values to config map, including runtime objects."
  [config]
  (let [config (merge specs/default-config config)]
    (-> config
        (cond-> (not (contains? config :executor))
          (assoc :executor (Executors/newVirtualThreadPerTaskExecutor)))
        (cond-> (not (contains? config :buffer-pool))
          (assoc :buffer-pool (bp/make-bytebuffer-pool {})))
        (update :entrypoints #(mapv apply-entrypoint-defaults %)))))

(defn validate-config
  "Validate entire config, returning vector of all errors."
  [config]
  (let [entrypoints (:entrypoints config)
        missing-entrypoints? (or (nil? entrypoints) (empty? entrypoints))
        spec-errors (when (and (not missing-entrypoints?)
                               (not (specs/valid-config? config)))
                      [{:msg "Config failed spec validation."
                        :error ::config-spec-invalid
                        :data {:problems (-> config
                                             specs/explain-config
                                             :clojure.spec.alpha/problems)}}])]
    (cond-> (vec spec-errors)
      missing-entrypoints?
      (conj {:msg "Config must include at least one entrypoint."
             :error ::config-no-entrypoints
             :data {}})

      (and (vector? entrypoints) (seq entrypoints))
      (into (mapcat #(validate-entrypoint [] %) entrypoints)))))

(defn load!
  "Load and validate config.
  Returns validated config map with defaults applied and runtime objects created.
  Throws ex-info with :errors key containing all validation errors on failure."
  [user-config]
  (let [config (apply-config-defaults user-config)
        errors (validate-config config)]
    (when (seq errors)
      (throw (ex-info "Invalid configuration"
                      {:errors errors
                       :config config})))
    config))

(defn entrypoint->listeners
  "Convert an entrypoint to the internal listener format used by the server.
  Each bind address in the entrypoint becomes a separate listener.
  The :http3? flag is moved into :tls for compatibility with http3-enabled?."
  [entrypoint]
  (let [{:keys [bind http3? tls]} entrypoint
        binds (bind-values bind)]
    (mapv (fn [addr]
            (let [parsed (parse-bind-address addr)
                  base (cond
                         (:unix parsed)
                         {:unix (:unix parsed)}

                         (:address parsed)
                         {:host (:address parsed) :port (:port parsed)}

                         :else
                         {:port (:port parsed)})]
              (if tls
                (assoc base :tls (assoc tls :http3? http3?))
                base)))
          binds)))

(defn config->listeners
  "Convert config with :entrypoints to internal :listeners format.
  This flattens all entrypoints' bind addresses into a single listeners vector."
  [config]
  (let [entrypoints (:entrypoints config)
        listeners (into [] (mapcat entrypoint->listeners) entrypoints)]
    (assoc config :listeners listeners)))

(comment
  (load! {})
  (->
   (apply-config-defaults {})
   :entrypoints)

;
  )
