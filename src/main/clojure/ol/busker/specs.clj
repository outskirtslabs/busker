(ns ol.busker.specs
  "clojure.spec definitions and descriptor metadata for Busker configuration."
  (:refer-clojure :exclude [name])
  (:require
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [ol.busker.buffer-pool :as bp]
   [ol.clave.specs :as clave]
   [ol.clave.storage :as storage])
  (:import
   [java.time Duration]
   [java.util.concurrent ExecutorService]))

(defn- non-blank-string?
  [v]
  (and (string? v)
       (not (str/blank? v))))

(defn- managed-entrypoint?
  [entrypoint]
  (let [tls (:tls entrypoint)]
    (and (map? tls)
         (seq (:issuers tls)))))

(def key-source
  {:key     ::key-source
   :doc     "Component that generates or obtains session ticket encryption keys."
   :default nil})
(s/def ::key-source any?)

(def rotation-interval
  {:key ::rotation-interval
   :doc "Interval between automatic STEK rotations."
   :default (Duration/ofHours 12)})
(s/def ::rotation-interval #(instance? Duration %))

(def max-keys
  {:key ::max-keys
   :doc "Maximum STEKs retained in active rotation."
   :default 4})
(s/def ::max-keys pos-int?)

(def rotation-disabled?
  {:key ::rotation-disabled?
   :doc "When true, prevents automatic STEK rotation."
   :default false})
(s/def ::rotation-disabled? boolean?)

(def disabled?
  {:key ::disabled?
   :doc "When true, disables session ticket resumption."
   :default false})
(s/def ::disabled? boolean?)

(def session-tickets
  {:key ::session-tickets
   :doc "TLS session ticket configuration for resumption and 0-RTT."
   :default nil})
(s/def ::session-tickets
  (s/keys :req-un [::key-source]
          :opt-un [::rotation-interval
                   ::max-keys
                   ::rotation-disabled?
                   ::disabled?]))

(def key-type
  {:key ::key-type
   :doc "Algorithm for generating certificate keys."
   :default :p256})
(s/def ::key-type #{:p256 :p384 :rsa2048 :rsa4096 :rsa8192 :ed25519})

(def protocol-version
  {:key ::protocol-version
   :doc "HTTP protocol version."
   :default nil})
(s/def ::protocol-version #{:h1 :h2 :h2c :h3})

(def issuer-selection
  {:key ::issuer-selection
   :doc "Strategy for selecting ACME issuer from configured options."
   :default :in-order})
(s/def ::issuer-selection #{:in-order :shuffle})

(def email
  {:key ::email
   :doc "Contact email for certificate notifications."
   :default nil})
(s/def ::email non-blank-string?)

(def eab
  {:key ::eab
   :doc "External account binding credentials for restricted CAs."
   :default nil})
(s/def ::eab map?)

(def issuer-config
  {:key ::issuer-config
   :doc "ACME certificate authority issuer configuration."
   :default nil})
(s/def ::issuer-config
  (s/keys :req-un [::clave/directory-url]
          :opt-un [::email ::eab]))

(def issuers
  {:key ::issuers
   :doc "ACME certificate authorities to attempt, in priority order."
   :default nil})
(s/def ::issuers (s/coll-of ::issuer-config :kind vector?))

(def enabled?
  {:key ::enabled?
   :doc "Enable the feature for this configuration section."
   :default true})
(s/def ::enabled? boolean?)

(def must-staple?
  {:key ::must-staple?
   :doc "Require OCSP must-staple extension in certificates."
   :default false})
(s/def ::must-staple? boolean?)

(def ocsp
  {:key ::ocsp
   :doc "OCSP stapling configuration."
   :default nil})
(s/def ::ocsp
  (s/keys :opt-un [::enabled? ::must-staple?]))

(def cache-capacity
  {:key ::cache-capacity
   :doc "Certificate cache size limit; random eviction when full."
   :default 1000})
(s/def ::cache-capacity pos-int?)

(def cert-file
  {:key ::cert-file
   :doc "Path to PEM encoded certificate chain file."
   :default nil})
(s/def ::cert-file non-blank-string?)

(def key-file
  {:key ::key-file
   :doc "Path to PEM encoded private key file."
   :default nil})
(s/def ::key-file non-blank-string?)

(def storage
  {:key ::storage
   :doc "Persistence layer for certificates and ACME account data."
   :default nil})
(s/def ::storage #(satisfies? storage/Storage %))

(def key-reuse
  {:key ::key-reuse
   :doc "Reuse existing private key when renewing certificates."
   :default false})
(s/def ::key-reuse boolean?)

(def tls-compatibility-mode
  {:key ::tls-compatibility-mode
   :doc "TLS cipher/protocol compatibility preset."
   :default :modern})
(s/def ::tls-compatibility-mode #{:modern :intermediate})

(def solvers
  {:key ::solvers
   :doc "Challenge solver implementations mapped by challenge type."
   :default nil})
(s/def ::solvers map?)

(def config-fn
  {:key ::config-fn
   :doc "Function mapping domain to per-domain configuration overrides."
   :default nil})
(s/def ::config-fn ifn?)

(def http-client
  {:key ::http-client
   :doc "HTTP client settings for ACME protocol requests."
   :default nil})
(s/def ::http-client map?)

(def tls
  {:key ::tls
   :doc "TLS subsystem configuration for an entrypoint, or false to disable TLS explicitly."
   :default {}})
(s/def ::tls
  (s/or :enabled
        (s/keys :opt-un [::cache-capacity
                         ::session-tickets
                         ::cert-file
                         ::key-file
                         ::storage
                         ::issuers
                         ::issuer-selection
                         ::key-type
                         ::key-reuse
                         ::tls-compatibility-mode
                         ::solvers
                         ::ocsp
                         ::config-fn
                         ::http-client])
        :disabled false?))

(def bind-address
  {:key ::bind-address
   :doc "Bind address string, e.g. :443, 127.0.0.1:8080, [::1]:8080, unix:/tmp.sock."
   :default nil})
(s/def ::bind-address non-blank-string?)

(def bind
  {:key ::bind
   :doc "One or more bind addresses for an entrypoint."
   :default nil})
(s/def ::bind
  (s/or :single ::bind-address
        :multiple (s/and vector?
                         (s/coll-of ::bind-address
                                    :kind vector?
                                    :min-count 1))))

(def name
  {:key ::name
   :doc "Keyword identifier for an entrypoint."
   :default nil})
(s/def ::name keyword?)

(def http1?
  {:key ::http1?
   :doc "Enable HTTP/1.1 for an entrypoint."
   :default true})
(s/def ::http1? boolean?)

(def http2?
  {:key ::http2?
   :doc "Enable HTTP/2 for an entrypoint."
   :default true})
(s/def ::http2? boolean?)

(def http3?
  {:key ::http3?
   :doc "Enable HTTP/3 for an entrypoint. When omitted, config defaults to true if TLS is enabled and false otherwise."
   :default nil})
(s/def ::http3? boolean?)

(def entrypoint
  {:key ::entrypoint
   :doc "Entrypoint definition including bind, protocol toggles, and TLS."
   :default {:http1? true
             :http2? true
             :tls (:default tls)}})
(s/def ::entrypoint
  (s/keys :req-un [::name ::bind]
          :opt-un [::http1? ::http2? ::http3? ::tls]))

(def entrypoints
  {:key ::entrypoints
   :doc "Vector of entrypoint definitions."
   :default [{:name :http
              :bind ["127.0.0.1:8080" "[::1]:8080"]
              :http3? false}
             {:name :https
              :bind ["127.0.0.1:8443" "[::1]:8443"]
              :http3? true}]})

(s/def ::entrypoints
  (s/and vector?
         (s/coll-of ::entrypoint :kind vector? :min-count 1)))

(def managed-entrypoints
  {:key ::managed-entrypoints
   :doc "Managed TLS entrypoints with ACME issuers configured."
   :default nil})
(s/def ::managed-entrypoints
  (s/and vector?
         (s/coll-of ::entrypoint :kind vector? :min-count 1)
         #(every? managed-entrypoint?
                  (map (partial s/unform ::entrypoint) %))))

(def clave-config
  {:key ::clave-config
   :doc "Clave automation config extracted from managed TLS settings."
   :default nil})
(s/def ::clave-config
  (s/keys :req-un [::issuers]
          :opt-un [::storage
                   ::issuer-selection
                   ::key-type
                   ::key-reuse
                   ::cache-capacity
                   ::solvers
                   ::ocsp
                   ::config-fn
                   ::http-client]))

(def managed-plan
  {:key ::managed-plan
   :doc "Managed TLS runtime plan for Clave lifecycle and HTTP-01 middleware."
   :default nil})
(s/def ::managed-plan
  (s/keys :req-un [::domains
                   ::managed-entrypoints
                   ::clave-config]))

(def domains
  {:key ::domains
   :doc "Domain names this server handles."
   :default nil})
(s/def ::domains (s/coll-of non-blank-string? :kind vector?))

(def n-workers
  {:key ::n-workers
   :doc "Event loop worker thread count."
   :default 1})
(s/def ::n-workers pos-int?)

(def executor
  {:key ::executor
   :doc "ExecutorService for Ring handler execution."
   :default nil})
(s/def ::executor #(instance? ExecutorService %))

(def max-connections
  {:key ::max-connections
   :doc "Maximum concurrent connections across all workers."
   :default 1024})
(s/def ::max-connections pos-int?)

(def output-buffer-size
  {:key ::output-buffer-size
   :doc "Response aggregation buffer size in bytes; 0 disables buffering."
   :default 32768})
(s/def ::output-buffer-size nat-int?)

(def server-name
  {:key ::server-name
   :doc "Server identifier for Server response header."
   :default "ol.busker/dev"})
(s/def ::server-name non-blank-string?)

(def proxy-status-identity
  {:key ::proxy-status-identity
   :doc "Identity for Proxy-Status header (RFC 9209)."
   :default nil})
(s/def ::proxy-status-identity non-blank-string?)

(def max-request-entity-size
  {:key ::max-request-entity-size
   :doc "Maximum request body size in bytes."
   :default nil})
(s/def ::max-request-entity-size pos-int?)

(def max-delegations
  {:key ::max-delegations
   :doc "Maximum internal request delegations."
   :default nil})
(s/def ::max-delegations pos-int?)

(def max-reprocesses
  {:key ::max-reprocesses
   :doc "Maximum internal request reprocesses."
   :default nil})
(s/def ::max-reprocesses pos-int?)

(def handshake-timeout
  {:key ::handshake-timeout
   :doc "TLS handshake timeout in milliseconds."
   :default nil})
(s/def ::handshake-timeout pos-int?)

(def max-spare-pipes
  {:key ::max-spare-pipes
   :doc "Maximum idle connection pipes to retain."
   :default nil})
(s/def ::max-spare-pipes nat-int?)

(def http1-req-timeout
  {:key ::http1-req-timeout
   :doc "HTTP/1.1 request timeout in milliseconds."
   :default nil})
(s/def ::http1-req-timeout pos-int?)

(def http1-req-io-timeout
  {:key ::http1-req-io-timeout
   :doc "HTTP/1.1 request I/O timeout in milliseconds."
   :default nil})
(s/def ::http1-req-io-timeout pos-int?)

(def http1-upgrade?
  {:key ::http1-upgrade?
   :doc "Allow HTTP/1.1 to HTTP/2 upgrade via h2c."
   :default true})
(s/def ::http1-upgrade? boolean?)

(def http2-idle-timeout
  {:key ::http2-idle-timeout
   :doc "HTTP/2 idle timeout in milliseconds."
   :default nil})
(s/def ::http2-idle-timeout pos-int?)

(def http2-graceful-shutdown-timeout
  {:key ::http2-graceful-shutdown-timeout
   :doc "HTTP/2 graceful shutdown timeout in milliseconds."
   :default nil})
(s/def ::http2-graceful-shutdown-timeout nat-int?)

(def http2-max-streams
  {:key ::http2-max-streams
   :doc "Maximum concurrent HTTP/2 streams per connection."
   :default nil})
(s/def ::http2-max-streams pos-int?)

(def http2-max-requests
  {:key ::http2-max-requests
   :doc "Maximum concurrent HTTP/2 requests per connection."
   :default nil})
(s/def ::http2-max-requests pos-int?)

(def http2-max-streaming-requests
  {:key ::http2-max-streaming-requests
   :doc "Maximum concurrent streaming requests per connection."
   :default nil})
(s/def ::http2-max-streaming-requests pos-int?)

(def http2-max-priority-streams
  {:key ::http2-max-priority-streams
   :doc "Maximum IDLE/CLOSED streams tracked for priority."
   :default nil})
(s/def ::http2-max-priority-streams pos-int?)

(def http2-stream-window-size
  {:key ::http2-stream-window-size
   :doc "HTTP/2 per-stream flow control window size in bytes."
   :default nil})
(s/def ::http2-stream-window-size pos-int?)

(def http2-dos-delay
  {:key ::http2-dos-delay
   :doc "Delay in milliseconds when suspicious behavior is detected."
   :default nil})
(s/def ::http2-dos-delay pos-int?)

(def http3-idle-timeout
  {:key ::http3-idle-timeout
   :doc "HTTP/3 idle timeout in milliseconds."
   :default nil})
(s/def ::http3-idle-timeout pos-int?)

(def http3-graceful-shutdown-timeout
  {:key ::http3-graceful-shutdown-timeout
   :doc "HTTP/3 graceful shutdown timeout in milliseconds."
   :default nil})
(s/def ::http3-graceful-shutdown-timeout nat-int?)

(def http3-stream-window-size
  {:key ::http3-stream-window-size
   :doc "HTTP/3 per-stream flow control window size in bytes."
   :default nil})
(s/def ::http3-stream-window-size pos-int?)

(def http3-ack-frequency
  {:key ::http3-ack-frequency
   :doc "ACK frequency for HTTP/3; 0 uses QUIC library default."
   :default nil})
(s/def ::http3-ack-frequency nat-int?)

(def compress?
  {:key ::compress?
   :doc "Enable automatic response compression."
   :default true})
(s/def ::compress? boolean?)

(def compress-min-size
  {:key ::compress-min-size
   :doc "Minimum response size in bytes for compression."
   :default 100})
(s/def ::compress-min-size nat-int?)

(def compress-gzip-level
  {:key ::compress-gzip-level
   :doc "Gzip compression level (0-9)."
   :default 1})
(s/def ::compress-gzip-level (s/int-in 0 10))

(def compress-brotli-level
  {:key ::compress-brotli-level
   :doc "Brotli compression level (0-11)."
   :default 1})
(s/def ::compress-brotli-level (s/int-in 0 12))

(def compress-zstd-level
  {:key ::compress-zstd-level
   :doc "Zstandard compression level."
   :default 3})
(s/def ::compress-zstd-level int?)

(def session-ticket-lifetime-seconds
  {:key ::session-ticket-lifetime-seconds
   :doc "TLS session ticket lifetime in seconds."
   :default 86400})
(s/def ::session-ticket-lifetime-seconds pos-int?)

(def buffer-pool
  {:key ::buffer-pool
   :doc "ByteBuffer pool for response buffering."
   :default nil})
(s/def ::buffer-pool #(satisfies? bp/BufferPool %))

(def config
  {:key ::config
   :doc "Top-level Busker configuration map."
   :default nil})
(s/def ::config
  (s/keys :req-un [::entrypoints]
          :opt-un [::domains
                   ::n-workers
                   ::executor
                   ::max-connections
                   ::output-buffer-size
                   ::server-name
                   ::proxy-status-identity
                   ::max-request-entity-size
                   ::max-delegations
                   ::max-reprocesses
                   ::handshake-timeout
                   ::max-spare-pipes
                   ::http1-req-timeout
                   ::http1-req-io-timeout
                   ::http1-upgrade?
                   ::http2-idle-timeout
                   ::http2-graceful-shutdown-timeout
                   ::http2-max-streams
                   ::http2-max-requests
                   ::http2-max-streaming-requests
                   ::http2-max-priority-streams
                   ::http2-stream-window-size
                   ::http2-dos-delay
                   ::http3-idle-timeout
                   ::http3-graceful-shutdown-timeout
                   ::http3-stream-window-size
                   ::http3-ack-frequency
                   ::compress?
                   ::compress-min-size
                   ::compress-gzip-level
                   ::compress-brotli-level
                   ::compress-zstd-level
                   ::session-ticket-lifetime-seconds
                   ::buffer-pool]))

(def all-descriptors
  [key-source
   rotation-interval
   max-keys
   rotation-disabled?
   disabled?
   session-tickets
   key-type
   protocol-version
   issuer-selection
   email
   eab
   issuer-config
   issuers
   enabled?
   must-staple?
   ocsp
   cache-capacity
   cert-file
   key-file
   storage
   key-reuse
   tls-compatibility-mode
   solvers
   config-fn
   http-client
   tls
   bind-address
   bind
   name
   http1?
   http2?
   http3?
   entrypoint
   entrypoints
   managed-entrypoints
   clave-config
   managed-plan
   domains
   n-workers
   executor
   max-connections
   output-buffer-size
   server-name
   proxy-status-identity
   max-request-entity-size
   max-delegations
   max-reprocesses
   handshake-timeout
   max-spare-pipes
   http1-req-timeout
   http1-req-io-timeout
   http1-upgrade?
   http2-idle-timeout
   http2-graceful-shutdown-timeout
   http2-max-streams
   http2-max-requests
   http2-max-streaming-requests
   http2-max-priority-streams
   http2-stream-window-size
   http2-dos-delay
   http3-idle-timeout
   http3-graceful-shutdown-timeout
   http3-stream-window-size
   http3-ack-frequency
   compress?
   compress-min-size
   compress-gzip-level
   compress-brotli-level
   compress-zstd-level
   session-ticket-lifetime-seconds
   buffer-pool
   config])

(def descriptor-by-key
  (into {} (map (juxt :key identity) all-descriptors)))

(def default-entrypoint
  (:default entrypoint))

(def default-config
  {:entrypoints (:default entrypoints)
   :n-workers (:default n-workers)
   :max-connections (:default max-connections)
   :output-buffer-size (:default output-buffer-size)
   :server-name (:default server-name)
   :compress? (:default compress?)
   :compress-min-size (:default compress-min-size)
   :compress-gzip-level (:default compress-gzip-level)
   :compress-brotli-level (:default compress-brotli-level)
   :compress-zstd-level (:default compress-zstd-level)
   :http1-upgrade? (:default http1-upgrade?)
   :session-ticket-lifetime-seconds
   (:default session-ticket-lifetime-seconds)})

(defn valid-config?
  [config]
  (s/valid? ::config config))

(defn explain-config
  [config]
  (s/explain-data ::config config))
