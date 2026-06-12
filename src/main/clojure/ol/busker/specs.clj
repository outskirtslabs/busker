(ns ol.busker.specs
  "clojure.spec definitions and descriptor metadata for Busker configuration."
  (:refer-clojure :exclude [type load])
  (:require
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [ol.busker.buffer-pool :as bp]
   [ol.clave.issuers :as issuers]
   [ol.clave.specs :as clave]
   [ol.clave.storage :as storage])
  (:import
   [java.util.concurrent ExecutorService]))

(defn- non-blank-string?
  [v]
  (and (string? v)
       (not (str/blank? v))))

(defn- qualified-symbol-ref?
  [v]
  (and (symbol? v)
       (namespace v)
       (name v)))

(defn- function-ref?
  [v]
  (or (fn? v)
      (qualified-symbol-ref? v)))

(def disabled?
  {:key ::disabled?
   :doc "When true, disables session ticket resumption."
   :default false})
(s/def ::disabled? boolean?)

(def persistence
  {:key ::persistence
   :doc "Session ticket persistence mode."
   :default :memory})
(s/def ::persistence #{:memory :storage})

(def lifetime-seconds
  {:key ::lifetime-seconds
   :doc "TLS session ticket lifetime in seconds."
   :default 86400})
(s/def ::lifetime-seconds pos-int?)

(def max-keys
  {:key ::max-keys
   :doc "Maximum number of session ticket keys retained in rotation."
   :default 4})
(s/def ::max-keys pos-int?)

(def session-tickets
  {:key ::session-tickets
   :doc "TLS session ticket configuration for resumption and 0-RTT."
   :default {:disabled? (:default disabled?)
             :persistence (:default persistence)
             :max-keys (:default max-keys)
             :lifetime-seconds (:default lifetime-seconds)}})
(s/def ::session-tickets
  (s/keys :opt-un [::disabled?
                   ::persistence
                   ::max-keys
                   ::lifetime-seconds]))

(def key-type
  {:key ::key-type
   :doc "Algorithm for generating certificate keys."
   :default :p256})
(s/def ::key-type #{:p256 :p384 :rsa2048 :rsa4096 :rsa8192 :ed25519})

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

(def ^:private default-issuers
  [{:directory-url issuers/lets-encrypt-production-ca}])

(def issuers
  {:key ::issuers
   :doc "ACME certificate authorities to attempt, in priority order."
   :default default-issuers})
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

(def path
  {:key ::path
   :doc "Filesystem path for a TLS certificate source."
   :default nil})
(s/def ::path non-blank-string?)

(def type
  {:key ::type
   :doc "Type discriminator for data-driven config entries."
   :default nil})
(s/def ::type keyword?)

(def load
  {:key ::load
   :doc "Certificate sources that Busker should load at startup."
   :default []})

(def manage
  {:key ::manage
   :doc "Subject names that Busker should manage automatically."
   :default []})

(def certificate-source
  {:key ::certificate-source
   :doc "One certificate source entry in :tls :certificates :load."
   :default nil})
(s/def ::certificate-source
  (s/keys :req-un [::type]
          :opt-un [::path
                   ::cert-file
                   ::key-file]))

(s/def ::load (s/coll-of ::certificate-source :kind vector?))
(s/def ::manage (s/coll-of non-blank-string? :kind vector?))

(def certificates
  {:key ::certificates
   :doc "Static and managed certificate configuration."
   :default {:load []
             :manage []}})
(s/def ::certificates
  (s/keys :opt-un [::load ::manage]))

(def factory
  {:key ::factory
   :doc "Factory function or qualified symbol used to build a config-backed value."
   :default nil})
(s/def ::factory function-ref?)

(def storage-config
  {:key ::storage-config
   :doc "Config map for constructing a Storage implementation."
   :default nil})
(s/def ::storage-config
  (s/keys :req-un [::factory]))

(def storage
  {:key ::storage
   :doc "Persistence layer for certificates and ACME account data."
   :default nil})
(s/def ::storage
  (s/or :instance #(satisfies? storage/Storage %)
        :config ::storage-config))

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
   :doc "Function mapping subject name to per-name configuration overrides."
   :default nil})
(s/def ::config-fn function-ref?)

(def http-client
  {:key ::http-client
   :doc "HTTP client settings for ACME protocol requests."
   :default nil})
(s/def ::http-client map?)

(def tls
  {:key ::tls
   :doc "Global TLS subsystem configuration, or entrypoint-specific TLS settings when used under an entrypoint."
   :default {:session-tickets (:default session-tickets)
             :issuers (:default issuers)}})
(s/def ::tls
  (s/or :global
        (s/keys :opt-un [::storage
                         ::certificates
                         ::session-tickets
                         ::issuers
                         ::issuer-selection
                         ::key-type
                         ::key-reuse
                         ::cache-capacity
                         ::solvers
                         ::ocsp
                         ::config-fn
                         ::http-client
                         ::tls-compatibility-mode])
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

(def entrypoint-id
  {:key ::entrypoint-id
   :doc "Keyword identifier for an entrypoint."
   :default nil})
(s/def ::entrypoint-id keyword?)

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
   :doc "Entrypoint definition including bind, protocol toggles, and entrypoint-local TLS settings."
   :default {:http1? true
             :http2? true
             :http3? true
             :tls {:tls-compatibility-mode (:default tls-compatibility-mode)}}})
(s/def ::entrypoint
  (s/keys :req-un [::bind]
          :opt-un [::http1? ::http2? ::http3? ::tls]))

(def entrypoints
  {:key ::entrypoints
   :doc "Entrypoint definitions keyed by entrypoint id."
   :default {:http {:bind ["127.0.0.1:8080" "[::1]:8080"]
                    :tls false}
             :https {:bind ["127.0.0.1:8443" "[::1]:8443"]
                     :tls {:tls-compatibility-mode :modern}}}})
(s/def ::entrypoints
  (s/or :config-map
        (s/and map?
               seq
               (s/map-of ::entrypoint-id ::entrypoint))
        :dispatcher-binding
        (s/coll-of ::entrypoint-id :kind set?)))

(def group
  {:key ::group
   :doc "Group key for mutually exclusive dispatchers."
   :default nil})
(s/def ::group some?)

(def match
  {:key ::match
   :doc "Predicate function or qualified symbol used to decide whether a dispatcher matches."
   :default nil})
(s/def ::match function-ref?)

(def handler
  {:key ::handler
   :doc "Terminal handler function or qualified symbol for a dispatcher."
   :default nil})
(s/def ::handler function-ref?)

(def middleware-entry
  {:key ::middleware-entry
   :doc "One data-driven middleware declaration."
   :default nil})
(s/def ::middleware-entry
  (s/or :direct function-ref?
        :configured (s/and vector?
                           #(<= 1 (count %) 2)
                           (fn [[wrap-fn]]
                             (function-ref? wrap-fn)))))

(def middleware
  {:key ::middleware
   :doc "Vector of middleware entries compiled around a dispatcher handler."
   :default []})
(s/def ::middleware
  (s/coll-of ::middleware-entry :kind vector?))

(def terminal?
  {:key ::terminal?
   :doc "When true, no later dispatchers run after this dispatcher matches."
   :default false})
(s/def ::terminal? boolean?)

(def dispatcher
  {:key ::dispatcher
   :doc "A single dispatcher in the ordered :dispatch pipeline."
   :default {}})
(s/def ::dispatcher
  (s/keys :opt-un [::entrypoints
                   ::group
                   ::match
                   ::handler
                   ::middleware
                   ::terminal?]))

(def dispatch
  {:key ::dispatch
   :doc "Ordered dispatch pipeline."
   :default [{}]})
(s/def ::dispatch
  (s/and vector?
         (s/coll-of ::dispatcher :kind vector?)))

(def subject-names
  {:key ::subject-names
   :doc "Subject names that a managed TLS plan should obtain and renew."
   :default nil})
(s/def ::subject-names
  (s/and vector?
         (s/coll-of non-blank-string? :kind vector? :min-count 1)))

(def clave-config
  {:key ::clave-config
   :doc "Clave automation config extracted from top-level TLS settings."
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
  (s/keys :req-un [::subject-names
                   ::clave-config]))

(def n-workers
  {:key ::n-workers
   :doc "Event loop worker thread count."
   :default 1})
(s/def ::n-workers pos-int?)

(def executor
  {:key ::executor
   :doc "ExecutorService for request execution."
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
  (s/keys :req-un [::entrypoints
                   ::dispatch]
          :opt-un [::tls
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
                   ::buffer-pool]))

(def all-descriptors
  [disabled?
   persistence
   max-keys
   lifetime-seconds
   session-tickets
   key-type
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
   path
   type
   load
   manage
   certificate-source
   certificates
   factory
   storage-config
   storage
   key-reuse
   tls-compatibility-mode
   solvers
   config-fn
   http-client
   tls
   bind-address
   bind
   entrypoint-id
   http1?
   http2?
   http3?
   entrypoint
   entrypoints
   group
   match
   handler
   middleware-entry
   middleware
   terminal?
   dispatcher
   dispatch
   subject-names
   clave-config
   managed-plan
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
   buffer-pool
   config])

(def descriptor-by-key
  (into {} (map (juxt :key identity) all-descriptors)))

(def default-config
  {:tls (:default tls)
   :entrypoints (:default entrypoints)
   :dispatch (:default dispatch)
   :n-workers (:default n-workers)
   :max-connections (:default max-connections)
   :output-buffer-size (:default output-buffer-size)
   :server-name (:default server-name)
   :compress? (:default compress?)
   :compress-min-size (:default compress-min-size)
   :compress-gzip-level (:default compress-gzip-level)
   :compress-brotli-level (:default compress-brotli-level)
   :compress-zstd-level (:default compress-zstd-level)
   :http1-upgrade? (:default http1-upgrade?)})

(defn valid-config?
  [config]
  (s/valid? ::config config))

(defn explain-config
  [config]
  (s/explain-data ::config config))
