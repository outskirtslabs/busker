(ns ol.h2o.benchmark
  "This is a benchmark suite based on the implementation by Peter Taoussanis (@ptaoussanis) in http-kit

  ref: https://github.com/http-kit/http-kit/blob/master/test/org/httpkit/benchmark.clj

  All caveats and disclaimers apply here as well."
  (:require
   [clojure.edn :as edn]
   [ol.h2o.benchmark.server :as server]
   [ol.h2o.benchmark.utils :as u]))

(set! *warn-on-reflection* true)

(def ^:private default-metadata
  {:author "unknown"
   :description "ol.h2o benchmark run"
   :comments ""})

(defn bench
  "Run the server benchmark. Accepts an option map with optional keys:

    :metadata  map with :author/:description/:comments
    :profile   keyword or vector accepted by server/profiles
    :dry-run?  when true, prints planned specs without executing wrk
    :server    nested map merged into the server benchmark call

  Returns the benchmark result map from server/bench-by-profile."
  ([]
   (bench {}))
  ([{:keys [metadata profile dry-run? server]
     :or {profile :quick} :as opts}]
   (let [t0 (System/currentTimeMillis)
         metadata (merge default-metadata metadata)
         system-info (u/get-system-info)
         server-opts (merge {:profile profile
                             :dry-run? dry-run?
                             :metadata metadata
                             :system-info system-info}
                            server)]
     (binding [u/*dry-run?* dry-run?]
       (u/log "***************************")
       (u/log "[bench] starting run" u/newline (:as-str system-info))
       (reset! u/last-errors_ {})
       (let [[server-file server-result] (server/bench-by-profile server-opts)
             elapsed (u/secs-since t0)]
         (u/log "[bench] finished in " elapsed " seconds")
         (when (and server-file (not dry-run?))
           (u/log "[bench] wrote server results to " server-file))
         (when-let [errors (not-empty @u/last-errors_)]
           (u/log "[bench] *** WARN: errors captured during run ***")
           (doseq [[k v] errors]
             (u/log "  key: " k u/newline v)))
         (u/log "[bench] done")
         (u/log "***************************")
         [server-file server-result])))))

(defn -main [& args]
  (let [opts (when-let [first-arg (first args)]
               (try
                 (edn/read-string first-arg)
                 (catch Exception e
                   (throw (ex-info "[bench] failed to parse options. provide EDN."
                                   {:arg first-arg} e)))))]
    (bench (or opts {}))
    (System/exit 0)))
