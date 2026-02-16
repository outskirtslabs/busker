(ns ol.busker.metrics
  "Minimal CLJ metrics facade built on taoensso/trove.

  What this is:
  - A tiny, dependency-light API for recording counters, gauges, histograms, meters, and timers.
  - Emits structured \"metric events\" via `taoensso.trove/log!`, keeping the core free of registry/export deps.
  - Works in Clojure and uses a monotonic clock for timing.

  Why:
  - Library authors get a zero-registry metrics API without forcing apps to pull in Micrometer/Prometheus/etc.
  - Applications can plug in their preferred backend by setting `trove/*log-fn*` to an adapter log-fn.
  - Trove's lazy value semantics keep call-site overhead low and allow filtering before doing expensive work.

  How it emits:
  - Each metric call logs a structured event with:
    - `:level`   — typically `:trace` (so you can route/disable separately from normal logs).
    - `:id`      — shaped as `[:metrics kind metric-id]` for easy filtering and routing.
    - `:data`    — normalized namespaced fields per kind, e.g. `{::delta ...}` for counters,
                   `{::value ...}` for gauges/histograms, `{::n ...}` for meters,
                   `{::duration-ns ...}` for timers, plus optional `{::tags {...}}`.

  API surface:
  - `counter!(id delta [tags])`              — increment/decrement a counter.
  - `gauge-set!(id value [tags])`            — set a gauge to an absolute value.
  - `histogram-observe!(id value [tags])`    — observe a sample into a histogram.
  - `meter-mark!(id [n] [tags])`             — mark one or N occurrences on a meter.
  - `time!(id f [tags])`                     — time `f` and emit `::duration-ns`; returns `f`'s result.
  - `now-ns`                                 — cross-platform monotonic timestamp (ns).

  Backends (out of scope here):
  - This namespace does NOT implement a registry, percentile/reservoir logic, rate windows, scraping endpoints,
    or exporters. Those live in a Trove log-fn adapter you provide in your app, e.g. Micrometer, Prometheus, OTel, StatsD.
  - Write an adapter that recognizes `[:metrics kind id]` events and forwards to your registry/exporter.

  Caveats and guidance:
  - Keep tag sets small and low-cardinality to avoid backend explosion.
  - Percentiles/histogram behavior depends on your chosen backend/registry; this facade just emits observations.
  - Because events are normal Trove logs, you can filter by namespace, id, level, data, or tags.

  Intended use:
  - Libraries depend only on Trove and call this API.
  - Applications choose and configure a metrics backend by installing a Trove log-fn adapter."
  (:require
   [taoensso.trove :as trove]))

(defn now-ns
  "Cross-platform monotonic timestamp in nanoseconds."
  []
  (System/nanoTime))

(defmacro ^:private emit!
  "Internal: normalize + emit a metrics event via Trove.
   We encode routing in :id for easy Trove filtering: [:metrics kind metric-id]."
  [{:keys [kind id] :as ev}]
  `(let [event# (-> ~ev
                    (dissoc :kind :id))]
     (trove/log!
      {:level :trace
       :id [:metrics ~kind ~id]
       :data event#})))

(defn counter!
  "Increment a counter by `delta` (can be negative).
   - id: keyword or vector (e.g. :auth/logins or [:http :requests])
   - tags: optional map of low-cardinality labels"
  ([id delta] (counter! id delta nil))
  ([id delta tags]
   (emit! {:kind :counter :id id ::delta delta ::tags tags})))

(defn gauge-set!
  "Set a gauge to an absolute `value`."
  ([id value] (gauge-set! id value nil))
  ([id value tags]
   (emit! {:kind :gauge :id id ::value value ::tags tags})))

(defn histogram-observe!
  "Record a value into a histogram."
  ([id value] (histogram-observe! id value nil))
  ([id value tags]
   (emit! {:kind :histogram :id id ::value value ::tags tags})))

(defn meter-mark!
  "Mark N events on a meter (rate)."
  ([id] (meter-mark! id 1 nil))
  ([id n] (meter-mark! id n nil))
  ([id n tags]
   (emit! {:kind :meter :id id ::n n ::tags tags})))

(defn time!
  "Time the execution of thunk `f`, emit a timer metric (duration-ns), and return f's result.
   Usage: (time! :db/query #(run-query q) {:tags {:db :primary}})"
  ([id f] (time! id f nil))
  ([id f tags]
   (let [t0 (now-ns)
         ret (f)
         dur (- (now-ns) t0)]
     (emit! {:kind :timer :id id ::duration-ns dur ::tags tags})
     ret)))

(defn time-block!
  "Time an arbitrary block by passing a zero-arg function.
   This is just a small helper if you don't want an inline #(do ...)."
  ([id f] (time! id f nil))
  ([id f tags] (time! id f tags)))
