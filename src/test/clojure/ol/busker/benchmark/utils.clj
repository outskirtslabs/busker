(ns ol.busker.benchmark.utils
  "Utility helpers adapted from the http-kit benchmark suite.

  The goal is to keep the ergonomics of the original tooling while trimming the
  surface area to what our benchmarks need right now."
  (:refer-clojure :exclude [newline])
  (:require
   [clojure.java.io :as jio]
   [clojure.stacktrace :as st]
   [clojure.string :as str]))

(set! *warn-on-reflection* true)

;;;; Constants

(def ^:dynamic *verbose-logging?* false)

(def ^:const dep-busker "busker (local)")
(def ^:const dep-http-kit "http-kit v2.9.0-beta2")
(def ^:const dep-jetty "ring-jetty-adapter v1.15.3")
(def ^:const dep-sunng-jetty "sunng-jetty9-adapter v0.39.0")

;;;; General helpers

(def num-cores (.availableProcessors (Runtime/getRuntime)))

(def properties (into {} (System/getProperties)))
(def newline (properties "line.separator"))

(defn round0 ^long [n] (Math/round (double n)))
(defn round1 ^double [n] (/ (double (Math/round (* (double n) 1e1))) 1e1))
(defn round4 ^double [n] (/ (double (Math/round (* (double n) 1e4))) 1e4))

(let [dfs (java.text.DecimalFormatSymbols. java.util.Locale/US)
      df (java.text.DecimalFormat. "0.0000" dfs)]
  (defn format-round4 [n]
    (when n (.format df (double n)))))

(defn secs-since [^long millis]
  (round1 (/ (double (- (System/currentTimeMillis) millis)) 1000.0)))

(defn format-instant [^java.time.Instant inst]
  (let [formatter (.withZone
                   (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH-mm-ss")
                   java.time.ZoneOffset/UTC)]
    (.format formatter inst)))

(def ^:dynamic *dry-run?* false)

(defn log [& xs]
  (when-not *dry-run?*
    (println (apply str xs))))

(defn ex->str [t]
  (with-out-str (st/print-stack-trace t)))

(defonce last-errors_ (atom {}))

(defn error! [key err]
  (swap! last-errors_ assoc key err))

(defn quoted [x] (str \" x \"))

(defn join->csv [xs]
  (let [first?_ (atom true)]
    (reduce
     (fn rf [acc x]
       (cond
         (vector? x) (reduce rf acc x)
         :else
         (if (compare-and-set! first?_ true false)
           (str acc x)
           (str acc "," x))))
     "" xs)))

(defn standard-csv-rows
  ([] ; headers
   [["Meta.author" "Meta.description" "Meta.comments"]
    ["Timestamp" "System.cores" "System.memory (GiB)" "Clojure.version" "OS" "Java.version" "JVM.version" "JVM.memory (GiB)"]
    ["Resp.length (bytes)" "Resp.min-sleep (msecs)" "Resp.max-sleep (msecs)" "Resp.min-work (msecs)" "Resp.max-work (msecs)"]])

  ([{:keys [metadata system-info server-opts]}]
   [(let [{:keys [author description comments]} metadata]
      [(quoted author) (quoted description) (quoted comments)])

    (let [{:keys [instant sys-cores sys-mem clojure os java jvm jvm-mem]} system-info]
      [(when instant (format-instant instant)) sys-cores sys-mem clojure os java jvm jvm-mem])

    (let [{:keys [resp-len resp-work]} server-opts
          {:keys [sleep hot]} resp-work
          [min-sleep max-sleep] sleep
          [min-hot max-hot] hot]
      [resp-len min-sleep max-sleep min-hot max-hot])]))

(defn get-system-info []
  (let [^com.sun.management.OperatingSystemMXBean os
        (java.lang.management.ManagementFactory/getOperatingSystemMXBean)
        bytes->gib (fn [bytes] (round1 (/ (long bytes) (Math/pow 1024.0 3))))
        info {:instant   (java.time.Instant/now)
              :sys-cores num-cores
              :sys-mem   (bytes->gib (.getTotalPhysicalMemorySize os))
              :clojure   (clojure-version)
              :os        (str (properties "os.name") " " (properties "os.version"))
              :java      (properties "java.version")
              :jvm       (str (properties "java.vm.name") " " (properties "java.vm.version"))
              :jvm-mem   (bytes->gib (.maxMemory (Runtime/getRuntime)))}]
    (assoc info :as-str
           (str
            "  Timestamp: " (format-instant (:instant info)) newline
            "  Sys cores: " (:sys-cores info) newline
            "    Sys mem: " (:sys-mem info) " GiB" newline
            "    Clojure: " (:clojure info) newline
            "         OS: " (:os info) newline
            "       Java: " (:java info) newline
            "        JVM: " (:jvm info) newline
            "    JVM mem: " (:jvm-mem info) " GiB" newline))))

(defn or-defaults [opts defaults]
  (merge-with (fn [d o] (or o d)) defaults opts))

;;;; Shell helpers

(defn- read-stream [^java.io.InputStream stream]
  (with-open [reader (java.io.InputStreamReader. stream java.nio.charset.StandardCharsets/UTF_8)
              sw (java.io.StringWriter.)]
    (let [buf (char-array 8192)]
      (loop []
        (let [n (.read reader buf)]
          (when (pos? n)
            (.write sw buf 0 n)
            (recur)))))
    (str/trim (.toString sw))))

(defn shell
  "Execute an external command. Returns {:exit :out :err :okay?}.
  Accepts a vector of command arguments."
  ([args] (shell 0 args))
  ([timeout-msecs args]
   (let [proc (.start (ProcessBuilder. (mapv str args)))
         future-out (future (read-stream (.getInputStream proc)))
         future-err (future (read-stream (.getErrorStream proc)))
         exit (if (pos? timeout-msecs)
                (if (.waitFor proc timeout-msecs java.util.concurrent.TimeUnit/MILLISECONDS)
                  (.exitValue proc)
                  (do
                    (.destroyForcibly proc)
                    -1))
                (.waitFor proc))]
     {:exit exit
      :out @future-out
      :err @future-err
      :okay? (zero? exit)})))

(defn rand-free-port []
  (let [rng (java.util.concurrent.ThreadLocalRandom/current)
        min-port 20000
        max-port 65535
        max-attempts 32]
    (loop [attempt 0]
      (when (>= attempt max-attempts)
        (throw (ex-info "[rand-free-port] unable to find free port"
                        {:min min-port :max max-port :attempts attempt})))
      (let [candidate (+ min-port (.nextInt rng (inc (- max-port min-port))))
            available?
            (try
              (with-open [socket (java.net.ServerSocket. (int candidate))]
                (.setReuseAddress socket true)
                true)
              (catch java.net.BindException _
                false))]
        (if available?
          candidate
          (recur (inc attempt)))))))

;;;; wrk integration

(defonce wrk-script-path_
  (delay
    (let [path "src/test/clojure/ol/busker/benchmark/wrk-script.lua"]
      (when-not (.exists (java.io.File. path))
        (throw (ex-info "[wrk] Missing wrk-script.lua resource" {:path path})))
      path)))

(defn run-wrk
  "Runs wrk with the provided options.
  Returns {:opts ... :measurements ...} or throws when wrk fails."
  [{:as wrk-opts :keys [port warm-up duration timeout n-threads n-conns keep-alive? warm-up?]}]
  (when warm-up
    (let [warm-up-opts (assoc (dissoc wrk-opts :warm-up)
                              :after-warm-up warm-up
                              :duration warm-up
                              :warm-up? true)]
      (run-wrk warm-up-opts)))

  (doseq [[k v] {:duration duration :timeout timeout :n-threads n-threads :n-conns n-conns :port port}]
    (when (nil? v)
      (throw (ex-info "[wrk] missing required option" {:option k}))))

  (let [which (shell ["which" "wrk"])]
    (when-not (:okay? which)
      (throw (ex-info "[wrk] `which wrk` failed. ensure wrk is installed (via devshell)."
                      {:which which :fatal? true}))))

  (let [{:keys [out] :as version-r} (shell ["wrk" "--version"])
        version (some->> out (re-find #"wrk\s(.*)\sCopyright") second)]
    (when (nil? version)
      (throw (ex-info "[wrk] `wrk --version` failed" {:result version-r :fatal? true})))

    (let [script-file (java.io.File/createTempFile "ol.busker-wrk-script-" ".lua")]
      (spit script-file (slurp @wrk-script-path_))
      (try
        (let [args (cond-> ["wrk"
                            "--duration" duration
                            "--timeout" timeout
                            "--threads" (str n-threads)
                            "--connections" (str n-conns)
                            "--script" (.getAbsolutePath script-file)
                            (str "http://localhost:" port)]
                     (not keep-alive?) (conj "--header" "Connection: close"))
              {:keys [exit out] :as result} (shell args)]
          (cond
            warm-up?
            (do
              (when (not= 0 exit)
                (throw (ex-info "[wrk] warm-up failed" {:result result})))
              (log "[wrk] Warm-up done")
              result)

            (not (zero? exit))
            (throw (ex-info "[wrk] bench call failed" {:result result :args args :fatal? false}))

            :else
            (if-let [structured (some->> out (re-find #"(?m)^Structured results:(.*)$") second)]
              (let [[lmean lstdev lmin lp50 lp75 lp80 lp90 lp98 lp99 lp99-9 lp99-99 lp99-999 lmax
                     usecs reqs bytes
                     err-connect err-read err-write err-status err-timeout]
                    (str/split structured #",")
                    parse-long #(Long/parseLong %)
                    parse-round #(round0 (Double/parseDouble %))
                    reqs-num (parse-long reqs)
                    bytes-num (parse-long bytes)
                    usecs-num (parse-long usecs)
                    reqs-per-sec (if (pos? usecs-num)
                                   (round0 (* (/ (double reqs-num) usecs-num) 1e6))
                                   0)
                    bytes-per-sec (if (pos? usecs-num)
                                    (round0 (* (/ (double bytes-num) usecs-num) 1e6))
                                    0)
                    errors-map {:connect (parse-long err-connect)
                                :read    (parse-long err-read)
                                :write   (parse-long err-write)
                                :status  (parse-long err-status)
                                :timeout (parse-long err-timeout)}
                    error-total (reduce + (vals errors-map))]
                {:opts {:version version
                        :warm-up warm-up
                        :duration duration
                        :timeout timeout
                        :n-threads n-threads
                        :n-conns n-conns
                        :keep-alive? keep-alive?}
                 :measurements
                 {:reqs-per-sec reqs-per-sec
                  :bytes-per-sec bytes-per-sec
                  :usecs usecs-num
                  :reqs reqs-num
                  :bytes bytes-num
                  :latency {:mean (parse-round lmean)
                            :stdev (parse-round lstdev)
                            :min (parse-round lmin)
                            :p50 (parse-round lp50)
                            :p75 (parse-round lp75)
                            :p80 (parse-round lp80)
                            :p90 (parse-round lp90)
                            :p98 (parse-round lp98)
                            :p99 (parse-round lp99)
                            :p99-9 (parse-round lp99-9)
                            :p99-99 (parse-round lp99-99)
                            :p99-999 (parse-round lp99-999)
                            :max (parse-round lmax)}
                  :errors (assoc errors-map
                                 :total error-total
                                 :rate (when (pos? reqs-num)
                                         (/ (double error-total) (double reqs-num))))}})
              (throw (ex-info "[wrk] bench call succeeded but structured output missing"
                              {:result result :args args :fatal? false})))))
        (finally
          (.delete script-file))))))

;;;; Time plans

(defn time-str->msecs ^long [s]
  (round0
   (or
    (when-let [[_ n] (re-matches #"(\d+(\.\d+)?)s$" s)] (* (Double/parseDouble n) 1e3))
    (when-let [[_ n] (re-matches #"(\d+(\.\d+)?)m$" s)] (* (Double/parseDouble n) 6e4))
    (when-let [[_ n] (re-matches #"(\d+(\.\d+)?)h$" s)] (* (Double/parseDouble n) 3.6e6))
    0.0)))

(defn msecs->secs ^long [n] (round0 (/ (double n) 1e3)))
(defn msecs->mins ^double [n] (round1 (/ (double n) 6e4)))

(defn clamp [n-min n-max n] (max (min n n-max) n-min))

(defn get-wrk-timeplan [{:keys [runtime n-runs] :or {n-runs 1}}]
  (let [total-msecs (time-str->msecs runtime)
        per-run (if (pos? n-runs) (round0 (/ (double total-msecs) n-runs)) total-msecs)
        to-str (fn [msecs] (str (msecs->secs msecs) "s"))
        warm-up (clamp 3000 10000 (* 0.15 per-run))
        shutdown (clamp 3000 20000 (* 0.20 per-run))
        sleep (if (== n-runs 1)
                0
                (clamp 1000 10000 (* 0.05 per-run)))
        duration (max 3000 (- per-run warm-up shutdown sleep))
        total (* n-runs (+ warm-up shutdown sleep duration))]
    {:total-msecs (round0 total)
     :warm-up-msecs (round0 warm-up)
     :duration-msecs (round0 duration)
     :shutdown-msecs (round0 shutdown)
     :sleep-msecs (round0 sleep)
     :total-tstr (str (msecs->mins total) "m")
     :warm-up-tstr (to-str warm-up)
     :duration-tstr (to-str duration)
     :shutdown-tstr (to-str shutdown)
     :sleep-tstr (to-str sleep)}))

;;;; File helpers

(defn get-file [instant filename]
  (let [path (str (or (properties "user.dir") ".") "/benchmarks")
        ts (format-instant instant)
        file (java.io.File. path (str ts "-" filename))]
    (jio/make-parents file)
    file))

(def ^:private ^:dynamic *appender* nil)

(defn with-appender [instant filename header f]
  (let [file_ (delay (get-file instant filename))
        init_ (delay (spit @file_ (str (force header) newline)))
        appender (fn
                   ([] @file_)
                   ([row]
                    @init_
                    (spit @file_ (str row newline) :append true)))]
    (binding [*appender* (or *appender* (if *dry-run?* (constantly nil) appender))]
      [(*appender*) (f)])))

(defn append! [row]
  (when-let [appender *appender*]
    (appender row)))

;;;; Profiles

(defn aborted? [] false)
(defn throw-if-aborted [] nil)

(defn get-profile [profiles profile]
  (cond
    (vector? profile) (get-in profiles profile)
    (keyword? profile) (get profiles profile)
    (map? profile) profile
    :else (throw (ex-info "[get-profile] Unknown profile"
                          {:profile {:value profile :type (type profile)}}))))

;;;; Virtual thread detection (best effort)

(def ^:private have-vts?_
  (delay
    (try
      (let [t (java.lang.Thread/startVirtualThread (fn []))]
        (.join t)
        true)
      (catch Throwable _ false))))

(defn have-virtual-threads? [] (force have-vts?_))

;;;; Placeholder hooks for features we have not ported yet

(defn with-os-tuning [f]
  ;; TODO: BENCHMARK.md documents the missing OS tuning step
  (f))
