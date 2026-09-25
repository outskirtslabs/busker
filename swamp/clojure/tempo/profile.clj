(ns tempo.profile
  "Collects diagnostic stacks with the local benchmark workload."
  (:require
   [capra.benchmark :as benchmark]
   [clj-async-profiler.core :as profiler]
   [clojure.data.json :as json]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str])
  (:import
   [java.io File]
   [java.lang ProcessHandle]
   [java.nio.file AccessDeniedException Files NoSuchFileException]
   [java.lang.management ManagementFactory]
   [jdk.jfr Configuration Recording]))

(set! *warn-on-reflection* true)

(defn- write-json! [file data]
  (spit file (json/write-str data)))

(defn- optional-text [file]
  (try
    (Files/readString (.toPath (io/file file)))
    (catch NoSuchFileException _ nil)
    (catch AccessDeniedException _ nil)))

(defn- raw-process [directory]
  (into {} (map (fn [name] [(keyword name) (optional-text (io/file directory name))]))
        ["stat" "status" "schedstat"]))

(defn- counters []
  (let [pid (.pid (ProcessHandle/current))
        root (io/file "/proc" (str pid))
        process (raw-process root)
        membership (optional-text (io/file root "cgroup"))
        group (some #(second (re-matches #"0::(/.*)" %)) (str/split-lines (or membership "")))
        group (when (and group (not (some #{".."} (str/split group #"/")))) group)]
    (when-not (:stat process)
      (throw (ex-info "Cannot read server CPU counters" {:pid pid})))
    {:captured-at-ms (System/currentTimeMillis)
     :allocated-bytes (let [bean (ManagementFactory/getThreadMXBean)]
                        (when (instance? com.sun.management.ThreadMXBean bean)
                          (let [n (.getTotalThreadAllocatedBytes ^com.sun.management.ThreadMXBean bean)]
                            (when (not (neg? n)) n))))
     :process process
     :threads (into {} (map (fn [^File directory] [(.getName directory) (raw-process directory)]))
                    (.listFiles (io/file root "task")))
     :host-pressure (optional-text "/proc/pressure/cpu")
     :cgroup-path group
     :cgroup-stat (when group (optional-text (str "/sys/fs/cgroup" group "/cpu.stat")))
     :cgroup-pressure (when group (optional-text (str "/sys/fs/cgroup" group "/cpu.pressure")))}))

(defn- environment! []
  (let [environment (#'benchmark/environment)
        explicit (System/getProperty "ol.libh2oclj.path")
        resource (io/file (java.net.URI. ^String (:native-resource environment)))
        mappings (Files/readString (.toPath (io/file "/proc/self/maps")))]
    (when-not (and explicit
                   (= (.getCanonicalFile (io/file explicit)) (.getCanonicalFile resource))
                   (some #(str/ends-with? % (str " " (.getCanonicalPath resource)))
                         (str/split-lines mappings)))
      (throw (ex-info "Profiling requires the checksummed local native file in the live process mappings"
                      {:explicit explicit :resource (str resource)})))
    (assoc environment
           :native-mappings (filterv #(str/includes? % "h2oclj") (str/split-lines mappings))
           :garbage-collectors (mapv #(.getName ^java.lang.management.GarbageCollectorMXBean %)
                                     (ManagementFactory/getGarbageCollectorMXBeans)))))

(defn- record-stacks! [event directory f]
  (profiler/start {:event event :threads true :interval 1000000})
  (let [sample (try {:result (f)} (catch Exception failure {:failure failure}))
        stopped (try {:file (profiler/stop {:generate-flamegraph? false})}
                     (catch Exception failure {:failure failure}))]
    (when-let [file (:file stopped)]
      (io/copy (io/file file) (io/file directory "profile.collapsed")))
    (when-let [^Exception failure (or (:failure sample) (:failure stopped))]
      (when (and (:failure sample) (:failure stopped))
        (.addSuppressed failure ^Exception (:failure stopped)))
      (throw failure))
    (io/copy (profiler/generate-flamegraph (:file stopped) {}) (io/file directory "profile.html"))
    (:result sample)))

(defn- record! [event directory f]
  (if (= :jfr event)
    (with-open [recording (Recording. (Configuration/getConfiguration "profile"))]
      (let [file (io/file directory "profile.jfr")]
        (.withPeriod (.enable recording "jdk.ThreadAllocationStatistics") (java.time.Duration/ofSeconds 1))
        (.start recording)
        (let [result (try (f) (finally (.stop recording) (.dump recording (.toPath file))))]
          (#'benchmark/command! ["jfr" "print" "--json" "--events"
                                 "jdk.ThreadAllocationStatistics,jdk.ObjectAllocationSample,jdk.GarbageCollection,jdk.GCPhasePause,jdk.JavaMonitorEnter,jdk.ThreadPark,jdk.Compilation,jdk.VirtualThreadPinned,jdk.CPULoad"
                                 (str file)] (str directory "/jfr-events.json") 30)
          result)))
    (record-stacks! event directory f)))

(defn- collect! [{:keys [directory event protocol warmup duration] :as options}]
  (when-not (#{:ctimer :wall :alloc :jfr} event)
    (throw (ex-info "Unsupported recording event" {:event event})))
  (when-not (and (#{:h1 :tls-h1 :tls-h2} protocol)
                 (every? #(and (integer? %) (pos? %))
                         (map options [:warmup :duration :connections :streams :threads]))
                 (or (not= protocol :tls-h2)
                     (zero? (mod (:connections options) (:streams options)))))
    (throw (ex-info "Invalid profile workload settings" {})))
  (let [environment (assoc (environment!) :h2load-version
                           (str/trim (#'benchmark/command! ["h2load" "--version"]
                                                           (str directory "/h2load-version.log") 10)))
        fixtures (when-not (= :h1 protocol) (#'benchmark/fixtures! directory))
        stop (#'benchmark/start-server :busker protocol 5800 fixtures)]
    (try
      (#'benchmark/await-ready! protocol 5800 fixtures)
      (#'benchmark/record-affinity! directory "ready")
      (#'benchmark/measurements
       (#'benchmark/command! (#'benchmark/load-command protocol 5800 warmup options)
                             (str directory "/warmup.log") (+ warmup 30)) protocol)
      (#'benchmark/record-affinity! directory "measurement-start")
      (let [result (record!
                    event directory
                    (fn []
                      (write-json! (io/file directory "before.json") (counters))
                      (let [output (try
                                     (#'benchmark/command!
                                      (into ["time" "-f" "{\"userSeconds\":%U,\"systemSeconds\":%S,\"voluntarySwitches\":%w,\"involuntarySwitches\":%c}"
                                             "-o" (str directory "/client-cpu.json")]
                                            (#'benchmark/load-command protocol 5800 duration options))
                                      (str directory "/load.log") (+ duration 30))
                                     (finally (write-json! (io/file directory "after.json") (counters))))]
                        (#'benchmark/measurements output protocol))))]
        (#'benchmark/check-response! protocol 5800 fixtures)
        (#'benchmark/record-affinity! directory "measurement-end")
        {:kind :profile :score nil :event event :protocol protocol
         :environment environment :parameters (dissoc options :directory :event :protocol)
         :load result :profiler-version (#'benchmark/version ["com.clojure-goes-fast" "clj-async-profiler"])
         :clock-ticks-per-second (parse-long (str/trim (#'benchmark/command! ["getconf" "CLK_TCK"]
                                                                             (str directory "/clock-ticks.log") 10)))})
      (finally (stop)))))

(defn -main
  "Reads one EDN request file and saves a diagnostic recording in its output directory."
  [request-file]
  (let [options (edn/read-string (slurp request-file))
        result-file (io/file (:directory options) "profile.json")]
    (try
      (write-json! result-file (collect! options))
      (catch Exception failure
        (write-json! result-file {:kind :profile :score nil :error (str failure)})
        (throw failure))
      (finally (shutdown-agents)))))
