(ns tasks.pebble
  (:require
   [babashka.fs :as fs]
   [babashka.process :as p]
   [clojure.java.io :as io]
   [clojure.string :as str]))

(defn- env
  [name default]
  (let [value (System/getenv name)]
    (if (str/blank? value)
      default
      value)))

(defn- root-dir []
  (str (fs/cwd)))

(defn- state-dir []
  (env "BUSKER_PEBBLE_STATE_DIR" (str (fs/path (root-dir) "target" "pebble"))))

(defn- pid-file []
  (str (fs/path (state-dir) "pebble.pid")))

(defn- log-file []
  (str (fs/path (state-dir) "pebble.log")))

(defn- config-file []
  (env "BUSKER_PEBBLE_CONFIG"
       (str (fs/path (root-dir) "src" "test" "fixtures" "pebble-config.json"))))

(defn directory-url
  []
  (env "BUSKER_PEBBLE_DIRECTORY_URL" "https://localhost:14000/dir"))

(defn- parse-pid
  [s]
  (let [trimmed (some-> s str str/trim)]
    (when (and (some? trimmed)
               (re-matches #"\d+" trimmed))
      (Long/parseLong trimmed))))

(defn- read-pid
  []
  (let [path (pid-file)]
    (when (fs/exists? path)
      (parse-pid (slurp path)))))

(defn- pid-alive?
  [pid]
  (boolean
   (and (some? pid)
        (zero? (:exit (p/sh {:continue true}
                            "kill" "-0" (str pid)))))))

(defn running?
  []
  (when-let [pid (read-pid)]
    (when (pid-alive? pid)
      pid)))

(defn- ensure-pebble-binary!
  []
  (let [{:keys [exit]} (p/sh {:continue true}
                             "pebble" "-version")]
    (when-not (zero? exit)
      (throw (ex-info "pebble binary is not on PATH." {})))))

(defn- wait-ready?
  [pid]
  (loop [remaining 120]
    (when (pos? remaining)
      (if-not (pid-alive? pid)
        false
        (let [{:keys [exit]} (p/sh {:continue true}
                                   "curl" "-kfsS" (directory-url))]
          (if (zero? exit)
            true
            (do
              (Thread/sleep 100)
              (recur (dec remaining)))))))))

(defn- launch-pebble!
  []
  (let [{:keys [exit out err]}
        (p/sh {:continue true
               :out :string
               :err :string
               :extra-env {"PEBBLE_VA_NOSLEEP" (env "PEBBLE_VA_NOSLEEP" "1")
                           "PEBBLE_VA_ALWAYS_VALID"
                           (env "PEBBLE_VA_ALWAYS_VALID" "1")}}
              "bash" "-c"
              "if command -v setsid >/dev/null 2>&1; then setsid pebble -config \"$1\" </dev/null >>\"$2\" 2>&1 & else nohup pebble -config \"$1\" </dev/null >>\"$2\" 2>&1 & fi; echo $!"
              "pebble-launch" (config-file) (log-file))
        pid (parse-pid out)]
    (when (or (not (zero? exit)) (nil? pid))
      (throw (ex-info "Failed to launch pebble."
                      {:exit exit :out out :err err})))
    pid))

(defn stop!
  []
  (if-let [pid (running?)]
    (do
      (p/sh {:continue true}
            "kill" (str pid))
      (loop [attempts 30]
        (when (and (pos? attempts) (pid-alive? pid))
          (Thread/sleep 100)
          (recur (dec attempts))))
      (when (pid-alive? pid)
        (p/sh {:continue true}
              "kill" "-9" (str pid)))
      (fs/delete-if-exists (pid-file))
      (println (str "Stopped Pebble PID " pid ".")))
    (do
      (fs/delete-if-exists (pid-file))
      (println "Pebble is not running.")))
  nil)

(defn status!
  []
  (if-let [pid (running?)]
    (do
      (println (str "Pebble is running with PID " pid "."))
      (println (str "Directory: " (directory-url)))
      (println (str "Log: " (log-file))))
    (println "Pebble is not running."))
  nil)

(defn logs!
  []
  (if (fs/exists? (log-file))
    (with-open [rdr (io/reader (log-file))]
      (doseq [line (take-last 200 (line-seq rdr))]
        (println line)))
    (println (str "No log file found at " (log-file))))
  nil)

(defn start!
  []
  (ensure-pebble-binary!)
  (if-let [pid (running?)]
    (println (str "Pebble is already running with PID " pid "."))
    (do
      (when-not (fs/exists? (config-file))
        (throw (ex-info (str "Pebble config file not found: " (config-file)) {})))
      (when (and (fs/exists? (pid-file))
                 (nil? (running?)))
        (fs/delete-if-exists (pid-file)))
      (fs/create-dirs (state-dir))
      (let [pid (launch-pebble!)]
        (spit (pid-file) (str pid))
        (if (wait-ready? pid)
          (do
            (println "Started Pebble.")
            (println (str "PID: " pid))
            (println (str "Config: " (config-file)))
            (println (str "Directory: " (directory-url)))
            (println (str "Log: " (log-file))))
          (do
            (println "Pebble did not become ready.")
            (logs!)
            (stop!)
            (throw (ex-info "Pebble did not become ready." {})))))))
  nil)

(defn restart!
  []
  (stop!)
  (start!)
  nil)
