(ns tasks.build
  (:require
   [babashka.fs          :as fs]
   [babashka.tasks       :as t]
   [borkdude.rewrite-edn :as r]
   [clojure.edn          :as edn]
   [clojure.string       :as str])
  (:import
   [java.net HttpURLConnection URI]))

;; Util

(def main-lib-dir ".")

(def lib-dirs
  ["shim/linux-aarch64"
   "shim/linux-x86-64"
   "shim/macos-aarch64"
   "shim/macos-x86-64"])

(def lib-maven-names
  '#{com.outskirtslabs/busker
     com.outskirtslabs.busker/linux-aarch64
     com.outskirtslabs.busker/linux-x86-64
     com.outskirtslabs.busker/macos-aarch64
     com.outskirtslabs.busker/macos-x86-64})

(defn ->deps-file [lib-dir]
  (-> lib-dir
      (fs/path "deps.edn")
      str))

(defn- lib-project [dir]
  (-> dir
      ->deps-file
      slurp
      edn/read-string
      :aliases
      :neil
      :project))

(defn artifact-url
  "Returns the remote Maven artifact URL for `lib`, `version`, and `extension`."
  ([lib version extension]
   (artifact-url "https://clojars.org/repo" lib version extension))
  ([repo-url lib version extension]
   (let [group-id (namespace lib)
         artifact-id (name lib)]
     (when (or (str/blank? group-id) (str/blank? artifact-id))
       (throw (ex-info "Maven lib must include group and artifact ids"
                       {:lib lib})))
     (format "%s/%s/%s/%s/%s-%s.%s"
             (str/replace repo-url #"/+$" "")
             (str/replace group-id "." "/")
             artifact-id
             version
             artifact-id
             version
             extension))))

(defn- remote-status [url]
  (let [conn (.openConnection (.toURL (URI/create url)))]
    (when-not (instance? HttpURLConnection conn)
      (throw (ex-info "Artifact URL must use HTTP or HTTPS"
                      {:url url})))
    (let [conn ^HttpURLConnection conn]
      (try
        (.setRequestMethod conn "HEAD")
        (.setConnectTimeout conn 10000)
        (.setReadTimeout conn 10000)
        (.setInstanceFollowRedirects conn true)
        (.getResponseCode conn)
        (finally
          (.disconnect conn))))))

(defn remote-file-exists?
  "Returns true when `url` exists remotely, and false when it returns 404."
  [url]
  (let [status (remote-status url)]
    (case status
      200 true
      404 false
      (throw (ex-info "Unexpected artifact URL status"
                      {:url url
                       :status status})))))

(defn remote-publish-state
  "Returns the remote publication state for the library in `dir`."
  [dir]
  (let [{lib :name version :version} (lib-project dir)
        urls {:pom (artifact-url lib version "pom")
              :jar (artifact-url lib version "jar")}
        exists (into {}
                     (map (fn [[k url]]
                            [k (remote-file-exists? url)])
                          urls))
        state-data {:dir dir
                    :lib lib
                    :version version
                    :urls urls
                    :exists exists}]
    (cond
      (every? true? (vals exists))
      (assoc state-data :state :published)

      (every? false? (vals exists))
      (assoc state-data :state :missing)

      :else
      (throw (ex-info "Partially published Maven coordinate"
                      state-data)))))

;; Tasks

(defn assoc-deps!
  "Adds libraries interdependencies in deps.edn files"
  [lib-dir _version]
  (let [deps-file (->deps-file lib-dir)]
    (-> deps-file
        slurp
        r/parse-string
        str
        (->> (spit deps-file)))))

(defn current-version []
  (-> (t/shell {:dir main-lib-dir :out :string} "neil" "version")
      :out
      edn/read-string
      :project))

(defn lib-bump! [dir component]
  (when-not (contains? #{"major" "minor" "patch"} component)
    (println (str "ERROR: First argument must be one of: major, minor, patch. Got: " (or component "nil")))
    (System/exit 1))

  (t/shell {:dir dir} (str "neil version " component " --no-tag"))

  (assoc-deps! dir (current-version)))

(defn lib-set-version! [dir version]
  (t/shell {:dir dir} (str "neil version set " version " --no-tag"))

  (assoc-deps! dir version))

(defn lib-clean! [dir]
  (t/clojure {:dir dir} "-T:build clean"))

(defn lib-jar! [dir]
  (println "----------------")
  (println "Building" dir)
  (println "----------------")
  (t/clojure {:dir dir} "-T:build jar"))

(defn lib-install! [dir]
  (println "----------------")
  (println "Installing" dir)
  (println "----------------")
  (t/clojure {:dir dir} "-T:build install"))

(defn lib-publish! [dir]
  (t/clojure {:dir dir} "-T:build deploy"))

(defn- publish-dir! [dir]
  (let [{:keys [state lib version]} (remote-publish-state dir)
        artifact (str lib ":" version)]
    (case state
      :published
      (println "Already published, skipping" artifact)

      :missing
      (try
        (lib-publish! dir)
        (catch Throwable e
          (let [{post-state :state} (remote-publish-state dir)]
            (if (= :published post-state)
              (println "Artifact is now published, continuing" artifact)
              (throw e))))))))

(defn publish-libs!
  "Publishes each library in `dirs`, skipping artifacts that already exist remotely."
  [dirs]
  (let [failures (reduce (fn [failures dir]
                           (try
                             (publish-dir! dir)
                             failures
                             (catch Throwable e
                               (println "Failed to publish" dir ":" (ex-message e))
                               (conj failures {:dir dir
                                               :message (ex-message e)
                                               :error e}))))
                         []
                         dirs)]
    (when (seq failures)
      (throw (ex-info "One or more native artifacts failed to publish"
                      {:failures (mapv #(select-keys % [:dir :message]) failures)}
                      (:error (first failures)))))
    nil))

