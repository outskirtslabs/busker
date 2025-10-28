(ns tasks.build
  (:require
   [babashka.fs          :as fs]
   [babashka.tasks       :as t]
   [borkdude.rewrite-edn :as r]
   [clojure.edn          :as edn]))

;; -----------------------------------------------------------------------------
;; Util
;; -----------------------------------------------------------------------------
(def main-lib-dir                                ".")

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

;; -----------------------------------------------------------------------------
;; Tasks
;; -----------------------------------------------------------------------------
(defn ->deps-file [lib-dir]
  (-> lib-dir
      (fs/path "deps.edn")
      str))

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

