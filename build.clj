(ns build
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [clojure.tools.build.api :as b]))

(def project (-> (edn/read-string (slurp "deps.edn"))
                 :aliases :neil :project))
(def lib (:name project))
(def version (:version project))
(def description (:description project))

(defn- git-origin-url []
  (try
    (some-> (b/git-process {:git-args "remote get-url origin"})
            str/trim
            (str/replace #"\.git$" ""))
    (catch Exception _
      nil)))

(defn- git-rev []
  (or (some-> (System/getenv "GIT_REV")
              str/trim
              not-empty)
      (some-> (b/git-process {:git-args "rev-parse HEAD"})
              str/trim
              not-empty)))

(def repo-url-prefix (or (:url project) (git-origin-url)))
(def scm (:scm project))
(def rev (git-rev))

(assert lib ":name must be set in deps.edn under the :neil alias")
(assert version ":version must be set in deps.edn under the :neil alias")
(assert description ":description must be set in deps.edn under the :neil alias")
(assert repo-url-prefix "Either :url must be set in deps.edn under the :neil alias or git remote origin must exist")
(assert scm ":scm must be set in deps.edn under the :neil alias")
(assert rev "Either GIT_REV must be set or git rev-parse HEAD must succeed")

(def class-dir "target/classes")
(def basis_ (delay (b/create-basis {:project "deps.edn"})))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(def ^:private shim-resource-dirs
  ["shim/linux-aarch64/resources"
   "shim/linux-x86-64/resources"
   "shim/macos-aarch64/resources"
   "shim/macos-x86-64/resources"])

(defn- existing-paths [paths]
  (->> paths
       (filter #(.exists (io/file %)))
       vec))

(defn- jar-src-dirs []
  (into (existing-paths ["src/main/clojure" "resources"])
        (existing-paths shim-resource-dirs)))

(defn clean [_]
  (b/delete {:path "target"}))

(defn permalink [subpath]
  (str repo-url-prefix "/blob/" rev "/" subpath))

(defn compile-shim
  "Compile the native shim for the current host platform."
  [_]
  (let [optimize (or (some-> (System/getenv "BUSKER_ZIG_OPTIMIZE")
                             str/trim
                             not-empty)
                     "ReleaseSafe")
        build-result (shell/sh "zig"
                               "build"
                               "-Dall-targets=false"
                               (str "-Doptimize=" optimize)
                               :dir "shim")]
    (when (not= 0 (:exit build-result))
      (println (:out build-result))
      (println (:err build-result))
      (throw (ex-info "Zig build failed" build-result)))))

(defn jar [_]
  (clean nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis_
                :src-dirs (existing-paths ["src/main/clojure"])
                :pom-data [[:description description]
                           [:url repo-url-prefix]
                           [:licenses
                            [:license
                             [:name "EUPL-1.2"]
                             [:url (permalink "LICENSE")]]]
                           (conj scm [:tag rev])]})
  (b/copy-dir {:src-dirs (jar-src-dirs)
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn install [_]
  (jar {})
  (b/install {:basis @basis_
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir}))

(defn deploy [opts]
  (jar opts)
  ((requiring-resolve 'deps-deploy.deps-deploy/deploy)
   (merge {:installer :remote
           :artifact jar-file
           :pom-file (b/pom-path {:lib lib :class-dir class-dir})}
          opts))
  opts)
