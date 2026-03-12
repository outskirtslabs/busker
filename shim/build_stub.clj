(require
 '[clojure.string :as str]
 '[clojure.tools.build.api :as b]
 '[clojure.edn :as edn])

(def root-project (-> (edn/read-string (slurp "../../deps.edn"))
                      :aliases :neil :project))
(def repo-url-prefix (:url root-project))
(def scm (:scm root-project))
(def project (-> (edn/read-string (slurp "deps.edn"))
                 :aliases :neil :project))
(def cwd (-> (java.io.File. ".")  .getCanonicalFile .getName))
(defn- git-rev []
  (or (some-> (System/getenv "GIT_REV")
              str/trim
              not-empty)
      (some-> (b/git-process {:git-args "rev-parse HEAD"})
              str/trim
              not-empty)))

(def rev (git-rev))
(def lib (:name project))
(def version (:version project))
(def description (:description project))
(assert lib ":name must be set in deps.edn under the :neil alias")
(assert version ":version must be set in deps.edn under the :neil alias")
(assert description ":description must be set in deps.edn under the :neil alias")
(assert rev "Either GIT_REV must be set or git rev-parse HEAD must succeed")

(def class-dir "target/classes")
(def basis (delay (b/create-basis {:project "deps.edn"})))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn permalink [subpath]
  (str repo-url-prefix "/blob/" rev "/" subpath))

(defn jar [_]
  (clean nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["resources"]
                :pom-data [[:description description]
                           [:url (permalink (str "shim/" cwd))]
                           [:licenses
                            [:license
                             [:name "EUPL-1.2"]
                             [:url (permalink "LICENSE")]]]
                           (conj scm [:tag (str "v" version)])]})
  (b/copy-dir {:src-dirs ["resources"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn install [_]
  (jar {})
  (b/install {:basis @basis
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
