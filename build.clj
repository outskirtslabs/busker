(ns build
  (:require
   [clojure.string :as str]
   [clojure.tools.build.api :as b]
   [clojure.java.shell :as shell]
   [clojure.edn :as edn]))

(def project (-> (edn/read-string (slurp "deps.edn"))
                 :aliases :neil :project))
(def repo-url-prefix (:url project))
(def scm (:scm project))
(def rev (str/trim (b/git-process {:git-args "rev-parse HEAD"})))
(def lib (:name project))
(def version (:version project))
(def description (:description project))
(assert lib ":name must be set in deps.edn under the :neil alias")
(assert version ":version must be set in deps.edn under the :neil alias")
(assert description ":description must be set in deps.edn under the :neil alias")

(def class-dir "target/classes")
(def basis (delay (b/create-basis {:project "deps.edn"})))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn permalink [subpath]
  (str repo-url-prefix "/blob/" rev "/" subpath))

(defn compile-shim
  "Compile C shim library using Zig."
  [_]
  (println "Compiling C shim library...")
  (let [build-result (shell/sh "zig" "build" "-Doptimize=Debug" :dir "shim/")]
    (when (not= 0 (:exit build-result))
      (println (:err build-result))
      (throw (ex-info "Zig build failed" build-result)))
    (println "Compiled C shim to zig-out/lib/libh2oclj.so")))

(defn jar [_]
  (clean nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["resources"]
                :pom-data [[:description description]
                           [:url repo-url-prefix]
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
