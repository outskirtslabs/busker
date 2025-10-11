(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]))

(def lib 'io.cljnet/http-clj)
(def class-dir "target/classes")

(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn system-includes
  "Get system include paths from gcc."
  []
  (let [cmd                    ["gcc" "-E" "-Wp,-v" "-xc" "/dev/null"]
        {:keys [err out exit]} (b/process {:command-args cmd
                                           :out          :capture
                                           :err          :capture})]
    (if (zero? exit)
      (-> err
          str/split-lines
          (->> (drop-while #(not= % "#include <...> search starts here:"))
               (drop 1)
               (take-while #(not= % "End of search list."))
               (map str/trim)))
      (throw (ex-info "Failed to find system includes"
                      {:command-was (str/join " " cmd)
                       :out         out
                       :err         err
                       :exit        exit})))))

(defn h2o-include-dir
  "Find h2o.h include directory from CMAKE_INCLUDE_PATH."
  []
  (let [cmake-path (System/getenv "CMAKE_INCLUDE_PATH")]
    (when-not cmake-path
      (throw (ex-info "CMAKE_INCLUDE_PATH not set" {})))
    (let [paths (str/split cmake-path #":")]
      (or (some #(when (.exists (clojure.java.io/file % "h2o.h")) %) paths)
          (throw (ex-info "h2o.h not found in CMAKE_INCLUDE_PATH"
                          {:cmake-include-path cmake-path}))))))

(defn -jextract
  "Run jextract with given args."
  [{:keys [args]}]
  (let [cmd                            (concat ["./jextract/jextract-22/bin/jextract"] args)
        {:keys [out exit err] :as ret} (b/process {:command-args cmd
                                                   :out          :capture
                                                   :err          :capture})]
    (when-not (zero? exit)
      (println "JEXTRACT STDOUT:" out)
      (println "JEXTRACT STDERR:" err)
      (throw (ex-info "jextract failed"
                      {:command-was (str/join " " cmd)
                       :out         out
                       :err         err
                       :exit        exit})))
    ret))

(defn jextract-h2o
  "Generate Java bindings for h2o structs using jextract.
   Generates all structs to avoid dependency issues."
  [_]
  (println "Generating Java bindings from h2o headers...")
  (b/delete {:path "target/jextract"})

  (let [h2o-include (h2o-include-dir)
        sys-includes (system-includes)
        include-args (mapcat (fn [path] ["--include-dir" path]) sys-includes)
        ;; Generate everything - simpler than tracking dependencies
        args (concat include-args
                     ["--include-dir" h2o-include
                      "--output" "target/jextract"
                      "--target-package" "org.h2o.generated"
                      (str h2o-include "/h2o.h")])]

    (-jextract {:args args})
    (println "Generated Java bindings in target/jextract")))

(defn compile-jextract
  "Compile jextract-generated Java sources."
  [_]
  (println "Compiling jextract-generated Java...")
  (b/javac {:src-dirs ["target/jextract"]
            :class-dir class-dir
            :basis @basis
            :javac-opts ["--release" "25" "--enable-preview"]})
  (println "Compiled Java classes to" class-dir))

(defn build-all
  "Run jextract and compile Java."
  [opts]
  (jextract-h2o opts)
  (compile-jextract opts))
