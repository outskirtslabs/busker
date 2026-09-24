(ns ol.busker.packaged-library-test
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [clojure.test :refer [deftest is]])
  (:import
   [java.io File]
   [java.util.concurrent TimeUnit]))

(deftest packaged-library-loads-reloads-and-stops
  (let [{:keys [exit out err]} (shell/sh "clojure" "-Srepro" "-T:build" "jar")]
    (is (zero? exit) (str out err))
    (when (zero? exit)
      (let [project (-> (slurp "deps.edn") edn/read-string :aliases :neil :project)
            jar (io/file "target" (str (name (:name project)) "-" (:version project) ".jar"))
            excluded (set (map #(.getCanonicalPath (io/file %))
                               ["src/main/clojure" "target/classes"
                                "shim/linux-x86-64/resources" "shim/linux-aarch64/resources"
                                "shim/macos-x86-64/resources" "shim/macos-aarch64/resources"]))
            classpath (str/join File/pathSeparator
                                (cons (.getCanonicalPath jar)
                                      (remove #(contains? excluded (.getCanonicalPath (io/file %)))
                                              (str/split (System/getProperty "java.class.path")
                                                         (re-pattern File/pathSeparator)))))
            form '(do
                    (require '[clojure.java.io :as io]
                             '[clojure.test :as test]
                             'ol.busker.native-test
                             'ol.busker.reload-test)
                    (assert (.startsWith (str (io/resource "ol/busker/native.clj")) "jar:"))
                    (binding [test/*report-counters* (ref test/*initial-report-counters*)]
                      (test/test-vars
                       [#'ol.busker.native-test/bundled-library-remains-callable-after-extraction
                        #'ol.busker.native-test/native-library-override-and-failures
                        #'ol.busker.native-test/namespace-reload-retains-loaded-library
                        #'ol.busker.reload-test/reload-activates-new-generation-and-drains-old-test])
                      (let [{:keys [fail error] :as result} @test/*report-counters*]
                        (prn result)
                        (System/exit (if (zero? (+ fail error)) 0 1)))))
            output (File/createTempFile "busker-package-" ".log")
            process (-> (ProcessBuilder.
                         ^java.util.List [(str (System/getProperty "java.home") "/bin/java")
                                          "--enable-native-access=ALL-UNNAMED"
                                          "-cp" classpath "clojure.main" "-e" (pr-str form)])
                        (.redirectErrorStream true)
                        (.redirectOutput output)
                        (.start))]
        (try
          (let [finished? (.waitFor process 120 TimeUnit/SECONDS)]
            (is finished? (slurp output))
            (when finished?
              (is (zero? (.exitValue process)) (slurp output))))
          (finally
            (.destroyForcibly process)
            (.delete output)))))))
