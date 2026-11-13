(ns ol.busker.packaged-library-test
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker.native.loader :as loader])
  (:import
   [java.io File]
   [java.nio ByteBuffer ByteOrder]
   [java.nio.file Files]
   [java.security MessageDigest]
   [java.util.concurrent TimeUnit]))

(defn- object-identity
  "Returns the object format and machine of the shared library `bytes`."
  [^bytes bytes]
  (let [buffer (.order (ByteBuffer/wrap bytes) ByteOrder/LITTLE_ENDIAN)]
    (cond
      (= [0x7f 0x45 0x4c 0x46] (mapv #(bit-and 0xff %) (take 4 bytes)))
      [:elf ({62 :x86-64 183 :aarch64} (.getShort buffer 18))]
      (= (unchecked-int 0xfeedfacf) (.getInt buffer 0))
      [:mach-o ({0x01000007 :x86-64 0x0100000c :aarch64} (.getInt buffer 4))]
      :else [:unknown nil])))

(defn- sha-256 [^bytes bytes]
  (vec (.digest (MessageDigest/getInstance "SHA-256") bytes)))

;; Only the host target can be loaded here; the other three are checked by
;; object format and machine.
(deftest packaged-targets-match-their-platforms
  (doseq [[target library identity] [["linux-x86-64" "libh2oclj.so" [:elf :x86-64]]
                                     ["linux-aarch64" "libh2oclj.so" [:elf :aarch64]]
                                     ["macos-x86-64" "libh2oclj.dylib" [:mach-o :x86-64]]
                                     ["macos-aarch64" "libh2oclj.dylib" [:mach-o :aarch64]]]]
    (testing target
      (let [file (io/file "shim" target "resources" target library)]
        (is (.isFile file))
        (when (.isFile file)
          (let [bytes (Files/readAllBytes (.toPath file))]
            (is (= identity (object-identity bytes)))
            (when (= target (loader/get-os-arch))
              (is (= (sha-256 bytes)
                     (sha-256 (with-open [in (io/input-stream (io/resource (str target "/" library)))]
                                (.readAllBytes in))))
                  "The default loaded resource is the built host library."))))))))

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
