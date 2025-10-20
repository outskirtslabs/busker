(ns ol.h2o.native.loader
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [coffi.ffi :as ffi])
  (:import
   [java.nio.file Files]))

(defn copy-resource [resource-path output-path]
  (with-open [in (io/input-stream (io/resource resource-path))
              out (io/output-stream (io/file output-path))]
    (io/copy in out)))

(defn get-os-arch []
  (let [os-name (str/lower-case (System/getProperty "os.name"))
        os (cond (str/includes? os-name "win") "windows"
                 (str/includes? os-name "nux") "linux"
                 (str/includes? os-name "mac") "macos")
        arch-raw (System/getProperty "os.arch")
        arch (cond (= arch-raw "amd64") "x86-64"
                   (= arch-raw "x86_64") "x86-64"
                   (= arch-raw "aarch64") "aarch64"
                   :else arch-raw)]
    (str os "-" arch)))

(defn load-bundled-library []
  (let [platform (get-os-arch)
        _ (when (str/includes? platform "windows") (throw (Exception. "Windows is not supported, please use wsl.")))
        lib-name (if (str/includes? platform "macos")
                   "libh2oclj.dylib"
                   "libh2oclj.so")
        resource-path (str platform "/" lib-name)
        temp-dir (System/getProperty "java.io.tmpdir")
        temp-lib-path (str temp-dir "/h2oclj_" (System/currentTimeMillis) "_" lib-name)]
    #_(println "resource path" resource-path)
    (try
      (copy-resource resource-path temp-lib-path)
      (ffi/load-library temp-lib-path)
      (finally
        (try
          (Files/deleteIfExists (.toPath (io/file temp-lib-path)))
          (catch Exception _))))))

(if-let [lib-path (System/getProperty "ol.libh2oclj.path")]
  (ffi/load-library lib-path)
  (load-bundled-library))
