(ns ol.h2o.native.loader
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [coffi.ffi :as ffi]))

(defn copy-resource [resource-path output-path]
  (with-open [in (io/input-stream (io/resource resource-path))
              out (io/output-stream (io/file output-path))]
    (io/copy in out)))

(defn get-arch+os []
  (let [os-name (str/lower-case (System/getProperty "os.name"))]
    (str (System/getProperty "os.arch") "-"
         (cond (str/includes? os-name "win") "windows"
               (str/includes? os-name "nux") "linux"
               (str/includes? os-name "mac") "macos"))))

#_(defn load-bundled-library []
    (let [res-file (case (get-arch+os)
                     "aarch64-linux" "libh2o_aarch64-linux-gnu.so"
                     "aarch64-macos" "libh2o_aarch64-macos-none.so"
                     ("x86-linux"
                      "amd64-linux") "libh2o_x86_64-linux-gnu.so"
                     ("x86-macos"
                      "amd64-macos") "libh2o_x86_64-macos-none.so"
                     ("x86-windows"
                      "amd64-windows") "libh2o_x86_64-windows-gnu.dll")
          temp-lib-filename (str "h2oclj_temp_" res-file)]
      (copy-resource res-file temp-lib-filename)
      (ffi/load-library temp-lib-filename)
      (Files/deleteIfExists (.toPath (io/file temp-lib-filename)))))

#_(defn load-system-library []
    (ffi/load-system-library "libh2o"))

;; (ffi/load-library (System/getProperty "ol.libh2o.path"))
(try
  (ffi/load-library "zig-out/lib/libh2o-evloop.so")
  (ffi/load-library "zig-out/lib/libh2oclj.so")
  (catch UnsatisfiedLinkError e
    (throw (Exception. "unable to load native h2o library; is it installed?" e))))
