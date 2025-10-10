(ns ol.h2o.native
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [coffi.ffi :as ffi :refer [defcfn]]
   [coffi.mem :as mem])
  (:import [java.nio.file Files]))

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

(defn load-bundled-library []
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

(defn load-system-library []
  (ffi/load-system-library "libh2o"))

(let [src (System/getProperty "ol.libh2o.path")
      src "/home/ramblurr/src/ol/http/libs/h2o/cmake-build-debug/libh2oclj.so"]
  (cond
    ;; default to bundled
    (or (nil? src)
        (= src "bundled")) (load-bundled-library)
    (= src "system") (load-system-library)
    :else
    (ffi/load-library src)))

(defcfn evloop-create
  "Creates a new event loop. Returns a pointer to h2o_evloop_t."
  h2o_evloop_create
  [] ::mem/pointer)

(defcfn evloop-destroy
  "Destroys an event loop and frees associated resources."
  h2o_evloop_destroy
  [::mem/pointer] ::mem/void)

(defcfn evloop-run
  "Runs the event loop once. Returns 0 if successful, -1 on error (typically EINTR).
   
   Parameters:
   - loop: pointer to h2o_evloop_t
   - max-wait: maximum time to wait in milliseconds (int32)"
  h2o_evloop_run
  [::mem/pointer ::mem/int] ::mem/int)

(defcfn evloop-get-time
  "Gets the current time in milliseconds from the event loop."
  h2o_now
  [::mem/pointer] ::mem/long)

(defcfn evloop-get-time-nanosec
  "Gets the current time in nanoseconds from the event loop."
  h2o_now_nanosec
  [::mem/pointer] ::mem/long)
