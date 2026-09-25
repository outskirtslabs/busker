(ns capra.benchmark-test
  (:require
   [capra.benchmark :as benchmark]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]))

(def output
  (str "TLS Protocol: TLSv1.3\nCipher: TLS_AES_128_GCM_SHA256\nApplication protocol: h2\n"
       "finished in 1.00s, 100.00 req/s, 1MB/s\n"
       "requests: 100 total, 110 started, 100 done, 100 succeeded, 0 failed, 0 errored, 0 timeout\n"
       "status codes: 105 2xx, 0 3xx, 0 4xx, 0 5xx\n"
       "request     : 1us 9us 3us 7us 8us 4us 1us 99%\n"))

(deftest reads-completed-requests
  (is (= {:requests-per-second 100.0 :requests 100
          :latency {:median "3us" :p95 "7us" :p99 "8us" :mean "4us"}}
         (#'benchmark/measurements output :tls-h2)))
  (testing "response headers can arrive before their bodies finish"
    (is (= 100 (:requests (#'benchmark/measurements output :tls-h2)))))
  (testing "plaintext H1 does not require TLS metadata"
    (is (= 100 (:requests (#'benchmark/measurements
                           (str/replace output "Application protocol: h2" "Application protocol: http/1.1") :h1))))))

(deftest tolerates-interleaved-alpn-diagnostics
  (doseq [interleaved [(str/replace output "TLS Protocol: "
                                    "TLS Protocol: Server does not support ALPN. Falling back to HTTP/1.1.\n")
                       (str/replace output "TLS Protocol: "
                                    "No protocol negotiated. Fallback behaviour may be activatedTLS Protocol: \n")
                       (str/replace output "Application protocol: " "Application protocol: \n")]]
    (is (= 100 (:requests (#'benchmark/measurements interleaved :tls-h2))))))

(deftest rejects-invalid-measurements
  (doseq [[before after] [["0 failed" "1 failed"] ["0 errored" "1 errored"]
                          ["0 timeout" "1 timeout"] ["0 3xx" "1 3xx"]
                          ["0 4xx" "1 4xx"] ["0 5xx" "1 5xx"]
                          ["100 succeeded" "99 succeeded"] ["105 2xx" "99 2xx"]
                          ["105 2xx" "111 2xx"] ["100 done" "99 done"]
                          ["100.00 req/s" "0.00 req/s"] ["protocol: h2" "protocol: http/1.1"]
                          ["TLSv1.3" "TLSv1.2"] ["TLS_AES_128_GCM_SHA256" "other"]]]
    (is (thrown? clojure.lang.ExceptionInfo
                 (#'benchmark/measurements (str/replace output before after) :tls-h2))
        (str before " -> " after)))
  (is (thrown? clojure.lang.ExceptionInfo (#'benchmark/measurements "partial log" :h1))))

(deftest uses-equivalent-concurrency
  (let [h1 (#'benchmark/load-command :h1 5800 30 benchmark/defaults)
        h2 (#'benchmark/load-command :tls-h2 5800 30 benchmark/defaults)]
    (is (= ["-c" "128" "-m" "1" "-t" "2"] (subvec h1 3 9)))
    (is (= ["-c" "2" "-m" "64" "-t" "2"] (subvec h2 3 9)))
    (is (some #{"--h1"} h1))
    (is (some #{"--alpn-list=h2"} h2))))

(deftest aggregates-all-repetitions
  (let [samples (mapv #(hash-map :status "ok" :requests-per-second %) [100.0 300.0 200.0])
        result (#'benchmark/summary samples)]
    (is (= 200.0 (:requests-per-second result)))
    (is (= [100.0 300.0] (:range result)))
    (is (= samples (:samples result)))
    (is (= "failed" (:status (#'benchmark/summary (conj samples {:status "failed"})))))))

(deftest validates-options
  (is (= benchmark/defaults (#'benchmark/options [])))
  (is (= 1 (:repetitions (#'benchmark/options ["--smoke"]))))
  (doseq [args [["--duration" "0"] ["--duration" "oops"] ["--duration"]
                ["--connections" "127"] ["--streams" "128"]
                ["--protocol" "h3"] ["--adapter" "unknown"] ["--unknown" "1"]]]
    (is (thrown? clojure.lang.ExceptionInfo (#'benchmark/options args)) (pr-str args))))

(deftest does-not-pretend-unsupported-protocols-work
  (let [support (into {} (map (juxt :id :protocols)) benchmark/adapters)]
    (is (= #{:h1} (:capra support) (:http-kit support)))
    (is (= #{:h1 :tls-h1} (:http-exchange support)))
    (is (= (set benchmark/protocols) (:busker support)))))

(deftest identifies-local-checkout-and-rejects-mixed-sources
  (let [root (.toFile (java.nio.file.Files/createTempDirectory
                       "busker-bench-origin-" (make-array java.nio.file.attribute.FileAttribute 0)))
        source (io/file root "src/main/clojure/ol/busker.clj")
        resource-path "linux-x86-64/libh2oclj.so"
        native (io/file root "shim/linux-x86-64/resources" resource-path)
        url #(.toURL (.toURI ^java.io.File %))
        origin #(#'benchmark/local-origin root (url source) (url native) resource-path)
        git! (fn [& args]
               (let [result (apply shell/sh "git" (concat args [:dir (str root)]))]
                 (when-not (zero? (:exit result)) (throw (ex-info "Fixture Git command failed" result)))
                 nil))]
    (try
      (doseq [file [source native]] (io/make-parents file) (spit file "fixture"))
      (git! "init" "--quiet")
      (git! "add" ".")
      (git! "-c" "user.name=Benchmark test" "-c" "user.email=benchmark@example.invalid"
            "-c" "commit.gpgsign=false" "-c" "core.hooksPath=/dev/null"
            "commit" "--quiet" "-m" "fixture")
      (let [result (origin)]
        (is (= :local (:busker-mode result)))
        (is (false? (:busker-dirty? result)))
        (is (re-matches #"[0-9a-f]{40}" (:busker-revision result)))
        (is (nil? (:native-version result)))
        (is (= (.getCanonicalPath root) (:busker-checkout result))))
      (spit source "changed")
      (is (true? (:busker-dirty? (origin))))
      (is (thrown? clojure.lang.ExceptionInfo
                   (#'benchmark/local-origin root (url native) (url native) resource-path)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (#'benchmark/local-origin root (url source) (url source) resource-path)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (#'benchmark/local-origin root (url source)
                                             (java.net.URL. "jar:file:/published.jar!/linux-x86-64/libh2oclj.so")
                                             resource-path)))
      (io/delete-file native)
      (is (thrown? clojure.lang.ExceptionInfo (origin)))
      (finally
        (doseq [file (reverse (file-seq root))] (io/delete-file file))))))