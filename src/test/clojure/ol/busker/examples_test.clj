(ns ol.busker.examples-test
  (:require
   [babashka.process :as p]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker.test-utils :as util]))

(def tls-host "localhost.examp1e.net")

(defn- example-path
  [& parts]
  (.getPath (apply io/file "examples" parts)))

(defn- read-deps-edn
  [example]
  (edn/read-string (slurp (example-path example "deps.edn"))))

(def example-ports
  {"quickstart" {:http 18080 :https 18443}
   "early-hints" {:http 18081 :https 18444}
   "sse" {:http 18082 :https 18445}})

(defn- start-example!
  [example]
  (let [{:keys [http https]} (get example-ports example)]
    (p/process ["clojure" "-M:run"]
               {:dir (example-path example)
                :extra-env {"BUSKER_HTTP_BIND" (format "127.0.0.1:%d" http)
                            "BUSKER_HTTPS_BIND" (format "127.0.0.1:%d" https)}
                :out :string
                :err :string
                :shutdown p/destroy-tree})))

(defn- stop-example!
  [proc]
  (p/destroy-tree proc)
  (deref proc 10000 {:exit :timeout}))

(defn- wait-for-server!
  ([proc port]
   (wait-for-server! proc port "/"))
  ([proc port path]
   (loop [attempt 0]
     (when-let [proc-result (deref proc 0 nil)]
       (throw (ex-info "Example server exited early" proc-result)))
     (let [result (util/curl :http :h1 port path :max-time 1)]
       (cond
         (zero? (:exit result))
         nil

         (< attempt 120)
         (do
           (Thread/sleep 250)
           (recur (inc attempt)))

         :else
         (throw (ex-info "Example server did not start"
                         {:port port
                          :path path
                          :proc (deref proc 0 nil)
                          :result result})))))))

(defn- curl*
  [scheme proto port path & {:as opts}]
  (apply util/curl scheme proto port path (mapcat identity opts)))

(defn- header-value
  [response header]
  (some->> (str/split-lines response)
           (some (fn [line]
                   (when (str/starts-with? (str/lower-case line)
                                           (str (str/lower-case header) ":"))
                     (str/trim (second (str/split line #":" 2))))))))

(deftest examples-layout-test
  (doseq [example ["quickstart" "early-hints" "sse"]]
    (testing example
      (is (.exists (io/file (example-path example "main.clj"))))
      (is (.exists (io/file (example-path example "deps.edn"))))
      (is (.exists (io/file (example-path example "README.md"))))
      (let [deps-edn (read-deps-edn example)
            readme (slurp (example-path example "README.md"))]
        (is (= {:local/root "../../"}
               (get-in deps-edn [:deps 'com.outskirtslabs/busker])))
        (is (= ["-m" "main"]
               (get-in deps-edn [:aliases :run :main-opts])))
        (is (str/includes? readme "clojure -M:run"))
        (is (str/includes? readme "curl"))))))

(deftest quickstart-example-test
  (let [{:keys [http https]} (example-ports "quickstart")
        proc (start-example! "quickstart")]
    (try
      (wait-for-server! proc http)
      (testing "hello, echo, and large response"
        (let [hello (curl* :http :h1 http "/" :args ["-i"])
              large (curl* :http :h1 http "/large"
                           :args ["-D" "-" "-o" "/dev/null"])
              echo (curl* :http :h1 http "/echo"
                          :args ["-i" "-X" "POST" "-H" "content-type: text/plain"
                                 "--data" "ping"])]
          (is (zero? (:exit hello)))
          (is (str/includes? (:out hello) "Hello from Busker o/"))
          (is (zero? (:exit large)))
          (is (= "1048576" (header-value (:out large) "x-body-size")))
          (is (zero? (:exit echo)))
          (is (str/includes? (:out echo) "ping"))))
      (testing "protocol headers"
        (let [h1 (curl* :http :h1 http "/" :args ["-D" "-" "-o" "/dev/null"])
              h2 (curl* :https :h2 https "/" :args ["-D" "-" "-o" "/dev/null"])
              h3 (curl* :https :h3 https "/" :args ["-D" "-" "-o" "/dev/null"])]
          (is (= "HTTP/1.1" (header-value (:out h1) "x-protocol")))
          (is (= "HTTP/2.0" (header-value (:out h2) "x-protocol")))
          (is (= "HTTP/3.0" (header-value (:out h3) "x-protocol")))))
      (finally
        (stop-example! proc)))))

(deftest early-hints-example-test
  (let [{:keys [http https]} (example-ports "early-hints")
        proc (start-example! "early-hints")]
    (try
      (wait-for-server! proc http)
      (testing "html and css"
        (let [html (curl* :http :h1 http "/" :args ["-i"])
              css (curl* :http :h1 http "/app.css" :args ["-i"])]
          (is (zero? (:exit html)))
          (is (str/includes? (:out html) "<!doctype html>"))
          (is (zero? (:exit css)))
          (is (str/includes? (:out css) "font-family"))))
      (testing "early hints over http2"
        (let [h2 (curl* :https :h2 https "/" :args ["-v"])]
          (is (zero? (:exit h2)))
          (is (re-find #"HTTP/2 103" (:err h2)))
          (is (re-find #"link: </app.css>; rel=preload; as=style" (:err h2)))))
      (testing "protocol headers"
        (let [h1 (curl* :http :h1 http "/" :max-time 2 :args ["-D" "-" "-o" "/dev/null"])
              h2 (curl* :https :h2 https "/" :max-time 2 :args ["-D" "-" "-o" "/dev/null"])
              h3 (curl* :https :h3 https "/" :max-time 2 :args ["-D" "-" "-o" "/dev/null"])]
          (is (= "HTTP/1.1" (header-value (:out h1) "x-protocol")))
          (is (= "HTTP/2.0" (header-value (:out h2) "x-protocol")))
          (is (= "HTTP/3.0" (header-value (:out h3) "x-protocol")))))
      (finally
        (stop-example! proc)))))

(deftest sse-example-test
  (let [{:keys [http https]} (example-ports "sse")
        proc (start-example! "sse")]
    (try
      (wait-for-server! proc http "/missing")
      (testing "events stream"
        (let [stream (curl* :http :h1 http "/"
                            :max-time 3
                            :args ["-i" "--no-buffer"])]
          (is (= 28 (:exit stream)))
          (is (str/includes? (:out stream) "content-type: text/event-stream"))
          (is (re-find #"data: \(ns main" (:out stream)))))
      (testing "brotli stream"
        (let [stream (curl* :https :h2 https "/"
                            :max-time 3
                            :args ["-i" "--no-buffer" "--compressed"
                                   "-H" "accept-encoding: br"])]
          (is (= 28 (:exit stream)))
          (is (= "br" (header-value (:out stream) "content-encoding")))
          (is (re-find #"data: \(ns main" (:out stream)))))
      (testing "protocol headers"
        (let [h1 (curl* :http :h1 http "/missing" :args ["-D" "-" "-o" "/dev/null"])
              h2 (curl* :https :h2 https "/missing" :args ["-D" "-" "-o" "/dev/null"])
              h3 (curl* :https :h3 https "/missing" :args ["-D" "-" "-o" "/dev/null"])]
          (is (= "HTTP/1.1" (header-value (:out h1) "x-protocol")))
          (is (= "HTTP/2.0" (header-value (:out h2) "x-protocol")))
          (is (= "HTTP/3.0" (header-value (:out h3) "x-protocol")))))
      (finally
        (stop-example! proc)))))
