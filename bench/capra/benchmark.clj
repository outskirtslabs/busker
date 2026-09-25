(ns capra.benchmark
  "Compares Ring adapters over H1, TLS H1 and TLS H2 with h2load.

  Adapted from https://github.com/weavejester/capra at
  f7b01e5f3179e50739ea108a6c1178ceec07eaa0.
  Copyright © 2026 James Reeves. Licensed under EPL-2.0."
  (:require
   [aleph.http :as aleph]
   [capra.server :as capra]
   [clojure.data.json :as json]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [ol.busker :as busker]
   [org.httpkit.server :as http-kit]
   [ring-http-exchange.core :as http-exchange]
   [ring-http-exchange.ssl :as ssl]
   [ring.adapter.jetty :as jetty]
   [ring.adapter.undertow :as undertow]
   [s-exp.hirundo :as hirundo])
  (:import
   [io.helidon.common.tls TlsConfig]
   [java.io Closeable]
   [java.net URI]
   [java.net.http HttpClient HttpClient$Version HttpRequest HttpResponse HttpResponse$BodyHandlers]
   [java.nio.file Files NoSuchFileException]
   [java.security MessageDigest]
   [java.time Duration Instant]
   [java.util Properties]
   [java.util.concurrent TimeUnit]
   [org.eclipse.jetty.alpn.server ALPNServerConnectionFactory]
   [org.eclipse.jetty.http2.server HTTP2ServerConnectionFactory]
   [org.eclipse.jetty.server ConnectionFactory HttpConfiguration HttpConnectionFactory
    SecureRequestCustomizer Server ServerConnector SslConnectionFactory]
   [org.eclipse.jetty.util.ssl SslContextFactory$Server]))

(set! *warn-on-reflection* true)

(def protocols [:h1 :tls-h1 :tls-h2])
(def defaults {:warmup 10 :duration 30 :repetitions 3 :connections 128 :streams 64 :threads 2})
(def adapters
  [{:id :busker :label "Busker" :protocols (set protocols)}
   {:id :aleph :label "Aleph" :protocols (set protocols) :dependency ["aleph" "aleph"]}
   {:id :capra :label "Capra" :protocols #{:h1} :dependency ["dev.weavejester" "capra"]}
   {:id :hirundo :label "Hirundo" :protocols (set protocols) :dependency ["com.s-exp" "hirundo"]}
   {:id :http-exchange :label "http-exchange" :protocols #{:h1 :tls-h1}
    :dependency ["org.clojars.jj" "ring-http-exchange"]}
   {:id :http-kit :label "http-kit" :protocols #{:h1} :dependency ["http-kit" "http-kit"]}
   {:id :jetty :label "Ring Jetty" :protocols (set protocols) :dependency ["ring" "ring-jetty-adapter"]}
   {:id :undertow :label "Ring Undertow" :protocols (set protocols)
    :dependency ["luminus" "ring-undertow-adapter"]}])

(defn- handler [_]
  {:status 200 :headers {"content-type" "text/plain; charset=UTF-8" "content-length" "11"}
   :body "Hello World"})

(defn- command! [args output seconds]
  (io/make-parents output)
  (let [builder (doto (ProcessBuilder. ^java.util.List (mapv str args))
                  (.redirectError (io/file (str output ".err")))
                  (.redirectOutput (io/file output)))
        _       (doseq [key ["JAVA_TOOL_OPTIONS" "JDK_JAVA_OPTIONS" "_JAVA_OPTIONS" "CLJ_JVM_OPTS"]]
                  (.remove (.environment builder) key))
        process (.start builder)]
    (try
      (when-not (.waitFor process (long seconds) TimeUnit/SECONDS)
        (throw (ex-info "Command timed out" {:command (first args) :log output})))
      (when-not (zero? (.exitValue process))
        (throw (ex-info "Command failed" {:command (first args) :exit (.exitValue process) :log output})))
      (slurp output)
      (finally
        (when (.isAlive process)
          (with-open [children (.descendants process)]
            (doseq [^java.lang.ProcessHandle child (.toList children)] (.destroyForcibly child)))
          (.destroyForcibly process)
          (.waitFor process))))))

(defn- fixtures! [directory]
  (let [certificate (str directory "/server.crt")
        private-key (str directory "/server.key")
        keystore    (str directory "/server.p12")]
    (command! ["openssl" "req" "-x509" "-newkey" "rsa:2048" "-nodes" "-days" "2"
               "-subj" "/CN=localhost" "-addext" "subjectAltName=DNS:localhost,IP:127.0.0.1"
               "-keyout" private-key "-out" certificate] (str directory "/certificate.log") 30)
    (command! ["openssl" "pkcs12" "-export" "-inkey" private-key "-in" certificate
               "-out" keystore "-passout" "pass:benchmark"] (str directory "/keystore.log") 30)
    {:certificate certificate :private-key private-key :keystore keystore}))

(defn- jetty-tls! [^Server server port context]
  (let [config (doto (HttpConfiguration.) (.addCustomizer (SecureRequestCustomizer.)))
        alpn   (ALPNServerConnectionFactory. ^"[Ljava.lang.String;" (into-array String ["h2" "http/1.1"]))
        ssl    (doto (SslContextFactory$Server.) (.setSslContext context))
        parts  (into-array ConnectionFactory
                           [(SslConnectionFactory. ssl (.getProtocol alpn)) alpn
                            (HTTP2ServerConnectionFactory. config) (HttpConnectionFactory. config)])]
    (.addConnector server (doto (ServerConnector. server ^"[Lorg.eclipse.jetty.server.ConnectionFactory;" parts)
                            (.setHost "127.0.0.1") (.setPort port)))))

(defn- start-server [adapter protocol port {:keys [certificate private-key keystore]}]
  (let [tls?    (not= :h1 protocol)
        context (when tls? (ssl/keystore->ssl-context keystore "benchmark"))]
    (case adapter
      :busker
      (let [server (busker/start!
                    (cond-> {:entrypoints {:benchmark {:bind (str "127.0.0.1:" port)
                                                       :tls (if tls? {:tls-compatibility-mode :modern} false)
                                                       :http3? false}}
                             :dispatch [{:handler handler}]}
                      tls? (assoc :tls {:certificates {:load [{:type :pem :cert-file certificate
                                                               :key-file private-key}]}})))]
        #(busker/stop! server))
      :aleph
      (let [^Closeable server (aleph/start-server handler
                                                  (cond-> {:host "127.0.0.1" :port port}
                                                    tls? (assoc :http-versions [:http2 :http1]
                                                                :ssl-context {:certificate-chain certificate :private-key private-key})))]
        #(.close server))
      :capra
      (let [^Closeable server (capra/run-server handler :host "127.0.0.1" :port port)]
        #(.close server))
      :hirundo
      (let [server (hirundo/start! (cond-> {:host "127.0.0.1" :port port :http-handler handler}
                                     tls? (assoc :tls (.build (doto (TlsConfig/builder) (.sslContext context))))))]
        #(hirundo/stop! server))
      :http-exchange
      (let [server (http-exchange/run-http-server handler
                                                  (cond-> {:host "127.0.0.1" :port port} tls? (assoc :ssl-context context)))]
        #(http-exchange/stop-http-server server))
      :http-kit (http-kit/run-server handler {:ip "127.0.0.1" :port port})
      :jetty
      (let [server (jetty/run-jetty handler
                                    (cond-> {:host "127.0.0.1" :port port :join? false}
                                      tls? (assoc :http? false :configurator #(jetty-tls! % port context))))]
        #(.stop server))
      :undertow
      (let [server (undertow/run-undertow handler
                                          (cond-> {:host "127.0.0.1" :port port}
                                            tls? (assoc :http? false :ssl-port port :ssl-context context :http2? true)))]
        #(.stop server)))))

(defn- endpoint [protocol port]
  (str (if (= :h1 protocol) "http" "https") "://localhost:" port "/"))

(defn- check-response! [protocol port fixtures]
  (let [version (if (= :tls-h2 protocol) HttpClient$Version/HTTP_2 HttpClient$Version/HTTP_1_1)
        builder (doto (HttpClient/newBuilder) (.version version) (.connectTimeout (Duration/ofSeconds 3)))
        _       (when-not (= :h1 protocol)
                  (.sslContext builder (ssl/keystore->ssl-context (:keystore fixtures) "benchmark")))
        request (-> (HttpRequest/newBuilder (URI/create (endpoint protocol port)))
                    (.timeout (Duration/ofSeconds 5)) (.GET) (.build))]
    (with-open [client (.build builder)]
      (let [^HttpResponse response (.send client request (HttpResponse$BodyHandlers/ofString))]
        (when-not (and (= 200 (.statusCode response)) (= "Hello World" (.body response))
                       (= "11" (.orElse (.firstValue (.headers response) "content-length") nil))
                       (= "text/plain;charset=utf-8"
                          (some-> (.orElse (.firstValue (.headers response) "content-type") nil)
                                  str/lower-case (str/replace #"\s+" "")))
                       (= version (.version response)))
          (throw (ex-info "Response or negotiated protocol differs" {:status (.statusCode response)
                                                                     :body (.body response)
                                                                     :protocol (str (.version response))})))
        (when-not (= :h1 protocol)
          (let [^javax.net.ssl.SSLSession session (.get (.sslSession response))]
            (when-not (= "TLSv1.3" (.getProtocol session))
              (throw (ex-info "Expected TLS 1.3" {:negotiated (.getProtocol session)})))))
        {:status (.statusCode response) :body (.body response) :protocol (str (.version response))}))))

(defn- await-ready! [protocol port fixtures]
  (loop [attempt 0]
    (let [result (try (check-response! protocol port fixtures) (catch Exception e e))]
      (if (instance? Exception result)
        (if (< attempt 30)
          (do (Thread/sleep 100) (recur (inc attempt)))
          (throw result))
        result))))

(defn- load-command [protocol port seconds {:keys [connections streams threads]}]
  (let [h2? (= :tls-h2 protocol)]
    (into ["h2load" "-D" (str seconds) "-c" (str (if h2? (quot connections streams) connections))
           "-m" (str (if h2? streams 1)) "-t" (str threads)
           "--tls13-ciphers=TLS_AES_128_GCM_SHA256"]
          (concat (if h2? ["--alpn-list=h2"] ["--h1"]) [(endpoint protocol port)]))))

(defn- measurements [output protocol]
  (let [output (-> output
                   (str/replace "No protocol negotiated. Fallback behaviour may be activated" "")
                   (str/replace "Server does not support ALPN. Falling back to HTTP/1.1." ""))
        requests (some->> (re-find #"requests:\s+(\d+) total, (\d+) started, (\d+) done, (\d+) succeeded, (\d+) failed, (\d+) errored, (\d+) timeout" output)
                          rest (mapv parse-long))
        statuses (some->> (re-find #"status codes:\s+(\d+) 2xx, (\d+) 3xx, (\d+) 4xx, (\d+) 5xx" output)
                          rest (mapv parse-long))
        rate     (some-> (re-find #"finished in [^,]+,\s+([0-9.]+) req/s" output) second parse-double)
        negotiated (some-> (re-find #"Application protocol:\s+(h2|http/1\.1)\b" output) second)
        latency  (some-> (re-find #"(?m)^request\s*:\s+(.+)$" output) second str/trim (str/split #"\s+"))]
    (when-not (and requests statuses rate (pos? rate))
      (throw (ex-info "Incomplete h2load summary" {})))
    (let [[total started done succeeded failed errored timeout] requests
          [ok redirect client-error server-error] statuses]
      (when-not (and (= total done succeeded) (<= done ok started) (pos? done)
                     (every? zero? [failed errored timeout redirect client-error server-error])
                     (= negotiated (if (= protocol :tls-h2) "h2" "http/1.1")))
        (throw (ex-info "Invalid h2load result" {:requests requests :statuses statuses :protocol negotiated})))
      (when-not (= :h1 protocol)
        (when-not (and (re-find #"TLS Protocol:\s+TLSv1\.3\b" output)
                       (re-find #"Cipher:\s+TLS_AES_128_GCM_SHA256\b" output))
          (throw (ex-info "Unexpected load-generator TLS negotiation" {}))))
      {:requests-per-second rate :requests done
       :latency (when (>= (count latency) 6)
                  (zipmap [:median :p95 :p99 :mean] (take 4 (drop 2 latency))))})))

(defn- record-affinity! [directory phase]
  (when-let [method (System/getenv "TEMPO_BENCHMARK_METHOD")]
    (let [threads (->> (.listFiles (io/file "/proc/self/task"))
                       (keep (fn [^java.io.File task]
                               (try
                                 {:tid     (parse-long (.getName task))
                                  :allowed (second (re-find #"(?m)^Cpus_allowed_list:\s*(\S+)"
                                                            (Files/readString (.toPath (io/file task "status")))))}
                                 (catch NoSuchFileException _ nil))))
                       vec)
          evidence {:method method :phase phase :threads threads}]
      (spit (str directory "/server-affinity-" phase ".json") (json/write-str evidence))
      (when (or (not= method "pinned-server-10-11-client-12-13")
                (empty? threads)
                (some #(not= "10-11" (:allowed %)) threads))
        (throw (ex-info "Server CPU affinity differs from the benchmark method" evidence)))
      evidence)))

(defn- sample! [{:keys [adapter protocol warmup duration directory fixtures] :as options}]
  (let [port 5800
        stop (start-server adapter protocol port fixtures)]
    (try
      (let [response (await-ready! protocol port fixtures)
            warm-log (str directory "/warmup.log")
            load-log (str directory "/measurement.log")]
        (record-affinity! directory "ready")
        (measurements (command! (load-command protocol port warmup options) warm-log (+ warmup 30)) protocol)
        (record-affinity! directory "measurement-start")
        (let [result (measurements (command! (load-command protocol port duration options) load-log (+ duration 30)) protocol)]
          (check-response! protocol port fixtures)
          (record-affinity! directory "measurement-end")
          (assoc result :status "ok" :response response)))
      (finally (stop)))))

(defn- version [[group artifact]]
  (let [props (Properties.)]
    (with-open [in (io/input-stream (io/resource (str "META-INF/maven/" group "/" artifact "/pom.properties")))]
      (.load props in))
    (.getProperty props "version")))

(defn- local-origin [root source native resource-path]
  (doseq [[resource path] [[source (io/file root "src/main/clojure/ol/busker.clj")]
                           [native (io/file root "shim" (first (str/split resource-path #"/"))
                                            "resources" resource-path)]]]
    (when-not (and resource (= "file" (.getProtocol ^java.net.URL resource))
                   (.isFile ^java.io.File path)
                   (= (.getCanonicalFile (io/file resource)) (.getCanonicalFile ^java.io.File path)))
      (throw (ex-info "Local benchmark requires checkout source and local native build output; run bb build first"
                      {:expected (str path) :actual (str resource)}))))
  (let [directory (.getCanonicalPath (io/file root))
        revision (shell/sh "git" "rev-parse" "HEAD" :dir directory)
        status (shell/sh "git" "status" "--porcelain" :dir directory)]
    (when-not (= 0 (:exit revision) (:exit status))
      (throw (ex-info "Cannot identify benchmark checkout" {:directory directory})))
    {:busker-mode :local :busker-checkout directory
     :busker-revision (str/trim (:out revision))
     :busker-dirty? (not (str/blank? (:out status))) :native-version nil}))

(defn- environment []
  (let [source (io/resource "ol/busker.clj")
        revision (second (re-find #"/busker/([0-9a-f]{40})/" (str source)))
        platform (if (str/includes? (System/getProperty "os.name") "Mac") "macos" "linux")
        arch (if (= "aarch64" (System/getProperty "os.arch")) "aarch64" "x86-64")
        artifact (str platform "-" arch)
        resource-path (str artifact "/libh2oclj." (if (= platform "macos") "dylib" "so"))
        native (io/resource resource-path)
        origin (if (Boolean/getBoolean "busker.bench.local")
                 (local-origin "." source native resource-path)
                 (do
                   (when-not (and revision native (= "0.0.5" (version ["com.outskirtslabs.busker" artifact])))
                     (throw (ex-info "Benchmark requires Git Busker and native 0.0.5"
                                     {:source (str source) :native (str native)})))
                   {:busker-mode :pinned :busker-revision revision :native-version "0.0.5"}))
        digest (MessageDigest/getInstance "SHA-256")]
    (with-open [in (io/input-stream native)]
      (let [buffer (byte-array 65536)]
        (loop [] (let [n (.read in buffer)] (when (pos? n) (.update digest buffer 0 n) (recur))))))
    (merge origin {:busker-resource (str source) :native-resource (str native)
                   :native-sha256 (.formatHex (java.util.HexFormat/of) (.digest digest))
                   :java-version (System/getProperty "java.version") :processors (.availableProcessors (Runtime/getRuntime))
                   :max-heap-bytes (.maxMemory (Runtime/getRuntime))
                   :jvm-options (vec (.getInputArguments (java.lang.management.ManagementFactory/getRuntimeMXBean)))
                   :adapter-versions (into {} (keep (fn [{:keys [id dependency]}] (when dependency [id (version dependency)]))) adapters)})))

(defn- run-sample [options]
  (let [{:keys [directory warmup duration]} options
        input (str directory "/input.edn")
        output (str directory "/result.json")]
    (io/make-parents input)
    (spit input (pr-str options))
    (try
      (command! [(str (System/getProperty "java.home") "/bin/java")
                 "-Xms512m" "-Xmx2g" "-XX:ActiveProcessorCount=2" "--enable-native-access=ALL-UNNAMED"
                 (str "-Dbusker.bench.local=" (Boolean/getBoolean "busker.bench.local"))
                 "-cp" (System/getProperty "java.class.path") "clojure.main" "-m" "capra.benchmark" "--sample" input]
                (str directory "/server.log") (+ warmup duration 120))
      (let [result (json/read-str (slurp output) :key-fn keyword)
            errors (str (slurp (str directory "/server.log"))
                        (slurp (str directory "/server.log.err")))]
        (if (re-find #"(?m)ERROR[: ]|SEVERE:|Exception in thread|UT005071" errors)
          (assoc result :status "failed" :error "Server logged errors; see server.log and server.log.err")
          result))
      (catch Exception e
        (merge (when (.isFile (io/file output)) (json/read-str (slurp output) :key-fn keyword))
               {:status "failed" :process-error (ex-message e) :directory directory})))))

(defn- summary [samples]
  (if (every? #(= "ok" (:status %)) samples)
    (let [rates (vec (sort (map :requests-per-second samples)))
          n (count rates)]
      {:status "ok" :requests-per-second (/ (+ (nth rates (quot (dec n) 2)) (nth rates (quot n 2))) 2.0)
       :range [(first rates) (last rates)] :samples samples})
    {:status "failed" :samples samples}))

(defn- options [args]
  (let [opts (loop [opts defaults [flag value & more :as remaining] args]
               (cond
                 (empty? remaining) opts
                 (= flag "--smoke") (recur (assoc opts :warmup 1 :duration 1 :repetitions 1) (rest remaining))
                 (and value (#{"--adapter" "--protocol" "--output"} flag))
                 (recur (assoc opts (keyword (subs flag 2)) value) more)
                 (and value (str/starts-with? flag "--") (contains? defaults (keyword (subs flag 2))))
                 (recur (assoc opts (keyword (subs flag 2)) (parse-long value)) more)
                 :else (throw (ex-info "Unknown option or missing value" {:argument flag}))))]
    (doseq [key (keys defaults)]
      (when-not (pos-int? (get opts key)) (throw (ex-info "Expected a positive integer" {:option key}))))
    (when-not (and (zero? (mod (:connections opts) (:streams opts)))
                   (zero? (mod (quot (:connections opts) (:streams opts)) (:threads opts))))
      (throw (ex-info "Connections must divide into H2 streams and client threads" {})))
    (doseq [[key choices] [[:adapter (map (comp name :id) adapters)] [:protocol (map name protocols)]]]
      (when (and (get opts key) (not (some #{(get opts key)} choices)))
        (throw (ex-info "Unknown selection" {:option key :choices choices}))))
    opts))

(defn benchmark
  "Runs selected protocols in fresh JVMs and writes results and logs under `:output`.
  Durations are seconds; `:connections` is the total in-flight request budget.
  H2 divides that budget into connections with `:streams` requests each."
  [{:keys [output repetitions adapter protocol] :as opts}]
  (let [directory (.getAbsolutePath (io/file (or output (str "bench/results/" (System/currentTimeMillis)))))
        _ (when (.exists (io/file directory))
            (throw (ex-info "Output directory already exists; choose a new run directory" {:directory directory})))
        fixtures (fixtures! directory)
        selected (filter #(or (nil? adapter) (= adapter (name (:id %)))) adapters)
        modes (filter #(or (nil? protocol) (= protocol (name %))) protocols)
        env (assoc (environment) :h2load-version
                   (str/trim (command! ["h2load" "--version"] (str directory "/h2load-version.log") 10)))
        result {:run-at (str (Instant/now)) :environment env :parameters (dissoc opts :output)
                :results
                (mapv (fn [{:keys [id label protocols]}]
                        {:adapter id :label label
                         :protocols
                         (into {} (for [mode modes]
                                    [mode (if-not (protocols mode)
                                            {:status "unsupported"}
                                            (summary
                                             (mapv (fn [i]
                                                     (println label (name mode) "repetition" (inc i)) (flush)
                                                     (run-sample (assoc opts :adapter id :protocol mode :fixtures fixtures
                                                                        :directory (str directory "/" (name id) "/" (name mode) "/" (inc i)))))
                                                   (range repetitions))))]))}) selected)}]
    (spit (str directory "/results.json") (json/write-str result))
    (println "Results:" (str directory "/results.json"))
    result))

(defn -main [& args]
  (let [sample? (= "--sample" (first args))
        exit (try
               (if sample?
                 (let [opts (edn/read-string (slurp (second args)))
                       result (try (assoc (sample! opts) :environment (environment))
                                   (catch Throwable e {:status "failed" :error (ex-message e)}))]
                   (spit (str (:directory opts) "/result.json") (json/write-str result))
                   (if (= "ok" (:status result)) 0 1))
                 (let [result (benchmark (options args))]
                   (doseq [{:keys [label protocols]} (:results result)]
                     (println label (into {} (for [[p r] protocols] [p (or (:requests-per-second r) (:status r))]))))
                   (if (some #(= "failed" (:status %)) (mapcat (comp vals :protocols) (:results result))) 1 0)))
               (catch Throwable e (binding [*out* *err*] (println (ex-message e))) 1))]
    (shutdown-agents)
    (System/exit exit)))
