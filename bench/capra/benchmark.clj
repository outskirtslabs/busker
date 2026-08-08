(ns capra.benchmark
  "Ring adapter benchmarks adapted from Capra.

  Source: https://github.com/weavejester/capra
  Source revision: f7b01e5f3179e50739ea108a6c1178ceec07eaa0
  Copyright © 2026 James Reeves. Licensed under EPL-2.0."
  (:require
   [aleph.http :as aleph]
   [capra.server :as capra]
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [org.httpkit.server :as http-kit]
   [ring-http-exchange.core :as http-exchange]
   [ring.adapter.jetty :as jetty]
   [ring.adapter.jetty9 :as jetty9]
   [ring.adapter.undertow :as undertow]
   [s-exp.hirundo :as hirundo]
   [ol.busker :as busker])
  (:import
   [java.net URI]
   [java.net.http HttpClient HttpRequest HttpResponse HttpResponse$BodyHandlers]
   [java.time Instant]
   [java.util Properties]))

(set! *warn-on-reflection* true)

(def ^:private benchmark-revision
  "f7b01e5f3179e50739ea108a6c1178ceec07eaa0")

(defn- minimal-handler [_]
  {:status 200
   :headers {"content-type" "text/plain; charset=UTF-8"}
   :body "Hello World"})

(defn- realistic-handler [_]
  (loop [i 0
         x 0.0]
    (if (< i 1000)
      (recur (inc i) (+ x (Math/random)))
      (do
        (Thread/sleep 5)
        {:status 200
         :headers {"content-type" "text/plain; charset=UTF-8"}
         :body "Simulated work and I/O response"}))))

(def scenarios
  [{:id :minimal
    :handler minimal-handler
    :expected-body "Hello World"}
   {:id :realistic
    :handler realistic-handler
    :expected-body "Simulated work and I/O response"}])

(defn- response [port]
  (let [request (-> (HttpRequest/newBuilder (URI/create (str "http://127.0.0.1:" port)))
                    (.GET)
                    (.build))]
    (.send (HttpClient/newHttpClient) request (HttpResponse$BodyHandlers/ofString))))

(defn- await-ready [port expected-body]
  (loop [attempt 0
         last-error nil]
    (let [result (try
                   (let [^HttpResponse result (response port)]
                     (when-not (and (= 200 (.statusCode result))
                                    (= expected-body (.body result)))
                       (throw (ex-info "Unexpected benchmark response"
                                       {:status (.statusCode result)
                                        :body (.body result)})))
                     result)
                   (catch Throwable error
                     error))]
      (if-not (instance? Throwable result)
        result
        (if (= 50 attempt)
          (throw (ex-info "Benchmark server did not become ready"
                          {:port port}
                          (or last-error result)))
          (do
            (Thread/sleep 100)
            (recur (inc attempt) result)))))))

(defn- busker-server [handler port]
  (let [server (busker/start!
                {:entrypoints {:benchmark {:bind (str "127.0.0.1:" port)
                                           :tls false}}
                 :dispatch [{:handler handler}]})]
    #(busker/stop! server)))

(defn- aleph-server [handler port]
  (let [^java.io.Closeable server (aleph/start-server handler {:port port})]
    #(.close server)))

(defn- capra-server [handler port]
  (let [server (capra/run-server handler :port port :error-logger (fn [_]))]
    #(.close server)))

(defn- hirundo-server [handler port]
  (let [server (hirundo/start! {:http-handler handler :port port})]
    #(hirundo/stop! server)))

(defn- http-exchange-server [handler port]
  (let [server (http-exchange/run-http-server handler {:port port})]
    #(http-exchange/stop-http-server server)))

(defn- http-kit-server [handler port]
  (http-kit/run-server handler {:port port}))

(defn- jetty-server [handler port]
  (let [server (jetty/run-jetty handler {:port port :join? false})]
    #(.stop server)))

(defn- jetty9-server [handler port]
  (let [^org.eclipse.jetty.server.Server server (jetty9/run-jetty handler {:port port :join? false})]
    #(.stop server)))

(defn- undertow-server [handler port]
  (let [server (undertow/run-undertow handler {:port port})]
    #(.stop server)))

(def adapters
  [{:id :busker :label "Busker" :start busker-server}
   {:id :aleph :label "Aleph" :start aleph-server :dependency ["aleph" "aleph"]}
   {:id :capra :label "Capra" :start capra-server :dependency ["dev.weavejester" "capra"]}
   {:id :hirundo :label "Hirundo" :start hirundo-server :dependency ["com.s-exp" "hirundo"]}
   {:id :http-exchange :label "http-exchange" :start http-exchange-server
    :dependency ["org.clojars.jj" "ring-http-exchange"]}
   {:id :http-kit :label "http-kit" :start http-kit-server :dependency ["http-kit" "http-kit"]}
   {:id :jetty :label "Ring Jetty" :start jetty-server :dependency ["ring" "ring-jetty-adapter"]}
   {:id :jetty9 :label "ring-jetty9-adapter" :start jetty9-server
    :dependency ["info.sunng" "ring-jetty9-adapter"]}
   {:id :undertow :label "Ring Undertow" :start undertow-server
    :dependency ["luminus" "ring-undertow-adapter"]}])

(defn- wrk [port {:keys [warmup duration connections threads]}]
  (let [run (fn [duration]
              (shell/sh "wrk" "--duration" duration
                        "--connections" (str connections)
                        "--threads" (str threads)
                        (str "http://127.0.0.1:" port)))]
    (when-not (zero? (:exit (run warmup)))
      (throw (ex-info "wrk warm-up failed" {:port port})))
    (let [result (run duration)]
      (if (zero? (:exit result))
        result
        (throw (ex-info "wrk measurement failed" {:port port :result result}))))))

(defn- parse-number [output pattern]
  (some-> (re-find pattern output) second Double/parseDouble))

(defn- request-errors [output]
  (let [values (or (some-> (re-find #"Socket errors: ([^\n]+)" output) second) "")
        errors (into {:connect 0 :read 0 :write 0 :timeout 0}
                     (map (fn [[_ name count]] [(keyword name) (parse-long count)]))
                     (re-seq #"(connect|read|write|timeout) (\d+)" values))]
    (assoc errors :total (reduce + (vals errors)))))

(defn- wrk-measurements [output]
  {:requests-per-second (parse-number output #"Requests/sec:\s+([0-9.]+)")
   :transfer-per-second (some-> (re-find #"Transfer/sec:\s+([^\s]+)" output) second)
   :latency (some-> (re-find #"Latency\s+([^\s]+)" output) second)
   :request-errors (request-errors output)})

(defn- run-adapter [adapter scenario port options]
  (let [{:keys [id label start]} adapter
        {:keys [handler expected-body] scenario-id :id} scenario]
    (println "Running" label "for" (name scenario-id) "scenario...")
    (flush)
    (try
      (let [stop (start handler port)]
        (try
          (await-ready port expected-body)
          (let [{:keys [out]} (wrk port options)
                measurements  (wrk-measurements out)
                error-count   (get-in measurements [:request-errors :total])]
            (cond-> {:adapter      (name id)
                     :label        label
                     :status       (if (zero? error-count) "ok" "failed")
                     :measurements measurements
                     :wrk-output   out}
              (pos? error-count)
              (assoc :error (str "wrk reported " error-count " request errors"))))
          (finally
            (stop))))
      (catch Throwable error
        {:adapter (name id)
         :label   label
         :status  "failed"
         :error   (or (ex-message error) (str error))}))))

(defn- dependency-version [[group artifact]]
  (let [properties (Properties.)
        resource (io/resource (str "META-INF/maven/" group "/" artifact "/pom.properties"))]
    (with-open [stream (io/input-stream resource)]
      (.load properties stream))
    (.getProperty properties "version")))

(defn- environment []
  {:busker-revision (-> (shell/sh "git" "rev-parse" "HEAD") :out str/trim)
   :capra-benchmark-revision benchmark-revision
   :java-version (System/getProperty "java.version")
   :java-vm (System/getProperty "java.vm.name")
   :operating-system (str (System/getProperty "os.name") " " (System/getProperty "os.version"))
   :processors (.availableProcessors (Runtime/getRuntime))
   :adapter-versions (into {}
                           (keep (fn [{:keys [id dependency]}]
                                   (when dependency
                                     [id (dependency-version dependency)])))
                           adapters)})

(defn- selected-items [kind selection items]
  (if-not selection
    items
    (let [matches (filterv #(= selection (name (:id %))) items)]
      (when (empty? matches)
        (throw (ex-info (str "Unknown " (name kind))
                        {kind       selection
                         :available (mapv (comp name :id) items)})))
      matches)))

(defn benchmark
  "Runs selected Ring adapters and handler scenarios, then writes the results.

  Options:

  | key            | description
  |----------------|-------------
  | `:warmup`      | `wrk` warm-up duration
  | `:duration`    | `wrk` measurement duration
  | `:connections` | simultaneous connections
  | `:threads`     | `wrk` threads
  | `:adapter`     | optional adapter ID
  | `:scenario`    | optional scenario ID
  | `:output`      | JSON result file

  Returns the complete result map."
  [{:keys [output adapter scenario]
    :as options}]
  (let [options            (merge {:warmup     "5s"
                                   :duration   "1m"
                                   :connections 128
                                   :threads     2}
                                  options)
        selected-adapters  (selected-items :adapter adapter adapters)
        selected-scenarios (selected-items :scenario scenario scenarios)
        environment        (environment)
        parameters         (dissoc options :output)
        _                  (println "Benchmark environment:" (json/write-str environment))
        _                  (println "Benchmark parameters:" (json/write-str parameters))
        result             {:run-at      (str (Instant/now))
                            :environment environment
                            :parameters  parameters
                            :scenarios
                            (mapv
                             (fn [scenario-index {:keys [id] :as selected-scenario}]
                               {:scenario (name id)
                                :results
                                (mapv
                                 (fn [adapter-index selected-adapter]
                                   (run-adapter selected-adapter selected-scenario
                                                (+ 5800
                                                   (* scenario-index (count selected-adapters))
                                                   adapter-index)
                                                options))
                                 (range)
                                 selected-adapters)})
                             (range)
                             selected-scenarios)}]
    (io/make-parents output)
    (spit output (json/write-str result))
    result))

(defn- command-line-options [args]
  (loop [options {}
         [arg value & remaining] args]
    (case arg
      nil options
      "--smoke" (recur (merge options {:warmup     "1s"
                                       :duration   "1s"
                                       :connections 16})
                       (cons value remaining))
      "--adapter" (if value
                    (recur (assoc options :adapter value) remaining)
                    (throw (ex-info "Missing --adapter value" {})))
      "--scenario" (if value
                     (recur (assoc options :scenario value) remaining)
                     (throw (ex-info "Missing --scenario value" {})))
      "--output" (if value
                   (recur (assoc options :output value) remaining)
                   (throw (ex-info "Missing --output value" {})))
      (throw (ex-info "Unknown benchmark argument" {:argument arg})))))

(defn -main [& args]
  (let [options (command-line-options args)
        output  (or (:output options)
                    (str "bench/results/" (System/currentTimeMillis) ".json"))
        result  (benchmark (assoc options :output output))]
    (println "Wrote benchmark results to" output)
    (doseq [{:keys [scenario results]} (:scenarios result)]
      (println scenario)
      (doseq [{:keys [label status measurements error]} results]
        (println " " label status (or (:requests-per-second measurements) error))))
    (when (some #(not= "ok" (:status %))
                (mapcat :results (:scenarios result)))
      (throw (ex-info "Benchmark did not complete successfully" {:output output})))))
