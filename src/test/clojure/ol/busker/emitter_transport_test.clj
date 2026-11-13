(ns ol.busker.emitter-transport-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.protocols :as response]
   [ol.busker.test-utils :as util])
  (:import
   [java.io ByteArrayOutputStream InputStream]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent TimeUnit]))

(defn- await-call! [f]
  (let [result (promise)
        thread (Thread/startVirtualThread
                #(deliver result (try {:value (f)} (catch Throwable cause {:error cause}))))
        outcome (deref result 20000 ::timeout)]
    (when (= ::timeout outcome)
      (.interrupt thread)
      (throw (ex-info "Client operation timed out" {})))
    (if-let [cause (:error outcome)] (throw cause) (:value outcome))))

(defn- read-head! [^InputStream input]
  (await-call!
   #(let [out (ByteArrayOutputStream.)]
      (loop []
        (let [b (.read input)]
          (when (neg? b) (throw (ex-info "EOF before response headers" {})))
          (.write out b)
          (let [text (.toString out "UTF-8")]
            (if (str/ends-with? text "\r\n\r\n") text (recur))))))))

(defn- read-text! [^InputStream input text]
  (let [size (alength (.getBytes ^String text StandardCharsets/UTF_8))]
    (await-call! #(String. (.readNBytes input size) StandardCharsets/UTF_8))))

(defn- header [head name]
  (some (fn [line]
          (when (str/starts-with? (str/lower-case line) (str name ":"))
            (str/trim (second (str/split line #":" 2)))))
        (str/split-lines head)))

(defn- with-stream [transport encoding config f]
  (let [port (util/free-port)
        tls? (not= :h1 transport)
        proto (if (= :tls-h1 transport) :h1 transport)
        ready (promise)
        closed (promise)
        server (busker/start!
                (util/with-handler
                  (fn [{:keys [uri protocol] emitter :ol.busker.request/emitter}]
                    (if (= "/__ready" uri)
                      {:status 200 :body "ready"}
                      (do
                        (response/on-close! emitter #(deliver closed (.isVirtual (Thread/currentThread))))
                        (response/emit! emitter
                                        {:status 200
                                         :headers {"content-type" "text/event-stream"
                                                   "x-protocol" protocol}})
                        (deliver ready emitter)
                        {:body emitter})))
                  (merge (util/with-static-tls
                           {:n-workers 1
                            :entrypoints {:test {:bind (str "127.0.0.1:" port)
                                                 :tls (if tls? {} false) :http3? tls?}}
                            :compress-min-size 1})
                         config)))]
    (try
      (let [scheme (if tls? :https :http)
            readiness (util/wait-for-curl-ready! scheme proto port "/__ready")]
        (when-not (zero? (:exit readiness))
          (throw (ex-info "Listener did not become ready; protocol fixture required" readiness)))
        (let [host (if tls? util/tls-sni-host "127.0.0.1")
              args (into ["curl" "-k" "-sS" "-i" "--no-buffer" "--compressed"
                          "--max-time" "120" "--noproxy" "*"
                          (case proto :h1 "--http1.1" :h2 "--http2" :h3 "--http3-only")
                          "-H" (str "accept-encoding: " encoding)]
                         (concat (when tls? ["--resolve" (str host ":" port ":127.0.0.1")])
                                 [(str (name scheme) "://" host ":" port "/stream")]))
              process (.start (ProcessBuilder. ^java.util.List args))
              errors (promise)]
          (Thread/startVirtualThread #(deliver errors (slurp (.getErrorStream process))))
          (try
            (let [emitter (deref ready 15000 ::timeout)]
              (when (= ::timeout emitter) (throw (ex-info "Handler timed out" {})))
              (f {:emitter emitter :process process :input (.getInputStream process)
                  :errors errors :closed closed}))
            (finally
              (when (realized? ready) (response/close! @ready))
              (.destroyForcibly process)
              (.waitFor process 5 TimeUnit/SECONDS)))))
      (finally (busker/stop! server)))))

(defn- emit-flushed! [emitter text]
  (let [result (await-call! #(response/emit! emitter text {:wait? false :flush? true}))]
    (when-not (= :accepted (:status result))
      (throw (ex-info "Write rejected" result)))
    result))

(defn- finish-client! [{:keys [emitter ^Process process ^InputStream input errors closed]}]
  (is (true? (response/close! emitter)))
  (is (false? (response/close! emitter)))
  (is (= "" (await-call! #(slurp input))))
  (is (.waitFor process 10 TimeUnit/SECONDS))
  (is (= [0 "" true] [(.exitValue process) (deref errors 1000 ::timeout)
                      (deref closed 1000 ::timeout)])))

(defn- check-cell! [transport encoding]
  (with-stream
    transport encoding {}
    (fn [{:keys [emitter input] :as client}]
      (let [first-frame "data: first λ frame\n\n"
            first-result (emit-flushed! emitter first-frame)
            head (read-head! input)
            proto (if (= :tls-h1 transport) :h1 transport)]
        (is (str/starts-with? head (case proto :h1 "HTTP/1.1 200" :h2 "HTTP/2 200" :h3 "HTTP/3 200")))
        (is (= [(case proto :h1 "HTTP/1.1" :h2 "HTTP/2.0" :h3 "HTTP/3.0")
                (when-not (= "identity" encoding) encoding)]
               [(header head "x-protocol") (header head "content-encoding")]))
        (is (= first-frame (read-text! input first-frame)))
        (is (response/open? emitter))
        (is (= {:status :proceeded} (deref (:progress first-result) 10000 ::timeout)))
        (let [a "data: second\n\n"
              b "data: third\n\n"
              [accepted-a accepted-b] (await-call!
                                       #(vector (response/emit! emitter a)
                                                (response/emit! emitter b {:wait? false})))
              flushed (response/flush! emitter)]
          (is (= [:accepted :accepted] (mapv :status [accepted-a accepted-b])))
          (is (identical? (:progress accepted-b) flushed))
          (is (identical? flushed (response/flush! emitter)))
          (is (= (str a b) (read-text! input (str a b))))
          (is (response/open? emitter))
          (is (= [{:status :proceeded} {:status :proceeded}]
                 (mapv #(deref (:progress %) 10000 ::timeout) [accepted-a accepted-b]))))
        (let [result (await-call!
                      #(response/emit! emitter
                                       (fn [^ByteBuffer buffer]
                                         (.put buffer (.getBytes ": native\n\n" StandardCharsets/UTF_8)))
                                       {:capacity 10 :wait? false :flush? true}))]
          (is (= :accepted (:status result)))
          (is (= ": native\n\n" (read-text! input ": native\n\n")))
          (is (= {:status :proceeded} (deref (:progress result) 10000 ::timeout))))
        (finish-client! client)))))

(deftest incremental-output-across-protocols-and-encodings-test
  (doseq [transport [:h1 :tls-h1 :h2 :h3]
          encoding ["identity" "gzip" "br" "zstd"]]
    (testing (str transport " / " encoding)
      (check-cell! transport encoding))))

(deftest silent-http1-stream-outlasts-io-timeout-test
  (with-stream
    :h1 "identity" {:http1-req-io-timeout 500}
    (fn [{:keys [emitter input closed] :as client}]
      (let [before "data: before idle\n\n"
            after "data: after idle\n\n"
            accepted (emit-flushed! emitter before)]
        (read-head! input)
        (is (= before (read-text! input before)))
        (is (= {:status :proceeded} (deref (:progress accepted) 10000 ::timeout)))
        (Thread/sleep 2000)
        (is (response/open? emitter) "Silence longer than the I/O timeout keeps the stream open.")
        (is (not (realized? closed)))
        (let [result (emit-flushed! emitter after)]
          (is (= after (read-text! input after)))
          (is (= {:status :proceeded} (deref (:progress result) 10000 ::timeout)))))
      (finish-client! client))))

(defn- check-heartbeats! [interval-ms count config]
  (with-stream
    :h1 "identity" config
    (fn [{:keys [emitter input] :as client}]
      (let [initial (emit-flushed! emitter ": initial\n\n")]
        (read-head! input)
        (is (= ": initial\n\n" (read-text! input ": initial\n\n")))
        (is (= {:status :proceeded} (deref (:progress initial) 10000 ::timeout)))
        (dotimes [_ count]
          (Thread/sleep interval-ms)
          (let [result (emit-flushed! emitter ":\n\n")]
            (is (= ":\n\n" (read-text! input ":\n\n")))
            (is (= {:status :proceeded} (deref (:progress result) 10000 ::timeout)))
            (is (response/open? emitter)))))
      (finish-client! client))))

(deftest two-second-heartbeats-with-default-timeout-test
  (check-heartbeats! 2000 6 {}))
