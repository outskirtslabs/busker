(ns ol.busker.response-length-test
  "Content length and charset errors in handler responses, over HTTP/1.1,
  HTTP/2 and HTTP/3."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [ol.busker :as busker]
   [ol.busker.protocols :as p]
   [ol.busker.test-utils :as util]
   [taoensso.trove :as trove])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream InputStream]
   [java.net InetSocketAddress Socket SocketTimeoutException]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]))

(def ^:private hello (.getBytes "Hello World" StandardCharsets/UTF_8))

(def ^:private no-encoder-type "text/plain; charset=x-JISAutoDetect")

(defn- emit-then [emitter f]
  (Thread/startVirtualThread f)
  {:body emitter})

(defn- handler [{:keys [uri] emitter :ol.busker.request/emitter}]
  (case uri
    "/ok" {:status 200 :body "second"}
    "/match" {:status 200 :headers {"content-length" "11"} :body hello}
    "/nil" {:status 200 :headers {"content-length" "11"}}
    "/empty-string" {:status 200 :headers {"content-length" "11"} :body ""}
    "/empty-bytes" {:status 200 :headers {"content-length" "11"} :body (byte-array 0)}
    "/short" {:status 200 :headers {"Content-Length" "5"} :body hello}
    "/long" {:status 200 :headers {"content-length" 20} :body "Hello World"}
    "/buffer" {:status 200 :headers {"content-length" "5"} :body (ByteBuffer/wrap hello)}
    "/large" {:status 200 :headers {"content-length" "5"} :body (byte-array 40000 (byte 66))}
    "/not-a-number" {:status 200 :headers {"content-length" "abc"} :body hello}
    "/negative" {:status 200 :headers {"content-length" "-5"} :body hello}
    "/stream-not-a-number" {:status 200 :headers {"content-length" "abc"} :body (ByteArrayInputStream. hello)}
    "/stream-negative" {:status 200 :headers {"content-length" -5} :body (ByteArrayInputStream. hello)}
    "/vector" {:status 200 :headers {"content-length" ["11" "11"]} :body hello}
    "/after-103" (do (p/emit! emitter {:status 103 :headers {"link" "</a.css>; rel=preload"}})
                     {:status 200 :headers {"content-length" "5"} :body hello})
    "/no-encoder" {:status 200 :headers {"content-type" no-encoder-type} :body "café"}
    "/no-encoder-large" {:status 200 :headers {"content-type" no-encoder-type}
                         :body (apply str (repeat 40000 "é"))}
    "/no-encoder-seq" {:status 200 :headers {"content-type" no-encoder-type} :body (list "ca" "fé")}
    "/stream-short" {:status 200 :headers {"content-length" "5"} :body (ByteArrayInputStream. hello)}
    "/stream-long" {:status 200 :headers {"content-length" "20"} :body (ByteArrayInputStream. hello)}
    "/emit-close" (emit-then emitter
                             #(do (p/emit! emitter {:status 200 :headers {"content-length" "20"} :body hello})
                                  (p/close! emitter)))
    "/emit-producer" (emit-then emitter
                                #(let [result (try
                                                (p/emit! emitter {:status 200 :headers {"content-length" "5"}
                                                                  :body (fn [^ByteBuffer b] (.put b ^bytes hello))}
                                                         {:end? true :capacity 16})
                                                (catch Exception _ :threw))]
                                   (p/emit! emitter {:status 500 :body (str result)} {:end? true})))
    {:status 200 :body "ready"}))

(def ^:private handler-error-paths
  "Responses that are a handler error before their head is sent."
  ["/nil" "/empty-string" "/empty-bytes" "/short" "/long" "/buffer" "/large"
   "/not-a-number" "/negative" "/stream-not-a-number" "/stream-negative" "/vector"
   "/after-103" "/no-encoder" "/no-encoder-large" "/no-encoder-seq"])

(def ^:private empty-head-paths
  "Responses whose HEAD answer sends no body and gives the GET length."
  #{"/nil" "/empty-string" "/empty-bytes"})

(def ^:private streamed-mismatch-paths
  "Responses of unknown length whose body differs from their content length."
  ["/stream-short" "/stream-long" "/emit-close"])

(defn- start-server []
  (let [port (util/free-port)
        tls-port (util/free-port)
        server (busker/start!
                (util/with-static-tls
                  (util/with-handler
                    handler
                    {:n-workers 1
                     :entrypoints {:plain {:bind (str "127.0.0.1:" port) :tls false :http3? false}
                                   :tls {:bind (str "127.0.0.1:" tls-port)
                                         :http3? true
                                         :tls {:tls-compatibility-mode :modern}}}})))]
    (util/wait-for-curl-ready! :http :h1 port "/")
    (util/wait-for-curl-ready! :https :h3 tls-port "/")
    {:server server :port port :tls-port tls-port}))

(defn- with-server [f]
  (let [server (start-server)]
    (try (f server) (finally (busker/stop! (:server server))))))

(defn- send-request! [^Socket socket method path]
  (doto (.getOutputStream socket)
    (.write (.getBytes (str method " " path " HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                       StandardCharsets/US_ASCII))
    (.flush)))

(defn- read-head
  "Reads one response head. Returns its status, its lower-cased head text, and
  its content length."
  [^InputStream in]
  (let [out (ByteArrayOutputStream.)]
    (loop []
      (let [b (.read in)]
        (when (neg? b) (throw (ex-info "EOF in response head" {:head (str out)})))
        (.write out b)
        (when-not (str/ends-with? (str out) "\r\n\r\n") (recur))))
    (let [head (str/lower-case (str out))]
      {:status (parse-long (second (str/split head #" ")))
       :head head
       :content-length (some-> (re-find #"(?m)^content-length: (\d+)" head) second parse-long)})))

(defn- read-response
  "Reads one final response, after any informational heads."
  [^InputStream in method]
  (let [{:keys [status content-length] :as head} (read-head in)]
    (if (< status 200)
      (read-response in method)
      (assoc head :body (if (= "HEAD" method)
                          ""
                          (String. (.readNBytes in (int content-length)) StandardCharsets/UTF_8))))))

(defn- read-to-eof
  "Returns the bytes read until the server closes the connection, or `:open`
  when it stays open for two seconds."
  [^Socket socket]
  (.setSoTimeout socket 2000)
  (try (count (.readAllBytes (.getInputStream socket)))
       (catch SocketTimeoutException _ :open)))

(defn- keep-alive-pair
  "Sends `method` `path` and then GET /ok on one HTTP/1.1 connection. Returns
  both responses."
  [port method path]
  (with-open [socket (Socket.)]
    (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 2000)
    (.setSoTimeout socket 5000)
    (let [in (.getInputStream socket)
          _ (send-request! socket method path)
          first-response (read-response in method)
          _ (send-request! socket "GET" "/ok")]
      [first-response (read-response in "GET")])))

(defn- curl-head
  "Sends HEAD for `path` over `proto` with curl. Returns the exit code, the
  status and the content length."
  [proto port path]
  (let [{:keys [exit out]} (util/curl :https proto port path :max-time 5 :args ["-I"])
        head (str/lower-case out)]
    {:exit exit
     :status (some-> (re-find #"^http/\S+ (\d+)" head) second parse-long)
     :content-length (some-> (re-find #"(?m)^content-length: (\d+)" head) second parse-long)}))

(defn- curl-exchange [proto port path]
  (let [{:keys [exit out]} (util/curl :https proto port path :max-time 5
                                      :args ["-w" "\n%{http_code}"])
        split (str/last-index-of out "\n")]
    {:exit exit
     :status (parse-long (subs out (inc split)))
     :body (subs out 0 split)}))

(deftest known-length-mismatch-is-a-handler-error-over-http1-test
  (with-server
    (fn [{:keys [port]}]
      (let [logged_ (atom [])]
        (with-redefs [trove/*log-fn* (fn [_ns _coords _level id _lazy] (swap! logged_ conj id))]
          (doseq [path handler-error-paths
                  method ["GET" "HEAD"]
                  :when (not (and (= "HEAD" method) (empty-head-paths path)))]
            (let [[first-response second-response] (keep-alive-pair port method path)
                  context [method path]]
              (is (= 500 (:status first-response)) (pr-str context))
              (is (= 21 (:content-length first-response)) (pr-str context))
              (when (= "GET" method)
                (is (= "Internal Server Error" (:body first-response)) (pr-str context)))
              (is (= [200 "second"] ((juxt :status :body) second-response)) (pr-str context)))))
        (is (= (- (* 2 (count handler-error-paths)) (count empty-head-paths))
               (count (filter #{:ol.busker.response/response-failed} @logged_))))))))

(deftest matching-content-length-is-sent-unchanged-test
  (with-server
    (fn [{:keys [port tls-port]}]
      (doseq [[method path body] (into [["GET" "/match" "Hello World"]]
                                       (map (fn [path] ["HEAD" path ""]))
                                       (sort empty-head-paths))]
        (let [[first-response second-response] (keep-alive-pair port method path)]
          (is (= [200 11 body] ((juxt :status :content-length :body) first-response))
              (pr-str [method path]))
          (is (= [200 "second"] ((juxt :status :body) second-response)) (pr-str [method path]))))
      (doseq [proto [:h2 :h3]
              path (sort empty-head-paths)]
        (is (= {:exit 0 :status 200 :content-length 11} (curl-head proto tls-port path))
            (pr-str [proto path]))))))

(deftest known-length-mismatch-is-a-handler-error-over-http2-and-http3-test
  (with-server
    (fn [{:keys [tls-port]}]
      (doseq [proto [:h2 :h3]
              path handler-error-paths]
        (is (= {:exit 0 :status 500 :body "Internal Server Error"}
               (curl-exchange proto tls-port path))
            (pr-str [proto path]))))))

(deftest streamed-length-mismatch-closes-the-http1-connection-test
  (with-server
    (fn [{:keys [port]}]
      (doseq [path streamed-mismatch-paths]
        (with-open [socket (Socket.)]
          (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 2000)
          (.setSoTimeout socket 5000)
          (send-request! socket "GET" path)
          (let [{:keys [status content-length]} (read-head (.getInputStream socket))
                received (read-to-eof socket)]
            (is (= 200 status) path)
            (is (number? received) (str path " leaves the connection open"))
            (when (number? received)
              (is (< received content-length) path))))))))

;; HTTP/2 and HTTP/3 clients reset these streams without the worker's byte
;; count too, so this test only guards against a regression.
(deftest streamed-length-mismatch-resets-http2-and-http3-streams-test
  (with-server
    (fn [{:keys [tls-port]}]
      (doseq [proto [:h2 :h3]
              path streamed-mismatch-paths]
        (is (not= 0 (:exit (curl-exchange proto tls-port path))) (pr-str [proto path]))))))

(deftest fixed-producer-length-mismatch-throws-before-output-test
  (with-server
    (fn [{:keys [port tls-port]}]
      (let [[first-response second-response] (keep-alive-pair port "GET" "/emit-producer")]
        (is (= [500 ":threw"] ((juxt :status :body) first-response)))
        (is (= [200 "second"] ((juxt :status :body) second-response))))
      (doseq [proto [:h2 :h3]]
        (is (= {:exit 0 :status 500 :body ":threw"} (curl-exchange proto tls-port "/emit-producer"))
            (pr-str proto))))))
