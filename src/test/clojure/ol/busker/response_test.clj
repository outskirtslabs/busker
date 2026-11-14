(ns ol.busker.response-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as protocols]
   [ol.busker.response :as response]
   [ol.busker.test-utils :as util]
   [ring.core.protocols :as ring-protocols])
  (:import
   [java.io ByteArrayInputStream InputStream OutputStream]
   [java.net InetSocketAddress Socket]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.security KeyStore]
   [java.security.cert CertificateFactory]
   [java.util.concurrent CountDownLatch TimeUnit]
   [javax.net.ssl SNIHostName SSLContext SSLSocket SSLSocketFactory TrustManagerFactory]))

(defn- start-server
  ([handler] (start-server handler {}))
  ([handler config] (start-server handler config nil))
  ([handler config tls-protocol]
   (let [port (util/free-port)
         server (busker/start!
                 (util/with-handler
                   (fn [request]
                     (if (= "/__ready" (:uri request))
                       {:status 200 :body "ready"}
                       (handler request)))
                   (merge {:n-workers 1
                           :compress? false
                           :entrypoints {:http {:bind (str "127.0.0.1:" port)
                                                :http3? false :tls (if tls-protocol {} false)}}}
                          (when tls-protocol {:tls util/static-tls})
                          config)))
         ready (util/wait-for-curl-ready! (if tls-protocol :https :http) :h1 port "/__ready" :max-time 2)]
     (if (zero? (:exit ready))
       [server port]
       (do (busker/stop! server)
           (throw (ex-info "Server did not become ready" {:port port :ready ready})))))))

(defn- on-virtual-thread [f]
  (let [result (promise)]
    (Thread/startVirtualThread
     (bound-fn []
       (deliver result (try {:value (f)} (catch Throwable cause {:error cause})))))
    result))

(defn- await-result [result]
  (let [outcome (deref result 10000 ::timeout)]
    (when (= ::timeout outcome) (throw (ex-info "Operation timed out" {})))
    (if-let [cause (:error outcome)] (throw cause) (:value outcome))))

(def ^:private tls-socket-factory
  (delay
    (let [store (KeyStore/getInstance (KeyStore/getDefaultType))
          trust (TrustManagerFactory/getInstance (TrustManagerFactory/getDefaultAlgorithm))
          context (SSLContext/getInstance "TLS")]
      (.load store nil nil)
      (with-open [input (io/input-stream (util/fixture-cert-path))]
        (.setCertificateEntry store "fixture" (.generateCertificate (CertificateFactory/getInstance "X.509") input)))
      (.init trust store)
      (.init context nil (.getTrustManagers trust) nil)
      (.getSocketFactory context))))

(defn- connect
  ([port] (connect port {}))
  ([port {:keys [receive-buffer-size send-buffer-size tls-protocol]}]
   (let [socket (Socket.)]
     (when receive-buffer-size (.setReceiveBufferSize socket (int receive-buffer-size)))
     (when send-buffer-size (.setSendBufferSize socket (int send-buffer-size)))
     (.setSoTimeout socket 10000)
     (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 2000)
     (if tls-protocol
       (try
         (let [^SSLSocket tls (.createSocket ^SSLSocketFactory @tls-socket-factory socket ^String util/tls-sni-host (int port) true)
               parameters (.getSSLParameters tls)]
           (.setProtocols parameters (into-array String [tls-protocol]))
           (.setServerNames parameters [(SNIHostName. ^String util/tls-sni-host)])
           (.setEndpointIdentificationAlgorithm parameters "HTTPS")
           (.setSSLParameters tls parameters)
           (.setSoTimeout tls 10000)
           (.startHandshake tls)
           (is (= tls-protocol (.getProtocol (.getSession tls))))
           tls)
         (catch Throwable cause
           (.close socket)
           (throw cause)))
       socket))))

(defn- request-bytes [port method path]
  (with-open [^Socket socket (connect port)]
    (let [out (.getOutputStream socket)
          request (str method " " path " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")]
      (.write out (.getBytes request StandardCharsets/US_ASCII))
      (.flush out)
      (.readAllBytes (.getInputStream socket)))))

(defn- request-text [port method path]
  (String. ^bytes (request-bytes port method path) StandardCharsets/UTF_8))

(defn- request-stream! [^Socket socket]
  (let [out (.getOutputStream socket)]
    (.write out (.getBytes "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
                           StandardCharsets/US_ASCII))
    (.flush out)))

(defn- read-through! [^Socket socket expected]
  (let [input (.getInputStream socket)
        response (StringBuilder.)]
    (loop []
      (if (str/includes? (str response) expected)
        (str response)
        (let [b (.read input)]
          (when (neg? b)
            (throw (ex-info "EOF before expected data" {:response (str response) :expected expected})))
          (.append response (char b))
          (recur))))))

(defn- request-stream-with-unfinished-body!
  "Sends a request whose body never completes, so the HTTP/1 request I/O
  timeout stays armed while the response streams."
  ([^Socket socket] (request-stream-with-unfinished-body! socket "/"))
  ([^Socket socket path]
   (let [out (.getOutputStream socket)]
     (.write out (.getBytes (str "POST " path " HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                                 "Content-Length: 100\r\nConnection: close\r\n\r\npartial")
                            StandardCharsets/US_ASCII))
     (.flush out))))

(defn- read-response! [^Socket socket]
  (let [headers (read-through! socket "\r\n\r\n")
        content-length (some->> (re-find #"(?im)^content-length: (\d+)\r?$" headers)
                                second Long/parseLong)]
    {:headers headers
     :body (.readNBytes (.getInputStream socket) (int content-length))}))

(defn- with-emitter [config f]
  (let [ready (promise)
        [server port] (start-server
                       (fn [{emitter :ol.busker.request/emitter}]
                         (deliver ready emitter)
                         {:body emitter})
                       config)
        client (on-virtual-thread #(util/curl :http :h1 port "/" :max-time 20))]
    (try
      (let [emitter (deref ready 5000 ::timeout)]
        (when (= ::timeout emitter) (throw (ex-info "Handler timed out" {})))
        (await-result (on-virtual-thread #(f emitter client))))
      (finally
        (when (realized? ready) (protocols/close! @ready))
        (busker/stop! server)))))

(deftest ordinary-string-response-byte-lengths
  (let [response_ (atom {:status 200 :body ""})
        [server port] (start-server (fn [_] @response_))]
    (try
      (doseq [[charset-name charset] [["utf-8" StandardCharsets/UTF_8]
                                      ["invalid-charset" StandardCharsets/UTF_8]
                                      ["utf-16" StandardCharsets/UTF_16]
                                      ["iso-8859-1" StandardCharsets/ISO_8859_1]]
              body ["" "Hello World" "é漢😀" (str (char 0xd800) "x" (char 0xdc00))
                    (apply str (repeat 32768 "a"))
                    (apply str (repeat 32769 "a"))]]
        (reset! response_ {:status 200
                           :headers {"content-type" (str "text/plain; charset=" charset-name)}
                           :body body})
        (with-open [^Socket socket (connect port)]
          (request-stream! socket)
          (let [expected (.getBytes ^String body ^java.nio.charset.Charset charset)
                {:keys [headers body]} (read-response! socket)]
            (is (str/includes? headers "200 OK"))
            (is (= (vec expected) (vec body))))))
      (finally (busker/stop! server)))))

(def ^:private plan-request
  {:dispatch-module-id 1 :dispatch-request-seq 1 :config {:output-buffer-size 32768}})

(defn- head-fields [head]
  (select-keys head [:status :headers :content-length :compress-hint]))

(defn- fixed-head [response]
  (some-> (#'response/ordinary-fixed-plan plan-request response) first head-fields))

(deftest header-pairs-preserve-order-and-coercion
  (doseq [[headers expected]
          [[nil {:headers [] :content-length -1}]
           [{} {:headers [] :content-length -1}]
           [(array-map "X-Empty" [] "X-List" '(1 nil) "X-Key" :value
                       "X-Lazy" (map str [2 3]) "CONTENT-LENGTH" 4)
            {:headers [["X-List" "1"] ["X-List" ""] ["X-Key" ":value"]
                       ["X-Lazy" "2"] ["X-Lazy" "3"]]
             :content-length 4}]
           [{"X-Nested" [[1 2]]} {:headers [["X-Nested" "[1 2]"]] :content-length -1}]
           [(array-map "X-Nil" nil "X-Number" 7 "X-Vector" ["a" :b 3])
            {:headers [["X-Nil" ""] ["X-Number" "7"] ["X-Vector" "a"] ["X-Vector" ":b"]
                       ["X-Vector" "3"]]
             :content-length -1}]]]
    (is (= expected
           (select-keys (#'response/normalized-head {:headers headers} nil false)
                        [:headers :content-length])))))

(deftest fixed-head-content-length-from-header-or-body
  (let [compress-hint (response/get-compress-hint {})]
    (doseq [header-name ["content-length" "CONTENT-LENGTH" "Content-Length" "cOnTeNt-LeNgTh"]
            [value expected] [["5" 5] [5 5] [5.9 5]]]
      (testing (str header-name " " (pr-str value))
        (is (= {:status 200
                :headers [["content-type" "text/plain"] ["X-After" "1"]]
                :content-length expected
                :compress-hint compress-hint}
               (fixed-head {:body "ok"
                            :headers (array-map "content-type" "text/plain"
                                                header-name value
                                                "X-After" "1")})))))
    (doseq [[body expected] [["ok" 2] ["é" 2] ["" 0]
                             [(.getBytes "body" StandardCharsets/UTF_8) 4]
                             [(byte-array 0) 0]]]
      (is (= {:status 200 :headers [["content-type" "text/plain"]]
              :content-length expected :compress-hint compress-hint}
             (fixed-head {:body body :headers {"content-type" "text/plain"}}))))
    (is (= {:status 200 :headers [] :content-length 2 :compress-hint compress-hint}
           (fixed-head {:body "ok"})))
    (is (= {:status 201 :headers [["X-Repeat" "first"] ["X-Repeat" "second"] ["X-Lazy" "1"]]
            :content-length 2 :compress-hint compress-hint}
           (fixed-head {:status 201 :body "ok"
                        :headers (array-map "X-Repeat" ["first" "second"] "X-Lazy" (map str [1]))})))))

(deftest fixed-head-status-rules
  (is (= 200 (:status (fixed-head {:body "ok"}))))
  (doseq [headers [{} {"Content-Length" "9"}]]
    (is (= {:status 205 :headers [] :content-length 0}
           (select-keys (fixed-head {:status 205 :body "cat" :headers headers})
                        [:status :headers :content-length]))))
  (doseq [[response content-length]
          [[{:status 204} -1]
           [{:status 204 :body "ok" :headers {"Content-Length" "2"}} -1]
           [{:status 304} -1]
           [{:status 304 :body "ok"} 2]
           [{:status 304 :headers {"Content-Length" "9"}} 9]
           [{:status 200} 0]]]
    (is (= content-length (:content-length (fixed-head response))) (pr-str response)))
  (let [head-request (assoc plan-request :req-ctx {:request-method :head})
        head-length #(:content-length (first (#'response/ordinary-fixed-plan head-request %)))]
    (is (= -1 (head-length {:status 200})))
    (is (= 9 (head-length {:status 200 :headers {"Content-Length" "9"}})))
    (is (= 2 (head-length {:status 200 :body "ok"}))))
  (doseq [status [101 103 "200"]]
    (is (nil? (#'response/ordinary-fixed-plan plan-request {:status status :body "ok"})))))

(deftest fixed-head-compress-hint
  (let [request (assoc-in plan-request [:config :compress-min-size] 3)
        hint (fn [response] (:compress-hint (first (#'response/ordinary-fixed-plan request response))))]
    (is (= h2o/H2O_COMPRESS_HINT_ENABLE (hint {:body "okay"})))
    (is (= h2o/H2O_COMPRESS_HINT_DISABLE (hint {:body "ok"})))
    (is (= h2o/H2O_COMPRESS_HINT_ENABLE_BR
           (hint {:body "ok" :h2o/compress-hint :h2o.compress/enable-br})))
    (is (= h2o/H2O_COMPRESS_HINT_DISABLE
           (hint {:body "okay" :h2o/compress-hint :h2o.compress/disable})))))

(deftest fixed-head-rejects-non-string-header-names
  (doseq [body ["ok" (.getBytes "ok" StandardCharsets/UTF_8)]
          headers [{:x-key "v"} {'x-key "v"}]]
    (is (thrown? ClassCastException
                 (#'response/ordinary-fixed-plan plan-request {:body body :headers headers}))))
  (is (thrown? ClassCastException
               (#'response/normalized-head {:headers {:x-key "v"}} nil false))))

(deftest fixed-head-header-storage-limits
  (let [pairs (fn [n] (vec (repeat n "value")))]
    (is (= 64 (count (:headers (fixed-head {:body "ok"
                                            :headers (array-map "X-Repeat" (pairs 64)
                                                                "Content-Length" "2")})))))
    (doseq [[headers header-count] [[{"X-Repeat" (pairs 65)} 65]
                                    [(array-map "X-Repeat" (pairs 63) "X-Other" ["a" "b"]) 65]
                                    [{"X-Large" (apply str (repeat 16384 "x"))} 1]]
            build [#(#'response/ordinary-fixed-plan plan-request {:body "ok" :headers headers})
                   #(#'response/normalized-head {:body "ok" :headers headers} 2 false)
                   #(#'response/normalized-head {:status 103 :headers headers} nil true)]]
      (let [error (try (build) nil (catch clojure.lang.ExceptionInfo e e))]
        (is (= "Response headers exceed bounded header storage" (ex-message error)))
        (is (= {:header-count header-count} (ex-data error)))))))

(deftest informational-head-lowercases-names-and-drops-content-length
  (is (= {:status 103 :headers [["link" "</a.css>; rel=preload"] ["x-two" "1"] ["x-two" "2"]]
          :content-length 3}
         (select-keys (#'response/normalized-head
                       {:status 103 :headers (array-map "Link" "</a.css>; rel=preload"
                                                        "Content-Length" "3"
                                                        "X-Two" [1 2])}
                       nil true)
                      [:status :headers :content-length]))))

(deftest fixed-path-charset-selection
  (doseq [[headers fixed?]
          [[{} true]
           [{"content-type" "text/plain"} true]
           [{"content-type" "text/x-charset-free"} true]
           [{"content-type" "text/plain; charset=utf-8"} true]
           [{"Content-Type" "text/plain;CHARSET=UTF-8"} true]
           [{"content-type" "text/plain; charset=\"utf-8\""} true]
           [{"content-type" "text/plain; charset=not-a-charset"} true]
           [{"content-type" "text/plain; charset=iso-8859-1"} false]
           [{"CONTENT-TYPE" "text/plain; Charset=UTF-16"} false]
           [{"content-type" "text/plain; charset=\"iso-8859-1\""} false]
           [{"content-type" "text/plain; format=flowed; charset=us-ascii"} false]
           [(array-map "Content-Type" "text/plain; charset=iso-8859-1"
                       "content-type" "text/plain; charset=utf-8") true]
           [(array-map "content-type" "text/plain; charset=iso-8859-1"
                       "Content-Type" "text/plain; charset=utf-8") false]]]
    (testing (pr-str headers)
      (is (= fixed? (some? (fixed-head {:body "ok" :headers headers}))))
      (is (some? (fixed-head {:body (.getBytes "ok" StandardCharsets/UTF_8) :headers headers}))))))

(deftest ordinary-response-header-normalization
  (let [response_ (atom {})
        [server port] (start-server (fn [_] @response_))]
    (try
      (doseq [body ["é" (.getBytes "é" StandardCharsets/UTF_8)]
              length-headers [{} {"content-length" "2"} {"CoNtEnT-LeNgTh" "2"}
                              {"CONTENT-LENGTH" 2} {"Content-Length" 2}]]
        (reset! response_ {:body body
                           :headers (merge {"Content-Type" "text/plain; charset=invalid-charset"
                                            "X-Repeat" '("first" "second")}
                                           length-headers)})
        (let [wire (request-text port "GET" "/")]
          (is (str/starts-with? wire "HTTP/1.1 200"))
          (is (= ["2"] (mapv second (re-seq #"(?im)^content-length: ([^\r]+)\r?$" wire))))
          (is (= ["first" "second"] (mapv second (re-seq #"(?im)^x-repeat: ([^\r]+)\r?$" wire))))
          (is (str/ends-with? wire "\r\n\r\né"))))
      (doseq [status [204 304]]
        (reset! response_ {:status status :headers {"Content-Length" "7"} :body "ignored"})
        (let [wire (request-text port "GET" "/")]
          (is (str/starts-with? wire (str "HTTP/1.1 " status)))
          (is (= "" (second (str/split wire #"\r\n\r\n" 2))))))
      (finally (busker/stop! server)))))

(deftest ordinary-response-rejects-invalid-headers-before-output
  (let [response_ (atom {})
        [server port] (start-server (fn [_] @response_))]
    (try
      (reset! response_ {:body "ok" :headers {"X-Repeat" (repeat 64 "value")}})
      (let [wire (request-text port "GET" "/")]
        (is (= 64 (count (re-seq #"(?im)^x-repeat: value\r?$" wire))))
        (is (str/ends-with? wire "\r\n\r\nok")))
      (finally (busker/stop! server))))
  (doseq [headers [{"X-Repeat" (repeat 65 "value")}
                   {"X-Large" (apply str (repeat 16384 "x"))}
                   {"Content-Length" "not-a-number"}
                   {"Content-Length" ["2" "2"]}]]
    (is (thrown? Exception
                 (#'response/normalized-head {:body "ok" :headers headers} 2 false)))
    (is (thrown? Exception
                 (#'response/ordinary-fixed-plan
                  {:dispatch-module-id 1 :dispatch-request-seq 1
                   :config {:output-buffer-size 32768}}
                  {:body "ok" :headers headers})))))

(deftest invalid-ring-response-terminates-transport
  (doseq [headers [{"X-Repeat" (repeat 65 "value")}
                   {"X-Large" (apply str (repeat 16384 "x"))}
                   {"Content-Length" "not-a-number"}
                   {"Content-Length" ["2" "2"]}]
          informational? [false true]]
    (let [closed (promise)
          [server port] (start-server
                         (fn [{emitter :ol.busker.request/emitter}]
                           (protocols/on-close! emitter #(deliver closed true))
                           (when informational? (protocols/emit! emitter {:status 103}))
                           {:body "invalid-body" :headers headers}))]
      (try
        (with-open [^Socket socket (connect port)]
          (.setSoTimeout socket 2000)
          (request-stream! socket)
          (let [wire (String. (.readAllBytes (.getInputStream socket)) StandardCharsets/UTF_8)]
            (is (str/includes? wire "HTTP/1.1 200 OK\r\n"))
            (is (str/includes? (str/lower-case wire) "transfer-encoding: chunked\r\n"))
            (is (not (str/includes? wire "invalid-body")))
            (is (not (str/ends-with? wire "\r\n0\r\n\r\n")))))
        (is (true? (deref closed 2000 false)))
        (finally
          (is (= {:value nil}
                 (deref (on-virtual-thread #(busker/stop! server)) 5000 ::timeout))))))))

(deftest ordinary-ring-body-forms
  (let [response_ (atom {:status 200 :body nil})
        [server port] (start-server (fn [_] @response_))
        bytes (.getBytes "body" StandardCharsets/UTF_8)
        buffer (doto (ByteBuffer/wrap (.getBytes "_body_" StandardCharsets/UTF_8))
                 (.position 1)
                 (.limit 5))]
    (try
      (doseq [[body expected] [[nil ""]
                               ["body" "body"]
                               [bytes "body"]
                               [(byte-array 0) ""]
                               [buffer "body"]
                               [(ByteArrayInputStream. bytes) "body"]
                               [(list "bo" "dy") "body"]
                               [(reify ring-protocols/StreamableResponseBody
                                  (write-body-to-stream [_ _ out]
                                    (.write ^OutputStream out bytes))) "body"]]]
        (reset! response_ {:status 200 :body body})
        (is (= {:exit 0 :out expected}
               (select-keys (util/curl :http :h1 port "/") [:exit :out]))))
      (is (= [1 5] [(.position buffer) (.limit buffer)]))
      (finally (busker/stop! server)))))

(defrecord ExtendedEmitter [emitter])

(extend-type ExtendedEmitter
  protocols/ResponseEmitter
  (open? [this] (protocols/open? (:emitter this)))
  (committed? [this] (protocols/committed? (:emitter this)))
  (emit!
    ([this data] (protocols/emit! (:emitter this) data))
    ([this data opts] (protocols/emit! (:emitter this) data opts)))
  (flush! [this] (protocols/flush! (:emitter this)))
  (close! [this] (protocols/close! (:emitter this)))
  (on-close! [this callback] (protocols/on-close! (:emitter this) callback)))

(deftest externally-extended-emitter-body-keeps-response-open
  (with-emitter
    {}
    (fn [emitter client]
      (let [body (->ExtendedEmitter emitter)]
        (response/send-ring-response! emitter {:status 201 :body body})
        (is (= [true true] [(protocols/open? body) (protocols/committed? body)]))
        (protocols/emit! body "custom emitter" {:end? true})
        (is (= {:exit 0 :out "custom emitter"}
               (select-keys (await-result client) [:exit :out])))))))

(deftest unified-emitter-results-and-defaults-test
  (with-emitter
    {}
    (fn [emitter client]
      (is (= [true false] [(protocols/open? emitter) (protocols/committed? emitter)]))
      (is (nil? (protocols/flush! emitter)))
      (is (= {:status :accepted} (protocols/emit! emitter {:status 103})))
      (is (false? (protocols/committed? emitter)))
      (is (= {:status :accepted}
             (protocols/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})))
      (let [a (protocols/emit! emitter "first")
            b (protocols/emit! emitter "second" {:wait? false})
            empty (protocols/emit! emitter nil)
            receipt (:progress empty)]
        (is (= [:accepted :accepted :accepted] (mapv :status [a b empty])))
        (is (= #{:status :progress} (set (keys a))))
        (is (instance? clojure.lang.IPending receipt))
        (is (identical? receipt (protocols/flush! emitter)))
        (is (identical? receipt (protocols/flush! emitter)))
        (is (= [{:status :proceeded} {:status :proceeded} {:status :proceeded}]
               (mapv #(deref (:progress %) 5000 ::timeout) [a b empty])))
        (is (true? (protocols/close! emitter)))
        (is (false? (protocols/close! emitter)))
        (is (identical? receipt (protocols/flush! emitter)))
        (is (= {:status :closed} (protocols/emit! emitter :invalid {:unknown true})))
        (is (= {:exit 0 :out "firstsecond"}
               (select-keys (await-result client) [:exit :out])))))))

(deftest validation-does-not-commit-or-consume-input-test
  (with-emitter
    {:output-buffer-size 8}
    (fn [emitter client]
      (let [called (atom 0)
            producer (fn [_] (swap! called inc))
            source (proxy [InputStream] [] (read [] (swap! called inc) -1))]
        (doseq [[body opts] [["x" {:unknown true}]
                             ["x" {:wait? nil}]
                             ["x" {:flush? 1}]
                             ["x" {:end? :yes}]
                             ["x" {:capacity 1}]
                             [producer {}]
                             [producer {:capacity 0}]
                             [producer {:capacity 9}]
                             [producer {:capacity 9 :wait? false}]
                             ["123456789" {:wait? false}]
                             [(byte-array 9) {:wait? false}]
                             [source {:wait? false}]
                             [(lazy-seq (swap! called inc) (list "x")) {:wait? false}]
                             [{:status 103 :body nil} {}]
                             [{:status 103} {:end? true}]
                             [{:status 101} {}]]]
          (is (thrown? Exception (protocols/emit! emitter body opts)))
          (is (false? (protocols/committed? emitter))))
        (is (= 0 @called))
        (is (= :accepted (:status (protocols/emit! emitter "ok" {:end? true}))))
        (is (= "ok" (:out (await-result client))))))))

(deftest body-first-commit-and-final-head-rejection-test
  (with-emitter
    {}
    (fn [emitter client]
      (is (= :accepted (:status (protocols/emit! emitter "body"))))
      (is (protocols/committed? emitter))
      (is (thrown? Exception (protocols/emit! emitter {:status 201})))
      (protocols/close! emitter)
      (is (= "body" (:out (await-result client)))))))

(deftest native-producer-is-synchronous-confined-and-revoked-test
  (with-emitter
    {}
    (fn [emitter client]
      (let [caller (Thread/currentThread)
            aliases (atom nil)
            called (atom 0)
            result (protocols/emit!
                    emitter
                    (fn [^ByteBuffer buffer]
                      (swap! called inc)
                      (is (identical? caller (Thread/currentThread)))
                      (is (.isVirtual caller))
                      (let [slice (.slice buffer)
                            duplicate (.duplicate buffer)]
                        (reset! aliases [buffer slice duplicate])
                        (doseq [^ByteBuffer alias @aliases]
                          (let [attempt (await-result
                                         (on-virtual-thread
                                          #(try (.put alias 0 (byte 9)) :accessible
                                                (catch Exception _ :confined))))]
                            (is (= :confined attempt)))))
                      (is (thrown? Exception (protocols/emit! emitter "recursive")))
                      (.put buffer (.getBytes "native" StandardCharsets/UTF_8)))
                    {:capacity 6 :flush? true})]
        (is (= [1 :accepted] [@called (:status result)]))
        (doseq [^ByteBuffer alias @aliases]
          (is (thrown? IllegalStateException (.get alias 0))))
        (is (= {:status :proceeded} (deref (:progress result) 5000 ::timeout)))
        (protocols/close! emitter)
        (is (= "native" (:out (await-result client))))))))

(deftest producer-failure-and-overflow-publish-nothing-test
  (with-emitter
    {}
    (fn [emitter client]
      (is (thrown-with-msg? Exception #"producer failed"
                            (protocols/emit! emitter
                                             {:status 201 :body (fn [^ByteBuffer b]
                                                                  (.put b (byte 65))
                                                                  (throw (ex-info "producer failed" {})))}
                                             {:capacity 1})))
      (is (thrown? java.nio.BufferOverflowException
                   (protocols/emit! emitter (fn [^ByteBuffer b] (.putLong b 42)) {:capacity 1})))
      (is (false? (protocols/committed? emitter)))
      (protocols/emit! emitter "safe" {:end? true})
      (is (= "safe" (:out (await-result client)))))))

(deftest close-does-not-wait-for-private-producer-test
  (with-emitter
    {}
    (fn [emitter client]
      (let [entered (promise)
            release (promise)
            skipped (atom 0)
            producer (on-virtual-thread
                      #(protocols/emit! emitter
                                        {:status 201
                                         :body (fn [^ByteBuffer b]
                                                 (.put b (byte 65))
                                                 (deliver entered true)
                                                 @release
                                                 (.put b (byte 66)))}
                                        {:capacity 2}))]
        (try
          (is (true? (deref entered 5000 false)))
          (is (= {:status :full}
                 (protocols/emit! emitter (fn [_] (swap! skipped inc))
                                  {:capacity 1 :wait? false})))
          (is (= 0 @skipped))
          (is (nil? (protocols/flush! emitter)))
          (is (true? (protocols/close! emitter)))
          (is (= {:exit 0 :out ""} (select-keys (await-result client) [:exit :out])))
          (finally (deliver release true)))
        (is (= {:status :closed} (await-result producer)))))))

(defn- await-waiting! [^Thread thread]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (while (not= Thread$State/WAITING (.getState thread))
      (when (> (System/currentTimeMillis) deadline)
        (throw (ex-info "Thread did not wait" {:state (.getState thread)})))
      (Thread/sleep 1))))

(defn- blocking-producer [entered release]
  (fn [^ByteBuffer b]
    (.put b (byte 65))
    (deliver entered true)
    @release
    (.put b (byte 66))))

(deftest second-emit-waits-for-fixed-producer-and-gets-closed-test
  (with-emitter
    {}
    (fn [emitter client]
      (let [entered (promise)
            release (promise)
            closed (atom 0)
            first-result (on-virtual-thread
                          #(protocols/emit! emitter {:status 200 :body (blocking-producer entered release)}
                                            {:capacity 2 :end? true}))]
        (protocols/on-close! emitter #(swap! closed inc))
        (try
          (is (true? (deref entered 5000 false)))
          (let [second-result (promise)
                waiter (Thread/startVirtualThread
                        #(deliver second-result (protocols/emit! emitter "late")))]
            (await-waiting! waiter)
            (deliver release true)
            (is (= :accepted (:status (await-result first-result))))
            (is (= {:status :closed} (deref second-result 5000 ::timeout))))
          (is (= {:exit 0 :out "AB"} (select-keys (await-result client) [:exit :out])))
          (is (= 1 @closed))
          (finally (deliver release true)))))))

(deftest close-during-fixed-producer-publishes-nothing-test
  (with-emitter
    {}
    (fn [emitter client]
      (let [entered (promise)
            release (promise)
            closed (atom 0)
            result (on-virtual-thread
                    #(protocols/emit! emitter {:status 201 :body (blocking-producer entered release)}
                                      {:capacity 2 :end? true}))]
        (protocols/on-close! emitter #(swap! closed inc))
        (try
          (is (true? (deref entered 5000 false)))
          (is (true? (protocols/close! emitter)))
          (deliver release true)
          (is (= {:status :closed} (await-result result)))
          (is (false? (protocols/close! emitter)))
          (is (= {:exit 0 :out ""} (select-keys (await-result client) [:exit :out])))
          (is (= 1 @closed))
          (finally (deliver release true)))))))

(deftest head-response-from-an-emitted-body-sends-no-body-test
  (let [[server port] (start-server
                       (fn [_]
                         {:status 200
                          :headers {"content-type" "text/plain"}
                          :body (java.io.ByteArrayInputStream.
                                 (.getBytes "hello" StandardCharsets/US_ASCII))}))]
    (try
      (with-open [^Socket socket (connect port)]
        (let [out (.getOutputStream socket)]
          (.write out (.getBytes (str "HEAD / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
                                      "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                                 StandardCharsets/US_ASCII))
          (.flush out))
        (let [response (String. (.readAllBytes (.getInputStream socket)) StandardCharsets/US_ASCII)]
          (is (= 2 (count (re-seq #"HTTP/1\.1 200 OK" response))) response)
          (is (= 1 (count (re-seq #"hello" response))) response)
          (is (str/includes? response "text/plain\r\n\r\nHTTP/1.1 200 OK") response)))
      (finally (busker/stop! server)))))

(deftest stop-of-a-closed-emitter-does-not-take-its-lock-test
  (let [emitter (response/new-response-emitter {} (fn [^Runnable task] (.run task)))
        lock (.lifecycle-lock ^ol.busker.response.H2OResponseEmitter emitter)
        held (java.util.concurrent.CountDownLatch. 1)
        release (java.util.concurrent.CountDownLatch. 1)]
    (response/stop-emitter emitter)
    (let [holder (Thread/startVirtualThread
                  #(locking lock
                     (.countDown held)
                     (.await release)))]
      (try
        (is (.await held 2 java.util.concurrent.TimeUnit/SECONDS))
        (is (= :returned (deref (future (response/stop-emitter emitter) :returned)
                                1000 :blocked)))
        (is (false? (protocols/open? emitter)))
        (finally
          (.countDown release)
          (.join holder 2000))))))

(deftest stop-after-close-changes-nothing-test
  (let [emitter (response/new-response-emitter {} (fn [^Runnable task] (.run task)))
        lifecycle_ (.lifecycle_ ^ol.busker.response.H2OResponseEmitter emitter)
        calls (atom [])]
    (protocols/on-close! emitter #(swap! calls conj :registered))
    (response/stop-emitter emitter)
    (is (= [:registered] @calls))
    (is (false? (protocols/open? emitter)))
    (let [state @lifecycle_]
      (is (= {:phase :closed :callbacks [] :producer nil} state))
      (response/stop-emitter emitter)
      (is (identical? state @lifecycle_))
      (is (false? (protocols/close! emitter)))
      (is (identical? state @lifecycle_)))
    (is (= [:registered] @calls))
    (protocols/on-close! emitter #(swap! calls conj :late))
    (is (= [:registered :late] @calls))))

(deftest mutable-input-is-copied-and-buffer-range-is-preserved-test
  (with-emitter
    {}
    (fn [emitter client]
      (let [bytes (.getBytes "abc" StandardCharsets/UTF_8)
            buffer (ByteBuffer/wrap (.getBytes "_def_" StandardCharsets/UTF_8))]
        (.position buffer 1)
        (.limit buffer 4)
        (protocols/emit! emitter bytes)
        (protocols/emit! emitter buffer {:wait? false})
        (aset-byte bytes 0 (byte 120))
        (.put buffer 1 (byte 120))
        (is (= [1 4] [(.position buffer) (.limit buffer)]))
        (protocols/close! emitter)
        (is (= "abcdef" (:out (await-result client))))))))

(deftest close-settles-unflushed-and-end-receipts-test
  (doseq [end? [false true]]
    (with-emitter
      {}
      (fn [emitter client]
        (let [result (protocols/emit! emitter {:status 200 :body "final"} {:end? end?})]
          (when-not end? (protocols/close! emitter))
          (is (= {:status :closed} (deref (:progress result) 1000 ::timeout)))
          (is (false? (protocols/open? emitter)))
          (is (= "final" (:out (await-result client)))))))))

(deftest callbacks-are-ordered-isolated-late-and-virtual-test
  (with-emitter
    {}
    (fn [emitter client]
      (let [callbacks (atom [])
            completed (promise)]
        (protocols/on-close! emitter #(swap! callbacks conj [:first (.isVirtual (Thread/currentThread))]))
        (protocols/on-close! emitter #(throw (ex-info "expected callback failure" {})))
        (protocols/on-close! emitter #(swap! callbacks conj [:third (.isVirtual (Thread/currentThread))]))
        (protocols/close! emitter)
        (protocols/on-close! emitter #(do (swap! callbacks conj [:late (.isVirtual (Thread/currentThread))])
                                          (deliver completed true)))
        (is (true? (deref completed 5000 false)))
        (is (= [[:first true] [:third true] [:late true]] @callbacks))
        (is (= 0 (:exit (await-result client))))))))

(deftest callback-registration-racing-with-close-delivers-once-test
  (with-emitter
    {}
    (fn [emitter client]
      (let [registrations 32
            ready (CountDownLatch. registrations)
            start (CountDownLatch. 1)
            delivered (CountDownLatch. registrations)
            callbacks (atom [])]
        (doseq [i (range registrations)]
          (Thread/startVirtualThread
           #(do (.countDown ready)
                (.await start)
                (protocols/on-close! emitter
                                     (fn [] (swap! callbacks conj i) (.countDown delivered))))))
        (is (.await ready 2 TimeUnit/SECONDS))
        (let [closer (on-virtual-thread #(do (.await start) (protocols/close! emitter)))]
          (.countDown start)
          (is (.await delivered 5 TimeUnit/SECONDS))
          (is (= (vec (range registrations)) (vec (sort @callbacks))))
          (is (true? (await-result closer))))
        (is (= 0 (:exit (await-result client))))))))

(deftest ordinary-ring-close-callbacks-run-once-test
  (let [calls (atom [])
        delivered (promise)
        [server port] (start-server
                       (fn [{emitter :ol.busker.request/emitter}]
                         (protocols/on-close! emitter #(swap! calls conj :first))
                         (protocols/on-close! emitter #(do (swap! calls conj :second)
                                                           (deliver delivered true)))
                         {:status 200 :body "once"}))]
    (try
      (is (= "once" (:out (util/curl :http :h1 port "/"))))
      (is (true? (deref delivered 5000 false)))
      (finally (busker/stop! server)))
    (is (= [:first :second] @calls))))

(deftest ordinary-ring-final-headers-head-and-charset-test
  (let [closed (promise)
        [server port] (start-server
                       (fn [{:keys [uri] emitter :ol.busker.request/emitter}]
                         (case uri
                           "/unicode" {:status 200 :body "λ"}
                           "/empty" {:status 200 :body (byte-array 0)}
                           "/head" {:status 200 :body "visible"}
                           "/latin1" {:status 200
                                      :headers {"content-type" "text/plain; charset=iso-8859-1"}
                                      :body "é"}
                           "/headers" {:status 200
                                       :headers {"Content-Type" "text/plain"
                                                 "X-Custom" "value"
                                                 "X-Repeat" ["first" "second"]}
                                       :body "headers"}
                           "/callback" (do (protocols/on-close! emitter #(deliver closed (.isVirtual (Thread/currentThread))))
                                           {:status 200 :body "complete"})
                           "/info" (do (protocols/emit! emitter {:status 103 :headers {"link" "</style.css>; rel=preload"}})
                                       {:status 200 :body "final"})
                           {:status 404 :body "missing"})))]
    (try
      (is (= "λ" (:out (util/curl :http :h1 port "/unicode"))))
      (is (= "" (:out (util/curl :http :h1 port "/empty"))))
      (let [head-response (request-text port "HEAD" "/head")
            info-response (request-text port "GET" "/info")
            headers (request-text port "GET" "/headers")
            latin1 (request-bytes port "GET" "/latin1")]
        (is (re-find #"(?i)content-length: 7" head-response))
        (is (= "" (second (str/split head-response #"\r\n\r\n" 2))))
        (is (< (.indexOf info-response "103") (.indexOf info-response "200")))
        (is (str/ends-with? info-response "final"))
        (is (re-find #"(?im)^content-type: text/plain\r?$" headers))
        (is (re-find #"(?im)^x-custom: value\r?$" headers))
        (is (= ["first" "second"] (mapv second (re-seq #"(?im)^x-repeat: ([^\r]+)\r?$" headers))))
        (is (= (byte -23) (aget ^bytes latin1 (dec (alength ^bytes latin1))))))
      (is (= "complete" (:out (util/curl :http :h1 port "/callback"))))
      (is (true? (deref closed 2000 false)))
      (finally (busker/stop! server)))))

(deftest reset-content-suppresses-body-and-preserves-connection
  (let [[server port] (start-server
                       (fn [{:keys [uri] emitter :ol.busker.request/emitter}]
                         (case uri
                           "/fixed" {:status 205 :body "cat"}
                           "/explicit" {:status 205 :headers {"Content-Length" "3"} :body "cat"}
                           "/stream" (do
                                       (protocols/emit! emitter {:status 205})
                                       (protocols/emit! emitter "cat" {:end? true})
                                       {:body emitter})
                           {:status 200 :body "next"})))]
    (try
      (doseq [path ["/fixed" "/explicit" "/stream"]]
        (with-open [^Socket socket (connect port)]
          (let [^OutputStream out (.getOutputStream socket)]
            (.write out (.getBytes (str "GET " path " HTTP/1.1\r\nHost: localhost\r\n\r\n")
                                   StandardCharsets/US_ASCII))
            (.flush out)
            (let [headers (read-through! socket "\r\n\r\n")]
              (is (str/starts-with? headers "HTTP/1.1 205"))
              (is (re-find #"(?im)^content-length: 0\r?$" headers)))
            (.write out (.getBytes "GET /next HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                                   StandardCharsets/US_ASCII))
            (.flush out)
            (let [{:keys [headers body]} (read-response! socket)]
              (is (str/starts-with? headers "HTTP/1.1 200"))
              (is (= "next" (String. ^bytes body StandardCharsets/UTF_8)))))))
      (finally (busker/stop! server)))))

(deftest incremental-close-sends-accepted-prefix-without-source-eof-test
  (let [ready (promise)
        blocked (promise)
        release (promise)
        completed (promise)
        reads (atom 0)
        source (proxy [InputStream] []
                 (read
                   ([] (throw (ex-info "Expected bulk reads" {})))
                   ([buffer] (.read ^InputStream this buffer 0 (alength ^bytes buffer)))
                   ([buffer offset length]
                    (case (swap! reads inc)
                      1 (do (java.util.Arrays/fill ^bytes buffer (int offset) (int (+ offset (min 4 length))) (byte 65))
                            (min 4 length))
                      2 (do (deliver blocked true)
                            @release
                            (aset-byte buffer offset (byte 66))
                            1)
                      -1))))
        [server port] (start-server
                       (fn [{emitter :ol.busker.request/emitter}]
                         (deliver ready emitter)
                         (Thread/startVirtualThread
                          #(deliver completed (protocols/emit! emitter source)))
                         {:body emitter})
                       {:output-buffer-size 4})]
    (try
      (with-open [socket (connect port)]
        (request-stream! socket)
        (let [emitter (deref ready 5000 nil)]
          (is (true? (deref blocked 5000 false)))
          (let [receipt (protocols/flush! emitter)]
            (is (some? receipt))
            (read-through! socket "AAAA")
            (is (not (realized? receipt)))
            (is (true? (protocols/close! emitter)))
            (is (= {:status :closed} (deref receipt 1000 ::timeout)))
            (is (not (str/includes? (slurp (.getInputStream socket)) "B")))
            (deliver release true)
            (let [result (deref completed 5000 ::timeout)]
              (is (= :accepted (:status result)))
              (is (identical? receipt (:progress result)))))))
      (finally
        (deliver release true)
        (when (realized? ready) (protocols/close! @ready))
        (busker/stop! server)))))

(deftest reported-client-termination-delivers-close-callback-test
  (let [closed (promise)
        ready (promise)
        [server port] (start-server
                       (fn [{emitter :ol.busker.request/emitter}]
                         (protocols/on-close! emitter #(deliver closed (.isVirtual (Thread/currentThread))))
                         (protocols/emit! emitter "event: ready\n\n" {:flush? true})
                         (deliver ready emitter)
                         {:body emitter}))]
    (try
      (with-open [socket (connect port)]
        (.setSoLinger socket true 0)
        (request-stream! socket)
        (read-through! socket "event: ready\n\n"))
      (let [emitter (deref ready 5000 nil)]
        (await-result (on-virtual-thread #(protocols/emit! emitter "event: probe\n\n" {:flush? true})))
        (is (true? (deref closed 5000 false))))
      (finally
        (when (realized? ready) (protocols/close! @ready))
        (busker/stop! server)))))

(deftest server-stop-drains-stream-before-close-callback-test
  (let [ready (promise)
        closed (promise)
        [server port] (start-server
                       (fn [{emitter :ol.busker.request/emitter}]
                         (protocols/on-close! emitter #(deliver closed (.isVirtual (Thread/currentThread))))
                         (protocols/emit! emitter "event: ready\n\n" {:flush? true})
                         (deliver ready emitter)
                         {:body emitter}))]
    (try
      (with-open [socket (connect port)]
        (request-stream! socket)
        (read-through! socket "event: ready\n\n")
        (let [emitter (deref ready 5000 nil)
              stopped (on-virtual-thread #(busker/stop! server))]
          (is (= ::pending (deref stopped 100 ::pending)))
          (is (not (realized? closed)))
          (await-result (on-virtual-thread #(protocols/emit! emitter "event: done\n\n" {:end? true})))
          (read-through! socket "event: done\n\n")
          (is (true? (deref closed 5000 false)))
          (await-result stopped)))
      (finally
        (when (realized? ready) (protocols/close! @ready))
        (when (= :running (:phase (busker/state server))) (busker/stop! server))))))

(deftest stalled-response-reader-resumes-with-exact-bytes-test
  (doseq [tls-protocol [nil "TLSv1.2" "TLSv1.3"]
          stop? [false true]]
    (testing (str (or tls-protocol "TCP") ", stop while stalled: " stop?)
      (let [payload (byte-array (* 4 1024 1024) (byte 65))
            started (promise)
            completed (promise)
            closed (promise)
            close-count (atom 0)
            [server port] (start-server
                           (fn [{:keys [uri] emitter :ol.busker.request/emitter}]
                             (if (= "/fixed" uri)
                               {:status 200 :body "fixed"}
                               (do
                                 (protocols/on-close! emitter
                                                      #(do (swap! close-count inc)
                                                           (deliver closed (.isVirtual (Thread/currentThread)))))
                                 (Thread/startVirtualThread
                                  #(do (deliver started true)
                                       (deliver completed
                                                (protocols/emit! emitter
                                                                 {:status 200
                                                                  :headers {"content-length" (str (alength payload))}
                                                                  :body payload}
                                                                 {:end? true}))))
                                 {:body emitter})))
                           {} tls-protocol)]
        (try
          (with-open [^Socket socket (connect port {:receive-buffer-size 1024 :tls-protocol tls-protocol})]
            (let [out (.getOutputStream socket)]
              (.write out (.getBytes "GET /fixed HTTP/1.1\r\nHost: localhost\r\n\r\n" StandardCharsets/US_ASCII))
              (.flush out)
              (let [{:keys [headers body]} (read-response! socket)]
                (is (str/starts-with? headers "HTTP/1.1 200"))
                (is (= "fixed" (String. ^bytes body StandardCharsets/US_ASCII)))))
            (request-stream! socket)
            (is (true? (deref started 2000 false)))
            (is (= ::stalled (deref completed 500 ::stalled)))
            (let [stopped (when stop? (on-virtual-thread #(busker/stop! server)))]
              (when stop?
                (is (= ::pending (deref stopped 100 ::pending)))
                (is (not (realized? closed))))
              (let [{:keys [headers body]} (read-response! socket)]
                (is (str/starts-with? headers "HTTP/1.1 200"))
                (is (java.util.Arrays/equals ^bytes payload ^bytes body))
                (is (= :accepted (:status (deref completed 10000 ::timeout)))))
              (is (= -1 (.read (.getInputStream socket))))
              (is (true? (deref closed 5000 false)))
              (when stop? (await-result stopped))))
          (finally (busker/stop! server)))
        (is (= 1 @close-count))))))

(deftest stalled-request-upload-resumes-with-exact-bytes-test
  (let [payload (byte-array (* 4 1024 1024) (byte 66))
        ready (promise)
        release (promise)
        received (promise)
        [server port] (start-server
                       (fn [{:keys [body]}]
                         (deliver ready true)
                         @release
                         (deliver received (.readAllBytes ^InputStream body))
                         {:status 200 :headers {"content-length" "2"} :body "ok"}))]
    (try
      (with-open [socket (connect port {:send-buffer-size 1024})]
        (let [^OutputStream out (.getOutputStream socket)]
          (.write out (.getBytes (str "POST / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: "
                                      (alength payload) "\r\n\r\n") StandardCharsets/US_ASCII))
          (.flush out)
          (let [upload (on-virtual-thread #(do (.write out payload) (.flush out) true))]
            (is (true? (deref ready 2000 false)))
            (is (= ::stalled (deref upload 500 ::stalled)))
            (deliver release true)
            (is (true? (await-result upload)))
            (is (= "ok" (String. ^bytes (:body (read-response! socket)) StandardCharsets/US_ASCII)))
            (is (java.util.Arrays/equals ^bytes payload ^bytes (deref received 10000 nil))))))
      (finally (deliver release true) (busker/stop! server)))))
(deftest streamed-chunks-on-an-idle-server-do-not-wait-for-the-poll-timeout-test
  ;; The idle poll waits for the cleanup timer, about 1000 ms.
  (let [timings_ (atom [])
        [server port]
        (start-server
         (fn [{emitter :ol.busker.request/emitter}]
           (Thread/startVirtualThread
            (fn []
              (try
                (protocols/emit! emitter {:status 200
                                          :headers {"content-type" "application/octet-stream"}})
                (dotimes [_ 8]
                  (let [{:keys [progress]} (protocols/emit! emitter (byte-array 1000 (byte 97)))
                        _ (Thread/sleep 20)
                        start (System/nanoTime)
                        _ (protocols/flush! emitter)
                        result (deref progress 5000 :timeout)]
                    (swap! timings_ conj [(:status result result)
                                          (quot (- (System/nanoTime) start) 1000000)])))
                (finally
                  (protocols/close! emitter)))))
           {:body emitter}))]
    (try
      (Thread/sleep 200)
      (let [result (util/curl :http :h1 port "/stream" :max-time 30
                              :args ["-N" "-o" "/dev/null" "-w" "%{size_download}"])]
        (is (= {:exit 0 :out "8000"} (select-keys result [:exit :out]))))
      (is (= 8 (count @timings_)))
      (is (every? (fn [[status ms]] (and (= :proceeded status) (< ms 500))) @timings_)
          (pr-str @timings_))
      (finally
        (busker/stop! server)))))

(defn- response-body ^bytes [^bytes response]
  (let [text (String. response StandardCharsets/ISO_8859_1)
        start (+ 4 (.indexOf text "\r\n\r\n"))]
    (java.util.Arrays/copyOfRange response start (alength response))))

(deftest ring-bodies-larger-than-the-output-buffer-arrive-intact-test
  (let [text (apply str (repeat 40 "é水😀 plain "))
        latin (str text (char 0xd800) "end")
        buffer (doto (ByteBuffer/wrap (byte-array (range 120)))
                 (.position 3)
                 (.limit 117))
        bodies {"/utf8" [{:status 200 :body text}
                         (.getBytes text StandardCharsets/UTF_8)]
                "/latin1" [{:status 200
                            :headers {"content-type" "text/plain; charset=ISO-8859-1"}
                            :body latin}
                           (.getBytes latin StandardCharsets/ISO_8859_1)]
                "/buffer" [{:status 200 :body buffer}
                           (byte-array (range 3 117))]}
        [server port] (start-server (fn [{:keys [uri]}] (first (get bodies uri)))
                                    {:output-buffer-size 16})]
    (try
      (doseq [[path [_ expected]] bodies]
        (is (= (vec expected) (vec (response-body (request-bytes port "GET" path)))) path))
      (is (= [3 117] [(.position buffer) (.limit buffer)]))
      (finally (busker/stop! server)))))

(deftest blocked-producer-does-not-delay-other-responses-test
  (doseq [path ["/streaming" "/fixed"]]
    (testing path
      (let [entered (promise)
            release (promise)
            completed (promise)
            [server port]
            (start-server
             (fn [{:keys [uri] emitter :ol.busker.request/emitter}]
               (if (= "/other" uri)
                 {:status 200 :body "other"}
                 (do
                   (Thread/startVirtualThread
                    #(deliver completed
                              (try
                                (when (= "/streaming" uri)
                                  (protocols/emit! emitter {:status 200} {:flush? true}))
                                (protocols/emit! emitter
                                                 (fn [^ByteBuffer b]
                                                   (.put b (.getBytes "blocked" StandardCharsets/US_ASCII))
                                                   (deliver entered true)
                                                   @release)
                                                 {:capacity 7 :end? true})
                                (catch Throwable t t))))
                   {:body emitter}))))]
        (try
          (with-open [socket (connect port)]
            (.write (.getOutputStream socket)
                    (.getBytes (str "GET " path " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                               StandardCharsets/US_ASCII))
            (is (true? (deref entered 5000 false)))
            (is (str/ends-with? (request-text port "GET" "/other") "other")
                "The worker serves another response while a producer fills.")
            (is (not (realized? completed)))
            (deliver release true)
            (is (= :accepted (:status (deref completed 5000 ::timeout))))
            (is (str/includes? (slurp (.getInputStream socket)) "blocked")))
          (finally
            (deliver release true)
            (busker/stop! server)))))))

(deftest native-termination-during-fill-keeps-the-block-test
  (let [entered (promise)
        release (promise)
        closed (promise)
        late-write (promise)
        completed (promise)
        [server port]
        (start-server
         (fn [{emitter :ol.busker.request/emitter}]
           (protocols/on-close! emitter #(deliver closed true))
           (Thread/startVirtualThread
            #(deliver completed
                      (try
                        (protocols/emit! emitter "head\n" {:flush? true})
                        (protocols/emit! emitter
                                         (fn [^ByteBuffer b]
                                           (.put b (byte 65))
                                           (deliver entered true)
                                           @release
                                           (deliver late-write
                                                    (try (.put b (byte 66)) :written
                                                         (catch Throwable t t))))
                                         {:capacity 2 :flush? true})
                        (catch Throwable t t))))
           {:body emitter})
         {:http1-req-io-timeout 500})]
    (try
      (with-open [socket (connect port)]
        (request-stream-with-unfinished-body! socket)
        (read-through! socket "head\n")
        (is (true? (deref entered 5000 false)))
        (is (true? (deref closed 10000 false)) "The I/O timeout ends the response during the fill.")
        (deliver release true)
        (is (= :written (deref late-write 5000 ::timeout)))
        (is (= {:status :closed} (deref completed 5000 ::timeout)))
        (is (not (str/includes? (slurp (.getInputStream socket)) "AB"))))
      (finally
        (deliver release true)
        (busker/stop! server)))))

(deftest heartbeats-behind-stalled-output-stay-bounded-test
  (let [ready (promise)
        [server port] (start-server
                       (fn [{emitter :ol.busker.request/emitter}]
                         (deliver ready emitter)
                         {:body emitter}))]
    (try
      (with-open [socket (connect port {:receive-buffer-size 1024})]
        (request-stream! socket)
        (let [emitter (deref ready 5000 nil)
              chunk (byte-array 32768 (byte 65))
              outcome
              (deref
               (on-virtual-thread
                (fn []
                  (protocols/emit! emitter {:status 200 :headers {"content-type" "text/event-stream"}})
                   ;; The client never reads. A chunk receipt that stays pending
                   ;; for two seconds marks the output as stalled.
                  (loop [last-receipt nil]
                    (let [result (protocols/emit! emitter chunk {:wait? false :flush? true})]
                      (cond
                        (= :accepted (:status result)) (recur (:progress result))
                        (and last-receipt (= ::stalled (deref last-receipt 2000 ::stalled))) nil
                        :else (recur last-receipt))))
                  (let [results (doall (repeatedly 10000 (fn [] (protocols/emit! emitter ":\n\n"
                                                                                 {:wait? false :flush? true}))))
                        accepted (filter #(= :accepted (:status %)) results)]
                    {:accepted (count accepted)
                     :full? (boolean (some #(= :full (:status %)) results))
                     :settled (count (filter (comp realized? :progress) accepted))})))
               60000 ::timeout)]
          (is (map? (:value outcome)) (pr-str outcome))
          (let [{:keys [accepted full? settled]} (:value outcome)]
            (is (<= accepted 64) "Pending receipts bound heartbeats behind stalled output.")
            (is (true? full?))
            (is (zero? settled)))
          (protocols/close! emitter)))
      (finally (busker/stop! server)))))

(deftest io-timeout-ends-a-response-the-client-does-not-read-test
  (let [ready (promise)
        closed (promise)
        [server port] (start-server
                       (fn [{emitter :ol.busker.request/emitter}]
                         (protocols/on-close! emitter #(deliver closed true))
                         (deliver ready emitter)
                         {:body emitter})
                       {:http1-req-io-timeout 500})]
    (try
      (with-open [socket (connect port {:receive-buffer-size 1024})]
        (request-stream! socket)
        (let [emitter (deref ready 5000 nil)
              chunk (byte-array 32768 (byte 65))
              writer (on-virtual-thread
                      #(loop [writes 0]
                         (if (= :accepted (:status (protocols/emit! emitter chunk {:flush? true})))
                           (recur (inc writes))
                           writes)))
              start (System/nanoTime)
              outcome (deref writer 30000 ::timeout)
              elapsed-ms (/ (- (System/nanoTime) start) 1000000.0)]
          (is (pos-int? (:value outcome)) (pr-str outcome))
          (is (< elapsed-ms 4000) (str "stalled write ended after " elapsed-ms "ms"))
          (is (true? (deref closed 5000 false)) "The I/O timeout ends the stalled write.")
          (is (not (protocols/open? emitter)))))
      (finally (busker/stop! server)))))

(deftest sustained-empty-writes-keep-other-connections-and-timers-responsive-test
  (let [busy (promise)
        busy-emitter (promise)
        silent-closed (promise)
        stop (promise)
        operations (atom 0)
        [server port]
        (start-server
         (fn [{:keys [uri] emitter :ol.busker.request/emitter}]
           (case uri
             "/other" {:status 200 :body "other"}
             "/silent" (do (protocols/on-close! emitter #(deliver silent-closed true))
                           (protocols/emit! emitter "silent\n" {:flush? true})
                           {:body emitter})
             "/busy" (do (deliver busy-emitter emitter)
                         (Thread/startVirtualThread
                          #(deliver busy
                                    (try
                                      (loop [n 0]
                                        (if (realized? stop)
                                          n
                                          (do
                                            (protocols/emit! emitter "" {:flush? true})
                                            (protocols/flush! emitter)
                                            (when (zero? (mod n 200))
                                              (protocols/emit! emitter ":\n\n" {:flush? true}))
                                            (swap! operations inc)
                                            (recur (inc n)))))
                                      (catch Throwable t t))))
                         {:body emitter})))
         {:http1-req-io-timeout 1500})]
    (try
      (with-open [busy-socket (connect port)
                  silent-socket (connect port)]
        (.write (.getOutputStream busy-socket)
                (.getBytes "GET /busy HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n" StandardCharsets/US_ASCII))
        (let [drain (Thread/startVirtualThread
                     #(try (.transferTo (.getInputStream busy-socket) (java.io.OutputStream/nullOutputStream))
                           (catch Throwable _ nil)))]
          (try
            (let [start @operations]
              (request-stream-with-unfinished-body! silent-socket "/silent")
              (read-through! silent-socket "silent\n")
              (dotimes [_ 20]
                (is (str/ends-with? (request-text port "GET" "/other") "other")))
              (is (true? (deref silent-closed 10000 false))
                  "The native I/O timer closes the silent stream.")
              (is (> @operations (+ start 1000)) "Control traffic continued meanwhile.")
              (is (protocols/open? @busy-emitter)))
            (deliver stop true)
            (is (integer? (deref busy 10000 ::timeout)))
            (finally
              (deliver stop true)
              (when (realized? busy-emitter) (protocols/close! @busy-emitter))
              (.close busy-socket)
              (.join drain 5000)))))
      (finally
        (deliver stop true)
        (busker/stop! server)))))
