(ns ol.busker.response-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [ol.busker :as busker]
   [ol.busker.protocols :as protocols]
   [ol.busker.test-utils :as util])
  (:import
   [java.io InputStream OutputStream]
   [java.net InetSocketAddress Socket]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent CountDownLatch TimeUnit]))

(defn- start-server
  ([handler] (start-server handler {}))
  ([handler config]
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
                                                :http3? false :tls false}}}
                          config)))
         ready (util/wait-for-curl-ready! :http :h1 port "/__ready" :max-time 2)]
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

(defn- connect
  ([port] (connect port {}))
  ([port {:keys [receive-buffer-size send-buffer-size]}]
   (let [socket (Socket.)]
     (when receive-buffer-size (.setReceiveBufferSize socket (int receive-buffer-size)))
     (when send-buffer-size (.setSendBufferSize socket (int send-buffer-size)))
     (.setSoTimeout socket 10000)
     (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 2000)
     socket)))

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
  (let [payload (byte-array (* 4 1024 1024) (byte 65))
        started (promise)
        completed (promise)
        [server port] (start-server
                       (fn [{emitter :ol.busker.request/emitter}]
                         (Thread/startVirtualThread
                          #(do (deliver started true)
                               (deliver completed
                                        (protocols/emit! emitter
                                                         {:status 200
                                                          :headers {"content-length" (str (alength payload))}
                                                          :body payload}
                                                         {:end? true}))))
                         {:body emitter}))]
    (try
      (with-open [socket (connect port {:receive-buffer-size 1024})]
        (request-stream! socket)
        (is (true? (deref started 2000 false)))
        (is (= ::stalled (deref completed 500 ::stalled)))
        (let [{:keys [headers body]} (read-response! socket)]
          (is (str/starts-with? headers "HTTP/1.1 200"))
          (is (java.util.Arrays/equals ^bytes payload ^bytes body))
          (is (= :accepted (:status (deref completed 10000 ::timeout))))))
      (finally (busker/stop! server)))))

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
