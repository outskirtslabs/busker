(ns ol.busker.response-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [coffi.mem :as mem]
   [ol.busker :as busker]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as protocols]
   [ol.busker.response :as response]
   [ol.busker.response-queue :as response-queue]
   [ol.busker.byte-bounded-queue :as bbq]
   [ol.busker.test-utils :as util])
  (:import
   [java.io ByteArrayOutputStream OutputStream]
   [java.lang Thread$State]
   [java.net InetSocketAddress Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

(defn- start-server
  [handler]
  (let [port (util/free-port)
        server (busker/start!
                (util/with-handler
                  (fn [request]
                    (if (= "/__ready" (:uri request))
                      {:status 200 :body "ready"}
                      (handler request)))
                  {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                        :http3? false
                                        :tls false}}}))
        ready (util/wait-for-curl-ready! :http :h1 port "/__ready" :max-time 2)]
    (if (zero? (:exit ready))
      [server port]
      (do
        (busker/stop! server)
        (throw (ex-info "Server did not become ready" {:port port :ready ready}))))))

(defn- connect
  ([port]
   (connect port {}))
  ([port {:keys [receive-buffer-size send-buffer-size]}]
   (let [socket (Socket.)]
     (when receive-buffer-size
       (.setReceiveBufferSize socket (int receive-buffer-size)))
     (when send-buffer-size
       (.setSendBufferSize socket (int send-buffer-size)))
     (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 2000)
     socket)))

(defn- request-stream!
  [^Socket socket]
  (let [request (.getBytes "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n"
                           StandardCharsets/US_ASCII)
        out (.getOutputStream socket)]
    (.write out request)
    (.flush out)))

(defn- read-through!
  [^Socket socket expected]
  (let [input (.getInputStream socket)
        response (StringBuilder.)]
    (loop []
      (if (str/includes? (str response) expected)
        (str response)
        (let [b (.read input)]
          (when (neg? b)
            (throw (ex-info "Server closed before sending expected stream data"
                            {:response (str response)
                             :expected expected})))
          (.append response (char b))
          (recur))))))

(defn- post-stream!
  [^Socket socket content-length]
  (let [request (.getBytes (str "POST / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nContent-Length: " content-length "\r\n\r\n")
                           StandardCharsets/US_ASCII)
        out (.getOutputStream socket)]
    (.write out request)
    (.flush out)
    out))

(defn- read-response!
  [^Socket socket]
  (let [headers (read-through! socket "\r\n\r\n")
        content-length (some->> (re-find #"(?im)^content-length: (\d+)\r?$" headers)
                                second
                                Long/parseLong)
        body (.readNBytes (.getInputStream socket) (int content-length))]
    {:headers headers
     :body body}))

(defrecord TestWriter [stopped?_ out-stream]
  pi/Stopable
  (stop [_]
    (.set ^AtomicBoolean stopped?_ true)))

(defn- headers-summary
  [headers headers-len]
  (let [header-size (mem/size-of ::h2o/clj-header-t)
        header-segments (mapv #(mem/slice headers (* % header-size) header-size)
                              (range headers-len))]
    {:headers
     (mapv (fn [header-seg]
             (let [{:keys [name name_len value value_len]}
                   (mem/deserialize header-seg ::h2o/clj-header-t)]
               [(h2o/->string name name_len)
                (h2o/->string value value_len)]))
           header-segments)
     :layout-equivalent?
     (every? (fn [header-seg]
               (let [header (mem/deserialize header-seg ::h2o/clj-header-t)
                     generic (mem/serialize header ::h2o/clj-header-t)]
                 (java.util.Arrays/equals
                  ^bytes (mem/read-bytes header-seg header-size)
                  ^bytes (mem/read-bytes generic header-size))))
             header-segments)}))


(defn- test-emitter
  ([]
   (test-emitter {:stopped?_ (AtomicBoolean. false)
                  :out-stream (ByteArrayOutputStream.)}
                 #(Thread/startVirtualThread %)))
  ([writer]
   (test-emitter writer #(Thread/startVirtualThread %)))
  ([writer callback-dispatch]
   (let [callback-tail_ (atom (promise))]
     (deliver @callback-tail_ true)
     (response/->H2OResponseEmitter
      nil
      writer
      (atom nil)
      (Object.)
      (atom {:phase :open :callbacks []})
      callback-dispatch
      callback-tail_))))

(deftest response-header-layout-preserved-test
  (let [[final-headers final-headers-len final-content-length]
        (response/build-headers {:headers {"Content-Length" "9"
                                           "X-Repeat" ["first" "second"]}})
        [_ informational-headers informational-headers-len informational-content-length]
        (response/build-headers2 {:headers {"X-Repeat" ["first" "second"]}})]
    (is (= {:final {:content-length 9
                   :headers [["X-Repeat" "first"]
                             ["X-Repeat" "second"]]
                   :layout-equivalent? true}
            :informational {:content-length -1
                            :headers [["x-repeat" "first"]
                                      ["x-repeat" "second"]]
                            :layout-equivalent? true}}
           {:final (assoc (headers-summary final-headers final-headers-len)
                          :content-length final-content-length)
            :informational (assoc (headers-summary informational-headers informational-headers-len)
                                  :content-length informational-content-length)}))))


(defn- blocked?
  [^Thread thread]
  (loop [attempt 0]
    (cond
      (= Thread$State/WAITING (.getState thread)) true
      (= attempt 100) false
      :else (do
              (Thread/sleep 10)
              (recur (inc attempt))))))

(deftest explicit-close-delivers-on-virtual-thread-test
  (let [callback_ (promise)
        callback-count_ (atom 0)
        [server port]
        (start-server
         (fn [{emitter :ol.busker.request/emitter}]
           (protocols/on-close emitter
                               (fn []
                                 (swap! callback-count_ inc)
                                 (deliver callback_ (Thread/currentThread))))
           (protocols/emit! emitter {:status 200 :headers {}})
           (protocols/emit! emitter "closed")
           (protocols/close emitter)
           {:body emitter}))]
    (try
      (is (= "closed" (:out (util/curl :http :h1 port "/" :max-time 5))))
      (let [callback-thread (deref callback_ 2000 ::timeout)]
        (is (not= ::timeout callback-thread))
        (is (.isVirtual ^Thread callback-thread))
        (is (= 1 @callback-count_)))
      (finally
        (busker/stop! server)))))

(deftest final-ring-response-delivers-close-callback-test
  (let [callback_ (promise)
        callback-count_ (atom 0)
        [server port]
        (start-server
         (fn [{emitter :ol.busker.request/emitter}]
           (protocols/on-close emitter
                               (fn []
                                 (swap! callback-count_ inc)
                                 (deliver callback_ true)))
           {:status 200 :body "complete"}))]
    (try
      (is (= "complete" (:out (util/curl :http :h1 port "/" :max-time 5))))
      (is (true? (deref callback_ 2000 false)))
      (is (= 1 @callback-count_))
      (finally
        (busker/stop! server)))))

(deftest reported-client-termination-delivers-close-callback-test
  (let [callback_ (promise)
        callback-count_ (atom 0)
        write-after-disconnect_ (promise)
        [server port]
        (start-server
         (fn [{emitter :ol.busker.request/emitter}]
           (protocols/on-close emitter
                               #(do
                                  (swap! callback-count_ inc)
                                  (throw (ex-info "expected close callback failure" {}))))
           (protocols/on-close emitter
                               #(do
                                  (swap! callback-count_ inc)
                                  (deliver callback_ (Thread/currentThread))))
           (Thread/startVirtualThread
            (fn []
              (protocols/emit! emitter {:status 200
                                        :headers {"content-type" "text/event-stream"}})
              (protocols/emit! emitter "event: ready\n\n")
              (protocols/flush emitter)
              @write-after-disconnect_
              (try
                (protocols/emit! emitter "event: probe\n\n")
                (protocols/flush emitter)
                (catch Exception _
                  nil))))
           {:body emitter}))]
    (try
      (let [^Socket socket (connect port)]
        (try
          (.setSoTimeout socket 2000)
          (.setSoLinger socket true 0)
          (request-stream! socket)
          (read-through! socket "event: ready\n\n")
          (finally
            (.close socket))))
      (deliver write-after-disconnect_ true)
      (let [callback-thread (deref callback_ 2000 ::timeout)]
        (is (not= ::timeout callback-thread))
        (is (.isVirtual ^Thread callback-thread))
        (is (= 2 @callback-count_)))
      (finally
        (busker/stop! server)))))

(deftest server-stop-drains-stream-before-close-callback-test
  (let [stream-ready_ (promise)
        finish-stream_ (promise)
        callback_ (promise)
        [server port]
        (start-server
         (fn [{emitter :ol.busker.request/emitter}]
           (protocols/on-close emitter
                               #(deliver callback_ (Thread/currentThread)))
           (Thread/startVirtualThread
            (fn []
              (protocols/emit! emitter {:status 200
                                        :headers {"content-type" "text/event-stream"}})
              (protocols/emit! emitter "event: ready\n\n")
              (protocols/flush emitter)
              (deliver stream-ready_ true)
              @finish-stream_
              (protocols/emit! emitter "event: done\n\n")
              (protocols/close emitter)))
           {:body emitter}))]
    (try
      (let [^Socket socket (connect port)]
        (try
          (.setSoTimeout socket 2000)
          (request-stream! socket)
          (read-through! socket "event: ready\n\n")
          (is (true? (deref stream-ready_ 2000 false)))
          (let [stop-future (future (busker/stop! server))]
            (is (= ::timeout (deref stop-future 100 ::timeout)))
            (is (= ::timeout (deref callback_ 100 ::timeout)))
            (deliver finish-stream_ true)
            (read-through! socket "event: done\n\n")
            (let [callback-thread (deref callback_ 2000 ::timeout)]
              (is (not= ::timeout callback-thread))
              (when (instance? Thread callback-thread)
                (is (.isVirtual ^Thread callback-thread))))
            (is (not= ::timeout (deref stop-future 10000 ::timeout))))
          (finally
            (.close socket))))
      (finally
        (deliver finish-stream_ true)
        (when (= :running (:phase (busker/state server)))
          (busker/stop! server))))))

(deftest close-callbacks-are-ordered-isolated-and-late-test
  (let [emitter (test-emitter)
        callbacks_ (atom [])
        completed_ (promise)]
    (protocols/on-close emitter #(swap! callbacks_ conj :first))
    (protocols/on-close emitter #(throw (ex-info "expected callback failure" {})))
    (protocols/on-close emitter #(swap! callbacks_ conj :third))
    (is (true? (protocols/close emitter)))
    (is (false? (protocols/close emitter)))
    (protocols/on-close emitter #(do
                                   (swap! callbacks_ conj :late)
                                   (deliver completed_ (Thread/currentThread))))
    (let [callback-thread (deref completed_ 2000 ::timeout)]
      (is (= [:first :third :late] @callbacks_))
      (is (.isVirtual ^Thread callback-thread))
      (is (false? (protocols/open? emitter))))))

(deftest callback-registration-racing-with-close-delivers-once-test
  (let [emitter (test-emitter)
        registrations 32
        ready (CountDownLatch. registrations)
        start (CountDownLatch. 1)
        delivered (CountDownLatch. registrations)
        callbacks_ (atom [])]
    (doseq [i (range registrations)]
      (Thread/startVirtualThread
       (fn []
         (.countDown ready)
         (.await start)
         (protocols/on-close emitter
                             #(do
                                (swap! callbacks_ conj i)
                                (.countDown delivered))))))
    (.await ready 2 TimeUnit/SECONDS)
    (Thread/startVirtualThread
     (fn []
       (.await start)
       (protocols/close emitter)))
    (.countDown start)
    (is (.await delivered 2 TimeUnit/SECONDS))
    (is (= (set (range registrations)) (set @callbacks_)))
    (is (= registrations (count @callbacks_)))))

(deftest request-cleanup-stops-writer-before-dispatching-callbacks-test
  (let [stopped?_ (AtomicBoolean. false)
        callback_ (promise)
        emitter (test-emitter (->TestWriter stopped?_ (ByteArrayOutputStream.)))]
    (protocols/on-close emitter #(deliver callback_ (.get stopped?_)))
    (response/stop-emitter emitter)
    (is (true? (deref callback_ 2000 false)))))

(deftest late-registration-waits-for-terminal-callback-batch-test
  (let [first-dispatch_ (promise)
        release-first_ (promise)
        first? (AtomicBoolean. true)
        callbacks_ (atom [])
        completed_ (promise)
        registered_ (promise)
        emitter
        (test-emitter
         {:stopped?_ (AtomicBoolean. false)
          :out-stream (ByteArrayOutputStream.)}
         (fn [task]
           (if (.compareAndSet first? true false)
             (do
               (deliver first-dispatch_ true)
               @release-first_
               (Thread/startVirtualThread task))
             (Thread/startVirtualThread task))))]
    (protocols/on-close emitter #(swap! callbacks_ conj :before))
    (Thread/startVirtualThread #(protocols/close emitter))
    (is (true? (deref first-dispatch_ 1000 false)))
    (Thread/startVirtualThread
     #(do
        (protocols/on-close emitter
                            (fn []
                              (swap! callbacks_ conj :late)
                              (deliver completed_ true)))
        (deliver registered_ true)))
    (is (true? (deref registered_ 1000 false)))
    (is (= [] @callbacks_))
    (deliver release-first_ true)
    (is (true? (deref completed_ 1000 false)))
    (is (= [:before :late] @callbacks_))))

(deftest stopped-writer-close-delivers-callback-and-returns-false-test
  (let [stopped?_ (AtomicBoolean. true)
        callback_ (promise)
        emitter (test-emitter (->TestWriter stopped?_ (ByteArrayOutputStream.)))]
    (protocols/on-close emitter #(deliver callback_ true))
    (is (false? (protocols/open? emitter)))
    (is (false? (protocols/close emitter)))
    (is (true? (deref callback_ 1000 false)))
    (is (false? (protocols/close emitter)))))

(deftest request-cleanup-releases-blocked-response-writers-before-callbacks-test
  (let [queue (bbq/byte-bounded-spsc-queue 5)
        stopped?_ (AtomicBoolean. false)
        writer (response-queue/->H2OResponseWriter
                nil queue 5
                (AtomicBoolean. false)
                (AtomicReference. nil)
                (AtomicBoolean. false)
                stopped?_
                (AtomicBoolean. false)
                (AtomicReference. nil)
                nil {})
        emitter (test-emitter writer)
        blocked (response-queue/->Chunk [] 5 false)
        producer-ready_ (promise)
        producer-released_ (promise)
        callback_ (promise)]
    (bbq/put queue blocked)
    (let [producer
          (Thread/startVirtualThread
           #(do
              (deliver producer-ready_ true)
              (try
                (bbq/put queue blocked)
                (catch IllegalStateException _
                  (deliver producer-released_ true)))))]
      (is (true? (deref producer-ready_ 1000 false)))
      (is (blocked? producer)))
    (protocols/on-close emitter #(deliver callback_ (deref producer-released_ 1000 false)))
    (response/stop-emitter emitter)
    (is (true? (deref callback_ 1000 false)))
    (is (.get stopped?_))))

(deftest concurrent-close-does-not-dispatch-before-writer-closes-test
  (let [close-entered_ (promise)
        release-close_ (promise)
        callback_ (promise)
        out-stream (proxy [OutputStream] []
                     (close []
                       (deliver close-entered_ true)
                       @release-close_))
        emitter (test-emitter (->TestWriter (AtomicBoolean. false) out-stream))]
    (protocols/on-close emitter #(deliver callback_ true))
    (let [first-close (Thread/startVirtualThread #(protocols/close emitter))]
      (is (true? (deref close-entered_ 1000 false)))
      (is (false? (protocols/close emitter)))
      (is (= ::timeout (deref callback_ 100 ::timeout)))
      (deliver release-close_ true)
      (.join first-close 1000)
      (is (true? (deref callback_ 1000 false))))))

(deftest stalled-response-reader-resumes-with-exact-bytes-test
  (let [payload (byte-array (* 4 1024 1024) (byte 65))
        write-started_ (promise)
        write-completed_ (promise)
        socket-errors_ (atom [])
        producer_ (promise)
        [server port]
        (start-server
         (fn [{emitter :ol.busker.request/emitter}]
           (deliver producer_
                    (Thread/startVirtualThread
                     (fn []
                       (deliver write-started_ true)
                       (protocols/emit! emitter {:status 200
                                                 :headers {"content-length" (str (alength payload))}})
                       (protocols/emit! emitter payload)
                       (protocols/close emitter)
                       (deliver write-completed_ true))))
           {:body emitter}))]
    (try
      (with-open [^Socket socket (connect port {:receive-buffer-size 1024})]
        (.setSoTimeout socket 10000)
        (request-stream! socket)
        (is (true? (deref write-started_ 2000 false)))
        (is (= ::stalled (deref write-completed_ 500 ::stalled)))
        (is (blocked? (deref producer_ 2000 nil)))
        (let [{:keys [headers body]}
              (try
                (read-response! socket)
                (catch Throwable e
                  (swap! socket-errors_ conj (ex-message e))
                  {}))]
          (is (= [] @socket-errors_))
          (is (str/starts-with? (or headers "") "HTTP/1.1 200"))
          (is (= (alength payload) (alength ^bytes body)))
          (is (java.util.Arrays/equals ^bytes payload ^bytes body))
          (is (true? (deref write-completed_ 10000 false)))))
      (finally
        (busker/stop! server)))))

(deftest stalled-request-upload-resumes-with-exact-bytes-test
  (let [payload (byte-array (* 4 1024 1024) (byte 66))
        reader-ready_ (promise)
        release-reader_ (promise)
        received_ (promise)
        upload-started_ (promise)
        upload-completed_ (promise)
        socket-errors_ (atom [])
        [server port]
        (start-server
         (fn [{:keys [body]}]
           (let [^java.io.InputStream body body]
             (deliver reader-ready_ true)
             @release-reader_
             (let [received (.readAllBytes body)]
               (deliver received_ received)
               {:status 200 :headers {"content-length" "2"} :body "ok"}))))]
    (try
      (with-open [^Socket socket (connect port {:send-buffer-size 1024})]
        (.setSoTimeout socket 10000)
        (let [^OutputStream out (post-stream! socket (alength payload))
              producer (Thread/startVirtualThread
                        (fn []
                          (try
                            (deliver upload-started_ true)
                            (.write out payload)
                            (.flush out)
                            (deliver upload-completed_ true)
                            (catch Throwable e
                              (swap! socket-errors_ conj (ex-message e))
                              (deliver upload-completed_ :error)))))]
          (is (true? (deref reader-ready_ 2000 false)))
          (is (true? (deref upload-started_ 2000 false)))
          (is (= ::stalled (deref upload-completed_ 500 ::stalled)))
          (is (blocked? producer))
          (deliver release-reader_ true)
          (is (true? (deref upload-completed_ 10000 false)))
          (let [{:keys [headers body]}
                (try
                  (read-response! socket)
                  (catch Throwable e
                    (swap! socket-errors_ conj (ex-message e))
                    {}))]
            (is (= [] @socket-errors_))
            (is (str/starts-with? (or headers "") "HTTP/1.1 200"))
            (is (= "ok" (some-> ^bytes body (String. StandardCharsets/US_ASCII))))
            (is (java.util.Arrays/equals ^bytes payload ^bytes (deref received_ 10000 nil))))))
      (finally
        (deliver release-reader_ true)
        (busker/stop! server)))))