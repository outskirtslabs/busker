(ns ol.busker.large-response-head-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as protocols]
   [ol.busker.response :as response]
   [ol.busker.response-queue :as response-queue]
   [ol.busker.response-serialization :as serialization]
   [ol.busker.test-utils :as util]
   [taoensso.trove :as trove])
  (:import
   [java.lang.foreign Arena FunctionDescriptor Linker Linker$Option MemoryLayout
    MemorySegment ValueLayout]
   [java.lang.invoke MethodHandle]
   [java.net URI]
   [java.net.http HttpClient HttpClient$Version HttpRequest HttpResponse
    HttpResponse$BodyHandlers]
   [java.io InputStream]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.time Duration]
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicInteger]))

(defn- pairs [n value]
  (mapv (fn [i] [(format "x-h-%04d" i) value]) (range n)))

;; The fixed header space takes 64 pairs and 16384 staging bytes. A single pair
;; stages 32 descriptor bytes, its name and value, and two zero bytes.
(def ^:private heads
  {"count-limit" (pairs 64 "v")
   "count-over"  (pairs 65 "v")
   "bytes-limit" [["x-large" (apply str (repeat 16343 "b"))]]
   "bytes-over"  [["x-large" (apply str (repeat 16344 "b"))]]
   ;; 1000 pairs and about 1 MiB of names and values.
   "huge"        (pairs 1000 (apply str (repeat 1040 "v")))
   ;; curl rejects response heads above about 300 KiB.
   "curl-huge"   (pairs 1000 (apply str (repeat 250 "v")))})

(defn- fits? [case-name]
  (let [head (heads case-name)]
    (fixed-final/head-fits? (count head) (serialization/header-bytes head))))

(defn- header-map [head]
  (apply array-map (mapcat identity head)))

(defn- body-bytes ^bytes [] (.getBytes "ok" StandardCharsets/UTF_8))

(def ^:private emit-statuses (atom []))

(def ^:private blocking-body (atom nil))

(defn- gated-stream
  "Returns an InputStream whose first read counts down `entered` and waits
  for `release`, then yields \"ok\" and ends."
  ^InputStream [^CountDownLatch entered ^CountDownLatch release]
  (let [sent? (atom false)]
    (proxy [InputStream] []
      (read
        ([] (throw (UnsupportedOperationException.)))
        ([^bytes buffer] (.read ^InputStream this buffer 0 (alength buffer)))
        ([^bytes buffer offset length]
         (.countDown entered)
         (when-not (.await release 20 TimeUnit/SECONDS)
           (throw (ex-info "Body was not released" {})))
         (if (first (reset-vals! sent? true))
           -1
           (let [ok (body-bytes)]
             (System/arraycopy ok 0 buffer (int offset) (alength ok))
             (alength ok))))))))

(defn- emit-recorded! [emitter response opts]
  (let [status (:status (protocols/emit! emitter response opts))]
    (swap! emit-statuses conj status)
    status))

(defn- handler
  [{:keys [uri] emitter :ol.busker.request/emitter}]
  (let [[_ shape case-name] (str/split uri #"/")
        headers (header-map (heads case-name))]
    (case shape
      "fixed" {:status 200 :headers headers :body "ok"}
      "producer" (do (protocols/emit! emitter {:status 200 :headers headers
                                               :body (fn [^ByteBuffer buffer] (.put buffer (body-bytes)))}
                                      {:end? true :capacity 2})
                     {:body emitter})
      "stream" (do (protocols/emit! emitter {:status 200 :headers headers})
                   (protocols/emit! emitter "ok" {:end? true})
                   {:body emitter})
      "info" (do (protocols/emit! emitter {:status 103 :headers headers})
                 {:status 200 :headers {"x-final" "yes"} :body "ok"})
      "retry" (let [emit #(emit-recorded! emitter {:status 200 :headers headers :body "ok"}
                                          {:wait? false :end? true})]
                (when (= :full (emit)) (emit))
                {:body emitter})
      "blocking" {:status 200 :headers headers :body @blocking-body}
      "wrong-length" {:status 200 :headers (assoc headers "content-length" "5") :body "ok"}
      "closed" (do (emit-recorded! emitter {:status 200 :headers headers :body "ok"} {:end? true})
                   (protocols/close! emitter)
                   {:body emitter})
      {:status 200 :body "ready"})))

(defn- start-server []
  (let [tls-port (util/free-port)
        plain-port (util/free-port)
        server (busker/start!
                (util/with-static-tls
                  (util/with-handler
                    handler
                    {:n-workers 1
                     :compress? false
                     :entrypoints {:tls {:bind (str "127.0.0.1:" tls-port) :http3? true}
                                   :plain {:bind (str "127.0.0.1:" plain-port) :tls false}}})))]
    (util/wait-for-curl-ready! :https :h1 tls-port "/")
    (util/wait-for-curl-ready! :http :h1 plain-port "/")
    {:server server :tls-port tls-port :plain-port plain-port}))

(defn- observe
  "Calls `f` with `redefs` in place and returns its result with the fixed slots
  published, the streaming writers created and the data of each large-head
  warning while it ran."
  ([f] (observe {} f))
  ([redefs f]
   (let [published (AtomicInteger.)
         writers (AtomicInteger.)
         warnings (atom [])
         publish h2o/mt-response-publish
         create-writer response-queue/create-response-writer]
     (with-redefs-fn
       (merge
        {#'h2o/mt-response-publish
         (fn [receiver claim-handle]
           (let [result (publish receiver claim-handle)]
             (when (= 1 result) (.incrementAndGet published))
             result))
         #'response-queue/create-response-writer
         (fn [req] (.incrementAndGet writers) (create-writer req))
         #'trove/*log-fn*
         (fn [_ns _coords level id lazy_]
           (when (= ::response/large-head id)
             (swap! warnings conj [level (:data (force lazy_))])))}
        redefs)
       (fn []
         (let [result (f)]
           {:result result :paths [(.get published) (.get writers)] :warnings @warnings}))))))

(defn- curl-heads
  "Requests `path` over `proto` with curl and returns its exit code, its body
  and each response head as `[status x-headers]`, x-headers in wire order."
  [proto port path]
  (let [{:keys [exit out err]} (util/curl :https proto port path :args ["-i"] :max-time 30)
        blocks (str/split out #"\r\n\r\n")
        head (fn [block]
               (let [[status-line & lines] (str/split-lines block)]
                 [(parse-long (second (str/split status-line #" ")))
                  (into []
                        (keep (fn [line]
                                (let [[k v] (str/split line #":\s*" 2)]
                                  (when (str/starts-with? (str/lower-case k) "x-")
                                    [(str/lower-case k) v]))))
                        lines)]))]
    {:exit exit :err err :body (last blocks) :heads (mapv head (butlast blocks))}))

(defn- request-path
  "Returns the path of a test request. Its query string must not reach the log."
  [shape case-name]
  (str "/" shape "/" case-name "?token=secret"))

(defn- warning [status shape case-name]
  (let [head (heads case-name)]
    [:warn {:request-method :get :uri (str "/" shape "/" case-name)
            :status status :header-count (count head)
            :header-bytes (serialization/header-bytes head)}]))

(deftest the-cases-sit-on-either-side-of-the-fixed-header-space
  (is (= 16384 (serialization/header-staging-bytes (heads "bytes-limit"))))
  (is (= 16385 (serialization/header-staging-bytes (heads "bytes-over"))))
  (is (= [true true false false false false]
         (mapv fits? ["count-limit" "bytes-limit" "count-over" "bytes-over" "huge" "curl-huge"]))))

(deftest heads-above-the-fixed-space-are-sent-intact-over-every-protocol
  (let [{:keys [server tls-port]} (start-server)]
    (try
      (doseq [proto [:h1 :h2 :h3]
              case-name ["count-limit" "bytes-limit" "count-over" "bytes-over"]
              shape ["fixed" "producer" "stream" "info"]]
        (let [{:keys [result paths warnings]}
              (observe #(curl-heads proto tls-port (request-path shape case-name)))
              context (pr-str [proto shape case-name (:err result)])
              fits (fits? case-name)
              sent (heads case-name)]
          (is (zero? (:exit result)) context)
          (is (= "ok" (:body result)) context)
          (is (= (cond
                   (not= "info" shape) [[200 sent]]
                   ;; H2O sends no 1xx over HTTP/1.1 by default (send-informational: except-h1).
                   (= :h1 proto) [[200 [["x-final" "yes"]]]]
                   :else [[103 sent] [200 [["x-final" "yes"]]]])
                 (:heads result))
              context)
          (is (= (if (and fits (#{"fixed" "producer"} shape)) [1 0] [0 1]) paths) context)
          (is (= (if fits [] [(warning (if (= "info" shape) 103 200) shape case-name)]) warnings)
              context)))
      (finally
        (busker/stop! server)))))

(defn- jdk-client [version]
  ;; The JDK client limits a head to 384 KiB and an HPACK block to 512 indexed
  ;; literals. The test alias raises both with jdk.http.maxHeaderSize and
  ;; jdk.httpclient.maxLiteralWithIndexing.
  (-> (HttpClient/newBuilder)
      (.version version)
      (.connectTimeout (Duration/ofSeconds 10))
      (.build)))

(defn- jdk-heads
  "Requests `path` over plain HTTP with `client` and returns the protocol
  version, the status, the body and the x-headers as a map of value lists."
  [^HttpClient client port path]
  (let [request (-> (HttpRequest/newBuilder (URI. (str "http://127.0.0.1:" port path)))
                    (.timeout (Duration/ofSeconds 30))
                    (.build))
        ^HttpResponse response (.send client request (HttpResponse$BodyHandlers/ofString))]
    {:version (.version response)
     :status (.statusCode response)
     :body (.body response)
     :headers (into (sorted-map)
                    (keep (fn [[k v]] (when (str/starts-with? k "x-") [k (vec v)])))
                    (.map (.headers response)))}))

(defn- multimap [head]
  (reduce (fn [m [k v]] (update m k (fnil conj []) v)) (sorted-map) head))

(deftest a-head-of-a-thousand-headers-and-a-mebibyte-is-sent-intact
  (let [{:keys [server tls-port plain-port]} (start-server)]
    (try
      (doseq [[proto version] [[:h1 HttpClient$Version/HTTP_1_1] [:h2 HttpClient$Version/HTTP_2]]
              :let [client (jdk-client version)]
              shape ["fixed" "producer" "stream"]]
        (let [{:keys [result paths warnings]}
              (observe #(jdk-heads client plain-port (request-path shape "huge")))
              context (pr-str [proto shape])]
          (is (= [version 200 "ok"] [(:version result) (:status result) (:body result)]) context)
          (is (= (multimap (heads "huge")) (:headers result)) context)
          (is (= [0 1] paths) context)
          (is (= [(warning 200 shape "huge")] warnings) context)))
      (doseq [shape ["fixed" "producer" "stream"]]
        (let [{:keys [result paths warnings]}
              (observe #(curl-heads :h3 tls-port (request-path shape "curl-huge")))
              context (pr-str [:h3 shape (:err result)])]
          (is (zero? (:exit result)) context)
          (is (= [[200 (heads "curl-huge")]] (:heads result)) context)
          (is (= "ok" (:body result)) context)
          (is (= [0 1] paths) context)
          (is (= [(warning 200 shape "curl-huge")] warnings) context)))
      (finally
        (busker/stop! server)))))

(defn- refusing-emit-bounded
  "Returns `emit-bounded!` that answers `status` to its first `n` calls without
  queueing, then queues as usual."
  [n status]
  (let [real response-queue/emit-bounded!
        left (atom n)]
    (fn [st head capacity producer opts]
      (if (pos? (long (first (swap-vals! left dec))))
        {:status status}
        (real st head capacity producer opts)))))

(deftest a-large-head-warns-once-when-it-is-accepted
  (let [{:keys [server tls-port]} (start-server)]
    (try
      (doseq [proto [:h1 :h2 :h3]]
        (testing "a nonblocking emit! that gets :full and is retried"
          (reset! emit-statuses [])
          (let [{:keys [result warnings]}
                (observe {#'response-queue/emit-bounded! (refusing-emit-bounded 1 :full)}
                         #(curl-heads proto tls-port (request-path "retry" "count-over")))]
            (is (= [:full :accepted] @emit-statuses) (pr-str proto))
            (is (= [[200 (heads "count-over")]] (:heads result)) (pr-str proto))
            (is (= [(warning 200 "retry" "count-over")] warnings) (pr-str proto))))
        (testing "an emit! that gets :closed"
          (reset! emit-statuses [])
          (let [{:keys [result warnings]}
                (observe {#'response-queue/emit-bounded! (refusing-emit-bounded 1000 :closed)}
                         #(curl-heads proto tls-port (request-path "closed" "count-over")))]
            (is (zero? (:exit result)) (pr-str proto (:err result)))
            (is (= [:closed] @emit-statuses) (pr-str proto))
            (is (= [] warnings) (pr-str proto)))))
      (finally
        (busker/stop! server)))))

(deftest a-large-head-with-a-wrong-content-length-gets-a-500
  (let [{:keys [server tls-port]} (start-server)]
    (try
      (doseq [proto [:h1 :h2 :h3]]
        (let [{:keys [result warnings]}
              (observe #(curl-heads proto tls-port (request-path "wrong-length" "count-over")))]
          (is (zero? (:exit result)) (pr-str proto (:err result)))
          (is (= [[500 []]] (:heads result)) (pr-str proto))
          (is (= "Internal Server Error" (:body result)) (pr-str proto))
          (is (= [] warnings) (pr-str proto))))
      (finally
        (busker/stop! server)))))

(deftest a-streaming-body-warns-once-before-it-ends
  (let [{:keys [server tls-port]} (start-server)
        warnings (atom [])]
    (try
      (with-redefs [trove/*log-fn* (fn [_ns _coords level id lazy_]
                                     (when (= ::response/large-head id)
                                       (swap! warnings conj [level (:data (force lazy_))])))]
        (doseq [proto [:h1 :h2 :h3]]
          (reset! warnings [])
          (let [entered (CountDownLatch. 1)
                release (CountDownLatch. 1)
                _ (reset! blocking-body (gated-stream entered release))
                result (future (curl-heads proto tls-port (request-path "blocking" "count-over")))
                expected [(warning 200 "blocking" "count-over")]]
            (try
              (is (.await entered 10 TimeUnit/SECONDS) (pr-str proto))
              (is (= expected @warnings) (pr-str proto :while-the-body-blocks))
              (finally (.countDown release)))
            (let [received (deref result 30000 ::timeout)]
              (is (zero? (:exit received)) (pr-str proto (:err received)))
              (is (= [[200 (heads "count-over")]] (:heads received)) (pr-str proto))
              (is (= "ok" (:body received)) (pr-str proto)))
            (is (= expected @warnings) (pr-str proto :after-the-body-ends)))))
      (finally
        (busker/stop! server)))))

;; Native heap in use, from glibc mallinfo2: uordblks plus hblkhd, the bytes of
;; mmapped blocks. Arena memory and the H2O request pool both come from malloc.
(def ^:private ^MethodHandle mallinfo2
  (let [linker (Linker/nativeLinker)]
    (when-let [symbol (.orElse (.find (.defaultLookup linker) "mallinfo2") nil)]
      (.downcallHandle linker symbol
                       (FunctionDescriptor/of
                        (MemoryLayout/structLayout
                         (into-array MemoryLayout (repeat 10 ValueLayout/JAVA_LONG)))
                        (into-array MemoryLayout []))
                       (into-array Linker$Option [])))))

(defn- heap-in-use ^long []
  (with-open [arena (Arena/ofConfined)]
    (let [^MemorySegment info (.invokeWithArguments mallinfo2 ^java.util.List [arena])]
      (+ (.get info ValueLayout/JAVA_LONG 32) (.get info ValueLayout/JAVA_LONG 56)))))

(defn- heap-follows-allocation-once? []
  (let [size (* 4 1024 1024)
        before (heap-in-use)
        arena (Arena/ofConfined)
        _ (.fill (.allocate arena (long size)) (byte 1))
        allocated (heap-in-use)
        _ (.close arena)
        released (heap-in-use)]
    (and (>= allocated (+ before size)) (< released (+ before (quot size 2))))))

(defn- heap-follows-allocation?
  "Other threads allocate between the readings, so one attempt in ten must
  see the probe allocation and its release."
  []
  (boolean (some (fn [_] (heap-follows-allocation-once?)) (range 10))))

(defn- settled-heap-in-use
  "Runs a full GC first, so native memory freed by collected Java objects is
  not counted."
  ^long []
  (System/gc)
  (heap-in-use))

(deftest large-heads-return-their-native-memory
  (if (nil? mallinfo2)
    (println "Skipping native heap check: mallinfo2 is not available")
    (let [_ (is (heap-follows-allocation?) "mallinfo2 does not follow a 4 MiB allocation")
          {:keys [server tls-port plain-port]} (start-server)]
      (try
        (doseq [[proto rounds request! case-name]
                [[:h1 200 (let [client (jdk-client HttpClient$Version/HTTP_1_1)]
                            #(jdk-heads client plain-port "/fixed/huge"))
                  "huge"]
                 [:h2 200 (let [client (jdk-client HttpClient$Version/HTTP_2)]
                            #(jdk-heads client plain-port "/fixed/huge"))
                  "huge"]
                 [:h3 300 #(curl-heads :h3 tls-port "/fixed/curl-huge") "curl-huge"]]]
          (let [head-bytes (serialization/header-bytes (heads case-name))]
            (dotimes [_ 10] (request!))
            (let [before (settled-heap-in-use)]
              (dotimes [_ rounds] (request!))
              (let [growth (- (settled-heap-in-use) before)]
                ;; One leaked staging copy per response would grow the heap by
                ;; at least rounds * head-bytes.
                (is (< growth (quot (* rounds head-bytes) 2))
                    (pr-str [proto :growth growth :head-bytes head-bytes]))))))
        (finally
          (busker/stop! server))))))
