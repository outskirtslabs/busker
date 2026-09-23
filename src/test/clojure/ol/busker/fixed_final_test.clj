(ns ol.busker.fixed-final-test
  (:require
   [clojure.test :refer [deftest is]]
   [clojure.string :as str]
   [coffi.mem :as mem]
   [ol.busker :as busker]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.worker-context :as worker-context]
   [ol.busker.generation :as generation]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.response :as response]
   [ol.busker.test-utils :as util])
  (:import
   [java.lang.foreign MemorySegment]
   [java.net InetSocketAddress Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent.atomic AtomicBoolean AtomicReference]
   [java.util.concurrent.locks ReentrantReadWriteLock]))

(defn- prepared-command
  ([^bytes body]
   (prepared-command 1 1 200 [["content-type" "text/plain"]] 2 body))
  ([module-id request-seq status headers compress-hint ^bytes body]
   (let [header-staging-bytes (fixed-final/header-staging-bytes headers)
         body-length (alength body)]
     (fixed-final/prepared-command module-id request-seq status headers
                                   header-staging-bytes body-length compress-hint body))))

(defn- direct-plan
  [module-id request-seq status headers content-length compress-hint body body-length]
  (fixed-final/direct-response-plan (long module-id) (long request-seq)
                                    (long status) headers
                                    (fixed-final/header-utf8-bytes headers) body
                                    (long body-length) (long content-length)
                                    (long compress-hint)))

(defn- direct-response-worker
  [receiver response-slots response-slot-payload-capacity]
  {:response-receiver_             (AtomicReference. receiver)
   :response-receiver-open?_       (AtomicBoolean. true)
   :response-receiver-lock         (ReentrantReadWriteLock.)
   :response-slots                 response-slots
   :response-slot-payload-capacity response-slot-payload-capacity})

(defn- unary-long-fn
  [f]
  (proxy [clojure.lang.AFn clojure.lang.IFn$OL] []
    (invokePrim [value] (long (f value)))
    (invoke [value] (f value))))

(defn- receiver-handle->long-fn
  [f]
  (proxy [clojure.lang.AFn clojure.lang.IFn$OLL] []
    (invokePrim [receiver claim-handle] (long (f receiver claim-handle)))
    (invoke [receiver claim-handle] (f receiver claim-handle))))

(defn- serialized-headers
  [headers headers-len]
  (let [header-size (mem/size-of ::h2o/clj-header-t)]
    (mapv
     (fn [idx]
       (let [{:keys [name name_len value value_len]}
             (mem/deserialize (mem/slice headers (* idx header-size) header-size)
                              ::h2o/clj-header-t)]
         [(h2o/->string name name_len)
          (h2o/->string value value_len)]))
     (range headers-len))))

(deftest response-slot-contains-packed-metadata-headers-and-body
  (with-open [arena (mem/confined-arena)]
    (let [body (.getBytes "cat" StandardCharsets/UTF_8)
          headers [["content-type" "text/plain"]]
          data-size (mem/size-of ::h2o/clj-fixed-response-slot-data-t)
          slot (mem/alloc (+ data-size 65) arena)
          _ (fixed-final/write-direct-response-slot!
             slot 64 (direct-plan 1 1 200 headers 3 2 body 3))
          data (mem/deserialize (mem/slice slot 0 data-size)
                                ::h2o/clj-fixed-response-slot-data-t)
          staged-headers (mapv (fn [{:keys [name name_len value value_len]}]
                                 [(h2o/->string name name_len)
                                  (h2o/->string value value_len)])
                               (take (:headers-len data) (:headers data)))
          payload (mem/read-bytes (mem/slice slot data-size 64)
                                  (long (:payload-len data)))]
      (is (= {:module-id 1
              :claim-token 0
              :request-seq 1
              :headers-len 1
              :content-length 3
              :body-offset 22
              :body-len 3
              :payload-len 25
              :status 200
              :compress-hint 2
              :headers [["content-type" "text/plain"]]
              :payload "content-typetext/plaincat"}
             (assoc (dissoc data :headers)
                    :headers staged-headers
                    :payload (String. payload StandardCharsets/UTF_8)))))))

(deftest direct-response-slot-enforces-exact-header-and-payload-limits
  (with-open [arena (mem/confined-arena)]
    (let [headers (mapv (fn [index] [(str "x" index) "v"]) (range fixed-final/max-header-pairs))
          body (byte-array [1 2 3])
          header-bytes (fixed-final/header-utf8-bytes headers)
          payload-capacity (+ header-bytes (alength body))
          data-size (mem/size-of ::h2o/clj-fixed-response-slot-data-t)
          slot (mem/alloc (+ data-size (inc payload-capacity)) arena)
          plan (direct-plan 1 1 200 headers (alength body) 0 body (alength body))]
      (fixed-final/write-direct-response-slot! slot payload-capacity plan)
      (let [data (mem/deserialize (mem/slice slot 0 data-size)
                                  ::h2o/clj-fixed-response-slot-data-t)
            payload-start (.address ^MemorySegment (mem/slice slot data-size (inc payload-capacity)))
            payload-end (+ payload-start payload-capacity)
            descriptors (take (:headers-len data) (:headers data))]
        (is (= fixed-final/max-header-pairs (:headers-len data)))
        (is (= payload-capacity (:payload-len data)))
        (is (every? (fn [{:keys [name name_len value value_len]}]
                      (and (<= payload-start (.address ^MemorySegment name) payload-end)
                           (<= name_len (- payload-end (.address ^MemorySegment name)))
                           (<= payload-start (.address ^MemorySegment value) payload-end)
                           (<= value_len (- payload-end (.address ^MemorySegment value)))))
                    descriptors)))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"does not fit"
                            (fixed-final/write-direct-response-slot!
                             slot payload-capacity
                             (direct-plan 1 1 200 (conj headers ["overflow" "v"])
                                          (alength body) 0 body (alength body))))))))
(deftest direct-response-slot-utf8-matches-java-encoding
  (doseq [body ["ascii" "λ" "🐈" (str (char 0xd800) "x") (str "x" (char 0xdc00))]]
    (with-open [arena (mem/confined-arena)]
      (let [expected (.getBytes ^String body StandardCharsets/UTF_8)
            headers [["x-🐈" "välue"]]
            header-bytes (fixed-final/header-utf8-bytes headers)
            data-size (mem/size-of ::h2o/clj-fixed-response-slot-data-t)
            slot (mem/alloc (+ data-size 129) arena)]
        (fixed-final/write-direct-response-slot!
         slot 128 (direct-plan 7 9 201 headers (alength expected) 1 body
                               (alength expected)))
        (let [data (mem/deserialize (mem/slice slot 0 data-size)
                                    ::h2o/clj-fixed-response-slot-data-t)
              staged-headers (mapv (fn [{:keys [name name_len value value_len]}]
                                     [(h2o/->string name name_len)
                                      (h2o/->string value value_len)])
                                   (take (:headers-len data) (:headers data)))
              payload (mem/read-bytes (mem/slice slot data-size 128)
                                      (long (:payload-len data)))]
          (is (= {:module-id 7
                  :request-seq 9
                  :headers-len 1
                  :content-length (alength expected)
                  :body-offset header-bytes
                  :payload-len (+ header-bytes (alength expected))
                  :status 201
                  :compress-hint 1}
                 (select-keys data [:module-id :request-seq :headers-len
                                    :content-length :body-offset :payload-len
                                    :status :compress-hint])))
          (is (= headers staged-headers))
          (is (= (alength expected) (:body-len data)))
          (is (= (vec expected)
                 (vec (drop header-bytes payload)))))))))
(deftest direct-string-body-uses-hidden-terminator-byte-at-logical-capacity
  (with-open [arena (mem/confined-arena)]
    (let [data-size (mem/size-of ::h2o/clj-fixed-response-slot-data-t)
          slot (mem/alloc (+ data-size 4) arena)]
      (fixed-final/write-direct-response-slot!
       slot 3 (direct-plan 1 1 200 [] 3 0 "cat" 3))
      (is (= [99 97 116 0]
             (vec (mem/read-bytes (mem/slice slot data-size 4) 4)))))))

(deftest full-response-ring-does-not-pack-direct-response
  (with-open [arena (mem/confined-arena)]
    (let [receiver (mem/alloc 1 arena)
          worker (direct-response-worker receiver [] 64)
          writes_ (atom 0)]
      (with-redefs [h2o/mt-response-try-claim (unary-long-fn (constantly 0))
                    fixed-final/write-direct-response-slot!
                    (fn [& _] (swap! writes_ inc))]
        (is (= :overloaded
               (fixed-final/try-publish-direct-response!
                worker (direct-plan 1 1 200 [] 0 0 (byte-array 0) 0))))
        (is (zero? @writes_))))))

(deftest direct-response-slot-is-aborted-when-packing-fails
  (with-open [arena (mem/confined-arena)]
    (let [receiver (mem/alloc 1 arena)
          slot (mem/alloc (mem/size-of ::h2o/clj-fixed-response-slot-data-t) arena)
          claim-handle 65537
          worker (direct-response-worker receiver [slot] 0)
          aborted_ (atom [])]
      (with-redefs [h2o/mt-response-try-claim (unary-long-fn (constantly claim-handle))
                    h2o/mt-response-publish (receiver-handle->long-fn (constantly 1))
                    h2o/mt-response-abort
                    (receiver-handle->long-fn
                     (fn [actual-receiver actual-handle]
                       (swap! aborted_ conj [actual-receiver actual-handle])
                       1))]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"does not fit"
                              (fixed-final/try-publish-direct-response!
                               worker (direct-plan 1 1 200 [] 1 0
                                                   (byte-array [1]) 1))))
        (is (= [[receiver claim-handle]] @aborted_))))))
(deftest direct-response-slot-is-aborted-when-publication-fails
  (with-open [arena (mem/confined-arena)]
    (let [receiver (mem/alloc 1 arena)
          slot (mem/alloc (inc (mem/size-of ::h2o/clj-fixed-response-slot-data-t)) arena)
          claim-handle 65537
          worker (direct-response-worker receiver [slot] 0)
          aborted_ (atom [])]
      (with-redefs [h2o/mt-response-try-claim
                    (unary-long-fn (constantly claim-handle))
                    h2o/mt-response-publish
                    (receiver-handle->long-fn (constantly 0))
                    h2o/mt-response-abort
                    (receiver-handle->long-fn
                     (fn [actual-receiver actual-handle]
                       (swap! aborted_ conj [actual-receiver actual-handle])
                       1))]
        (is (= :overloaded
               (fixed-final/try-publish-direct-response!
                worker (direct-plan 1 1 200 [] 0 0 (byte-array 0) 0))))
        (is (= [[receiver claim-handle]] @aborted_))))))

(deftest worker-reuses-and-explicitly-releases-fixed-final-scratch
  (let [scratch_ (AtomicReference.)
        worker {:fixed-final-scratch_ scratch_}
        req {:worker worker
             :req-ctx-ptr :request-context
             :config {:output-buffer-size 3}}
        sent_ (atom [])
        staging-calls_ (atom 0)
        first-command (prepared-command (byte-array [65 66 67]))
        second-command (prepared-command (byte-array [88 89]))]
    (.set worker-context/worker-context worker)
    (try
      (is (nil? (.get scratch_)))
      (with-redefs [fixed-final/header-staging-bytes (fn [_]
                                                       (swap! staging-calls_ inc)
                                                       0)
                    h2o/send-fixed-final
                    (fn [_ status headers headers-len content-length compress-hint body body-len]
                      (swap! sent_ conj {:status status
                                         :headers (serialized-headers headers headers-len)
                                         :content-length content-length
                                         :compress-hint compress-hint
                                         :body (vec (mem/read-bytes body (long body-len)))
                                         :body-segment body}))]
        (fixed-final/execute! req first-command)
        (fixed-final/execute! req second-command))
      (is (zero? @staging-calls_))
      (is (= [{:status 200
               :headers [["content-type" "text/plain"]]
               :content-length 3
               :compress-hint 2
               :body [65 66 67]}
              {:status 200
               :headers [["content-type" "text/plain"]]
               :content-length 2
               :compress-hint 2
               :body [88 89]}]
             (mapv #(dissoc % :body-segment) @sent_)))
      (is (= (mapv #(.address ^MemorySegment (:body-segment %)) @sent_)
             (repeat 2 (.address ^MemorySegment (:body-segment (first @sent_))))))
      (is (= [88 89]
             (vec (mem/read-bytes (:body-segment (first @sent_)) 2))))
      (is (= (+ fixed-final/max-header-staging-bytes 3)
             (.-capacity ^ol.busker.fixed_final.FixedFinalScratch (.get scratch_))))
      (fixed-final/close-worker-scratch! worker)
      (is (nil? (.get scratch_)))
      (is (thrown? IllegalStateException
                   (mem/read-bytes (:body-segment (first @sent_)) 2)))
      (finally
        (fixed-final/close-worker-scratch! worker)
        (.remove worker-context/worker-context)))))

(deftest workers-use-separate-fixed-final-scratch
  (let [first-worker {:fixed-final-scratch_ (AtomicReference.)}
        second-worker {:fixed-final-scratch_ (AtomicReference.)}]
    (try
      (let [first-segment (#'fixed-final/worker-scratch-segment first-worker 8)
            second-segment (#'fixed-final/worker-scratch-segment second-worker 8)]
        (is (not= (.address ^MemorySegment first-segment)
                  (.address ^MemorySegment second-segment))))
      (finally
        (fixed-final/close-worker-scratch! first-worker)
        (fixed-final/close-worker-scratch! second-worker)))))

(deftest worker-reasserts-output-buffer-size-before-native-send
  (let [worker (Object.)
        req {:worker worker
             :req-ctx-ptr :request-context
             :config {:output-buffer-size 3}}
        sent?_ (atom false)]
    (.set worker-context/worker-context worker)
    (try
      (with-redefs [h2o/send-fixed-final (fn [& _] (reset! sent?_ true))]
        (is (thrown? clojure.lang.ExceptionInfo
                     (fixed-final/execute! req (prepared-command (byte-array 4)))))
        (is (false? @sent?_)))
      (finally
        (.remove worker-context/worker-context)))))

(deftest stale-command-skips-native-work-and-live-command-runs-on-the-worker
  (with-open [arena (mem/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          worker {:callback-dispatch dispatch}
          req {:worker worker :req-ctx-ptr :request-context}
          module-id (:module-id dispatch)
          command (assoc (prepared-command (byte-array [65])) :module-id module-id)
          executed_ (AtomicReference.)]
      (callback-dispatch/bind-thread! dispatch (Thread/currentThread))
      (callback-dispatch/register! dispatch module-id 1 req nil)
      (.set worker-context/worker-context worker)
      (try
        (with-redefs [fixed-final/execute!
                      (fn [actual-req actual-command]
                        (.set executed_ {:request-context (:req-ctx-ptr actual-req)
                                         :command actual-command
                                         :thread (Thread/currentThread)}))]
          (generation/evloop-msg-processor :h2o/send-fixed-final
                                           [(assoc command :module-id (inc module-id))])
          (is (nil? (.get executed_)))
          (generation/evloop-msg-processor :h2o/send-fixed-final
                                           [command])
          (is (= {:request-context :request-context
                  :command command
                  :thread (Thread/currentThread)}
                 (.get executed_))))
        (finally
          (.remove worker-context/worker-context))))))

(defn- fixed-final-server
  []
  (let [port (util/free-port)
        server (busker/start!
                (util/with-handler
                  (constantly {:status 200 :body "generic"})
                  {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                        :http3? false
                                        :tls false}}}))]
    (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2)
    [server port]))

(defn- request-text
  [port method]
  (with-open [socket (Socket.)]
    (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 2000)
    (let [out (.getOutputStream socket)
          request (str method " / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")]
      (.write out (.getBytes request StandardCharsets/US_ASCII))
      (.flush out)
      (slurp (.getInputStream socket)))))

(deftest fixed-final-response-skips-clojure-message-dispatch-test
  (let [ops_ (atom [])
        process-message generation/evloop-msg-processor]
    (with-redefs-fn
      {#'generation/evloop-msg-processor
       (fn [op args]
         (swap! ops_ conj op)
         (process-message op args))}
      #(let [[server port] (fixed-final-server)]
         (try
           (is (.contains ^String (request-text port "GET") "generic"))
           (finally
             (busker/stop! server)))))
    (is (zero? (count (filter #{:h2o/send-fixed-final} @ops_))))))

(deftest response-ring-applies-live-response-from-a-virtual-thread
  (let [[server port] (fixed-final-server)
        claim-threads_ (atom [])
        wake-threads_ (atom [])
        drain-threads_ (atom [])
        try-claim h2o/mt-response-try-claim
        wake h2o/mt-wakeup
        drain h2o/mt-response-ring-drain]
    (try
      (with-redefs-fn
        {#'h2o/mt-response-try-claim
         (fn [receiver]
           (swap! claim-threads_ conj (Thread/currentThread))
           (try-claim receiver))
         #'h2o/mt-wakeup
         (fn [receiver]
           (swap! wake-threads_ conj (Thread/currentThread))
           (wake receiver))
         #'h2o/mt-response-ring-drain
         (fn [receiver]
           (swap! drain-threads_ conj (Thread/currentThread))
           (drain receiver))}
        #(let [http-response (request-text port "GET")]
           (is (.contains ^String http-response "generic"))))
      (is (seq @claim-threads_))
      (is (every? #(.isVirtual ^Thread %) @claim-threads_))
      (is (seq @wake-threads_))
      (is (every? (fn [^Thread thread] (not (.isVirtual thread)))
                  @wake-threads_))
      (is (seq @drain-threads_))
      (is (every? (fn [^Thread thread] (not (.isVirtual thread)))
                  @drain-threads_))
      (finally
        (busker/stop! server)))))
(deftest native-helper-sends-final-body-and-suppresses-head-body
  (let [[server port] (fixed-final-server)
        commit-final! (fn [req write-resp committed_ response _]
                        (let [body (.getBytes "inline" StandardCharsets/UTF_8)
                              headers [["content-type" "text/plain"]]
                              command (prepared-command (:dispatch-module-id req)
                                                        (:dispatch-request-seq req)
                                                        (:status response)
                                                        headers
                                                        h2o/H2O_COMPRESS_HINT_ENABLE
                                                        body)
                              head (dissoc response :body)]
                          (when (compare-and-set! committed_ nil head)
                            (when-let [writer (response/writer-if-created write-resp)]
                              (pi/stop writer))
                            (pi/send-msg (:worker req) [:h2o/send-fixed-final command])
                            {:head head :body nil})))]
    (try
      (with-redefs-fn {#'response/commit-final! commit-final!}
        #(let [get-response (request-text port "GET")
               head-response (request-text port "HEAD")]
           (is (.contains get-response "inline"))
           (is (not (.contains (second (str/split get-response #"\r\n\r\n" 2)) "generic")))
           (is (not (.contains (second (str/split head-response #"\r\n\r\n" 2)) "inline")))))
      (finally
        (busker/stop! server)))))