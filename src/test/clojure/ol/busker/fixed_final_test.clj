(ns ol.busker.fixed-final-test
  (:require
   [clojure.test :refer [deftest is]]
   [clojure.string :as str]
   [babashka.ffi :as mem]
   [ol.busker :as busker]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.test-utils :as util])
  (:import
   [java.lang.foreign MemorySegment]
   [java.net InetSocketAddress Socket]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicReference]
   [java.util.concurrent.locks ReentrantReadWriteLock]))

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
   :response-slot-monitor          (Object.)
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

(deftest response-slot-contains-packed-metadata-headers-and-body
  (with-open [arena (mem/confined-arena)]
    (let [body (.getBytes "cat" StandardCharsets/UTF_8)
          headers [["content-type" "text/plain"]]
          data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
          slot (mem/alloc arena (+ data-size 65))
          _ (fixed-final/write-direct-response-slot!
             slot 64 (direct-plan 1 1 200 headers 3 2 body 3))
          data (mem/read (mem/slice slot 0 data-size)
                         h2o/ffi-clj-fixed-response-slot-data-t)
          staged-headers (mapv (fn [{:keys [name name_len value value_len]}]
                                 [(h2o/->string name name_len)
                                  (h2o/->string value value_len)])
                               (take (:headers-len data) (:headers data)))
          payload (mem/read-array (mem/slice slot data-size 64)
                                  :byte (long (:payload-len data)))]
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
          data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
          slot (mem/alloc arena (+ data-size (inc payload-capacity)))
          plan (direct-plan 1 1 200 headers (alength body) 0 body (alength body))]
      (fixed-final/write-direct-response-slot! slot payload-capacity plan)
      (let [data (mem/read (mem/slice slot 0 data-size)
                           h2o/ffi-clj-fixed-response-slot-data-t)
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
            data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
            slot (mem/alloc arena (+ data-size 129))]
        (fixed-final/write-direct-response-slot!
         slot 128 (direct-plan 7 9 201 headers (alength expected) 1 body
                               (alength expected)))
        (let [data (mem/read (mem/slice slot 0 data-size)
                             h2o/ffi-clj-fixed-response-slot-data-t)
              staged-headers (mapv (fn [{:keys [name name_len value value_len]}]
                                     [(h2o/->string name name_len)
                                      (h2o/->string value value_len)])
                                   (take (:headers-len data) (:headers data)))
              payload (mem/read-array (mem/slice slot data-size 128)
                                      :byte (long (:payload-len data)))]
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
    (let [data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
          slot (mem/alloc arena (+ data-size 4))]
      (fixed-final/write-direct-response-slot!
       slot 3 (direct-plan 1 1 200 [] 3 0 "cat" 3))
      (is (= [99 97 116 0]
             (vec (mem/read-array (mem/slice slot data-size 4) :byte 4)))))))

(deftest full-response-ring-does-not-pack-direct-response
  (with-open [arena (mem/confined-arena)]
    (let [receiver (mem/alloc arena 1)
          worker (direct-response-worker receiver [] 64)
          writes_ (atom 0)]
      (with-redefs [h2o/mt-response-use-acquire (constantly 1)
                    h2o/mt-response-use-release (fn [_])
                    h2o/mt-response-try-claim (unary-long-fn (constantly 0))
                    fixed-final/write-direct-response-slot!
                    (fn [& _] (swap! writes_ inc))]
        (is (= :overloaded
               (fixed-final/publish-direct-producer!
                worker (direct-plan 1 1 200 [] -1 0 nil 0) 1
                (fn [_] (swap! writes_ inc))
                false (constantly false) (fn [publish!] (publish!)))))
        (is (zero? @writes_))))))

(deftest direct-response-slot-is-aborted-when-packing-fails
  (with-open [arena (mem/confined-arena)]
    (let [receiver (mem/alloc arena 1)
          slot (mem/alloc arena h2o/ffi-clj-fixed-response-slot-data-t)
          claim-handle 65537
          worker (direct-response-worker receiver [slot] 0)
          aborted_ (atom [])]
      (with-redefs [h2o/mt-response-use-acquire (constantly 1)
                    h2o/mt-response-use-release (fn [_])
                    h2o/mt-response-try-claim (unary-long-fn (constantly claim-handle))
                    h2o/mt-response-publish (receiver-handle->long-fn (constantly 1))
                    h2o/mt-response-abort
                    (receiver-handle->long-fn
                     (fn [actual-receiver actual-handle]
                       (swap! aborted_ conj [actual-receiver actual-handle])
                       1))]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"does not fit"
                              (fixed-final/publish-direct-response!
                               worker (direct-plan 1 1 200 [] 1 0
                                                   (byte-array [1]) 1))))
        (is (= [[receiver claim-handle]] @aborted_))))))
(deftest direct-response-slot-is-aborted-when-publication-fails
  (with-open [arena (mem/confined-arena)]
    (let [receiver (mem/alloc arena 1)
          slot (mem/alloc arena (inc (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)))
          claim-handle 65537
          worker (direct-response-worker receiver [slot] 0)
          aborted_ (atom [])]
      (with-redefs [h2o/mt-response-use-acquire (constantly 1)
                    h2o/mt-response-use-release (fn [_])
                    h2o/mt-response-try-claim
                    (unary-long-fn (constantly claim-handle))
                    h2o/mt-response-publish
                    (receiver-handle->long-fn (constantly 0))
                    h2o/mt-response-abort
                    (receiver-handle->long-fn
                     (fn [actual-receiver actual-handle]
                       (swap! aborted_ conj [actual-receiver actual-handle])
                       1))]
        (is (thrown-with-msg? IllegalStateException
                              #"publication failed"
                              (fixed-final/publish-direct-response!
                               worker (direct-plan 1 1 200 [] 0 0 (byte-array 0) 0))))
        (is (= [[receiver claim-handle]] @aborted_))))))

(deftest producer-view-expires-without-invalidating-slot-storage
  (with-open [arena (mem/shared-arena)]
    (let [size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
          slot (mem/alloc arena (+ size 65))
          aliases_ (atom [])
          foreign-result (promise)
          caller_ (atom nil)
          result (promise)
          producer (fn [^ByteBuffer buffer]
                     (reset! caller_ (Thread/currentThread))
                     (reset! aliases_ [buffer (.slice buffer) (.duplicate buffer)])
                     (let [thread (Thread/startVirtualThread
                                   #(deliver foreign-result
                                             (try (.put buffer 0 (byte 99)) :wrote
                                                  (catch java.lang.WrongThreadException _ :rejected))))]
                       (.join thread))
                     (.put buffer (byte-array [99 97 116])))
          thread (Thread/startVirtualThread
                  #(deliver result
                            (try
                              (#'fixed-final/write-direct-producer-slot!
                               slot 64 (direct-plan 1 1 200 [] -1 0 nil 0) 32 producer)
                              {:status :prepared
                               :aliases (mapv (fn [^ByteBuffer buffer]
                                                (try (.get buffer 0) :readable
                                                     (catch IllegalStateException _ :expired)))
                                              @aliases_)}
                              (catch Throwable error error))))]
      (.join thread 2000)
      (is (= {:status :prepared :aliases [:expired :expired :expired]}
             (deref result 1000 :timeout)))
      (is (= :rejected (deref foreign-result 1000 :timeout)))
      (is (identical? thread @caller_))
      (is (= {:body-len 3 :payload-len 3 :content-length 3}
             (select-keys (mem/read slot h2o/ffi-clj-fixed-response-slot-data-t)
                          [:body-len :payload-len :content-length])))
      (is (= [99 97 116] (vec (mem/read-array (mem/slice slot size 3) :byte 3)))))))

(deftest producer-exception-closes-alias-and-preserves-explicit-content-length
  (with-open [arena (mem/shared-arena)]
    (let [size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
          slot (mem/alloc arena (+ size 65))
          alias_ (atom nil)
          plan (direct-plan 1 1 200 [] 7 0 nil 0)]
      (is (thrown-with-msg? Exception #"producer failed"
                            (#'fixed-final/write-direct-producer-slot!
                             slot 64 plan 16
                             (fn [buffer]
                               (reset! alias_ buffer)
                               (throw (Exception. "producer failed"))))))
      (is (thrown? IllegalStateException (.get ^ByteBuffer @alias_ 0)))
      (#'fixed-final/write-direct-producer-slot!
       slot 64 plan 16 (fn [^ByteBuffer buffer] (.put buffer (byte 1))))
      (is (= {:body-len 1 :content-length 7}
             (select-keys (mem/read slot h2o/ffi-clj-fixed-response-slot-data-t)
                          [:body-len :content-length]))))))

(deftest accepted-fixed-publication-survives-wake-failure
  (let [worker {:response-slot-monitor (Object.)
                :response-receiver-open?_ (AtomicBoolean. true)
                :response-slots [:slot]
                :response-slot-payload-capacity 64}
        released?_ (atom false)]
    (with-redefs [h2o/mt-response-publish (receiver-handle->long-fn (constantly 1))
                  h2o/mt-response-abort (fn [& _] (throw (AssertionError. "published slot aborted")))
                  h2o/mt-response-use-release (fn [_] (reset! released?_ true))
                  pi/wake (fn [_] (throw (Exception. "wake failed")))]
      (is (= :accepted
             (#'fixed-final/prepare-and-publish!
              worker [:receiver 65537] (constantly false) (fn [& _])
              (fn [publish!] (publish!))))))
    (is (true? @released?_))))

(deftest fixed-producer-rejection-skips-callback
  (doseq [rejection [:closed :overloaded]]
    (with-redefs-fn
      {#'fixed-final/claim-response! (fn [& _] rejection)}
      #(is (= rejection
              (fixed-final/publish-direct-producer!
               {:response-slot-payload-capacity 64}
               (direct-plan 1 1 200 [] -1 0 nil 0) 16
               (fn [_] (throw (AssertionError. "rejected producer invoked")))
               false (constantly false)
               (fn [_] (throw (AssertionError. "rejected response accepted")))))))))

(deftest fixed-capacity-wait-rechecks-release-and-cancellation
  (doseq [cancel? [false true]]
    (let [worker {:response-slot-monitor (Object.)}
          checked (CountDownLatch. 1)
          available?_ (atom false)
          cancelled?_ (atom false)
          result (promise)]
      (with-redefs-fn
        {#'fixed-final/claim-once!
         (fn [_]
           (if @available?_
             [:receiver 65537]
             (do (.countDown checked) :overloaded)))}
        (fn []
          (let [thread (Thread/startVirtualThread
                        #(deliver result
                                  (#'fixed-final/claim-response!
                                   worker true (fn [] @cancelled?_))))]
            (try
              (is (.await checked 2 TimeUnit/SECONDS))
              (if cancel? (reset! cancelled?_ true) (reset! available?_ true))
              (fixed-final/notify-capacity! worker)
              (is (= (if cancel? :closed [:receiver 65537])
                     (deref result 2000 :timeout)))
              (finally
                (.interrupt thread)
                (.join thread 2000)))))))))

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
      (is (every? #(.isVirtual ^Thread %) @wake-threads_))
      (is (seq @drain-threads_))
      (is (every? (fn [^Thread thread] (not (.isVirtual thread)))
                  @drain-threads_))
      (finally
        (busker/stop! server)))))
(deftest retained-fixed-response-suppresses-head-body
  (let [[server port] (fixed-final-server)]
    (try
      (is (= "generic" (second (str/split (request-text port "GET") #"\r\n\r\n" 2))))
      (is (= "" (second (str/split (request-text port "HEAD") #"\r\n\r\n" 2))))
      (finally
        (busker/stop! server)))))