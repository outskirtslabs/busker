(ns ol.busker.fixed-final-test
  (:require
   [clojure.test :refer [deftest is]]
   [clojure.string :as str]
   [coffi.mem :as mem]
   [ol.busker :as busker]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.evloop :as evloop]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.generation :as generation]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.response :as response]
   [ol.busker.test-utils :as util])
  (:import
   [java.lang.foreign MemorySegment]
   [java.net InetSocketAddress Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicReference]))

(defn- command
  ([body] (command body (alength ^bytes body)))
  ([body body-limit]
   (let [headers [["content-type" "text/plain"]]
         header-bytes (fixed-final/header-staging-bytes headers)]
     (fixed-final/command 1 1 200 headers header-bytes (alength ^bytes body) 2 body-limit body))))

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

(deftest command-contains-only-copied-jvm-data
  (let [body (byte-array [1 2 3])
        command (command body)]
    (aset-byte body 0 (byte 9))
    (is (= {:module-id 1
            :request-seq 1
            :status 200
            :headers [["content-type" "text/plain"]]
            :header-staging-bytes (fixed-final/header-staging-bytes [["content-type" "text/plain"]])
            :content-length 3
            :compress-hint 2}
           (dissoc command :body)))
    (is (= (:module-id command) (fixed-final/command-module-id command)))
    (is (= (:request-seq command) (fixed-final/command-request-seq command)))
    (is (= (:content-length command) (fixed-final/command-content-length command)))
    (is (= (:status command)
           (.-status ^ol.busker.fixed_final.FixedFinalCommand command)))
    (is (= [1 2 3] (vec ^bytes (:body command))))
    (is (not-any? #(instance? MemorySegment %)
                  (concat (:headers command) [(:body command)])))))

(deftest command-admits-body-at-output-buffer-size
  (is (some? (command (byte-array 3) 3)))
  (is (thrown? clojure.lang.ExceptionInfo
               (command (byte-array 4) 3))))

(deftest dispatcher-stages-on-a-platform-thread
  (let [receiver (mem/alloc 1 (mem/global-arena))
        seen_ (promise)
        admission_ (promise)
        command (command (.getBytes "cat" StandardCharsets/UTF_8))]
    (with-redefs [h2o/mt-submit-fixed-final
                  (fn [_ module-id request-seq status headers headers-len
                       content-length compress-hint body body-len]
                    (deliver seen_
                             {:module-id module-id
                              :request-seq request-seq
                              :status status
                              :headers (serialized-headers headers headers-len)
                              :content-length content-length
                              :compress-hint compress-hint
                              :body (vec (mem/read-bytes body (long body-len)))
                              :thread-name (.getName (Thread/currentThread))
                              :virtual? (.isVirtual (Thread/currentThread))})
                    1)]
      (let [dispatcher (fixed-final/start-dispatcher! receiver 3 (constantly :accepted)
                                                      "fixed-final-test")]
        (try
          (let [caller (Thread/startVirtualThread
                        #(deliver admission_
                                  {:result (fixed-final/submit-dispatcher! dispatcher command)
                                   :virtual? (.isVirtual (Thread/currentThread))}))]
            (.join caller 2000)
            (is (= {:admission {:result :accepted :virtual? true}
                    :submission {:module-id 1
                                 :request-seq 1
                                 :status 200
                                 :headers [["content-type" "text/plain"]]
                                 :content-length 3
                                 :compress-hint 2
                                 :body [99 97 116]
                                 :thread-name "fixed-final-test"
                                 :virtual? false}}
                   {:admission (deref admission_ 2000 :timeout)
                    :submission (deref seen_ 2000 :timeout)})))
          (finally
            (fixed-final/stop-dispatcher! dispatcher)))
        (is (fixed-final/dispatcher-stopped? dispatcher))))))

(deftest dispatcher-drains-accepted-commands-in-fifo-order
  (let [receiver (mem/alloc 1 (mem/global-arena))
        request-seqs_ (atom [])]
    (with-redefs [h2o/mt-submit-fixed-final
                  (fn [_ _ request-seq _ _ _ _ _ _ _]
                    (swap! request-seqs_ conj request-seq)
                    1)]
      (let [dispatcher (fixed-final/start-dispatcher! receiver 1 (constantly :accepted)
                                                      "fixed-final-fifo")
            commands (mapv #(assoc (command (byte-array [65])) :request-seq %)
                           (range 1 33))]
        (is (= (repeat 32 :accepted)
               (mapv #(fixed-final/submit-dispatcher! dispatcher %) commands)))
        (fixed-final/stop-dispatcher! dispatcher)
        (is (= {:request-seqs (vec (range 1 33))
                :stopped? true}
               {:request-seqs @request-seqs_
                :stopped? (fixed-final/dispatcher-stopped? dispatcher)}))))))

(deftest dispatcher-reports-overload-at-its-bounded-capacity
  (let [receiver (mem/alloc 1 (mem/global-arena))
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        command (command (byte-array [65]))]
    (with-redefs [h2o/mt-submit-fixed-final
                  (fn [& _]
                    (.countDown entered)
                    (.await release 2 TimeUnit/SECONDS)
                    1)]
      (let [dispatcher (fixed-final/start-dispatcher! receiver 1 (constantly :accepted)
                                                      "fixed-final-bounded")]
        (try
          (is (= :accepted (fixed-final/submit-dispatcher! dispatcher command)))
          (is (.await entered 2 TimeUnit/SECONDS))
          (is (= {:queued (vec (repeat 256 :accepted))
                  :overflow :overloaded}
                 {:queued (mapv (fn [_]
                                  (fixed-final/submit-dispatcher! dispatcher command))
                                (range 256))
                  :overflow (fixed-final/submit-dispatcher! dispatcher command)}))
          (finally
            (.countDown release)
            (fixed-final/stop-dispatcher! dispatcher)))))))

(deftest dispatcher-falls-back-after-native-submission-failure
  (let [receiver (mem/alloc 1 (mem/global-arena))
        command (command (byte-array [65]))
        fallback_ (promise)]
    (with-redefs [h2o/mt-submit-fixed-final (constantly 0)
                  h2o/report-almost-fatal-error (fn [& _])]
      (let [dispatcher (fixed-final/start-dispatcher!
                        receiver 1
                        #(deliver fallback_
                                  {:command %
                                   :thread-name (.getName (Thread/currentThread))
                                   :virtual? (.isVirtual (Thread/currentThread))})
                        "fixed-final-fallback")]
        (try
          (is (= :accepted (fixed-final/submit-dispatcher! dispatcher command)))
          (is (= {:command command
                  :thread-name "fixed-final-fallback"
                  :virtual? false}
                 (deref fallback_ 2000 :timeout)))
          (finally
            (fixed-final/stop-dispatcher! dispatcher)))))))

(deftest worker-reuses-and-explicitly-releases-fixed-final-scratch
  (let [scratch_ (AtomicReference.)
        worker {:fixed-final-scratch_ scratch_}
        req {:worker worker
             :req-ctx-ptr :request-context
             :config {:output-buffer-size 3}}
        sent_ (atom [])
        staging-calls_ (atom 0)
        first-command (command (byte-array [65 66 67]))
        second-command (command (byte-array [88 89]))]
    (.set evloop/worker-context worker)
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
        (.remove evloop/worker-context)))))

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
    (.set evloop/worker-context worker)
    (try
      (with-redefs [h2o/send-fixed-final (fn [& _] (reset! sent?_ true))]
        (is (thrown? clojure.lang.ExceptionInfo
                     (fixed-final/execute! req (command (byte-array 4) 4))))
        (is (false? @sent?_)))
      (finally
        (.remove evloop/worker-context)))))

(deftest stale-command-skips-native-work-and-live-command-runs-on-the-worker
  (with-open [arena (mem/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          worker {:callback-dispatch dispatch}
          req {:worker worker :req-ctx-ptr :request-context}
          module-id (:module-id dispatch)
          command (assoc (command (byte-array [65])) :module-id module-id)
          executed_ (AtomicReference.)]
      (callback-dispatch/bind-thread! dispatch (Thread/currentThread))
      (callback-dispatch/register! dispatch module-id 1 req nil)
      (.set evloop/worker-context worker)
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
          (.remove evloop/worker-context))))))

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
(deftest native-helper-sends-final-body-and-suppresses-head-body
  (let [[server port] (fixed-final-server)
        commit-final! (fn [req write-resp committed_ response _]
                        (let [body (.getBytes "inline" StandardCharsets/UTF_8)
                              headers [["content-type" "text/plain"]]
                              command (fixed-final/command (:dispatch-module-id req)
                                                           (:dispatch-request-seq req)
                                                           (:status response)
                                                           headers
                                                           (fixed-final/header-staging-bytes headers)
                                                           (alength body)
                                                           h2o/H2O_COMPRESS_HINT_ENABLE
                                                           (get-in req [:config :output-buffer-size])
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