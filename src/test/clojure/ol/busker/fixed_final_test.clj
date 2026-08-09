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
    (is (= [1 2 3] (vec ^bytes (:body command))))
    (is (not-any? #(instance? MemorySegment %)
                  (concat (:headers command) [(:body command)])))))

(deftest command-admits-body-at-output-buffer-size
  (is (some? (command (byte-array 3) 3)))
  (is (thrown? clojure.lang.ExceptionInfo
               (command (byte-array 4) 3))))
(deftest worker-stages-and-releases-fixed-final-source
  (let [worker (Object.)
        req {:worker worker
             :req-ctx-ptr :request-context
             :config {:output-buffer-size 3}}
        sent_ (atom nil)
        command (command (byte-array [65 66 67]))]
    (.set evloop/worker-context worker)
    (try
      (with-redefs [h2o/send-fixed-final
                    (fn [_ status headers headers-len content-length compress-hint body body-len]
                      (reset! sent_ {:status status
                                     :headers (serialized-headers headers headers-len)
                                     :content-length content-length
                                     :compress-hint compress-hint
                                     :body (vec (mem/read-bytes body body-len))
                                     :body-segment body}))]
        (fixed-final/execute! req command))
      (is (= {:status 200
              :headers [["content-type" "text/plain"]]
              :content-length 3
              :compress-hint 2
              :body [65 66 67]}
             (dissoc @sent_ :body-segment)))
      (is (thrown? IllegalStateException
                   (mem/read-bytes (:body-segment @sent_) 3)))
      (finally
        (.remove evloop/worker-context)))))

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
                            (pi/stop write-resp)
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