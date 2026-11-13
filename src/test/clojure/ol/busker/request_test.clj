(ns ol.busker.request-test
  (:require
   [babashka.ffi :as ffi]
   [clojure.edn :as edn]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.request :as request]
   [ol.busker.request-dispatch :as request-dispatch]
   [ol.busker.response-queue :as queue]
   [ol.busker.test-utils :as util]
   [ol.busker.worker-context :as worker-context])
  (:import
   [java.net Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent AbstractExecutorService]
   [java.util.concurrent.atomic AtomicReference]))

(deftest request-construction-preserves-context-and-worker-services
  (doseq [method [:post nil]]
    (with-open [arena (ffi/shared-arena)]
      (let [dispatch (callback-dispatch/create arena)
            _ (callback-dispatch/bind-thread! dispatch (Thread/currentThread))
            pool (Object.)
            worker {:callback-dispatch dispatch
                    :stream-pool pool
                    :response-receiver_ (AtomicReference. ffi/null)
                    :reserve-command! vector
                    :publish-command! vector
                    :release-command! vector
                    :wake-command-waiters! vector
                    :on-native-failure! vector}
            config {:output-buffer-size 64
                    :entrypoints {:http {}}}
            ctx {:has-body 0 :request-method method :entrypoint 0}
            installed_ (atom nil)
            tasks_ (atom [])
            executor (proxy [AbstractExecutorService] []
                       (execute [task] (swap! tasks_ conj task)))]
        (with-redefs [pi/running? (constantly true)
                      worker-context/get-current-worker (constantly worker)
                      h2o/install-request-dispatch
                      (fn [_ _ module-id request-seq _]
                        (reset! installed_ [module-id request-seq])
                        1)]
          (is (= h2o/CLJ_HANDLER_OK
                 (request/on-request executor identity config identity ffi/null ctx)))
          (let [[module-id request-seq] @installed_
                [req _] (callback-dispatch/entry dispatch module-id request-seq)]
            (is (= {:worker worker
                    :config config
                    :req-ctx-ptr ffi/null :req-ctx ctx :write-req nil
                    :callback-pointers (callback-dispatch/callback-pointers dispatch)
                    :dispatch-module-id module-id :dispatch-request-seq request-seq}
                   (into {} req)))
            (is (identical? worker (:worker req)))
            (is (identical? config (:config req)))
            (is (identical? pool (:buffer-pool (queue/new-response-state req))))
            (is (nil? (meta req)))
            (is (= 1 (count @tasks_)))))))))

(deftest reuse-mode-dispatches-requests-by-connection-id
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          _ (callback-dispatch/bind-thread! dispatch (Thread/currentThread))
          shutdown?_ (atom false)
          tasks_ (atom [])
          executor (proxy [AbstractExecutorService] []
                     (execute [task] (swap! tasks_ conj task))
                     (isShutdown [] @shutdown?_))
          worker {:callback-dispatch dispatch
                  :request-dispatch (request-dispatch/create executor)
                  :response-receiver_ (AtomicReference. ffi/null)}
          config {:entrypoints {:http {}}}
          context (fn [id]
                    (doto (ffi/alloc arena h2o/ffi-clj-req-ctx-t)
                      (ffi/write (ffi/place h2o/ffi-clj-req-ctx-t [:meta :connection_id]) id)))
          on-request (fn [ctx-ptr]
                       (request/on-request executor identity config identity ctx-ptr
                                           {:has-body 0 :entrypoint 0}))]
      (with-redefs [pi/running? (constantly true)
                    worker-context/get-current-worker (constantly worker)
                    h2o/install-request-dispatch (constantly 1)]
        (let [first-connection (context 41)]
          (is (= [h2o/CLJ_HANDLER_OK h2o/CLJ_HANDLER_OK h2o/CLJ_HANDLER_OK]
                 [(on-request first-connection) (on-request first-connection)
                  (on-request (context 42))]))
          (is (= 2 (count @tasks_)) "one drainer per connection, the second request queued")
          (reset! shutdown?_ true)
          (is (= h2o/CLJ_HANDLER_SHUTTING_DOWN (on-request first-connection))))))))

(deftest request-without-a-known-entrypoint-is-not-dispatched
  (doseq [index [h2o/CLJ_ENTRYPOINT_UNKNOWN 2 nil]]
    (let [executed_ (atom 0)
          installed_ (atom 0)
          executor (proxy [AbstractExecutorService] []
                     (execute [_] (swap! executed_ inc)))]
      (with-redefs [pi/running? (constantly true)
                    worker-context/get-current-worker (constantly {})
                    h2o/install-request-dispatch (fn [& _] (swap! installed_ inc) 1)]
        (is (= h2o/CLJ_HANDLER_ERROR
               (binding [*err* (java.io.StringWriter.)]
                 (request/on-request executor identity
                                     {:entrypoints {:http {} :admin {}}}
                                     identity ffi/null
                                     {:has-body 0 :entrypoint index})))
            (str "index " index))
        (is (= [0 0] [@executed_ @installed_]))))))

(deftest closed-required-proceed-fails-request-body-read
  (let [calls_ (atom [])]
    (with-redefs [pi/send-required-msg
                  (fn [worker message]
                    (swap! calls_ conj [worker message])
                    :closed)]
      (let [{:keys [input-stream write-chunk]}
            (request/set-req-body-channel :worker 7 11)]
        (write-chunk (byte-array [(byte 65)]) false)
        (is (thrown-with-msg? java.io.IOException #"Request worker closed"
                              (.read input-stream)))
        (is (= [[:worker [:h2o/proceed-request 7 11]]] @calls_))))))
(deftest write-request-channel-final-chunk-test
  (testing "a final chunk and EOF can be delivered before the handler reads"
    (let [{:keys [input-stream write-chunk]}
          (request/create-write-req-channel (fn [] nil))
          write-result (future
                         (write-chunk (.getBytes "abc" StandardCharsets/UTF_8) true)
                         :done)]
      (is (= :done (deref write-result 1000 :blocked)))
      (let [buf (byte-array 8)
            n (.read input-stream buf)]
        (is (= 3 n))
        (is (= "abc" (String. buf 0 n StandardCharsets/UTF_8))))
      (is (= -1 (.read input-stream))))))

(deftest write-request-channel-empty-final-chunk-test
  (testing "empty final chunks do not occupy the body queue"
    (let [{:keys [input-stream write-chunk]}
          (request/create-write-req-channel (fn [] nil))
          write-result (future
                         (write-chunk (byte-array 0) true)
                         :done)]
      (is (= :done (deref write-result 1000 :blocked)))
      (is (= -1 (.read input-stream))))))

(defn- raw-request-headers
  "Sends a GET with the extra `header-lines` to `port` and returns the response body."
  [port header-lines]
  (with-open [socket (Socket. "127.0.0.1" (int port))]
    (let [out (.getOutputStream socket)]
      (.write out (.getBytes (str "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                                  (apply str (map #(str % "\r\n") header-lines))
                                  "Connection: close\r\n\r\n")
                             StandardCharsets/ISO_8859_1))
      (.flush out)
      (let [response (String. (.readAllBytes (.getInputStream socket)) StandardCharsets/UTF_8)]
        (subs response (+ 4 (str/index-of response "\r\n\r\n")))))))

(deftest empty-request-header-values-reach-the-handler-as-strings-test
  (let [port (util/free-port)
        server (busker/start!
                (util/with-handler
                  (fn [request]
                    {:status 200
                     :headers {"content-type" "text/plain"}
                     :body (pr-str (select-keys (:headers request) ["x-empty" "cookie"]))})
                  {:n-workers 1
                   :entrypoints {:http {:bind (str "127.0.0.1:" port)
                                        :http3? false :tls false}}}))]
    (try
      (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2)
      (doseq [[header-lines expected]
              [[["X-Empty:"] {"x-empty" ""}]
               [["X-Empty:    "] {"x-empty" ""}]
               [["X-Empty:\t"] {"x-empty" ""}]
               [["X-Empty:" "X-Empty: b"] {"x-empty" ",b"}]
               [["X-Empty: a" "X-Empty:"] {"x-empty" "a,"}]
               [["X-Empty: a" "X-Empty:" "X-Empty: c"] {"x-empty" "a,,c"}]
               [["Cookie:" "Cookie: b=2"] {"cookie" ";b=2"}]
               [[] {}]]]
        (is (= expected (edn/read-string (raw-request-headers port header-lines)))
            (pr-str header-lines)))
      (finally
        (busker/stop! server)))))

(defn- finishes-within?
  "Runs `f` on a daemon platform thread and returns true when it returns or
  throws within `timeout-ms`."
  [timeout-ms f]
  (let [thread (doto (Thread. (fn [] (try (f) (catch Throwable _ nil))))
                 (.setDaemon true)
                 (.start))]
    (.join thread (long timeout-ms))
    (not (.isAlive thread))))

(deftest write-chunk-does-not-block-when-the-body-queue-is-full-test
  (let [{:keys [write-chunk]} (request/create-write-req-channel (fn [] nil))]
    (write-chunk (byte-array [(byte 1)]) false)
    (is (finishes-within? 1000 #(write-chunk (byte-array [(byte 2)]) true))
        "A chunk delivered without the previous one being read must not block the worker")))

(deftest write-chunk-after-close-is-dropped-test
  (let [{:keys [channel write-chunk]} (request/create-write-req-channel (fn [] nil))]
    (.close ^java.nio.channels.Channel channel)
    (is (finishes-within? 1000 #(write-chunk (byte-array [(byte 1)]) true)))
    (is (thrown? java.nio.channels.ClosedChannelException
                 (.read ^java.nio.channels.ReadableByteChannel channel
                        (java.nio.ByteBuffer/allocate 8))))))

(deftest close-racing-a-final-chunk-does-not-block-the-writer-test
  (let [blocked (atom 0)]
    (dotimes [_ 2000]
      (let [{:keys [channel write-chunk]} (request/create-write-req-channel (fn [] nil))
            barrier (java.util.concurrent.CyclicBarrier. 2)
            writer (doto (Thread. (fn []
                                    (.await barrier)
                                    (write-chunk (byte-array 8) true)))
                     (.setDaemon true)
                     (.start))]
        (.await barrier)
        (.close ^java.nio.channels.Channel channel)
        (.join writer 200)
        (when (.isAlive writer)
          (swap! blocked inc))))
    (is (zero? @blocked)
        "Closing the body while the worker enqueues the final chunk must not block the worker")))

(deftest handler-closing-the-request-body-early-leaves-the-worker-serving-test
  (let [port (util/free-port)
        upload (doto (java.io.File/createTempFile "busker-upload" ".bin") (.deleteOnExit))
        read-counts (atom [])
        server (busker/start!
                (util/with-handler
                  (fn [request]
                    (if-let [^java.io.InputStream body (:body request)]
                      (let [buffer (byte-array 4096)
                            n (.read body buffer)]
                        (.close body)
                        (swap! read-counts conj n)
                        {:status 200 :body "closed early"})
                      {:status 200 :body "no body"}))
                  {:n-workers 1
                   :entrypoints {:http {:bind (str "127.0.0.1:" port)
                                        :http3? false :tls false}}}))]
    (try
      (with-open [out (java.io.FileOutputStream. upload)]
        (.write out (byte-array (* 8 1024 1024) (byte 120))))
      (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2)
      (dotimes [_ 3]
        (let [result (util/curl :http :h1 port "/upload" :max-time 10
                                :args ["--data-binary" (str "@" upload)])]
          (is (= "closed early" (:out result)) (:err result))))
      (is (= {:exit 0 :out "no body"}
             (select-keys (util/curl :http :h1 port "/" :max-time 5) [:exit :out]))
          "The worker keeps serving after handlers closed request bodies early")
      (is (= 3 (count @read-counts)))
      (is (every? pos? @read-counts))
      (finally
        (busker/stop! server)))))
