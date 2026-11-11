(ns ol.busker.request-test
  (:require
   [babashka.ffi :as ffi]
   [clojure.test :refer [deftest is testing]]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.request :as request]
   [ol.busker.response-queue :as queue]
   [ol.busker.worker-context :as worker-context])
  (:import
   [java.nio.charset StandardCharsets]
   [java.util.concurrent AbstractExecutorService]
   [java.util.concurrent.atomic AtomicReference]))

(deftest request-construction-preserves-context-and-worker-services
  (doseq [[method expected] [[:post :post] [nil :get]]]
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
            config {:output-buffer-size 64 :response-buffer-size 8192}
            ctx {:has-body 0 :request-method method}
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
                    :dispatch-module-id module-id :dispatch-request-seq request-seq
                    :request-method expected}
                   (into {} req)))
            (is (identical? worker (:worker req)))
            (is (identical? config (:config req)))
            (is (identical? pool (:buffer-pool (queue/new-response-state req))))
            (is (nil? (meta req)))
            (is (= 1 (count @tasks_)))))))))

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
