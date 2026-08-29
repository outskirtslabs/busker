(ns ol.busker.request-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.request :as request])
  (:import
   [java.nio.charset StandardCharsets]
   [java.util.concurrent Executors]))

(deftest request-task-runs-without-future-wrapper-test
  (with-open [executor (Executors/newVirtualThreadPerTaskExecutor)]
    (let [classes_ (promise)
          task (fn []
                 (deliver classes_
                          (mapv #(.getClassName ^StackTraceElement %)
                                (.getStackTrace (Thread/currentThread)))))]
      (#'request/execute-request! executor task)
      (let [classes (deref classes_ 5000 :timeout)]
        (is (vector? classes))
        (is (not-any? #{"java.util.concurrent.FutureTask"
                        "java.util.concurrent.Executors$RunnableAdapter"
                        "java.util.concurrent.ThreadPerTaskExecutor$ThreadBoundFuture"}
                      classes))))))

(deftest native-request-body-marker-test
  (is (false? (#'request/request-has-body? {:has-body 0})))
  (is (true? (#'request/request-has-body? {:has-body 1}))))

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
