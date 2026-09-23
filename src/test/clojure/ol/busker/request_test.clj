(ns ol.busker.request-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.request :as request])
  (:import
   [java.nio.charset StandardCharsets]))

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
