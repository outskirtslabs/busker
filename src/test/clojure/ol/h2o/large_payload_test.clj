(ns ol.h2o.large-payload-test
  (:require
   [clojure.test :as test :refer [deftest is testing]]
   [ol.h2o.server-test :as st]))

(def gib (* 1024 1024 1024))
(def mib (* 1024 1024))
(def kib 1024)

(defn repeat-input-stream ^java.io.InputStream
  [n b]
  (let [b   (bit-and (int b) 0xFF)        ; 0..255
        bb  (unchecked-byte b)
        cnt (java.util.concurrent.atomic.AtomicLong. n)]
    (proxy [java.io.InputStream] []
      (read
        ([] (let [r (.get cnt)]
              (if (pos? r)
                (do (.decrementAndGet cnt) b) ; returns 0..255
                -1)))
        ([buf]
         (let [r (.get cnt)]
           (if (zero? r)
             -1
             (let [k (int (min r (alength buf)))]
               (java.util.Arrays/fill buf 0 k bb)
               (.addAndGet cnt (- k))
               k))))
        ([buf off len]
         (let [r (.get cnt)]
           (if (zero? r)
             -1
             (let [k (int (min r len))]
               (java.util.Arrays/fill buf off (+ off k) bb)
               (.addAndGet cnt (- k))
               k))))))))

(defn sha256-hex [^java.io.InputStream is]
  (let [md    (java.security.MessageDigest/getInstance "SHA-256")
        buf   (byte-array 65536)
        total (loop [n     (.read is buf)
                     total 0]
                (if (pos? n)
                  (do
                    (.update md buf 0 n)
                    (recur (.read is buf)
                           (+ total n)))
                  total))]
    [(format "%064x" (BigInteger. 1 (.digest md)))
     total]))

(def payload-size (* 1024 mib))
(def value (byte \b))
(def sha (first (with-open [is (repeat-input-stream payload-size value)] (sha256-hex is))))
(println "Large test with payload size " payload-size "has sha" sha)

(deftest request-body
  (st/with-server [_server
                   (st/test-server
                    (fn [{:keys [uri body]}]
                      (case uri
                        "/sink"   {:status  200
                                   :headers {"x-len"    (str payload-size)
                                             "x-sha256" (with-open [input-stream body] (first (sha256-hex input-stream)))}}
                        "/source" {:status  200
                                   :headers {"content-type" "application/octet-stream"
                                             "x-len"        (str payload-size)
                                             "x-sha256"     sha}
                                   :body    (repeat-input-stream payload-size value)}
                        {:status 404}))
                    :max-request-entity-size (* 5 gib))]
    (let [resp (st/req :post "/sink"
                       :headers {"content-type" "application/octet-stream"}
                       :timeout 120000
                       :body (repeat-input-stream payload-size value))]
      (is (= 200 (:status #p resp)))
      (is (= (str payload-size) (get-in resp [:headers "x-len"]))))))

(deftest response-body
  (st/with-server [_server
                   (st/test-server
                    (fn [{:keys [uri body]}]
                      (case uri
                        "/sink"   {:status  200
                                   :headers {"x-len"    (str payload-size)
                                             "x-sha256" (with-open [input-stream body] (first (sha256-hex input-stream)))}}
                        "/source" {:status  200
                                   :headers {"content-type" "application/octet-stream"
                                             "x-len"        (str payload-size)
                                             "x-sha256"     sha}
                                   :body    (repeat-input-stream payload-size value)}
                        {:status 404}))
                    :max-request-entity-size (* 5 gib))]
    (let [resp (st/req :get "/source" :as :stream :timeout 120000)

          [sha total] (with-open [input-stream (:body resp)] (sha256-hex input-stream))]
      (is (= 200 (:status resp)))
      (is (= (get-in resp [:headers "x-sha256"]) sha))
      (is (= (str payload-size) (get-in resp [:headers "x-len"]))))))
