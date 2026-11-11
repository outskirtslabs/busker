(ns ol.busker.connection-spread-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.generation :as generation]
   [ol.busker.test-utils :as util])
  (:import
   [java.io ByteArrayOutputStream]
   [java.net InetSocketAddress Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent.atomic AtomicLongArray]))

(deftest at-minimum-rule
  (let [at-minimum? #'generation/at-minimum?
        counts (fn [& xs] (AtomicLongArray. (long-array xs)))]
    (testing "equal counts"
      (is (at-minimum? 0 (counts 0 0 0 0)))
      (is (at-minimum? 3 (counts 3 3 3))))
    (testing "above the minimum"
      (is (not (at-minimum? 1 (counts 1 0 1))))
      (is (not (at-minimum? 2 (counts 1 2 2)))))
    (testing "at the minimum while others are higher"
      (is (at-minimum? 1 (counts 1 2 3)))
      (is (at-minimum? 0 (counts 2 0 Long/MAX_VALUE))))
    (testing "single worker"
      (is (at-minimum? 0 (counts 0)))
      (is (at-minimum? 7 (counts 7))))))

(defn- start-server [n-workers]
  (let [port (util/free-port)
        server (busker/start!
                (util/with-handler
                  (fn [_] {:status 200 :body "ok"})
                  {:n-workers n-workers
                   :entrypoints {:http {:bind (str "127.0.0.1:" port)
                                        :http3? false
                                        :tls false}}}))]
    (is (zero? (:exit (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2))))
    [server port]))

(defn- counts [server]
  (let [^AtomicLongArray counts (::generation/worker-connection-counts
                                 (:instance (:active @(:busker/state server))))]
    (mapv #(.get counts (int %)) (range (.length counts)))))

(defn- await-counts! [server pred]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (let [current (counts server)]
        (cond
          (pred current) current
          (> (System/currentTimeMillis) deadline) current
          :else (do (Thread/sleep 5) (recur)))))))

(defn- open-answered!
  "Opens a keep-alive connection, sends one request and reads its response."
  ^Socket [port timeout-ms]
  (let [socket (Socket.)]
    (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) (int timeout-ms))
    (.setSoTimeout socket (int timeout-ms))
    (let [out (.getOutputStream socket)
          in (.getInputStream socket)
          seen (ByteArrayOutputStream.)]
      (.write out (.getBytes "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n" StandardCharsets/US_ASCII))
      (.flush out)
      (loop []
        (let [b (.read in)]
          (when (neg? b)
            (throw (ex-info "Connection closed before the response" {:seen (str seen)})))
          (.write seen b)
          (when-not (str/ends-with? (.toString seen "US-ASCII") "\r\n\r\nok")
            (recur))))
      socket)))

(defn- close-all! [sockets]
  (doseq [^Socket socket sockets] (.close socket)))

(deftest four-workers-share-eight-connections
  (let [[server port] (start-server 4)]
    (try
      (is (= [0 0 0 0] (await-counts! server #(every? zero? %))))
      (dotimes [round 20]
        (let [sockets (vec (repeatedly 8 #(open-answered! port 500)))]
          (is (= [2 2 2 2] (counts server)) (str "round " round))
          (close-all! sockets)
          (is (= [0 0 0 0] (await-counts! server #(every? zero? %))))))
      (let [sockets (vec (repeatedly 8 #(open-answered! port 500)))]
        (is (= [2 2 2 2] (counts server)))
        (close-all! (subvec sockets 0 4))
        (is (= 4 (reduce + (await-counts! server #(= 4 (reduce + %))))))
        (let [more (vec (repeatedly 4 #(open-answered! port 500)))]
          (is (= [2 2 2 2] (counts server)))
          (close-all! (into (subvec sockets 4) more))))
      (finally (busker/stop! server)))))

(deftest three-workers-differ-by-at-most-one
  (let [[server port] (start-server 3)]
    (try
      (await-counts! server #(every? zero? %))
      (let [sockets (vec (repeatedly 7 #(open-answered! port 500)))
            current (counts server)]
        (is (= 7 (reduce + current)))
        (is (<= (- (apply max current) (apply min current)) 1) (pr-str current))
        (close-all! sockets))
      (finally (busker/stop! server)))))

(deftest every-connection-is-answered-while-workers-take-turns
  (let [[server port] (start-server 2)]
    (try
      (await-counts! server #(every? zero? %))
      (dotimes [_ 2]
        (let [sockets (vec (repeatedly 50 #(open-answered! port 500)))]
          (is (= [25 25] (counts server)))
          (close-all! sockets)
          (is (= [0 0] (await-counts! server #(every? zero? %))))))
      (finally (busker/stop! server)))))
