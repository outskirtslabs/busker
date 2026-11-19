(ns ol.busker.connection-spread-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.generation :as generation]
   [ol.busker.generation.accept :as accept]
   [ol.busker.test-utils :as util])
  (:import
   [java.io ByteArrayOutputStream]
   [java.net InetSocketAddress Socket SocketException SocketTimeoutException]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent.atomic AtomicLongArray]))

(deftest at-minimum-rule
  (let [at-minimum? #'accept/at-minimum?
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

(defn- start-server [n-workers & {:as config}]
  (let [port (util/free-port)
        server (busker/start!
                (util/with-handler
                  (fn [_] {:status 200 :body "ok"})
                  (merge {:n-workers n-workers
                          :entrypoints {:http {:bind (str "127.0.0.1:" port)
                                               :http3? false
                                               :tls false}}}
                         config)))]
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

(defn- send-request!
  ^Socket [port timeout-ms]
  (let [socket (Socket.)]
    (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) (int timeout-ms))
    (doto (.getOutputStream socket)
      (.write (.getBytes "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n" StandardCharsets/US_ASCII))
      (.flush))
    socket))

(defn- response
  [^Socket socket timeout-ms]
  (.setSoTimeout socket (int timeout-ms))
  (let [in (.getInputStream socket)
        seen (ByteArrayOutputStream.)]
    (try
      (loop []
        (let [b (.read in)]
          (if (neg? b)
            :closed
            (do (.write seen b)
                (if (str/ends-with? (.toString seen "US-ASCII") "\r\n\r\nok")
                  :answered
                  (recur))))))
      (catch SocketTimeoutException _ :silent)
      (catch SocketException _ :closed))))

(defn- open-answered!
  "Opens a keep-alive connection, sends one request and reads its response."
  ^Socket [port timeout-ms]
  (let [socket (send-request! port timeout-ms)
        result (response socket timeout-ms)]
    (when-not (= :answered result)
      (throw (ex-info "No response" {:result result})))
    socket))

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

(deftest connections-above-the-limit-wait-for-a-free-slot
  (let [[server port] (start-server 1 :max-connections 4)]
    (try
      (let [served (vec (repeatedly 4 #(open-answered! port 2000)))
            waiting (vec (repeatedly 8 #(send-request! port 2000)))]
        (is (= :silent (response (first waiting) 200))
            "A fifth connection gets no response while four are open.")
        (is (= (conj (vec (repeat 7 [:answered :silent])) [:answered nil])
               (mapv (fn [^Socket old i]
                       (.close old)
                       [(response (nth waiting i) 2000)
                        (some-> (get waiting (inc i)) (response 200))])
                     (into served waiting)
                     (range 8)))
            "Each close lets only the next waiting connection in, and none is closed.")
        (close-all! (into served waiting)))
      (finally (busker/stop! server)))))
