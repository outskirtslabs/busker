(ns ol.busker.request-threads-test
  "`:request-threads` through the public API: one virtual thread per request
  by default; with `:reuse-within-connection`, quick requests of one connection
  share virtual threads and requests of different connections never do."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.test-utils :as util])
  (:import
   [java.util.concurrent ConcurrentLinkedQueue]))

(defn- recording-handler
  "Returns a handler that records `[connection-label thread]` for every request,
  the label coming from the x-conn header, and then calls `respond`."
  [^ConcurrentLinkedQueue seen respond]
  (fn [req]
    (.add seen [(get-in req [:headers "x-conn"]) (Thread/currentThread)])
    (respond req)))

(defn- ok [_] {:status 200 :body ""})

(defn- start!
  "Starts a one-worker server with a plain entrypoint (HTTP/1.1 and h2c) on
  `port` and, given `tls-port`, an HTTP/3 entrypoint."
  [handler port & {:keys [request-threads tls-port]}]
  (let [server (busker/start!
                (cond-> (util/with-handler
                          handler
                          {:entrypoints (cond-> {:plain {:bind (str "127.0.0.1:" port)
                                                         :tls false :http3? false}}
                                          tls-port (assoc :tls {:bind (str "127.0.0.1:" tls-port)
                                                                :http3? true
                                                                :tls {:tls-compatibility-mode :modern}}))
                           :n-workers 1})
                  request-threads (assoc :request-threads request-threads)
                  tls-port util/with-static-tls))]
    (util/wait-for-curl-ready! :http :h1 port "/")
    (when tls-port
      (util/wait-for-curl-ready! :https :h3 tls-port "/" :max-time 5))
    server))

(defmacro ^:private with-server
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[server-sym init-expr] & body]
  `(let [~server-sym ~init-expr]
     (try
       ~@body
       (finally
         (busker/stop! ~server-sym)))))

(defn- one-connection
  "Sends `n` requests to `path` on one connection with one curl process, as
  parallel streams for h2c and HTTP/3, one after another for HTTP/1.1. Returns
  the status code of each response, followed by the seconds the request took
  when `timed?`."
  [proto port n & {:keys [label path timed? max-time] :or {path "/" max-time 30}}]
  (let [scheme (if (= :h3 proto) :https :http)
        host (if (= :h3 proto) "localhost.examp1e.net" "127.0.0.1")
        url (str (name scheme) "://" host ":" port path)
        result (util/curl scheme (when-not (= :h2c proto) proto) port path
                          :max-time max-time
                          :args (concat (case proto
                                          :h2c ["--http2-prior-knowledge" "--parallel"
                                                "--parallel-max" "100"]
                                          :h3 ["--parallel" "--parallel-max" "100"]
                                          [])
                                        (when label ["-H" (str "x-conn: " label)])
                                        ["-o" "/dev/null" "-w" (if timed?
                                                                 "%{http_code} %{time_total}\n"
                                                                 "%{http_code}\n")]
                                        (mapcat (fn [_] ["-o" "/dev/null" url]) (range (dec n)))))]
    (str/split-lines (:out result))))

(defn- threads-serving-several-connections
  [seen]
  (->> seen
       (group-by second)
       (filter (fn [[_ rows]] (< 1 (count (distinct (map first rows))))))
       count))

(deftest per-request-runs-every-request-on-a-new-thread-test
  (doseq [request-threads [nil :per-request]]
    (let [seen (ConcurrentLinkedQueue.)
          port (util/free-port)]
      (with-server [_ (start! (recording-handler seen ok) port :request-threads request-threads)]
        (.clear seen)
        (testing (str "h2c burst on one connection, :request-threads " request-threads)
          (is (= (repeat 100 "200") (one-connection :h2c port 100)))
          (is (= 100 (count (distinct (map second seen))))))
        (.clear seen)
        (testing (str "HTTP/1.1 keep-alive, :request-threads " request-threads)
          (is (= (repeat 30 "200") (one-connection :h1 port 30)))
          (is (= 30 (count (distinct (map second seen))))))
        (is (every? #(.isVirtual ^Thread (second %)) seen))))))

(deftest quick-requests-on-one-connection-share-handler-threads-test
  (let [seen (ConcurrentLinkedQueue.)
        port (util/free-port)]
    (with-server [_ (start! (recording-handler seen ok) port
                            :request-threads :reuse-within-connection)]
      (.clear seen)
      (is (= (repeat 100 "200") (one-connection :h2c port 100)))
      (is (= 100 (count seen)))
      (is (< (count (distinct (map second seen))) 100))
      (is (every? #(.isVirtual ^Thread (second %)) seen)))))

(defn- hammer-concurrently
  "Runs `n` requests on each of the connections labelled `labels` at the same
  time. Returns the status codes of all responses."
  [proto port n labels]
  (->> labels
       (mapv (fn [label] (future (one-connection proto port n :label label))))
       (mapcat deref)))

(deftest connections-of-one-worker-never-share-a-thread-test
  (let [seen (ConcurrentLinkedQueue.)
        port (util/free-port)
        tls-port (util/free-port)]
    (with-server [_ (start! (recording-handler seen ok) port
                            :request-threads :reuse-within-connection :tls-port tls-port)]
      (doseq [[proto port n] [[:h2c port 400] [:h1 port 100] [:h3 tls-port 200]]]
        (testing (name proto)
          (.clear seen)
          (dotimes [_ 3]
            (is (= (repeat (* 2 n) "200") (hammer-concurrently proto port n ["a" "b"]))))
          (is (= (* 6 n) (count seen)))
          (is (zero? (threads-serving-several-connections seen))
              "a thread runs requests of one connection only")
          (is (every? #(.isVirtual ^Thread (second %)) seen)))))))

(deftest blocking-handlers-on-shared-connections-run-concurrently-test
  (let [port (util/free-port)]
    (with-server [_ (start! (fn [_]
                              (Thread/sleep 50)
                              {:status 200 :body ""})
                            port :request-threads :reuse-within-connection)]
      (let [start (System/nanoTime)
            statuses (->> (repeatedly 4 #(future (one-connection :h2c port 8 :path "/sleep")))
                          doall
                          (mapcat deref))
            elapsed-ms (/ (- (System/nanoTime) start) 1e6)]
        (is (= (repeat 32 "200") statuses))
        (is (< elapsed-ms 800)
            (str "32 handlers of 50 ms take 1600 ms one after another; took " elapsed-ms))))))

(deftest fast-requests-do-not-wait-behind-a-slow-handler-test
  (let [fast-count 20
        fast-done_ (atom 0)
        all-fast-done (promise)
        slow-result_ (atom nil)
        port (util/free-port)]
    (with-server [_ (start! (fn [{:keys [uri]}]
                              (if (= "/slow" uri)
                                (reset! slow-result_ (deref all-fast-done 10000 :timed-out))
                                (when (= fast-count (swap! fast-done_ inc))
                                  (deliver all-fast-done :fast-done)))
                              {:status 200 :body ""})
                            port :request-threads :reuse-within-connection)]
      (let [urls (map #(str "http://127.0.0.1:" port %) (repeat fast-count "/fast"))
            result (util/curl :http nil port "/slow"
                              :max-time 20
                              :args (concat ["--http2-prior-knowledge" "--parallel"
                                             "--parallel-max" (str (inc fast-count))
                                             "-o" "/dev/null" "-w" "%{http_code}\n"]
                                            (mapcat (fn [url] ["-o" "/dev/null" url]) urls)))]
        (is (= (repeat (inc fast-count) "200") (str/split-lines (:out result)))))
      (is (= :fast-done @slow-result_)
          "every fast request completes while the slow handler still runs"))))

(deftest reload-and-stop-with-requests-in-flight-test
  (let [port (util/free-port)
        config (fn [body]
                 (util/with-handler (fn [_] {:status 200 :body body})
                   {:entrypoints {:plain {:bind (str "127.0.0.1:" port) :tls false :http3? false}}
                    :n-workers 1
                    :request-threads :reuse-within-connection}))
        server (busker/start! (config "one"))]
    (try
      (util/wait-for-curl-ready! :http :h1 port "/")
      (testing "a reload while bursts of quick requests run answers every request"
        (let [bursts (doall (repeatedly 4 #(future (one-connection :h2c port 200))))]
          (busker/reload! server (config "two"))
          (is (= (repeat 800 "200") (mapcat deref bursts)))))
      (testing "a stop while bursts run returns, and every request it accepted is answered"
        (let [bursts (doall (repeatedly 4 #(future (one-connection :h2c port 200
                                                                   :timed? true :max-time 20))))
              stopped (future (Thread/sleep 20) (busker/stop! server) :stopped)
              results (map #(str/split % #" ") (mapcat #(deref % 40000 ["timeout 0"]) bursts))]
          (is (= :stopped (deref stopped 30000 :timeout)))
          (is (= 800 (count results)))
          (is (every? (fn [[status seconds]]
                        (or (#{"200" "503"} status)
                            ;; curl prints 000 for a request the closing
                            ;; connection refused at once; one the server
                            ;; accepted and never answered runs into max-time.
                            (and (= "000" status) (< (parse-double seconds) 10))))
                      results)
              (pr-str (remove (comp #{"200" "503"} first) results)))))
      (finally
        (busker/stop! server)))))

(deftest invalid-request-threads-value-is-rejected-at-start-test
  (let [port (util/free-port)
        failure (try
                  (busker/stop! (busker/start! (util/with-handler ok
                                                 {:entrypoints {:plain {:bind (str "127.0.0.1:" port)
                                                                        :tls false}}
                                                  :request-threads :per-connection})))
                  nil
                  (catch Throwable t t))
        errors (->> (iterate ex-cause failure)
                    (take-while some?)
                    (mapcat (comp :errors ex-data)))]
    (is (some? failure))
    (is (some (fn [{:keys [error data]}]
                (and (= :ol.busker.config/config-spec-invalid error)
                     (some #(= [:request-threads] (:path %)) (:problems data))))
              errors))))
