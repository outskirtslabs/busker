(ns ol.busker.handler-end-test
  "How the end of a handler ends a response whose emitter it used, over HTTP/1.1,
  HTTP/2 and HTTP/3."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [ol.busker :as busker]
   [ol.busker.protocols :as p]
   [ol.busker.test-utils :as util])
  (:import
   [java.io ByteArrayOutputStream InputStream]
   [java.net InetSocketAddress Socket SocketTimeoutException]
   [java.nio.charset StandardCharsets]))

(defn- no-encoder-head! [emitter]
  (p/emit! emitter {:status 200 :headers {"content-type" "text/plain; charset=x-JISAutoDetect"}}))

(defn- head! [emitter]
  (p/emit! emitter {:status 200 :headers {"content-type" "text/plain; charset=utf-8"}}))

(defn- handler [{:keys [uri] emitter :ol.busker.request/emitter}]
  (case uri
    "/emit-throws" (do (no-encoder-head! emitter)
                       (try (p/emit! emitter "café") (catch Exception _ nil))
                       nil)
    "/head-then-map" (do (head! emitter) {:status 200 :body "late"})
    "/head-then-exception" (do (head! emitter) (throw (ex-info "handler failed" {})))
    "/head-then-error" (do (head! emitter) (throw (AssertionError. "handler failed")))
    "/body-then-exception" (do (head! emitter)
                               (p/emit! emitter "partial" {:flush? true})
                               (throw (ex-info "handler failed" {})))
    "/large-body-then-exception" (do (head! emitter)
                                     (p/emit! emitter (byte-array (* 2 1024 1024) (byte 66))
                                              {:flush? true})
                                     (throw (ex-info "handler failed" {})))
    "/error" (throw (AssertionError. "handler failed"))
    "/handoff" (do (head! emitter)
                   (Thread/startVirtualThread
                    #(do (Thread/sleep 200)
                         (p/emit! emitter "later")
                         (p/close! emitter)))
                   {:body emitter})
    {:status 200 :body "ok"}))

(def ^:private abandoned-paths
  "Handlers that end after a head without closing the emitter or returning it."
  ["/emit-throws" "/head-then-map" "/head-then-exception" "/head-then-error"
   "/body-then-exception" "/large-body-then-exception"])

(defn- with-server [f]
  (let [port (util/free-port)
        tls-port (util/free-port)
        server (busker/start!
                (util/with-static-tls
                  (util/with-handler
                    handler
                    {:n-workers 1
                     :entrypoints {:plain {:bind (str "127.0.0.1:" port) :tls false :http3? false}
                                   :tls {:bind (str "127.0.0.1:" tls-port)
                                         :http3? true
                                         :tls {:tls-compatibility-mode :modern}}}})))]
    (try
      (util/wait-for-curl-ready! :http :h1 port "/")
      (util/wait-for-curl-ready! :https :h3 tls-port "/")
      (f port tls-port)
      (finally (busker/stop! server)))))

(defn- read-head [^InputStream in]
  (let [out (ByteArrayOutputStream.)]
    (loop []
      (let [b (.read in)]
        (when (neg? b) (throw (ex-info "EOF in response head" {:head (str out)})))
        (.write out b)
        (when-not (str/ends-with? (str out) "\r\n\r\n") (recur))))
    (parse-long (second (str/split (str out) #" ")))))

(defn- http1-status-and-end
  "Sends GET `path` and returns the status and `:closed` when the server closes
  the connection within five seconds, or `:open`."
  [port path]
  (with-open [socket (Socket.)]
    (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 2000)
    (.setSoTimeout socket 5000)
    (.write (.getOutputStream socket)
            (.getBytes (str "GET " path " HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                       StandardCharsets/US_ASCII))
    (let [in (.getInputStream socket)
          status (read-head in)]
      [status (try (.readAllBytes in) :closed
                   (catch SocketTimeoutException _ :open))])))

(defn- curl-exchange [scheme proto port path]
  (let [{:keys [exit out]} (util/curl scheme proto port path :max-time 5
                                      :args ["-w" "\n%{http_code}"])
        split (str/last-index-of out "\n")]
    {:exit exit
     :status (parse-long (subs out (inc split)))
     :body (subs out 0 split)}))

(deftest abandoned-emitter-closes-the-http1-connection-test
  (with-server
    (fn [port _]
      (doseq [path abandoned-paths]
        (is (= [200 :closed] (http1-status-and-end port path)) path)))))

(deftest abandoned-emitter-resets-the-http2-stream-test
  (with-server
    (fn [_ tls-port]
      (doseq [path abandoned-paths]
        ;; curl exits 92 on an HTTP/2 stream error.
        (is (= 92 (:exit (curl-exchange :https :h2 tls-port path))) path)))))

(deftest abandoned-emitter-resets-the-http3-stream-test
  (with-server
    (fn [_ tls-port]
      (doseq [path abandoned-paths]
        ;; curl exits 18 when body bytes came before the reset, so its message
        ;; shows the reset.
        (is (str/includes? (:err (util/curl :https :h3 tls-port path :max-time 5
                                            :args ["-S" "-o" "/dev/null"]))
                           "HTTP/3 stream 0 reset by server (error 0x102")
            path)))))

(deftest handler-error-before-output-sends-500-test
  (with-server
    (fn [port tls-port]
      (doseq [[scheme proto port] [[:http :h1 port] [:https :h2 tls-port] [:https :h3 tls-port]]]
        (is (= {:exit 0 :status 500 :body "Internal Server Error"}
               (curl-exchange scheme proto port "/error"))
            (pr-str proto))))))

(deftest emitter-handed-to-another-thread-stays-open-test
  (with-server
    (fn [port tls-port]
      (doseq [[scheme proto port] [[:http :h1 port] [:https :h2 tls-port] [:https :h3 tls-port]]]
        (is (= {:exit 0 :status 200 :body "later"} (curl-exchange scheme proto port "/handoff"))
            (pr-str proto))))))
