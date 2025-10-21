(ns ol.h2o.server-test
  (:require
   [babashka.http-client :as http]
   [babashka.process :as p]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :as test :refer [deftest is testing]]
   [ol.h2o.server :as server]
   [ol.h2o.test-utils :as util]))

(def plain-port 7890)

(def base (str "http://127.0.0.1:" plain-port))

(defn test-server [handler & {:as opts}]
  (server/run-server handler (merge {:listeners [{:port plain-port}]
                                     :compress-min-size 10
                                     :max-connections 1024}
                                    opts)))

(defn req [method path & {:as opts}]
  (->
   (http/request (merge {:timeout 5000 :throw false
                         :headers {"Accept-Encoding" []}}
                        opts
                        {:uri (str base path)
                         :method method}))
   (dissoc :request)))

(defmacro with-server
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[server-sym init-expr] & body]
  `(let [~server-sym ~init-expr]
     (try
       ~@body
       (finally
         (server/stop-server ~server-sym)))))

(def abcs (cycle "abcdefghijklmnopqrstuvwxyz"))
(def large-payload-str (str "START" (str/join "" (take 1000000 (cycle "abcdefghijklmnopqrstuvwxyz"))) "END"))
(defn lazy-abcs [n] ((fn step [s]
                       (lazy-seq
                        (Thread/sleep 100)
                        (when (seq s)
                          (let [n 2
                                chunk (apply str (take n s))]
                            (cons chunk (step (drop n s)))))))
                     (take n abcs)))

(deftest test-simple-request
  (with-server [_server (test-server (fn [{:keys [uri] :as req}]
                                       (cond
                                         (= "/simple" uri)
                                         {:status  200
                                          :headers {"content-type" "text/plain"}
                                          :body    "Hello, World Hello, World Hello, World"}

                                         (= "/chunked" uri)
                                         {:status  200
                                          :headers {"content-type" "text/plain"}
                                          :body    (lazy-abcs 26)}
                                         (= "/large" uri)
                                         {:status  201
                                          :headers {"content-type" "text/plain"}
                                          :body    large-payload-str}
                                         :else {:status 400})))]
    (testing "simple"
      (is (util/submap? {:status  200
                         :version :http1.1
                         :body    "Hello, World Hello, World Hello, World"
                         :headers {"connection"     "close"
                                   "content-length" "38"
                                   "content-type"   "text/plain"}}
                        (req :get "/simple"))))
    (testing "chunked"
      (is (util/submap? {:status  200
                         :version :http1.1
                         :body    "abcdefghijklmnopqrstuvwxyz"
                         :headers {"connection" "close" "content-type" "text/plain" "transfer-encoding" "chunked"}}
                        (req :get "/chunked"))))
    (testing "large"
      (is (util/submap? {:status  201
                         :version :http1.1
                         :body    large-payload-str
                         :headers {"connection" "close" "content-type" "text/plain" "content-length" "1000008"}}
                        (req :post "/large"))))
    (testing "gzip"
      (is (util/submap? {:status  200
                         :version :http1.1
                         :body    "Hello, World Hello, World Hello, World"
                         :headers {"connection"     "close"
                                   "content-encoding" "gzip"
                                   "vary" "accept-encoding"
                                   "content-type"   "text/plain"}}
                        (req :get "/simple" {:headers {"Accept-Encoding" ["gzip"]}}))))))

(deftest test-content-length
  (testing "content-length with get and head"
    (with-server [_server (test-server (fn [{:keys [request-method]}]
                                         (if (= :head request-method)
                                           {:status 200
                                            :headers {"content-type" "text/plain"
                                                      "content-length" "3"}}
                                           {:status 200
                                            :headers {"content-type" "text/plain"
                                                      "content-length" "3"}
                                            :body "wow"})))]
      (let [response (req :head "/")]
        (is (= 200 (:status response)))
        (is (= "3" (get-in response [:headers "content-length"])))
        (is (= "" (:body response))))
      (let [response (req :get "/")]
        (is (= 200 (:status response)))
        (is (= "3" (get-in response [:headers "content-length"])))
        (is (= "wow" (:body response)))))))

(deftest test-different-status-codes
  (testing "Different status codes are returned correctly"
    (with-server [_server (test-server (fn [{:keys [uri]}]
                                         (case uri
                                           "/200" {:status 200 :body "OK"}
                                           "/201" {:status 201 :body "Created"}
                                           "/204" {:status 204}
                                           "/400" {:status 400 :body "Bad Request"}
                                           "/404" {:status 404 :body "Not Found"}
                                           "/500" {:status 500 :body "Internal Server Error"}
                                           {:status 200 :body "Default"})))]
      (is (= 200 (:status (req :get "/200"))))
      (is (= 201 (:status (req :get "/201"))))
      (is (= 204 (:status (req :get "/204"))))
      (is (= 400 (:status (req :get "/400"))))
      (is (= 404 (:status (req :get "/404"))))
      (is (= 500 (:status (req :get "/500")))))))

(deftest test-query-strings
  (testing "Query strings are parsed correctly"
    (with-server [_server (test-server (fn [{:keys [query-string]}]
                                         {:status 200
                                          :headers {"content-type" "text/plain"}
                                          :body (str "Query: " (or query-string "none"))}))]
      (let [response (req :get "/test?foo=bar&baz=qux")]
        (is (= 200 (:status response)))
        (is (re-find #"Query: foo=bar&baz=qux" (:body response))))

      (let [response (req :get "/test")]
        (is (= 200 (:status response)))
        (is (re-find #"Query: none" (:body response)))))))

(deftest test-request-info
  (testing "Request info is correctly populated"
    (let [request-info (atom nil)]
      (with-server [_server (test-server (fn [req]
                                           (reset! request-info req)
                                           {:status 200 :body "OK"}))]
        (req :get "/test?foo=bar")
        (Thread/sleep 100)
        (let [req @request-info]
          (is (= :get (:request-method req)))
          (is (= "/test" (:uri req)))
          (is (= "foo=bar" (:query-string req)))
          (is (= "127.0.0.1" (:server-name req)))
          (is (= plain-port (:server-port req)))
          (is (= :http (:scheme req)))
          (is (string? (:protocol req)))
          (is (map? (:headers req))))))))

(deftest test-ring-body-types
  (testing "All Ring StreamableResponseBody types work correctly"
    (with-server [_server (test-server (fn [{:keys [uri]}]
                                         (case uri
                                           "/string" {:status 200
                                                      :headers {"content-type" "text/plain"}
                                                      :body "Hello String"}

                                           "/bytes" {:status 200
                                                     :headers {"content-type" "application/octet-stream"}
                                                     :body (.getBytes "Hello Bytes" "UTF-8")}

                                           "/seq" {:status 200
                                                   :headers {"content-type" "text/plain"}
                                                   :body (lazy-abcs 27)}

                                           "/file" (let [temp-file (java.io.File/createTempFile "test" ".txt")]
                                                     (.deleteOnExit temp-file)
                                                     (spit temp-file "Hello from file")
                                                     {:status 200
                                                      :headers {"content-type" "text/plain"}
                                                      :body temp-file})

                                           "/stream" (let [data "Hello from stream"
                                                           input-stream (java.io.ByteArrayInputStream. (.getBytes data "UTF-8"))]
                                                       {:status 200
                                                        :headers {"content-type" "text/plain"}
                                                        :body input-stream})

                                           "/nil-body" {:status 200
                                                        :headers {"content-type" "text/plain"}
                                                        :body nil}

                                           "/no-body" {:status 204
                                                       :headers {}}

                                           {:status 404})))]
      (testing "String body"
        (let [response (req :get "/string")]
          (is (= 200 (:status response)))
          (is (= "Hello String" (:body response)))))

      (testing "Byte array body"
        (let [response (req :get "/bytes")]
          (is (= 200 (:status response)))
          (is (= "Hello Bytes" (:body response)))))

      (testing "Sequence body"
        (let [response (req :get "/seq")]
          (is (= 200 (:status response)))
          (is (= "abcdefghijklmnopqrstuvwxyza" (:body response)))))

      (testing "File body"
        (let [response (req :get "/file")]
          (is (= 200 (:status response)))
          (is (= "Hello from file" (:body response)))))

      (testing "InputStream body"
        (let [response (req :get "/stream")]
          (is (= 200 (:status response)))
          (is (= "Hello from stream" (:body response)))))

      (testing "Nil body"
        (let [response (req :get "/nil-body")]
          (is (= 200 (:status response)))
          (is (= "" (:body response)))))

      (testing "No body (204)"
        (let [response (req :get "/no-body")]
          (is (= 204 (:status response)))
          (is (= "" (:body response))))))))

(deftest test-tls-listener
  (testing "TLS listener with HTTPS connections"
    (let [cert-file (.getAbsolutePath (io/file "src/test/fixtures/server.crt"))
          key-file (.getAbsolutePath (io/file "src/test/fixtures/server.key"))]
      (with-server [_server (test-server
                             (fn [{:keys [scheme uri]}]
                               {:status 200
                                :headers {"content-type" "text/plain"}
                                :body (str "Hello via " (name scheme) " at " uri)})
                             :listeners [{:port plain-port}
                                         {:port 7891
                                          :tls {:cert-file cert-file
                                                :key-file key-file}}])]
        (testing "plaintext HTTP endpoint works"
          (let [response (req :get "/hello")]
            (is (= 200 (:status response)))
            (is (= "Hello via http at /hello" (:body response)))))

        (testing "HTTPS endpoint works"
          (let [result (p/shell {:out :string :err :string :continue true}
                                "curl" "--insecure" "-s"
                                "https://127.0.0.1:7891/secure")]
            (is (= 0 (:exit result)) "HTTPS request should succeed")
            (is (re-find #"Hello via https at /secure" (:out result)) "HTTPS should return expected response")))

        (testing "HTTPS with HTTP/2 ALPN negotiation"
          (let [result (p/shell {:out :string :err :string}
                                "curl" "--http2" "--insecure" "-v" "-s"
                                "https://127.0.0.1:7891/h2")]
            (is (= 0 (:exit result)) "HTTP/2 request should succeed")
            (is (re-find #"Hello via https at /h2" (:out result)) "HTTP/2 should return expected response")
            (is (re-find #"ALPN.*h2" (:err result)) "Should negotiate HTTP/2 via ALPN")))))))

;; TODO: Implement these tests once streaming support is complete
#_(deftest test-exceptions
    (testing "Throwing an exception in a handler causes a 500"
      (with-server [_server (test-server (fn [_]
                                           (throw (RuntimeException. "elephant"))))]
        (let [response (req :get "/")]
          (is (= 500 (:status response)))
          (is (re-find #"elephant" (:body response))))))

    (testing "Throwing an exception in the middle of writing StreamableResponseBody cannot turn a 200 into a 500"
      (with-server [_server (test-server (fn [_]
                                           {:status 200
                                            :body   (take 5 (concat ["chunk1" "chunk2" "chunk3"]
                                                                    (lazy-seq (throw (Exception. "Failed after 3 chunks")))))}))]
        (let [response (req :get "/")]
          (println (:body response))
          (is (= 200 (:status response)))))))

