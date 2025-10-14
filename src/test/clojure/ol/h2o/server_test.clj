(ns ol.h2o.server-test
  (:require
   [babashka.http-client :as http]
   [clojure.test :as test :refer [deftest is testing]]
   [clojure.string :as str]
   [ol.h2o.server :as server]
   [ol.h2o.test-utils :as util]))

(def plain-port 7890)

(def base (str "http://127.0.0.1:" plain-port))

(defn test-server [handler & {:as opts}]
  (let [config (merge {:handler handler
                       :listeners [{:port plain-port}]
                       :max-connections 1024}
                      opts)]
    (-> (server/create-server config)
        (server/start-server))))

(defn req [method path & {:as opts}]
  (->
   (http/request (merge {:timeout 5000 :throw false}
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
  (with-server [_server (test-server (fn [{:keys [uri]}]
                                       (cond
                                         (= "/simple" uri)
                                         {:status 200
                                          :headers {"content-type" "text/plain"}
                                          :body "Hello, World"}

                                         (= "/chunked" uri)
                                         {:status 200
                                          :headers {"content-type" "text/plain"}
                                          :body (lazy-abcs 26)}
                                         (= "/large" uri)
                                         {:status 201
                                          :headers {"content-type" "text/plain"}
                                          :body large-payload-str}
                                         :else {:status 400})))]
    (testing "simple"
      (is (util/submap? {:status 200
                         :version :http1.1
                         :body "Hello, World"
                         :headers {"connection" "close",
                                   "content-length" "12",
                                   "content-type" "text/plain",
                                   "server" "h2o/2.3.0-DEV"}}
                        (req :get "/simple"))))
    (testing "chunked"
      (is (util/submap? {:status 200
                         :version :http1.1
                         :body "abcdefghijklmnopqrstuvwxyz"
                         :headers {"connection" "close" "content-type" "text/plain" "server" "h2o/2.3.0-DEV" "transfer-encoding" "chunked"}}
                        (req :get "/chunked"))))
    (testing "large"
      (is (util/submap? {:status 201
                         :version :http1.1
                         :body large-payload-str
                         :headers {"connection" "close" "content-type" "text/plain" "server" "h2o/2.3.0-DEV" "content-length" "1000008"}}
                        (req :post "/large"))))))

(deftest test-request-headers
  (let [received-headers (atom nil)]
    (with-server [_server (test-server (fn [req]
                                         (println "=== Request received ===")
                                         (println "Method:" (:request-method req))
                                         (println "URI:" (:uri req))
                                         (println "Headers:" (pr-str (:headers req)))
                                         (println "=======================")
                                         (reset! received-headers (:headers req))
                                         {:status 200
                                          :headers {"content-type" "text/plain"}
                                          :body (str "Headers count: " (count (:headers req)))}))]
      (testing "Request with headers"
        (let [response (req :get "/hello"
                            :headers {"X-Custom-Header" "test-value"
                                      "Content-Type" "application/json"})]
          (is (= 200 (:status response)))
          (is (re-find #"Headers count:" (:body response)))
          ;; Give the async request handler time to complete
          (Thread/sleep 100)
          ;; Check that headers were parsed
          (let [headers @received-headers]
            (is (map? headers))
            (is (> (count headers) 0))
            (is (contains? headers "x-custom-header"))
            (is (= "test-value" (get headers "x-custom-header")))))))))

(deftest test-large-response
  (testing "Large responses work correctly"
    (let [large-content (apply str (repeat 10000 "This is a large response chunk. "))]
      (with-server [_server (test-server (fn [_]
                                           {:status 200
                                            :headers {"content-type" "text/plain"}
                                            :body large-content}))]
        (let [response (req :get "/large")]
          (is (= 200 (:status response)))
          (is (= (count large-content) (count (:body response))))
          (is (.startsWith (:body response) "This is a large response chunk.")))))))

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

;; TLS tests disabled - not yet implemented
#_(deftest test-http2-request
    (let [cert-file (.getAbsolutePath (io/file "src/test/fixtures/server.crt"))
          key-file (.getAbsolutePath (io/file "src/test/fixtures/server.key"))
          config {:handler (fn [req]
                             {:status 200
                              :headers {"content-type" "text/plain"}
                              :body (str "Hello via " (:protocol req) " from " (:server-name req "unknown"))})
                  :listeners [{:port 8080 :host "127.0.0.1"}
                              {:port 8443
                               :host "127.0.0.1"
                               :ssl {:certificate-file cert-file
                                     :private-key-file key-file}}]}]
      (with-server [_server (server/start-server (server/create-server config))]
        (testing "http endpoint works"
          (let [response (http/get "http://127.0.0.1:8080/hello")]
            (is (= 200 (:status response)) "HTTP endpoint should return 200")
            (is (re-find #"Hello via HTTP" (:body response)) "HTTP endpoint should return expected body")))
        (testing "https with HTTP/2 negotiation"
          (let [result (p/shell {:out :string :err :string}
                                "curl" "--http2" "--insecure" "-v" "-s"
                                "https://127.0.0.1:8443/hello")]
            (is (= 0 (:exit result)) "HTTPS request should succeed")
            (is (re-find #"Hello via HTTP" (:out result)) "HTTPS should return expected response body")
            (is (re-find #"ALPN: server accepted h2" (:err result)) "Should negotiate protocol via ALPN"))))))

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
                                            :body (take 5 (concat ["chunk1" "chunk2" "chunk3"]
                                                                  (lazy-seq (throw (Exception. "Failed after 3 chunks")))))}))]
        (let [response (req :get "/")]
          (println (:body response))
          (is (= 200 (:status response)))))))

#_(deftest test-ring-body-types
    (testing "All Ring StreamableResponseBody types work correctly"
      (with-server [_server (test-server (fn [{:keys [uri] :as req}]
                                           (println req)
                                           (case uri
                                             "/string" {:status 200
                                                        :headers {"content-type" "text/plain"}
                                                        :body "Hello String"}

                                             "/bytes" {:status 200
                                                       :headers {"content-type" "application/octet-stream"}
                                                       :body (.getBytes "Hello Bytes" "UTF-8")}

                                             "/seq" {:status 200
                                                     :headers {"content-type" "text/plain"}
                                                     :body (seq ["Hello " "from " "sequence"])}

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
            (is (= "Hello from sequence" (:body response)))))

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
