(ns ol.busker.header-case-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [ol.busker :as busker]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.protocols :as protocols]
   [ol.busker.response-serialization :as serialization]
   [ol.busker.test-utils :as util])
  (:import
   [java.io ByteArrayOutputStream]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.util.zip GZIPOutputStream]))

(def ^:private capitalised
  (array-map "Content-Type" "text/plain; charset=UTF-8"
             "Content-Length" "2"
             "X-Custom" "one"
             "x-MiXeD-cAsE" "two"))

;; 65 pairs do not fit the fixed header space.
(def ^:private large
  (into capitalised (map (fn [i] [(format "X-Large-%02d" i) "v"])) (range 65)))

(defn- ok-bytes ^bytes [] (.getBytes "ok" StandardCharsets/UTF_8))

;; Its gzip form is above the 100 bytes of the default compress-min-size.
(def ^:private text (apply str (range 2000)))

(defn- gzip ^bytes [^String s]
  (let [out (ByteArrayOutputStream.)]
    (with-open [gz (GZIPOutputStream. out)]
      (.write gz (.getBytes s StandardCharsets/UTF_8)))
    (.toByteArray out)))

(defn- handler
  [{:keys [uri] emitter :ol.busker.request/emitter}]
  (case uri
    "/string" {:status 200 :headers capitalised :body "ok"}
    "/bytes" {:status 200 :headers capitalised :body (ok-bytes)}
    "/seq" {:status 200 :headers capitalised :body (map str "ok")}
    "/producer" (do (protocols/emit! emitter {:status 200 :headers capitalised
                                              :body (fn [^ByteBuffer buffer] (.put buffer (ok-bytes)))}
                                     {:end? true :capacity 2})
                    {:body emitter})
    "/stream" (do (protocols/emit! emitter {:status 200 :headers capitalised})
                  (protocols/emit! emitter "ok" {:end? true})
                  {:body emitter})
    "/info" (do (protocols/emit! emitter {:status 103 :headers {"Link" "</a>; rel=preload"
                                                                "X-Custom" "early"}})
                {:status 200 :headers capitalised :body "ok"})
    "/large" {:status 200 :headers large :body "ok"}
    "/latin-1" {:status 200 :headers {"Content-Type" "text/plain; charset=ISO-8859-1"} :body "é"}
    "/wrong-length" {:status 200 :headers {"Content-Length" "5"} :body "ok"}
    "/encoded" {:status 200 :headers {"Content-Type" "text/plain" "Content-Encoding" "gzip"}
                :body (gzip text)}
    {:status 200 :body "ready"}))

(defn- start-server []
  (let [port (util/free-port)
        server (busker/start!
                (util/with-static-tls
                  (util/with-handler
                    handler
                    {:n-workers 1
                     :compress? true
                     :entrypoints {:tls {:bind (str "127.0.0.1:" port) :http3? true}}})))]
    (util/wait-for-curl-ready! :https :h1 port "/")
    {:server server :port port}))

(def ^:private checked-names #{"content-type" "content-length" "content-encoding" "link"})

(defn- curl-heads
  "Requests `path` over `proto` and returns curl's exit code, the body and each
  response head as `[status headers]`. The headers are the sorted name and value
  pairs of content-type, content-length, content-encoding, link and x- headers,
  with their names as sent. `args` go to curl after -i. Busker sends no content-length header. H2O writes it from the length,
  as \"Content-Length\" over HTTP/1.1, so that name is lower-cased here."
  [proto port path & args]
  (let [{:keys [exit out err]} (util/curl :https proto port path :args (into ["-i"] args)
                                          :max-time 30)
        blocks (str/split out #"\r\n\r\n")
        head (fn [block]
               (let [[status-line & lines] (str/split-lines block)]
                 [(parse-long (second (str/split status-line #" ")))
                  (into (sorted-set)
                        (keep (fn [line]
                                (let [[k v] (str/split line #":\s*" 2)
                                      lower (str/lower-case k)]
                                  (when (or (checked-names lower) (str/starts-with? lower "x-"))
                                    [(if (= "content-length" lower) lower k) v]))))
                        lines)]))]
    {:exit exit :err err :body (last blocks) :heads (mapv head (butlast blocks))}))

(defn- lower-case-pairs [headers]
  (into (sorted-set) (map (fn [[k v]] [(str/lower-case k) v])) headers))

(deftest the-large-head-takes-the-streaming-path
  (is (not (fixed-final/head-fits? (count large) (serialization/header-bytes (vec large))))))

(deftest capitalised-header-names-are-sent-lower-case-over-every-protocol
  (let [{:keys [server port]} (start-server)
        final [200 (lower-case-pairs capitalised)]]
    (try
      (doseq [proto [:h1 :h2 :h3]]
        (doseq [path ["/string" "/bytes" "/seq" "/producer" "/stream" "/large"]]
          (let [result (curl-heads proto port path)
                context (pr-str [proto path (:err result)])]
            (is (zero? (:exit result)) context)
            (is (= "ok" (:body result)) context)
            (is (= [(if (= "/large" path) [200 (lower-case-pairs large)] final)] (:heads result))
                context)))
        (let [result (curl-heads proto port "/info")
              context (pr-str [proto "/info" (:err result)])]
          (is (zero? (:exit result)) context)
          (is (= "ok" (:body result)) context)
          ;; H2O sends no 1xx over HTTP/1.1 by default (send-informational: except-h1).
          (is (= (cond->> [final]
                   (not= :h1 proto)
                   (cons [103 (sorted-set ["link" "</a>; rel=preload"] ["x-custom" "early"])]))
                 (:heads result))
              context))
        (let [result (curl-heads proto port "/latin-1")]
          (is (= [[200 (sorted-set ["content-length" "1"]
                                   ["content-type" "text/plain; charset=ISO-8859-1"])]]
                 (:heads result))
              (pr-str [proto "/latin-1" (:err result)])))
        (let [result (curl-heads proto port "/wrong-length")]
          (is (= 500 (ffirst (:heads result))) (pr-str [proto "/wrong-length" (:err result)])))
        ;; H2O's compress filter finds content-encoding by its token, so it must
        ;; see the capitalised name to leave an encoded body alone.
        (let [result (curl-heads proto port "/encoded" "--compressed")
              context (pr-str [proto "/encoded" (:err result)])]
          (is (zero? (:exit result)) context)
          (is (= text (:body result)) context)
          (is (= [["content-encoding" "gzip"]]
                 (filterv #(= "content-encoding" (str/lower-case (first %)))
                          (second (first (:heads result)))))
              context)))
      (finally
        (busker/stop! server)))))
