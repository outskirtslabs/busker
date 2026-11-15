(ns ol.busker.date-header-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.protocols :as protocols]
   [ol.busker.response-serialization :as serialization]
   [ol.busker.test-utils :as util])
  (:import
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.time Duration Instant ZonedDateTime]
   [java.time.format DateTimeFormatter]))

(def ^:private given "Sun, 06 Nov 1994 08:49:37 GMT")

;; 65 pairs do not fit the fixed header space.
(def ^:private large
  (into {"content-type" "text/plain"} (map (fn [i] [(format "x-large-%02d" i) "v"])) (range 65)))

(defn- handler
  [{:keys [uri] emitter :ol.busker.request/emitter}]
  (case uri
    "/fixed" {:status 200 :headers {"content-type" "text/plain"} :body "ok"}
    "/redirect" {:status 302 :headers {"location" "/fixed"} :body "ok"}
    "/no-content" {:status 204}
    "/not-modified" {:status 304 :headers {"etag" "\"a\""}}
    "/throws" (throw (ex-info "handler failure" {}))
    "/producer" (do (protocols/emit! emitter {:status 200
                                              :body (fn [^ByteBuffer buffer]
                                                      (.put buffer (.getBytes "ok" StandardCharsets/UTF_8)))}
                                     {:end? true :capacity 2})
                    {:body emitter})
    "/stream" (do (protocols/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})
                  (protocols/emit! emitter "ok" {:end? true})
                  {:body emitter})
    "/large" {:status 200 :headers large :body "ok"}
    "/info" (do (protocols/emit! emitter {:status 103 :headers {"link" "</a>; rel=preload"}})
                {:status 200 :body "ok"})
    "/given-fixed" {:status 200 :headers {"Date" given} :body "ok"}
    "/given-stream" (do (protocols/emit! emitter {:status 200 :headers {"DATE" given}})
                        (protocols/emit! emitter "ok" {:end? true})
                        {:body emitter})
    "/given-large" {:status 200 :headers (assoc large "date" given) :body "ok"}
    nil))

(defn- start-server []
  (let [port (util/free-port)
        server (busker/start!
                (util/with-static-tls
                  (util/with-handler
                    handler
                    {:n-workers 1
                     :entrypoints {:tls {:bind (str "127.0.0.1:" port) :http3? true}}})))]
    (util/wait-for-curl-ready! :https :h1 port "/fixed")
    {:server server :port port}))

(defn- curl-heads
  "Requests `path` over `proto` and returns curl's exit code, the body and each
  response head as `[status date-values]`, where `date-values` are the values
  of every Date field in that head, in order. `args` go to curl after -i."
  [proto port path & args]
  (let [{:keys [exit out err]} (util/curl :https proto port path :args (into ["-i"] args)
                                          :max-time 30)
        blocks (str/split out #"\r\n\r\n" -1)
        head (fn [block]
               (let [[status-line & lines] (str/split-lines block)]
                 [(parse-long (second (str/split status-line #" ")))
                  (into []
                        (keep (fn [line]
                                (let [[k v] (str/split line #":\s*" 2)]
                                  (when (= "date" (str/lower-case k)) v))))
                        lines)]))]
    {:exit exit :err err :body (last blocks) :heads (mapv head (butlast blocks))}))

;; IMF-fixdate, RFC 9110 section 5.6.7.
(def ^:private imf-fixdate
  #"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{2}:\d{2}:\d{2} GMT")

(defn- current-date? [value]
  (and (string? value)
       (re-matches imf-fixdate value)
       (let [at (.toInstant (ZonedDateTime/parse value DateTimeFormatter/RFC_1123_DATE_TIME))]
         (< (Math/abs (.toSeconds (Duration/between at (Instant/now)))) 60))))

(deftest the-large-head-takes-the-streaming-path
  (is (not (fixed-final/head-fits? (count large) (serialization/header-bytes (vec large))))))

(deftest every-final-response-gets-a-date-over-every-protocol
  (let [{:keys [server port]} (start-server)]
    (try
      (doseq [proto [:h1 :h2 :h3]]
        (testing "Busker adds one current Date to a final head without one"
          (doseq [[path status & args] [["/fixed" 200] ["/fixed" 200 "-I"] ["/redirect" 302]
                                        ["/no-content" 204] ["/not-modified" 304]
                                        ["/producer" 200] ["/stream" 200] ["/stream" 200 "-I"]
                                        ["/large" 200] ["/missing" 404] ["/throws" 500]]]
            (let [{:keys [exit heads] :as result} (apply curl-heads proto port path args)
                  context (pr-str [proto path args (:err result) heads])]
              (is (zero? exit) context)
              (is (= 1 (count heads)) context)
              (is (= status (ffirst heads)) context)
              (is (= 1 (count (second (first heads)))) context)
              (is (current-date? (first (second (first heads)))) context))))
        (testing "an informational head gets no Date"
          (let [{:keys [exit heads] :as result} (curl-heads proto port "/info")
                context (pr-str [proto "/info" (:err result) heads])]
            (is (zero? exit) context)
            ;; H2O sends no 1xx over HTTP/1.1 by default (send-informational: except-h1).
            (when (not= :h1 proto)
              (is (= [103 []] (first heads)) context))
            (is (= 200 (first (last heads))) context)
            (is (current-date? (first (second (last heads)))) context)))
        (testing "a Date from the handler in any letter case is sent once and unchanged"
          (doseq [path ["/given-fixed" "/given-stream" "/given-large"]]
            (let [{:keys [exit heads] :as result} (curl-heads proto port path)
                  context (pr-str [proto path (:err result) heads])]
              (is (zero? exit) context)
              (is (= [[200 [given]]] heads) context)))))
      (finally
        (busker/stop! server)))))
