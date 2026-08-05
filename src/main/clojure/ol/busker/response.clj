(ns ^:no-doc ol.busker.response
  "Response handling for h2o HTTP server."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [coffi.mem :as mem]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as p]
   [ol.busker.protocols.content-length]
   [ol.busker.response-queue :as response-queue]
   [ol.busker.util :refer [compile-if]]
   [ol.busker.util.headers :as hdr.util])
  (:import
   [java.io InputStream OutputStream]
   [java.lang.foreign MemorySegment]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent.atomic AtomicBoolean]
   [ol.busker.internal.protocols Request]))

(set! *warn-on-reflection* true)

(defn dissoc-header
  "Remove all case variations of a header by name (case-insensitive)."
  [headers ^String header-name]
  (let [target-lower (.toLowerCase header-name java.util.Locale/ROOT)]
    (into {}
          (remove (fn [[k _]]
                    (= (.toLowerCase (str k) java.util.Locale/ROOT) target-lower))
                  headers))))

(defn- coerce-content-length
  [val]
  (if (number? val) (long val)
      (Long/parseLong val)))

;; TODO intern common header names?

(defn build-headers2
  "Return a tuple [resp headers headers-len content-length]
  - headers is a clj_header_t *headers MemorySegment
  - headers-len is size_t (number of headers, aka length of headers array)
  - content-length is the content length header if any was found or -1 (SIZE_MAX)

  headers memorysegment should NOT contain content-length if it was found"
  [resp]
  (let [headers (:headers resp)
        content-length-val (second (hdr.util/find-header resp "content-length"))
        content-length (if content-length-val
                         (coerce-content-length content-length-val)
                         -1)
        filtered-headers (if content-length-val
                           (dissoc-header headers "content-length")
                           headers)
        header-pairs (vec filtered-headers)
        headers-count (count header-pairs)]

    (if (zero? headers-count)
      [(mem/as-segment 0) 0 content-length]

      (let [header-size (mem/size-of ::h2o/clj-header-t)
            total-size (* headers-count header-size)
            headers-seg (mem/alloc total-size)]

        (doseq [[idx [name-key value-val]] (map-indexed vector header-pairs)]
          (let [offset (* idx header-size)
                ;; h2 and h3 require lower case headers
                name-str (str/lower-case (if (string? name-key) name-key (str name-key)))
                value-str (if (string? value-val) value-val (str value-val))
                name-ptr ^MemorySegment (mem/serialize name-str ::mem/c-string)
                value-ptr ^MemorySegment (mem/serialize value-str ::mem/c-string)
                header-data {:name name-ptr
                             :name_len (dec (.byteSize name-ptr))
                             :value value-ptr
                             :value_len (dec (.byteSize value-ptr))}
                header-seg (mem/serialize header-data ::h2o/clj-header-t)
                dest-seg (mem/slice headers-seg offset header-size)]
            (mem/copy-segment dest-seg header-seg)))
        [resp headers-seg headers-count content-length]))))

(defn build-headers
  "Return a tuple [headers headers-len content-length]
  - headers is a clj_header_t *headers MemorySegment
  - headers-len is size_t (number of headers, aka length of headers array)
  - content-length is the content length header if any was found or -1 (SIZE_MAX)

  headers memorysegment should NOT contain content-length if it was found"
  [resp]
  (let [headers (:headers resp)
        content-length-val (second (hdr.util/find-header resp "content-length"))
        content-length (if content-length-val
                         (coerce-content-length content-length-val)
                         -1)
        filtered-headers (if content-length-val
                           (dissoc-header headers "content-length")
                           headers)
        header-pairs (vec filtered-headers)
        headers-count (count header-pairs)]

    (if (zero? headers-count)
      [(mem/as-segment 0) 0 content-length]

      (let [header-size (mem/size-of ::h2o/clj-header-t)
            total-size (* headers-count header-size)
            headers-seg (mem/alloc total-size)]

        (doseq [[idx [name-key value-val]] (map-indexed vector header-pairs)]
          (let [offset (* idx header-size)
                name-str (if (string? name-key) name-key (str name-key))
                value-str (if (string? value-val) value-val (str value-val))
                name-ptr ^MemorySegment (mem/serialize name-str ::mem/c-string)
                value-ptr ^MemorySegment (mem/serialize value-str ::mem/c-string)
                header-data {:name name-ptr
                             :name_len (dec (.byteSize name-ptr))
                             :value value-ptr
                             :value_len (dec (.byteSize value-ptr))}
                header-seg (mem/serialize header-data ::h2o/clj-header-t)
                dest-seg (mem/slice headers-seg offset header-size)]
            (mem/copy-segment dest-seg header-seg)))
        [headers-seg headers-count content-length]))))

(defn with-content-length [response]
  (if (hdr.util/get-header response "content-length")
    response
    (if-let [size (p/body-size-in-bytes (:body response) response)]
      (hdr.util/header response "content-length" (str size))
      response)))

(def ^:private byte-array-class (Class/forName "[B"))

(defn- informational-status? [status]
  (and (<= 100 status)
       (< status 200)
       (not= 101 status)))

(defn- final-status? [status]
  (>= status 200))

(defn- schedule-start-response!
  [^Request req status headers headers-len content-length compress-hint write-resp]
  (pi/send-msg (:worker req)
               [:h2o/start-response
                (fn []
                  (h2o/start-response (:req-ctx-ptr req) status
                                      headers headers-len
                                      content-length compress-hint
                                      (:on-proceed-cb-ptr write-resp)
                                      (:on-stop-cb-ptr write-resp)))]))

(defn- schedule-informational!
  [^Request req status headers headers-len]
  (pi/send-msg (:worker req)
               [:h2o/send-informational
                (fn []
                  (h2o/send-informational (:req-ctx-ptr req) status headers headers-len))]))

(defn- send-informational!
  [^Request req resp]
  (when (:body resp)
    (throw (ex-info "Body payloads are not allowed for 1xx informational responses" {:status (:status resp)})))
  (let [status (:status resp)
        headers (or (:headers resp) {})
        [_ headers-seg headers-len _] (build-headers2 {:headers headers})]
    (schedule-informational! req status headers-seg headers-len)))

(defn get-compress-hint [resp]
  (get h2o/->compress-hint (:h2o/compress-hint resp) h2o/H2O_COMPRESS_HINT_ENABLE))

(defn- write-fallback-body-to-stream!
  [chunk _response ^OutputStream out]
  (cond
    (nil? chunk)                       nil
    (instance? byte-array-class chunk) (.write out ^bytes chunk)
    (number? chunk)                    (.write out (int chunk))
    (string? chunk)                    (.write out (.getBytes ^String chunk StandardCharsets/UTF_8))
    (instance? ByteBuffer chunk)       (let [dup   (.duplicate ^ByteBuffer chunk)
                                             len   (.remaining dup)
                                             bytes (byte-array len)]
                                         (.get dup bytes)
                                         (.write out bytes))
    (instance? InputStream chunk)      (io/copy chunk out)
    (sequential? chunk)                (doseq [part chunk]
                                         (write-fallback-body-to-stream! part nil out))
    :else                              (throw (ex-info "Unsupported response chunk" {:type (class chunk)}))))

#_{:clj-kondo/ignore [:unresolved-namespace]}
(compile-if
 (do
   (require '[ring.core.protocols :as ring-protocols])
   true)
 (do
   (defn- streamable-response-body?
     [chunk]
     (satisfies? ring.core.protocols/StreamableResponseBody chunk))
   (defn- write-body-to-stream!
     [chunk response ^OutputStream out]
     (ring.core.protocols/write-body-to-stream chunk response out)))
 (do
   (defn- streamable-response-body?
     [_]
     false)
   (defn- write-body-to-stream!
     [chunk response ^OutputStream out]
     (write-fallback-body-to-stream! chunk response out))))

(defn- commit-final!
  [^Request req write-resp committed_ {:keys [body status] :as response} final?]
  (when-not (and (some? status) (final-status? status))
    (throw (ex-info "Final response status must be >= 200" {:status status})))
  (let [response' (cond-> response
                    (nil? status) (assoc :status 200)
                    final? with-content-length)
        head (dissoc response' :body)
        [headers headers-len content-length] (build-headers head)]
    (when (compare-and-set! committed_ nil head)
      (schedule-start-response!
       req (:status head) headers headers-len content-length (get-compress-hint response') write-resp)
      {:head head
       :body body})))

(defn- write-body-chunk!
  [chunk ^OutputStream out response close-after?]
  (if close-after?
    (write-body-to-stream! chunk response out)
    (cond
      (nil? chunk)                       nil
      (instance? byte-array-class chunk) (.write out ^bytes chunk)
      (number? chunk)                    (.write out (int chunk))
      (string? chunk)                    (.write out (.getBytes ^String chunk StandardCharsets/UTF_8))
      (instance? ByteBuffer chunk)       (let [dup   (.duplicate ^ByteBuffer chunk)
                                               len   (.remaining dup)
                                               bytes (byte-array len)]
                                           (.get dup bytes)
                                           (.write out bytes))
      (instance? InputStream chunk)      (io/copy chunk out)
      (sequential? chunk)                (doseq [part chunk]
                                           (write-body-chunk! part out response false))
      (streamable-response-body? chunk)  (throw (ex-info "StreamableResponseBody chunk requires close-after? true" {:type (class chunk)}))
      :else                              (throw (ex-info "Unsupported response chunk" {:type (class chunk)})))))

(deftype H2OResponseEmitter [^Request req
                             write-resp
                             committed_
                             close-cb_
                             closed?_]
  p/ResponseEmitter
  (open? [_]
    (and (not @closed?_)
         (not (.get ^AtomicBoolean (:stopped?_ write-resp)))))
  (committed? [_]
    (boolean @committed_))
  (emit! [this data]
    (p/emit! this data {}))
  (emit! [this data {:keys [close-after?]
                     :or   {close-after? false}}]

    (let [os ^OutputStream (:out-stream write-resp)]
      (cond
        ;; we are closed
        (not (p/open? this)) false
        ;; map data, possibly with headers/status
        (map? data)
        (let [status   (:status data)
              resp-map (assoc data :status status)]
          (if (informational-status? status)
            (when (nil? @committed_)
              (send-informational! req resp-map)
              true)
            (when (nil? @committed_)
              (when-let [{:keys [head body]} (commit-final! req write-resp committed_ resp-map close-after?)]
                (reset! committed_ head)
                (when body
                  (write-body-chunk! body os head true))
                (when close-after?
                  (p/close this))
                true))))
        ;; body data
        :else
        (let [head (if-let [c @committed_]
                     c
                     (when-let [{:keys [head]} (commit-final! req write-resp committed_ {:status 200 :headers {}} close-after?)]
                       (reset! committed_ head)))]
          (when head
            (write-body-chunk! data os head close-after?)
            (when close-after? (p/close this))
            true)))))
  (flush [_]
    (.flush ^OutputStream (:out-stream write-resp)))
  (close [this]
    (if (not (p/open? this))
      false
      (do
        (.close ^OutputStream (:out-stream write-resp))
        true)))
  (on-close [_ cb]
    (swap! close-cb_ conj cb)))

(defn new-response-emitter [req]
  (let [write-resp (response-queue/create-response-writer req)
        committed_ (atom nil)
        closed?_   (atom false)
        close-cb_  (atom nil)]
    (H2OResponseEmitter. req write-resp committed_ close-cb_ closed?_)))

(defn stop-emitter
  "Stop right away, the connection is closing/closed."
  [^H2OResponseEmitter emitter]
  (pi/stop (.write-resp emitter))
  (p/close emitter))

(defn send-ring-response!
  [emitter ring-resp]
  (try
    (if (satisfies? p/ResponseEmitter (:body ring-resp))
      (when (:status ring-resp)
        (p/emit! emitter (dissoc ring-resp :body) {:close-after? false}))
      (p/emit! emitter ring-resp {:close-after? true}))
    (catch Throwable e
      e
      (.printStackTrace e)))
  nil)
