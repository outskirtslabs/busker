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
   [ol.busker.util.headers :as hdr.util]
   [taoensso.trove :as trove])
  (:import
   [java.io InputStream OutputStream]
   [java.lang.foreign MemorySegment]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent.atomic AtomicBoolean]
   [ol.busker.internal.protocols Request]))

(set! *warn-on-reflection* true)

(def ^:private clj-header-size (mem/size-of ::h2o/clj-header-t))
(def ^:private clj-header-name-offset (mem/struct-field-offset ::h2o/clj-header-t :name))
(def ^:private clj-header-name-len-offset (mem/struct-field-offset ::h2o/clj-header-t :name_len))
(def ^:private clj-header-value-offset (mem/struct-field-offset ::h2o/clj-header-t :value))
(def ^:private clj-header-value-len-offset (mem/struct-field-offset ::h2o/clj-header-t :value_len))

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

(defn- expand-header-values
  [headers]
  (mapcat (fn [[name value]]
            (if (sequential? value)
              (map #(vector name %) value)
              [[name value]]))
          headers))

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
        header-pairs (vec (expand-header-values filtered-headers))
        headers-count (count header-pairs)]

    (if (zero? headers-count)
      [(mem/as-segment 0) 0 content-length]

      (let [total-size (* headers-count clj-header-size)
            headers-seg (mem/alloc total-size)]

        (doseq [[idx [name-key value-val]] (map-indexed vector header-pairs)]
          (let [offset (* idx clj-header-size)
                ;; h2 and h3 require lower case headers
                name-str (str/lower-case (if (string? name-key) name-key (str name-key)))
                value-str (if (string? value-val) value-val (str value-val))
                name-ptr ^MemorySegment (mem/serialize name-str ::mem/c-string)
                value-ptr ^MemorySegment (mem/serialize value-str ::mem/c-string)]
            (mem/write-address headers-seg (+ offset clj-header-name-offset) name-ptr)
            (mem/write-int headers-seg (+ offset clj-header-name-len-offset) (dec (.byteSize name-ptr)))
            (mem/write-address headers-seg (+ offset clj-header-value-offset) value-ptr)
            (mem/write-int headers-seg (+ offset clj-header-value-len-offset) (dec (.byteSize value-ptr)))))
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
        header-pairs (vec (expand-header-values filtered-headers))
        headers-count (count header-pairs)]

    (if (zero? headers-count)
      [(mem/as-segment 0) 0 content-length]

      (let [total-size (* headers-count clj-header-size)
            headers-seg (mem/alloc total-size)]

        (doseq [[idx [name-key value-val]] (map-indexed vector header-pairs)]
          (let [offset (* idx clj-header-size)
                name-str (if (string? name-key) name-key (str name-key))
                value-str (if (string? value-val) value-val (str value-val))
                name-ptr ^MemorySegment (mem/serialize name-str ::mem/c-string)
                value-ptr ^MemorySegment (mem/serialize value-str ::mem/c-string)]
            (mem/write-address headers-seg (+ offset clj-header-name-offset) name-ptr)
            (mem/write-int headers-seg (+ offset clj-header-name-len-offset) (dec (.byteSize name-ptr)))
            (mem/write-address headers-seg (+ offset clj-header-value-offset) value-ptr)
            (mem/write-int headers-seg (+ offset clj-header-value-len-offset) (dec (.byteSize value-ptr)))))
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
                (:dispatch-module-id req)
                (:dispatch-request-seq req)
                (fn [live-req]
                  (h2o/start-response (:req-ctx-ptr live-req) status
                                      headers headers-len
                                      content-length compress-hint
                                      (:on-proceed-cb-ptr write-resp)
                                      (:on-stop-cb-ptr write-resp)))]))

(defn- schedule-informational!
  [^Request req status headers headers-len]
  (pi/send-msg (:worker req)
               [:h2o/send-informational
                (:dispatch-module-id req)
                (:dispatch-request-seq req)
                (fn [live-req]
                  (h2o/send-informational (:req-ctx-ptr live-req)
                                          status headers headers-len))]))

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

(defn- start-virtual-thread!
  [^Runnable task]
  (Thread/startVirtualThread task))

(defn- run-close-callbacks!
  [callbacks]
  (doseq [callback callbacks]
    (try
      (callback)
      (catch Throwable t
        (trove/log! {:level :error :id ::close-callback-failed :ex t})))))

(defn- reserve-close-callbacks!
  [callback-tail_ callbacks]
  (when (seq callbacks)
    (let [complete (promise)
          task (reify Runnable
                 (run [_]
                   (try
                     (run-close-callbacks! callbacks)
                     (finally
                       (deliver complete true)))))]
      (loop []
        (let [previous @callback-tail_
              chained-task (reify Runnable
                             (run [_]
                               (try
                                 @previous
                                 (.run task)
                                 (finally
                                   (deliver complete true)))))]
          (if (compare-and-set! callback-tail_ previous complete)
            chained-task
            (recur)))))))

(defn- dispatch-close-callback!
  [callback-dispatch task]
  (when task
    (try
      (callback-dispatch task)
      (catch Throwable _
        (start-virtual-thread! task)))))

(defn- terminal!
  [lifecycle-lock lifecycle_ callback-tail_ callback-dispatch]
  (let [[changed? task]
        (locking lifecycle-lock
          (let [{:keys [phase callbacks]} @lifecycle_]
            (if (= :closed phase)
              [false nil]
              (let [task (reserve-close-callbacks! callback-tail_ callbacks)]
                (reset! lifecycle_ {:phase :closed :callbacks []})
                [true task]))))]
    (dispatch-close-callback! callback-dispatch task)
    changed?))

(defn- begin-close!
  [lifecycle-lock lifecycle_]
  (locking lifecycle-lock
    (let [{:keys [phase] :as lifecycle} @lifecycle_]
      (when (= :open phase)
        (reset! lifecycle_ (assoc lifecycle :phase :closing))
        true))))

(defn- register-close-callback!
  [lifecycle-lock lifecycle_ callback-tail_ callback-dispatch callback]
  (let [task
        (locking lifecycle-lock
          (let [{:keys [phase] :as lifecycle} @lifecycle_]
            (if (= :closed phase)
              (reserve-close-callbacks! callback-tail_ [callback])
              (do
                (reset! lifecycle_ (update lifecycle :callbacks conj callback))
                nil))))]
    (dispatch-close-callback! callback-dispatch task)))

(deftype H2OResponseEmitter [^Request req
                             write-resp
                             committed_
                             lifecycle-lock
                             lifecycle_
                             callback-dispatch
                             callback-tail_]
  p/ResponseEmitter
  (open? [_]
    (and (= :open (:phase @lifecycle_))
         (not (.get ^AtomicBoolean (:stopped?_ write-resp)))))
  (committed? [_]
    (boolean @committed_))
  (emit! [this data]
    (p/emit! this data {}))
  (emit! [this data {:keys [close-after?]
                     :or   {close-after? false}}]

    (let [os ^OutputStream (:out-stream write-resp)]
      (cond
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
  (close [_]
    (if (and (not (.get ^AtomicBoolean (:stopped?_ write-resp)))
             (begin-close! lifecycle-lock lifecycle_))
      (try
        (.close ^OutputStream (:out-stream write-resp))
        true
        (finally
          (terminal! lifecycle-lock lifecycle_ callback-tail_ callback-dispatch)))
      (if (.get ^AtomicBoolean (:stopped?_ write-resp))
        (do
          (terminal! lifecycle-lock lifecycle_ callback-tail_ callback-dispatch)
          false)
        false)))
  (on-close [_ callback]
    (register-close-callback! lifecycle-lock lifecycle_ callback-tail_ callback-dispatch callback)))

(defn new-response-emitter
  ([req]
   (new-response-emitter req start-virtual-thread!))
  ([req callback-dispatch]
   (let [write-resp (response-queue/create-response-writer req)
         committed_ (atom nil)
         lifecycle-lock (Object.)
         lifecycle_ (atom {:phase :open :callbacks []})
         callback-tail_ (atom (promise))]
     (deliver @callback-tail_ true)
     (H2OResponseEmitter. req write-resp committed_ lifecycle-lock lifecycle_ callback-dispatch callback-tail_))))

(defn stop-emitter
  "Stops the response writer before dispatching emitter close callbacks."
  [^H2OResponseEmitter emitter]
  (pi/stop (.write-resp emitter))
  (terminal! (.lifecycle-lock emitter)
             (.lifecycle_ emitter)
             (.callback-tail_ emitter)
             (.callback-dispatch emitter)))

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