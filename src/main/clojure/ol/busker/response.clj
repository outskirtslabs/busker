(ns ^:no-doc ol.busker.response
  "Response handling for h2o HTTP server."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as p]
   [ol.busker.protocols.content-length]
   [ol.busker.response-head :as response-head]
   [ol.busker.response-queue :as response-queue]
   [ol.busker.util :refer [compile-if]]
   [ol.busker.util.headers :as hdr.util]
   [taoensso.trove :as trove])
  (:import
   [java.io InputStream OutputStream]
   [java.nio ByteBuffer]
   [java.nio.charset Charset StandardCharsets]
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

(defn- expand-header-values
  [headers]
  (mapcat (fn [[name value]]
            (if (sequential? value)
              (map #(vector name %) value)
              [[name value]]))
          headers))

;; TODO intern common header names?

(defn build-headers2
  "Build lowercase Java header pairs for an informational response.
  Returns `[response headers header-count content-length]`."
  [resp]
  (let [content-length-val (second (hdr.util/find-header resp "content-length"))
        content-length (if content-length-val
                         (coerce-content-length content-length-val)
                         -1)
        filtered-headers (if content-length-val
                           (dissoc-header (:headers resp) "content-length")
                           (:headers resp))
        header-pairs (->> (expand-header-values filtered-headers)
                          (mapv (fn [[name value]]
                                  [(str/lower-case (str name)) (str value)])))]
    [resp header-pairs (count header-pairs) content-length]))

(defn build-headers
  "Build Java header pairs for a final response.
  Returns `[headers header-count content-length]`."
  [resp]
  (let [content-length-val (second (hdr.util/find-header resp "content-length"))
        content-length (if content-length-val
                         (coerce-content-length content-length-val)
                         -1)
        filtered-headers (if content-length-val
                           (dissoc-header (:headers resp) "content-length")
                           (:headers resp))
        header-pairs (->> (expand-header-values filtered-headers)
                          (mapv (fn [[name value]]
                                  [(str name) (str value)])))]
    [header-pairs (count header-pairs) content-length]))

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

(defn- final-status? [^long status]
  (>= status 200))

(defn- schedule-start-response!
  [^Request req status headers content-length compress-hint]
  (pi/send-msg
   (:worker req)
   [:h2o/start-response
    (response-head/start-command (:dispatch-module-id req)
                                 (:dispatch-request-seq req)
                                 status
                                 headers
                                 content-length
                                 compress-hint)]))

(defn- schedule-informational!
  [^Request req status headers]
  (pi/send-msg
   (:worker req)
   [:h2o/send-informational
    (response-head/informational-command (:dispatch-module-id req)
                                         (:dispatch-request-seq req)
                                         status
                                         headers)]))

(defn- send-informational!
  [^Request req resp]
  (when (:body resp)
    (throw (ex-info "Body payloads are not allowed for 1xx informational responses" {:status (:status resp)})))
  (let [status (:status resp)
        headers (or (:headers resp) {})
        [_ header-pairs _ _] (build-headers2 {:headers headers})]
    (schedule-informational! req status header-pairs)))

(defn get-compress-hint [resp]
  (get h2o/->compress-hint (:h2o/compress-hint resp) h2o/H2O_COMPRESS_HINT_ENABLE))

(defn- string-body-bytes
  [^String body response]
  (try
    (let [^Charset charset (Charset/forName (or (hdr.util/get-charset response) "utf-8"))]
      (.getBytes body charset))
    (catch IllegalArgumentException _
      (.getBytes body StandardCharsets/UTF_8))))

(defn- write-fallback-body-to-stream!
  [chunk response ^OutputStream out]
  (cond
    (nil? chunk)                       nil
    (instance? byte-array-class chunk) (.write out ^bytes chunk)
    (number? chunk)                    (.write out (int chunk))
    (string? chunk)                    (.write out ^bytes (string-body-bytes chunk response))
    (instance? ByteBuffer chunk)       (let [dup   (.duplicate ^ByteBuffer chunk)
                                             len   (.remaining dup)
                                             bytes (byte-array len)]
                                         (.get dup bytes)
                                         (.write out bytes))
    (instance? InputStream chunk)      (io/copy chunk out)
    (sequential? chunk)                (doseq [part chunk]
                                         (write-fallback-body-to-stream! part response out))
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

(defn- fixed-final-body-bytes
  [body]
  (cond
    (string? body)                     (.getBytes ^String body StandardCharsets/UTF_8)
    (instance? byte-array-class body) (aclone ^bytes body)
    :else                              nil))

(defn- utf8-charset?
  [charset]
  (try
    (= StandardCharsets/UTF_8 (Charset/forName charset))
    (catch IllegalArgumentException _
      false)))

(def ^:private missing-content-length (Object.))

(defn- append-fixed-header
  [headers name value]
  (let [name (str name)]
    (if (sequential? value)
      (reduce (fn [result item]
                (conj result [name (str item)]))
              headers
              value)
      (conj headers [name (str value)]))))

(defn- fixed-final-header-data
  [headers]
  (reduce-kv
   (fn [[result content-length] name value]
     (if (.equalsIgnoreCase ^String (str name) "content-length")
       [result (if (identical? missing-content-length content-length)
                 value
                 content-length)]
       [(append-fixed-header result name value) content-length]))
   [[] missing-content-length]
   (or headers {})))

(defn- fixed-final-command
  [^Request req response final?]
  (let [status (:status response)
        body (:body response)
        output-buffer-size (get-in req [:config :output-buffer-size])
        [headers content-length-value] (fixed-final-header-data (:headers response))
        charset (hdr.util/get-charset response)]
    (when (and final?
               (some? status)
               (final-status? status)
               (not (contains? #{204 304} status))
               (nat-int? output-buffer-size)
               (or (string? body) (instance? byte-array-class body))
               (or (not (string? body))
                   (nil? charset)
                   (utf8-charset? charset)))
      (let [header-staging-bytes (fixed-final/header-staging-bytes headers)]
        (when (and (<= (count headers) fixed-final/max-header-pairs)
                   (<= header-staging-bytes fixed-final/max-header-staging-bytes))
          (let [body-bytes (fixed-final-body-bytes body)]
            (when (<= (alength ^bytes body-bytes) output-buffer-size)
              (let [content-length (if (identical? missing-content-length content-length-value)
                                     (alength ^bytes body-bytes)
                                     (coerce-content-length content-length-value))
                    compress-hint (get-compress-hint response)
                    compress-hint (if (and (= h2o/H2O_COMPRESS_HINT_ENABLE compress-hint)
                                           (< (alength ^bytes body-bytes)
                                              (or (get-in req [:config :compress-min-size]) 0)))
                                    h2o/H2O_COMPRESS_HINT_DISABLE
                                    compress-hint)]
                (fixed-final/prepared-command (:dispatch-module-id req)
                                              (:dispatch-request-seq req)
                                              status
                                              headers
                                              header-staging-bytes
                                              content-length
                                              compress-hint
                                              body-bytes)))))))))

(defn- schedule-fixed-final!
  [^Request req command]
  (pi/send-msg (:worker req) [:h2o/send-fixed-final command]))

(defn- response-writer
  [write-resp]
  (if (delay? write-resp) @write-resp write-resp))

(defn- created-response-writer
  [write-resp]
  (if (delay? write-resp)
    (when (realized? write-resp) @write-resp)
    write-resp))

(defn- commit-final!
  [^Request req write-resp committed_ {:keys [body status] :as response} final?]
  (let [response' (cond-> response
                    (nil? status) (assoc :status 200))]
    (when-not (final-status? (:status response'))
      (throw (ex-info "Final response status must be >= 200" {:status (:status response')})))
    (let [command (fixed-final-command req response' final?)
          response' (if command
                      (if (hdr.util/get-header response' "content-length")
                        response'
                        (hdr.util/header response' "content-length"
                                         (fixed-final/command-content-length command)))
                      (cond-> response'
                        final? with-content-length))
          head (dissoc response' :body)]
      (when (compare-and-set! committed_ nil head)
        (if (and command (schedule-fixed-final! req command))
          (do
            (when-let [writer (created-response-writer write-resp)]
              (pi/stop writer))
            {:head head :body nil})
          (let [[headers _header-count content-length] (build-headers head)]
            (response-writer write-resp)
            (schedule-start-response!
             req (:status head) headers content-length
             (get-compress-hint response'))
            {:head head :body body}))))))

(defn- write-body-chunk!
  [chunk ^OutputStream out response close-after?]
  (if close-after?
    (write-body-to-stream! chunk response out)
    (cond
      (nil? chunk)                       nil
      (instance? byte-array-class chunk) (.write out ^bytes chunk)
      (number? chunk)                    (.write out (int chunk))
      (string? chunk)                    (.write out ^bytes (string-body-bytes chunk response))
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
    (let [writer (created-response-writer write-resp)]
      (and (= :open (:phase @lifecycle_))
           (or (nil? writer)
               (not (.get ^AtomicBoolean (:stopped?_ writer)))))))
  (committed? [_]
    (boolean @committed_))
  (emit! [this data]
    (p/emit! this data {}))
  (emit! [this data {:keys [close-after?]
                     :or   {close-after? false}}]

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
                (write-body-chunk! body (:out-stream (response-writer write-resp)) head true))
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
          (write-body-chunk! data (:out-stream (response-writer write-resp)) head close-after?)
          (when close-after? (p/close this))
          true))))
  (flush [_]
    (.flush ^OutputStream (:out-stream (response-writer write-resp))))
  (close [_]
    (let [writer (or (created-response-writer write-resp)
                     (when (nil? @committed_)
                       (response-writer write-resp)))]
      (if (and (or (nil? writer)
                   (not (.get ^AtomicBoolean (:stopped?_ writer))))
               (begin-close! lifecycle-lock lifecycle_))
        (try
          (when writer
            (.close ^OutputStream (:out-stream writer)))
          true
          (finally
            (terminal! lifecycle-lock lifecycle_ callback-tail_ callback-dispatch)))
        (if (and writer (.get ^AtomicBoolean (:stopped?_ writer)))
          (do
            (terminal! lifecycle-lock lifecycle_ callback-tail_ callback-dispatch)
            false)
          false))))
  (on-close [_ callback]
    (register-close-callback! lifecycle-lock lifecycle_ callback-tail_ callback-dispatch callback)))

(defn ^:no-doc writer-if-created
  [value]
  (created-response-writer
   (if (instance? H2OResponseEmitter value)
     (.write-resp ^H2OResponseEmitter value)
     value)))

(defn new-response-emitter
  ([req]
   (new-response-emitter req start-virtual-thread!))
  ([req callback-dispatch]
   (let [write-resp (delay (response-queue/create-response-writer req))
         committed_ (atom nil)
         lifecycle-lock (Object.)
         lifecycle_ (atom {:phase :open :callbacks []})
         callback-tail_ (atom (promise))]
     (deliver @callback-tail_ true)
     (H2OResponseEmitter. req write-resp committed_ lifecycle-lock lifecycle_ callback-dispatch callback-tail_))))

(defn stop-emitter
  "Stops an initialized response writer before dispatching emitter close callbacks."
  [^H2OResponseEmitter emitter]
  (when-let [writer (created-response-writer (.write-resp emitter))]
    (pi/stop writer))
  (terminal! (.lifecycle-lock emitter)
             (.lifecycle_ emitter)
             (.callback-tail_ emitter)
             (.callback-dispatch emitter)))

(defn- response-emitter-body?
  [body]
  (and (some? body)
       (not (string? body))
       (not (instance? byte-array-class body))
       (satisfies? p/ResponseEmitter body)))

(defn send-ring-response!
  [emitter ring-resp]
  (try
    (if (response-emitter-body? (:body ring-resp))
      (when (:status ring-resp)
        (p/emit! emitter (dissoc ring-resp :body) {:close-after? false}))
      (p/emit! emitter ring-resp {:close-after? true}))
    (catch Throwable e
      e
      (.printStackTrace e)))
  nil)