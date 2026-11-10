(ns ^:no-doc ol.busker.response
  "Adapts Ring responses and implements [[ol.busker.protocols/ResponseEmitter]].

  Handlers and producers run on virtual threads. This module validates options,
  normalizes headers and content length, encodes body data and dispatches ordered
  close callbacks. Strings use the response charset with a UTF-8 fallback;
  ByteBuffers are copied through duplicates without changing caller positions.
  Blocking streams, sequences and Ring body writers publish incrementally.

  Eligible complete responses use [[ol.busker.fixed-final]] slots. An ordinary
  Ring return on that path needs no streaming controller or public receipt.
  Informational output and other bodies use [[ol.busker.response-queue]], which
  accepts bounded operations atomically and sends them on the platform worker.
  Close callbacks run on virtual threads; receipt settlement and native storage
  release are separate events."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as p]
   [ol.busker.protocols.content-length]
   [ol.busker.response-head :as response-head]
   [ol.busker.response-queue :as response-queue]
   [ol.busker.response-serialization :as serialization]
   [ol.busker.util :refer [compile-if]]
   [ol.busker.util.headers :as hdr.util]
   [taoensso.trove :as trove])
  (:import
   [java.io InputStream OutputStream]
   [java.nio ByteBuffer CharBuffer]
   [java.nio.charset Charset CharsetEncoder CodingErrorAction StandardCharsets]
   [ol.busker.internal.protocols Request]))

(set! *warn-on-reflection* true)

(def ^:private byte-array-class (Class/forName "[B"))

(defn dissoc-header
  "Removes all case variations of `header-name`."
  [headers ^String header-name]
  (into {} (remove (fn [[k _]] (.equalsIgnoreCase header-name (str k)))) headers))

(defn- coerce-content-length [value]
  (if (number? value) (long value) (Long/parseLong value)))

(defn- expand-header-values [headers]
  (mapcat (fn [[name value]]
            (if (sequential? value)
              (map #(vector name %) value)
              [[name value]]))
          headers))

(defn build-headers
  "Returns `[headers header-count content-length]` for a final response.
  Expands sequential values into separate pairs and converts names and values to
  strings. Removes content-length from the pairs; absent length is `-1`."
  [response]
  (let [length (second (hdr.util/find-header response "content-length"))
        headers (cond-> (:headers response) length (dissoc-header "content-length"))
        pairs (mapv (fn [[name value]] [(str name) (str value)])
                    (expand-header-values headers))]
    [pairs (count pairs) (if length (coerce-content-length length) -1)]))

(defn build-headers2
  "Returns `[response headers header-count content-length]` for an informational
  response, with lowercase header names and the normalization of [[build-headers]]."
  [response]
  (let [[headers n length] (build-headers response)]
    [response (mapv (fn [[name value]] [(str/lower-case name) value]) headers) n length]))

(defn get-compress-hint [response]
  (get h2o/->compress-hint (:h2o/compress-hint response) h2o/H2O_COMPRESS_HINT_ENABLE))

(defn- response-charset ^Charset [response]
  (try
    (Charset/forName (or (hdr.util/get-charset response) "utf-8"))
    (catch IllegalArgumentException _ StandardCharsets/UTF_8)))

(defn- encoder ^CharsetEncoder [response]
  (doto (.newEncoder (response-charset response))
    (.onMalformedInput CodingErrorAction/REPLACE)
    (.onUnmappableCharacter CodingErrorAction/REPLACE)))

(defn- encoded-length [^String value response]
  (if (= StandardCharsets/UTF_8 (response-charset response))
    (serialization/utf8-length value)
    (let [encoder (encoder response)
          chars (CharBuffer/wrap value)
          bytes (ByteBuffer/allocate 8192)]
      (loop [total 0 flushing? false]
        (let [result (if flushing? (.flush encoder bytes) (.encode encoder chars bytes true))
              total (Math/addExact (long total) (long (.position bytes)))]
          (.clear bytes)
          (cond
            (.isError result) (do (.throwException result) total)
            (.isOverflow result) (recur total flushing?)
            flushing? total
            :else (recur total true)))))))

(defn- known-length [body response]
  (cond
    (nil? body) 0
    (string? body) (encoded-length body response)
    (instance? byte-array-class body) (alength ^bytes body)
    (instance? ByteBuffer body) (.remaining ^ByteBuffer body)
    (fn? body) nil
    :else (p/body-size-in-bytes body response)))

(defn with-content-length [response]
  (if (hdr.util/get-header response "content-length")
    response
    (if-let [length (known-length (:body response) response)]
      (hdr.util/header response "content-length" (str length))
      response)))

(defn- write-string! [^String body response ^OutputStream out]
  (let [encoder (encoder response)
        chars (CharBuffer/wrap body)
        bytes (ByteBuffer/allocate 8192)]
    (loop [flushing? false]
      (let [result (if flushing? (.flush encoder bytes) (.encode encoder chars bytes true))]
        (when (.isError result) (.throwException result))
        (.write out (.array bytes) 0 (.position bytes))
        (.clear bytes)
        (cond
          (.isOverflow result) (recur flushing?)
          (not flushing?) (recur true))))))

(defn- write-byte-buffer! [^ByteBuffer body ^OutputStream out]
  (let [source (.duplicate body)
        bytes (byte-array (min 8192 (.remaining source)))]
    (while (.hasRemaining source)
      (let [n (min (alength bytes) (.remaining source))]
        (.get source bytes 0 n)
        (.write out bytes 0 n)))))

#_{:clj-kondo/ignore [:unresolved-namespace]}
(compile-if
 (do (require '[ring.core.protocols :as ring-protocols]) true)
 (do
   (defn- streamable-response-body? [body]
     (satisfies? ring.core.protocols/StreamableResponseBody body))
   (defn- write-ring-body! [body response out]
     (ring.core.protocols/write-body-to-stream body response out)))
 (do
   (defn- streamable-response-body? [_] false)
   (defn- write-ring-body! [body _ _]
     (throw (ex-info "Unsupported response body" {:type (class body)})))))

(defn- write-body! [body response ^OutputStream out]
  (cond
    (nil? body) nil
    (string? body) (write-string! body response out)
    (instance? byte-array-class body) (.write out ^bytes body)
    (instance? ByteBuffer body) (write-byte-buffer! body out)
    (instance? InputStream body) (io/copy body out)
    (number? body) (.write out (int body))
    (sequential? body) (doseq [part body] (write-body! part response out))
    :else (write-ring-body! body response out)))

(defn- write-adapted-body! [body response out complete?]
  (if (and complete? (streamable-response-body? body)
           (not (or (string? body) (instance? byte-array-class body)
                    (instance? ByteBuffer body) (nil? body))))
    (write-ring-body! body response out)
    (write-body! body response out)))

(defn- bounded-body? [body]
  (or (nil? body) (string? body) (instance? byte-array-class body)
      (instance? ByteBuffer body) (fn? body)))

(defn- supported-body? [body]
  (or (bounded-body? body) (number? body) (instance? InputStream body)
      (sequential? body) (streamable-response-body? body)))

(defn- bounded-producer [body response]
  (cond
    (fn? body) body
    (nil? body) (fn [_])
    (string? body)
    (fn [^ByteBuffer buffer]
      (let [encoder (encoder response)
            result (.encode encoder (CharBuffer/wrap ^String body) buffer true)]
        (when (.isError result) (.throwException result))
        (when (.isOverflow result) (throw (java.nio.BufferOverflowException.)))
        (let [result (.flush encoder buffer)]
          (when (.isError result) (.throwException result))
          (when (.isOverflow result) (throw (java.nio.BufferOverflowException.))))))
    (instance? byte-array-class body) (fn [^ByteBuffer buffer] (.put buffer ^bytes body))
    :else (fn [^ByteBuffer buffer] (.put buffer (.duplicate ^ByteBuffer body)))))

(defn- options [opts body buffer-size]
  (when-not (map? opts)
    (throw (ex-info "Emitter options must be a map" {:options opts})))
  (doseq [key (keys opts)]
    (when-not (contains? #{:wait? :flush? :end? :capacity} key)
      (throw (ex-info "Unknown emitter option" {:option key}))))
  (doseq [key [:wait? :flush? :end?]]
    (when (and (contains? opts key) (not (boolean? (get opts key))))
      (throw (ex-info "Emitter option must be boolean" {:option key :value (get opts key)}))))
  (if (fn? body)
    (when-not (and (integer? (:capacity opts)) (pos? (:capacity opts))
                   (<= (:capacity opts) buffer-size))
      (throw (ex-info "Producer capacity must be a positive integer no larger than output-buffer-size"
                      {:capacity (:capacity opts) :output-buffer-size buffer-size})))
    (when (contains? opts :capacity)
      (throw (ex-info "Capacity is only valid for a producer function" {}))))
  (merge {:wait? true :flush? false :end? false} opts))

(defn- informational? [status]
  (and (<= 100 status 199) (not= 101 status)))

(defn- normalized-head [response length informational?]
  (let [response (assoc response :status (or (:status response) 200))
        response (if (and (some? length) (not informational?)
                          (not (hdr.util/get-header response "content-length")))
                   (hdr.util/header response "content-length" (str length))
                   response)
        [headers n content-length] (build-headers response)
        headers (if informational?
                  (mapv (fn [[name value]] [(str/lower-case name) value]) headers)
                  headers)]
    (when (or (> n fixed-final/max-header-pairs)
              (> (fixed-final/header-staging-bytes headers) fixed-final/max-header-staging-bytes))
      (throw (ex-info "Response headers exceed bounded header storage" {:header-count n})))
    {:status (:status response) :headers headers :content-length (if (= 205 (:status response)) 0 content-length)
     :compress-hint (get-compress-hint response) :response (dissoc response :body)}))

(defn- apply-head! [req head informational?]
  (let [{:keys [status headers content-length compress-hint]} head
        module-id (:dispatch-module-id req)
        request-seq (:dispatch-request-seq req)]
    (if informational?
      (response-head/execute-informational!
       req (response-head/informational-command module-id request-seq status headers))
      (response-head/execute-start!
       req (response-head/start-command module-id request-seq status headers content-length compress-hint)))))

(defn- run-close-callbacks! [callbacks]
  (doseq [callback callbacks]
    (try (callback)
         (catch Throwable t
           (try (trove/log! {:level :error :id ::close-callback-failed :ex t})
                (catch Throwable _ nil))))))

(defn- reserve-close-callbacks! [callback-tail_ callbacks]
  (when (seq callbacks)
    (let [complete (promise)
          previous (swap-vals! callback-tail_ (constantly complete))]
      (reify Runnable
        (run [_]
          (try
            @(first previous)
            (run-close-callbacks! callbacks)
            (finally (deliver complete true))))))))

(defn- dispatch-close-callback! [dispatch task]
  (when task
    (try (dispatch task)
         (catch Throwable _ (Thread/startVirtualThread task)))))

(defn- terminal! [lock state_ tail_ dispatch]
  (let [[changed? task]
        (locking lock
          (if (= :closed (:phase @state_))
            [false nil]
            (let [task (reserve-close-callbacks! tail_ (:callbacks @state_))]
              (swap! state_ assoc :phase :closed :callbacks [])
              (when-let [receipt (:last-receipt @state_)]
                (deliver receipt {:status :closed}))
              (.notifyAll ^Object lock)
              [true task])))]
    (dispatch-close-callback! dispatch task)
    changed?))

(defn- acquire-producer! [lock state_ wait?]
  (locking lock
    (loop []
      (let [{:keys [phase producer]} @state_]
        (cond
          (= producer (Thread/currentThread))
          (throw (ex-info "Recursive emit! on the same response is not supported" {}))
          (= :closed phase) :closed
          (nil? producer) (do (swap! state_ assoc :producer (Thread/currentThread)) :accepted)
          (not wait?) :full
          :else (do (.wait ^Object lock) (recur)))))))

(defn- release-producer! [lock state_]
  (locking lock
    (swap! state_ assoc :producer nil)
    (.notifyAll ^Object lock)))

(defn- wake-slot-waiters! [req]
  (when-let [monitor (get-in req [:worker :response-slot-monitor])]
    (locking monitor (.notifyAll ^Object monitor))))

(declare emit-value! close-emitter!)

(deftype H2OResponseEmitter [^Request req writer_ lifecycle-lock lifecycle_ callback-dispatch callback-tail_]
  p/ResponseEmitter
  (open? [_]
    (and (= :open (:phase @lifecycle_))
         (if-let [writer @writer_] (response-queue/open? writer) true)))
  (committed? [_]
    (if-let [writer @writer_]
      (response-queue/committed? writer)
      (some? (:committed @lifecycle_))))
  (emit! [this data] (p/emit! this data {}))
  (emit! [this data opts] (emit-value! this data opts))
  (flush! [_]
    (if-let [writer @writer_]
      (response-queue/flush! writer)
      (:last-receipt @lifecycle_)))
  (close! [this] (close-emitter! this))
  (on-close! [_ callback]
    (let [task (locking lifecycle-lock
                 (if (= :closed (:phase @lifecycle_))
                   (reserve-close-callbacks! callback-tail_ [callback])
                   (do (swap! lifecycle_ update :callbacks conj callback) nil)))]
      (dispatch-close-callback! callback-dispatch task))))

(defn writer-if-created [value]
  (if (instance? H2OResponseEmitter value)
    @(.writer_ ^H2OResponseEmitter value)
    (if (delay? value) (when (realized? value) @value) value)))

(defn- ensure-writer! [^H2OResponseEmitter emitter]
  (let [lock (.lifecycle-lock emitter)]
    (locking lock
      (when (= :open (:phase @(.lifecycle_ emitter)))
        (or @(.writer_ emitter)
            (let [req (.req emitter)
                  writer (response-queue/create-response-writer
                          (assoc req
                                 :apply-head! #(apply-head! req %1 %2)
                                 :on-terminal! #(do
                                                  (terminal! (.lifecycle-lock emitter) (.lifecycle_ emitter)
                                                             (.callback-tail_ emitter) (.callback-dispatch emitter))
                                                  (wake-slot-waiters! req))))]
              (reset! (.writer_ emitter) writer)))))))

(defn close-emitter! [^H2OResponseEmitter emitter]
  (let [lock (.lifecycle-lock emitter)
        changed?
        (locking lock
          (if (= :closed (:phase @(.lifecycle_ emitter)))
            false
            (if-let [writer (ensure-writer! emitter)]
              (response-queue/close! writer)
              false)))]
    (wake-slot-waiters! (.req emitter))
    changed?))

(defn new-response-emitter
  ([req] (new-response-emitter req Thread/startVirtualThread))
  ([req dispatch]
   (let [tail (promise)]
     (deliver tail true)
     (H2OResponseEmitter. req (atom nil) (Object.)
                          (atom {:phase :open :callbacks [] :producer nil}) dispatch (atom tail)))))

(defn stop-emitter
  "Stops admission and settles receipts without releasing native in-flight storage."
  [^H2OResponseEmitter emitter]
  (let [lock (.lifecycle-lock emitter)]
    (locking lock
      (when-let [writer @(.writer_ emitter)]
        (response-queue/on-stop writer nil))
      (terminal! (.lifecycle-lock emitter) (.lifecycle_ emitter)
                 (.callback-tail_ emitter) (.callback-dispatch emitter))))
  (wake-slot-waiters! (.req emitter)))

(defn- fixed-plan [req head body length]
  (fixed-final/direct-response-plan
   (long (:dispatch-module-id req)) (long (:dispatch-request-seq req))
   (long (:status head)) (:headers head)
   (long (fixed-final/header-utf8-bytes (:headers head))) body (long length)
   (long (:content-length head)) (long (:compress-hint head))))

(defn- fixed-producer! [^H2OResponseEmitter emitter head producer capacity opts]
  (let [req (.req emitter)
        lock (.lifecycle-lock emitter)
        receipt (promise)
        result (fixed-final/publish-direct-producer!
                (:worker req) (fixed-plan req head nil 0) capacity producer (:wait? opts)
                #(not (p/open? emitter))
                (fn [publish!]
                  (locking lock
                    (if (p/open? emitter)
                      (do
                        (swap! (.lifecycle_ emitter) assoc :committed head :last-receipt receipt)
                        (publish!)
                        (terminal! (.lifecycle-lock emitter) (.lifecycle_ emitter)
                                   (.callback-tail_ emitter) (.callback-dispatch emitter))
                        :accepted)
                      :closed))))]
    (case result
      :accepted {:status :accepted :progress receipt}
      :overloaded {:status :full}
      :closed {:status :closed})))

(defn emit-value! [^H2OResponseEmitter emitter data raw-opts]
  (if-not (p/open? emitter)
    {:status :closed}
    (let [req (.req emitter)
          response? (map? data)
          body? (or (not response?) (contains? data :body))
          body (if response? (:body data) data)
          buffer-size (get-in req [:config :output-buffer-size])
          opts (options raw-opts body buffer-size)
          status (if response? (or (:status data) 200) 200)
          _ (when-not (and (integer? status) (<= 100 status 999) (not= 101 status))
              (throw (ex-info "Unsupported response status" {:status status})))
          info? (informational? status)
          _ (when (and info? (or body? (:end? opts)))
              (throw (ex-info "Informational responses reject a body and end?" {:status status})))
          _ (when (and body? (not (supported-body? body)))
              (throw (ex-info "Unsupported response body" {:type (class body)})))
          _ (when (and body? (not (:wait? opts)) (not (bounded-body? body)))
              (throw (ex-info "Nonblocking output requires a bounded body" {:type (class body)})))
          current-head (when-let [writer @(.writer_ emitter)] (response-queue/committed-head writer))
          validation-response (if response? data (or (:response current-head) {:status 200}))
          validation-length (when (and body? (not (fn? body))) (known-length body validation-response))
          _ (when (and (not (:wait? opts)) validation-length (> validation-length buffer-size))
              (throw (ex-info "Nonblocking body exceeds output-buffer-size"
                              {:bytes validation-length :output-buffer-size buffer-size})))
          _ (when response?
              (normalized-head data (when (:end? opts) validation-length) info?))
          _ (when (and response? (not info?) (p/committed? emitter))
              (throw (ex-info "Final response headers have already been accepted" {})))
          grant (acquire-producer! (.lifecycle-lock emitter) (.lifecycle_ emitter) (:wait? opts))]
      (if (not= :accepted grant)
        {:status grant}
        (try
          (let [writer @(.writer_ emitter)
                committed (if writer (response-queue/committed-head writer) (:committed @(.lifecycle_ emitter)))
                _ (when (and response? (not info?) committed)
                    (throw (ex-info "Final response headers have already been accepted" {})))
                response (if response? data (or (:response committed) {:status 200 :headers {}}))
                length (when (and body? (not (fn? body))) (known-length body response))
                capacity (if (fn? body) (:capacity opts) (or length 0))
                _ (when (and (not (:wait? opts)) (> capacity buffer-size))
                    (throw (ex-info "Nonblocking body exceeds output-buffer-size"
                                    {:bytes capacity :output-buffer-size buffer-size})))
                head (when (or response? (nil? committed))
                       (normalized-head response (when (:end? opts) length) info?))
                effective-head (or head committed (normalized-head response nil false))
                opts (assoc opts :body? body? :informational? info?
                            :suppress-body? (or (= :head (:request-method req))
                                                (contains? #{204 205 304} (:status effective-head))))]
            (if (and (fn? body) (:end? opts) (nil? writer) (not info?)
                     (<= capacity fixed-final/response-ring-max-body-bytes)
                     (not (:suppress-body? opts)))
              (fixed-producer! emitter effective-head body capacity opts)
              (if-let [writer (ensure-writer! emitter)]
                (if (and (bounded-body? body) (<= capacity buffer-size))
                  (response-queue/emit-bounded! writer head capacity (bounded-producer body response) opts)
                  (response-queue/emit-incremental! writer head
                                                    #(write-adapted-body! body response % (:end? opts)) opts))
                {:status :closed})))
          (finally (release-producer! (.lifecycle-lock emitter) (.lifecycle_ emitter))))))))

(defn- ordinary-fixed-plan [req response]
  (let [body (:body response)
        status (or (:status response) 200)
        b (get-in req [:config :output-buffer-size])]
    (when (and (integer? status) (<= 200 status 999) (not (contains? #{204 304} status))
               (or (instance? byte-array-class body)
                   (and (string? body) (= StandardCharsets/UTF_8 (response-charset response)))))
      (let [length (if (string? body)
                     (serialization/utf8-length body)
                     (alength ^bytes body))]
        (when (<= length (min b fixed-final/response-ring-max-body-bytes))
          (let [head (normalized-head response length false)
                head (if (and (= h2o/H2O_COMPRESS_HINT_ENABLE (:compress-hint head))
                              (< length (or (get-in req [:config :compress-min-size]) 0)))
                       (assoc head :compress-hint h2o/H2O_COMPRESS_HINT_DISABLE)
                       head)]
            [head (fixed-plan req head body length)]))))))

(defn send-ring-response! [^H2OResponseEmitter emitter response]
  (let [lock (.lifecycle-lock emitter)
        body (:body response)]
    (try
      (if (and body (not (string? body)) (not (instance? byte-array-class body))
               (satisfies? p/ResponseEmitter body))
        (when (:status response) (p/emit! emitter (dissoc response :body)))
        (if-let [[head plan] (when (and (p/open? emitter) (nil? @(.writer_ emitter)))
                               (ordinary-fixed-plan (.req emitter) response))]
          (when (= :accepted (acquire-producer! (.lifecycle-lock emitter) (.lifecycle_ emitter) true))
            (try
              (if (and (p/open? emitter) (nil? @(.writer_ emitter)))
                (fixed-final/publish-direct-response!
                 (:worker (.req emitter)) plan #(not (p/open? emitter))
                 (fn [publish!]
                   (locking lock
                     (if (p/open? emitter)
                       (do
                         (swap! (.lifecycle_ emitter) assoc :committed head)
                         (publish!)
                         (terminal! (.lifecycle-lock emitter) (.lifecycle_ emitter)
                                    (.callback-tail_ emitter) (.callback-dispatch emitter))
                         :accepted)
                       :closed))))
                (when (p/open? emitter)
                  (response-queue/emit-incremental!
                   (ensure-writer! emitter) (normalized-head response nil false)
                   #(write-adapted-body! (:body response) response % true)
                   {:wait? true :end? true :body? true})))
              (finally (release-producer! (.lifecycle-lock emitter) (.lifecycle_ emitter)))))
          (p/emit! emitter response {:end? true})))
      (catch Throwable error
        (when-let [writer (ensure-writer! emitter)]
          (response-queue/fail! writer error {:if-open? true}))
        (trove/log! {:level :error :id ::response-failed :ex error})))
    nil))
