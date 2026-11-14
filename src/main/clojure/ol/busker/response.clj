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
   [java.util Arrays Iterator]
   [clojure.lang MapEntry RT]
   [ol.busker.internal.protocols Request]
   [ol.busker.response_queue BlockOutput]))

(set! *warn-on-reflection* true)

(def ^:private byte-array-class (Class/forName "[B"))

(defn- invalid-response
  "Returns an exception for a response that Busker cannot send as the handler
  gave it. [[send-ring-response!]] answers it with a 500 when no head is sent."
  [message data]
  (ex-info message (assoc data ::invalid-response true)))

(defn- coerce-content-length [value]
  (let [length (cond
                 (number? value) (long value)
                 (string? value) (try (Long/parseLong value) (catch NumberFormatException _ -1))
                 :else -1)]
    (when (neg? length)
      (throw (invalid-response "Invalid content-length header" {:content-length value})))
    length))

(defn get-compress-hint [response]
  (get h2o/->compress-hint (:h2o/compress-hint response) h2o/H2O_COMPRESS_HINT_ENABLE))

(defn- response-charset ^Charset [response]
  (try
    (Charset/forName (or (hdr.util/get-charset response) "utf-8"))
    (catch IllegalArgumentException _ StandardCharsets/UTF_8)))

(defn- encodable ^Charset [^Charset charset]
  (when-not (.canEncode charset)
    (throw (invalid-response "Response charset cannot encode" {:charset (.name charset)})))
  charset)

(defn- encoder ^CharsetEncoder [response]
  (doto (.newEncoder (encodable (response-charset response)))
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

(defn- spill-size
  "Returns a byte count that holds the encoding of any one character, a
  surrogate pair or a final flush of `encoder`."
  ^long [^CharsetEncoder encoder]
  (max 16 (* 2 (long (Math/ceil (.maxBytesPerChar encoder))))))

(defn- write-string! [^String body response ^BlockOutput out]
  ;; Encoder output goes straight into each block. When a block cannot hold one
  ;; encoded character, that character is encoded into `spill` and copied over.
  (when (pos? (.length body))
    (let [encoder (encoder response)
          chars (CharBuffer/wrap body)
          ^ByteBuffer spill (.flip (ByteBuffer/allocate (spill-size encoder)))
          phase_ (volatile! :encode)
          encode! (fn [^ByteBuffer target]
                    (loop []
                      (let [result (if (= :flush @phase_)
                                     (.flush encoder target)
                                     (.encode encoder chars target true))]
                        (when (.isError result) (.throwException result))
                        (cond
                          (.isOverflow result) nil
                          (= :flush @phase_) (vreset! phase_ :done)
                          :else (do (vreset! phase_ :flush) (recur))))))
          drain! (fn [^ByteBuffer buffer]
                   (let [n (int (min (.remaining buffer) (.remaining spill)))]
                     (.put buffer (.slice spill (.position spill) n))
                     (.position spill (unchecked-add-int (.position spill) n))))]
      (while (or (.hasRemaining spill) (not= :done @phase_))
        (.fillBlock out
                    (fn [^ByteBuffer buffer]
                      (drain! buffer)
                      (when-not (or (.hasRemaining spill) (= :done @phase_))
                        (encode! buffer)
                        (when (and (zero? (.position buffer)) (not= :done @phase_))
                          (.clear spill)
                          (encode! spill)
                          (.flip spill)
                          (drain! buffer)))))))))

(defn- write-byte-buffer! [^ByteBuffer body ^BlockOutput out]
  (let [source (.duplicate body)]
    (while (.hasRemaining source)
      (.fillBlock out
                  (fn [^ByteBuffer buffer]
                    (let [n (int (min (.remaining buffer) (.remaining source)))]
                      (.put buffer (.slice source (.position source) n))
                      (.position source (unchecked-add-int (.position source) n))))))))

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

(defn- with-capacity
  "Returns `pairs`, or a larger copy when it has fewer than `needed` slots."
  ^objects [^objects pairs ^long needed]
  (if (<= needed (alength pairs))
    pairs
    (Arrays/copyOf pairs (int (max needed (* 2 (alength pairs)))))))

(defn- put-header-pairs!
  "Writes the pairs for `header-name` into `pairs` from index `n`, one per item
  when `value` is sequential."
  [^objects pairs ^long n header-name value]
  (if (sequential? value)
    (let [^Iterator values (RT/iter value)]
      (loop [i n]
        (when (.hasNext values)
          (aset pairs i (MapEntry/create header-name (str (.next values))))
          (recur (inc i)))))
    (aset pairs n (MapEntry/create header-name (str value)))))

;; Boxed constants, so comparing them with a boxed status allocates nothing.
(def ^:private no-content-status 204)
(def ^:private reset-content-status 205)
(def ^:private not-modified-status 304)

(defn- build-head
  "Returns the head of `response` from one pass over its headers.

  Pairs keep header order. Names and values pass through `str`; each item of a
  sequential value becomes its own pair; informational heads use lowercase names.
  Content-length headers in any letter case stay out of the pairs. The first
  one with a value gives the content length, otherwise `length` does for a final
  head, otherwise it is `-1`. Status 205 always has content length 0 and status
  204 never has one (RFC 9110, section 8.6).
  `:header-bytes` is the UTF-8 byte count of the pair names and values.
  `compress-hint` is stored as given. `retain-response?` stores the response
  without its body under `:response`, for later body operations that need its
  headers."
  [response length informational? retain-response? compress-hint]
  (let [status (or (:status response) 200)
        headers (:headers response)
        ^Iterator entries (RT/iter headers)]
    (loop [pairs (object-array (count headers))
           n 0
           header-length nil]
      (if (.hasNext entries)
        (let [entry (.next entries)
              header-name (key entry)
              value (val entry)]
          (if (.equalsIgnoreCase "content-length" ^String header-name)
            (recur pairs n (if (nil? header-length) value header-length))
            (let [needed (+ n (if (sequential? value) (long (count value)) 1))
                  pairs (with-capacity pairs needed)]
              (put-header-pairs! pairs n
                                 (if informational? (str/lower-case (str header-name)) (str header-name))
                                 value)
              (recur pairs needed header-length))))
        (let [pairs (cond
                      (zero? n) []
                      (= n (alength pairs)) (vec pairs)
                      :else (vec (Arrays/copyOf pairs (int n))))
              content-length (cond
                               (some? header-length) (coerce-content-length header-length)
                               (and (some? length) (not informational?)) (long length)
                               :else -1)
              header-bytes (when-not (> n fixed-final/max-header-pairs)
                             (serialization/header-bytes pairs))]
          (when (or (nil? header-bytes)
                    (> (serialization/staging-bytes n header-bytes)
                       fixed-final/max-header-staging-bytes))
            (throw (ex-info "Response headers exceed bounded header storage" {:header-count n})))
          {:status status :headers pairs :header-bytes header-bytes
           :content-length (cond
                             (= reset-content-status status) 0
                             (= no-content-status status) -1
                             :else content-length)
           :compress-hint compress-hint
           :response (when retain-response? (-> response (assoc :status status) (dissoc :body)))})))))

(defn- check-known-length!
  "Throws when final `head` declares a content length other than `length`, the
  length of the body. Call it only when the two differ. Statuses 204, 205 and
  304 and a HEAD response with an empty body are not checked: no body goes out,
  and the header describes another response (RFC 9110, section 8.6)."
  [head length request-method]
  (let [status (:status head)]
    (when-not (or (= no-content-status status)
                  (= reset-content-status status)
                  (= not-modified-status status)
                  (and (zero? (long length)) (= :head request-method)))
      (throw (invalid-response "Response content-length does not match its body"
                               {:content-length (:content-length head) :body-length length})))))

(defn- normalized-head [response length informational?]
  (build-head response length informational? true (get-compress-hint response)))

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
           (try (trove/log! {:level :error :id ::close-callback-failed :error t})
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

(defn- close-lifecycle!
  "Marks the lifecycle in `state_` closed under `lock`, settles the last receipt
  and wakes lock waiters. Returns the reserved close-callback task, or nil when
  there are no callbacks or the lifecycle was already closed.

  Every write to `state_` happens under `lock`, so a read followed by `reset!`
  cannot lose an update."
  [lock state_ tail_]
  (locking lock
    (let [state @state_]
      (when-not (= :closed (:phase state))
        (let [callbacks (:callbacks state)
              task (reserve-close-callbacks! tail_ callbacks)]
          (reset! state_ (cond-> (assoc state :phase :closed)
                           (seq callbacks) (assoc :callbacks [])))
          (when-let [receipt (:last-receipt state)]
            (deliver receipt {:status :closed}))
          (.notifyAll ^Object lock)
          task)))))

(defn- terminal! [lock state_ tail_ dispatch]
  (dispatch-close-callback! dispatch (close-lifecycle! lock state_ tail_)))

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
  (fixed-final/notify-capacity! (:worker req)))

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

(def ^:private completed-callback-tail
  "The callback tail of an emitter that has queued no close callbacks. Tails are
  only dereferenced and replaced, never delivered again, so every emitter starts
  from this one delivered promise."
  (doto (promise) (deliver true)))

(def ^:private open-lifecycle
  {:phase :open :callbacks [] :producer nil})

(defn new-response-emitter
  ([req] (new-response-emitter req Thread/startVirtualThread))
  ([req dispatch]
   (H2OResponseEmitter. req (atom nil) (Object.) (atom open-lifecycle) dispatch
                        (atom completed-callback-tail))))

(defn- stop-open-emitter
  [^H2OResponseEmitter emitter]
  (let [lock (.lifecycle-lock emitter)]
    (locking lock
      (when-let [writer @(.writer_ emitter)]
        (response-queue/on-stop writer nil))
      (terminal! (.lifecycle-lock emitter) (.lifecycle_ emitter)
                 (.callback-tail_ emitter) (.callback-dispatch emitter))))
  (wake-slot-waiters! (.req emitter)))

(defn stop-emitter
  "Stops admission and settles receipts without releasing native in-flight storage."
  [^H2OResponseEmitter emitter]
  ;; A closed emitter without a writer is left unchanged by the locked path:
  ;; :closed is terminal, a writer is only created while the lifecycle is open,
  ;; and closing a closed lifecycle does nothing. Its slot waiters were woken
  ;; when it closed: stop-emitter and close-emitter! wake them, and a fixed
  ;; publication closes it from its only producer, which has already claimed.
  ;; So an unlocked read that sees both is final, and nothing is left to do.
  (when-not (and (= :closed (:phase @(.lifecycle_ emitter)))
                 (nil? @(.writer_ emitter)))
    (stop-open-emitter emitter)))

(defn- fixed-plan [req head body length]
  (fixed-final/direct-response-plan
   (long (:dispatch-module-id req)) (long (:dispatch-request-seq req))
   (long (:status head)) (:headers head)
   (long (:header-bytes head)) body (long length)
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
                        (reset! (.lifecycle_ emitter)
                                (-> @(.lifecycle_ emitter)
                                    (assoc :committed head)
                                    (assoc :last-receipt receipt)))
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
              (let [head (normalized-head data (when (:end? opts) validation-length) info?)]
                (when (and (:end? opts) (some? validation-length)
                           (not (== (long (:content-length head)) (long validation-length))))
                  (check-known-length! head validation-length (:request-method (:req-ctx req))))))
          ;; Strings check their charset when their length is computed.
          _ (when (sequential? body) (encodable (response-charset validation-response)))
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
                            :suppress-body? (or (= :head (:request-method (:req-ctx req)))
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

(defn- fixed-body
  "Returns `body` in a form a fixed slot takes, or `::ineligible`. A string in a
  response charset other than UTF-8 becomes its encoded bytes when it has at
  most `limit` chars."
  [body response ^long limit]
  (cond
    (or (nil? body) (instance? byte-array-class body) (instance? ByteBuffer body)) body
    (not (string? body)) ::ineligible
    :else (let [charset (response-charset response)]
            (cond
              (= StandardCharsets/UTF_8 charset) body
              (<= (.length ^String body) limit) (.getBytes ^String body (encodable charset))
              :else ::ineligible))))

(defn- ordinary-fixed-plan [req response]
  (let [status (or (:status response) 200)
        config (:config req)
        limit (min (:output-buffer-size config) fixed-final/response-ring-max-body-bytes)]
    (when (and (integer? status) (<= 200 status 999))
      (let [body (fixed-body (:body response) response limit)
            length (cond
                     (nil? body) 0
                     (string? body) (serialization/utf8-length body)
                     (instance? byte-array-class body) (alength ^bytes body)
                     (instance? ByteBuffer body) (.remaining ^ByteBuffer body)
                     :else -1)]
        (when (<= 0 length limit)
          (let [hint (get-compress-hint response)
                hint (if (and (= h2o/H2O_COMPRESS_HINT_ENABLE hint)
                              (< length (or (:compress-min-size config) 0)))
                       h2o/H2O_COMPRESS_HINT_DISABLE
                       hint)
                ;; Without a body, the length a GET or a 200 would carry is unknown.
                unknown-length? (and (nil? body)
                                     (or (= not-modified-status status)
                                         (= :head (:request-method (:req-ctx req)))))
                head (build-head response (when-not unknown-length? length) false false hint)]
            (when-not (== (long (:content-length head)) (long length))
              (check-known-length! head length (:request-method (:req-ctx req))))
            [head (fixed-plan req head body length)]))))))

(def ^:private internal-error-response
  {:status 500
   :headers {"content-type" "text/plain; charset=utf-8"}
   :body "Internal Server Error"})

(defn send-ring-response! [^H2OResponseEmitter emitter response]
  (let [lock (.lifecycle-lock emitter)
        body (:body response)]
    (try
      ;; satisfies? allocates several KiB when it misses, so complete bodies skip it.
      (if (and body (not (string? body)) (not (instance? byte-array-class body))
               (not (instance? ByteBuffer body))
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
        ;; The 500 cannot fail this way, so this recursion ends.
        (if (and (::invalid-response (ex-data error))
                 (p/open? emitter) (not (p/committed? emitter)))
          (send-ring-response! emitter internal-error-response)
          (when-let [writer (ensure-writer! emitter)]
            (response-queue/fail! writer error {:if-open? true})))
        (trove/log! {:level :error :id ::response-failed :error error})))
    nil))
