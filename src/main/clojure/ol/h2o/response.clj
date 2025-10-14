(ns ol.h2o.response
  "Response handling for h2o HTTP server."
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.h2o.evloop :as evloop]
   [ol.h2o.native :as h2o]
   [ol.h2o.protocols :as protocols :refer [WriteRes]]
   [ol.h2o.protocols.content-length :as content-length]
   #_[ol.h2o.streaming-output :as streaming]
   [ol.h2o.util :as util]
   [ring.core.protocols :as ring-protocols])
  (:import
   [java.lang.foreign Arena MemorySegment ValueLayout]
   [java.nio.channels Channels WritableByteChannel]
   [java.util.concurrent Semaphore]
   [ol.h2o.protocols Request]))

(set! *warn-on-reflection* true)

(defn create-write-res-channel
  "Creates a WritableByteChannel for streaming response body."
  [req content-length evloop-system]
  (let [proceed-sem          (Semaphore. 1 true)
        close-complete-sem   (Semaphore. 0 true)
        final-chunk-pending? (atom false)
        closed?              (atom false)
        content-sent         (atom 0)
        final-sent?          (atom false)
        error                (atom nil)
        arena                (Arena/ofAuto)
        ;; reuse a memory seg for the array container
        send-vec-array-seg   (mem/alloc (mem/size-of ::h2o/h2o-sendvec-t) arena)
        on-proceed-callback
        (mem/serialize
         (fn [_ctx-ptr]
           (try
             (.release proceed-sem)
             (when @final-chunk-pending?
               (with-open [scratch (Arena/ofConfined)]
                 (let [chunk-seg (mem/alloc 0 scratch)]
                   (h2o/sendvec-init-raw send-vec-array-seg chunk-seg 0)
                   (h2o/sendvec (-> req :req-ctx :req) send-vec-array-seg 1
                                h2o/H2O_SEND_STATE_FINAL)
                   (reset! final-sent? true)
                   (reset! closed? true)
                   (.release close-complete-sem))))
             (catch Exception e
               (println "Error in on-proceed callback" e)
               (reset! error e)
               (.release close-complete-sem))))
         [::ffi/fn [::mem/pointer] ::mem/void])

        on-stop-callback
        (mem/serialize
         (fn [_ctx-ptr reason]
           (try
             (reset! closed? true)
             (reset! error (ex-info "Response generator stopped" {:reason reason}))
             (.release proceed-sem)
             (catch Exception e
               (println "on-stop callback error" e)
               (reset! error e))))
         [::ffi/fn [::mem/pointer ::mem/int] ::mem/void])

        send-chunk-internal!
        (fn [^bytes chunk is-final]
          (let [len (alength chunk)]
            (evloop/send-msg! evloop-system
                              [:h2o/sendvec
                               (fn []
                                 (with-open [scratch (Arena/ofConfined)]
                                   (let [chunk-seg (mem/alloc len scratch)]
                                     (when (pos? len)
                                       (MemorySegment/copy chunk 0 chunk-seg ValueLayout/JAVA_BYTE 0 len))
                                     (try
                                       (h2o/sendvec-init-raw send-vec-array-seg chunk-seg len)
                                       (h2o/sendvec (-> req :req-ctx :req) send-vec-array-seg 1
                                                    (if is-final
                                                      h2o/H2O_SEND_STATE_FINAL
                                                      h2o/H2O_SEND_STATE_IN_PROGRESS))
                                       (when is-final
                                         (reset! closed? true))
                                       (catch Exception e
                                         (println e)
                                         (.release proceed-sem)
                                         (reset! error e))))))])))

        body-channel
        (reify
          WriteRes
          (output-stream [this]
            (Channels/newOutputStream this))

          WritableByteChannel
          (write [_ src]
            (when @closed? (throw (java.nio.channels.ClosedChannelException.)))
            (when @error (throw @error))
            (let [remaining (.remaining src)]
              (if (zero? remaining)
                0
                (do
                  (.acquire proceed-sem)
                  (let [chunk     (byte-array remaining)
                        is-final? (and (not= -1 content-length)
                                       (>= (+ (count chunk) @content-sent) content-length))]
                    (.get src chunk)
                    (send-chunk-internal! chunk is-final?)
                    (swap! content-sent + (count chunk))
                    remaining)))))

          (isOpen [_]
            (not @closed?))

          (close [_]
            (when-not @closed?
              (when-not @final-sent?
                (if (.tryAcquire proceed-sem)
                  (do
                    (send-chunk-internal! (byte-array 0) true)
                    (reset! closed? true))
                  (do
                    (reset! final-chunk-pending? true)
                    (.acquire close-complete-sem))))
              (when-not @closed?
                (reset! closed? true)))))]

    {:channel    body-channel
     :on-proceed on-proceed-callback
     :on-stop    on-stop-callback}))

(defn dissoc-header
  "Remove all case variations of a header by name (case-insensitive)."
  [headers ^String header-name]
  (let [target-lower  (.toLowerCase header-name java.util.Locale/ROOT)]
    (into {}
          (remove (fn [[k _]]
                    (= (.toLowerCase (str k) java.util.Locale/ROOT) target-lower))
                  headers))))

(defn- coerce-content-length
  [val]
  (if (number? val) (long val)
      (Long/parseLong val)))

(defn build-headers
  "Return a tuple [headers headers-len content-length]
  - headers is a clj_header_t *headers MemorySegment
  - headers-len is size_t (number of headers, aka length of headers array)
  - content-length is the content length header if any was found or -1 (SIZE_MAX)

  headers memorysegment should NOT contain content-length if it was found"
  [resp]
  (let [headers (:headers resp)
        content-length-val (second (util/find-header resp "content-length"))
        content-length (if content-length-val
                         (coerce-content-length content-length-val)
                         -1)
        filtered-headers (if  content-length-val
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
                name-ptr (mem/serialize name-str ::mem/c-string)
                value-ptr (mem/serialize value-str ::mem/c-string)
                header-data {:name name-ptr
                             :name_len (count name-str)
                             :value value-ptr
                             :value_len (count value-str)}
                header-seg (mem/serialize header-data ::h2o/clj-header-t)
                dest-seg (mem/slice headers-seg offset header-size)]
            (mem/copy-segment dest-seg header-seg)))
        [headers-seg headers-count content-length]))))

(defn with-cl-or-te [response]
  (if (util/get-header response "content-length")
    response
    (if-let [size (content-length/body-size-in-bytes (:body response) response)]
      (util/header response "content-length" (str size))
      response)))

(defn send-ring-response!
  "Send a Ring response map using StreamableResponseBody protocol."
  [^Request req ring-resp evloop-system]
  (let [{:keys [status body]
         :as ring-resp} (with-cl-or-te ring-resp)
        [headers headers-len content-length] (build-headers ring-resp)
        req-ctx-ptr (:req-ctx-ptr req)
        {:keys [channel on-proceed on-stop]} (create-write-res-channel req content-length evloop-system)]
    (h2o/start-response req-ctx-ptr status headers headers-len content-length on-proceed on-stop)
    (let [out-stream (protocols/output-stream channel)]
      (if body
        (ring-protocols/write-body-to-stream body ring-resp out-stream)
        (.close out-stream)))))

(comment
  ;; keep around old code
  #_(try
      (let [len (alength bytes)]
        (with-open [scratch (Arena/ofConfined)]
          (let [chunk-seg (mem/alloc len scratch)]
            (when (pos? len)
              (MemorySegment/copy bytes 0 chunk-seg ^ValueLayout$OfByte ValueLayout/JAVA_BYTE 0 len))
            (acquire-permit)
            (when @closed? (println "send chunk got permit but closed"))
            (when (not @closed?)
              (println "sendvec!" (-> req :req-ctx :req))
              (h2o/sendvec-init-raw send-vec-array-seg chunk-seg len)
              (h2o/sendvec (-> req :req-ctx :req) send-vec-array-seg 1  (if #p is-final
                                                                          h2o/H2O_SEND_STATE_FINAL
                                                                          h2o/H2O_SEND_STATE_IN_PROGRESS))
              (when is-final
                (reset! closed? true))))))

      (catch Exception e
        (println e)
        (throw (ex-info "Failed to send response chunk" {} e)))))
