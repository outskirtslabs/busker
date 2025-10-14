(ns ol.h2o.response
  "Response handling for h2o HTTP server."
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.h2o.evloop :as evloop]
   [ol.h2o.native :as h2o]
   [ol.h2o.protocols.content-length :as content-length]
   [ol.h2o.util :as util]
   [ring.core.protocols :as ring-protocols])
  (:import
   [java.io OutputStream]
   [java.lang.foreign Arena MemorySegment ValueLayout]
   [java.nio.channels Channels WritableByteChannel]
   [java.nio ByteBuffer]
   [java.util.concurrent Semaphore]
   [ol.h2o.protocols Request]))

(set! *warn-on-reflection* true)

(def default-output-buffer-size
  "How much body data in bytes accumulates before writing to the network"
  32768)
(def default-output-aggregation-size
  "A per-write threshold in bytes. Writes <= this size are copied into the aggregation buffer; writes >= this size flush the buffer and are sent directly (bypass copy)"
  8192)

(defn create-write-res-channel
  "Creates a WritableByteChannel for streaming response body."
  [req evloop-system {:keys [output-aggregation-size output-buffer-size]
                      :or {output-buffer-size default-output-buffer-size
                           output-aggregation-size default-output-aggregation-size}}]
  (let [proceed-sem (Semaphore. 1 true)
        close-complete-sem (Semaphore. 0 true)
        final-chunk-pending? (atom false)
        closed? (atom false)
        final-sent? (atom false)
        error (atom nil)
        n-bytes-sent (atom 0)
        arena (Arena/ofAuto)
        ;; Array of sendvec structs for vectorized sends (max 2: buffer + large write)
        send-vec-array-seg (mem/alloc (* 2 (mem/size-of ::h2o/h2o-sendvec-t)) arena)
        aggregation-buffer (ByteBuffer/allocateDirect output-buffer-size)
        ;; keep reference to native segments that are in flight so they
        ;; aren't GCed until libh2o is finished with them (signaled by on-proceed)
        in-flight-segments (atom [])
        empty-seg (mem/alloc 0 arena)
        on-proceed-callback
        (mem/serialize
         (fn [_ctx-ptr]
           (try
             (reset! in-flight-segments [])
             (.release proceed-sem)
             (when @final-chunk-pending?
               (let [agg-pos (.position aggregation-buffer)]
                 (if (pos? agg-pos)
                   (do
                     (.flip aggregation-buffer)
                     (let [stable-seg (mem/alloc agg-pos arena)]
                       (MemorySegment/copy (MemorySegment/ofBuffer aggregation-buffer) 0
                                           stable-seg 0 agg-pos)
                       (swap! in-flight-segments conj [{:seg stable-seg :len agg-pos}])
                       (h2o/sendvec-init-raw send-vec-array-seg stable-seg agg-pos)
                       (h2o/sendvec (-> req :req-ctx :req) send-vec-array-seg 1
                                    h2o/H2O_SEND_STATE_FINAL))
                     (.clear aggregation-buffer))
                   ;; No buffered data - send empty final
                   (do
                     (h2o/sendvec-init-raw send-vec-array-seg empty-seg 0)
                     (h2o/sendvec (-> req :req-ctx :req) send-vec-array-seg 1
                                  h2o/H2O_SEND_STATE_FINAL))))
               (reset! final-sent? true)
               (reset! closed? true)
               (.release close-complete-sem))
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

        send-vecs-internal!
        (fn [vecs vec-count is-final]

          (swap! in-flight-segments conj vecs)
          (evloop/send-msg! evloop-system
                            [:h2o/sendvec
                             (fn []
                               (doseq [[idx {:keys [seg len]}] (map-indexed vector vecs)]
                                 (let [offset (* idx (mem/size-of ::h2o/h2o-sendvec-t))
                                       vec-seg (mem/slice send-vec-array-seg offset (mem/size-of ::h2o/h2o-sendvec-t))]
                                   (h2o/sendvec-init-raw vec-seg seg len)))
                               (try
                                 (h2o/sendvec (-> req :req-ctx :req) send-vec-array-seg vec-count
                                              (if is-final
                                                h2o/H2O_SEND_STATE_FINAL
                                                h2o/H2O_SEND_STATE_IN_PROGRESS))
                                 (when is-final
                                   (reset! closed? true))
                                 (catch Exception e
                                   (println e)
                                   (.release proceed-sem)
                                   (reset! error e))))]))

        flush-buffer!
        (fn [is-final]
          (let [pos (.position aggregation-buffer)]
            (if (pos? pos)
              ;; Buffer has data: copy to stable segment and send
              (do
                (.flip aggregation-buffer)
                (let [stable-seg (mem/alloc pos arena)]
                  (MemorySegment/copy (MemorySegment/ofBuffer aggregation-buffer) 0
                                      stable-seg 0 pos)
                  (send-vecs-internal! [{:seg stable-seg :len pos}] 1 is-final))
                (.clear aggregation-buffer))
              ;; Buffer is empty but we need to send final marker
              (when is-final
                (send-vecs-internal! [{:seg empty-seg :len 0}] 1 true)))))

        body-channel
        (reify
          WritableByteChannel
          (write [_ src]
            (when @closed? (throw (java.nio.channels.ClosedChannelException.)))
            (when @error (throw @error))
            (let [chunk-size (.remaining src)]
              (if (zero? chunk-size)
                0
                (if (>= chunk-size output-aggregation-size)
                  ;; Large write: send buffer + large write in one vectorized call
                  (do
                    (.acquire proceed-sem)
                    (let [agg-buffer-pos (.position aggregation-buffer)
                          chunk (byte-array chunk-size)
                          chunk-seg (mem/alloc chunk-size arena)]
                      (.get src chunk)
                      (MemorySegment/copy chunk 0 chunk-seg ValueLayout/JAVA_BYTE 0 chunk-size)
                      (if (pos? agg-buffer-pos)
                        ;; Send both buffer and large write as 2-vec array using stable copy
                        (do
                          (.flip aggregation-buffer)
                          (let [buffer-stable-seg (mem/alloc agg-buffer-pos arena)]
                            (MemorySegment/copy (MemorySegment/ofBuffer aggregation-buffer) 0
                                                buffer-stable-seg 0 agg-buffer-pos)
                            (send-vecs-internal! [{:seg buffer-stable-seg :len agg-buffer-pos}
                                                  {:seg chunk-seg :len chunk-size}]
                                                 2 false))
                          (.clear aggregation-buffer)
                          (swap! n-bytes-sent + agg-buffer-pos))
                        ;; Only large write, no buffer data
                        (send-vecs-internal! [{:seg chunk-seg :len chunk-size}
                                              {:seg empty-seg :len 0}] 1 false))
                      (swap! n-bytes-sent + chunk-size)
                      chunk-size))
                  ;; Small write: accumulate in buffer
                  (let [pos (.position aggregation-buffer)
                        new-pos (+ pos chunk-size)]
                    (if (>= new-pos output-buffer-size)
                      ;; Buffer would overflow: flush first, then buffer this write
                      (do
                        (.acquire proceed-sem)
                        (flush-buffer! false)
                        (.put aggregation-buffer src)
                        (swap! n-bytes-sent + chunk-size)
                        chunk-size)
                      ;; Room in buffer: accumulate
                      (do
                        (.put aggregation-buffer src)
                        (swap! n-bytes-sent + chunk-size)
                        chunk-size)))))))

          (isOpen [_] (not @closed?))

          (close [_]
            (when-not @closed?
              (when-not @final-sent?
                (if (.tryAcquire proceed-sem)
                  (do
                    (flush-buffer! true)
                    (reset! closed? true))
                  (do
                    (reset! final-chunk-pending? true)
                    (.acquire close-complete-sem))))
              (when-not @closed?
                (reset! closed? true)))))]

    {:channel body-channel
     :on-proceed on-proceed-callback
     :to-output-stream (fn [] (Channels/newOutputStream body-channel))
     :on-stop on-stop-callback}))

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
        {:keys [to-output-stream on-proceed on-stop]} (create-write-res-channel req evloop-system {})]
    (h2o/start-response req-ctx-ptr status headers headers-len content-length on-proceed on-stop)
    (let [out-stream ^OutputStream (to-output-stream)]
      (if body
        (ring-protocols/write-body-to-stream body ring-resp out-stream)
        (.close out-stream)))))
