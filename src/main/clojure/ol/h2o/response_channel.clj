(ns ol.h2o.response-channel
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.h2o.internal.protocols :as p]
   [ol.h2o.native :as h2o])
  (:import
   [java.lang.foreign Arena MemorySegment ValueLayout]
   [java.nio ByteBuffer]
   [java.nio.channels Channels WritableByteChannel]
   [java.util.concurrent Semaphore]))

(set! *warn-on-reflection* true)

(def default-output-buffer-size
  "How much body data in bytes accumulates before writing to the network"
  32768)
(def default-output-aggregation-size
  "A per-write threshold in bytes. Writes <= this size are copied into the aggregation buffer; writes >= this size flush the buffer and are sent directly (bypass copy)"
  8192)

(defn create-write-res-channel
  "Creates a WritableByteChannel for streaming response body."
  [req {:keys [output-aggregation-size output-buffer-size]
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

        signal-close-complete!
        (fn []
          (reset! final-sent? true)
          (reset! closed? true)
          (.release close-complete-sem))

        handle-error!
        (fn [e & {:keys [message release-proceed?]
                  :or {message nil release-proceed? false}}]
          (when message (println message e))
          (when-not message (println e))
          (reset! error e)
          (when release-proceed? (.release proceed-sem))
          (when @final-chunk-pending? (.release close-complete-sem)))

        acquire-with-wake (fn []
                            (when-not (.tryAcquire proceed-sem)
                              (p/wake (:worker req))
                              (.acquire proceed-sem)))

        on-proceed-cb (fn [_ctx-ptr]
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
                                  (.clear aggregation-buffer)
                                  (swap! n-bytes-sent + agg-pos))
                   ;; No buffered data - send empty final
                                (do
                                  (h2o/sendvec-init-raw send-vec-array-seg empty-seg 0)
                                  (h2o/sendvec (-> req :req-ctx :req) send-vec-array-seg 1
                                               h2o/H2O_SEND_STATE_FINAL))))
                            (signal-close-complete!))
                          (catch Exception e
                            (handle-error! e :message "Error in on-proceed callback"))))
        on-proceed-cb-ptr (mem/serialize on-proceed-cb [::ffi/fn [::mem/pointer] ::mem/void])

        on-stop-cb (fn [_ctx-ptr reason]
                     (try
                       (reset! closed? true)
                       (reset! error (ex-info "Response generator stopped" {:reason reason}))
                       (.release proceed-sem)
                       (when @final-chunk-pending? (.release close-complete-sem))
                       (catch Exception e
                         (handle-error! e :message "on-stop callback error" :release-proceed? false))))
        on-stop-cb-ptr (mem/serialize on-stop-cb [::ffi/fn [::mem/pointer ::mem/int] ::mem/void])

        send-vecs-internal!
        (fn [vecs vec-count is-final]

          (swap! in-flight-segments conj vecs)
          (p/send-msg (:worker req)
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
                             (handle-error! e :release-proceed? true))))]))

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
                (.clear aggregation-buffer)
                pos)
              ;; Buffer is empty but we need to send final marker
              (do
                (when is-final
                  (send-vecs-internal! [{:seg empty-seg :len 0}] 1 true))
                0))))

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
                    (acquire-with-wake)
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
                        (send-vecs-internal! [{:seg chunk-seg :len chunk-size}] 1 false))
                      (swap! n-bytes-sent + chunk-size)
                      chunk-size))
                  ;; Small write: accumulate in buffer
                  (let [pos (.position aggregation-buffer)
                        new-pos (+ pos chunk-size)]
                    (if (>= new-pos output-buffer-size)
                      ;; Buffer would overflow: flush first, then buffer this write
                      (do
                        (acquire-with-wake)
                        (let [flushed-bytes (flush-buffer! false)]
                          (swap! n-bytes-sent + flushed-bytes))
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
                    (p/wake (:worker req))
                    (.acquire close-complete-sem))))
              (when-not @closed?
                (reset! closed? true)))))]

    {::on-proceed-cb    on-proceed-cb
     ::on-stop-cb       on-stop-cb
     ::channel          body-channel
     :cancel            (fn [] (reset! closed? true))
     :out-stream        (Channels/newOutputStream body-channel)
     :on-proceed-cb-ptr on-proceed-cb-ptr
     :on-stop-cb-ptr    on-stop-cb-ptr}))
