(ns ol.h2o.request
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.h2o.response :as response]
   [ol.h2o.evloop :as evloop]
   [ol.h2o.native :as h2o]
   [ol.h2o.protocols :as protocols :refer [WriteReq]])
  (:import
   [java.nio ByteBuffer]
   [java.nio.channels Channels ReadableByteChannel]
   [java.util.concurrent LinkedBlockingQueue]
   [java.util.concurrent ExecutorService Executors]
   [ol.h2o.protocols Request]))

(set! *warn-on-reflection* true)

(defonce vthread-executor
  (delay (Executors/newVirtualThreadPerTaskExecutor)))

(defn create-write-req-channel
  "Creates a ReadableByteChannel + RequestBody for streaming request body.
   proceed-callback is called after each chunk is fully consumed."
  [proceed-callback]
  (let [queue (LinkedBlockingQueue. 1)
        eof-marker ::eof
        closed? (volatile! false)
        current-buf (volatile! nil)
        body-channel
        (reify
          WriteReq
          (add-chunk [_ chunk is-last]
            (when-not @closed?

              (when chunk
                (.put queue chunk))
              (when is-last
                (.put queue eof-marker))))

          (input-stream [this]
            (Channels/newInputStream this))

          ReadableByteChannel
          (read [_ dst]
            (when @closed?
              (throw (java.nio.channels.ClosedChannelException.)))

            ;; Load next chunk if current is exhausted
            (when (or (nil? @current-buf)
                      (not (.hasRemaining ^ByteBuffer @current-buf)))
              (let [chunk (.take queue)]
                (if (identical? chunk eof-marker)
                  (vreset! closed? true)
                  (vreset! current-buf (ByteBuffer/wrap chunk)))))

            (if @closed?
              -1
              ;; Copy from current buffer to destination
              (let [buf ^ByteBuffer @current-buf
                    initial-pos (.position dst)
                    to-copy (min (.remaining dst) (.remaining buf))]
                (when (pos? to-copy)
                  ;; Transfer bytes
                  (let [old-limit (.limit buf)]
                    (.limit buf (+ (.position buf) to-copy))
                    (.put dst buf)
                    (.limit buf old-limit))

                  ;; Request next chunk when this one is consumed
                  (when-not (.hasRemaining buf)
                    (proceed-callback)))

                ;; Return bytes read
                (- (.position dst) initial-pos))))

          (isOpen [_]
            (not @closed?))

          (close [_]
            (vreset! closed? true)
            (.clear queue)
            (.offer queue eof-marker)
            (vreset! current-buf nil)))]

    body-channel))

(defn proceed-request [req-ctx]
  [:h2o/proceed-request req-ctx])

(defn enqueue-request
  "Process request asynchronously on virtual thread.

   The handler runs on a vthread and when complete, the response
   is enqueued back to the same worker thread that received the request.

   Parameters:
   - worker-id: ID of the worker thread that received this request
   - req: The Request
   - ring-handler: Ring handler function (request-map -> response-map)
   - evloop-system: Event loop system for sending messages back to worker"
  [^Request req ring-handler evloop-system]
  (.submit ^ExecutorService @vthread-executor
           ^Runnable (fn []
                       (try
                         (let [ring-resp (ring-handler (:ring-req req))]
                           (response/send-ring-response! req ring-resp evloop-system)
                           #_(evloop/send-msg! evloop-system
                                               (send-response req ring-resp)))
                         (catch Exception e
                           (println "Handler error:" (.getMessage e))
                           (.printStackTrace e)
                           ;; TODO
                           #_(evloop/send-msg! evloop-system worker-id
                                               (send-response req {:status 500
                                                                   :headers {"content-type" "text/plain"}
                                                                   :body "Internal Server Error"})))))))
(defn set-req-body-channel [evloop-system req-ctx-ptr req-ctx]
  (let [proceed-callback  (fn []
                            (evloop/send-msg! evloop-system
                                              (proceed-request req-ctx)))
        write-req-channel  (create-write-req-channel proceed-callback)
        on-req-body-chunk (mem/serialize (fn [_ chunk-seg ^long chunk-len ^long is-last]
                                           #_#p{:chunk-len chunk-len :is-last is-last}
                                           (protocols/add-chunk write-req-channel (mem/read-bytes (mem/reinterpret chunk-seg chunk-len) chunk-len) (if (= 1 is-last) true false)))
                                         [::ffi/fn [::mem/pointer ::mem/pointer ::mem/long ::mem/int] ::mem/void])]
    (h2o/set-on-request-body-chunk-callback req-ctx-ptr on-req-body-chunk)
    write-req-channel))
(defn on-request [ring-handler evloop-system req-ctx-ptr req-ctx]
  (let [evloop-system (assoc evloop-system :worker-id (:id (evloop/get-current-worker)))
        write-req-channel (set-req-body-channel evloop-system req-ctx-ptr req-ctx)
        ring-req (h2o/build-ring-request (:meta req-ctx) (protocols/input-stream write-req-channel))
        req (Request. req-ctx-ptr req-ctx ring-req write-req-channel)]

    (enqueue-request req ring-handler evloop-system)
    ;; TODO: return CLJ_HANDLER_OVERLOADED if system cannot handle more requests
    h2o/CLJ_HANDLER_OK))

(defn on-request-cleanup [ring-handler evloop-system req-ctx-ptr req-ctx]
  ;; TODO
  #_(println "CLEANUP!"))
