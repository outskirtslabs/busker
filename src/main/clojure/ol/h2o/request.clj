(ns ol.h2o.request
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.h2o.evloop :as evloop]
   [ol.h2o.native :as h2o]
   [ol.h2o.protocols :as p]
   [ol.h2o.response :as response])
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

    {:channel body-channel
     :write-chunk (fn [chunk is-last]
                    (when-not @closed?
                      (when chunk
                        (.put queue chunk))
                      (when is-last
                        (.put queue eof-marker))))
     :input-stream (Channels/newInputStream body-channel)}))

(defn enqueue-request
  "Process request asynchronously on virtual thread.

   The handler runs on a vthread and when complete, the response
   is enqueued back to the same worker thread that received the request.

   Parameters:
   - worker-id: ID of the worker thread that received this request
   - req: The Request
   - ring-handler: Ring handler function (request-map -> response-map) "
  [^Request req ring-handler]
  (.submit ^ExecutorService @vthread-executor
           ^Runnable (fn []
                       (try
                         (let [ring-resp (ring-handler (:ring-req req))]
                           (response/send-ring-response! req ring-resp))
                         (catch Exception e
                           (println "Handler error:" (.getMessage e))
                           (.printStackTrace e)
                           (response/send-ring-response! req {:status 500
                                                              :headers {"content-type" "text/plain"}
                                                              :body "Internal Server Error"}))))))
(defn set-req-body-channel [worker req-ctx-ptr req-ctx]
  (let [proceed-callback  (fn []
                            (p/send-msg worker [:h2o/proceed-request req-ctx]))
        {:keys [write-chunk] :as write-req} (create-write-req-channel proceed-callback)
        on-req-body-chunk-cb (fn [_ chunk-seg ^long chunk-len ^long is-last]
                               (write-chunk
                                (when-not (mem/null? chunk-seg) (mem/read-bytes (mem/reinterpret chunk-seg chunk-len) chunk-len))
                                (if (= 1 is-last) true false)))
        on-req-body-chunk-cb-ptr  (mem/serialize on-req-body-chunk-cb [::ffi/fn [::mem/pointer ::mem/pointer ::mem/long ::mem/int] ::mem/void])]
    (h2o/set-on-request-body-chunk-callback req-ctx-ptr on-req-body-chunk-cb-ptr)
    (assoc write-req
           ::on-req-body-chunk-cb on-req-body-chunk-cb
           ::on-req-body-chunk-cb-ptr on-req-body-chunk-cb-ptr)))

(defn on-request [ring-handler req-ctx-ptr req-ctx]
  (let [worker    (evloop/get-current-worker)
        has-body? (:has_body (:meta req-ctx))
        write-req (when has-body? (set-req-body-channel worker req-ctx-ptr req-ctx))
        ring-req  (h2o/build-ring-request (:meta req-ctx) (:input-stream write-req))
        req-id    (h2o/cstr-array->string (:req-id req-ctx))
        req (response/with-response-writer (Request. worker req-id req-ctx-ptr req-ctx ring-req write-req nil))]
    (p/add-req worker req)
    (enqueue-request req ring-handler)
    ;; TODO: return CLJ_HANDLER_OVERLOADED if system cannot handle more requests
    h2o/CLJ_HANDLER_OK))

(defn on-request-cleanup
  "Completion cleanup callback - this is called by h2o when our request dies
   such as when the client disconnects abruptly
   ref: https://github.com/h2o/h2o/issues/1894#issuecomment-437231273"
  [_ring-handler _req-ctx-ptr req-ctx]
  (let [req (p/reap-req (evloop/get-current-worker) (h2o/cstr-array->string (:req-id req-ctx)))]
    (when-some [input-stream (-> req :write-req :input-stream)]
      (println "closeing req entity")
      (.close input-stream))
    (when-some [output-stream (-> req :write-resp :out-stream)]
      (println "closeing resp entity")
      (.close output-stream))))
