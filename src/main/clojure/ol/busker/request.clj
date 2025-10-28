(ns ol.busker.request
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.busker.evloop :as evloop]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.response :as response])
  (:import
   [java.io InputStream]
   [java.nio ByteBuffer]
   [java.nio.channels Channels ReadableByteChannel]
   [java.util.concurrent ExecutorService LinkedBlockingQueue RejectedExecutionException]
   [ol.busker.internal.protocols Request]))

(set! *warn-on-reflection* true)

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

(defn set-req-body-channel [worker req-ctx-ptr req-ctx]
  (let [proceed-callback (fn []
                           (pi/send-msg worker [:h2o/proceed-request req-ctx]))
        {:keys [write-chunk]
         :as write-req} (create-write-req-channel proceed-callback)
        on-req-body-chunk-cb (fn [_ chunk-seg ^long chunk-len ^long is-last]
                               (write-chunk
                                (when-not (mem/null? chunk-seg) (mem/read-bytes (mem/reinterpret chunk-seg chunk-len) chunk-len))
                                (if (= 1 is-last) true false)))
        on-req-body-chunk-cb-ptr (mem/serialize on-req-body-chunk-cb [::ffi/fn [::mem/pointer ::mem/pointer ::mem/long ::mem/int] ::mem/void])]
    (h2o/set-on-request-body-chunk-callback req-ctx-ptr on-req-body-chunk-cb-ptr)
    (assoc write-req
           ::on-req-body-chunk-cb on-req-body-chunk-cb
           ::on-req-body-chunk-cb-ptr on-req-body-chunk-cb-ptr)))

(defn run-handler
  "Run the handler given the ring request map. Must return a ring response map."
  [handler ring-req]
  (or (try
        (handler ring-req)
        (catch InterruptedException _
          (.interrupt (Thread/currentThread))
          {:status 503
           :headers {"content-type" "text/plain; charset=utf-8"}
           :body "Server shutting down"})
        (catch Exception e
          (println "Handler error:" (.getMessage e))
          (.printStackTrace e)
          {:status 500
           :headers {"content-type" "text/plain; charset=utf-8"}
           :body "Internal Server Error"}))
      {:status 404
       :headers {"content-type" "text/plain; charset=utf-8"}
       :body "Page Not Found"}))

(defn close-streams [req emitter]
  (when req
    (when-some [^InputStream input-stream (-> req :write-req :input-stream)]
      (.close input-stream)))
  (when emitter
    (response/stop-emitter emitter)))

(defn on-request
  [^ExecutorService executor config ring-handler req-ctx-ptr req-ctx]
  (if-not (pi/running? (evloop/get-current-worker))
    h2o/CLJ_HANDLER_SHUTTING_DOWN
    (try
      (let [worker    (evloop/get-current-worker)
            has-body? (:has_body (:meta req-ctx))
            write-req (when has-body? (set-req-body-channel worker req-ctx-ptr req-ctx))
            req-id    (h2o/cstr-array->string (:req-id req-ctx))
            req       (Request. worker config req-id req-ctx-ptr req-ctx write-req)
            emitter   (response/new-response-emitter req)
            ring-req  (assoc (h2o/build-ring-request (:meta req-ctx) (:input-stream write-req))
                             ::emitter emitter)]
        (pi/add-req worker req-id [req emitter])
        (try
          (letfn [(request-task []
                    (let [ring-resp (run-handler ring-handler ring-req)]
                      (response/send-ring-response! emitter ring-resp)
                      #_(response/send-ring-response! req ring-resp)))]
            (.submit executor ^Runnable request-task))
          h2o/CLJ_HANDLER_OK
          (catch RejectedExecutionException e
            ;; Handler never ran: remove and close local state so the shim cleanup does not see a dangling queue.
            (pi/reap-req worker req-id)
            (close-streams req emitter)
            h2o/CLJ_HANDLER_SHUTTING_DOWN)))
      (catch InterruptedException e
        (.interrupt (Thread/currentThread))
        h2o/CLJ_HANDLER_SHUTTING_DOWN)
      (catch Exception e
        (h2o/report-almost-fatal-error "The request handler errored with" e)
        h2o/CLJ_HANDLER_OVERLOADED)
      (catch Throwable t
        #p t
        h2o/CLJ_HANDLER_OVERLOADED))))

(defn on-request-cleanup
  "Completion cleanup callback - this is called by h2o when our request dies
   such as when the client disconnects abruptly
   ref: https://github.com/h2o/h2o/issues/1894#issuecomment-437231273

   Exceptions thrown from this function will crash the jvm."
  [_ring-handler _req-ctx-ptr req-ctx]
  (try
    (when-some [req-id (-> req-ctx :req-id (h2o/cstr-array->string))]
      (let [[req emitter] (pi/reap-req (evloop/get-current-worker) req-id)]
        (close-streams req emitter)))
    (catch Exception e
      (h2o/report-almost-fatal-error "The request cleanup callback errored" e))))
