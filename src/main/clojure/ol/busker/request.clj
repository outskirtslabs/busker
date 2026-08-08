(ns ^:no-doc ol.busker.request
  (:require
   [coffi.mem :as mem]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.evloop :as evloop]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.response :as response])
  (:import
   [java.nio ByteBuffer]
   [java.nio.channels Channels ReadableByteChannel]
   [java.util.concurrent ExecutorService LinkedBlockingQueue RejectedExecutionException]
   [ol.busker.internal.protocols Request]))

(set! *warn-on-reflection* true)

(defn create-write-req-channel
  "Creates a ReadableByteChannel + RequestBody for streaming request body.
   proceed-callback is called after each chunk is fully consumed."
  [proceed-callback]
  (let [queue (LinkedBlockingQueue. 2)
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
                      (when (and chunk (pos? (alength ^bytes chunk)))
                        (.put queue chunk))
                      (when is-last
                        (.put queue eof-marker))))
     :input-stream (Channels/newInputStream body-channel)}))

(defn set-req-body-channel
  [worker module-id request-seq]
  (create-write-req-channel
   (fn []
     (pi/send-msg worker [:h2o/proceed-request module-id request-seq]))))

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

(defn on-request
  [^ExecutorService executor close-callback-dispatch config ring-handler req-ctx-ptr req-ctx]
  (if-not (pi/running? (evloop/get-current-worker))
    h2o/CLJ_HANDLER_SHUTTING_DOWN
    (try
      (let [worker (evloop/get-current-worker)
            dispatch (:callback-dispatch worker)
            [module-id request-seq] (callback-dispatch/allocate-identity! dispatch)
            callback-pointers (callback-dispatch/callback-pointers dispatch)
            has-body? (:has_body (:meta req-ctx))
            write-req (when has-body?
                        (set-req-body-channel worker module-id request-seq))
            req-id (h2o/cstr-array->string (:req-id req-ctx))
            req (Request. worker config req-id req-ctx-ptr req-ctx write-req
                          callback-pointers module-id request-seq)
            emitter (response/new-response-emitter req close-callback-dispatch)
            ring-req (assoc (h2o/build-ring-request (:meta req-ctx)
                                                    (:input-stream write-req))
                            ::emitter emitter)]
        (callback-dispatch/register! dispatch module-id request-seq req emitter)
        (try
          (h2o/install-request-dispatch req-ctx-ptr module-id request-seq
                                        (if has-body? (:body callback-pointers) mem/null))
          (letfn [(request-task []
                    (let [ring-resp (run-handler ring-handler ring-req)]
                      (response/send-ring-response! emitter ring-resp)))]
            (.submit executor ^Runnable request-task))
          h2o/CLJ_HANDLER_OK
          (catch RejectedExecutionException _
            (callback-dispatch/retire! dispatch module-id request-seq)
            h2o/CLJ_HANDLER_SHUTTING_DOWN)
          (catch Throwable t
            (callback-dispatch/retire! dispatch module-id request-seq)
            (throw t))))
      (catch InterruptedException _
        (.interrupt (Thread/currentThread))
        h2o/CLJ_HANDLER_SHUTTING_DOWN)
      (catch Exception e
        (h2o/report-almost-fatal-error "The request handler errored with" e)
        h2o/CLJ_HANDLER_OVERLOADED)
      (catch Throwable t
        (h2o/report-almost-fatal-error "The request bridge errored with" t)
        h2o/CLJ_HANDLER_OVERLOADED))))

(defn on-request-cleanup
  "Retires request state using the scalar dispatch identity from native cleanup."
  [module-id request-seq]
  (try
    (when-let [worker (evloop/get-current-worker)]
      (callback-dispatch/retire! (:callback-dispatch worker) module-id request-seq))
    (catch Exception e
      (h2o/report-almost-fatal-error "The request cleanup callback errored" e))))
