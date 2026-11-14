(ns ^:no-doc ol.busker.request
  "Adapts native request callbacks to Ring-compatible request processing.

  Key functions create streaming request-body channels, dispatch handlers on virtual threads,
  and coordinate callback completion with [[ol.busker.evloop]]."
  (:require
   [babashka.ffi :as mem]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.config :as cfg]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.worker-context :as worker-context]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.request-dispatch :as request-dispatch]
   [ol.busker.response :as response])
  (:import
   [java.io IOException]
   [java.nio ByteBuffer]
   [java.nio.channels Channels ReadableByteChannel]
   [java.util.concurrent ExecutorService LinkedBlockingQueue RejectedExecutionException]
   [java.util.concurrent.atomic AtomicReference]
   [ol.busker.internal.protocols Request]))

(set! *warn-on-reflection* true)

(defn create-write-req-channel
  "Creates a ReadableByteChannel + RequestBody for streaming request body.
   proceed-callback is called after each chunk is fully consumed.

   `:write-chunk` runs on the event-loop worker and never blocks. H2O delivers
   the next chunk only after proceed, so the queue holds at most one chunk and
   the EOF marker; `:write-chunk`, `:fail!` and `close` take `admission` only
   to check closure and enqueue, so close cannot slip in between and fill the
   queue. After `:fail!` every read throws an IOException with its cause."
  [proceed-callback]
  (let [queue (LinkedBlockingQueue. 2)
        eof-marker ::eof
        admission (Object.)
        closed? (volatile! false)
        failure (volatile! nil)
        failed! #(throw (IOException. "Request body delivery failed" ^Throwable @failure))
        current-buf (volatile! nil)
        body-channel
        (reify
          ReadableByteChannel
          (read [_ dst]
            (when @failure (failed!))
            (when @closed?
              (throw (java.nio.channels.ClosedChannelException.)))

            ;; Load next chunk if current is exhausted
            (when (or (nil? @current-buf)
                      (not (.hasRemaining ^ByteBuffer @current-buf)))
              (let [chunk (.take queue)]
                (if (identical? chunk eof-marker)
                  (if @failure (failed!) (vreset! closed? true))
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
            (locking admission
              (vreset! closed? true)
              (.clear queue)
              (.offer queue eof-marker))
            (vreset! current-buf nil)))]

    {:channel body-channel
     :write-chunk (fn [chunk is-last]
                    (locking admission
                      (when-not (or @closed? @failure)
                        (when (and chunk
                                   (pos? (alength ^bytes chunk))
                                   (not (.offer queue chunk)))
                          (throw (IllegalStateException.
                                  "Request body chunk delivered before the previous one was read")))
                        (when (and is-last (not (.offer queue eof-marker)))
                          (throw (IllegalStateException.
                                  "Request body end delivered before the previous chunk was read"))))))
     :fail! (fn [^Throwable cause]
              (locking admission
                (when-not (or @closed? @failure)
                  (vreset! failure cause)
                  (.clear queue)
                  (.offer queue eof-marker))))
     :input-stream (Channels/newInputStream body-channel)}))

(defn set-req-body-channel
  [worker module-id request-seq]
  (create-write-req-channel
   (fn []
     (when (= :closed
              (pi/send-required-msg worker [:h2o/proceed-request module-id request-seq]))
       (throw (java.io.IOException. "Request worker closed"))))))

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

(defn- request-entrypoint
  "Returns the entrypoint id for the shim's entrypoint `index`, or nil when the
  index names no entrypoint of `config`."
  [config index]
  (let [ids (cfg/entrypoint-ids config)]
    (when (and (int? index) (< -1 (long index) (count ids)))
      (nth ids index))))

(defn request-callback-failed!
  "Counts a failure of the request or cleanup callback in the current worker's
  dispatch and logs it under `id`. Never throws, since the callback returns to
  native code."
  [id ^Throwable t]
  (try
    (callback-dispatch/callback-fault!
     (:callback-dispatch (worker-context/get-current-worker)) id t)
    (catch Throwable _ nil)))

(defn on-request
  [^ExecutorService executor close-callback-dispatch config ring-handler req-ctx-ptr]
  (try
    (let [req-ctx (h2o/copy-request-context req-ctx-ptr)
          entrypoint (request-entrypoint config (:entrypoint req-ctx))]
      (cond
        (not (pi/running? (worker-context/get-current-worker)))
        h2o/CLJ_HANDLER_SHUTTING_DOWN

        (nil? entrypoint)
        (do
          (h2o/report-almost-fatal-error
           (str "Rejected a request whose listener has no known entrypoint"
                " [:ol.busker/unknown-entrypoint index=" (:entrypoint req-ctx) "]")
           nil)
          h2o/CLJ_HANDLER_ERROR)

        :else
        (let [worker (worker-context/get-current-worker)
              dispatch (:callback-dispatch worker)
              module-id (:module-id dispatch)
              request-seq (callback-dispatch/allocate-identity! dispatch)
              callback-pointers (callback-dispatch/callback-pointers dispatch)
              has-body? (= 1 (:has-body req-ctx))
              write-req (when has-body?
                          (set-req-body-channel worker module-id request-seq))
              req (Request. worker config
                            req-ctx-ptr req-ctx write-req
                            callback-pointers module-id request-seq)
              emitter (response/new-response-emitter req close-callback-dispatch)]
          (callback-dispatch/register! dispatch module-id request-seq req emitter)
          (try
            (let [receiver (.get ^AtomicReference (:response-receiver_ worker))]
              (when-not (= 1 (h2o/install-request-dispatch
                              req-ctx-ptr receiver module-id request-seq
                              (if has-body? (:body callback-pointers) mem/null)))
                (throw (ex-info "Could not install native request dispatch"
                                {:module-id module-id
                                 :request-seq request-seq}))))
            (letfn [(request-task []
                      (let [ring-req (h2o/overlay-lazy-ring-request
                                      #(h2o/assemble-ring-request
                                        req-ctx
                                        (:input-stream write-req))
                                      h2o/assembled-request-keys
                                      {::emitter emitter
                                       :ol.busker/entrypoint entrypoint})
                            ring-resp (run-handler ring-handler ring-req)]
                        (response/send-ring-response! emitter ring-resp)))]
              (if-let [reuse (:request-dispatch worker)]
                (request-dispatch/dispatch! reuse (h2o/request-connection-id req-ctx-ptr) request-task)
                (.execute executor request-task)))
            h2o/CLJ_HANDLER_OK
            (catch RejectedExecutionException _
              (callback-dispatch/retire! dispatch module-id request-seq)
              h2o/CLJ_HANDLER_SHUTTING_DOWN)
            (catch Throwable t
              (callback-dispatch/retire! dispatch module-id request-seq)
              (throw t))))))
    (catch InterruptedException _
      (.interrupt (Thread/currentThread))
      h2o/CLJ_HANDLER_SHUTTING_DOWN)
    (catch Throwable t
      (request-callback-failed! ::request-callback-failed t)
      h2o/CLJ_HANDLER_OVERLOADED)))

(defn on-request-cleanup
  "Retires request state using the scalar dispatch identity from native cleanup."
  [module-id request-seq]
  (try
    (when-let [worker (worker-context/get-current-worker)]
      (try
        (callback-dispatch/retire! (:callback-dispatch worker) module-id request-seq)
        (finally (fixed-final/notify-capacity! worker))))
    (catch Throwable t
      (request-callback-failed! ::cleanup-callback-failed t))))
