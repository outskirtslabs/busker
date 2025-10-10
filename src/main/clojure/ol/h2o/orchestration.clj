(ns ol.h2o.orchestration
  "Orchestration layer that wires together FFI, evloop, request, and response.
   This is the only namespace that depends on all the others, breaking circular dependencies."
  (:import [java.util.concurrent Executors])
  (:require
   [ol.h2o.native :as h2o]
   [ol.h2o.evloop :as evloop]
   [ol.h2o.response :as response]
   [coffi.mem :as mem]))

(set! *warn-on-reflection* true)

(defonce vthread-executor
  (delay (Executors/newVirtualThreadPerTaskExecutor)))

(defn create-message-handler
  "Create the message handler function for workers.
   This function will be called by drain-mailbox! for each message."
  []
  (fn [op args]
    (case op
      :h2o/send-response
      (let [[req-ptr ring-resp] args]
        (response/send-ring-response! req-ptr ring-resp))
      nil)))

(defn enqueue-request!
  "Process request asynchronously on virtual thread.

   The handler runs on a vthread and when complete, the response
   is enqueued back to the same worker thread that received the request.

   Parameters:
   - worker-id: ID of the worker thread that received this request
   - req-ptr: Native pointer to h2o_req_t
   - ring-handler: Ring handler function (request-map -> response-map)
   - evloop-system: Event loop system for sending messages back to worker"
  [worker-id req-ptr ring-handler evloop-system]
  (.submit @vthread-executor
           (fn []
             (try
               (let [ring-req (h2o/build-ring-request req-ptr)
                     ring-resp (ring-handler ring-req)]
                 (evloop/send-msg! evloop-system worker-id
                                   [:h2o/send-response req-ptr ring-resp]))
               (catch Exception e
                 (println "Handler error:" (.getMessage e))
                 (.printStackTrace e)
                 (evloop/send-msg! evloop-system worker-id
                                   [:h2o/send-response req-ptr {:status 500
                                                                :headers {"content-type" "text/plain"}
                                                                :body "Internal Server Error"}]))))))

(defn on-request-callback
  [ring-handler evloop-system req-ptr]
  (let [worker (evloop/get-current-worker)
        worker-id (:id worker)]
    (enqueue-request! worker-id req-ptr ring-handler evloop-system)))

(defn create-ring-handler
  "Create an h2o handler that delegates to a Ring handler.
   Returns handler pointer that must be kept alive."
  [pathconf-ptr ring-handler evloop-system]
  (let [handler-ptr (h2o/create-handler pathconf-ptr (h2o/handler-size))
        on-req-callback (h2o/create-request-callback ring-handler evloop-system
                                                     (partial on-request-callback ring-handler evloop-system))]
    (h2o/handler-set-on-req handler-ptr on-req-callback)
    handler-ptr))

(defn create-server-config
  "Create and initialize h2o global configuration with a default host and Ring handler.
   Uses global arena for server lifetime resources.
   Returns map with ::h2o/arena, ::h2o/config-ptr, ::h2o/hostconf-ptr, ::h2o/pathconf-ptr, ::h2o/handler-ptr"
  [ring-handler evloop-system]
  (let [arena (mem/global-arena)
        size (h2o/globalconf-size)
        config-ptr (mem/alloc size arena)]
    (h2o/config-init config-ptr)
    (let [host-iovec-seg (h2o/create-iovec "default" arena)
          host-iovec-data (mem/deserialize host-iovec-seg ::h2o/h2o-iovec-t)
          hostconf-ptr (h2o/config-register-host config-ptr host-iovec-data 65535)
          pathconf-ptr (h2o/config-register-path hostconf-ptr "/" 0)
          handler-ptr (create-ring-handler pathconf-ptr ring-handler evloop-system)]
      {::h2o/arena arena
       ::h2o/config-ptr config-ptr
       ::h2o/hostconf-ptr hostconf-ptr
       ::h2o/pathconf-ptr pathconf-ptr
       ::h2o/handler-ptr handler-ptr})))
