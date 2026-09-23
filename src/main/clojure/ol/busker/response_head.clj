(ns ^:no-doc ol.busker.response-head
  "Event-loop commands for response status and headers.

  A response head is the status and headers before body delivery; it is not the
  HTTP `HEAD` method. [[execute-start!]] begins a streaming response and
  [[execute-informational!]] sends a 1xx response. Both use
  [[ol.busker.response-serialization]] for native header descriptors.

  ## Related Namespaces

  - [[ol.busker.response]] selects and queues response operations.
  - [[ol.busker.fixed-final]] sends eligible complete final responses.
  - [[ol.busker.worker-context]] checks event-loop affinity."
  (:require
   [coffi.mem :as mem]
   [ol.busker.native :as h2o]
   [ol.busker.response-serialization :as serialization]
   [ol.busker.worker-context :as worker-context]))

(set! *warn-on-reflection* true)

(defrecord StartCommand [^long module-id ^long request-seq ^long status headers
                         ^long content-length ^long compress-hint])

(alter-meta! #'->StartCommand assoc :doc
             "Creates a response-start command. `module-id` and `request-seq` identify the request; `status`, `headers`, `content-length`, and `compress-hint` describe the final response head.")

(defrecord InformationalCommand [^long module-id ^long request-seq ^long status headers])

(alter-meta! #'->InformationalCommand assoc :doc
             "Creates an informational-response command. `module-id` and `request-seq` identify the request; `status` and `headers` describe the 1xx response head.")

(defn start-command
  "Creates a start command without allocating native memory.

  `headers` is copied to a vector. The event-loop worker later stages its native
  descriptors during [[execute-start!]]."
  [module-id request-seq status headers content-length compress-hint]
  (->StartCommand module-id request-seq status (vec headers) content-length compress-hint))

(defn informational-command
  "Creates an informational command without allocating native memory.

  `headers` is copied to a vector. The event-loop worker later stages its native
  descriptors during [[execute-informational!]]."
  [module-id request-seq status headers]
  (->InformationalCommand module-id request-seq status (vec headers)))

(defn command-module-id
  "Returns the request module identifier from `command`."
  ^long [command]
  (:module-id command))

(defn command-request-seq
  "Returns the request sequence from `command`."
  ^long [command]
  (:request-seq command))

(defn- headers-segment
  [headers arena]
  (if (zero? (count headers))
    (mem/as-segment 0)
    (let [descriptor-bytes (serialization/descriptor-bytes headers)
          header-bytes (serialization/header-bytes headers)
          segment (mem/alloc (inc (+ descriptor-bytes header-bytes)) arena)
          descriptors (mem/slice segment 0 descriptor-bytes)
          payload (mem/slice segment descriptor-bytes (inc header-bytes))]
      (serialization/stage-headers! descriptors payload headers)
      descriptors)))

(defn- ensure-worker!
  [req]
  (when-not (identical? (:worker req) (worker-context/get-current-worker))
    (throw (ex-info "Response head command ran outside its event-loop worker" {}))))

(defn execute-start!
  "Starts a response on its event-loop worker.

  Stages command headers in a confined arena, invokes native start-response, and
  returns its result. The native call copies the header bytes before the arena
  closes. Throws when called from another thread."
  [req ^StartCommand command]
  (ensure-worker! req)
  (with-open [arena (mem/confined-arena)]
    (let [headers (.-headers command)
          segment (headers-segment headers arena)
          {:keys [proceed stop]} (:callback-pointers req)]
      (h2o/start-response (:req-ctx-ptr req) (.-status command) segment
                          (count headers) (.-content-length command)
                          (.-compress-hint command) proceed stop))))

(defn execute-informational!
  "Sends a 1xx response on its event-loop worker.

  Stages command headers in a confined arena and returns after native code copies
  them. Throws when called from another thread."
  [req ^InformationalCommand command]
  (ensure-worker! req)
  (with-open [arena (mem/confined-arena)]
    (let [headers (.-headers command)]
      (h2o/send-informational (:req-ctx-ptr req) (.-status command)
                              (headers-segment headers arena) (count headers)))))