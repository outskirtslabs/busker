(ns ^:no-doc ol.busker.internal.protocols
  "Internal contracts shared by the event loop, request handling, and response code.

  [[WorkerThread]] describes nonblocking worker control operations. [[Request]] carries
  the native request context and dispatch identity while it is live. [[Stopable]] gives
  response writers and similar lifecycle participants one stop operation.

  ## Related Namespaces

  - [[ol.busker.evloop]] implements [[WorkerThread]].
  - [[ol.busker.request]] creates [[Request]] values.
  - [[ol.busker.response]] and [[ol.busker.response-queue]] stop response writers.")

(set! *warn-on-reflection* true)

(defprotocol Stopable
  "Stops `this` and releases or closes its active operation. Implementations define whether
  the call is idempotent and which thread may call it."
  (stop [this] "Stops `this`."))

(defrecord Request
           [worker config req-ctx-ptr req-ctx write-req callback-pointers dispatch-module-id
            dispatch-request-seq])

(alter-meta! #'->Request assoc :doc
             "Creates a live request record. `worker` runs event-loop actions; `config` is the generation configuration; `req-ctx-ptr` and `req-ctx` identify native request data; `write-req` writes streaming request data; `callback-pointers` retains native callbacks; `dispatch-module-id` and `dispatch-request-seq` identify the dispatch entry.")

(defprotocol WorkerThread
  "Coordinates work on one event-loop thread. Calls that may wait for mailbox capacity
  must not run on virtual-thread request application code."
  (running? [worker] "Returns true while `worker` accepts ordinary work.")
  (wake [worker] "Requests a native wake for `worker`; it does not transfer work.")
  (send-msg [worker msg] "Offers `msg` and returns `:accepted`, `:closed`, or `:overloaded`.")
  (send-required-msg [worker msg] "Waits in Java for mailbox capacity and returns `:accepted` or `:closed`.")
  (count-msgs [worker] "Returns the number of queued mailbox messages.")
  (add-req [worker req-id request] "Registers live `request` under string `req-id`.")
  (reap-req [worker req-id] "Removes the request for `req-id` and returns it, or `nil`."))
