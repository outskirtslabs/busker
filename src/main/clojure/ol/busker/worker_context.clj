(ns ^:no-doc ol.busker.worker-context
  "Worker-thread context shared by event-loop response operations.

  The event loop binds a [[ol.busker.evloop/Worker]] to this thread-local value
  while it runs. Response preparation uses [[get-current-worker]] to reject
  calls from other threads without depending on the event-loop implementation.

  ## Related Namespaces

  - [[ol.busker.evloop]] runs worker threads.
  - [[ol.busker.fixed-final]] and [[ol.busker.response-head]] perform
    worker-affine response operations.")

(set! *warn-on-reflection* true)

(def ^ThreadLocal worker-context (ThreadLocal.))

(defn get-current-worker
  "Returns the Worker bound to the current event-loop thread, or `nil`.

  Callers use this value only while the event loop invokes worker-affine work."
  []
  (.get worker-context))
