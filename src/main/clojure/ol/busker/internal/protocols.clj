(ns ^:no-doc ol.busker.internal.protocols)

(set! *warn-on-reflection* true)

(defprotocol Stopable
  (stop [this]))

(defrecord Request
           [worker
            config
            req-ctx-ptr
            req-ctx
            write-req
            callback-pointers
            dispatch-module-id
            dispatch-request-seq])

(defprotocol WorkerThread
  (running? [_])
  (wake [_] "Wake up the worker")
  (send-msg [_ msg] "Offers a message and returns :accepted, :closed, or :overloaded")
  (send-required-msg [_ msg] "Waits in Java for capacity and returns :accepted or :closed")
  (count-msgs [_] "The number of messages in the worker mailbox")
  (add-req [_ ^String req-id req] "Add an in-flight request")
  (reap-req [_ ^String req-id] "Remove an in-flight request by ID, returning the Request"))
