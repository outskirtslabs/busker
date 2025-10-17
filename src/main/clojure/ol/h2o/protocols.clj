(ns ol.h2o.protocols)

(set! *warn-on-reflection* true)

(defrecord Request [worker req-id req-ctx-ptr req-ctx ring-req write-req write-resp])

(defprotocol WorkerThread
  (wake [_] "Wake up the worker")
  (send-msg [_ msg] "Send a message to the worker")
  (count-msgs [_] "The number of messages in the worker mailbox")
  (add-req [_ ^Request req] "Add an in-flight request")
  (reap-req [_ ^String req-id] "Remove an in-flight request by ID, returning the Request"))
