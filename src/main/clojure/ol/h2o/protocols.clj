(ns ol.h2o.protocols)

(set! *warn-on-reflection* true)

(defrecord Request [req-ctx-ptr req-ctx ring-req write-req])

(defprotocol WriteReq
  (add-chunk [this chunk is-last] "Add a chunk of bytes to the channel. is-last indicates end of stream.")
  (input-stream [this]))

(defprotocol WriteRes
  (output-stream [this] "Get OutputStream adapter for this writable channel"))
