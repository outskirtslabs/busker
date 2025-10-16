(ns ol.h2o.protocols)

(set! *warn-on-reflection* true)

(defrecord Request [worker req-ctx-ptr req-ctx ring-req write-req write-resp])
