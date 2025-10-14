(ns ol.h2o.protocols)

(set! *warn-on-reflection* true)

(defrecord Request [req-ctx-ptr req-ctx ring-req write-req])
