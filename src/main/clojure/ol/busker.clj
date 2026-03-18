(ns ol.busker
  "Public lifecycle API for Busker.

  `ol.busker` is the stable entrypoint namespace for starting, stopping, and
  inspecting a running Busker server handle during the `012` cutover."
  (:require
   [ol.busker.runtime :as runtime]))

(defn start!
  "Start a Busker server from `config` and return an opaque server handle."
  [config]
  (runtime/start! config))

(defn stop!
  "Synchronously stop `server`."
  [server]
  (runtime/stop! server))

(defn state
  "Return pure runtime state for `server`."
  [server]
  (runtime/state server))
