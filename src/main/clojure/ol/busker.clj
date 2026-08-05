(ns ol.busker
  "Public lifecycle API for Busker.

  See xref:configuration.adoc[Configuration] for the map accepted by [[start!]]
  and [[reload!]]."
  (:require
   [ol.busker.runtime :as runtime]))

(defn start!
  "Start a Busker server from `config` and return an opaque server handle.

  See xref:configuration.adoc[Configuration] for the config shape."
  [config]
  (runtime/start! config))

(defn reload!
  "Compile and activate a new runtime snapshot for `server` from `config`.

  See xref:configuration.adoc[Configuration] for the config shape."
  ([server config]
   (runtime/reload! server config))
  ([server config opts]
   (runtime/reload! server config opts)))

(defn stop!
  "Synchronously stop `server`."
  [server]
  (runtime/stop! server))

(defn state
  "Return pure runtime state for `server`.

  The returned state includes the normalized config snapshot.
  See xref:configuration.adoc[Configuration] for that config shape."
  [server]
  (runtime/state server))
