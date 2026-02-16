(ns ol.busker.pebble-fixture
  (:require
   [babashka.process :as p]
   [clojure.java.io :as io]))

(defn directory-url
  []
  (or (System/getenv "BUSKER_PEBBLE_DIRECTORY_URL")
      "https://localhost:14000/dir"))

(defn http-client-opts
  []
  {:ssl-context
   {:trust-store (or (System/getenv "BUSKER_ACME_TRUST_STORE")
                     (.getPath (io/file "src/test/fixtures/pebble-truststore.p12")))
    :trust-store-pass (or (System/getenv "BUSKER_ACME_TRUST_STORE_PASS")
                          "changeit")}})

(defn start!
  []
  (p/shell "./scripts/pebble-fixture.sh" "start"))

(defn stop!
  []
  (p/shell "./scripts/pebble-fixture.sh" "stop"))

(defn fixture
  [f]
  (start!)
  (try
    (f)
    (finally
      (stop!))))
