(ns tasks.smoke
  (:require
   [babashka.process :as p]
   [clojure.string :as str]
   [tasks.pebble :as pebble]))

(defn- env
  [name default]
  (let [value (System/getenv name)]
    (if (str/blank? value)
      default
      value)))

(defn smoke-managed!
  [& args]
  (let [started-pebble? (if (pebble/running?)
                          false
                          (do
                            (pebble/start!)
                            true))
        keep-running? (= "1" (env "BUSKER_PEBBLE_KEEP_RUNNING" "0"))
        extra-env {"BUSKER_SMOKE_DOMAIN" (env "BUSKER_SMOKE_DOMAIN" "example.com")
                   "BUSKER_ACME_DIRECTORY_URL"
                   (env "BUSKER_ACME_DIRECTORY_URL" "https://localhost:14000/dir")
                   "BUSKER_ACME_TRUST_STORE"
                   (env "BUSKER_ACME_TRUST_STORE" "src/test/fixtures/pebble-truststore.p12")
                   "BUSKER_ACME_TRUST_STORE_PASS"
                   (env "BUSKER_ACME_TRUST_STORE_PASS" "changeit")}
        cmd (into ["clojure" "-M:dev" "-m" "smoke"] args)]
    (try
      (let [{:keys [exit]} (apply p/shell {:continue true :extra-env extra-env}
                                  cmd)]
        (when-not (zero? exit)
          (throw (ex-info "Managed TLS smoke run failed." {:exit exit}))))
      (finally
        (when (and started-pebble? (not keep-running?))
          (pebble/stop!)))))
  nil)
