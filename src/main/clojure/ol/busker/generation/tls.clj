(ns ^:no-doc ol.busker.generation.tls
  "Reads the TLS certificates of a configuration and answers the certificate
  lookups of TLS handshakes.

  ## Related Namespaces

  - [[ol.busker.generation]] installs the lookup in the TLS contexts."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.clave.certificate :as clave-certificate]
   [taoensso.trove :as trove])
  (:import
   [java.io File]
   [java.security.cert CertificateFactory X509Certificate]
   [java.util Base64]))

(set! *warn-on-reflection* true)

(defn http3-enabled?
  [listener]
  (boolean
   (when-let [tls (:tls listener)]
     (and (map? tls)
          (not= false (:http3? tls))))))

(defn- x509-certificate->pem
  [^X509Certificate cert]
  (let [line-separator (byte-array [(byte 10)])
        encoder (Base64/getMimeEncoder 64 line-separator)
        cert-b64 (.encodeToString encoder (.getEncoded cert))]
    (str "-----BEGIN CERTIFICATE-----\n"
         cert-b64
         "\n-----END CERTIFICATE-----\n")))

(defn- certificate->pem
  [certificate]
  (cond
    (string? certificate)
    certificate

    (instance? X509Certificate certificate)
    (x509-certificate->pem certificate)

    :else
    (throw (ex-info "Unsupported certificate value in TLS lookup bundle."
                    {:certificate-type (some-> certificate class .getName)}))))

(defn- private-key->pem
  [private-key]
  (cond
    (nil? private-key)
    nil

    (string? private-key)
    private-key

    :else
    (clave-certificate/private-key->pem private-key)))

(defn- clave-bundle->tls-material
  [bundle]
  (let [certificates (:certificate bundle)
        cert-chain (cond
                     (nil? certificates) nil
                     (sequential? certificates) certificates
                     :else [certificates])
        private-key-pem (private-key->pem (:private-key bundle))]
    (when (and (seq cert-chain) private-key-pem)
      {:cert-chain-pem (apply str (map certificate->pem cert-chain))
       :private-key-pem private-key-pem})))

(defn- read-certificates
  [cert-file]
  ;; Parse from in-memory bytes so certificate loading does not depend on
  ;; FileInputStream close semantics during startup or reload.
  (with-open [in (java.io.ByteArrayInputStream.
                  (java.nio.file.Files/readAllBytes
                   (.toPath (io/file cert-file))))]
    (vec (.generateCertificates (CertificateFactory/getInstance "X.509") in))))

(defn- certificate-subject-names
  [^X509Certificate certificate]
  (let [sans (or (.getSubjectAlternativeNames certificate) [])
        san-names (->> sans
                       (keep (fn [entry]
                               (let [entry-type (.get ^java.util.List entry 0)
                                     value (.get ^java.util.List entry 1)]
                                 (when (or (= entry-type 2)
                                           (= entry-type 7))
                                   (str value)))))
                       distinct
                       vec)]
    (if (seq san-names)
      san-names
      (when-let [[_ cn] (re-find #"CN=([^,]+)"
                                 (.getName (.getSubjectX500Principal certificate)))]
        [cn]))))

(defn- wildcard-subject-name?
  [subject-name]
  (and (string? subject-name)
       (str/starts-with? subject-name "*.")))

(defn- subject-name-matches-hostname?
  [subject-name hostname]
  (cond
    (or (nil? subject-name) (nil? hostname))
    false

    (= subject-name hostname)
    true

    (wildcard-subject-name? subject-name)
    (let [suffix (subs subject-name 1)
          hostname-parts (str/split hostname #"\.")
          suffix-parts (str/split (subs subject-name 2) #"\.")]
      (and (str/ends-with? hostname suffix)
           (= (count hostname-parts)
              (inc (count suffix-parts)))))

    :else
    false))

(defn- tls-material-from-files
  [cert-file key-file]
  (let [certificates (read-certificates cert-file)]
    {:names (or (some-> certificates first certificate-subject-names)
                [])
     :material {:cert-chain-pem (slurp cert-file)
                :private-key-pem (slurp key-file)}}))

(defn- directory-pem-pair
  [^File dir]
  (let [cert-pem (io/file dir "cert.pem")
        key-pem (io/file dir "key.pem")
        fullchain-pem (io/file dir "fullchain.pem")
        privkey-pem (io/file dir "privkey.pem")]
    (cond
      (and (.isFile cert-pem) (.isFile key-pem))
      {:cert-file (.getPath cert-pem)
       :key-file (.getPath key-pem)}

      (and (.isFile fullchain-pem) (.isFile privkey-pem))
      {:cert-file (.getPath fullchain-pem)
       :key-file (.getPath privkey-pem)}

      :else
      nil)))

(defn- load-folder-tls-materials
  [path]
  (->> (file-seq (io/file path))
       (filter #(.isDirectory ^File %))
       (keep directory-pem-pair)
       (mapv (fn [{:keys [cert-file key-file]}]
               (tls-material-from-files cert-file key-file)))))

(defn- load-static-tls-materials
  [config]
  (->> (get-in config [:tls :certificates :load])
       (mapcat (fn [{:keys [type path cert-file key-file]}]
                 (case type
                   :folder (load-folder-tls-materials path)
                   :pem [(tls-material-from-files cert-file key-file)]
                   [])))
       vec))

(defn first-static-tls-identity
  [config]
  (some (fn [{:keys [type path cert-file key-file]}]
          (case type
            :pem
            (when (and cert-file key-file)
              {:cert-file cert-file
               :key-file key-file})

            :folder
            (some->> (file-seq (io/file path))
                     (filter #(.isDirectory ^File %))
                     (keep directory-pem-pair)
                     first)

            nil))
        (get-in config [:tls :certificates :load])))

(defn tls-callback-required?
  [config]
  (boolean
   (some (fn [[_ entrypoint]]
           (map? (:tls entrypoint)))
         (:entrypoints config))))

(defn build-tls-lookup-fn
  [config cert-runtime]
  (let [static-materials (load-static-tls-materials config)
        fallback-material (some-> static-materials first :material)
        fallback-subject-name (first (:subject-names cert-runtime))
        cert-lookup-fn
        (or (:lookup-fn cert-runtime)
            (let [lookup-certificate clave-adapter/lookup-certificate]
              (fn [hostname]
                (lookup-certificate cert-runtime hostname))))]
    (fn [hostname]
      (let [sni-hostname (when (and (string? hostname)
                                    (not (.isEmpty ^String hostname)))
                           hostname)
            static-match (when sni-hostname
                           (some (fn [{:keys [names material]}]
                                   (when (some #(subject-name-matches-hostname? % sni-hostname)
                                               names)
                                     material))
                                 static-materials))]
        (cond
          static-match
          static-match

          sni-hostname
          (some-> (cert-lookup-fn sni-hostname)
                  clave-bundle->tls-material)

          fallback-material
          fallback-material

          fallback-subject-name
          (some-> (cert-lookup-fn fallback-subject-name)
                  clave-bundle->tls-material)

          :else
          (do
            (trove/log! {:level :trace
                         :id ::tls-lookup-no-sni-miss
                         :data {}})
            nil))))))
