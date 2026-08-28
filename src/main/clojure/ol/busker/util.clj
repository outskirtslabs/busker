(ns ^:no-doc ol.busker.util
  (:require
   [clojure.string :as str]
   [ol.clave.crypto.impl.parse-ip :as parse-ip]))

(defmacro compile-if
  "Evaluates `test`. If it returns logical true (and doesn't throw), expands
  to `then`, otherwise expands to `else`."
  {:style/indent 1}
  [test then else]
  (if (try (eval test) (catch Throwable _ false))
    `(do ~then)
    `(do ~else)))

(def ^:private re-reg-name
  #"^(?:[A-Za-z0-9._~!$&'()*+,;=-]|%[0-9A-Fa-f]{2})+$")

(def ^:private re-ipvfuture
  #"(?i)^v[0-9A-F]+\.[A-Za-z0-9._~!$&'()*+,;=:-]+$")

(defn parse-port
  [port-str]
  (when (re-matches #"\d+" port-str)
    (let [port (parse-long port-str)]
      (when (<= 0 port 65535)
        port))))

(defn- valid-ipv6-literal?
  [host]
  (and (not (str/includes? host "%"))
       (re-find #":" host)
       ;; Keep literal validation pure and non-blocking.
       (some? (parse-ip/ip-string->bytes host))))

(defn- valid-ip-literal?
  [host]
  (or (valid-ipv6-literal? host)
      (boolean (re-matches re-ipvfuture host))))

(defn- authority-result
  [server-name server-port]
  {:server-name server-name
   :server-port server-port})

(defn parse-authority
  "Parse an HTTP authority or Host value into Ring-friendly server metadata.

   Valid authorities follow RFC 9110 `Host = uri-host [\":\" port ]`.
   Busker accepts reg-name or IPv4 hosts with an optional numeric port, and
   bracketed IP literals with an optional numeric port.

   Invalid authorities are treated as opaque host strings and keep the
   `default-port`."
  [authority-str default-port]
  (let [^String authority-str (or authority-str "")]
    (cond
      (str/blank? authority-str)
      (authority-result "localhost" default-port)

      ;; Host and :authority do not allow userinfo.
      (str/includes? authority-str "@")
      (authority-result authority-str default-port)

      (= \[ (.charAt authority-str 0))
      (if-let [[_ host port-str] (re-matches #"^\[([^\[\]]+)\](?::(\d+))?$"
                                             authority-str)]
        (let [port (when port-str (parse-port port-str))]
          (if (and (valid-ip-literal? host)
                   (or (nil? port-str) (some? port)))
            (authority-result host (or port default-port))
            (authority-result authority-str default-port)))
        (authority-result authority-str default-port))

      :else
      (let [colon (.indexOf authority-str ":")]
        (if (= colon (.lastIndexOf authority-str ":"))
          (let [host (if (neg? colon) authority-str (subs authority-str 0 colon))
                port-str (when-not (neg? colon) (subs authority-str (inc colon)))
                port (when port-str (parse-port port-str))]
            (if (and (re-matches re-reg-name host)
                     (or (nil? port-str) (some? port)))
              (authority-result host (or port default-port))
              (authority-result authority-str default-port)))
          (authority-result authority-str default-port))))))
