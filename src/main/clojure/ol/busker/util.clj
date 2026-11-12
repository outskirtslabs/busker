(ns ^:no-doc ol.busker.util
  "Internal parsing and compile-time utility functions.

  Key functions parse listener ports and addresses, normalize values, and provide
  [[compile-if]] for optional compile-time forms.

  ## Related Namespaces

  - [[ol.busker.config]] compiles server configuration."
  (:require
   [clojure.string :as str]
   [ol.clave.crypto.impl.parse-ip :as parse-ip]))

(set! *warn-on-reflection* true)

(defmacro compile-if
  "Evaluates `test`. If it returns logical true (and doesn't throw), expands
  to `then`, otherwise expands to `else`."
  {:style/indent 1}
  [test then else]
  (if (try (eval test) (catch Throwable _ false))
    `(do ~then)
    `(do ~else)))

(def ^:private re-ipvfuture
  #"(?i)^v[0-9A-F]+\.[A-Za-z0-9._~!$&'()*+,;=:-]+$")

(defn parse-port
  "Returns the port number of the ASCII digit string `port-str` when it is 0 to
  65535, or nil. A digit string too large for a long is out of range as well."
  [port-str]
  (when (re-matches #"\d+" port-str)
    (when-let [port (parse-long port-str)]
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

(defn- reg-name-char?
  "True when the character code `c` is an ASCII letter or digit or one of
  `-._~!$&'()*+,;=`, the characters an RFC 3986 reg-name allows unencoded."
  [^long c]
  (or (and (<= 97 c) (<= c 122))
      (and (<= 65 c) (<= c 90))
      (and (<= 48 c) (<= c 57))
      (case c
        (46 95 126 33 36 38 39 40 41 42 43 44 59 61 45) true
        false)))

(defn- hex-digit?
  [^long c]
  (or (and (<= 48 c) (<= c 57)) (and (<= 65 c) (<= c 70)) (and (<= 97 c) (<= c 102))))

(defn- reg-name-prefix?
  "True when the first `end` characters of `s` form a non-empty RFC 3986
  reg-name: characters accepted by [[reg-name-char?]] and `%` followed by two
  hexadecimal digits."
  [^String s ^long end]
  (and (pos? end)
       (loop [i 0]
         (if (= i end)
           true
           (let [c (int (.charAt s i))]
             (cond
               (reg-name-char? c) (recur (inc i))

               (and (= c 37)
                    (< (+ i 2) end)
                    (hex-digit? (int (.charAt s (inc i))))
                    (hex-digit? (int (.charAt s (+ i 2)))))
               (recur (+ i 3))

               :else false))))))

(defn- authority-port
  "Returns what [[parse-port]] returns for the characters of `s` from `start`."
  [^String s ^long start]
  (let [end (.length s)
        length (- end start)]
    (if (and (<= 1 length) (<= length 5))
      (loop [i start
             port 0]
        (if (= i end)
          (when (<= port 65535)
            port)
          (let [digit (- (int (.charAt s i)) 48)]
            (when (and (<= 0 digit) (<= digit 9))
              (recur (inc i) (+ (* 10 port) digit))))))
      (parse-port (subs s start)))))

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
          (let [port (when-not (neg? colon) (authority-port authority-str (inc colon)))]
            (if (and (reg-name-prefix? authority-str
                                       (if (neg? colon) (.length authority-str) colon))
                     (or (neg? colon) (some? port)))
              (authority-result (if (neg? colon) authority-str (subs authority-str 0 colon))
                                (or port default-port))
              (authority-result authority-str default-port)))
          (authority-result authority-str default-port))))))
