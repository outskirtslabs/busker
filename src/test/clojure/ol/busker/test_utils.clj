;; Copyright © 2017-2022 Rich Hickey, Alex Miller, and contributors
;;
;; All rights reserved. The use and distribution terms for this software are covered by the Eclipse Public License 1.0 which can be found in the file LICENSE at the root of this distribution. By using this software in any fashion, you are agreeing to be bound by the terms of this license. You must not remove this notice, or any other, from this software.
;; https://github.com/clojure/tools.deps/blob/ecc80420c1b734b384f7a42df91684cdbc37ddc6/src/test/clojure/clojure/tools/deps/util.clj
(ns ol.busker.test-utils
  (:require
   [babashka.process :as p]
   [clojure.java.io :as io])
  (:import
   [java.net DatagramSocket InetAddress InetSocketAddress ServerSocket Socket]))

(defn fixture-cert-path
  []
  (.getAbsolutePath (io/file "src/test/fixtures/server.crt")))

(defn fixture-key-path
  []
  (.getAbsolutePath (io/file "src/test/fixtures/server.key")))

(def static-tls
  {:certificates {:load [{:type :pem
                          :cert-file (fixture-cert-path)
                          :key-file (fixture-key-path)}]}})

(defn fixture-cert-pem
  []
  (slurp (fixture-cert-path)))

(defn fixture-key-pem
  []
  (slurp (fixture-key-path)))

(defn fixture-tls-bundle
  []
  {:certificate [(fixture-cert-pem)]
   :private-key (fixture-key-pem)})

(defn deep-merge
  [& values]
  (if (every? map? values)
    (apply merge-with deep-merge values)
    (last values)))

(defn with-static-tls
  [config]
  (update config :tls #(deep-merge static-tls (or % {}))))

(defn with-handler
  [handler config]
  (assoc config :dispatch [{:handler handler}]))

(defn curl
  [scheme proto port path & {:keys [host max-time args]
                             :or {host "127.0.0.1" max-time 10}
                             :as opts}]
  (let [scheme (or (:scheme opts) scheme)
        https? (= :https scheme)
        explicit-host? (contains? opts :host)
        host (if (and https? (not explicit-host?) (= "127.0.0.1" host))
               "localhost.examp1e.net"
               host)
        sni-resolve-args (if (and https? (not explicit-host?)
                                  (= "localhost.examp1e.net" host))
                           ["--resolve" (str host ":" port ":127.0.0.1")]
                           [])
        proto-args (case proto
                     :h1 ["--http1.1"]
                     :h2 ["--http2"]
                     :h3 ["--http3-only"]
                     [])
        default-args ["-k" "-s" "--max-time" (str max-time)]
        url (str (name scheme) "://" host ":" port path)
        curl-args (concat proto-args
                          default-args
                          sni-resolve-args
                          (or args [])
                          [url])]
    (apply p/shell {:out :string :err :string :continue true}
           "curl"
           curl-args)))

(defn wait-for-curl-ready!
  [scheme proto port path & {:keys [host max-time args attempts delay-ms]
                             :or {attempts 30
                                  delay-ms 100
                                  max-time 2}
                             :as _opts}]
  (loop [attempt 0]
    (let [curl-opts (cond-> [:max-time max-time]
                      host (conj :host host)
                      args (conj :args args))
          result (try
                   (apply curl scheme proto port path curl-opts)
                   (catch Throwable t
                     t))]
      (cond
        (and (map? result) (zero? (:exit result)))
        result

        (< attempt (dec attempts))
        (do
          (Thread/sleep delay-ms)
          (recur (inc attempt)))

        (instance? Throwable result)
        (throw result)

        :else
        result))))

(defn wait-for-port-open!
  [host port & {:keys [attempts delay-ms connect-timeout-ms]
                :or {attempts 30
                     delay-ms 100
                     connect-timeout-ms 200}}]
  (loop [attempt 0]
    (let [open?
          (try
            (with-open [socket (Socket.)]
              (.connect socket (InetSocketAddress. ^String host (int port))
                        (int connect-timeout-ms))
              true)
            (catch Throwable _
              false))]
      (cond
        open?
        true

        (< attempt (dec attempts))
        (do
          (Thread/sleep delay-ms)
          (recur (inc attempt)))

        :else
        false))))

(defn free-port
  []
  (let [loopback (InetAddress/getByName "127.0.0.1")]
    (loop [attempt 0]
      (when (>= attempt 32)
        (throw (ex-info "Unable to allocate a free test port"
                        {:attempts attempt})))
      (let [candidate (+ 20000 (rand-int (- 32767 20000)))
            available?
            (try
              (with-open [tcp (ServerSocket. candidate 0 loopback)
                          udp (DatagramSocket. candidate loopback)]
                (.setReuseAddress tcp true)
                (.setReuseAddress udp true)
                true)
              (catch java.net.BindException _
                false))]
        (if available?
          candidate
          (recur (inc attempt)))))))

(defn submap?
  "Is m1 a subset of m2?"
  [m1 m2]
  (if (and (map? m1) (map? m2))
    (every? (fn [[k v]] (and (contains? m2 k)
                             (submap? v (get m2 k))))
            m1)
    (= m1 m2)))

(defn submap-debug?
  "Is m1 a subset of m2?
   Print missing keys or mismatched values."
  [m1 m2]
  (if (and (map? m1) (map? m2))
    (every? (fn [[k v]]
              (when (not (contains? m2 k)) (println "m1 has key, m2 does not: " k))
              (and (contains? m2 k) (submap? v (get m2 k))))
            m1)
    (if (= m1 m2)
      true
      (do
        (println "Nested values don't match, m1 val=" m1 "m2 val=" m2)
        false))))
