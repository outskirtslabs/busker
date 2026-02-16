;; Copyright © 2017-2022 Rich Hickey, Alex Miller, and contributors
;;
;; All rights reserved. The use and distribution terms for this software are covered by the Eclipse Public License 1.0 which can be found in the file LICENSE at the root of this distribution. By using this software in any fashion, you are agreeing to be bound by the terms of this license. You must not remove this notice, or any other, from this software.
;; https://github.com/clojure/tools.deps/blob/ecc80420c1b734b384f7a42df91684cdbc37ddc6/src/test/clojure/clojure/tools/deps/util.clj
(ns ol.busker.test-utils
  (:require
   [babashka.process :as p]))

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
