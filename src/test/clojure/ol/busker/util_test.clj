(ns ol.busker.util-test
  (:require
   [clojure.test :refer [are deftest testing]]
   [ol.busker.util :as util]))

(deftest parse-authority-test
  (testing "parses valid RFC 9110 host forms"
    (are [authority default-port expected]
         (= expected (util/parse-authority authority default-port))
      nil                   443 {:server-name "localhost"         :server-port 443}
      ""                     80 {:server-name "localhost"         :server-port 80}
      "example.com"          80 {:server-name "example.com"       :server-port 80}
      "example.com:8443"     80 {:server-name "example.com"       :server-port 8443}
      "127.0.0.1:8080"       80 {:server-name "127.0.0.1"         :server-port 8080}
      "foo%20bar.example"    80 {:server-name "foo%20bar.example" :server-port 80}
      "example.com:0"        80 {:server-name "example.com"       :server-port 0}
      "example.com:65535"    80 {:server-name "example.com"       :server-port 65535}
      "[::1]"                80 {:server-name "::1"               :server-port 80}
      "[2001:db8::1]:9443"   80 {:server-name "2001:db8::1"       :server-port 9443}
      "[v1.fe80::1]:443"     80 {:server-name "v1.fe80::1"        :server-port 443}))

  (testing "treats malformed authorities as opaque values with the default port"
    (are [authority default-port]
         (= {:server-name authority
             :server-port default-port}
            (util/parse-authority authority default-port))
      "::1"                80
      "example.com:abc"    80
      "example.com:65536"  80
      "user@example.com"   80
      ":8080"              80
      "[::1"               80
      "[]:443"             80
      "[::1]:abc"          80
      "[::1]:65536"        80
      "[::1]extra"         80
      "exa mple"           80
      "[::1%lo0]:443"      80)))
