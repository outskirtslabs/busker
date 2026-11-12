(ns ol.busker.entrypoint-test
  "The entrypoint of a request is the entrypoint of the listener that accepted
  its connection, whatever the Host header or `:authority` says."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.test-utils :as util]))

(defn- distinct-ports
  [n]
  (loop [ports #{}]
    (if (= n (count ports))
      (vec ports)
      (recur (conj ports (util/free-port))))))

(def ^:private dispatch
  "An `:admin`-only dispatcher ahead of a catch-all; both answer with the
  entrypoint they saw."
  [{:entrypoints #{:admin}
    :terminal? true
    :handler (fn [req]
               {:status 200
                :body (str "admin-only:" (name (:ol.busker/entrypoint req)))})}
   {:handler (fn [req]
               {:status 200
                :body (str "any:" (name (:ol.busker/entrypoint req)))})}])

(defn- start!
  [entrypoints]
  (busker/start! (util/with-static-tls {:entrypoints entrypoints
                                        :dispatch dispatch})))

(defn- request
  "Sends one request and returns `[body http-version]`; `host` is sent as the
  Host header (or `:authority`)."
  [scheme proto port host & {:keys [args unix-socket]}]
  (let [result (apply util/curl scheme proto port "/"
                      (cond-> [:max-time 5
                               :args (into ["-H" (str "Host: " host)
                                            "-w" "\n%{http_version}"]
                                           args)]
                        unix-socket (conj :unix-socket unix-socket)))
        out (:out result)
        split (str/last-index-of out "\n")]
    (is (= 0 (:exit result)) (str "curl failed: " (:err result)))
    (if split
      [(subs out 0 split) (subs out (inc split))]
      [out nil])))

(defn- wait-ready!
  [scheme proto port & opts]
  (let [ready (apply util/wait-for-curl-ready! scheme proto port "/" opts)]
    (is (= 0 (:exit ready)) (str "listener not ready: " (:err ready)))))

(deftest forged-host-port-test
  (testing "a Host port naming another listener does not select that listener's entrypoint"
    (let [[public admin] (distinct-ports 2)
          server (start! {:public {:bind (str "127.0.0.1:" public) :tls false}
                          :admin {:bind (str "127.0.0.1:" admin) :tls false}})]
      (try
        (wait-ready! :http :h1 public)
        (wait-ready! :http :h1 admin)
        (is (= ["any:public" "1.1"]
               (request :http :h1 public (str "x:" admin)))
            "the admin-only dispatcher must not run for the public listener")
        (is (= ["admin-only:admin" "1.1"]
               (request :http :h1 admin (str "x:" public))))
        (finally
          (busker/stop! server))))))

(deftest host-without-port-test
  (testing "each of two plain entrypoints receives the requests of its listener"
    (let [[one admin] (distinct-ports 2)
          server (start! {:one {:bind (str "127.0.0.1:" one) :tls false}
                          :admin {:bind (str "127.0.0.1:" admin) :tls false}})]
      (try
        (wait-ready! :http :h1 one)
        (wait-ready! :http :h1 admin)
        (is (= ["any:one" "1.1"] (request :http :h1 one "example")))
        (is (= ["admin-only:admin" "1.1"] (request :http :h1 admin "example")))
        (finally
          (busker/stop! server))))))

(deftest plain-and-tls-test
  (let [[plain secure] (distinct-ports 2)
        server (start! {:plain {:bind (str "127.0.0.1:" plain) :tls false}
                        :admin {:bind (str "127.0.0.1:" secure)
                                :http3? true
                                :tls {:tls-compatibility-mode :modern}}})]
    (try
      (wait-ready! :http :h1 plain)
      (wait-ready! :https :h1 secure)
      (testing "a plaintext request naming the TLS port stays on the plain entrypoint"
        (is (= ["any:plain" "1.1"] (request :http :h1 plain (str "x:" secure))))
        (is (= ["any:plain" "1.1"] (request :http :h1 plain "x:443"))))
      (testing "HTTP/1.1 over TLS naming the plain port stays on the TLS entrypoint"
        (is (= ["admin-only:admin" "1.1"]
               (request :https :h1 secure (str "localhost.examp1e.net:" plain)))))
      (testing "HTTP/2 over TLS with a forged :authority port"
        (is (= ["admin-only:admin" "2"]
               (request :https :h2 secure (str "localhost.examp1e.net:" plain)))))
      (testing "HTTP/2 with prior knowledge on the plain listener"
        (is (= ["any:plain" "2"]
               (request :http nil plain (str "x:" secure)
                        :args ["--http2-prior-knowledge"]))))
      (testing "HTTP/1.1 upgraded to HTTP/2 on the plain listener"
        (is (= ["any:plain" "2"]
               (request :http :h2 plain (str "x:" secure)))))
      (testing "HTTP/3 with a forged :authority port"
        (wait-ready! :https :h3 secure)
        (is (= ["admin-only:admin" "3"]
               (request :https :h3 secure (str "localhost.examp1e.net:" plain)))))
      (finally
        (busker/stop! server)))))

(deftest unix-socket-test
  (testing "requests on a Unix socket listener get the Unix socket's entrypoint"
    (let [[public] (distinct-ports 1)
          path (util/temp-unix-socket-path)
          server (start! {:public {:bind (str "127.0.0.1:" public) :tls false}
                          :admin {:bind (str "unix:" path) :tls false}})]
      (try
        (wait-ready! :http :h1 public)
        (wait-ready! :http :h1 nil :unix-socket path)
        (is (= ["admin-only:admin" "1.1"]
               (request :http :h1 nil (str "x:" public) :unix-socket path)))
        (is (= ["any:public" "1.1"]
               (request :http :h1 public "localhost")))
        (finally
          (busker/stop! server))))))
