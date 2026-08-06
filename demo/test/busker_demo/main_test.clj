(ns busker-demo.main-test
  (:require
   [busker-demo.main :as main]
   [clojure.test :refer [deftest is testing]]
   [ol.busker.config :as busker-config]))

(deftest handler-test
  (testing "GET / returns a plain text greeting"
    (is (= {:status 200
            :headers {"content-type" "text/plain"}
            :body "Hello from Busker.\n"}
           (main/handler {:request-method :get
                          :uri "/"}))))
  (testing "unknown GET routes return 404"
    (is (= {:status 404
            :headers {"content-type" "text/plain"}
            :body "Not found.\n"}
           (main/handler {:request-method :get
                          :uri "/missing"}))))
  (testing "non-GET requests return 405"
    (is (= {:status 405
            :headers {"allow" "GET"
                      "content-type" "text/plain"}
            :body "Method not allowed.\n"}
           (main/handler {:request-method :post
                          :uri "/"})))))

(deftest config-test
  (let [config (main/config)]
    (testing "uses Busker's default ACME issuer"
      (is (= {:manage ["busker.outskirtslabs.com"]}
             (get-in config [:tls :certificates])))
      (is (not (contains? (:tls config) :issuers))))
    (testing "uses portable dual-stack wildcard binds"
      (is (= {:binds {:http ":80"
                      :https ":443"}
              :listeners [{:entrypoint :http
                           :host "0.0.0.0"
                           :port 80}
                          {:entrypoint :http
                           :host "::"
                           :port 80}
                          {:entrypoint :https
                           :host "0.0.0.0"
                           :port 443}
                          {:entrypoint :https
                           :host "::"
                           :port 443}]}
             {:binds (update-vals (:entrypoints config) :bind)
              :listeners (->> config
                              busker-config/normalized-snapshot
                              :listeners
                              (mapv #(select-keys % [:entrypoint :host :port])))})))))
