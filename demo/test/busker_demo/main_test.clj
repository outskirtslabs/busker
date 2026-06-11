(ns busker-demo.main-test
  (:require
   [busker-demo.main :as main]
   [clojure.test :refer [deftest is testing]]))

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
