(ns ol.busker.listen-test
  (:require
   [clojure.test :refer [deftest is testing]])
  (:import
   java.net.BindException
   java.net.InetSocketAddress
   java.nio.channels.DatagramChannel
   java.nio.channels.ServerSocketChannel))

(defn- tcp-bindable?
  [port]
  (try
    (with-open [channel (ServerSocketChannel/open)]
      (.bind channel (InetSocketAddress. "127.0.0.1" (int port)))
      true)
    (catch BindException _
      false)))

(defn- udp-bindable?
  [port]
  (try
    (with-open [channel (DatagramChannel/open)]
      (.bind channel (InetSocketAddress. "127.0.0.1" (int port)))
      true)
    (catch BindException _
      false)))

(deftest listener-key-normalizes-bind-identity-test
  (testing "Listener keys normalize host defaults and keep transports distinct"
    (let [listener-key (requiring-resolve 'ol.busker.listen/listener-key)]
      (is (= {:transport :tcp
              :host "0.0.0.0"
              :port 18460}
             (listener-key {:transport :tcp
                            :port 18460})))
      (is (= (listener-key {:transport :udp
                            :host "0.0.0.0"
                            :port 18460})
             (listener-key {:transport :udp
                            :port 18460})))
      (is (not= (listener-key {:transport :tcp
                               :port 18460})
                (listener-key {:transport :udp
                               :port 18460}))))))

(deftest pooled-tcp-claim-retains-port-until-final-release-test
  (testing "TCP claims keep the bound port unavailable until the final release"
    (let [open-pool (requiring-resolve 'ol.busker.listen/open-pool)
          acquire-claim (requiring-resolve 'ol.busker.listen/acquire-claim)
          fake-close! (requiring-resolve 'ol.busker.listen/fake-close!)
          release! (requiring-resolve 'ol.busker.listen/release!)
          pool (open-pool)
          spec {:transport :tcp
                :host "127.0.0.1"
                :port 18461}]
      (is (tcp-bindable? (:port spec))
          "Port should be available before the first TCP claim")
      (let [claim-a (acquire-claim pool spec)
            claim-b (acquire-claim pool spec)]
        (try
          (is (not (tcp-bindable? (:port spec)))
              "TCP port should stay bound while claims exist")
          (fake-close! claim-a)
          (release! claim-a)
          (is (not (tcp-bindable? (:port spec)))
              "TCP port should remain bound until the last claim is released")
          (finally
            (release! claim-b))))
      (is (tcp-bindable? (:port spec))
          "TCP port should become available after the final claim is released"))))

(deftest pooled-udp-claim-retains-port-until-final-release-test
  (testing "UDP claims keep the bound port unavailable until the final release"
    (let [open-pool (requiring-resolve 'ol.busker.listen/open-pool)
          acquire-claim (requiring-resolve 'ol.busker.listen/acquire-claim)
          fake-close! (requiring-resolve 'ol.busker.listen/fake-close!)
          release! (requiring-resolve 'ol.busker.listen/release!)
          pool (open-pool)
          spec {:transport :udp
                :host "127.0.0.1"
                :port 18462}]
      (is (udp-bindable? (:port spec))
          "Port should be available before the first UDP claim")
      (let [claim-a (acquire-claim pool spec)
            claim-b (acquire-claim pool spec)]
        (try
          (is (not (udp-bindable? (:port spec)))
              "UDP port should stay bound while claims exist")
          (fake-close! claim-a)
          (release! claim-a)
          (is (not (udp-bindable? (:port spec)))
              "UDP port should remain bound until the last claim is released")
          (finally
            (release! claim-b))))
      (is (udp-bindable? (:port spec))
          "UDP port should become available after the final claim is released"))))