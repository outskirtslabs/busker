(ns ol.busker.listen-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [coffi.mem :as mem]
   [ol.busker.listen :as listen]
   [ol.busker.native.socket :as socket]
   [ol.busker.test-utils :as util])
  (:import
   java.io.File
   java.net.BindException
   java.net.InetSocketAddress
   java.nio.channels.DatagramChannel
   java.nio.channels.ServerSocketChannel))

(defn- tcp-bindable?
  ([port]
   (tcp-bindable? "127.0.0.1" port))
  ([host port]
   (try
     (with-open [channel (ServerSocketChannel/open)]
       (.bind channel (InetSocketAddress. ^String host (int port)))
       true)
     (catch BindException _
       false))))

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
          (release! claim-a)
          (is (not (tcp-bindable? (:port spec)))
              "TCP port should remain bound until the last claim is released")
          (finally
            (release! claim-b))))
      (is (tcp-bindable? (:port spec))
          "TCP port should become available after the final claim is released"))))

(deftest pooled-ipv6-tcp-claim-retains-port-until-final-release-test
  (testing "IPv6 TCP claims keep the bound port unavailable until the final release"
    (let [open-pool (requiring-resolve 'ol.busker.listen/open-pool)
          acquire-claim (requiring-resolve 'ol.busker.listen/acquire-claim)
          release! (requiring-resolve 'ol.busker.listen/release!)
          pool (open-pool)
          spec {:transport :tcp
                :host "::1"
                :port 18463}]
      (is (tcp-bindable? "::1" (:port spec))
          "IPv6 port should be available before the first TCP claim")
      (let [claim-a (acquire-claim pool spec)
            claim-b (acquire-claim pool spec)]
        (try
          (is (not (tcp-bindable? "::1" (:port spec)))
              "IPv6 TCP port should stay bound while claims exist")
          (release! claim-a)
          (is (not (tcp-bindable? "::1" (:port spec)))
              "IPv6 TCP port should remain bound until the last claim is released")
          (finally
            (release! claim-b))))
      (is (tcp-bindable? "::1" (:port spec))
          "IPv6 TCP port should become available after the final claim is released"))))

(deftest pooled-claims-expose-live-resources-test
  (testing "Claims return the live pooled resource for reuse"
    (let [pool (listen/open-pool)
          tcp-spec {:transport :tcp
                    :host "127.0.0.1"
                    :port (util/free-port)}
          udp-port (loop [port (util/free-port)]
                     (if (= port (:port tcp-spec))
                       (recur (util/free-port))
                       port))
          udp-spec {:transport :udp
                    :host "127.0.0.1"
                    :port udp-port}
          tcp-claim-a (listen/acquire-claim pool tcp-spec)
          tcp-claim-b (listen/acquire-claim pool tcp-spec)
          udp-claim-a (listen/acquire-claim pool udp-spec)
          udp-claim-b (listen/acquire-claim pool udp-spec)]
      (try
        (is (int? (listen/resource tcp-claim-a))
            "TCP claim should expose the pooled listener fd")
        (is (pos? (listen/resource tcp-claim-a))
            "TCP claim should expose a valid listener fd")
        (is (= (listen/resource tcp-claim-a)
               (listen/resource tcp-claim-b))
            "Repeated TCP claims should share the same pooled listener fd")
        (is (some? (listen/resource udp-claim-a))
            "UDP claim should expose the pooled listener pointer")
        (is (not (mem/null? (listen/resource udp-claim-a)))
            "UDP claim should expose a non-null pooled listener pointer")
        (is (identical? (listen/resource udp-claim-a)
                        (listen/resource udp-claim-b))
            "Repeated UDP claims should share the same pooled listener pointer")
        (finally
          (listen/release! tcp-claim-a)
          (listen/release! tcp-claim-b)
          (listen/release! udp-claim-a)
          (listen/release! udp-claim-b))))))

(deftest pooled-udp-claim-retains-port-until-final-release-test
  (testing "UDP claims keep the bound port unavailable until the final release"
    (let [open-pool (requiring-resolve 'ol.busker.listen/open-pool)
          acquire-claim (requiring-resolve 'ol.busker.listen/acquire-claim)
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
          (release! claim-a)
          (is (not (udp-bindable? (:port spec)))
              "UDP port should remain bound until the last claim is released")
          (finally
            (release! claim-b))))
      (is (udp-bindable? (:port spec))
          "UDP port should become available after the final claim is released"))))

(defn- temp-unix-abstract-name
  []
  (str "@busker-listen-test-" (System/nanoTime)))

(deftest pooled-unix-claim-retains-path-until-final-release-test
  (testing "Unix socket claims keep the socket path alive until the final release"
    (let [open-pool (requiring-resolve 'ol.busker.listen/open-pool)
          acquire-claim (requiring-resolve 'ol.busker.listen/acquire-claim)
          release! (requiring-resolve 'ol.busker.listen/release!)
          pool (open-pool)
          path (util/temp-unix-socket-path)
          spec {:transport :tcp
                :unix path}]
      (let [claim-a (acquire-claim pool spec)
            claim-b (acquire-claim pool spec)]
        (try
          (is (.exists (File. path))
              "Filesystem unix socket path should exist while the claim is active")
          (is (= (listen/resource claim-a)
                 (listen/resource claim-b))
              "Repeated unix claims should reuse the same pooled listener")
          (release! claim-a)
          (is (.exists (File. path))
              "Filesystem unix socket path should remain until the last claim is released")
          (finally
            (release! claim-b))))
      (is (not (.exists (File. path)))
          "Filesystem unix socket path should be removed after the final release"))))

(deftest pooled-abstract-unix-claim-has-no-filesystem-side-effects-test
  (testing "Abstract unix socket claims never create filesystem paths"
    (let [open-pool (requiring-resolve 'ol.busker.listen/open-pool)
          acquire-claim (requiring-resolve 'ol.busker.listen/acquire-claim)
          release! (requiring-resolve 'ol.busker.listen/release!)
          pool (open-pool)
          path (temp-unix-abstract-name)
          spec {:transport :tcp
                :unix path}
          fs-path (File. path)
          claim-a (acquire-claim pool spec)
          claim-b (acquire-claim pool spec)]
      (try
        (is (= (listen/resource claim-a)
               (listen/resource claim-b))
            "Repeated abstract unix claims should reuse the same pooled listener")
        (is (not (.exists fs-path))
            "Abstract unix sockets should not create a filesystem path while active")
        (finally
          (release! claim-a)
          (release! claim-b)))
      (is (not (.exists fs-path))
          "Abstract unix sockets should not create a filesystem path after release"))))

(deftest pooled-unix-claim-replaces-stale-socket-file-test
  (testing "Unix socket claims replace a stale socket file before binding"
    (let [open-pool (requiring-resolve 'ol.busker.listen/open-pool)
          acquire-claim (requiring-resolve 'ol.busker.listen/acquire-claim)
          release! (requiring-resolve 'ol.busker.listen/release!)
          pool (open-pool)
          path (util/temp-unix-socket-path)
          spec {:transport :tcp
                :unix path}
          stale-fd (socket/open-unix-listener {:path path})]
      (socket/close-fd! stale-fd)
      (is (.exists (File. path))
          "Closing the stale unix listener should leave a socket file behind")
      (let [claim (acquire-claim pool spec)]
        (try
          (is (.exists (File. path))
              "Acquiring a fresh unix claim should recreate the socket file")
          (is (pos? (listen/resource claim))
              "Acquiring a fresh unix claim should return a live listener fd")
          (finally
            (release! claim))))
      (is (not (.exists (File. path)))
          "Releasing the fresh unix claim should clean up the recreated socket path"))))

(deftest pooled-unix-claim-refuses-preexisting-nonsocket-path-test
  (testing "Unix socket claims refuse to overwrite a pre-existing non-socket path"
    (let [open-pool (requiring-resolve 'ol.busker.listen/open-pool)
          acquire-claim (requiring-resolve 'ol.busker.listen/acquire-claim)
          pool (open-pool)
          file (File/createTempFile "busker-listen-test-regular-file-" ".sock")
          path (.getAbsolutePath file)
          spec {:transport :tcp
                :unix path}
          _ (spit file "not-a-socket")]
      (try
        (let [error (try
                      (acquire-claim pool spec)
                      nil
                      (catch clojure.lang.ExceptionInfo e
                        e))]
          (is (some? error)
              "Acquiring a unix claim over a non-socket path should fail")
          (when error
            (is (= :listener-acquisition (:stage (ex-data error)))
                "Failure should be reported at the listener acquisition stage")
            (is (= :eaddrinuse (:errno (ex-data error)))
                "Failure should preserve the native EADDRINUSE cause")))
        (is (.exists file)
            "A non-socket path must not be deleted on listener acquisition failure")
        (is (= "not-a-socket" (slurp file))
            "A non-socket path must not be overwritten on listener acquisition failure")
        (is (= {} @(:state pool))
            "A failed acquisition must not leave a pooled entry behind")
        (finally
          (.delete file))))))

(deftest pooled-unix-final-cleanup-skips-replacement-nonsocket-file-test
  (testing "Final unix cleanup leaves a replacement non-socket file alone"
    (let [open-pool (requiring-resolve 'ol.busker.listen/open-pool)
          acquire-claim (requiring-resolve 'ol.busker.listen/acquire-claim)
          release! (requiring-resolve 'ol.busker.listen/release!)
          pool (open-pool)
          path (util/temp-unix-socket-path)
          spec {:transport :tcp
                :unix path}
          original-unlink! socket/unlink-unix-socket-if-still-socket!
          replacement-body "replacement-file"
          claim (acquire-claim pool spec)]
      (try
        (with-redefs [socket/unlink-unix-socket-if-still-socket!
                      (fn [p]
                        (original-unlink! p)
                        (spit p replacement-body)
                        (original-unlink! p))]
          (release! claim))
        (is (.exists (File. path))
            "A replacement non-socket file should survive final unix cleanup")
        (is (= replacement-body (slurp path))
            "Final unix cleanup should not delete or overwrite a replacement file")
        (finally
          (.delete (File. path)))))))
