(ns ol.h2o.websocket-test
  "WebSocket integration tests.
   
   Note: These tests are currently disabled because WebSocket support requires
   h2o to be built with wslay. Once the h2o-zig dependency is updated to include
   websocket support, these tests can be enabled by removing the ^:integration
   metadata and implementing a WebSocket client for testing."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [ol.h2o.server :as server]
   [ol.h2o.websocket :as ws]
   [ring.websocket.protocols :as wsp])
  (:import
   [java.nio ByteBuffer]
   [java.util.concurrent CountDownLatch TimeUnit]))

(defn- await-latch
  "Wait for a CountDownLatch with timeout"
  [^CountDownLatch latch timeout-ms]
  (.await latch timeout-ms TimeUnit/MILLISECONDS))

(deftest ^:integration test-websocket-echo
  (testing "WebSocket echo server"
    (let [received (atom [])
          opened? (atom false)
          closed? (atom false)
          open-latch (CountDownLatch. 1)
          message-latch (CountDownLatch. 2)
          close-latch (CountDownLatch. 1)
          
          ;; Create echo handler
          handler (fn [request]
                    (if (ws/websocket-handshake? (:req request))
                      (ws/websocket-response
                       {:on-open (fn [socket]
                                   (reset! opened? true)
                                   (.countDown open-latch))
                        :on-message (fn [socket message]
                                      (swap! received conj message)
                                      (.countDown message-latch)
                                      ;; Echo back
                                      (wsp/-send socket (str "Echo: " message)))
                        :on-close (fn [socket code reason]
                                    (reset! closed? true)
                                    (.countDown close-latch))})
                      {:status 200
                       :headers {"content-type" "text/plain"}
                       :body "Not a WebSocket request"}))
          
          ;; Start server
          server (server/run-server handler {:port 0})]
      
      (try
        ;; TODO: Implement WebSocket client connection and test
        ;; For now, just verify the handler structure
        (is (fn? handler))
        (is (= {:status 200
                :headers {"content-type" "text/plain"}
                :body "Not a WebSocket request"}
               (handler {:req nil})))
        
        (finally
          (server/stop-server server))))))

(deftest ^:integration test-websocket-binary
  (testing "WebSocket binary messages"
    (let [received (atom [])
          message-latch (CountDownLatch. 1)
          
          handler (fn [request]
                    (if (ws/websocket-handshake? (:req request))
                      (ws/websocket-response
                       {:on-message (fn [socket message]
                                      (swap! received conj message)
                                      (.countDown message-latch)
                                      ;; Echo back binary
                                      (when (instance? ByteBuffer message)
                                        (wsp/-send socket message)))})
                      {:status 404 :body "Not found"}))
          
          server (server/run-server handler {:port 0})]
      
      (try
        ;; TODO: Implement WebSocket client with binary message support
        (is (fn? handler))
        
        (finally
          (server/stop-server server))))))

(deftest ^:integration test-websocket-ping-pong
  (testing "WebSocket ping/pong"
    (let [pong-received? (atom false)
          pong-latch (CountDownLatch. 1)
          
          handler (fn [request]
                    (if (ws/websocket-handshake? (:req request))
                      (ws/websocket-response
                       {:on-open (fn [socket]
                                   ;; Send ping on open
                                   (wsp/-ping socket (ByteBuffer/wrap (.getBytes "ping-data" "UTF-8"))))
                        :on-pong (fn [socket data]
                                   (reset! pong-received? true)
                                   (.countDown pong-latch))})
                      {:status 404 :body "Not found"}))
          
          server (server/run-server handler {:port 0})]
      
      (try
        ;; TODO: Implement WebSocket client with ping/pong support
        (is (fn? handler))
        
        (finally
          (server/stop-server server))))))

(deftest ^:integration test-websocket-close
  (testing "WebSocket close with code and reason"
    (let [close-code (atom nil)
          close-reason (atom nil)
          close-latch (CountDownLatch. 1)
          
          handler (fn [request]
                    (if (ws/websocket-handshake? (:req request))
                      (ws/websocket-response
                       {:on-open (fn [socket]
                                   ;; Close immediately with custom code
                                   (wsp/-close socket 1001 "Going away"))
                        :on-close (fn [socket code reason]
                                    (reset! close-code code)
                                    (reset! close-reason reason)
                                    (.countDown close-latch))})
                      {:status 404 :body "Not found"}))
          
          server (server/run-server handler {:port 0})]
      
      (try
        ;; TODO: Implement WebSocket client to test close
        (is (fn? handler))
        
        (finally
          (server/stop-server server))))))

(deftest test-websocket-response-structure
  (testing "WebSocket response structure"
    (let [listener {:on-open (fn [_] nil)
                    :on-message (fn [_ _] nil)
                    :on-close (fn [_ _ _] nil)}
          response (ws/websocket-response listener)]
      
      (is (= 101 (:status response)))
      (is (map? (:headers response)))
      (is (= listener (::ws/listener response))))))

(deftest test-websocket-connection-protocol
  (testing "WebSocketConnection implements Socket protocol"
    (let [conn (ws/->WebSocketConnection
                nil
                (java.util.concurrent.atomic.AtomicBoolean. true)
                {})]
      
      (is (satisfies? wsp/Socket conn))
      (is (satisfies? wsp/AsyncSocket conn))
      (is (wsp/-open? conn)))))

(comment
  ;; Manual testing with a real WebSocket client
  ;; 
  ;; 1. Start a test server:
  (def test-server
    (server/run-server
     (fn [request]
       (if (ws/websocket-handshake? (:req request))
         (ws/websocket-response
          {:on-open (fn [socket]
                      (println "WebSocket opened"))
           :on-message (fn [socket message]
                         (println "Received:" message)
                         (wsp/-send socket (str "Echo: " message)))
           :on-close (fn [socket code reason]
                       (println "WebSocket closed:" code reason))})
         {:status 200
          :headers {"content-type" "text/plain"}
          :body "Use WebSocket client to connect"}))
     {:port 8080}))
  
  ;; 2. Connect with a WebSocket client (e.g., wscat):
  ;;    $ wscat -c ws://localhost:8080
  ;;    > Hello
  ;;    < Echo: Hello
  
  ;; 3. Stop the server:
  (server/stop-server test-server))
