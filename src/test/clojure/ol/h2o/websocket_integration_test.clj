(ns ol.h2o.websocket-integration-test
  "WebSocket integration tests using Java's WebSocket client."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [ol.h2o.server :as server]
   [ol.h2o.websocket :as ws]
   [ring.websocket.protocols :as wsp])
  (:import
   [java.net URI]
   [java.net.http HttpClient WebSocket WebSocket$Listener]
   [java.nio ByteBuffer]
   [java.time Duration]
   [java.util.concurrent CompletableFuture CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicReference]))

(set! *warn-on-reflection* true)

(defn- await-latch
  "Wait for a CountDownLatch with timeout"
  [^CountDownLatch latch timeout-ms]
  (.await latch timeout-ms TimeUnit/MILLISECONDS))

(deftest test-websocket-echo
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
          
          ;; Start server on a fixed port for testing
          port 18080
          _ (println "Starting server on port" port)
          server-instance (server/run-server handler {:port port})
          _ (do (Thread/sleep 500) (println "Server started"))
          
          ;; Create WebSocket client
          client (HttpClient/newHttpClient)
          client-received (atom [])
          client-latch (CountDownLatch. 2)
          
          listener (reify WebSocket$Listener
                     (onOpen [_ ws]
                       (println "Client: WebSocket opened")
                       (.request ws 1)
                       nil)
                     (onText [_ ws data last?]
                       (println "Client: Received text:" data)
                       (swap! client-received conj data)
                       (.countDown client-latch)
                       (.request ws 1)
                       nil)
                     (onClose [_ ws code reason]
                       (println "Client: WebSocket closed:" code reason)
                       nil)
                     (onError [_ ws error]
                       (println "Client: Error:" error)
                       nil))
          
          ws-future (.buildAsync (.newWebSocketBuilder client)
                                  (URI/create (str "ws://localhost:" port "/"))
                                  listener)]
      
      (try
        ;; Wait for connection
        (let [ws-client (.get ws-future 5 TimeUnit/SECONDS)]
          (is (some? ws-client) "WebSocket client connected")
          
          ;; Wait for server open
          (is (await-latch open-latch 2000) "Server WebSocket opened")
          (is @opened? "Server on-open called")
          
          ;; Send messages
          (.sendText ws-client "Hello" true)
          (.sendText ws-client "World" true)
          
          ;; Wait for echoes
          (is (await-latch client-latch 5000) "Client received echoes")
          (is (= 2 (count @client-received)) "Client received 2 messages")
          (is (= "Echo: Hello" (first @client-received)) "First echo correct")
          (is (= "Echo: World" (second @client-received)) "Second echo correct")
          
          ;; Wait for server to receive
          (is (await-latch message-latch 2000) "Server received messages")
          (is (= 2 (count @received)) "Server received 2 messages")
          (is (= "Hello" (first @received)) "Server received Hello")
          (is (= "World" (second @received)) "Server received World")
          
          ;; Close connection
          (.sendClose ws-client WebSocket/NORMAL_CLOSURE "Test complete")
          
          ;; Wait for close
          (is (await-latch close-latch 2000) "Server WebSocket closed")
          (is @closed? "Server on-close called"))
        
        (finally
          (server/stop-server server-instance))))))

(deftest test-websocket-binary
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
          
          port 18081
          server-instance (server/run-server handler {:port port})
          
          client (HttpClient/newHttpClient)
          client-received (atom [])
          client-latch (CountDownLatch. 1)
          
          listener (reify WebSocket$Listener
                     (onOpen [_ ws]
                       (.request ws 1)
                       nil)
                     (onBinary [_ ws data last?]
                       (let [bytes (byte-array (.remaining data))]
                         (.get data bytes)
                         (swap! client-received conj bytes))
                       (.countDown client-latch)
                       (.request ws 1)
                       nil)
                     (onClose [_ ws code reason]
                       nil)
                     (onError [_ ws error]
                       (println "Client error:" error)
                       nil))
          
          ws-future (.buildAsync (.newWebSocketBuilder client)
                                  (URI/create (str "ws://localhost:" port "/"))
                                  listener)]
      
      (try
        (let [ws-client (.get ws-future 5 TimeUnit/SECONDS)
              test-data (byte-array [1 2 3 4 5])]
          
          ;; Send binary message
          (.sendBinary ws-client (ByteBuffer/wrap test-data) true)
          
          ;; Wait for echo
          (is (await-latch client-latch 5000) "Client received binary echo")
          (is (= 1 (count @client-received)) "Client received 1 message")
          (is (java.util.Arrays/equals test-data (first @client-received))
              "Binary data echoed correctly")
          
          ;; Wait for server to receive
          (is (await-latch message-latch 2000) "Server received binary")
          (is (= 1 (count @received)) "Server received 1 message")
          (is (instance? ByteBuffer (first @received)) "Server received ByteBuffer")
          
          (.sendClose ws-client WebSocket/NORMAL_CLOSURE "Test complete"))
        
        (finally
          (server/stop-server server-instance))))))

(deftest test-websocket-ping-pong
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
          
          port 18081
          server-instance (server/run-server handler {:port port})
          
          client (HttpClient/newHttpClient)
          ping-received? (atom false)
          
          listener (reify WebSocket$Listener
                     (onOpen [_ ws]
                       (.request ws 1)
                       nil)
                     (onPing [_ ws data]
                       (reset! ping-received? true)
                       ;; Send pong back
                       (.sendPong ws data)
                       (.request ws 1)
                       nil)
                     (onClose [_ ws code reason]
                       nil)
                     (onError [_ ws error]
                       (println "Client error:" error)
                       nil))
          
          ws-future (.buildAsync (.newWebSocketBuilder client)
                                  (URI/create (str "ws://localhost:" port "/"))
                                  listener)]
      
      (try
        (let [ws-client (.get ws-future 5 TimeUnit/SECONDS)]
          
          ;; Wait for ping/pong exchange
          (is (await-latch pong-latch 5000) "Server received pong")
          (is @pong-received? "Server on-pong called")
          (is @ping-received? "Client received ping")
          
          (.sendClose ws-client WebSocket/NORMAL_CLOSURE "Test complete"))
        
        (finally
          (server/stop-server server-instance))))))

(deftest test-websocket-close-codes
  (testing "WebSocket close with custom code and reason"
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
          
          port 18081
          server-instance (server/run-server handler {:port port})
          
          client (HttpClient/newHttpClient)
          client-close-code (atom nil)
          client-close-reason (atom nil)
          client-latch (CountDownLatch. 1)
          
          listener (reify WebSocket$Listener
                     (onOpen [_ ws]
                       (.request ws 1)
                       nil)
                     (onClose [_ ws code reason]
                       (reset! client-close-code code)
                       (reset! client-close-reason reason)
                       (.countDown client-latch)
                       nil)
                     (onError [_ ws error]
                       (println "Client error:" error)
                       nil))
          
          ws-future (.buildAsync (.newWebSocketBuilder client)
                                  (URI/create (str "ws://localhost:" port "/"))
                                  listener)]
      
      (try
        (.get ws-future 5 TimeUnit/SECONDS)
        
        ;; Wait for close
        (is (await-latch client-latch 5000) "Client received close")
        (is (= 1001 @client-close-code) "Client received correct close code")
        (is (= "Going away" @client-close-reason) "Client received correct close reason")
        
        (finally
          (server/stop-server server-instance))))))

(comment
  ;; Run individual tests
  (test-websocket-echo)
  (test-websocket-binary)
  (test-websocket-ping-pong)
  (test-websocket-close-codes))
