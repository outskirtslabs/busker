(ns ol.h2o.websocket
  "WebSocket support for h2o server.
   
   This namespace provides Ring-compatible WebSocket support following the
   ring.websocket.protocols specification. The implementation uses libh2o's
   native WebSocket support via FFI.
   
   Note: WebSocket support requires h2o to be built with wslay. The current
   h2o-zig dependency does not include websocket support, so these functions
   are currently stubs that will be fully implemented once the dependency
   is updated."
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.h2o.native :as h2o]
   [ring.websocket.protocols :as ws])
  (:import
   [java.nio ByteBuffer]
   [java.util.concurrent.atomic AtomicBoolean]))

(set! *warn-on-reflection* true)

;; WebSocket opcodes from wslay
(def ^:const WSLAY_TEXT_FRAME 0x1)
(def ^:const WSLAY_BINARY_FRAME 0x2)
(def ^:const WSLAY_CLOSE 0x8)
(def ^:const WSLAY_PING 0x9)
(def ^:const WSLAY_PONG 0xA)

;; WebSocket close codes
(def ^:const WS_CLOSE_NORMAL 1000)
(def ^:const WS_CLOSE_GOING_AWAY 1001)
(def ^:const WS_CLOSE_PROTOCOL_ERROR 1002)
(def ^:const WS_CLOSE_UNSUPPORTED_DATA 1003)
(def ^:const WS_CLOSE_NO_STATUS 1005)
(def ^:const WS_CLOSE_ABNORMAL 1006)
(def ^:const WS_CLOSE_INVALID_PAYLOAD 1007)
(def ^:const WS_CLOSE_POLICY_VIOLATION 1008)
(def ^:const WS_CLOSE_MESSAGE_TOO_BIG 1009)
(def ^:const WS_CLOSE_MANDATORY_EXTENSION 1010)
(def ^:const WS_CLOSE_INTERNAL_ERROR 1011)
(def ^:const WS_CLOSE_SERVICE_RESTART 1012)
(def ^:const WS_CLOSE_TRY_AGAIN_LATER 1013)

;; FFI function definitions (stubs until h2o-zig is updated)
(ffi/defcfn is-websocket-handshake
  "Check if request is a WebSocket handshake"
  clj_h2o_is_websocket_handshake
  [::mem/pointer ::mem/pointer] ::mem/int)

(ffi/defcfn upgrade-to-websocket-native
  "Upgrade HTTP request to WebSocket"
  clj_h2o_upgrade_to_websocket
  [::mem/pointer ::mem/c-string ::mem/pointer ::mem/pointer] ::mem/pointer)

(ffi/defcfn websocket-send
  "Send text or binary message via WebSocket"
  clj_h2o_websocket_send
  [::mem/pointer ::mem/char ::mem/pointer ::mem/long] ::mem/int)

(ffi/defcfn websocket-ping
  "Send ping message via WebSocket"
  clj_h2o_websocket_ping
  [::mem/pointer ::mem/pointer ::mem/long] ::mem/int)

(ffi/defcfn websocket-pong
  "Send pong message via WebSocket"
  clj_h2o_websocket_pong
  [::mem/pointer ::mem/pointer ::mem/long] ::mem/int)

(ffi/defcfn websocket-close
  "Close WebSocket connection"
  clj_h2o_websocket_close
  [::mem/pointer ::mem/short ::mem/c-string ::mem/long] ::mem/void)

(defn websocket-handshake?
  "Returns true if the request is a valid WebSocket handshake request.
   Returns the client key if valid, nil otherwise."
  [req-ptr]
  (when (and req-ptr (not (mem/null? req-ptr)))
    (with-open [arena (mem/confined-arena)]
      (let [client-key-ptr-seg (mem/alloc-instance ::mem/pointer arena)
            result (is-websocket-handshake req-ptr client-key-ptr-seg)]
        (when (zero? result)
          (let [key-ptr (mem/read-address client-key-ptr-seg)]
            (when-not (mem/null? key-ptr)
              ;; Read C string - WebSocket keys are always 24 bytes
              (String. (mem/read-bytes (mem/reinterpret key-ptr 24) 24) "UTF-8"))))))))

(defrecord WebSocketConnection [conn-ptr
                                 ^AtomicBoolean open?
                                 listener
                                 arena]  ;; Keep arena alive for callbacks
  ws/Socket
  (-open? [_]
    (.get open?))

  (-send [_ message]
    (when (.get open?)
      (with-open [arena (mem/confined-arena)]
        (let [opcode (if (string? message) WSLAY_TEXT_FRAME WSLAY_BINARY_FRAME)
              data (if (string? message)
                     (.getBytes ^String message "UTF-8")
                     (if (instance? ByteBuffer message)
                       (let [bb ^ByteBuffer message
                             arr (byte-array (.remaining bb))]
                         (.get bb arr)
                         arr)
                       message))
              data-seg (mem/serialize data [::mem/array ::mem/char (count data)] arena)]
          (websocket-send conn-ptr opcode data-seg (count data))))))

  (-ping [_ data]
    (when (.get open?)
      (with-open [arena (mem/confined-arena)]
        (let [data-bytes (cond
                           (instance? ByteBuffer data)
                           (let [bb ^ByteBuffer data
                                 arr (byte-array (.remaining bb))]
                             (.get bb arr)
                             arr)
                           (bytes? data) data
                           :else (byte-array 0))
              data-seg (mem/serialize data-bytes [::mem/array ::mem/char (count data-bytes)] arena)]
          (websocket-ping conn-ptr data-seg (count data-bytes))))))

  (-pong [_ data]
    (when (.get open?)
      (with-open [arena (mem/confined-arena)]
        (let [data-bytes (cond
                           (instance? ByteBuffer data)
                           (let [bb ^ByteBuffer data
                                 arr (byte-array (.remaining bb))]
                             (.get bb arr)
                             arr)
                           (bytes? data) data
                           :else (byte-array 0))
              data-seg (mem/serialize data-bytes [::mem/array ::mem/char (count data-bytes)] arena)]
          (websocket-pong conn-ptr data-seg (count data-bytes))))))

  (-close [_ code reason]
    (when (.compareAndSet open? true false)
      (with-open [arena (mem/confined-arena)]
        (let [reason-bytes (when reason (.getBytes ^String reason "UTF-8"))
              reason-len (if reason-bytes (count reason-bytes) 0)]
          (if reason-bytes
            (let [reason-seg (mem/serialize reason-bytes [::mem/array ::mem/char reason-len] arena)]
              (websocket-close conn-ptr code reason-seg reason-len))
            (websocket-close conn-ptr code (mem/as-segment 0) 0))))))

  ws/AsyncSocket
  (-send-async [this message succeed fail]
    (try
      (ws/-send this message)
      (when succeed (succeed))
      (catch Exception e
        (when fail (fail e))))))

(defn upgrade-to-websocket
  "Upgrade an HTTP request to a WebSocket connection.
   
   Parameters:
   - req-ptr: pointer to h2o_req_t
   - client-key: WebSocket client key from handshake
   - listener: object implementing ring.websocket.protocols/Listener
   
   Returns: WebSocketConnection record or nil on failure"
  [req-ptr client-key listener]
  (let [arena (mem/shared-arena)  ;; Shared arena keeps callbacks alive
        open? (AtomicBoolean. true)
        conn (->WebSocketConnection nil open? listener arena)
        ;; For now, pass NULL callback - we'll implement message handling later
        conn-ptr (upgrade-to-websocket-native req-ptr client-key (mem/as-segment 0) (mem/as-segment 0))]
    (when-not (mem/null? conn-ptr)
      (assoc conn :conn-ptr conn-ptr))))

(defn handle-websocket-upgrade!
  "Handle WebSocket upgrade for a request.
   
   Parameters:
   - req: Request record containing req-ctx-ptr
   - listener: WebSocket listener (map or object implementing Listener protocol)
   
   This function performs the WebSocket upgrade and calls the listener's on-open callback."
  [req listener]
  (let [req-ptr (:req-ctx-ptr req)]
    (when-let [client-key (websocket-handshake? req-ptr)]
      (when-let [socket (upgrade-to-websocket req-ptr client-key listener)]
        ;; Call on-open callback
        (try
          (when-let [on-open-fn (if (map? listener)
                                  (:on-open listener)
                                  (when (satisfies? ws/Listener listener)
                                    #(ws/on-open listener %)))]
            (on-open-fn socket))
          (catch Exception e
            (println "Error in WebSocket on-open:" e)
            (.printStackTrace e)))
        true))))

(defn websocket-response
  "Create a Ring response map for WebSocket upgrade.
   
   The listener should implement ring.websocket.protocols/Listener:
   - (on-open [listener socket])
   - (on-message [listener socket message])
   - (on-pong [listener socket data])
   - (on-error [listener socket throwable])
   - (on-close [listener socket code reason])
   
   Example:
   (websocket-response
     {:on-open (fn [socket] (println \"WebSocket opened\"))
      :on-message (fn [socket msg] (ws/send socket (str \"Echo: \" msg)))
      :on-close (fn [socket code reason] (println \"WebSocket closed\"))})"
  [listener]
  {:status 101
   :headers {}
   ::listener listener})

(comment
  ;; Example usage:
  (defn echo-handler [request]
    (if (websocket-handshake? (:req request))
      (websocket-response
       {:on-open (fn [socket]
                   (println "WebSocket connection opened"))
        :on-message (fn [socket message]
                      (println "Received:" message)
                      (ws/send socket (str "Echo: " message)))
        :on-close (fn [socket code reason]
                    (println "WebSocket closed:" code reason))})
      {:status 200
       :headers {"content-type" "text/plain"}
       :body "Not a WebSocket request"})))
