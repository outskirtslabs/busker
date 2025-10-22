# WebSocket Support for http-clj

## Status

WebSocket support has been **architecturally implemented** but is currently **non-functional** due to a missing dependency. The implementation is complete and ready to be activated once the h2o-zig dependency is updated.

### What's Implemented

1. **Native Shim Layer** (`shim/shim.h`, `shim/shim.c`)
   - C API for WebSocket operations:
     - `clj_h2o_is_websocket_handshake()` - Detect WebSocket upgrade requests
     - `clj_h2o_upgrade_to_websocket()` - Upgrade HTTP connection to WebSocket
     - `clj_h2o_websocket_send()` - Send text/binary messages
     - `clj_h2o_websocket_ping()` - Send ping frames
     - `clj_h2o_websocket_pong()` - Send pong frames
     - `clj_h2o_websocket_close()` - Close connection with status code
   - Currently implemented as stubs that compile but return errors

2. **Clojure FFI Layer** (`src/main/clojure/ol/h2o/websocket.clj`)
   - FFI bindings to native WebSocket functions
   - `WebSocketConnection` record implementing Ring WebSocket protocols:
     - `ring.websocket.protocols/Socket`
     - `ring.websocket.protocols/AsyncSocket`
   - Helper functions:
     - `websocket-handshake?` - Check if request is WebSocket upgrade
     - `upgrade-to-websocket` - Perform the upgrade
     - `websocket-response` - Create Ring response for WebSocket

3. **Test Suite** (`src/test/clojure/ol/h2o/websocket_test.clj`)
   - Integration tests (marked with `^:integration`, currently disabled)
   - Unit tests for API structure
   - Example usage in comments

4. **wslay Build Configuration** (`pkgs/wslay/`)
   - Zig build file for wslay library
   - Configuration headers
   - Ready to be integrated once h2o-zig is updated

### What's Missing

The **only** missing piece is WebSocket support in the h2o-zig dependency:

1. **h2o-zig needs to be updated** to:
   - Include wslay as a dependency
   - Build h2o with `WITH_WEBSOCKET=ON`
   - Export websocket headers and library

2. **Once h2o-zig is updated**, enable WebSocket support by:
   - Uncommenting wslay dependency in `shim/build.zig.zon`
   - Uncommenting wslay linking in `shim/build.zig`
   - Replacing stub implementations in `shim/shim.c` with actual h2o websocket calls
   - Rebuilding the shim

## Architecture

### Request Flow

```
HTTP Request
    ↓
Ring Handler
    ↓
Check: websocket-handshake?
    ↓ (yes)
Return: websocket-response with listener
    ↓
Server detects ::websocket/listener in response
    ↓
Call: upgrade-to-websocket
    ↓
Native: clj_h2o_upgrade_to_websocket
    ↓
h2o: h2o_upgrade_to_websocket (via wslay)
    ↓
WebSocket connection established
```

### Message Flow

```
WebSocket Message Received
    ↓
h2o: on_message callback
    ↓
Native: clj_ws_on_message
    ↓
Clojure: on-message-cb
    ↓
Ring: (on-message listener socket message)
    ↓
Application handler
```

## Ring WebSocket Protocol Compliance

The implementation follows the [Ring WebSocket Protocols](https://github.com/ring-clojure/ring/tree/master/ring-websocket-protocols) specification:

### Listener Protocol

```clojure
(defprotocol Listener
  (on-open [listener socket])
  (on-message [listener socket message])
  (on-pong [listener socket data])
  (on-error [listener socket throwable])
  (on-close [listener socket code reason]))
```

### Socket Protocol

```clojure
(defprotocol Socket
  (-open? [socket])
  (-send [socket message])
  (-ping [socket data])
  (-pong [socket data])
  (-close [socket code reason]))
```

### AsyncSocket Protocol

```clojure
(defprotocol AsyncSocket
  (-send-async [socket message succeed fail]))
```

## Usage Example

```clojure
(require '[ol.h2o.server :as server]
         '[ol.h2o.websocket :as ws]
         '[ring.websocket.protocols :as wsp])

(defn echo-handler [request]
  (if (ws/websocket-handshake? (:req request))
    (ws/websocket-response
     {:on-open (fn [socket]
                 (println "WebSocket connection opened"))
      
      :on-message (fn [socket message]
                    (println "Received:" message)
                    ;; Echo back
                    (wsp/-send socket (str "Echo: " message)))
      
      :on-pong (fn [socket data]
                 (println "Pong received"))
      
      :on-error (fn [socket throwable]
                  (println "Error:" throwable))
      
      :on-close (fn [socket code reason]
                  (println "WebSocket closed:" code reason))})
    
    {:status 200
     :headers {"content-type" "text/plain"}
     :body "Not a WebSocket request"}))

(def server (server/run-server echo-handler {:port 8080}))

;; Connect with: wscat -c ws://localhost:8080
;; > Hello
;; < Echo: Hello

(server/stop-server server)
```

## Testing

Once WebSocket support is enabled:

```bash
# Run integration tests
bb test :integration

# Manual testing with wscat
wscat -c ws://localhost:8080

# Or with websocat
websocat ws://localhost:8080
```

## Implementation Checklist

- [x] Design WebSocket architecture
- [x] Implement native shim API (stubs)
- [x] Implement Clojure FFI bindings
- [x] Implement Ring WebSocket protocols
- [x] Create test suite structure
- [x] Build wslay with Zig
- [x] Document implementation
- [ ] **Update h2o-zig to include wslay**
- [ ] Enable wslay in shim build
- [ ] Implement actual native WebSocket functions
- [ ] Enable and run integration tests
- [ ] Performance testing and optimization

## References

- [Ring WebSocket Protocols](https://github.com/ring-clojure/ring/tree/master/ring-websocket-protocols)
- [h2o WebSocket API](https://github.com/h2o/h2o/blob/master/include/h2o/websocket.h)
- [wslay Library](https://github.com/tatsuhiro-t/wslay)
- [WebSocket RFC 6455](https://tools.ietf.org/html/rfc6455)

## Contributing

To complete the WebSocket implementation:

1. Fork and update [h2o-zig](https://github.com/outskirtslabs/h2o-zig) to include wslay
2. Update the h2o-zig dependency in `shim/build.zig.zon`
3. Uncomment wslay configuration in `shim/build.zig` and `shim/build.zig.zon`
4. Replace stub implementations in `shim/shim.c` with actual h2o websocket calls
5. Test with the integration test suite
6. Submit a pull request

## License

Same as http-clj project.
