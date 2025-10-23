# WebSocket Implementation Status

## Current State: Partially Working

The WebSocket implementation is functional but has a critical issue with request lifecycle management that causes a crash after the WebSocket upgrade completes.

### ✅ What Works

1. **WebSocket Handshake Detection** - Server correctly identifies WebSocket upgrade requests
2. **WebSocket Upgrade** - Successfully upgrades HTTP connection to WebSocket
3. **Client Connection** - WebSocket clients can connect successfully
4. **Protocol Implementation** - Full Ring WebSocket protocol implementation (Socket, AsyncSocket, Listener)
5. **Native Integration** - C shim functions for WebSocket operations (send_text, send_binary, close)
6. **Unit Tests** - All unit tests pass (6 tests, 11 assertions, 0 failures)

### ❌ Critical Issue: Request Lifecycle Crash

**Problem**: After WebSocket upgrade completes, h2o attempts to access request structures that are no longer valid, causing a crash:

```
thread panic: member access within null pointer of type 'h2o_ostream_t'
Assertion failed: req->_generator != NULL
```

**Root Cause**: The WebSocket upgrade takes ownership of the connection, invalidating the HTTP request. However, the request handling pipeline continues to execute cleanup/finalization code that tries to access the now-invalid request structures.

**What Happens**:
1. ✅ Handler returns WebSocket response with `::ws/listener`
2. ✅ `handle-websocket-upgrade!` is called
3. ✅ Native `h2o_upgrade_to_websocket` succeeds
4. ✅ Client connects and `on-open` callback fires
5. ❌ Request cleanup code tries to access invalidated request → CRASH

**Required Fix**: The request handler needs to signal to h2o that the request has been taken over by WebSocket and should not be cleaned up normally. This requires:
- Preventing request finalization after WebSocket upgrade
- Ensuring no HTTP response is sent after upgrade
- Proper thread context for WebSocket upgrade (must happen on h2o event loop thread)

## Implementation Details

### Architecture

```
Ring Handler (Virtual Thread)
    ↓
Detects WebSocket Request
    ↓
Returns {:status 101 ::ws/listener {...}}
    ↓
handle-websocket-upgrade! called
    ↓
Native h2o_upgrade_to_websocket
    ↓
WebSocket Connection Established
    ↓
[CRASH: Request cleanup attempts to use invalidated request]
```

### Files Modified

**Core Implementation:**
- `src/main/clojure/ol/h2o/websocket.clj` - WebSocket protocol implementation
- `src/main/clojure/ol/h2o/request.clj` - Integrated WebSocket detection
- `shim/shim.c` - WebSocket C functions
- `shim/shim.h` - WebSocket function declarations
- `shim/build.zig` - Added wslay include paths

**Tests:**
- `src/test/clojure/ol/h2o/websocket_test.clj` - Unit tests (passing)
- `src/test/clojure/ol/h2o/websocket_integration_test.clj` - Integration tests (crash)

**Configuration:**
- `deps.edn` - Added shim resources to classpath
- `shim/build.zig` - Updated to use upstream h2o-zig with wslay

### Native Layer

The C shim correctly wraps h2o's WebSocket functions:

```c
// Check if request is WebSocket handshake
int clj_h2o_is_websocket_handshake(clj_req_ctx_t *ctx, const char **client_key_out);

// Upgrade HTTP connection to WebSocket
clj_ws_conn_t *clj_h2o_upgrade_to_websocket(clj_req_ctx_t *ctx, const char *client_key,
                                            void *user_data, clj_ws_msg_callback on_message);

// Send text/binary messages
int clj_h2o_websocket_send(clj_ws_conn_t *conn, uint8_t opcode, const uint8_t *data, size_t len);

// Close WebSocket connection
void clj_h2o_websocket_close(clj_ws_conn_t *conn, uint16_t code, const char *reason, size_t reason_len);
```

### Known Limitations

1. **Message Callbacks Not Implemented** - Currently passing NULL callback to avoid coffi serialization issues
   - `mem/serialize` hangs when called from virtual thread context
   - Need to implement callback mechanism that works with virtual threads
   
2. **No Message Handling** - Without callbacks, server cannot receive WebSocket messages
   - Clients can connect but server cannot process incoming messages
   - Need to solve callback serialization issue

3. **Request Lifecycle Issue** - The critical crash prevents any WebSocket functionality from working end-to-end

## Next Steps

### Immediate (Required for Functionality)

1. **Fix Request Lifecycle** - Prevent request cleanup after WebSocket upgrade
   - Option A: Mark request as "taken over" so h2o skips cleanup
   - Option B: Perform WebSocket upgrade on h2o event loop thread instead of virtual thread
   - Option C: Implement proper request ownership transfer

2. **Implement Message Callbacks** - Solve coffi serialization issue
   - Investigate why `mem/serialize` hangs in virtual thread context
   - Consider alternative callback mechanisms
   - May need to use global callback registry instead of per-connection closures

### Future Enhancements

3. **Ping/Pong Support** - Implement keep-alive mechanism
4. **Binary Message Support** - Full support for binary frames
5. **Subprotocol Negotiation** - Support WebSocket subprotocols
6. **Compression** - Per-message deflate extension
7. **Performance Testing** - Benchmark throughput and concurrent connections

## Testing

### Unit Tests: ✅ PASSING

```bash
bb test --focus ol.h2o.websocket-test
# 6 tests, 11 assertions, 0 failures
```

### Integration Tests: ❌ CRASH

```bash
bb test --focus ol.h2o.websocket-integration-test
# Crashes after WebSocket upgrade completes
```

## Usage Example (Once Fixed)

```clojure
(require '[ol.h2o.server :as server]
         '[ol.h2o.websocket :as ws])

(defn handler [request]
  (if (and (= :get (:request-method request))
           (get-in request [:headers "sec-websocket-key"]))
    (ws/websocket-response
     {:on-open (fn [socket]
                 (println "WebSocket opened"))
      :on-message (fn [socket message]
                    (println "Received:" message)
                    ;; Echo back
                    (ws/-send socket (str "Echo: " message)))
      :on-close (fn [socket code reason]
                  (println "WebSocket closed:" code reason))})
    {:status 200 :body "HTTP endpoint"}))

(def server (server/run-server handler {:listeners [{:port 8080}]}))
```

## Technical Notes

### Thread Context Issue

WebSocket upgrade must happen on the h2o event loop thread, but Ring handlers run in virtual threads. This mismatch causes issues:

1. Handler runs in virtual thread (via ExecutorService)
2. Handler returns WebSocket response
3. `handle-websocket-upgrade!` called from virtual thread
4. Native `h2o_upgrade_to_websocket` called from wrong thread context
5. Request cleanup happens on event loop thread → crash

**Solution**: Send message to event loop thread to perform upgrade, similar to how `start-response` works.

### Callback Serialization Issue

`mem/serialize` hangs when serializing closures from virtual thread context. This prevents implementing message callbacks. Possible solutions:

1. Use global callback registry with connection ID lookup
2. Serialize callbacks on event loop thread instead
3. Use different FFI mechanism for callbacks
4. Pre-serialize callbacks during server startup

### Memory Management

- WebSocket connections use `mem/shared-arena` to keep callbacks alive
- Arena must live for the lifetime of the connection
- Need proper cleanup when connection closes
