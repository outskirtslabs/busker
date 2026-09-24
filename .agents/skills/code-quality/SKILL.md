---
name: code-quality
description: Code quality guidance for comments and native-boundary code. Use when writing or reviewing C shim code, FFM integration, concurrency, lifecycle, backpressure, error handling, memory ownership, or TODO comments.
---

# Code quality

## Commenting guidelines

Comments are a tool of last resort.
Code should explain how; comments should explain why.
Favor precise, minimal notes that capture invariants, edge cases, and intent that is not obvious from the code.
Avoid status updates, history, or restating the code.

### Core rules

- Prefer comments that record invariants, contracts, and "why" such as subtle behavior, cross-module assumptions, and race-condition avoidance
- Prefer block-level comments to line-by-line clutter
- Prefer citing external constraints when a workaround references a kernel quirk, library bug, or RFC rule with a version or ID
- Prefer no comment when names and structure already make intent clear
- Avoid narrating the obvious, such as "close fd" or "increment i"
- Avoid including project management or history, such as "Phase 1" or "before refactor X"
- Avoid explaining the language or standard API calls
- Avoid leaving TODOs without an owner and condition

### Examples

These 15 cases come from the shim's threads, sockets, FFM, backpressure, and request lifetimes.
Each case contrasts an unhelpful comment with code that records useful intent, or with clearer code that needs no comment.

#### 1. Obvious cleanup

Bad

```c
/* Close dup'd FDs */
for (uint32_t i = 0; i < listener->fds.size; ++i) {
    if (listener->fds.entries[i] >= 0) {
        close(listener->fds.entries[i]);
    }
}
/* Close master FD */
if (listener->master_fd >= 0) {
    close(listener->master_fd);
}
```

Good, with no comment

```c
for (uint32_t i = 0; i < listener->fds.size; ++i) {
    if (listener->fds.entries[i] >= 0) close(listener->fds.entries[i]);
}
if (listener->master_fd >= 0) close(listener->master_fd);
```

#### 2. Intent over narration

Bad

```c
/* Reserve space for per-thread fds */
h2o_vector_reserve(NULL, &listener_state->fds, s->num_threads);
listener_state->fds.size = s->num_threads; // set size
```

Good

```c
/* Each worker owns its own fd slot; pre-size vector for exact count */
h2o_vector_reserve(NULL, &listener_state->fds, s->num_threads);
listener_state->fds.size = s->num_threads;
```

#### 3. Do not restate code; document the invariant

Bad

```c
/* increment connection count */
__sync_add_and_fetch(&server->active_connections, 1);
```

Good

```c
/* Global connection cap uses this counter; must be atomic across workers */
__sync_add_and_fetch(&server->active_connections, 1);
```

#### 4. Error handling: capture the errno reason

Bad

```c
if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
    return CLJ_ERR_SOCKET_BIND; // bind failed
}
```

Good

```c
if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
    server_set_last_errno(s, errno, "bind", "tcp4", host, port);
    return CLJ_ERR_SOCKET_BIND; /* Surface cause via clj_server_get_last_error_* */
}
```

#### 5. Concurrency: explain why the barrier exists

Bad

```c
/* stop reading */
h2o_socket_read_stop(sock);
```

Good

```c
/* Pause accepts when at/over soft limit; avoids queue growth on all workers */
h2o_socket_read_stop(sock);
```

#### 6. Lifecycle: encode the contract, not the steps

Bad

```c
/* start server */
rc = clj_server_start(s);
/* wait for threads */
for (i = 0; i < s->num_threads; i++) pthread_join(th[i], NULL);
```

Good

```c
/* Start workers before exposing listeners; join ensures all per-loop shutdown phases complete */
rc = clj_server_start(s);
for (i = 0; i < s->num_threads; i++) pthread_join(th[i], NULL);
```

#### 7. Delete history and phase comments

Bad

```c
/* Phase 1: old approach with shared fd; Phase 2: SO_REUSEPORT */
int fd = socket(AF_INET, SOCK_STREAM, 0);
```

Good, with no comment

```c
int fd = socket(AF_INET, SOCK_STREAM, 0);
```

#### 8. FFM downcall: document the pinning assumption

Bad

```c
// call into native
rc = shim->send_vecs(...);
```

Good

```c
/* Downcall is enqueue-only; must not block to keep vthreads unpinned in Model A */
rc = shim->send_vecs(...);
```

#### 9. Backpressure: explain the retry gate

Bad

```c
if (rc == CLJ_AGAIN) { /* try later */ return; }
```

Good

```c
/* Native buffers/full flow window: wait for on_proceed before retrying to avoid busy loop */
if (rc == CLJ_AGAIN) return;
```

#### 10. Memory lifetime: explain pool ownership

Bad

```c
/* allocate ctx */
ctx = h2o_mem_alloc_shared(&req->pool, sizeof(*ctx), NULL);
```

Good

```c
/* Request-scoped: freed with req->pool; no manual free on normal or error paths */
ctx = h2o_mem_alloc_shared(&req->pool, sizeof(*ctx), NULL);
```

#### 11. Zero-copy send: justify the releaser

Bad

```c
vec.releaser = j_releaser; // call later
vec.jvm_handle = handle;
```

Good

```c
/* Borrowed off-heap buffer: releaser fires exactly once after transport consumes it */
vec.releaser = j_releaser;
vec.jvm_handle = handle;
```

#### 12. Do not comment what the name already says

Bad

```c
/* convert milliseconds to nanoseconds */
uint64_t ns = (uint64_t)ms * 1000000ULL;
```

Good, with no comment

```c
uint64_t ns = (uint64_t)ms * 1000000ULL;
```

#### 13. External constraint: cite the source

Bad

```c
/* we need SO_REUSEPORT here */
setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
```

Good

```c
/* One FD per worker; kernel load-balances accepts (Linux SO_REUSEPORT).
   See man 7 socket, and h2o main.c listener model. */
setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
```

#### 14. Upcall minimalism: explain why work is bounced

Bad

```c
/* queue work on JVM */
on_request(app_ctx, req_id, &meta);
```

Good

```c
/* Upcalls must enqueue and return immediately to keep the native loop responsive */
on_request(app_ctx, req_id, &meta);
```

#### 15. TODOs must be actionable or deleted

Bad

```c
// TODO: improve shutdown
```

Good

```c
/* TODO: add shutdown_timeout; force close after T if drains stall. */
```

### Language-specific style

- C shim and libh2o integration
  - Prefer `/* ... */` for block rationale over trailing `//` noise
  - Put "why" comments above the block they describe
  - Name invariants such as "single-writer event loop", "request-scoped pool", "on_proceed gate", and "no cross-loop access"
- Java FFM bindings
  - Use Javadoc for public ABI surfaces to capture contracts such as pinning and ownership
  - Use inline comments only for subtle FFM layout, padding, or lifetime details
- Clojure
  - Use docstrings on public functions
  - Use brief inline `;;` comments only when a concurrency or backpressure invariant is not obvious from the code
