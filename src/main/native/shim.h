#ifndef CLJ_H2O_SHIM_H
#define CLJ_H2O_SHIM_H

#ifndef CLJ_DEBUG
#define CLJ_DEBUG 0
#endif

#if CLJ_DEBUG
#define DEBUG_LOG(fmt, ...)                                                    \
  fprintf(stderr, "[SHIM DEBUG] " fmt "\n", ##__VA_ARGS__)
#else
#define DEBUG_LOG(fmt, ...)                                                    \
  do {                                                                         \
  } while (0)
#endif

#ifdef CLJ_DEBUG
#undef _FORTIFY_SOURCE
#define _FORTIFY_SOURCE 0
#endif

#include <h2o.h>
#include <h2o/socket/evloop.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
  CLJ_COMPLETE_OK = 0,
  CLJ_COMPLETE_RESET = 1,
  CLJ_COMPLETE_TIMEOUT = 2,
  CLJ_COMPLETE_ERROR = 3
} clj_complete_reason_t;

typedef enum {
  CLJ_HANDLER_OK = 0,        /* Handler accepted and will process the request */
  CLJ_HANDLER_DECLINED = -1, /* Handler declined; h2o should try next handler */
  CLJ_HANDLER_OVERLOADED = -2 /* System overloaded; send 503 immediately */
} clj_handler_status_t;

/* Per-request generator upcalls for streaming HTTP responses */
typedef struct {
  void (*on_proceed)(void *jvm_handle);
  void (*on_stop)(void *jvm_handle, clj_complete_reason_t reason);
} clj_generator_callbacks_t;

typedef struct clj_stream_ctx {
  uint16_t generation; /* Incremented each time this slot is reused */
  uint8_t in_use;      /* Whether this slot is currently allocated */
  uint8_t generator_active;
  uint8_t response_started;
  uint8_t closing;     /* Set when completion cleanup wants to deallocate but
                          generator is still active */
  uint8_t _padding[2]; /* Explicit padding for alignment */
  h2o_req_t *h2o_req;
  h2o_generator_t generator;
  clj_generator_callbacks_t callbacks;
  void *jvm_handle;
  atomic_uint_fast8_t send_inflight;
} clj_stream_ctx_t;

typedef struct {
  const uint8_t *name;
  size_t name_len;
  const uint8_t *value;
  size_t value_len;
} clj_header_t;

typedef struct {
  const uint8_t *authority;
  const uint8_t *charset;
  const uint8_t *method;
  const uint8_t *path;
  const uint8_t *remote_addr;
  const uint8_t *scheme;
  const clj_header_t *headers;

  size_t authority_len;
  size_t charset_len;
  size_t method_len;
  size_t path_len;
  size_t remote_addr_len;
  size_t scheme_len;
  size_t headers_len;

  int http_version;
  int has_body;
} clj_req_meta_t;

typedef struct clj_req_ctx_t clj_req_ctx_t;
struct clj_req_ctx_t {
  h2o_req_t *req;
  clj_req_meta_t meta;
  void (*on_request_cleanup)(clj_req_ctx_t *);
  void (*on_request_body_chunk)(clj_req_ctx_t *ctx, char *chunk,
                                size_t chunk_len, int is_end_stream);
  h2o_generator_t generator;
  int cleanup;
};

typedef struct {
  h2o_handler_t super; /* Must be first member for safe casting */
  int (*on_request)(clj_req_ctx_t *);
  void (*on_request_cleanup)(clj_req_ctx_t *);
  int shutting_down;
} clj_h2o_handler_t;

/* 1 if socket has a read callback (i.e. currently reading), else 0 */
int clj_h2o_socket_is_reading(h2o_socket_t *sock);

/* 1 if socket has a write callback (i.e. currently writing), else 0 */
int clj_h2o_socket_is_writing(h2o_socket_t *sock);

/* Optional debug helpers: raw callback pointers (NULL if not set) */
void *clj_h2o_socket_get_read_cb(h2o_socket_t *sock);
void *clj_h2o_socket_get_write_cb(h2o_socket_t *sock);

/* Socket on_close callback support for connection tracking */
void clj_h2o_socket_set_on_close(h2o_socket_t *sock, void *callback,
                                 void *data);

/* Return size of h2o_globalconf_t for FFI allocation */
size_t clj_h2o_globalconf_size(void);

/* Return size of h2o_context_t for FFI allocation */
size_t clj_h2o_context_size(void);

/* Return size of h2o_accept_ctx_t for FFI allocation */
size_t clj_h2o_accept_ctx_size(void);

/* Get hosts pointer from h2o_globalconf_t */
h2o_hostconf_t **clj_h2o_globalconf_get_hosts(h2o_globalconf_t *globalconf);

/* Set on_req callback for handler */
void clj_handler_set_on_req(h2o_handler_t *handler,
                            int (*callback)(h2o_handler_t *, h2o_req_t *));

/* Struct size helpers for FFI layout */
size_t clj_h2o_handler_size(void);

h2o_headers_t *clj_h2o_req_get_res_headers(h2o_req_t *req);

/* Standard tokens and generator for responses */
h2o_token_t *clj_h2o_get_content_type_token(void);
h2o_generator_t *clj_h2o_get_static_generator(void);

/* Event loop and context state access */
uint64_t clj_h2o_evloop_now(h2o_evloop_t *loop);
size_t clj_h2o_context_get_active_conns(h2o_context_t *ctx);
size_t clj_h2o_context_get_idle_conns(h2o_context_t *ctx);
size_t clj_h2o_context_get_shutdown_conns(h2o_context_t *ctx);

void clj_stream_start_response(h2o_req_t *req, int status,
                               const clj_header_t *headers,
                               uint32_t headers_len, size_t content_length,
                               const clj_generator_callbacks_t *generator_cb);

/* Create and configure h2o handler with optional callbacks.
 * All callback parameters can be NULL except on_req_callback.
 * supports_request_streaming: 1 to enable, 0 to disable
 * handles_expect: 1 to enable, 0 to disable
 */
clj_h2o_handler_t *
clj_h2o_create_handler(h2o_hostconf_t *hostconf,
                       int (*on_req_callback)(clj_req_ctx_t *),
                       void (*on_cleanup_callback)(clj_req_ctx_t *),
                       int supports_request_streaming, int handles_expect);

void clj_h2o_set_on_request_body_chunk(
    clj_req_ctx_t *ctx,
    void (*on_request_body_chunk)(clj_req_ctx_t *ctx, char *chunk,
                                  size_t chunk_len, int is_end_stream));
void clj_h2o_proceed_req(h2o_req_t *req);

#endif /* CLJ_H2O_SHIM_H */
