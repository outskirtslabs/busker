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
  CLJ_HANDLER_OVERLOADED = -2,   /* System overloaded; send 503 immediately */
  CLJ_HANDLER_SHUTTING_DOWN = -3 /* System is shutting down */
} clj_handler_status_t;

typedef struct {
  const char *name;
  size_t name_len;
  const char *value;
  size_t value_len;
} clj_header_t;

typedef struct {
  const uint8_t *authority;
  const uint8_t *method;
  const uint8_t *path;
  const uint8_t *remote_addr;
  const uint8_t *scheme;
  const clj_header_t *headers;

  size_t authority_len;
  size_t method_len;
  size_t path_len;
  size_t remote_addr_len;
  size_t scheme_len;
  size_t headers_len;

  int http_version;
  short int has_body;
} clj_req_meta_t;

typedef struct clj_req_ctx_t clj_req_ctx_t;

struct clj_req_ctx_t {
  h2o_req_t *req;
  clj_req_meta_t meta;
  void (*on_request_cleanup)(clj_req_ctx_t *);
  void (*on_request_body_chunk)(clj_req_ctx_t *ctx, char *chunk,
                                size_t chunk_len, int is_end_stream);
  h2o_generator_t generator;
  void (*on_response_generator_proceed)(clj_req_ctx_t *ctx);
  void (*on_response_generator_stop)(clj_req_ctx_t *ctx,
                                     clj_complete_reason_t reason);
  size_t preferred_chunk_size;
  char req_id[64];
  int cleanup;
  int closing;
  int response_started;
};

typedef struct {
  h2o_handler_t super; /* Must be first member for safe casting */
  int (*on_request)(clj_req_ctx_t *);
  void (*on_request_cleanup)(clj_req_ctx_t *);
  int shutting_down;
} clj_h2o_handler_t;

typedef struct clj_mt_receiver_t clj_mt_receiver_t;

void clj_h2o_handler_set_shutting_down(clj_h2o_handler_t *handler,
                                       int shutting_down);

/**
 * Flat representation of the subset of h2o_globalconf_t exposed via the shim.
 * Field names follow the <section>__<field> naming convention (e.g.
 * http1__req_timeout). Each value is guarded by a has_* flag so callers can opt
 * into overrides without mutating libh2o defaults unintentionally. String
 * values must be UTF-8 encoded and accompanied by their byte length.
 */
typedef struct {
  int has_server_name;
  const char *server_name;

  int has_proxy_status_identity;
  const char *proxy_status_identity;

  int has_max_request_entity_size;
  size_t max_request_entity_size;

  int has_max_delegations;
  unsigned max_delegations;

  int has_max_reprocesses;
  unsigned max_reprocesses;

  int has_handshake_timeout;
  uint64_t handshake_timeout;

  int has_max_spare_pipes;
  size_t max_spare_pipes;

  int has_http1__req_timeout;
  uint64_t http1__req_timeout;

  int has_http1__req_io_timeout;
  uint64_t http1__req_io_timeout;

  int has_http1__upgrade_to_http2;
  int http1__upgrade_to_http2;

  int has_http2__idle_timeout;
  uint64_t http2__idle_timeout;

  int has_http2__graceful_shutdown_timeout;
  uint64_t http2__graceful_shutdown_timeout;

  int has_http2__max_streams;
  uint32_t http2__max_streams;

  int has_http2__max_concurrent_requests_per_connection;
  size_t http2__max_concurrent_requests_per_connection;

  int has_http2__max_concurrent_streaming_requests_per_connection;
  size_t http2__max_concurrent_streaming_requests_per_connection;

  int has_http2__max_streams_for_priority;
  size_t http2__max_streams_for_priority;

  int has_http2__active_stream_window_size;
  uint32_t http2__active_stream_window_size;

  int has_http2__dos_delay;
  uint64_t http2__dos_delay;

  int has_http3__idle_timeout;
  uint64_t http3__idle_timeout;

  int has_http3__graceful_shutdown_timeout;
  uint64_t http3__graceful_shutdown_timeout;

  int has_http3__active_stream_window_size;
  uint32_t http3__active_stream_window_size;

  int has_http3__ack_frequency;
  uint16_t http3__ack_frequency;

  int has_compress_args;
  size_t compress_args_mine_size;
  int compress_args_gzip_quality;
  int compress_args_brotli_quality;
  int compress_args_zstd_quality;

} clj_h2o_flat_globalconf_t;

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

/* Return size of h2o_context_t for FFI allocation */
size_t clj_h2o_context_size(void);

/* Return size of h2o_globalconf_t for FFI allocation */
size_t clj_h2o_globalconf_size(void);

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

/* Returns the preferred_chunk_size by the ostream */
size_t clj_h2o_start_response(
    clj_req_ctx_t *ctx, int status, const clj_header_t *headers,
    size_t headers_len, size_t content_length, int compress_hint,
    void (*on_response_generator_proceed)(clj_req_ctx_t *ctx),
    void (*on_response_generator_stop)(clj_req_ctx_t *ctx,
                                       clj_complete_reason_t reason));

/* Create and configure h2o handler with optional callbacks.
 * All callback parameters can be NULL except on_req_callback.
 * supports_request_streaming: 1 to enable, 0 to disable
 * handles_expect: 1 to enable, 0 to disable
 */
clj_h2o_handler_t *
clj_h2o_create_handler(h2o_hostconf_t *hostconf,
                       int (*on_request)(clj_req_ctx_t *),
                       void (*on_request_cleanup)(clj_req_ctx_t *),
                       const clj_h2o_flat_globalconf_t *flat);

void clj_h2o_set_on_request_body_chunk(
    clj_req_ctx_t *ctx,
    void (*on_request_body_chunk)(clj_req_ctx_t *ctx, char *chunk,
                                  size_t chunk_len, int is_end_stream));

void clj_h2o_proceed_req(h2o_req_t *req);

int clj_h2o_cancel_request(clj_req_ctx_t *ctx);

/* Register a wakeup receiver on ctx->queue (one per h2o_context_t / worker) */
clj_mt_receiver_t *clj_h2o_mt_create_wakeup_receiver(h2o_context_t *ctx);

/* Unregister and free the wakeup receiver */
void clj_h2o_mt_destroy_wakeup_receiver(clj_mt_receiver_t *wr);

/* Send a wakeup message to the loop owning this receiver */
void clj_h2o_mt_wakeup(clj_mt_receiver_t *wr);

/* Create, initialize, and configure a new h2o_globalconf_t.
 * Allocates memory, calls h2o_config_init, and applies flat config if provided.
 * Returns NULL on allocation failure, otherwise returns configured globalconf
 * pointer.
 */
void clj_h2o_create_globalconf(h2o_globalconf_t *globalconf,
                               const clj_h2o_flat_globalconf_t *flat);

/* Create and configure SSL_CTX for TLS listener.
 * Parameters:
 *   cert_file: path to PEM certificate file
 *   key_file: path to PEM private key file
 *   enable_http2: 1 to register HTTP/2 ALPN protocols, 0 for HTTP/1.1 only
 * Returns: SSL_CTX pointer on success, NULL on error
 * Note: Caller must free with clj_h2o_free_ssl_ctx when done
 */
SSL_CTX *clj_h2o_create_ssl_ctx(const char *cert_file, const char *key_file,
                                int enable_http2);

/* Free SSL_CTX created by clj_h2o_create_ssl_ctx */
void clj_h2o_free_ssl_ctx(SSL_CTX *ssl_ctx);

/* WebSocket support */
typedef struct clj_ws_conn_t clj_ws_conn_t;

/* WebSocket message callback signature
 * opcode: WSLAY_TEXT_FRAME (0x1) or WSLAY_BINARY_FRAME (0x2)
 * msg: message data (may be NULL if connection closed)
 * msg_length: length of message data
 */
typedef void (*clj_ws_msg_callback)(clj_ws_conn_t *conn, uint8_t opcode,
                                    const uint8_t *msg, size_t msg_length);

struct clj_ws_conn_t {
  void *h2o_ws_conn; /* h2o_websocket_conn_t pointer */
  void *user_data;
  clj_ws_msg_callback on_message;
};

/* Check if request is a WebSocket handshake
 * Returns: 0 if valid handshake, -1 if invalid, 1 if not a websocket request
 * client_key_out: pointer to receive the client key (if valid handshake)
 */
int clj_h2o_is_websocket_handshake(h2o_req_t *req, const char **client_key_out);

/* Upgrade HTTP request to WebSocket
 * Returns: WebSocket connection handle, or NULL on error
 * user_data: arbitrary data to associate with connection
 * on_message: callback for received messages (NULL message indicates close)
 */
clj_ws_conn_t *clj_h2o_upgrade_to_websocket(h2o_req_t *req,
                                            const char *client_key,
                                            void *user_data,
                                            clj_ws_msg_callback on_message);

/* Send text or binary message via WebSocket
 * opcode: WSLAY_TEXT_FRAME (0x1) or WSLAY_BINARY_FRAME (0x2)
 * Returns: 0 on success, -1 on error
 */
int clj_h2o_websocket_send(clj_ws_conn_t *conn, uint8_t opcode,
                           const uint8_t *data, size_t length);

/* Send ping message via WebSocket
 * Returns: 0 on success, -1 on error
 */
int clj_h2o_websocket_ping(clj_ws_conn_t *conn, const uint8_t *data,
                           size_t length);

/* Send pong message via WebSocket
 * Returns: 0 on success, -1 on error
 */
int clj_h2o_websocket_pong(clj_ws_conn_t *conn, const uint8_t *data,
                           size_t length);

/* Close WebSocket connection
 * code: close status code (e.g., 1000 for normal closure)
 * reason: close reason string (may be NULL)
 * reason_length: length of reason string
 */
void clj_h2o_websocket_close(clj_ws_conn_t *conn, uint16_t code,
                             const char *reason, size_t reason_length);

#endif /* CLJ_H2O_SHIM_H */
