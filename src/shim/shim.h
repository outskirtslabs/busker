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
  short int is_early_data;  /* 1 if request arrived via 0-RTT */
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

void clj_h2o_send_informational(clj_req_ctx_t *ctx, int status,
                                const clj_header_t *headers,
                                size_t headers_len);

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

/* TLS certificate lookup callback used during handshake.
 * Return values:
 *   1  success (all out pointers and lengths must be set)
 *   0  certificate not found
 *  -1  internal error
 *
 * The server name can be omitted by clients. In that case:
 *   sni_hostname == NULL and sni_hostname_len == 0.
 *
 * Ownership:
 *   On success, cert_chain_pem_out and private_key_pem_out must point to heap
 *   memory allocated by clj_h2o_tls_memdup (or malloc-compatible allocator).
 *   Native side frees those pointers with free(3) after use.
 */
typedef int (*clj_tls_lookup_cb)(
    const uint8_t *sni_hostname, size_t sni_hostname_len,
    uint8_t **cert_chain_pem_out, size_t *cert_chain_pem_len_out,
    uint8_t **private_key_pem_out, size_t *private_key_pem_len_out,
    void *user_ctx);

/* Heap helpers for Clojure callback marshalling. */
void *clj_h2o_tls_memdup(const void *value, size_t value_len);

/* Create and configure SSL_CTX for TLS listener.
 * Parameters:
 *   cert_file: path to PEM certificate file (optional, must pair with key_file)
 *   key_file: path to PEM private key file (optional, must pair with cert_file)
 *   enable_http2: 1 to register HTTP/2 ALPN protocols, 0 for HTTP/1.1 only
 *   lookup_cb: TLS lookup callback for SNI handshakes, or NULL to disable
 *   lookup_user_ctx: opaque user context passed to lookup_cb
 * Returns: SSL_CTX pointer on success, NULL on error
 * Note: Caller must free with clj_h2o_free_ssl_ctx when done
 */
SSL_CTX *clj_h2o_create_ssl_ctx(const char *cert_file, const char *key_file,
                                int enable_http2, clj_tls_lookup_cb lookup_cb,
                                void *lookup_user_ctx);

/* Free SSL_CTX created by clj_h2o_create_ssl_ctx */
void clj_h2o_free_ssl_ctx(SSL_CTX *ssl_ctx);

/* HTTP/3 support in libh2o uses picotls for TLS 1.3 (separate from OpenSSL
 * SSL_CTX) and quicly for the QUIC transport layer. These functions manage the
 * context lifecycle for QUIC listeners.
 */

/* Forward declarations for QUIC/HTTP3 types */
typedef struct st_ptls_context_t ptls_context_t;
typedef struct st_quicly_context_t quicly_context_t;
typedef struct clj_http3_ctx_t clj_http3_ctx_t;
typedef struct clj_http3_udp_transport_t clj_http3_udp_transport_t;

/* Session ticket key constants */
#define CLJ_TICKET_KEY_NAME_LEN 16   /* bytes */
#define CLJ_TICKET_AES_KEY_LEN  32   /* bytes (AES-256) */
#define CLJ_TICKET_HMAC_KEY_LEN 64   /* bytes (SHA-256 block size) */
#define CLJ_MAX_EARLY_DATA_SIZE 8192 /* bytes, matches h2o */

/* Session ticket key - matches Clojure key map */
typedef struct clj_session_ticket {
    uint8_t name[CLJ_TICKET_KEY_NAME_LEN];   /* Key identifier (in ticket header) */
    uint8_t aes_key[CLJ_TICKET_AES_KEY_LEN]; /* AES-256 encryption key */
    uint8_t hmac_key[CLJ_TICKET_HMAC_KEY_LEN]; /* HMAC-SHA256 key */
    uint64_t not_before;                     /* Activation time (ms since epoch) */
    uint64_t not_after;                      /* Expiration time (ms since epoch) */
} clj_session_ticket_t;

/* Forward declarations for ticket manager types */
typedef struct clj_ticket_manager clj_ticket_manager_t;
typedef struct clj_encrypt_ticket clj_encrypt_ticket_t;

/* Create picotls context for QUIC TLS 1.3.
 * Uses OpenSSL-backed primitives for cryptographic operations.
 * Configures ALPN callback for HTTP/3 protocol negotiation.
 * Parameters:
 *   cert_file: path to PEM certificate file (optional, must pair with key_file)
 *   key_file: path to PEM private key file (optional, must pair with cert_file)
 *   lookup_cb: TLS lookup callback for SNI handshakes, or NULL to disable
 *   lookup_user_ctx: opaque user context passed to lookup_cb
 * Returns: ptls_context_t pointer on success, NULL on error
 * Note: Caller must free with clj_h2o_free_ptls_ctx when done
 */
ptls_context_t *clj_h2o_create_ptls_ctx(const char *cert_file,
                                        const char *key_file,
                                        clj_tls_lookup_cb lookup_cb,
                                        void *lookup_user_ctx);

/* Free picotls context and associated resources */
void clj_h2o_free_ptls_ctx(ptls_context_t *ctx);

/* Create quicly context configured for HTTP/3.
 * Starts from quicly_spec_context and configures:
 *   - TLS context from ptls_ctx
 *   - CID encryptor with random key
 *   - HTTP/3 transport parameters via h2o_http3_server_amend_quicly_context
 * Parameters:
 *   ptls_ctx: picotls context from clj_h2o_create_ptls_ctx
 *   globalconf: h2o global configuration for HTTP/3 settings
 * Returns: quicly_context_t pointer on success, NULL on error
 * Note: Caller must free with clj_h2o_free_quicly_ctx when done
 */
quicly_context_t *clj_h2o_create_quicly_ctx(ptls_context_t *ptls_ctx,
                                            h2o_globalconf_t *globalconf);

/* Free quicly context and CID encryptor */
void clj_h2o_free_quicly_ctx(quicly_context_t *ctx);

/* Open a pooled UDP transport reservation for HTTP/3.
 * The returned handle owns the pooled transport metadata and reserves the port
 * until worker attachments take over.
 */
clj_http3_udp_transport_t *clj_h2o_http3_open_udp_transport(const char *host,
                                                            uint16_t port);

/* Attach an HTTP/3 worker context to a pooled UDP transport.
 * The worker attachment joins the transport under (node_id, thread_id) but
 * starts non-accepting until the transport activates that generation.
 */
clj_http3_ctx_t *
clj_h2o_http3_attach_udp_transport(h2o_context_t *h2o_ctx, h2o_evloop_t *loop,
                                   quicly_context_t *quic_ctx,
                                   h2o_hostconf_t **hosts,
                                   clj_http3_udp_transport_t *transport,
                                   uint64_t node_id, uint32_t thread_id);

/* Mark one generation as the active acceptor for a pooled UDP transport.
 * Existing connections for older generations continue to route by CID, while
 * new Initial packets are forwarded to the active generation.
 */
void clj_h2o_http3_activate_udp_transport_generation(
    clj_http3_udp_transport_t *transport,
    uint64_t node_id);

/* Release a worker context that was attached to a pooled UDP transport.
 * This closes only the worker-owned dup'd fd.
 */
void clj_h2o_http3_detach_udp_transport(clj_http3_ctx_t *ctx);

/* Release the pooled UDP transport and any reservation socket it still owns. */
void clj_h2o_http3_release_udp_transport(clj_http3_udp_transport_t *transport);

/* Stop accepting new HTTP/3 connections by setting acceptor to NULL.
   Existing connections (including those in handshake) continue to completion.
   Call this before requesting shutdown to prevent new connection attempts. */
void clj_h2o_http3_stop_accepting(clj_http3_ctx_t *ctx);

/* Get the number of active HTTP/3 connections on this context */
size_t clj_h2o_http3_num_connections(clj_http3_ctx_t *ctx);

/* Dispose HTTP/3 worker context and close UDP socket.
   Note: all connections must be closed first (num_connections == 0) */
void clj_h2o_http3_dispose_worker_ctx(clj_http3_ctx_t *ctx);

/* Global connection limit API.
   Process-global counter shared across all workers and listeners in the JVM.
   Used for consistent enforcement of max-connections across HTTP/1.1, HTTP/2, and HTTP/3. */

/* Set the maximum allowed connections. Zero means unlimited. */
void clj_h2o_conn_limit_set_max(uint32_t max);

/* Get current connection count. */
uint32_t clj_h2o_conn_limit_current(void);

/* Atomically try to acquire a connection slot.
   Returns 1 if acquired (count was < max), 0 if at limit. */
int clj_h2o_conn_limit_try_acquire(void);

/* Release a connection slot (decrement counter). */
void clj_h2o_conn_limit_release(void);

/* Session ticket manager lifecycle */

/* Create a new ticket manager. Returns NULL on allocation failure. */
clj_ticket_manager_t *clj_ticket_manager_create(uint32_t ticket_lifetime_seconds);

/* Destroy ticket manager and securely erase all keys. */
void clj_ticket_manager_destroy(clj_ticket_manager_t *mgr);

/* Replace all keys atomically. Thread-safe via copy-on-write.
   Keys are copied, caller retains ownership of input array. */
int clj_ticket_manager_set_keys(
    clj_ticket_manager_t *mgr,
    const clj_session_ticket_t *keys,
    size_t num_keys);

/* Get current key count (for monitoring). */
size_t clj_ticket_manager_key_count(clj_ticket_manager_t *mgr);

/* Set the QUIC transport params hash. Called after quicly_context creation. */
void clj_ticket_manager_set_quic_tag(
    clj_ticket_manager_t *mgr,
    const quicly_context_t *quic_ctx);

/* Wire ticket manager into ptls context for session tickets.
   Creates encrypt_ticket callback structure and configures ticket lifetime. */
clj_encrypt_ticket_t *clj_ticket_manager_create_encrypt_ticket(
    clj_ticket_manager_t *mgr,
    int is_quic);

/* Configure ptls context for session tickets.
   Sets encrypt_ticket callback, ticket lifetime, and max early data size. */
void clj_ptls_ctx_set_tickets(
    ptls_context_t *ctx,
    clj_encrypt_ticket_t *encrypt_ticket,
    uint32_t ticket_lifetime,
    uint32_t max_early_data_size);

/* Configure SSL_CTX for TCP TLS session tickets and 0-RTT.
   Wires the ticket manager into the OpenSSL ticket key callback. */
void clj_ssl_ctx_set_tickets(
    SSL_CTX *ctx,
    clj_ticket_manager_t *mgr,
    uint32_t max_early_data_size);

#endif /* CLJ_H2O_SHIM_H */
