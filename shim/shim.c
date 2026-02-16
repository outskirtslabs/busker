// Minimal exported helpers for libh2o interop
// Intended for FFI use from Clojure (coffi/FFM).
#include "shim.h"
#include "h2o/multithread.h"
#include "h2o/http3_server.h"
#include "picotls.h"
#include "picotls/openssl.h"
#include "quicly.h"
#include "quicly/defaults.h"
#include <inttypes.h>
#include <limits.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* Fallback for platforms without explicit_bzero */
#if !defined(__GLIBC__) && !defined(__FreeBSD__) && !defined(__OpenBSD__)
#include <openssl/crypto.h>
#define explicit_bzero(s, n) OPENSSL_cleanse(s, n)
#endif

#define REQ_ERROR "request error\n"

/* Global connection limit counter and maximum.
   Process-global, shared across all workers and listeners in the same JVM.
   TODO: refactor connection limit accounting to be per-server rather than
   process-global so multiple embedded servers can coexist safely. */
static _Atomic uint32_t clj_conn_count = 0;
static _Atomic uint32_t clj_conn_max = 0;

typedef struct {
  h2o_multithread_message_t super; /* must be first */
} clj_mt_msg_t;

struct clj_mt_receiver_t {
  h2o_multithread_receiver_t receiver;
  h2o_multithread_queue_t *queue; /* store queue for unregister */
};

typedef enum {
  OK = 200,
  INTERNAL_SERVER_ERROR = 500,
  BAD_GATEWAY = 502,
  SERVICE_UNAVAILABLE = 503,
  GATEWAY_TIMEOUT = 504
} http_status_code_t;

typedef struct {
  clj_tls_lookup_cb cb;
  void *user_ctx;
} clj_tls_lookup_binding_t;

static const char *status_code_to_string(http_status_code_t status_code) {
  const char *ret;

  switch (status_code) {
  case BAD_GATEWAY:
    ret = "Bad Gateway";
    break;
  case GATEWAY_TIMEOUT:
    ret = "Gateway Timeout";
    break;
  case INTERNAL_SERVER_ERROR:
    ret = "Internal Server Error";
    break;
  case OK:
    ret = "OK";
    break;
  case SERVICE_UNAVAILABLE:
    ret = "Service Unavailable";
    break;
  default:
    ret = "";
  }

  return ret;
}

void *clj_h2o_tls_memdup(const void *value, size_t value_len) {
  if (value == NULL || value_len == 0)
    return NULL;

  uint8_t *dup = malloc(value_len);
  if (dup == NULL)
    return NULL;

  memcpy(dup, value, value_len);
  return dup;
}
void send_error(http_status_code_t status_code, const char *body,
                h2o_req_t *req) {
  h2o_send_error_generic(req, status_code, status_code_to_string(status_code),
                         body, 0);
}

int clj_h2o_socket_is_reading(h2o_socket_t *sock) {
  return (sock != NULL && sock->_cb.read != NULL) ? 1 : 0;
}

int clj_h2o_socket_is_writing(h2o_socket_t *sock) {
  return (sock != NULL && sock->_cb.write != NULL) ? 1 : 0;
}

void *clj_h2o_socket_get_read_cb(h2o_socket_t *sock) {
  return sock ? (void *)sock->_cb.read : NULL;
}

void *clj_h2o_socket_get_write_cb(h2o_socket_t *sock) {
  return sock ? (void *)sock->_cb.write : NULL;
}

void clj_h2o_socket_set_on_close(h2o_socket_t *sock, void *callback,
                                 void *data) {
  if (sock) {
    sock->on_close.cb = (void (*)(void *))callback;
    sock->on_close.data = data;
  }
}

size_t clj_h2o_context_size(void) { return sizeof(h2o_context_t); }

size_t clj_h2o_globalconf_size(void) { return sizeof(h2o_globalconf_t); }

size_t clj_h2o_accept_ctx_size(void) { return sizeof(h2o_accept_ctx_t); }

h2o_hostconf_t **clj_h2o_globalconf_get_hosts(h2o_globalconf_t *globalconf) {
  return globalconf->hosts;
}

void clj_handler_set_on_req(h2o_handler_t *handler,
                            int (*callback)(h2o_handler_t *, h2o_req_t *)) {
  handler->on_req = callback;
}

size_t clj_h2o_handler_size(void) { return sizeof(h2o_handler_t); }

h2o_headers_t *clj_h2o_req_get_res_headers(h2o_req_t *req) {
  return &req->res.headers;
}

h2o_token_t *clj_h2o_get_content_type_token(void) {
  return H2O_TOKEN_CONTENT_TYPE;
}

uint64_t clj_h2o_evloop_now(h2o_evloop_t *loop) { return loop->_now_millisec; }

size_t clj_h2o_context_get_active_conns(h2o_context_t *ctx) {
  return ctx->_conns.num_conns.active;
}

size_t clj_h2o_context_get_idle_conns(h2o_context_t *ctx) {
  return ctx->_conns.num_conns.idle;
}

size_t clj_h2o_context_get_shutdown_conns(h2o_context_t *ctx) {
  return ctx->_conns.num_conns.shutdown;
}

static void clj_generator_proceed(h2o_generator_t *gen, h2o_req_t *req) {
  (void)req; /* unused parameter */

  if (!gen) {
    return;
  }

  /* gen points to the generator member inside clj_stream_ctx_t, not the start
   * of the struct. */
  clj_req_ctx_t *ctx = H2O_STRUCT_FROM_MEMBER(clj_req_ctx_t, generator, gen);

  if (!ctx) {
    return;
  }

  if (ctx->on_response_generator_proceed) {
    ctx->on_response_generator_proceed(ctx);
  }
}

static void clj_generator_stop(h2o_generator_t *gen, h2o_req_t *req) {
  (void)req; /* unused parameter */

  if (!gen) {
    return;
  }

  clj_req_ctx_t *ctx = H2O_STRUCT_FROM_MEMBER(clj_req_ctx_t, generator, gen);

  if (!ctx) {
    return;
  }

  if (ctx->on_response_generator_stop) {
    ctx->on_response_generator_stop(ctx, CLJ_COMPLETE_RESET);
  }
}

/* Copy headers from clj_header_t array to h2o_req_t response headers using pool
 * allocation */
static void copy_headers_to_response(h2o_req_t *req,
                                     const clj_header_t *headers,
                                     size_t headers_len) {
  for (uint32_t i = 0; i < headers_len; i++) {
    const char *name_data = headers[i].name;
    const char *value_data = headers[i].value;
    size_t name_len = headers[i].name_len;
    size_t value_len = headers[i].value_len;

    char *pool_name = h2o_mem_alloc_pool(&req->pool, char, name_len + 1);
    char *pool_value = h2o_mem_alloc_pool(&req->pool, char, value_len + 1);

    memcpy(pool_name, name_data, name_len);
    pool_name[name_len] = '\0';

    memcpy(pool_value, value_data, value_len);
    pool_value[value_len] = '\0';

    h2o_add_header_by_str(&req->pool, &req->res.headers, pool_name, name_len, 0,
                          pool_name, pool_value, value_len);
  }
}

void clj_h2o_send_informational(clj_req_ctx_t *ctx, int status,
                                const clj_header_t *headers,
                                size_t headers_len) {

  if (!ctx || !ctx->req)
    return;

  h2o_req_t *req = ctx->req;

  req->res.status = status;

  copy_headers_to_response(req, headers, headers_len);

  h2o_send_informational(req);
}

size_t clj_h2o_start_response(
    clj_req_ctx_t *ctx, int status, const clj_header_t *headers,
    size_t headers_len, size_t content_length, int compress_hint,
    void (*on_response_generator_proceed)(clj_req_ctx_t *ctx),
    void (*on_response_generator_stop)(clj_req_ctx_t *ctx,
                                       clj_complete_reason_t reason)) {
  (void)compress_hint; /* TODO: use for per-request compression control */

  if (!ctx || !ctx->req)
    return 0;

  h2o_req_t *req = ctx->req;

  req->res.status = status;
  req->res.reason = "OK";

  req->res.content_length = SIZE_MAX;
  if (content_length != SIZE_MAX) {
    req->res.content_length = content_length;
  }

  copy_headers_to_response(req, headers, headers_len);

  req->compress_hint = H2O_COMPRESS_HINT_ENABLE;
  ctx->generator.proceed = clj_generator_proceed;
  ctx->generator.stop = clj_generator_stop;
  ctx->on_response_generator_proceed = on_response_generator_proceed;
  ctx->on_response_generator_stop = on_response_generator_stop;
  ctx->response_started = 1;
  h2o_start_response(req, &ctx->generator);
  return req->preferred_chunk_size;
}

static void clj_h2o_extract_req_meta(h2o_req_t *req, clj_req_meta_t *meta) {
  meta->method = (const uint8_t *)req->method.base;
  meta->method_len = req->method.len;
  meta->path = (const uint8_t *)req->path.base;
  meta->path_len = req->path.len;
  meta->authority = (const uint8_t *)req->authority.base;
  meta->authority_len = req->authority.len;
  meta->http_version = req->version;
  if (req->headers.size > 0) {
    clj_header_t *headers =
        h2o_mem_alloc_pool(&req->pool, clj_header_t, req->headers.size);
    for (size_t i = 0; i < req->headers.size; i++) {
      h2o_header_t *h = &req->headers.entries[i];
      headers[i].name = h->name->base;
      headers[i].name_len = h->name->len;
      headers[i].value = h->value.base;
      headers[i].value_len = h->value.len;
    }
    meta->headers = headers;
    meta->headers_len = req->headers.size;
  } else {
    meta->headers = NULL;
    meta->headers_len = 0;
  }

  meta->has_body = (req->entity.base != NULL && req->entity.len > 0) ? 1 : 0;
  meta->is_early_data = h2o_conn_is_early_data(req->conn) ? 1 : 0;

  if (req->scheme) {
    meta->scheme = (const uint8_t *)req->scheme->name.base;
    meta->scheme_len = req->scheme->name.len;
  } else {
    meta->scheme = NULL;
    meta->scheme_len = 0;
  }

  /* Extract remote address from connection socket peername */
  if (req->conn && req->conn->callbacks && req->conn->callbacks->get_peername) {
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);
    if (req->conn->callbacks->get_peername(req->conn, (struct sockaddr *)&sa) ==
        0) {
      char addrbuf[NI_MAXHOST];
      if (getnameinfo((struct sockaddr *)&sa, salen, addrbuf, sizeof(addrbuf),
                      NULL, 0, NI_NUMERICHOST) == 0) {
        size_t addr_len = strlen(addrbuf);
        char *remote = h2o_mem_alloc_pool(&req->pool, char, addr_len + 1);
        memcpy(remote, addrbuf, addr_len + 1);
        meta->remote_addr = (const uint8_t *)remote;
        meta->remote_addr_len = (int32_t)addr_len;
      } else {
        meta->remote_addr = NULL;
        meta->remote_addr_len = 0;
      }
    } else {
      meta->remote_addr = NULL;
      meta->remote_addr_len = 0;
    }
  } else {
    meta->remote_addr = NULL;
    meta->remote_addr_len = 0;
  }
}

/* Completion cleanup callback - this is called by h2o when our request dies
   such as when the client disconnects abruptly
   We use this to signal to java that this req is no longer alive
   ref: https://github.com/h2o/h2o/issues/1894#issuecomment-437231273
   */
static void cleanup_request(void *ptr) {
  if (!ptr)
    return;
  clj_req_ctx_t *const ctx = (clj_req_ctx_t *)ptr;
  ctx->cleanup = 1;
  if (ctx->on_request_cleanup) {
    ctx->on_request_cleanup(ctx);
  }
}

/**
 * from h2o.h:
 * Called be the protocol handler to submit chunk of request body to the
 * generator. The callback returns 0 if successful, otherwise a non-zero value.
 * Once `write_req.cb` is called, subsequent invocations MUST be postponed until
 * the `proceed_req` is called. At the moment, `write_req_cb` is required to
 * create a copy of data being provided before returning. To avoid copying, we
 * should consider delegating the responsibility of retaining the buffer to the
 * caller.
 */
static int clj_body_write_callback(void *self, int is_end_stream) {
  clj_req_ctx_t *ctx = (clj_req_ctx_t *)self;
  if (!ctx || !ctx->req) {
    return 1;
  }

  if (ctx->on_request_body_chunk == 0) {
    return 1;
  }

  h2o_req_t *req = ctx->req;

  if (req->entity.base && req->entity.len > 0) {
    ctx->on_request_body_chunk(ctx, req->entity.base, req->entity.len,
                               is_end_stream ? 1 : 0);
  } else if (is_end_stream) {
    /* End of stream marker with no data */
    ctx->on_request_body_chunk(ctx, NULL, 0, 1);
  }

  return 0;
}

static int request_handler(h2o_handler_t *self, h2o_req_t *req) {
  // Cast self back to our custom handler type to access the server pointer
  clj_h2o_handler_t *handler = (clj_h2o_handler_t *)self;

  if (handler->shutting_down) {
    h2o_send_error_503(req, "Service Unavailable", "Server is shutting down",
                       H2O_SEND_ERROR_HTTP1_CLOSE_CONNECTION);
    return 0;
  }

  // Allocate ctx from pool so it's auto-freed with request
  clj_req_ctx_t *const ctx =
      h2o_mem_alloc_shared(&req->pool, sizeof(*ctx), cleanup_request);
  if (!ctx) {
    send_error(INTERNAL_SERVER_ERROR, REQ_ERROR, req);
  }
  memset(ctx, 0, sizeof(*ctx));
  ctx->req = req;

  /* Generate req_id string: "{uuid}-{req_id}" */
  const char *conn_uuid = h2o_conn_get_uuid(req->conn);
  uint64_t req_num = req->conn->callbacks->get_req_id(req);

  /* req_id is embedded in ctx struct, so it's valid during cleanup */
  snprintf(ctx->req_id, sizeof(ctx->req_id), "%s-%" PRIu64, conn_uuid, req_num);

  // DEBUG_LOG("request start id=%s uuid=%s req=%" PRIu64,
  // ctx->req_id, conn_uuid, req_num);

  ctx->preferred_chunk_size = req->preferred_chunk_size;
  ctx->cleanup = 0;
  ctx->on_request_body_chunk = 0;
  clj_h2o_extract_req_meta(req, &ctx->meta);
  ctx->on_request_cleanup = 0;
  if (handler->on_request_cleanup)
    ctx->on_request_cleanup = handler->on_request_cleanup;

  // upcall to the jvm's on-request-callback
  int ret = handler->on_request(ctx);

  if (ret == CLJ_HANDLER_OVERLOADED) {
    h2o_send_error_503(req, "Service Unavailable", "Server overloaded", 0);
    return 0;
  } else if (ret == CLJ_HANDLER_SHUTTING_DOWN) {
    h2o_send_error_503(req, "Service Unavailable", "Server is shutting down",
                       H2O_SEND_ERROR_HTTP1_CLOSE_CONNECTION);
    return 0;
  } else if (ret == CLJ_HANDLER_DECLINED) {
    return -1;
  }

  if (ctx->meta.has_body && ctx->on_request_body_chunk != 0) {
    if (req->proceed_req != NULL) {
      // Set up our body write callback to receive chunks
      req->write_req.cb = clj_body_write_callback;
      req->write_req.ctx = ctx;
      if (req->entity.base != NULL && req->entity.len > 0) {
        // Deliver the already-buffered chunk
        ctx->on_request_body_chunk(ctx, req->entity.base, req->entity.len, 0);
      } else {
        // Nothing buffered: request the first chunk
        req->proceed_req(req, NULL);
      }
    } else if (req->entity.base != NULL) {
      // Small body - already buffered by h2o, deliver immediately
      if (ctx->on_request_body_chunk)
        ctx->on_request_body_chunk(ctx, req->entity.base, req->entity.len, 1);
    }
  }
  return 0;
}

/*
static void on_context_init(h2o_handler_t *_self, h2o_context_t *ctx) {
  struct clj_h2o_handler_t *self = (void *)_self;
  DEBUG_LOG("on_context_init");
}
static void on_context_dispose(h2o_handler_t *_self, h2o_context_t *ctx) {
  struct clj_h2o_handler_t *self = (void *)_self;
  DEBUG_LOG("on_context_dispose");
}
static void on_handler_dispose(h2o_handler_t *_self) {
  struct clj_h2o_handler_t *self = (void *)_self;
  DEBUG_LOG("on_handler_dispose");
}
*/

clj_h2o_handler_t *
clj_h2o_create_handler(h2o_hostconf_t *hostconf,
                       int (*on_request)(clj_req_ctx_t *),
                       void (*on_request_cleanup)(clj_req_ctx_t *),
                       const clj_h2o_flat_globalconf_t *flat) {

  h2o_pathconf_t *pathconf = h2o_config_register_path(hostconf, "/", 0);
  clj_h2o_handler_t *handler = (clj_h2o_handler_t *)h2o_create_handler(
      pathconf, sizeof(clj_h2o_handler_t));
  if (flat->has_compress_args) {
    h2o_compress_args_t comp = {
        .min_size = flat->compress_args_mine_size,
        .gzip = {.quality = flat->compress_args_gzip_quality},
        .brotli = {.quality = flat->compress_args_brotli_quality},
        .zstd = {.quality = flat->compress_args_zstd_quality},
    };
    h2o_compress_register(pathconf, &comp);
  }
  handler->on_request = on_request;
  handler->on_request_cleanup = on_request_cleanup;
  handler->shutting_down = 0;
  handler->super.on_req = request_handler;
  handler->super.supports_request_streaming = 1;
  handler->super.handles_expect = 1;
  // handler->super.on_context_init = on_context_init;
  // handler->super.on_context_dispose = on_context_dispose;
  // handler->super.dispose = on_handler_dispose;
  return handler;
}

void clj_h2o_handler_set_shutting_down(clj_h2o_handler_t *handler,
                                       int shutting_down) {
  if (handler == NULL)
    return;
  handler->shutting_down = shutting_down ? 1 : 0;
}

void clj_h2o_set_on_request_body_chunk(
    clj_req_ctx_t *ctx,
    void (*on_request_body_chunk)(clj_req_ctx_t *ctx, char *chunk,
                                  size_t chunk_len, int is_end_stream)) {
  if (ctx) {
    ctx->on_request_body_chunk = on_request_body_chunk;
  }
}

void clj_h2o_proceed_req(h2o_req_t *req) {
  if (req && req->proceed_req)
    req->proceed_req(req, NULL);
}

int clj_h2o_cancel_request(clj_req_ctx_t *ctx) {
  if (!ctx || !ctx->req)
    return 0;
  if (ctx->req->_generator != NULL) {
    h2o_send(ctx->req, NULL, 0, H2O_SEND_STATE_ERROR);
  }
  return 1;
}

static void clj_mt_dispose(void *unused, h2o_linklist_t *messages) {
  (void)unused;
  while (!h2o_linklist_is_empty(messages)) {
    clj_mt_msg_t *m =
        H2O_STRUCT_FROM_MEMBER(clj_mt_msg_t, super.link, messages->next);
    h2o_linklist_unlink(&m->super.link);
    free(m);
  }
}

static void clj_mt_on_recv(h2o_multithread_receiver_t *receiver,
                           h2o_linklist_t *messages) {
  (void)receiver;
  /* We only use this receiver to wake the loop; just drain and free messages */
  clj_mt_dispose(NULL, messages);
}

clj_mt_receiver_t *clj_h2o_mt_create_wakeup_receiver(h2o_context_t *ctx) {
  clj_mt_receiver_t *wr = calloc(1, sizeof(*wr));
  if (wr == NULL)
    return NULL;
  wr->queue = ctx->queue;
  h2o_multithread_register_receiver(wr->queue, &wr->receiver, clj_mt_on_recv);
  return wr;
}

void clj_h2o_mt_destroy_wakeup_receiver(clj_mt_receiver_t *wr) {
  if (wr == NULL)
    return;
  h2o_multithread_unregister_receiver(wr->queue, &wr->receiver);
  free(wr);
}

void clj_h2o_mt_wakeup(clj_mt_receiver_t *wr) {
  if (wr == NULL)
    return;
  /* just wake the loop; no message allocation necessary */
  h2o_multithread_send_message(&wr->receiver, NULL);
}

static h2o_globalconf_t *
clj_h2o_apply_flat_config(h2o_globalconf_t *conf,
                          const clj_h2o_flat_globalconf_t *flat) {
  if (!conf || !flat)
    return conf;

  if (flat->has_server_name && flat->server_name != NULL) {
    size_t len = strlen(flat->server_name);
    conf->server_name = h2o_strdup(NULL, flat->server_name, len);
  }

  if (flat->has_proxy_status_identity && flat->proxy_status_identity != NULL) {
    size_t len = strlen(flat->proxy_status_identity);
    conf->proxy_status_identity =
        h2o_strdup(NULL, flat->proxy_status_identity, len);
  }

  if (flat->has_max_request_entity_size) {
    conf->max_request_entity_size = flat->max_request_entity_size;
  }

  if (flat->has_max_delegations) {
    conf->max_delegations = flat->max_delegations;
  }

  if (flat->has_max_reprocesses) {
    conf->max_reprocesses = flat->max_reprocesses;
  }

  if (flat->has_handshake_timeout) {
    conf->handshake_timeout = flat->handshake_timeout;
  }

  if (flat->has_max_spare_pipes) {
    conf->max_spare_pipes = flat->max_spare_pipes;
  }

  if (flat->has_http1__req_timeout) {
    conf->http1.req_timeout = flat->http1__req_timeout;
  }

  if (flat->has_http1__req_io_timeout) {
    conf->http1.req_io_timeout = flat->http1__req_io_timeout;
  }

  if (flat->has_http1__upgrade_to_http2) {
    conf->http1.upgrade_to_http2 = flat->http1__upgrade_to_http2;
  }

  if (flat->has_http2__idle_timeout) {
    conf->http2.idle_timeout = flat->http2__idle_timeout;
  }

  if (flat->has_http2__graceful_shutdown_timeout) {
    conf->http2.graceful_shutdown_timeout =
        flat->http2__graceful_shutdown_timeout;
  }

  if (flat->has_http2__max_streams) {
    conf->http2.max_streams = flat->http2__max_streams;
  }

  if (flat->has_http2__max_concurrent_requests_per_connection) {
    conf->http2.max_concurrent_requests_per_connection =
        flat->http2__max_concurrent_requests_per_connection;
  }

  if (flat->has_http2__max_concurrent_streaming_requests_per_connection) {
    conf->http2.max_concurrent_streaming_requests_per_connection =
        flat->http2__max_concurrent_streaming_requests_per_connection;
  }

  if (flat->has_http2__max_streams_for_priority) {
    conf->http2.max_streams_for_priority =
        flat->http2__max_streams_for_priority;
  }

  if (flat->has_http2__active_stream_window_size) {
    conf->http2.active_stream_window_size =
        flat->http2__active_stream_window_size;
  }

  if (flat->has_http2__dos_delay) {
    conf->http2.dos_delay = flat->http2__dos_delay;
  }

  if (flat->has_http3__idle_timeout) {
    conf->http3.idle_timeout = flat->http3__idle_timeout;
  }

  if (flat->has_http3__graceful_shutdown_timeout) {
    conf->http3.graceful_shutdown_timeout =
        flat->http3__graceful_shutdown_timeout;
  }

  if (flat->has_http3__active_stream_window_size) {
    conf->http3.active_stream_window_size =
        flat->http3__active_stream_window_size;
  }

  if (flat->has_http3__ack_frequency) {
    conf->http3.ack_frequency = flat->http3__ack_frequency;
  }

  return conf;
}

void clj_h2o_create_globalconf(h2o_globalconf_t *conf,
                               const clj_h2o_flat_globalconf_t *flat) {
  if (conf == NULL)
    return;

  h2o_config_init(conf);

  if (flat != NULL)
    clj_h2o_apply_flat_config(conf, flat);

  return;
}

/* TLS/SSL Support Implementation */

static int clj_tls_lookup_pem_for_sni(clj_tls_lookup_cb lookup_cb,
                                      void *lookup_user_ctx,
                                      const uint8_t *sni_hostname,
                                      size_t sni_hostname_len,
                                      uint8_t **cert_chain_pem_out,
                                      size_t *cert_chain_pem_len_out,
                                      uint8_t **private_key_pem_out,
                                      size_t *private_key_pem_len_out) {
  if (lookup_cb == NULL)
    return 0;

  uint8_t *cert_chain_pem = NULL;
  uint8_t *private_key_pem = NULL;
  size_t cert_chain_pem_len = 0;
  size_t private_key_pem_len = 0;
  const uint8_t *lookup_hostname = sni_hostname;
  size_t lookup_hostname_len = sni_hostname_len;
  if (lookup_hostname == NULL)
    lookup_hostname_len = 0;
  if (lookup_hostname_len == 0)
    lookup_hostname = NULL;

  int status = lookup_cb(lookup_hostname, lookup_hostname_len,
                  &cert_chain_pem, &cert_chain_pem_len, &private_key_pem,
                  &private_key_pem_len, lookup_user_ctx);

  if (status != 1) {
    free(cert_chain_pem);
    free(private_key_pem);
    return status;
  }

  if (cert_chain_pem == NULL || cert_chain_pem_len == 0 ||
      private_key_pem == NULL || private_key_pem_len == 0) {
    free(cert_chain_pem);
    free(private_key_pem);
    return -1;
  }

  *cert_chain_pem_out = cert_chain_pem;
  *cert_chain_pem_len_out = cert_chain_pem_len;
  *private_key_pem_out = private_key_pem;
  *private_key_pem_len_out = private_key_pem_len;
  return 1;
}

static int clj_bytes_equal(const uint8_t *lhs, size_t lhs_len,
                           const uint8_t *rhs, size_t rhs_len) {
  if (lhs_len != rhs_len)
    return 0;
  if (lhs_len == 0)
    return 1;
  if (lhs == NULL || rhs == NULL)
    return 0;
  return memcmp(lhs, rhs, lhs_len) == 0;
}

static int clj_ssl_use_chain_from_pem(SSL *ssl, const uint8_t *cert_chain_pem,
                                      size_t cert_chain_pem_len) {
  int ok = 0;
  BIO *cert_bio = NULL;
  X509 *leaf = NULL;

  if (cert_chain_pem_len > INT_MAX)
    return 0;

  cert_bio = BIO_new_mem_buf(cert_chain_pem, (int)cert_chain_pem_len);
  if (cert_bio == NULL)
    goto Exit;

  leaf = PEM_read_bio_X509_AUX(cert_bio, NULL, NULL, NULL);
  if (leaf == NULL)
    goto Exit;

  if (SSL_use_certificate(ssl, leaf) != 1)
    goto Exit;

#if OPENSSL_VERSION_NUMBER >= 0x10002000L
  SSL_clear_chain_certs(ssl);
#endif

  while (1) {
    X509 *chain_cert = PEM_read_bio_X509(cert_bio, NULL, NULL, NULL);
    if (chain_cert == NULL) {
      ERR_clear_error(); /* EOF on PEM chain */
      break;
    }
    if (SSL_add1_chain_cert(ssl, chain_cert) != 1) {
      X509_free(chain_cert);
      goto Exit;
    }
    X509_free(chain_cert);
  }

  ok = 1;

Exit:
  if (leaf != NULL)
    X509_free(leaf);
  if (cert_bio != NULL)
    BIO_free(cert_bio);
  return ok;
}

static int clj_ssl_use_private_key_from_pem(SSL *ssl,
                                            const uint8_t *private_key_pem,
                                            size_t private_key_pem_len) {
  int ok = 0;
  BIO *key_bio = NULL;
  EVP_PKEY *pkey = NULL;

  if (private_key_pem_len > INT_MAX)
    return 0;

  key_bio = BIO_new_mem_buf(private_key_pem, (int)private_key_pem_len);
  if (key_bio == NULL)
    goto Exit;

  pkey = PEM_read_bio_PrivateKey(key_bio, NULL, NULL, NULL);
  if (pkey == NULL)
    goto Exit;

  if (SSL_use_PrivateKey(ssl, pkey) != 1)
    goto Exit;
  if (SSL_check_private_key(ssl) != 1)
    goto Exit;

  ok = 1;

Exit:
  if (pkey != NULL)
    EVP_PKEY_free(pkey);
  if (key_bio != NULL)
    BIO_free(key_bio);
  return ok;
}

static int clj_ssl_apply_pem_identity(SSL *ssl, const uint8_t *cert_chain_pem,
                                      size_t cert_chain_pem_len,
                                      const uint8_t *private_key_pem,
                                      size_t private_key_pem_len) {
  if (!clj_ssl_use_chain_from_pem(ssl, cert_chain_pem, cert_chain_pem_len))
    return 0;
  if (!clj_ssl_use_private_key_from_pem(ssl, private_key_pem,
                                        private_key_pem_len))
    return 0;
  return 1;
}

static int clj_ssl_select_certificate_cb(SSL *ssl, void *arg) {
  clj_tls_lookup_binding_t *binding = (clj_tls_lookup_binding_t *)arg;

  if (binding == NULL || binding->cb == NULL) {
    /* Legacy/manual mode: no dynamic callback, use cert loaded on SSL_CTX. */
    return SSL_get_certificate(ssl) != NULL ? 1 : 0;
  }

  const char *server_name = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name);
  const uint8_t *server_name_bytes = NULL;
  size_t server_name_len = 0;
  if (server_name != NULL && server_name[0] != '\0') {
    server_name_bytes = (const uint8_t *)server_name;
    server_name_len = strlen(server_name);
  }
  uint8_t *cert_chain_pem = NULL;
  uint8_t *private_key_pem = NULL;
  size_t cert_chain_pem_len = 0;
  size_t private_key_pem_len = 0;
  int status = clj_tls_lookup_pem_for_sni(binding->cb, binding->user_ctx,
                                          server_name_bytes, server_name_len,
                                          &cert_chain_pem,
                                          &cert_chain_pem_len,
                                          &private_key_pem,
                                          &private_key_pem_len);
  if (status != 1)
    return 0;

  int ok = clj_ssl_apply_pem_identity(ssl, cert_chain_pem, cert_chain_pem_len,
                                      private_key_pem, private_key_pem_len);
  free(cert_chain_pem);
  free(private_key_pem);
  return ok ? 1 : 0;
}

SSL_CTX *clj_h2o_create_ssl_ctx(const char *cert_file, const char *key_file,
                                int enable_http2, clj_tls_lookup_cb lookup_cb,
                                void *lookup_user_ctx) {
  int has_static_identity =
      cert_file != NULL && cert_file[0] != '\0' && key_file != NULL &&
      key_file[0] != '\0';
  int has_partial_identity =
      (cert_file != NULL && cert_file[0] != '\0') ^
      (key_file != NULL && key_file[0] != '\0');

  if (has_partial_identity)
    return NULL;

  clj_tls_lookup_binding_t *binding =
      calloc(1, sizeof(clj_tls_lookup_binding_t));
  if (binding == NULL)
    return NULL;
  binding->cb = lookup_cb;
  binding->user_ctx = lookup_user_ctx;

  SSL_load_error_strings();
  SSL_library_init();
  OpenSSL_add_all_algorithms();

  SSL_CTX *ssl_ctx = SSL_CTX_new(TLS_server_method());
  if (!ssl_ctx) {
    ERR_print_errors_fp(stderr);
    free(binding);
    return NULL;
  }

  SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 |
                                   SSL_OP_NO_COMPRESSION);

  if (has_static_identity) {
    if (SSL_CTX_use_certificate_chain_file(ssl_ctx, cert_file) != 1) {
      ERR_print_errors_fp(stderr);
      SSL_CTX_free(ssl_ctx);
      free(binding);
      return NULL;
    }

    if (SSL_CTX_use_PrivateKey_file(ssl_ctx, key_file, SSL_FILETYPE_PEM) != 1) {
      ERR_print_errors_fp(stderr);
      SSL_CTX_free(ssl_ctx);
      free(binding);
      return NULL;
    }
  }

  SSL_CTX_set_app_data(ssl_ctx, binding);

  /* Per-handshake cert selection from dynamic lookup callback. */
  SSL_CTX_set_cert_cb(ssl_ctx, clj_ssl_select_certificate_cb, binding);

  /* Mozilla Intermediate cipher suite (modern, widely compatible) */
  SSL_CTX_set_cipher_list(
      ssl_ctx, "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
               "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:"
               "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256");

  if (enable_http2) {
    h2o_ssl_register_alpn_protocols(ssl_ctx, h2o_http2_alpn_protocols);
  }

  return ssl_ctx;
}

void clj_h2o_free_ssl_ctx(SSL_CTX *ssl_ctx) {
  if (ssl_ctx) {
    free(SSL_CTX_get_app_data(ssl_ctx));
    SSL_CTX_set_app_data(ssl_ctx, NULL);
    SSL_CTX_free(ssl_ctx);
  }
}

/* HTTP/3 (QUIC) context structure holding all resources for one worker */
struct clj_http3_ctx_t {
  h2o_http3_server_ctx_t h3_ctx;
  h2o_accept_ctx_t accept_ctx;
  h2o_socket_t *udp_sock;
  quicly_cid_plaintext_t next_cid;
  int fd;
};

typedef struct clj_ptls_identity_cache_entry {
  uint8_t *hostname;
  size_t hostname_len;
  uint8_t *cert_chain_pem;
  size_t cert_chain_pem_len;
  uint8_t *private_key_pem;
  size_t private_key_pem_len;
  ptls_context_t ctx;
  ptls_openssl_sign_certificate_t sign_certificate;
  struct clj_ptls_identity_cache_entry *next;
} clj_ptls_identity_cache_entry_t;

typedef struct clj_ptls_wrapper_t {
  ptls_context_t ctx;
  ptls_on_client_hello_t on_client_hello;
  ptls_openssl_sign_certificate_t sign_certificate;
  clj_tls_lookup_cb lookup_cb;
  void *lookup_user_ctx;
  clj_ptls_identity_cache_entry_t *cache_head;
  pthread_mutex_t cache_mutex;
} clj_ptls_wrapper_t;

/* ALPN negotiation callback for HTTP/3 */
static int clj_on_client_hello_cb(ptls_on_client_hello_t *self,
                                  ptls_t *tls,
                                  ptls_on_client_hello_parameters_t *params) {
  clj_ptls_wrapper_t *wrapper =
      H2O_STRUCT_FROM_MEMBER(clj_ptls_wrapper_t, on_client_hello, self);

  if (wrapper->lookup_cb != NULL) {
    const uint8_t *hostname = NULL;
    size_t hostname_len = 0;
    if (params->server_name.base != NULL && params->server_name.len != 0) {
      if (ptls_set_server_name(tls, (const char *)params->server_name.base,
                               params->server_name.len) != 0)
        return PTLS_ALERT_INTERNAL_ERROR;
      hostname = (const uint8_t *)params->server_name.base;
      hostname_len = params->server_name.len;
    }

    uint8_t *cert_chain_pem = NULL;
    uint8_t *private_key_pem = NULL;
    size_t cert_chain_pem_len = 0;
    size_t private_key_pem_len = 0;
    int status = clj_tls_lookup_pem_for_sni(
        wrapper->lookup_cb, wrapper->lookup_user_ctx, hostname, hostname_len,
        &cert_chain_pem,
        &cert_chain_pem_len, &private_key_pem, &private_key_pem_len);
    /* Return miss to fail closed without triggering the HTTP/3 close-path crash. */
    if (status != 1)
      return 0;

    pthread_mutex_lock(&wrapper->cache_mutex);

    clj_ptls_identity_cache_entry_t *entry = wrapper->cache_head;
    while (entry != NULL) {
      if (clj_bytes_equal(entry->hostname, entry->hostname_len, hostname,
                          hostname_len))
        break;
      entry = entry->next;
    }

    if (entry == NULL || entry->cert_chain_pem_len != cert_chain_pem_len ||
        memcmp(entry->cert_chain_pem, cert_chain_pem, cert_chain_pem_len) !=
            0 ||
        entry->private_key_pem_len != private_key_pem_len ||
        memcmp(entry->private_key_pem, private_key_pem, private_key_pem_len) !=
            0) {
      clj_ptls_identity_cache_entry_t *new_entry =
          calloc(1, sizeof(clj_ptls_identity_cache_entry_t));
      if (new_entry == NULL) {
        pthread_mutex_unlock(&wrapper->cache_mutex);
        free(cert_chain_pem);
        free(private_key_pem);
        return PTLS_ALERT_INTERNAL_ERROR;
      }

      if (hostname_len != 0) {
        new_entry->hostname = clj_h2o_tls_memdup(hostname, hostname_len);
        if (new_entry->hostname == NULL) {
          pthread_mutex_unlock(&wrapper->cache_mutex);
          free(cert_chain_pem);
          free(private_key_pem);
          free(new_entry);
          return PTLS_ALERT_INTERNAL_ERROR;
        }
      }
      new_entry->hostname_len = hostname_len;

      new_entry->cert_chain_pem = cert_chain_pem;
      new_entry->cert_chain_pem_len = cert_chain_pem_len;
      new_entry->private_key_pem = private_key_pem;
      new_entry->private_key_pem_len = private_key_pem_len;
      new_entry->ctx = wrapper->ctx;
      new_entry->ctx.certificates.list = NULL;
      new_entry->ctx.certificates.count = 0;
      new_entry->ctx.sign_certificate = NULL;

      if (new_entry->cert_chain_pem_len > INT_MAX ||
          new_entry->private_key_pem_len > INT_MAX) {
        pthread_mutex_unlock(&wrapper->cache_mutex);
        free(new_entry->hostname);
        free(new_entry->cert_chain_pem);
        free(new_entry->private_key_pem);
        free(new_entry);
        return PTLS_ALERT_INTERNAL_ERROR;
      }

      BIO *cert_bio = BIO_new_mem_buf(new_entry->cert_chain_pem,
                                      (int)new_entry->cert_chain_pem_len);
      BIO *key_bio = NULL;
      X509 *leaf = NULL;
      STACK_OF(X509) *chain = NULL;
      EVP_PKEY *pkey = NULL;
      int init_ok = 0;

      if (cert_bio != NULL) {
        leaf = PEM_read_bio_X509_AUX(cert_bio, NULL, NULL, NULL);
        if (leaf != NULL) {
          chain = sk_X509_new_null();
          if (chain != NULL) {
            while (1) {
              X509 *extra = PEM_read_bio_X509(cert_bio, NULL, NULL, NULL);
              if (extra == NULL) {
                ERR_clear_error();
                break;
              }
              if (sk_X509_push(chain, extra) == 0) {
                X509_free(extra);
                break;
              }
            }
            if (ptls_openssl_load_certificates(&new_entry->ctx, leaf, chain) ==
                0) {
              key_bio =
                  BIO_new_mem_buf(new_entry->private_key_pem,
                                  (int)new_entry->private_key_pem_len);
              if (key_bio != NULL) {
                pkey = PEM_read_bio_PrivateKey(key_bio, NULL, NULL, NULL);
                if (pkey != NULL &&
                    ptls_openssl_init_sign_certificate(
                        &new_entry->sign_certificate, pkey) == 0) {
                  new_entry->ctx.sign_certificate =
                      &new_entry->sign_certificate.super;
                  init_ok = 1;
                }
              }
            }
          }
        }
      }

      if (pkey != NULL)
        EVP_PKEY_free(pkey);
      if (key_bio != NULL)
        BIO_free(key_bio);
      if (chain != NULL)
        sk_X509_pop_free(chain, X509_free);
      if (leaf != NULL)
        X509_free(leaf);
      if (cert_bio != NULL)
        BIO_free(cert_bio);

      if (!init_ok) {
        pthread_mutex_unlock(&wrapper->cache_mutex);
        if (new_entry->ctx.certificates.list != NULL) {
          for (size_t i = 0; i != new_entry->ctx.certificates.count; ++i)
            free(new_entry->ctx.certificates.list[i].base);
          free(new_entry->ctx.certificates.list);
        }
        free(new_entry->hostname);
        free(new_entry->cert_chain_pem);
        free(new_entry->private_key_pem);
        free(new_entry);
        return PTLS_ALERT_INTERNAL_ERROR;
      }

      new_entry->next = wrapper->cache_head;
      wrapper->cache_head = new_entry;
      entry = new_entry;
    } else {
      free(cert_chain_pem);
      free(private_key_pem);
    }

    ptls_set_context(tls, &entry->ctx);
    pthread_mutex_unlock(&wrapper->cache_mutex);
  } else if (wrapper->ctx.sign_certificate == NULL ||
             wrapper->ctx.certificates.list == NULL) {
    return PTLS_ALERT_UNRECOGNIZED_NAME;
  }

  if (params->negotiated_protocols.count == 0)
    return 0;

  /* Match against h2o_http3_alpn protocols */
  for (size_t i = 0; i < sizeof(h2o_http3_alpn) / sizeof(h2o_http3_alpn[0]);
       ++i) {
    for (size_t j = 0; j < params->negotiated_protocols.count; ++j) {
      if (h2o_memis(h2o_http3_alpn[i].base, h2o_http3_alpn[i].len,
                    params->negotiated_protocols.list[j].base,
                    params->negotiated_protocols.list[j].len)) {
        ptls_set_negotiated_protocol(tls, (const char *)h2o_http3_alpn[i].base,
                                     h2o_http3_alpn[i].len);
        return 0;
      }
    }
  }
  return 0;
}

ptls_context_t *clj_h2o_create_ptls_ctx(const char *cert_file,
                                        const char *key_file,
                                        clj_tls_lookup_cb lookup_cb,
                                        void *lookup_user_ctx) {
  int has_static_identity =
      cert_file != NULL && cert_file[0] != '\0' && key_file != NULL &&
      key_file[0] != '\0';
  int has_partial_identity =
      (cert_file != NULL && cert_file[0] != '\0') ^
      (key_file != NULL && key_file[0] != '\0');

  if (has_partial_identity)
    return NULL;

  clj_ptls_wrapper_t *wrapper = calloc(1, sizeof(clj_ptls_wrapper_t));
  if (!wrapper)
    return NULL;

  pthread_mutex_init(&wrapper->cache_mutex, NULL);

  wrapper->on_client_hello.cb = clj_on_client_hello_cb;
  wrapper->lookup_cb = lookup_cb;
  wrapper->lookup_user_ctx = lookup_user_ctx;

  wrapper->ctx = (ptls_context_t){
      .random_bytes = ptls_openssl_random_bytes,
      .get_time = &ptls_get_time,
      .key_exchanges = ptls_openssl_key_exchanges,
      .cipher_suites = ptls_openssl_cipher_suites,
      .on_client_hello = &wrapper->on_client_hello,
  };

  if (has_static_identity) {
    if (ptls_load_certificates(&wrapper->ctx, cert_file) != 0) {
      DEBUG_LOG("failed to load certificates from %s", cert_file);
      pthread_mutex_destroy(&wrapper->cache_mutex);
      free(wrapper);
      return NULL;
    }

    FILE *fp = fopen(key_file, "r");
    if (!fp) {
      DEBUG_LOG("failed to open key file: %s", key_file);
      free(wrapper->ctx.certificates.list);
      pthread_mutex_destroy(&wrapper->cache_mutex);
      free(wrapper);
      return NULL;
    }

    EVP_PKEY *pkey = PEM_read_PrivateKey(fp, NULL, NULL, NULL);
    fclose(fp);
    if (!pkey) {
      DEBUG_LOG("failed to load private key from %s", key_file);
      free(wrapper->ctx.certificates.list);
      pthread_mutex_destroy(&wrapper->cache_mutex);
      free(wrapper);
      return NULL;
    }

    if (ptls_openssl_init_sign_certificate(&wrapper->sign_certificate, pkey) !=
        0) {
      DEBUG_LOG("failed to setup private key");
      EVP_PKEY_free(pkey);
      free(wrapper->ctx.certificates.list);
      pthread_mutex_destroy(&wrapper->cache_mutex);
      free(wrapper);
      return NULL;
    }
    EVP_PKEY_free(pkey);

    wrapper->ctx.sign_certificate = &wrapper->sign_certificate.super;
  }

  return &wrapper->ctx;
}

void clj_h2o_free_ptls_ctx(ptls_context_t *ctx) {
  if (!ctx)
    return;

  clj_ptls_wrapper_t *wrapper =
      H2O_STRUCT_FROM_MEMBER(clj_ptls_wrapper_t, ctx, ctx);

  if (wrapper->ctx.sign_certificate != NULL)
    ptls_openssl_dispose_sign_certificate(&wrapper->sign_certificate);

  if (wrapper->ctx.certificates.list) {
    for (size_t i = 0; i != wrapper->ctx.certificates.count; ++i)
      free(wrapper->ctx.certificates.list[i].base);
    free(wrapper->ctx.certificates.list);
  }

  clj_ptls_identity_cache_entry_t *entry = wrapper->cache_head;
  while (entry != NULL) {
    clj_ptls_identity_cache_entry_t *next = entry->next;
    ptls_openssl_dispose_sign_certificate(&entry->sign_certificate);
    if (entry->ctx.certificates.list != NULL) {
      for (size_t i = 0; i != entry->ctx.certificates.count; ++i)
        free(entry->ctx.certificates.list[i].base);
      free(entry->ctx.certificates.list);
    }
    free(entry->hostname);
    free(entry->cert_chain_pem);
    free(entry->private_key_pem);
    free(entry);
    entry = next;
  }

  pthread_mutex_destroy(&wrapper->cache_mutex);

  free(wrapper);
}

/* Internal structure to hold quicly context and its CID encryptor */
typedef struct {
  quicly_context_t ctx;
  quicly_cid_encryptor_t *cid_encryptor;
} clj_quicly_wrapper_t;

quicly_context_t *clj_h2o_create_quicly_ctx(ptls_context_t *ptls_ctx,
                                            h2o_globalconf_t *globalconf) {
  if (!ptls_ctx || !globalconf)
    return NULL;

  clj_quicly_wrapper_t *wrapper = calloc(1, sizeof(clj_quicly_wrapper_t));
  if (!wrapper)
    return NULL;

  /* Generate random CID key */
  uint8_t cid_key[32];
  ptls_openssl_random_bytes(cid_key, sizeof(cid_key));

  wrapper->ctx = quicly_spec_context;
  wrapper->ctx.tls = ptls_ctx;
  wrapper->ctx.now = &quicly_default_now;
  wrapper->ctx.init_cc = &quicly_default_init_cc;
  wrapper->ctx.crypto_engine = &quicly_default_crypto_engine;

  wrapper->cid_encryptor = quicly_new_default_cid_encryptor(
      &ptls_openssl_aes128ecb, &ptls_openssl_aes128ecb, &ptls_openssl_sha256,
      ptls_iovec_init(cid_key, sizeof(cid_key)));

  if (!wrapper->cid_encryptor) {
    DEBUG_LOG("failed to create CID encryptor");
    free(wrapper);
    return NULL;
  }

  wrapper->ctx.cid_encryptor = wrapper->cid_encryptor;

  quicly_amend_ptls_context(ptls_ctx);
  h2o_http3_server_amend_quicly_context(globalconf, &wrapper->ctx);

  return &wrapper->ctx;
}

void clj_h2o_free_quicly_ctx(quicly_context_t *ctx) {
  if (!ctx)
    return;

  clj_quicly_wrapper_t *wrapper =
      H2O_STRUCT_FROM_MEMBER(clj_quicly_wrapper_t, ctx, ctx);

  if (wrapper->cid_encryptor)
    free(wrapper->cid_encryptor);

  free(wrapper);
}

/* HTTP/3 connection destroy wrapper - releases connection limit slot */
static void clj_on_http3_conn_destroy(h2o_quic_conn_t *conn) {
  clj_h2o_conn_limit_release();
  H2O_HTTP3_CONN_CALLBACKS.super.destroy_connection(conn);
}

/* Wrapper callbacks for HTTP/3 connections with connection limit tracking */
static h2o_http3_conn_callbacks_t clj_http3_conn_callbacks;
/* TODO: refactor HTTP/3 callback init state to be per-server instead of
   process-global, so multiple servers in one process do not share mutable
   callback initialization state. */
static int clj_http3_conn_callbacks_initialized = 0;

static void clj_init_http3_conn_callbacks(void) {
  if (clj_http3_conn_callbacks_initialized)
    return;
  clj_http3_conn_callbacks = H2O_HTTP3_CONN_CALLBACKS;
  clj_http3_conn_callbacks.super.destroy_connection = clj_on_http3_conn_destroy;
  clj_http3_conn_callbacks_initialized = 1;
}

/* HTTP/3 accept callback with connection limit enforcement */
static h2o_quic_conn_t *
clj_on_http3_accept(h2o_quic_ctx_t *quic_ctx, quicly_address_t *destaddr,
                    quicly_address_t *srcaddr, quicly_decoded_packet_t *packet) {
  h2o_http3_server_ctx_t *h3ctx =
      H2O_STRUCT_FROM_MEMBER(h2o_http3_server_ctx_t, super, quic_ctx);

  /* Enforce global connection limit before accepting */
  if (!clj_h2o_conn_limit_try_acquire())
    return NULL;

  clj_init_http3_conn_callbacks();

  h2o_http3_conn_t *conn = h2o_http3_server_accept(
      h3ctx, destaddr, srcaddr, packet, NULL, &clj_http3_conn_callbacks);

  /* Release if accept failed without creating a connection */
  if (!conn) {
    clj_h2o_conn_limit_release();
    return NULL;
  }
  if (&conn->super == &h2o_quic_accept_conn_decryption_failed) {
    clj_h2o_conn_limit_release();
    return NULL;
  }
  /* h2o_http3_accept_conn_closed means conn was created then destroyed;
     destroy callback already fired, so don't release again */
  if (conn == &h2o_http3_accept_conn_closed)
    return NULL;

  return &conn->super;
}

clj_http3_ctx_t *clj_h2o_http3_create_worker_ctx(h2o_context_t *h2o_ctx,
                                                 h2o_evloop_t *loop,
                                                 quicly_context_t *quic_ctx,
                                                 h2o_hostconf_t **hosts,
                                                 const char *host,
                                                 uint16_t port,
                                                 uint32_t thread_id) {
  if (!h2o_ctx || !loop || !quic_ctx || !hosts)
    return NULL;

  clj_http3_ctx_t *ctx = calloc(1, sizeof(clj_http3_ctx_t));
  if (!ctx)
    return NULL;

  /* Create UDP socket */
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;

  if (host && host[0] != '\0') {
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
      addr.sin_addr.s_addr = htonl(INADDR_ANY);
    }
  } else {
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
  }
  addr.sin_port = htons(port);

  int fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if (fd == -1) {
    DEBUG_LOG("failed to create UDP socket: %s", strerror(errno));
    free(ctx);
    return NULL;
  }

  int optval = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) != 0) {
    DEBUG_LOG("setsockopt(SO_REUSEADDR) failed: %s", strerror(errno));
    close(fd);
    free(ctx);
    return NULL;
  }

#ifdef IP_PKTINFO
  if (setsockopt(fd, IPPROTO_IP, IP_PKTINFO, &optval, sizeof(optval)) != 0) {
    DEBUG_LOG("setsockopt(IP_PKTINFO) failed: %s", strerror(errno));
    close(fd);
    free(ctx);
    return NULL;
  }
#endif

  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    DEBUG_LOG("bind(UDP) failed: %s", strerror(errno));
    close(fd);
    free(ctx);
    return NULL;
  }

  h2o_socket_set_df_bit(fd, addr.sin_family);

  /* QUIC reads datagrams directly; prevent socket layer from consuming them */
  ctx->udp_sock = h2o_evloop_socket_create(loop, fd, H2O_SOCKET_FLAG_DONT_READ);
  if (!ctx->udp_sock) {
    DEBUG_LOG("failed to create evloop socket for UDP");
    close(fd);
    free(ctx);
    return NULL;
  }

  ctx->fd = fd;

  ctx->next_cid = (quicly_cid_plaintext_t){
      .master_id = 0,
      .thread_id = thread_id,
      .node_id = 0,
  };

  ctx->accept_ctx.ctx = h2o_ctx;
  ctx->accept_ctx.hosts = hosts;

  h2o_http3_server_init_context(h2o_ctx, &ctx->h3_ctx.super, loop, ctx->udp_sock,
                                quic_ctx, &ctx->next_cid, clj_on_http3_accept,
                                NULL, 0);

  ctx->h3_ctx.accept_ctx = &ctx->accept_ctx;

  return ctx;
}

void clj_h2o_http3_stop_accepting(clj_http3_ctx_t *ctx) {
  if (!ctx)
    return;

  /* Following h2o main.c pattern (line 4572): set acceptor to NULL to stop
     accepting new HTTP/3 connections. New Initial packets will be rejected
     with version negotiation. Existing connections (including handshakes in
     progress) continue to completion. */
  ctx->h3_ctx.super.acceptor = NULL;
}

size_t clj_h2o_http3_num_connections(clj_http3_ctx_t *ctx) {
  if (!ctx)
    return 0;
  return h2o_quic_num_connections(&ctx->h3_ctx.super);
}

void clj_h2o_http3_dispose_worker_ctx(clj_http3_ctx_t *ctx) {
  if (!ctx)
    return;

  /* h2o_quic_dispose_context closes the socket internally */
  h2o_quic_dispose_context(&ctx->h3_ctx.super);

  free(ctx);
}

/* Global connection limit API implementation */

void clj_h2o_conn_limit_set_max(uint32_t max) {
  atomic_store(&clj_conn_max, max);
}

uint32_t clj_h2o_conn_limit_current(void) {
  return atomic_load(&clj_conn_count);
}

int clj_h2o_conn_limit_try_acquire(void) {
  uint32_t max = atomic_load(&clj_conn_max);

  /* Zero means unlimited */
  if (max == 0) {
    atomic_fetch_add(&clj_conn_count, 1);
    return 1;
  }

  /* Atomically increment if under limit */
  uint32_t current = atomic_load(&clj_conn_count);
  while (current < max) {
    if (atomic_compare_exchange_weak(&clj_conn_count, &current, current + 1)) {
      return 1;
    }
  }
  return 0;
}

void clj_h2o_conn_limit_release(void) {
  atomic_fetch_sub(&clj_conn_count, 1);
}

/* Thread-safe key manager structure */
struct clj_ticket_manager {
    pthread_rwlock_t rwlock;
    clj_session_ticket_t *keys;
    size_t num_keys;
    size_t capacity;
    uint32_t ticket_lifetime_seconds;

    /* QUIC transport params hash for 0-RTT validation */
    uint8_t quic_tp_tag[8];
    int quic_tp_tag_valid;
};

/* Wrapper for ptls encrypt_ticket callback */
struct clj_encrypt_ticket {
    ptls_encrypt_ticket_t super;  /* Must be first (callback interface) */
    clj_ticket_manager_t *mgr;    /* Key manager reference */
    uint8_t is_quic;              /* QUIC mode flag */
};

/* Thread-local to pass manager to OpenSSL callback */
static __thread clj_ticket_manager_t *tls_current_mgr;

/* Get current time in milliseconds */
static uint64_t current_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* Generate initial random key with configured lifetime */
static void generate_random_key(clj_session_ticket_t *key, uint64_t now_ms,
                                uint32_t lifetime_seconds) {
    ptls_openssl_random_bytes(key->name, CLJ_TICKET_KEY_NAME_LEN);
    ptls_openssl_random_bytes(key->aes_key, CLJ_TICKET_AES_KEY_LEN);
    ptls_openssl_random_bytes(key->hmac_key, CLJ_TICKET_HMAC_KEY_LEN);
    key->not_before = now_ms;
    key->not_after = now_ms + (uint64_t)lifetime_seconds * 1000 - 1;
}

clj_ticket_manager_t *clj_ticket_manager_create(uint32_t ticket_lifetime_seconds) {
    clj_ticket_manager_t *mgr = calloc(1, sizeof(*mgr));
    if (!mgr) return NULL;

    if (pthread_rwlock_init(&mgr->rwlock, NULL) != 0) {
        free(mgr);
        return NULL;
    }

    mgr->ticket_lifetime_seconds = ticket_lifetime_seconds;
    mgr->quic_tp_tag_valid = 0;

    /* Generate initial key so server is ready immediately */
    clj_session_ticket_t initial_key;
    generate_random_key(&initial_key, current_time_ms(), ticket_lifetime_seconds);

    mgr->keys = malloc(sizeof(initial_key));
    if (!mgr->keys) {
        pthread_rwlock_destroy(&mgr->rwlock);
        free(mgr);
        return NULL;
    }
    memcpy(mgr->keys, &initial_key, sizeof(initial_key));
    mgr->num_keys = 1;
    mgr->capacity = 1;

    /* Secure erase the stack copy */
    explicit_bzero(&initial_key, sizeof(initial_key));

    return mgr;
}

void clj_ticket_manager_destroy(clj_ticket_manager_t *mgr) {
    if (!mgr) return;

    pthread_rwlock_wrlock(&mgr->rwlock);

    /* Secure erase all keys */
    if (mgr->keys) {
        explicit_bzero(mgr->keys, mgr->num_keys * sizeof(*mgr->keys));
        free(mgr->keys);
    }

    /* Secure erase QUIC tag */
    explicit_bzero(mgr->quic_tp_tag, sizeof(mgr->quic_tp_tag));

    pthread_rwlock_unlock(&mgr->rwlock);
    pthread_rwlock_destroy(&mgr->rwlock);

    free(mgr);
}

int clj_ticket_manager_set_keys(
    clj_ticket_manager_t *mgr,
    const clj_session_ticket_t *new_keys,
    size_t num_keys)
{
    if (!mgr || (num_keys > 0 && !new_keys)) return -1;

    /* Allocate new array outside lock */
    clj_session_ticket_t *copy = NULL;
    if (num_keys > 0) {
        copy = malloc(num_keys * sizeof(*copy));
        if (!copy) return -1;
        memcpy(copy, new_keys, num_keys * sizeof(*copy));
    }

    /* Atomic swap under write lock */
    pthread_rwlock_wrlock(&mgr->rwlock);
    clj_session_ticket_t *old = mgr->keys;
    size_t old_count = mgr->num_keys;
    mgr->keys = copy;
    mgr->num_keys = num_keys;
    mgr->capacity = num_keys;
    pthread_rwlock_unlock(&mgr->rwlock);

    /* Secure erase and free old keys after unlock */
    if (old) {
        explicit_bzero(old, old_count * sizeof(*old));
        free(old);
    }

    return 0;
}

size_t clj_ticket_manager_key_count(clj_ticket_manager_t *mgr) {
    if (!mgr) return 0;

    pthread_rwlock_rdlock(&mgr->rwlock);
    size_t count = mgr->num_keys;
    pthread_rwlock_unlock(&mgr->rwlock);

    return count;
}

void clj_ticket_manager_set_quic_tag(
    clj_ticket_manager_t *mgr,
    const quicly_context_t *quic_ctx)
{
    if (!mgr || !quic_ctx) return;

    /* Hash transport parameters that affect 0-RTT compatibility.
       Uses first 8 bytes of SHA256 hash. */
    uint64_t params[6] = {
        quic_ctx->transport_params.max_streams_bidi,
        quic_ctx->transport_params.max_streams_uni,
        quic_ctx->transport_params.max_stream_data.bidi_local,
        quic_ctx->transport_params.max_stream_data.bidi_remote,
        quic_ctx->transport_params.max_stream_data.uni,
        quic_ctx->transport_params.max_data
    };

    uint8_t hash[32];
    if (ptls_calc_hash(&ptls_openssl_sha256, hash, params, sizeof(params)) != 0)
        return;

    pthread_rwlock_wrlock(&mgr->rwlock);
    memcpy(mgr->quic_tp_tag, hash, 8);
    mgr->quic_tp_tag_valid = 1;
    pthread_rwlock_unlock(&mgr->rwlock);
}

/* OpenSSL ticket key callback - supplies key material; picotls does crypto */
static int clj_ticket_key_callback(
    unsigned char *key_name,   /* IN (decrypt) or OUT (encrypt): 16-byte key ID */
    unsigned char *iv,         /* IN (decrypt) or OUT (encrypt): IV */
    EVP_CIPHER_CTX *ctx,       /* OUT: initialized cipher context */
    HMAC_CTX *hctx,            /* OUT: initialized HMAC context */
    int enc)                   /* 1 = encrypt, 0 = decrypt */
{
    clj_ticket_manager_t *mgr = tls_current_mgr;
    if (!mgr) return 0;

    clj_session_ticket_t *key = NULL;

    pthread_rwlock_rdlock(&mgr->rwlock);

    if (enc) {
        /* Encryption: find newest valid key */
        uint64_t now = current_time_ms();
        for (size_t i = 0; i < mgr->num_keys; i++) {
            if (mgr->keys[i].not_before <= now && now < mgr->keys[i].not_after) {
                key = &mgr->keys[i];
                break;
            }
        }
        if (!key) {
            pthread_rwlock_unlock(&mgr->rwlock);
            return 0;  /* No valid key */
        }

        /* Output key name, generate random IV */
        memcpy(key_name, key->name, 16);
        RAND_bytes(iv, EVP_MAX_IV_LENGTH);

    } else {
        /* Decryption: lookup key by name from ticket */
        for (size_t i = 0; i < mgr->num_keys; i++) {
            if (memcmp(mgr->keys[i].name, key_name, 16) == 0) {
                key = &mgr->keys[i];
                break;
            }
        }
        if (!key) {
            pthread_rwlock_unlock(&mgr->rwlock);
            return 0;  /* Unknown key = ticket invalid */
        }
    }

    /* Initialize OpenSSL contexts with key material.
       OpenSSL copies key bytes internally so safe to unlock after. */
    EVP_CipherInit_ex(ctx, EVP_aes_256_cbc(), NULL, key->aes_key, iv, enc);
    HMAC_Init_ex(hctx, key->hmac_key, CLJ_TICKET_HMAC_KEY_LEN, EVP_sha256(), NULL);

    pthread_rwlock_unlock(&mgr->rwlock);
    return 1;
}

/* picotls encrypt_ticket callback (layers 1-3).
   Thin wrapper that handles QUIC tag and delegates to picotls. */
static int clj_encrypt_ticket_cb(
    ptls_encrypt_ticket_t *_self,
    ptls_t *tls,
    int is_encrypt,
    ptls_buffer_t *dst,
    ptls_iovec_t src)
{
    (void)tls;  /* unused */
    clj_encrypt_ticket_t *self = (clj_encrypt_ticket_t *)_self;
    clj_ticket_manager_t *mgr = self->mgr;

    /* Pass manager to callback via thread-local */
    tls_current_mgr = mgr;

    if (is_encrypt) {
        /* Append QUIC transport params tag if QUIC mode */
        ptls_iovec_t plaintext = src;
        uint8_t *buf = NULL;

        if (self->is_quic && mgr->quic_tp_tag_valid) {
            buf = malloc(src.len + 8);
            if (!buf) {
                tls_current_mgr = NULL;
                return PTLS_ERROR_NO_MEMORY;
            }
            memcpy(buf, src.base, src.len);
            pthread_rwlock_rdlock(&mgr->rwlock);
            memcpy(buf + src.len, mgr->quic_tp_tag, 8);
            pthread_rwlock_unlock(&mgr->rwlock);
            plaintext = (ptls_iovec_t){buf, src.len + 8};
        }

        /* Delegate to picotls - does AES-CBC + HMAC */
        int ret = ptls_openssl_encrypt_ticket(dst, plaintext, clj_ticket_key_callback);

        if (buf) free(buf);
        tls_current_mgr = NULL;
        return ret;

    } else {
        /* Delegate decryption to picotls */
        size_t start_off = dst->off;
        int ret = ptls_openssl_decrypt_ticket(dst, src, clj_ticket_key_callback);

        tls_current_mgr = NULL;

        if (ret != 0)
            return ret;

        /* Validate and strip QUIC tag if present */
        if (self->is_quic && mgr->quic_tp_tag_valid) {
            size_t decrypted_len = dst->off - start_off;
            if (decrypted_len < 8)
                return PTLS_ALERT_DECODE_ERROR;

            dst->off -= 8;
            pthread_rwlock_rdlock(&mgr->rwlock);
            int tag_match = (memcmp(dst->base + dst->off, mgr->quic_tp_tag, 8) == 0);
            pthread_rwlock_unlock(&mgr->rwlock);

            if (!tag_match)
                return PTLS_ERROR_REJECT_EARLY_DATA;  /* Config changed, reject 0-RTT */
        }

        return 0;
    }
}

clj_encrypt_ticket_t *clj_ticket_manager_create_encrypt_ticket(
    clj_ticket_manager_t *mgr,
    int is_quic)
{
    if (!mgr) return NULL;

    clj_encrypt_ticket_t *enc = calloc(1, sizeof(*enc));
    if (!enc) return NULL;

    enc->super.cb = clj_encrypt_ticket_cb;
    enc->mgr = mgr;
    enc->is_quic = is_quic ? 1 : 0;

    return enc;
}

void clj_ptls_ctx_set_tickets(
    ptls_context_t *ctx,
    clj_encrypt_ticket_t *encrypt_ticket,
    uint32_t ticket_lifetime,
    uint32_t max_early_data_size)
{
    if (!ctx) return;

    if (encrypt_ticket) {
        ctx->encrypt_ticket = &encrypt_ticket->super;
    }
    ctx->ticket_lifetime = ticket_lifetime;
    ctx->max_early_data_size = max_early_data_size;

    /* Send 2 NewSessionTicket messages (like Cloudflare).
       This gives client redundancy and curl seems to need 2 before sending early data. */
    ctx->ticket_requests.server.max_count = 2;
}

/* SSL_CTX ticket manager storage for TCP TLS.
   Uses ex_data to store the ticket manager pointer per SSL_CTX. */
static int ssl_ctx_ticket_mgr_index = -1;
static pthread_once_t ssl_ctx_ticket_mgr_once = PTHREAD_ONCE_INIT;

static void init_ssl_ctx_ticket_mgr_index(void) {
    ssl_ctx_ticket_mgr_index = SSL_CTX_get_ex_new_index(0, NULL, NULL, NULL, NULL);
}

/* OpenSSL ticket key callback for TCP TLS - retrieves manager from SSL_CTX ex_data */
static int clj_ssl_ctx_ticket_key_callback(
    SSL *ssl,
    unsigned char *key_name,
    unsigned char *iv,
    EVP_CIPHER_CTX *ctx,
    HMAC_CTX *hctx,
    int enc)
{
    SSL_CTX *ssl_ctx = SSL_get_SSL_CTX(ssl);
    clj_ticket_manager_t *mgr = SSL_CTX_get_ex_data(ssl_ctx, ssl_ctx_ticket_mgr_index);

    if (!mgr) return 0;

    /* Set thread-local for clj_ticket_key_callback */
    tls_current_mgr = mgr;
    int ret = clj_ticket_key_callback(key_name, iv, ctx, hctx, enc);
    tls_current_mgr = NULL;

    return ret;
}

void clj_ssl_ctx_set_tickets(
    SSL_CTX *ctx,
    clj_ticket_manager_t *mgr,
    uint32_t max_early_data_size)
{
    if (!ctx || !mgr) return;

    /* Initialize ex_data index once */
    pthread_once(&ssl_ctx_ticket_mgr_once, init_ssl_ctx_ticket_mgr_index);
    if (ssl_ctx_ticket_mgr_index < 0) {
        fprintf(stderr, "[TICKET] ERROR: failed to get SSL_CTX ex_data index\n");
        return;
    }

    /* Store manager in SSL_CTX ex_data */
    SSL_CTX_set_ex_data(ctx, ssl_ctx_ticket_mgr_index, mgr);

    /* Enable session resumption via stateless tickets (not server-side cache).
       SSL_SESS_CACHE_SERVER enables ticket generation, NO_AUTO_CLEAR prevents
       automatic purging of the internal cache (not needed for stateless). */
    SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_SERVER | SSL_SESS_CACHE_NO_AUTO_CLEAR);

    /* Session ID context identifies this server - required for session resumption.
       Use a fixed value since all listeners share the same ticket keys. */
    static const unsigned char session_id_ctx[] = "busker-h2o";
    SSL_CTX_set_session_id_context(ctx, session_id_ctx, sizeof(session_id_ctx) - 1);

    /* Set session timeout from ticket manager lifetime */
    SSL_CTX_set_timeout(ctx, mgr->ticket_lifetime_seconds);

    /* Set ticket key callback for TLS 1.2 and earlier.
       For TLS 1.3, session tickets use PSK-based resumption but still need this
       callback to encrypt/decrypt the ticket contents. */
    SSL_CTX_set_tlsext_ticket_key_cb(ctx, clj_ssl_ctx_ticket_key_callback);

    /* TLS 1.3: Set number of NewSessionTicket messages to send (default is 2).
       BoringSSL uses this to control ticket issuance. */
    SSL_CTX_set_num_tickets(ctx, 2);

    /* Enable early data (0-RTT) - available in OpenSSL 1.1.1+ / BoringSSL */
#ifdef SSL_CTX_set_max_early_data
    SSL_CTX_set_max_early_data(ctx, max_early_data_size);
#else
    (void)max_early_data_size;  /* Suppress unused warning for older OpenSSL */
#endif
}
