// Minimal exported helpers for libh2o interop
// Intended for FFI use from Clojure (coffi/FFM).
#include "shim.h"
#include "h2o/multithread.h"

#define REQ_ERROR "request error\n"

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

size_t clj_h2o_globalconf_size(void) { return sizeof(h2o_globalconf_t); }

size_t clj_h2o_context_size(void) { return sizeof(h2o_context_t); }

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
  clj_req_ctx_t *ctx = H2O_STRUCT_FROM_MEMBER(clj_req_ctx_t, generator, gen);

  /* Defensive guard: drop callback if slot is not in use */
  if (!ctx) {
    return;
  }

  if (ctx->on_response_generator_stop) {
    ctx->on_response_generator_stop(ctx, CLJ_COMPLETE_RESET);
  }

  /* Complete deferred deallocation if closing flag is set */
  if (ctx->closing) {
    //??? TODO
  }
}

size_t clj_h2o_start_response(
    clj_req_ctx_t *ctx, int status, const clj_header_t *headers,
    size_t headers_len, size_t content_length,
    void (*on_response_generator_proceed)(clj_req_ctx_t *ctx),
    void (*on_response_generator_stop)(clj_req_ctx_t *ctx,
                                       clj_complete_reason_t reason)) {
  if (!ctx || !ctx->req)
    return 0;

  h2o_req_t *req = ctx->req;

  req->res.status = status;
  req->res.reason = "OK";

  req->res.content_length = SIZE_MAX;
  if (content_length != SIZE_MAX) {
    req->res.content_length = content_length;
  }

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

    // DEBUG_LOG("copied header %s (%d) = %s (%d)", pool_name, name_len,
    //           pool_value, value_len);

    h2o_add_header_by_str(&req->pool, &req->res.headers, pool_name, name_len, 0,
                          pool_name, pool_value, value_len);
  }

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
      headers[i].name = (const uint8_t *)h->name->base;
      headers[i].name_len = h->name->len;
      headers[i].value = (const uint8_t *)h->value.base;
      headers[i].value_len = h->value.len;
    }
    meta->headers = headers;
    meta->headers_len = req->headers.size;
  } else {
    meta->headers = NULL;
    meta->headers_len = 0;
  }

  meta->has_body = (req->entity.base != NULL && req->entity.len > 0) ? 1 : 0;

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

  /* Extract charset from content-type if present */
  ssize_t ct_idx = h2o_find_header(&req->headers, H2O_TOKEN_CONTENT_TYPE, -1);
  if (ct_idx != -1) {
    h2o_iovec_t ct = req->headers.entries[ct_idx].value;
    const char *charset_marker = "; charset=";
    size_t charset_marker_len = strlen(charset_marker);
    size_t offset =
        h2o_strstr(ct.base, ct.len, charset_marker, charset_marker_len);
    if (offset != SIZE_MAX) {
      const char *charset_start = ct.base + offset + charset_marker_len;
      size_t charset_len = ct.len - offset - charset_marker_len;
      char *cs = h2o_mem_alloc_pool(&req->pool, char, charset_len + 1);
      memcpy(cs, charset_start, charset_len);
      cs[charset_len] = '\0';
      meta->charset = (const uint8_t *)cs;
      meta->charset_len = (int32_t)charset_len;
    } else {
      meta->charset = NULL;
      meta->charset_len = 0;
    }
  } else {
    meta->charset = NULL;
    meta->charset_len = 0;
  }
}

/* Completion cleanup callback - this is called by h2o when our request dies
   such as when the client disconnects abruptly
   We use this to signal to java that this req is no longer alive
   ref: https://github.com/h2o/h2o/issues/1894#issuecomment-437231273
   */
static void cleanup_request(void *ptr) {
  clj_req_ctx_t *const ctx = *(clj_req_ctx_t **)ptr;
  if (!ctx)
    return;
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
  /* Cast self back to our custom handler type to access the server pointer */
  clj_h2o_handler_t *handler = (clj_h2o_handler_t *)self;

  if (handler->shutting_down) {
    h2o_send_error_503(req, "Service Unavailable", "Server is shutting down",
                       H2O_SEND_ERROR_HTTP1_CLOSE_CONNECTION);
    return 0;
  }
  clj_req_ctx_t *const ctx = calloc(1, sizeof(*ctx));
  if (ctx) {
    clj_req_ctx_t **const p =
        h2o_mem_alloc_shared(&req->pool, sizeof(*p), cleanup_request);
    *p = ctx;
    ctx->req = req;
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
  } else {
    send_error(INTERNAL_SERVER_ERROR, REQ_ERROR, req);
  }
  return 0;
}

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

clj_h2o_handler_t *
clj_h2o_create_handler(h2o_hostconf_t *hostconf,
                       int (*on_request)(clj_req_ctx_t *),
                       void (*on_request_cleanup)(clj_req_ctx_t *),
                       int supports_request_streaming, int handles_expect) {

  h2o_pathconf_t *pathconf = h2o_config_register_path(hostconf, "/", 0);
  clj_h2o_handler_t *handler = (clj_h2o_handler_t *)h2o_create_handler(
      pathconf, sizeof(clj_h2o_handler_t));
  handler->on_request = on_request;
  handler->on_request_cleanup = on_request_cleanup;
  handler->shutting_down = 0;
  handler->super.on_req = request_handler;
  handler->super.supports_request_streaming =
      supports_request_streaming ? 1 : 0;
  // handler->super.on_context_init = on_context_init;
  // handler->super.on_context_dispose = on_context_dispose;
  // handler->super.dispose = on_handler_dispose;
  // handler->super.handles_expect = handles_expect ? 1 : 0;
  return handler;
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
