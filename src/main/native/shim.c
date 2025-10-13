// Minimal exported helpers for libh2o interop
// Intended for FFI use from Clojure (coffi/FFM).
#include "shim.h"

#define REQ_ERROR "request error\n"

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

static void clj_req_meta_print_offsets(void) {
  fprintf(stderr, "clj_req_meta_t field offsets:\n");
  fprintf(stderr, "  sizeof(clj_req_meta_t) = %zu\n", sizeof(clj_req_meta_t));
  fprintf(stderr, "  authority = %zu\n", offsetof(clj_req_meta_t, authority));
  fprintf(stderr, "  charset = %zu\n", offsetof(clj_req_meta_t, charset));
  fprintf(stderr, "  method = %zu\n", offsetof(clj_req_meta_t, method));
  fprintf(stderr, "  path = %zu\n", offsetof(clj_req_meta_t, path));
  fprintf(stderr, "  remote_addr = %zu\n",
          offsetof(clj_req_meta_t, remote_addr));
  fprintf(stderr, "  scheme = %zu\n", offsetof(clj_req_meta_t, scheme));
  fprintf(stderr, "  headers = %zu\n", offsetof(clj_req_meta_t, headers));
  fprintf(stderr, "  authority_len = %zu\n",
          offsetof(clj_req_meta_t, authority_len));
  fprintf(stderr, "  charset_len = %zu\n",
          offsetof(clj_req_meta_t, charset_len));
  fprintf(stderr, "  method_len = %zu\n", offsetof(clj_req_meta_t, method_len));
  fprintf(stderr, "  path_len = %zu\n", offsetof(clj_req_meta_t, path_len));
  fprintf(stderr, "  remote_addr_len = %zu\n",
          offsetof(clj_req_meta_t, remote_addr_len));
  fprintf(stderr, "  scheme_len = %zu\n", offsetof(clj_req_meta_t, scheme_len));
  fprintf(stderr, "  headers_len = %zu\n",
          offsetof(clj_req_meta_t, headers_len));
  fprintf(stderr, "  http_version = %zu\n",
          offsetof(clj_req_meta_t, http_version));
  fprintf(stderr, "  has_body = %zu\n", offsetof(clj_req_meta_t, has_body));
}
static void clj_req_ctx_print_offsets(void) {
  fprintf(stderr, "clj_req_ctx_t field offsets:\n");
  fprintf(stderr, "  sizeof(clj_req_ctx_t) = %zu\n", sizeof(clj_req_ctx_t));
  fprintf(stderr, "  req = %zu\n", offsetof(clj_req_ctx_t, req));
  fprintf(stderr, "  meta = %zu\n", offsetof(clj_req_ctx_t, meta));
  fprintf(stderr, "  cleanup = %zu\n", offsetof(clj_req_ctx_t, cleanup));
  fprintf(stderr, "  on_cleanup = %zu\n", offsetof(clj_req_ctx_t, on_cleanup));
  fprintf(stderr, "  generator = %zu\n", offsetof(clj_req_ctx_t, generator));

  fprintf(stderr, "  generator = %zu\n", offsetof(clj_req_ctx_t, generator));
}

static void clj_h2o_extract_req_meta(h2o_req_t *req, clj_req_meta_t *meta) {

  clj_req_ctx_print_offsets();
  clj_req_meta_print_offsets();
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

void clj_stream_start_response(h2o_req_t *req, int status,
                               const clj_header_t *headers,
                               uint32_t headers_len, size_t content_length,
                               const clj_generator_callbacks_t *generator_cb) {

  req->res.status = status;
  req->res.reason = "OK";

  if (content_length != SIZE_MAX) {
    req->res.content_length = content_length;
  }

  for (uint32_t i = 0; i < headers_len; i++) {
    const uint8_t *name_data = headers[i].name;
    const uint8_t *value_data = headers[i].value;
    uint32_t name_len = headers[i].name_len;
    uint32_t value_len = headers[i].value_len;

    /* TODO: do we need to allocate mem for headers again? */
    char *pool_name = h2o_mem_alloc_pool(&req->pool, char, name_len + 1);
    char *pool_value = h2o_mem_alloc_pool(&req->pool, char, value_len + 1);

    /* Copy header data into h2o pool memory */
    memcpy(pool_name, name_data, name_len);
    pool_name[name_len] = '\0'; /* null terminate */

    memcpy(pool_value, value_data, value_len);
    pool_value[value_len] = '\0'; /* null terminate */

    h2o_add_header_by_str(&req->pool, &req->res.headers, pool_name, name_len, 0,
                          pool_name, pool_value, value_len);
  }

  /* TODO: GENREATOR?*/
}

/* Completion cleanup callback - this is called by h2o when our request dies
   such as when the client disconnects abruptly
   We use this to signal to java that this req is no longer alive
   ref: https://github.com/h2o/h2o/issues/1894#issuecomment-437231273
   */
static void cleanup_request(void *ptr) {
  clj_req_ctx_t *const ctx = *(clj_req_ctx_t **)ptr;
  ctx->cleanup = 1;
  ctx->on_cleanup(ctx);
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

static int on_request(h2o_handler_t *self, h2o_req_t *req) {
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
    ctx->cleanup = 0;
    ctx->on_request_body_chunk = 0;
    clj_h2o_extract_req_meta(req, &ctx->meta);
    handler->on_cleanup = handler->on_cleanup;

    // upcall to the jvm's on-request-callback
    int ret = handler->on_req(ctx);
    if (ret == CLJ_HANDLER_OVERLOADED) {
      h2o_send_error_503(req, "Service Unavailable", "Server overloaded", 0);
      return 0;
    } else if (ret == CLJ_HANDLER_DECLINED) {
      return -1;
    }
    if (ctx->meta.has_body) {

      if (req->entity.base != NULL) {
        if (ctx->on_request_body_chunk)
          ctx->on_request_body_chunk(ctx, req->entity.base, req->entity.len, 1);
      } else if (req->proceed_req != NULL) {
        /* Set up our body write callback to receive chunks */
        req->write_req.cb = clj_body_write_callback;
        req->write_req.ctx = ctx;
        /* Start receiving body chunks - this will trigger
         * clj_body_write_callback when the first chunk arrives */
        req->proceed_req(req, NULL);
      }
    }
  } else {
    send_error(INTERNAL_SERVER_ERROR, REQ_ERROR, req);
  }
  return 0;
}

static void on_context_init(h2o_handler_t *_self, h2o_context_t *ctx) {
  struct clj_h2o_handler_t *self = (void *)_self;
}
static void on_context_dispose(h2o_handler_t *_self, h2o_context_t *ctx) {
  struct clj_h2o_handler_t *self = (void *)_self;
}
static void on_handler_dispose(h2o_handler_t *_self) {
  struct clj_h2o_handler_t *self = (void *)_self;
}

clj_h2o_handler_t *
clj_h2o_create_handler(h2o_hostconf_t *hostconf,
                       int (*on_req_callback)(clj_req_ctx_t *),
                       void (*on_cleanup_callback)(clj_req_ctx_t *),
                       int supports_request_streaming, int handles_expect) {

  h2o_pathconf_t *pathconf = h2o_config_register_path(hostconf, "/", 0);
  clj_h2o_handler_t *handler = (clj_h2o_handler_t *)h2o_create_handler(
      pathconf, sizeof(clj_h2o_handler_t));
  handler->on_req = on_req_callback;
  handler->on_cleanup = on_cleanup_callback;
  handler->shutting_down = 0;
  handler->super.on_req = on_request;
  handler->super.supports_request_streaming = 1;
  handler->super.on_context_init = on_context_init;
  handler->super.on_context_dispose = on_context_dispose;
  handler->super.dispose = on_handler_dispose;
  handler->super.supports_request_streaming =
      supports_request_streaming ? 1 : 0;
  handler->super.handles_expect = handles_expect ? 1 : 0;
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
