// Minimal exported helpers for libh2o interop
// Intended for FFI use from Clojure (coffi/FFM).
#include "shim.h"
#include <h2o.h>

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

void *clj_h2o_globalconf_get_hosts(void *globalconf_ptr) {
  h2o_globalconf_t *conf = (h2o_globalconf_t *)globalconf_ptr;
  return conf->hosts;
}

void clj_handler_set_on_req(void *handler_ptr, void *callback) {
  h2o_handler_t *handler = (h2o_handler_t *)handler_ptr;
  handler->on_req = (int (*)(h2o_handler_t *, h2o_req_t *))callback;
}

size_t clj_h2o_handler_size(void) { return sizeof(h2o_handler_t); }

void clj_h2o_req_set_status(void *req_ptr, int status) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  req->res.status = status;
}

void clj_h2o_req_set_reason(void *req_ptr, const char *reason) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  req->res.reason = reason;
}

void clj_h2o_req_set_content_length(void *req_ptr, size_t content_length) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  req->res.content_length = content_length;
}

void *clj_h2o_req_get_pool(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return &req->pool;
}

void *clj_h2o_req_get_res_headers(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return &req->res.headers;
}

void *clj_h2o_get_content_type_token(void) { return H2O_TOKEN_CONTENT_TYPE; }

static h2o_generator_t static_generator = {NULL, NULL};

void *clj_h2o_get_static_generator(void) { return &static_generator; }

void *clj_h2o_req_get_method(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return &req->method;
}

void *clj_h2o_req_get_path(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return &req->path;
}

void *clj_h2o_req_get_authority(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return &req->authority;
}

void *clj_h2o_req_get_query_at(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return &req->query_at;
}

void *clj_h2o_req_get_scheme(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return req->scheme ? (void *)&req->scheme->name : NULL;
}

void *clj_h2o_req_get_entity(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return &req->entity;
}

void *clj_h2o_req_get_headers(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return req->headers.entries;
}

uint32_t clj_h2o_req_get_headers_size(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return (uint32_t)req->headers.size;
}

uint16_t clj_h2o_req_get_version(void *req_ptr) {
  h2o_req_t *req = (h2o_req_t *)req_ptr;
  return req->version;
}

uint64_t clj_h2o_evloop_now(void *loop_ptr) {
  h2o_evloop_t *loop = (h2o_evloop_t *)loop_ptr;
  return loop->_now_millisec;
}

size_t clj_h2o_context_get_active_conns(void *ctx_ptr) {
  h2o_context_t *ctx = (h2o_context_t *)ctx_ptr;
  return ctx->_conns.num_conns.active;
}

size_t clj_h2o_context_get_idle_conns(void *ctx_ptr) {
  h2o_context_t *ctx = (h2o_context_t *)ctx_ptr;
  return ctx->_conns.num_conns.idle;
}

size_t clj_h2o_context_get_shutdown_conns(void *ctx_ptr) {
  h2o_context_t *ctx = (h2o_context_t *)ctx_ptr;
  return ctx->_conns.num_conns.shutdown;
}

/* Debug helper: print h2o_req_t field offsets for struct layout verification */
void clj_h2o_req_print_offsets(void) {
  fprintf(stderr, "h2o_req_t field offsets:\n");
  fprintf(stderr, "  sizeof(h2o_req_t) = %zu\n", sizeof(h2o_req_t));
  fprintf(stderr, "  conn = %zu\n", offsetof(h2o_req_t, conn));
  fprintf(stderr, "  input.scheme = %zu\n", offsetof(h2o_req_t, input.scheme));
  fprintf(stderr, "  input.authority = %zu\n", offsetof(h2o_req_t, input.authority));
  fprintf(stderr, "  input.method = %zu\n", offsetof(h2o_req_t, input.method));
  fprintf(stderr, "  input.path = %zu\n", offsetof(h2o_req_t, input.path));
  fprintf(stderr, "  input.query_at = %zu\n", offsetof(h2o_req_t, input.query_at));
  fprintf(stderr, "  hostconf = %zu\n", offsetof(h2o_req_t, hostconf));
  fprintf(stderr, "  pathconf = %zu\n", offsetof(h2o_req_t, pathconf));
  fprintf(stderr, "  scheme = %zu\n", offsetof(h2o_req_t, scheme));
  fprintf(stderr, "  authority = %zu\n", offsetof(h2o_req_t, authority));
  fprintf(stderr, "  method = %zu\n", offsetof(h2o_req_t, method));
  fprintf(stderr, "  path = %zu\n", offsetof(h2o_req_t, path));
  fprintf(stderr, "  query_at = %zu\n", offsetof(h2o_req_t, query_at));
  fprintf(stderr, "  version = %zu\n", offsetof(h2o_req_t, version));
  fprintf(stderr, "  headers = %zu\n", offsetof(h2o_req_t, headers));
  fprintf(stderr, "  entity = %zu\n", offsetof(h2o_req_t, entity));
  fprintf(stderr, "  content_length = %zu\n", offsetof(h2o_req_t, content_length));
  fprintf(stderr, "  res = %zu\n", offsetof(h2o_req_t, res));
  fprintf(stderr, "  res.status = %zu\n", offsetof(h2o_req_t, res.status));
  fprintf(stderr, "  res.reason = %zu\n", offsetof(h2o_req_t, res.reason));
  fprintf(stderr, "  res.content_length = %zu\n", offsetof(h2o_req_t, res.content_length));
  fprintf(stderr, "  res.headers = %zu\n", offsetof(h2o_req_t, res.headers));
  fprintf(stderr, "  pool = %zu\n", offsetof(h2o_req_t, pool));
  fprintf(stderr, "\n");
  fprintf(stderr, "h2o_res_t size and field offsets:\n");
  fprintf(stderr, "  sizeof(h2o_res_t) = %zu\n", sizeof(h2o_res_t));
  fprintf(stderr, "  status = %zu\n", offsetof(h2o_res_t, status));
  fprintf(stderr, "  reason = %zu\n", offsetof(h2o_res_t, reason));
  fprintf(stderr, "  content_length = %zu\n", offsetof(h2o_res_t, content_length));
  fprintf(stderr, "  headers = %zu\n", offsetof(h2o_res_t, headers));
  fprintf(stderr, "\n");
  fprintf(stderr, "h2o_headers_t size and field offsets:\n");
  fprintf(stderr, "  sizeof(h2o_headers_t) = %zu\n", sizeof(h2o_headers_t));
  fprintf(stderr, "  entries = %zu\n", offsetof(h2o_headers_t, entries));
  fprintf(stderr, "  size = %zu\n", offsetof(h2o_headers_t, size));
  fprintf(stderr, "  capacity = %zu\n", offsetof(h2o_headers_t, capacity));
}

typedef struct clj_streaming_generator_t {
  h2o_generator_t generator;
  void (*on_proceed_upcall)(void*);
  void (*on_stop_upcall)(void*, int);
  void* jvm_handle;
} clj_streaming_generator_t;

static void clj_streaming_proceed(h2o_generator_t* self, h2o_req_t* req) {
  (void)req;
  clj_streaming_generator_t* gen = (clj_streaming_generator_t*)self;
  if (gen->on_proceed_upcall) {
    gen->on_proceed_upcall(gen->jvm_handle);
  }
}

static void clj_streaming_stop(h2o_generator_t* self, h2o_req_t* req) {
  (void)req;
  clj_streaming_generator_t* gen = (clj_streaming_generator_t*)self;
  if (gen->on_stop_upcall) {
    gen->on_stop_upcall(gen->jvm_handle, 1);
  }
}

void* clj_create_streaming_generator(
    void* req_ptr,
    void* on_proceed_callback,
    void* on_stop_callback,
    void* jvm_handle) {

  h2o_req_t* req = (h2o_req_t*)req_ptr;
  clj_streaming_generator_t* gen =
      h2o_mem_alloc_shared(&req->pool, sizeof(clj_streaming_generator_t), NULL);

  gen->generator.proceed = clj_streaming_proceed;
  gen->generator.stop = clj_streaming_stop;
  gen->on_proceed_upcall = (void (*)(void*))on_proceed_callback;
  gen->on_stop_upcall = (void (*)(void*, int))on_stop_callback;
  gen->jvm_handle = jvm_handle;

  return &gen->generator;
}
