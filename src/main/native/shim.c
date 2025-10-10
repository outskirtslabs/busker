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
