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

#include <h2o/socket.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* 1 if socket has a read callback (i.e. currently reading), else 0 */
int clj_h2o_socket_is_reading(h2o_socket_t *sock);

/* 1 if socket has a write callback (i.e. currently writing), else 0 */
int clj_h2o_socket_is_writing(h2o_socket_t *sock);

/* Optional debug helpers: raw callback pointers (NULL if not set) */
void *clj_h2o_socket_get_read_cb(h2o_socket_t *sock);
void *clj_h2o_socket_get_write_cb(h2o_socket_t *sock);

/* Return size of h2o_globalconf_t for FFI allocation */
size_t clj_h2o_globalconf_size(void);

/* Return size of h2o_context_t for FFI allocation */
size_t clj_h2o_context_size(void);

/* Return size of h2o_accept_ctx_t for FFI allocation */
size_t clj_h2o_accept_ctx_size(void);

/* Get hosts pointer from h2o_globalconf_t */
void *clj_h2o_globalconf_get_hosts(void *globalconf_ptr);

/* Set on_req callback for handler */
void clj_handler_set_on_req(void *handler_ptr, void *callback);

/* Struct size helpers for FFI layout */
size_t clj_h2o_handler_size(void);

/* h2o_req_t field accessors - minimal helpers for struct access */
void clj_h2o_req_set_status(void *req_ptr, int status);
void clj_h2o_req_set_reason(void *req_ptr, const char *reason);
void *clj_h2o_req_get_pool(void *req_ptr);
void *clj_h2o_req_get_res_headers(void *req_ptr);

/* Standard tokens and generator for responses */
void *clj_h2o_get_content_type_token(void);
void *clj_h2o_get_static_generator(void);

#endif /* CLJ_H2O_SHIM_H */
