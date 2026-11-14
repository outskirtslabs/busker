// Minimal exported helpers for libh2o interop
// Called from Clojure through babashka/ffi and the JVM FFM API.
#ifdef __APPLE__
#define __APPLE_USE_RFC_3542 /* to use IPV6_RECVPKTINFO / IPV6_PKTINFO */
#endif

#include "shim.h"
#include "h2o/multithread.h"
#include "h2o/http3_server.h"
#include "picotls.h"
#include "picotls/openssl.h"
#include "quicly.h"
#include "quicly/defaults.h"
#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <openssl/err.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

/* Fallback for platforms without explicit_bzero */
#if !defined(__GLIBC__) && !defined(__FreeBSD__) && !defined(__OpenBSD__)
#include <openssl/crypto.h>
#define explicit_bzero(s, n) OPENSSL_cleanse(s, n)
#endif

_Static_assert(sizeof(clj_req_ctx_t) == 248, "FFM request context size");
_Static_assert(offsetof(clj_req_meta_t, entrypoint) == 104,
               "FFM request entrypoint offset");
_Static_assert(offsetof(clj_req_meta_t, connection_id) == 112,
               "FFM request connection id offset");
_Static_assert(offsetof(clj_req_ctx_t, fixed_slot) == 160, "FFM fixed slot offset");
_Static_assert(offsetof(clj_req_ctx_t, fixed_claim) == 168, "FFM fixed claim offset");
_Static_assert(offsetof(clj_req_ctx_t, generator) == 176, "FFM generator offset");
_Static_assert(offsetof(clj_req_ctx_t, on_response_generator_proceed) == 192,
               "FFM proceed callback offset");
_Static_assert(offsetof(clj_req_ctx_t, on_response_generator_stop) == 200,
               "FFM stop callback offset");

/* Global connection limit counter and maximum.
   Process-global, shared across all workers and listeners in the same JVM.
   TODO: refactor connection limit accounting to be per-server rather than
   process-global so multiple embedded servers can coexist safely. */
static _Atomic uint32_t clj_conn_count = 0;
static _Atomic uint32_t clj_conn_max = 0;


typedef enum {
  CLJ_RESPONSE_SLOT_FREE = 0,
  CLJ_RESPONSE_SLOT_CLAIMED = 1,
  CLJ_RESPONSE_SLOT_READY = 2,
  CLJ_RESPONSE_SLOT_IN_NATIVE = 3
} clj_response_slot_state_t;

#define CLJ_RESPONSE_HANDLE_INDEX_MASK UINT64_C(0xffff)
#define CLJ_RESPONSE_HANDLE_SEQUENCE_MASK UINT64_C(0xffffffffffff)

struct clj_response_slot_t {
  _Atomic uint32_t state;
  _Atomic uint64_t claim_token;
  clj_fixed_response_slot_data_t data;
};

struct clj_mt_receiver_t {
  h2o_multithread_receiver_t receiver;
  h2o_multithread_queue_t *queue;
  clj_req_ctx_t **request_buckets;
  size_t request_capacity;
  size_t request_size;
  _Atomic size_t ring_claimed;
  _Atomic size_t ring_ready;
  _Atomic size_t ring_retained;
  _Atomic size_t producer_uses;
  _Atomic size_t claim_cursor;
  _Atomic uint64_t claim_sequence;
  unsigned char *ring_storage;
  _Atomic uint64_t *ready_words;
  size_t ready_word_count;
  size_t ring_capacity;
  size_t ring_stride;
  size_t slot_payload_capacity;
  _Atomic bool accepts_responses;
};

typedef struct {
  clj_tls_lookup_cb cb;
  void *user_ctx;
} clj_tls_lookup_binding_t;

void *clj_h2o_tls_memdup(const void *value, size_t value_len) {
  if (value == NULL || value_len == 0)
    return NULL;

  uint8_t *dup = malloc(value_len);
  if (dup == NULL)
    return NULL;

  memcpy(dup, value, value_len);
  return dup;
}

static int clj_set_fd_cloexec(int fd) {
  int flags = fcntl(fd, F_GETFD, 0);
  if (flags == -1)
    return -1;
  return fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
}

static int clj_set_fd_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return -1;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int clj_h2o_open_tcp_listener(const char *host, uint16_t port, int backlog,
                              int reuseaddr, int reuseport, int nonblock,
                              int cloexec) {
  char servname[6];
  struct addrinfo hints, *res = NULL, *ai = NULL;
  const char *lookup_host = host != NULL && host[0] != '\0' ? host : NULL;
  int gai_err, fd = -1, saved_errno = EADDRNOTAVAIL;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_NUMERICSERV | AI_PASSIVE;

  snprintf(servname, sizeof(servname), "%u", (unsigned)port);
  gai_err = getaddrinfo(lookup_host, servname, &hints, &res);
  if (gai_err != 0) {
    if (gai_err != EAI_SYSTEM)
      errno = EINVAL;
    return -1;
  }

  for (ai = res; ai != NULL; ai = ai->ai_next) {
    fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd == -1) {
      saved_errno = errno;
      continue;
    }

    if (cloexec && clj_set_fd_cloexec(fd) != 0)
      goto Next;
    if (nonblock && clj_set_fd_nonblocking(fd) != 0)
      goto Next;

#ifdef IPV6_V6ONLY
    if (ai->ai_family == AF_INET6) {
      int flag = 1;
      if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &flag, sizeof(flag)) != 0)
        goto Next;
    }
#endif

    if (reuseaddr) {
      int on = 1;
      if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0)
        goto Next;
    }

#ifdef SO_REUSEPORT
    if (reuseport) {
      int on = 1;
      if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on)) != 0)
        goto Next;
    }
#endif

    if (bind(fd, ai->ai_addr, ai->ai_addrlen) != 0)
      goto Next;

    if (listen(fd, backlog > 0 ? backlog : SOMAXCONN) != 0)
      goto Next;

    freeaddrinfo(res);
    return fd;

  Next:
    saved_errno = errno;
    close(fd);
    fd = -1;
  }

  freeaddrinfo(res);
  errno = saved_errno;
  return -1;
}

int clj_h2o_open_unix_listener(const char *path, int backlog, int nonblock,
                               int cloexec) {
  struct sockaddr_un addr;
  socklen_t addrlen = 0;
  int fd = -1;
  int cleanup_path = 0;

  if (path == NULL || path[0] == '\0') {
    errno = EINVAL;
    return -1;
  }

  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;

  if (path[0] == '@') {
#if defined(__linux__)
    size_t name_len = strlen(path + 1);
    if (name_len == 0 || name_len > sizeof(addr.sun_path) - 1) {
      errno = ENAMETOOLONG;
      return -1;
    }
    memcpy(addr.sun_path + 1, path + 1, name_len);
    addrlen = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + 1 +
                          name_len);
#else
    errno = EAFNOSUPPORT;
    return -1;
#endif
  } else {
    struct stat st;
    size_t path_len = strlen(path);
    if (path_len >= sizeof(addr.sun_path)) {
      errno = ENAMETOOLONG;
      return -1;
    }

    if (lstat(path, &st) == 0) {
      if (S_ISSOCK(st.st_mode)) {
        unlink(path);
      } else {
        errno = EADDRINUSE;
        return -1;
      }
    }

    memcpy(addr.sun_path, path, path_len + 1);
    addrlen =
        (socklen_t)(offsetof(struct sockaddr_un, sun_path) + path_len + 1);
    cleanup_path = 1;
  }

  fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd == -1)
    return -1;

  if (cloexec && clj_set_fd_cloexec(fd) != 0)
    goto Error;
  if (nonblock && clj_set_fd_nonblocking(fd) != 0)
    goto Error;
  if (bind(fd, (struct sockaddr *)&addr, addrlen) != 0)
    goto Error;
  if (listen(fd, backlog > 0 ? backlog : SOMAXCONN) != 0)
    goto Error;

  return fd;

Error:
  {
    int saved_errno = errno;
    close(fd);
    if (cleanup_path)
      unlink(path);
    errno = saved_errno;
    return -1;
  }
}

int clj_h2o_unlink_unix_socket_if_still_socket(const char *path) {
  struct stat st;

  if (path == NULL || path[0] == '\0') {
    errno = EINVAL;
    return -1;
  }

  if (path[0] == '@')
    return 0;

  if (lstat(path, &st) != 0) {
    if (errno == ENOENT)
      return 0;
    return -1;
  }

  if (!S_ISSOCK(st.st_mode))
    return 0;

  if (unlink(path) != 0)
    return -1;

  return 1;
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

size_t clj_h2o_req_ctx_size(void) { return sizeof(clj_req_ctx_t); }

size_t clj_h2o_globalconf_size(void) { return sizeof(h2o_globalconf_t); }

size_t clj_h2o_accept_ctx_size(void) { return sizeof(h2o_accept_ctx_t); }

size_t clj_h2o_entrypoint_hosts_size(void) {
  return sizeof(clj_entrypoint_hosts_t);
}

h2o_hostconf_t **clj_h2o_entrypoint_hosts_init(clj_entrypoint_hosts_t *record,
                                               h2o_globalconf_t *globalconf,
                                               uint32_t entrypoint) {
  if (record == NULL || globalconf == NULL ||
      entrypoint == CLJ_ENTRYPOINT_UNKNOWN || globalconf->hosts[0] == NULL ||
      globalconf->hosts[1] != NULL)
    return NULL;
  record->hosts[0] = globalconf->hosts[0];
  record->hosts[1] = NULL;
  record->self = record;
  record->entrypoint = entrypoint;
  return record->hosts;
}

/* Returns the entrypoint index of the listener that accepted the request's
 * connection, from the hosts array H2O kept for the connection. */
static uint32_t clj_request_entrypoint(h2o_req_t *req) {
  h2o_hostconf_t **hosts = req->conn->hosts;
  if (hosts == NULL || hosts == req->conn->ctx->globalconf->hosts)
    return CLJ_ENTRYPOINT_UNKNOWN;
  clj_entrypoint_hosts_t *record =
      H2O_STRUCT_FROM_MEMBER(clj_entrypoint_hosts_t, hosts, hosts);
  return record->self == record ? record->entrypoint : CLJ_ENTRYPOINT_UNKNOWN;
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
  (void)req;
  if (gen == NULL)
    return;

  clj_req_ctx_t *ctx = H2O_STRUCT_FROM_MEMBER(clj_req_ctx_t, generator, gen);
  if (ctx != NULL && ctx->dispatch_module_id != 0 &&
      ctx->dispatch_request_seq != 0 &&
      ctx->on_response_generator_proceed != NULL)
    ctx->on_response_generator_proceed(ctx->dispatch_module_id,
                                       ctx->dispatch_request_seq);
}

static void clj_generator_stop(h2o_generator_t *gen, h2o_req_t *req) {
  (void)req;
  if (gen == NULL)
    return;

  clj_req_ctx_t *ctx = H2O_STRUCT_FROM_MEMBER(clj_req_ctx_t, generator, gen);
  if (ctx != NULL && ctx->dispatch_module_id != 0 &&
      ctx->dispatch_request_seq != 0 &&
      ctx->on_response_generator_stop != NULL)
    ctx->on_response_generator_stop(ctx->dispatch_module_id,
                                    ctx->dispatch_request_seq,
                                    CLJ_COMPLETE_RESET);
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

    const h2o_token_t *token = h2o_lookup_token(name_data, name_len);
    char *pool_value = h2o_mem_alloc_pool(&req->pool, char, value_len + 1);

    memcpy(pool_value, value_data, value_len);
    pool_value[value_len] = '\0';

    if (token != NULL) {
      h2o_add_header(&req->pool, &req->res.headers, token, NULL, pool_value,
                     value_len);
    } else {
      char *pool_name = h2o_mem_alloc_pool(&req->pool, char, name_len + 1);
      memcpy(pool_name, name_data, name_len);
      pool_name[name_len] = '\0';
      h2o_add_header_by_str(&req->pool, &req->res.headers, pool_name, name_len,
                            0, pool_name, pool_value, value_len);
    }
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
    clj_response_generator_proceed_cb on_response_generator_proceed,
    clj_response_generator_stop_cb on_response_generator_stop) {

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

  req->compress_hint = compress_hint;
  ctx->generator.proceed = clj_generator_proceed;
  ctx->generator.stop = clj_generator_stop;
  ctx->on_response_generator_proceed = on_response_generator_proceed;
  ctx->on_response_generator_stop = on_response_generator_stop;
  ctx->response_started = 1;
  h2o_start_response(req, &ctx->generator);
  return req->preferred_chunk_size;
}

static int clj_h2o_req_has_body(h2o_req_t *req) {
  if (!req)
    return 0;

  /* H2O exposes request body state through proceed_req for streaming bodies
     and entity.base for complete buffered bodies, including zero-length bodies. */
  return (req->proceed_req != NULL || req->entity.base != NULL) ? 1 : 0;
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

  meta->has_body = clj_h2o_req_has_body(req);
  meta->is_early_data = h2o_conn_is_early_data(req->conn) ? 1 : 0;
  meta->entrypoint = clj_request_entrypoint(req);
  meta->connection_id = req->conn->id;
#if CLJ_DEBUG
  assert(meta->entrypoint != CLJ_ENTRYPOINT_UNKNOWN);
#endif

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
    socklen_t salen =
        req->conn->callbacks->get_peername(req->conn, (struct sockaddr *)&sa);
    if (salen != 0) {
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

static size_t response_request_bucket(size_t capacity, uint64_t request_seq) {
  uint64_t hash = request_seq;
  hash ^= hash >> 33;
  hash *= UINT64_C(0xff51afd7ed558ccd);
  hash ^= hash >> 33;
  hash *= UINT64_C(0xc4ceb9fe1a85ec53);
  hash ^= hash >> 33;
  return (size_t)hash & (capacity - 1);
}

static clj_req_ctx_t *find_response_request(clj_mt_receiver_t *receiver,
                                            uint64_t request_seq) {
  if (receiver->request_buckets == NULL || request_seq == 0)
    return NULL;

  size_t bucket =
      response_request_bucket(receiver->request_capacity, request_seq);
  for (clj_req_ctx_t *ctx = receiver->request_buckets[bucket]; ctx != NULL;
       ctx = ctx->response_hash_next) {
    if (ctx->dispatch_request_seq == request_seq)
      return ctx;
  }
  return NULL;
}

static bool resize_response_requests(clj_mt_receiver_t *receiver,
                                     size_t capacity) {
  if (capacity == 0 || capacity > SIZE_MAX / sizeof(*receiver->request_buckets))
    return false;

  clj_req_ctx_t **buckets = calloc(capacity, sizeof(*buckets));
  if (buckets == NULL)
    return false;

  for (size_t index = 0; index < receiver->request_capacity; ++index) {
    clj_req_ctx_t *ctx = receiver->request_buckets[index];
    while (ctx != NULL) {
      clj_req_ctx_t *next = ctx->response_hash_next;
      size_t bucket =
          response_request_bucket(capacity, ctx->dispatch_request_seq);
      ctx->response_hash_next = buckets[bucket];
      buckets[bucket] = ctx;
      ctx = next;
    }
  }

  free(receiver->request_buckets);
  receiver->request_buckets = buckets;
  receiver->request_capacity = capacity;
  return true;
}

static bool register_response_request(clj_mt_receiver_t *receiver,
                                      clj_req_ctx_t *ctx,
                                      uint64_t request_seq) {
  if (find_response_request(receiver, request_seq) != NULL)
    return false;

  if (receiver->request_size + 1 >
      receiver->request_capacity - receiver->request_capacity / 4) {
    if (receiver->request_capacity > SIZE_MAX / 2 ||
        !resize_response_requests(receiver, receiver->request_capacity * 2))
      return false;
  }

  size_t bucket =
      response_request_bucket(receiver->request_capacity, request_seq);
  ctx->response_hash_next = receiver->request_buckets[bucket];
  receiver->request_buckets[bucket] = ctx;
  ++receiver->request_size;
  return true;
}

static void unregister_response_request(clj_req_ctx_t *ctx, uint64_t request_seq) {
  clj_mt_receiver_t *receiver = ctx->response_receiver;
  if (receiver == NULL || receiver->request_buckets == NULL || request_seq == 0)
    return;

  size_t bucket =
      response_request_bucket(receiver->request_capacity, request_seq);
  clj_req_ctx_t **entry = &receiver->request_buckets[bucket];
  while (*entry != NULL) {
    if (*entry == ctx) {
      *entry = ctx->response_hash_next;
      ctx->response_hash_next = NULL;
      --receiver->request_size;
      return;
    }
    entry = &(*entry)->response_hash_next;
  }
}

static void release_fixed_slot(clj_req_ctx_t *ctx) {
  clj_response_slot_t *slot = ctx->fixed_slot;
  uint64_t claim = ctx->fixed_claim;
  clj_mt_receiver_t *receiver = ctx->response_receiver;
  ctx->fixed_slot = NULL;
  ctx->fixed_claim = 0;
  if (slot == NULL)
    return;

  if (receiver == NULL ||
      atomic_load_explicit(&slot->claim_token, memory_order_acquire) != claim ||
      atomic_load_explicit(&slot->state, memory_order_acquire) !=
          CLJ_RESPONSE_SLOT_IN_NATIVE)
    h2o_fatal("invalid retained response slot claim");

  atomic_fetch_sub_explicit(&receiver->ring_retained, 1, memory_order_release);
  atomic_store_explicit(&slot->state, CLJ_RESPONSE_SLOT_FREE,
                        memory_order_release);
}

static void cleanup_request(void *ptr) {
  if (ptr == NULL)
    return;

  clj_req_ctx_t *const ctx = (clj_req_ctx_t *)ptr;
  if (ctx->cleanup)
    return;
  clj_request_cleanup_cb cleanup = ctx->on_request_cleanup;
  uint64_t module_id = ctx->dispatch_module_id;
  uint64_t request_seq = ctx->dispatch_request_seq;

  unregister_response_request(ctx, request_seq);
  release_fixed_slot(ctx);
  ctx->response_receiver = NULL;
  ctx->cleanup = 1;
  ctx->on_request_body_chunk = NULL;
  ctx->on_response_generator_proceed = NULL;
  ctx->on_response_generator_stop = NULL;
  ctx->generator.proceed = NULL;
  ctx->generator.stop = NULL;
  if (cleanup != NULL && module_id != 0 && request_seq != 0)
    cleanup(module_id, request_seq);

  ctx->dispatch_module_id = 0;
  ctx->dispatch_request_seq = 0;
  ctx->on_request_cleanup = NULL;
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
  if (ctx == NULL || ctx->req == NULL || ctx->dispatch_module_id == 0 ||
      ctx->dispatch_request_seq == 0 || ctx->on_request_body_chunk == NULL)
    return 1;

  h2o_req_t *req = ctx->req;
  if (req->entity.base != NULL && req->entity.len > 0)
    ctx->on_request_body_chunk(ctx->dispatch_module_id,
                               ctx->dispatch_request_seq, req->entity.base,
                               req->entity.len, is_end_stream ? 1 : 0);
  else if (is_end_stream)
    ctx->on_request_body_chunk(ctx->dispatch_module_id,
                               ctx->dispatch_request_seq, NULL, 0, 1);

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

  // Allocate ctx from pool so it's auto-freed with request; H2O aborts the
  // process if the allocation fails, so ctx is never NULL.
  clj_req_ctx_t *const ctx =
      h2o_mem_alloc_shared(&req->pool, sizeof(*ctx), cleanup_request);
  memset(ctx, 0, sizeof(*ctx));
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
  } else if (ret == CLJ_HANDLER_SHUTTING_DOWN) {
    h2o_send_error_503(req, "Service Unavailable", "Server is shutting down",
                       H2O_SEND_ERROR_HTTP1_CLOSE_CONNECTION);
    return 0;
  } else if (ret == CLJ_HANDLER_DECLINED) {
    return -1;
  } else if (ret == CLJ_HANDLER_ERROR) {
    h2o_send_error_500(req, "Internal Server Error", "Internal Server Error",
                       H2O_SEND_ERROR_HTTP1_CLOSE_CONNECTION);
    return 0;
  }

  if (ctx->meta.has_body && ctx->on_request_body_chunk != NULL) {
    if (req->proceed_req != NULL) {
      req->write_req.cb = clj_body_write_callback;
      req->write_req.ctx = ctx;
      if (req->entity.base != NULL && req->entity.len > 0)
        ctx->on_request_body_chunk(ctx->dispatch_module_id,
                                   ctx->dispatch_request_seq, req->entity.base,
                                   req->entity.len, 0);
      else
        req->proceed_req(req, NULL);
    } else if (req->entity.base != NULL) {
      ctx->on_request_body_chunk(ctx->dispatch_module_id,
                                 ctx->dispatch_request_seq, req->entity.base,
                                 req->entity.len, 1);
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
                       clj_request_cleanup_cb on_request_cleanup,
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

int clj_h2o_install_request_dispatch(
    clj_req_ctx_t *ctx, clj_mt_receiver_t *response_receiver,
    uint64_t module_id, uint64_t request_seq,
    clj_request_body_chunk_cb on_request_body_chunk) {
  if (ctx == NULL || response_receiver == NULL ||
      !atomic_load_explicit(&response_receiver->accepts_responses,
                            memory_order_acquire) ||
      response_receiver->request_buckets == NULL || module_id == 0 ||
      request_seq == 0)
    return 0;

  if (!register_response_request(response_receiver, ctx, request_seq))
    return 0;
  ctx->response_receiver = response_receiver;
  ctx->dispatch_module_id = module_id;
  ctx->dispatch_request_seq = request_seq;
  ctx->on_request_body_chunk = on_request_body_chunk;
  return 1;
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

static void clj_mt_wakeup_on_recv(h2o_multithread_receiver_t *receiver,
                                  h2o_linklist_t *messages) {
  (void)receiver;
  assert(h2o_linklist_is_empty(messages));
}

static clj_req_ctx_t *response_request(clj_mt_receiver_t *receiver,
                                       uint64_t module_id,
                                       uint64_t request_seq) {
  clj_req_ctx_t *ctx = find_response_request(receiver, request_seq);
  if (ctx == NULL || ctx->cleanup || ctx->response_receiver != receiver ||
      ctx->dispatch_module_id != module_id ||
      ctx->dispatch_request_seq != request_seq)
    return NULL;
  return ctx;
}

static clj_response_slot_t *response_slot_at(clj_mt_receiver_t *receiver,
                                             size_t index) {
  if (receiver == NULL || receiver->ring_storage == NULL ||
      index >= receiver->ring_capacity)
    return NULL;
  return (clj_response_slot_t *)(receiver->ring_storage +
                                 index * receiver->ring_stride);
}

static clj_response_slot_t *
response_slot_from_handle(clj_mt_receiver_t *receiver, uint64_t claim_handle) {
  if (receiver == NULL || claim_handle == 0)
    return NULL;

  uint64_t encoded_index = claim_handle & CLJ_RESPONSE_HANDLE_INDEX_MASK;
  if (encoded_index == 0)
    return NULL;

  clj_response_slot_t *slot =
      response_slot_at(receiver, (size_t)(encoded_index - 1));
  if (slot == NULL ||
      atomic_load_explicit(&slot->claim_token, memory_order_acquire) != claim_handle)
    return NULL;
  return slot;
}

clj_fixed_response_slot_data_t *
clj_h2o_response_slot_data(clj_mt_receiver_t *receiver, size_t index) {
  clj_response_slot_t *slot = response_slot_at(receiver, index);
  return slot != NULL ? &slot->data : NULL;
}

static bool response_range_valid(size_t offset, size_t length, size_t total) {
  return offset <= total && length <= total - offset;
}

static unsigned char *
response_slot_payload(clj_fixed_response_slot_data_t *data) {
  return (unsigned char *)data + sizeof(*data);
}

static bool response_pointer_range_valid(const char *value, size_t value_len,
                                         const unsigned char *payload,
                                         size_t payload_len) {
  if (value == NULL)
    return false;

  uintptr_t payload_start = (uintptr_t)payload;
  if (payload_len > UINTPTR_MAX - payload_start)
    return false;

  uintptr_t payload_end = payload_start + payload_len;
  uintptr_t value_start = (uintptr_t)value;
  if (value_start < payload_start || value_start > payload_end)
    return false;

  return value_len <= payload_end - value_start;
}

static bool response_slot_data_valid(clj_mt_receiver_t *receiver,
                                     clj_fixed_response_slot_data_t *data) {
  if (receiver == NULL || data == NULL || data->module_id == 0 ||
      data->request_seq == 0 || data->status < 200 ||
      data->headers_len > CLJ_RESPONSE_MAX_HEADERS ||
      data->payload_len > receiver->slot_payload_capacity ||
      !response_range_valid(data->body_offset, data->body_len,
                            data->payload_len))
    return false;

  const unsigned char *payload = response_slot_payload(data);
  for (size_t index = 0; index < data->headers_len; ++index) {
    const clj_header_t *header = data->headers + index;
    if (!response_pointer_range_valid(header->name, header->name_len, payload,
                                      data->payload_len) ||
        !response_pointer_range_valid(header->value, header->value_len, payload,
                                      data->payload_len))
      return false;
  }
  return true;
}

size_t clj_h2o_response_slot_data_size(void) {
  return sizeof(clj_fixed_response_slot_data_t);
}

size_t clj_h2o_response_ring_capacity(clj_mt_receiver_t *receiver) {
  return receiver != NULL ? receiver->ring_capacity : 0;
}

size_t
clj_h2o_response_slot_payload_capacity(clj_mt_receiver_t *receiver) {
  return receiver != NULL ? receiver->slot_payload_capacity : 0;
}

size_t clj_h2o_response_ring_claimed(clj_mt_receiver_t *receiver) {
  return receiver != NULL
             ? atomic_load_explicit(&receiver->ring_claimed, memory_order_acquire)
             : 0;
}

size_t clj_h2o_response_ring_ready(clj_mt_receiver_t *receiver) {
  return receiver != NULL
             ? atomic_load_explicit(&receiver->ring_ready, memory_order_acquire)
             : 0;
}

uint64_t clj_h2o_response_try_claim(clj_mt_receiver_t *receiver) {
  if (receiver == NULL ||
      !atomic_load_explicit(&receiver->accepts_responses,
                            memory_order_acquire) ||
      receiver->ring_capacity == 0)
    return 0;

  size_t start = atomic_fetch_add_explicit(&receiver->claim_cursor, 1,
                                            memory_order_relaxed);
  for (size_t attempt = 0; attempt < receiver->ring_capacity; ++attempt) {
    size_t index = (start + attempt) % receiver->ring_capacity;
    clj_response_slot_t *slot = response_slot_at(receiver, index);
    uint32_t expected = CLJ_RESPONSE_SLOT_FREE;
    if (atomic_compare_exchange_strong_explicit(
            &slot->state, &expected, CLJ_RESPONSE_SLOT_CLAIMED,
            memory_order_acq_rel, memory_order_relaxed)) {
      slot->data.module_id = 0;
      uint64_t sequence =
          (atomic_fetch_add_explicit(&receiver->claim_sequence, 1,
                                     memory_order_relaxed) +
           1) &
          CLJ_RESPONSE_HANDLE_SEQUENCE_MASK;
      uint64_t claim_handle =
          (sequence << 16) | (uint64_t)(index + 1);
      slot->data.claim_token = claim_handle;
      atomic_store_explicit(&slot->claim_token, claim_handle, memory_order_release);
      atomic_fetch_add_explicit(&receiver->ring_claimed, 1,
                                memory_order_release);
      return claim_handle;
    }
  }
  return 0;
}

int clj_h2o_response_publish(clj_mt_receiver_t *receiver,
                             uint64_t claim_handle) {
  clj_response_slot_t *slot =
      response_slot_from_handle(receiver, claim_handle);
  if (slot == NULL ||
      !atomic_load_explicit(&receiver->accepts_responses,
                            memory_order_acquire) ||
      !response_slot_data_valid(receiver, &slot->data))
    return 0;

  atomic_fetch_add_explicit(&receiver->ring_ready, 1, memory_order_release);
  uint32_t expected = CLJ_RESPONSE_SLOT_CLAIMED;
  if (!atomic_compare_exchange_strong_explicit(
          &slot->state, &expected, CLJ_RESPONSE_SLOT_READY,
          memory_order_release, memory_order_relaxed)) {
    atomic_fetch_sub_explicit(&receiver->ring_ready, 1,
                              memory_order_release);
    return 0;
  }

  /* READY is not discoverable until this bit is published. The producer use
     protects the receiver through publication, even if cleanup reuses the slot. */
  size_t index = (claim_handle & CLJ_RESPONSE_HANDLE_INDEX_MASK) - 1;
  atomic_fetch_or_explicit(&receiver->ready_words[index / 64],
                           UINT64_C(1) << (index % 64), memory_order_release);
  atomic_fetch_sub_explicit(&receiver->ring_claimed, 1,
                            memory_order_release);
  return 1;
}

int clj_h2o_response_abort(clj_mt_receiver_t *receiver,
                           uint64_t claim_handle) {
  clj_response_slot_t *slot =
      response_slot_from_handle(receiver, claim_handle);
  if (slot == NULL)
    return 0;

  uint32_t expected = CLJ_RESPONSE_SLOT_CLAIMED;
  if (!atomic_compare_exchange_strong_explicit(
          &slot->state, &expected, CLJ_RESPONSE_SLOT_FREE,
          memory_order_release, memory_order_relaxed))
    return 0;

  atomic_fetch_sub_explicit(&receiver->ring_claimed, 1,
                            memory_order_release);
  return 1;
}

static void send_retained_fixed(clj_req_ctx_t *ctx,
                                clj_response_slot_t *slot) {
  h2o_req_t *req = ctx->req;
  clj_fixed_response_slot_data_t *data = &slot->data;
  h2o_iovec_t body = h2o_iovec_init(
      response_slot_payload(data) + data->body_offset, data->body_len);
  bool allows_body = data->status != 204 && data->status != 205 &&
                     data->status != 304 &&
                     !h2o_memis(req->input.method.base, req->input.method.len,
                                H2O_STRLIT("HEAD"));
  req->res.status = data->status;
  req->res.reason = "OK";
  req->res.content_length = data->content_length;
  req->compress_hint = data->compress_hint;
  copy_headers_to_response(req, data->headers, data->headers_len);
  ctx->generator.proceed = NULL;
  ctx->generator.stop = NULL;
  ctx->on_response_generator_proceed = NULL;
  ctx->on_response_generator_stop = NULL;
  ctx->response_started = 1;
  ctx->fixed_slot = slot;
  ctx->fixed_claim = data->claim_token;
  h2o_start_response(req, &ctx->generator);
  h2o_send(req, allows_body ? &body : NULL, allows_body ? 1 : 0,
           H2O_SEND_STATE_FINAL);
  /* Request cleanup can release and recycle the slot during h2o_send. */
}

size_t clj_h2o_response_ring_drain(clj_mt_receiver_t *receiver) {
  if (receiver == NULL || receiver->ring_capacity == 0 ||
      atomic_load_explicit(&receiver->ring_ready, memory_order_acquire) == 0)
    return 0;

  size_t drained = 0;
  for (size_t word = 0; word < receiver->ready_word_count; ++word) {
    /* Detach before sending: synchronous cleanup can republish the same index.
       Its new bit must remain in the receiver for the next drain. */
    uint64_t ready = atomic_exchange_explicit(&receiver->ready_words[word], 0,
                                               memory_order_acquire);
    while (ready != 0) {
      size_t index = word * 64 + (size_t)__builtin_ctzll(ready);
      ready &= ready - 1;
      clj_response_slot_t *slot = response_slot_at(receiver, index);
      uint32_t expected = CLJ_RESPONSE_SLOT_READY;
      if (!atomic_compare_exchange_strong_explicit(
              &slot->state, &expected, CLJ_RESPONSE_SLOT_IN_NATIVE,
              memory_order_acquire, memory_order_relaxed))
        continue;

      clj_fixed_response_slot_data_t *data = &slot->data;
      clj_req_ctx_t *ctx = response_slot_data_valid(receiver, data)
                              ? response_request(receiver, data->module_id,
                                                 data->request_seq)
                              : NULL;
      if (ctx != NULL && !ctx->response_started && ctx->fixed_slot == NULL) {
        atomic_fetch_add_explicit(&receiver->ring_retained, 1,
                                  memory_order_release);
        atomic_fetch_sub_explicit(&receiver->ring_ready, 1,
                                  memory_order_release);
        ++drained;
        send_retained_fixed(ctx, slot);
      } else {
        atomic_fetch_sub_explicit(&receiver->ring_ready, 1,
                                  memory_order_release);
        atomic_store_explicit(&slot->state, CLJ_RESPONSE_SLOT_FREE,
                              memory_order_release);
        ++drained;
      }
    }
  }
  return drained;
}

static void clj_mt_response_on_recv(h2o_multithread_receiver_t *h2o_receiver,
                                    h2o_linklist_t *messages) {
  clj_mt_receiver_t *receiver = H2O_STRUCT_FROM_MEMBER(
      clj_mt_receiver_t, receiver, h2o_receiver);
  assert(h2o_linklist_is_empty(messages));
  clj_h2o_response_ring_drain(receiver);
}

clj_mt_receiver_t *clj_h2o_mt_create_wakeup_receiver(h2o_context_t *ctx) {
  clj_mt_receiver_t *receiver = calloc(1, sizeof(*receiver));
  if (receiver == NULL)
    return NULL;
  receiver->queue = ctx->queue;
  h2o_multithread_register_receiver(receiver->queue, &receiver->receiver,
                                    clj_mt_wakeup_on_recv);
  return receiver;
}

clj_mt_receiver_t *clj_h2o_mt_create_response_receiver(
    h2o_context_t *ctx, size_t ring_capacity, size_t slot_payload_capacity) {
  if (ctx == NULL || ring_capacity == 0 || ring_capacity > UINT16_MAX ||
      slot_payload_capacity == 0)
    return NULL;

  size_t slot_base_size = sizeof(clj_response_slot_t);
  if (slot_payload_capacity > SIZE_MAX - slot_base_size)
    return NULL;
  size_t ring_stride = slot_base_size + slot_payload_capacity;
  size_t alignment = _Alignof(max_align_t);
  size_t remainder = ring_stride % alignment;
  if (remainder != 0) {
    size_t padding = alignment - remainder;
    if (ring_stride > SIZE_MAX - padding)
      return NULL;
    ring_stride += padding;
  }
  if (ring_capacity > SIZE_MAX / ring_stride)
    return NULL;

  clj_mt_receiver_t *receiver = calloc(1, sizeof(*receiver));
  if (receiver == NULL)
    return NULL;

  receiver->request_capacity = 256;
  receiver->request_buckets =
      calloc(receiver->request_capacity, sizeof(*receiver->request_buckets));
  receiver->ring_storage = calloc(ring_capacity, ring_stride);
  receiver->ready_word_count = (ring_capacity + 63) / 64;
  receiver->ready_words =
      calloc(receiver->ready_word_count, sizeof(*receiver->ready_words));
  if (receiver->request_buckets == NULL || receiver->ring_storage == NULL ||
      receiver->ready_words == NULL) {
    free(receiver->ready_words);
    free(receiver->ring_storage);
    free(receiver->request_buckets);
    free(receiver);
    return NULL;
  }

  for (size_t word = 0; word < receiver->ready_word_count; ++word)
    atomic_init(&receiver->ready_words[word], 0);
  receiver->ring_capacity = ring_capacity;
  receiver->ring_stride = ring_stride;
  receiver->slot_payload_capacity = slot_payload_capacity;
  for (size_t index = 0; index < ring_capacity; ++index) {
    clj_response_slot_t *slot = response_slot_at(receiver, index);
    atomic_init(&slot->state, CLJ_RESPONSE_SLOT_FREE);
    atomic_init(&slot->claim_token, 0);
  }

  receiver->queue = ctx->queue;
  atomic_init(&receiver->accepts_responses, true);
  atomic_init(&receiver->ring_claimed, 0);
  atomic_init(&receiver->ring_ready, 0);
  atomic_init(&receiver->ring_retained, 0);
  atomic_init(&receiver->producer_uses, 0);
  atomic_init(&receiver->claim_cursor, 0);
  atomic_init(&receiver->claim_sequence, 0);
  clj_response_slot_t *first_slot = response_slot_at(receiver, 0);
  if (!atomic_is_lock_free(&receiver->ready_words[0]) ||
      !atomic_is_lock_free(&first_slot->state) ||
      !atomic_is_lock_free(&first_slot->claim_token) ||
      !atomic_is_lock_free(&receiver->ring_retained) ||
      !atomic_is_lock_free(&receiver->producer_uses) ||
      !atomic_is_lock_free(&receiver->ring_claimed) ||
      !atomic_is_lock_free(&receiver->ring_ready) ||
      !atomic_is_lock_free(&receiver->claim_cursor) ||
      !atomic_is_lock_free(&receiver->claim_sequence) ||
      !atomic_is_lock_free(&receiver->accepts_responses)) {
    free(receiver->ready_words);
    free(receiver->ring_storage);
    free(receiver->request_buckets);
    free(receiver);
    return NULL;
  }
  h2o_multithread_register_receiver(receiver->queue, &receiver->receiver,
                                    clj_mt_response_on_recv);
  return receiver;
}

void clj_h2o_mt_destroy_wakeup_receiver(clj_mt_receiver_t *receiver) {
  if (receiver == NULL)
    return;
  h2o_multithread_unregister_receiver(receiver->queue, &receiver->receiver);
  free(receiver);
}

void clj_h2o_mt_destroy_response_receiver(clj_mt_receiver_t *receiver) {
  if (receiver == NULL)
    return;
  atomic_store_explicit(&receiver->accepts_responses, false,
                        memory_order_release);
  assert(atomic_load_explicit(&receiver->ring_claimed,
                              memory_order_acquire) == 0);
  assert(atomic_load_explicit(&receiver->ring_ready, memory_order_acquire) ==
         0);
  assert(atomic_load_explicit(&receiver->ring_retained, memory_order_acquire) == 0);
  assert(atomic_load_explicit(&receiver->producer_uses, memory_order_acquire) == 0);
  assert(receiver->request_size == 0);
  for (size_t word = 0; word < receiver->ready_word_count; ++word)
    assert(atomic_load_explicit(&receiver->ready_words[word],
                                memory_order_acquire) == 0);
  h2o_multithread_unregister_receiver(receiver->queue, &receiver->receiver);
  free(receiver->ready_words);
  free(receiver->ring_storage);
  free(receiver->request_buckets);
  free(receiver);
}

void clj_h2o_mt_wakeup(clj_mt_receiver_t *receiver) {
  if (receiver == NULL)
    return;
  h2o_multithread_send_message(&receiver->receiver, NULL);
}

size_t clj_h2o_mt_response_pending(clj_mt_receiver_t *receiver) {
  if (receiver == NULL)
    return 0;
  return atomic_load_explicit(&receiver->ring_claimed, memory_order_acquire) +
         atomic_load_explicit(&receiver->ring_ready, memory_order_acquire) +
         atomic_load_explicit(&receiver->ring_retained, memory_order_acquire) +
         atomic_load_explicit(&receiver->producer_uses, memory_order_acquire);
}

int clj_h2o_response_use_acquire(clj_mt_receiver_t *receiver) {
  /* Caller holds receiver admission protection until this reference exists. */
  if (receiver == NULL ||
      !atomic_load_explicit(&receiver->accepts_responses, memory_order_acquire))
    return 0;
  atomic_fetch_add_explicit(&receiver->producer_uses, 1, memory_order_acq_rel);
  return 1;
}

void clj_h2o_response_use_release(clj_mt_receiver_t *receiver) {
  size_t previous = atomic_fetch_sub_explicit(&receiver->producer_uses, 1,
                                              memory_order_release);
  assert(previous != 0);
}

size_t clj_h2o_response_ring_retained(clj_mt_receiver_t *receiver) {
  return receiver != NULL
             ? atomic_load_explicit(&receiver->ring_retained, memory_order_acquire)
             : 0;
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

  if (flat->has_http1__req_io_timeout_while_generating) {
    conf->http1.req_io_timeout_while_generating =
        flat->http1__req_io_timeout_while_generating;
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
  /* By default silent HTTP/1 streams such as SSE stay open; the request I/O
     timeout covers request receipt and each response write. The flat
     configuration can turn the timeout on while generating. */
  conf->http1.req_io_timeout_while_generating = 0;

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

/* Thread-safe key manager structure */
struct clj_ticket_manager {
    pthread_rwlock_t rwlock;
    clj_session_ticket_t *keys;
    size_t num_keys;
    size_t capacity;
    uint32_t ticket_lifetime_seconds;
    /* Changes after each key replacement; workers rebuild QUIC keys when it differs */
    _Atomic uint64_t generation;

    /* QUIC transport params hash for 0-RTT validation */
    uint8_t quic_tp_tag[8];
    int quic_tp_tag_valid;
};

/* Address token keys derived from one session ticket key (h2o src/ssl.c
   st_quic_keyset_t, without the CID encryptor). */
typedef struct {
  uint8_t name;
  ptls_aead_context_t *enc;
  ptls_aead_context_t *dec;
} clj_quic_keyset_t;

/* A picotls AEAD context is not safe to use from two threads, so each worker
   keeps keysets for all ticket keys, as h2o keeps them per thread (quic_keys). */
typedef struct {
  clj_ticket_manager_t *mgr;
  uint64_t generation;
  clj_quic_keyset_t *keys;
  size_t num_keys;
} clj_address_token_keys_t;

/* QUIC load of one registered worker of a transport, freed when the worker
   unregisters. `conns` changes only on the worker's thread; `pending` counts
   new flows forwarded to the worker that it has not read yet. `forward_fd`
   is the write end of the worker's forwarding channel, -1 once the worker
   unregisters. `using_sets` is odd while the worker uses worker sets and
   what it found in them (see clj_http3_publish_workers). It, `forwarded` and
   `connected` change only on the worker's thread, on a cache line of their
   own, so that the forward path does not write to the line other workers
   read when they choose; records are allocated cache-line aligned. */
typedef struct clj_http3_worker_load_t {
  uint64_t node_id;
  uint32_t thread_id;
  _Atomic uint32_t conns;
  _Atomic uint32_t pending;
  _Atomic int accepting;
  _Atomic int withdrawn;
  _Atomic int forward_fd;
  struct clj_http3_worker_load_t *next;
  _Alignas(64) _Atomic uint64_t using_sets;
  _Atomic uint64_t forwarded;
  _Atomic uint32_t connected;
} clj_http3_worker_load_t;

_Static_assert(offsetof(clj_http3_worker_load_t, using_sets) == 64 &&
                   sizeof(clj_http3_worker_load_t) == 128,
               "worker load record lines");

/* The attached workers of a transport, for lookups without the mutex. A set
   is replaced, never changed, and freed once no worker uses it. */
typedef struct clj_http3_worker_set_t {
  uint64_t active_node_id;
  size_t num_loads;
  clj_http3_worker_load_t *loads[];
} clj_http3_worker_set_t;

/* A new flow seen by this worker, so its later Initial and 0-RTT datagrams
   follow its first one to the worker `target_node` and `target_thread` name */
typedef struct {
  uint64_t expires_at;
  quicly_address_t srcaddr;
  uint64_t target_node;
  uint32_t target_thread;
  uint8_t dcid_len;
  uint8_t dcid[QUICLY_MAX_CID_LEN_V1];
} clj_http3_new_flow_t;

#define CLJ_HTTP3_NEW_FLOWS 64
#define CLJ_HTTP3_NEW_FLOW_MILLIS 3000

/* A UDP socket connected to the client of a connection whose datagrams the
   kernel delivers to another worker, so that it delivers them here instead.
   Linux only: other kernels do not prefer a connected socket within a
   SO_REUSEPORT group in the same way. */
#ifdef __linux__
#define CLJ_HTTP3_CONNECTED_SOCKETS 1
#else
#define CLJ_HTTP3_CONNECTED_SOCKETS 0
#endif
/* Per listener, over all workers and generations. Every datagram to the
   port that no connected socket takes is looked up past all of them (Linux
   6.18: about 4 ns each), so this bounds the cost for the other flows. */
#define CLJ_HTTP3_CONNECTED_MAX 64
#define CLJ_HTTP3_CONNECT_RETRY_MILLIS 1000

typedef struct {
  clj_http3_ctx_t *ctx;
  h2o_quic_conn_t *conn;
  h2o_socket_t *sock;
  quicly_address_t local;
  quicly_address_t peer;
} clj_http3_connected_t;

enum {
  CLJ_HTTP3_READING_PUBLIC,
  CLJ_HTTP3_READING_FORWARDED,
  CLJ_HTTP3_READING_CONNECTED
};

/* HTTP/3 (QUIC) context structure holding all resources for one worker */
struct clj_http3_ctx_t {
  h2o_http3_server_ctx_t h3_ctx;
  h2o_accept_ctx_t accept_ctx;
  h2o_socket_t *udp_sock;
  h2o_socket_t *forwarded_sock;
  quicly_cid_plaintext_t next_cid;
  struct clj_http3_udp_transport_t *transport;
  clj_address_token_keys_t address_token;
  clj_http3_worker_load_t *load;
  uint64_t node_id;
  uint32_t thread_id;
  int forward_fd;
  int fd;
  /* The socket H2O is reading, and for a connected one its client */
  int reading;
  quicly_address_t reading_peer;
  /* Flows this worker kept for itself in the loop turn `kept_at` (the loop's
     time); their accepts follow after the whole receive batch */
  uint64_t kept_at;
  uint32_t kept;
  clj_http3_new_flow_t new_flows[CLJ_HTTP3_NEW_FLOWS];
  uint64_t connect_retry_at;
  uint32_t num_connected;
  clj_http3_connected_t *connected[CLJ_HTTP3_CONNECTED_MAX];
};

typedef struct {
  uint64_t node_id;
  uint32_t thread_id;
  struct clj_http3_ctx_t *ctx;
  clj_http3_worker_load_t *load;
} clj_http3_transport_attachment_t;

struct clj_http3_udp_transport_t {
  int reservation_fd;
  char *host;
  uint16_t port;
  pthread_mutex_t mutex;
  uint8_t cid_key[32];
  int cid_key_initialized;
  clj_http3_transport_attachment_t *attachments;
  size_t num_attachments;
  size_t attachments_capacity;
  uint64_t active_node_id;
  /* Every registered worker, also one whose slot a later registration of
     the same ids took. `unlisted` are unregistered workers that the current
     set still names, freed when that set is replaced. */
  clj_http3_worker_load_t *loads;
  clj_http3_worker_load_t *unlisted;
  _Atomic(clj_http3_worker_set_t *) workers;
  _Atomic uint32_t spread_cursor;
  _Atomic uint32_t num_connected;
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
  clj_encrypt_ticket_t *encrypt_ticket;
  clj_ptls_identity_cache_entry_t *cache_head;
  pthread_mutex_t cache_mutex;
} clj_ptls_wrapper_t;

static void clj_ptls_free_certificates(ptls_context_t *ctx) {
  for (size_t i = 0; i != ctx->certificates.count; ++i)
    free(ctx->certificates.list[i].base);
  free(ctx->certificates.list);
  ctx->certificates.list = NULL;
  ctx->certificates.count = 0;
}

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
        clj_ptls_free_certificates(&new_entry->ctx);
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
      clj_ptls_free_certificates(&wrapper->ctx);
      pthread_mutex_destroy(&wrapper->cache_mutex);
      free(wrapper);
      return NULL;
    }

    FILE *fp = fopen(key_file, "r");
    if (!fp) {
      DEBUG_LOG("failed to open key file: %s", key_file);
      clj_ptls_free_certificates(&wrapper->ctx);
      pthread_mutex_destroy(&wrapper->cache_mutex);
      free(wrapper);
      return NULL;
    }

    EVP_PKEY *pkey = PEM_read_PrivateKey(fp, NULL, NULL, NULL);
    fclose(fp);
    if (!pkey) {
      DEBUG_LOG("failed to load private key from %s", key_file);
      clj_ptls_free_certificates(&wrapper->ctx);
      pthread_mutex_destroy(&wrapper->cache_mutex);
      free(wrapper);
      return NULL;
    }

    if (ptls_openssl_init_sign_certificate(&wrapper->sign_certificate, pkey) !=
        0) {
      DEBUG_LOG("failed to setup private key");
      EVP_PKEY_free(pkey);
      clj_ptls_free_certificates(&wrapper->ctx);
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

  clj_ptls_free_certificates(&wrapper->ctx);
  free(wrapper->encrypt_ticket);

  clj_ptls_identity_cache_entry_t *entry = wrapper->cache_head;
  while (entry != NULL) {
    clj_ptls_identity_cache_entry_t *next = entry->next;
    ptls_openssl_dispose_sign_certificate(&entry->sign_certificate);
    clj_ptls_free_certificates(&entry->ctx);
    free(entry->hostname);
    free(entry->cert_chain_pem);
    free(entry->private_key_pem);
    free(entry);
    entry = next;
  }

  pthread_mutex_destroy(&wrapper->cache_mutex);

  free(wrapper);
}

/* Address tokens follow h2o's server: init_quic_keyset, update_quic_keys,
   quic_decrypt_address_token and generate_resumption_token in src/ssl.c, and
   validate_token and on_http3_accept in src/main.c. */
static int clj_init_quic_keyset(clj_quic_keyset_t *keyset,
                                const clj_session_ticket_t *ticket) {
  uint8_t master[CLJ_TICKET_AES_KEY_LEN + CLJ_TICKET_HMAC_KEY_LEN],
      master_digest[PTLS_SHA256_DIGEST_SIZE], keybuf[PTLS_SHA256_DIGEST_SIZE];
  memcpy(master, ticket->aes_key, CLJ_TICKET_AES_KEY_LEN);
  memcpy(master + CLJ_TICKET_AES_KEY_LEN, ticket->hmac_key,
         CLJ_TICKET_HMAC_KEY_LEN);

  *keyset = (clj_quic_keyset_t){.name = ticket->name[0]};
  if (ptls_calc_hash(&ptls_openssl_sha256, master_digest, master,
                     sizeof(master)) == 0 &&
      ptls_hkdf_expand_label(&ptls_openssl_sha256, keybuf, sizeof(keybuf),
                             ptls_iovec_init(master_digest,
                                             sizeof(master_digest)),
                             "address-token", ptls_iovec_init(NULL, 0),
                             "") == 0) {
    keyset->enc = ptls_aead_new(&ptls_openssl_aes128gcm, &ptls_openssl_sha256,
                                1, keybuf, "");
    keyset->dec = ptls_aead_new(&ptls_openssl_aes128gcm, &ptls_openssl_sha256,
                                0, keybuf, "");
  }

  ptls_clear_memory(master, sizeof(master));
  ptls_clear_memory(master_digest, sizeof(master_digest));
  ptls_clear_memory(keybuf, sizeof(keybuf));
  return keyset->enc != NULL && keyset->dec != NULL;
}

static void clj_dispose_quic_keyset(clj_quic_keyset_t *keyset) {
  if (keyset->enc != NULL)
    ptls_aead_free(keyset->enc);
  if (keyset->dec != NULL)
    ptls_aead_free(keyset->dec);
  *keyset = (clj_quic_keyset_t){0};
}

static void clj_address_token_keys_clear(clj_address_token_keys_t *keys) {
  while (keys->num_keys != 0)
    clj_dispose_quic_keyset(keys->keys + --keys->num_keys);
  free(keys->keys);
  keys->keys = NULL;
}

static void clj_address_token_keys_dispose(clj_address_token_keys_t *keys) {
  clj_address_token_keys_clear(keys);
  *keys = (clj_address_token_keys_t){0};
}

/* Returns the active keyset (the newest ticket key), or NULL when the worker
   has no ticket keys. h2o aborts where this returns NULL; the JVM must not. */
static clj_quic_keyset_t *
clj_update_quic_keys(clj_address_token_keys_t *keys) {
  clj_ticket_manager_t *mgr = keys->mgr;
  uint64_t new_generation;

  if (mgr == NULL)
    return NULL;
  while ((new_generation = atomic_load(&mgr->generation)) != keys->generation) {
    clj_address_token_keys_clear(keys);

    pthread_rwlock_rdlock(&mgr->rwlock);
    if (mgr->num_keys != 0 &&
        (keys->keys = calloc(mgr->num_keys, sizeof(*keys->keys))) == NULL) {
      pthread_rwlock_unlock(&mgr->rwlock);
      return NULL;
    }
    for (; keys->num_keys != mgr->num_keys; ++keys->num_keys) {
      if (!clj_init_quic_keyset(keys->keys + keys->num_keys,
                                mgr->keys + keys->num_keys)) {
        clj_dispose_quic_keyset(keys->keys + keys->num_keys);
        pthread_rwlock_unlock(&mgr->rwlock);
        clj_address_token_keys_clear(keys);
        return NULL;
      }
    }
    pthread_rwlock_unlock(&mgr->rwlock);

    keys->generation = new_generation;
  }

  return keys->num_keys != 0 ? keys->keys : NULL;
}

static clj_quic_keyset_t *
clj_find_quic_keyset(clj_address_token_keys_t *keys, uint8_t key_id) {
  for (size_t i = 0; i != keys->num_keys; ++i) {
    if (keys->keys[i].name == key_id)
      return keys->keys + i;
  }
  return NULL;
}

/* No generator error only skips the token: quicly returns it from
   quicly_send, where h2o_quic_send destroys the connection for most codes and
   SENDBUF_FULL leaves NEW_TOKEN pending ahead of stream data, stalling it. A
   worker without keys therefore sends random bytes of a real token's length
   (quicly_encrypt_address_token) with the resumption type, which no server
   decrypts or answers with INVALID_TOKEN. */
static quicly_error_t
clj_random_address_token(ptls_buffer_t *buf,
                         const quicly_address_token_plaintext_t *token) {
  size_t address_len = token->remote.sa.sa_family == AF_INET6 ? 16 + 4 : 4;
  size_t len = 1 + 1 + PTLS_AESGCM_IV_SIZE + 8 + 1 + address_len + 2 + 1 +
               token->resumption.len + 1 + token->appdata.len +
               PTLS_AESGCM_TAG_SIZE;
  int ret;
  if ((ret = ptls_buffer_reserve(buf, len)) != 0)
    return ret;
  ptls_openssl_random_bytes(buf->base + buf->off, len);
  buf->base[buf->off + 1] = QUICLY_ADDRESS_TOKEN_TYPE_RESUMPTION;
  buf->off += len;
  return 0;
}

static quicly_error_t
clj_encrypt_address_token(clj_address_token_keys_t *keys, ptls_buffer_t *buf,
                          const quicly_address_token_plaintext_t *token) {
  clj_quic_keyset_t *keyset = clj_update_quic_keys(keys);
  int ret;

  if (keyset == NULL)
    return clj_random_address_token(buf, token);

  if ((ret = ptls_buffer_reserve(buf, 1)) != 0)
    return ret;
  buf->base[buf->off++] = keyset->name;
  return quicly_encrypt_address_token(ptls_openssl_random_bytes, keyset->enc,
                                      buf, buf->off - 1, token);
}

static quicly_error_t
clj_decrypt_address_token(clj_address_token_keys_t *keys,
                          quicly_address_token_plaintext_t *pt,
                          ptls_iovec_t input, const char **err_desc) {
  clj_quic_keyset_t *keyset;

  clj_update_quic_keys(keys);

  if ((keyset = clj_find_quic_keyset(keys, input.base[0])) == NULL)
    return PTLS_ERROR_INCOMPATIBLE_KEY;
  return quicly_decrypt_address_token(keyset->dec, pt, input.base, input.len, 1,
                                      err_desc);
}

/* Runs on the worker loop of the connection, so the worker's keys are safe to use */
static quicly_error_t
clj_generate_resumption_token(quicly_generate_resumption_token_t *self,
                              quicly_conn_t *conn, ptls_buffer_t *buf,
                              quicly_address_token_plaintext_t *token) {
  (void)self;
  h2o_quic_conn_t *qconn = *quicly_get_data(conn);
  clj_http3_ctx_t *ctx =
      H2O_STRUCT_FROM_MEMBER(clj_http3_ctx_t, h3_ctx.super, qconn->ctx);
  return clj_encrypt_address_token(&ctx->address_token, buf, token);
}

static quicly_generate_resumption_token_t clj_resumption_token_generator = {
    clj_generate_resumption_token};

static int clj_validate_address_token(quicly_context_t *quic,
                                      struct sockaddr *remote,
                                      ptls_iovec_t client_cid,
                                      ptls_iovec_t server_cid,
                                      quicly_address_token_plaintext_t *token) {
  double now, age;
  quic->now->cb(quic->now, &now);
  if ((age = now - token->issued_at) < 0)
    age = 0;

  token->address_mismatch =
      h2o_socket_compare_address(remote, &token->remote.sa,
                                 token->type == QUICLY_ADDRESS_TOKEN_TYPE_RETRY) != 0;

  switch (token->type) {
  case QUICLY_ADDRESS_TOKEN_TYPE_RETRY:
    return !token->address_mismatch && age <= 30 * 1000 &&
           quicly_cid_is_equal(&token->retry.client_cid, client_cid) &&
           quicly_cid_is_equal(&token->retry.server_cid, server_cid);
  case QUICLY_ADDRESS_TOKEN_TYPE_RESUMPTION:
    if (age > 10 * 60 * 1000)
      token->address_mismatch = 1;
    return 1;
  }
  return 0;
}

/* Sets *token to the token quicly_accept should use, or NULL to accept as if
   the Initial carried none. QUICLY_TRANSPORT_ERROR_INVALID_TOKEN means the
   packet carried a damaged Retry token and must be answered with
   INVALID_TOKEN instead of a connection. */
static quicly_error_t clj_read_address_token(
    clj_address_token_keys_t *keys, quicly_context_t *quic,
    struct sockaddr *remote,
    quicly_decoded_packet_t *packet, quicly_address_token_plaintext_t *buf,
    quicly_address_token_plaintext_t **token, const char **err_desc) {
  *token = NULL;
  *err_desc = NULL;
  if (packet->token.len == 0)
    return 0;
  quicly_error_t ret =
      clj_decrypt_address_token(keys, buf, packet->token, err_desc);
  if (ret == 0) {
    if (clj_validate_address_token(quic, remote, packet->cid.src,
                                   packet->cid.dest.encrypted, buf))
      *token = buf;
  } else if (ret == QUICLY_TRANSPORT_ERROR_INVALID_TOKEN) {
    return ret;
  }
  return 0;
}

/* Internal structure to hold quicly context and its CID encryptor */
typedef struct {
  quicly_context_t ctx;
  quicly_cid_encryptor_t *cid_encryptor;
} clj_quicly_wrapper_t;

static int clj_quicly_wrapper_set_cid_key(quicly_context_t *ctx,
                                          const uint8_t *cid_key,
                                          size_t cid_key_len) {
  clj_quicly_wrapper_t *wrapper =
      H2O_STRUCT_FROM_MEMBER(clj_quicly_wrapper_t, ctx, ctx);
  quicly_cid_encryptor_t *cid_encryptor =
      quicly_new_default_cid_encryptor(&ptls_openssl_aes128ecb,
                                       &ptls_openssl_aes128ecb,
                                       &ptls_openssl_sha256,
                                       ptls_iovec_init((void *)cid_key,
                                                       cid_key_len));
  if (cid_encryptor == NULL)
    return 0;

  if (wrapper->cid_encryptor != NULL)
    quicly_free_default_cid_encryptor(wrapper->cid_encryptor);

  wrapper->cid_encryptor = cid_encryptor;
  wrapper->ctx.cid_encryptor = cid_encryptor;
  return 1;
}

static quicly_closed_t clj_quic_closed;

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
  wrapper->ctx.generate_resumption_token = &clj_resumption_token_generator;
  wrapper->ctx.closed = &clj_quic_closed;

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
    quicly_free_default_cid_encryptor(wrapper->cid_encryptor);

  free(wrapper);
}

static void clj_http3_take_pending(clj_http3_worker_load_t *load) {
  uint32_t pending = atomic_load(&load->pending);
  while (pending != 0 &&
         !atomic_compare_exchange_weak(&load->pending, &pending, pending - 1))
    ;
}

static void clj_http3_count_accept(h2o_quic_ctx_t *quic_ctx) {
  clj_http3_worker_load_t *load =
      H2O_STRUCT_FROM_MEMBER(clj_http3_ctx_t, h3_ctx.super, quic_ctx)->load;
  if (load != NULL)
    atomic_fetch_add(&load->conns, 1);
}

static void clj_http3_count_close(h2o_quic_ctx_t *quic_ctx) {
  clj_http3_worker_load_t *load =
      H2O_STRUCT_FROM_MEMBER(clj_http3_ctx_t, h3_ctx.super, quic_ctx)->load;
  if (load == NULL)
    return;
  uint32_t conns = atomic_load(&load->conns);
  while (conns != 0 &&
         !atomic_compare_exchange_weak(&load->conns, &conns, conns - 1))
    ;
}

/* Flow table ages; wall-clock steps must not keep entries alive */
static uint64_t clj_monotonic_millis(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void clj_http3_count_forward(clj_http3_ctx_t *ctx) {
  if (ctx->load != NULL)
    atomic_store_explicit(
        &ctx->load->forwarded,
        atomic_load_explicit(&ctx->load->forwarded, memory_order_relaxed) + 1,
        memory_order_relaxed);
}

static void clj_http3_count_connected(clj_http3_ctx_t *ctx) {
  if (ctx->load != NULL)
    atomic_store(&ctx->load->connected, ctx->num_connected);
}

/* The connected socket is bound to the address the client sends to, so it
   needs that address in the listener's family and on its port, and a
   unicast one: a refused bind would pause the worker's attempts for all
   connections. IPv6 link-local addresses are left out: binding one needs
   the arriving interface, which H2O does not keep. */
static int clj_http3_may_connect(clj_http3_ctx_t *ctx,
                                 const quicly_address_t *local,
                                 const quicly_address_t *peer) {
  h2o_quic_socket_t *listener = &ctx->h3_ctx.super.sock;
  if (listener->sock == NULL || local->sa.sa_family != peer->sa.sa_family ||
      local->sa.sa_family != listener->addr.ss_family)
    return 0;
  switch (local->sa.sa_family) {
  case AF_INET: {
    uint32_t addr = ntohl(local->sin.sin_addr.s_addr);
    return local->sin.sin_port == *listener->port && addr != INADDR_ANY &&
           addr != INADDR_BROADCAST && !IN_MULTICAST(addr) &&
           peer->sin.sin_port != 0;
  }
  case AF_INET6:
    return local->sin6.sin6_port == *listener->port &&
           !IN6_IS_ADDR_UNSPECIFIED(&local->sin6.sin6_addr) &&
           !IN6_IS_ADDR_MULTICAST(&local->sin6.sin6_addr) &&
           !IN6_IS_ADDR_LINKLOCAL(&local->sin6.sin6_addr) &&
           !IN6_IS_ADDR_LINKLOCAL(&peer->sin6.sin6_addr) &&
           !IN6_IS_ADDR_V4MAPPED(&peer->sin6.sin6_addr) &&
           peer->sin6.sin6_port != 0;
  default:
    return 0;
  }
}

/* SO_REUSEPORT as on the listener's sockets (clj_h2o_http3_open_udp_fd), so
   the socket joins their group: until its connect it takes only its share
   of the port's datagrams, where a socket bound with SO_REUSEADDR alone
   would take them all. Packet info tells H2O each datagram's destination
   address. No DF bit: H2O sends from the listener's socket, never from
   this one. */
static int clj_http3_bound_fd(const quicly_address_t *local) {
#if CLJ_HTTP3_CONNECTED_SOCKETS
  int on = 1, family = local->sa.sa_family;
  int fd = socket(family, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                  IPPROTO_UDP);
  if (fd == -1)
    return -1;
  if ((family == AF_INET6 &&
       setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &on, sizeof(on)) != 0) ||
      setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0 ||
      setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on)) != 0 ||
      (family == AF_INET
           ? setsockopt(fd, IPPROTO_IP, IP_PKTINFO, &on, sizeof(on))
           : setsockopt(fd, IPPROTO_IPV6, IPV6_RECVPKTINFO, &on,
                        sizeof(on))) != 0 ||
      bind(fd, &local->sa, quicly_get_socklen((struct sockaddr *)&local->sa)) !=
          0) {
    int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    return -1;
  }
  return fd;
#else
  (void)local;
  errno = ENOTSUP;
  return -1;
#endif
}

static int clj_http3_open_connected_fd(const quicly_address_t *local,
                                       const quicly_address_t *peer) {
  int fd = clj_http3_bound_fd(local);
  if (fd != -1 &&
      connect(fd, &peer->sa, quicly_get_socklen((struct sockaddr *)&peer->sa)) !=
          0) {
    int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    return -1;
  }
  return fd;
}

static int clj_http3_reserve_connected(clj_http3_udp_transport_t *transport) {
  uint32_t num = atomic_load(&transport->num_connected);
  while (num < CLJ_HTTP3_CONNECTED_MAX) {
    if (atomic_compare_exchange_weak(&transport->num_connected, &num, num + 1))
      return 1;
  }
  return 0;
}

static void clj_on_conn_update(h2o_quic_ctx_t *quic_ctx, h2o_quic_conn_t *conn);

/* While the worker reads its forwarding channel, and while it has connected
   sockets, whose clients may move to a path the kernel delivers to it
   directly; a worker without either pays nothing */
static void clj_http3_set_conn_update(clj_http3_ctx_t *ctx) {
  ctx->h3_ctx.super.notify_conn_update =
      CLJ_HTTP3_CONNECTED_SOCKETS && (ctx->reading == CLJ_HTTP3_READING_FORWARDED ||
                                      ctx->num_connected != 0)
          ? clj_on_conn_update
          : NULL;
}

static clj_http3_connected_t **
clj_http3_find_connected(clj_http3_ctx_t *ctx, h2o_quic_conn_t *conn) {
  for (uint32_t idx = 0; idx < ctx->num_connected; ++idx) {
    if (ctx->connected[idx]->conn == conn)
      return ctx->connected + idx;
  }
  return NULL;
}

static void clj_http3_close_connected(clj_http3_ctx_t *ctx,
                                      clj_http3_connected_t **slot) {
  clj_http3_connected_t *connected = *slot;
  *slot = ctx->connected[--ctx->num_connected];
  h2o_socket_close(connected->sock);
  free(connected);
  atomic_fetch_sub(&ctx->transport->num_connected, 1);
  clj_http3_count_connected(ctx);
  clj_http3_set_conn_update(ctx);
}

static void clj_http3_connected_on_read(h2o_socket_t *sock, const char *err) {
  clj_http3_connected_t *connected = sock->data;
  clj_http3_ctx_t *ctx = connected->ctx;

  if (err != NULL)
    return;

  /* A copy: the connection, and `connected` with it, may close while H2O
     processes the batch */
  ctx->reading_peer = connected->peer;
  ctx->reading = CLJ_HTTP3_READING_CONNECTED;
  h2o_quic_read_socket(&ctx->h3_ctx.super, sock);
  ctx->reading = CLJ_HTTP3_READING_PUBLIC;
}

/* Gives `conn` a socket bound to `local` and connected to `peer`. Returns 0
   when the connection keeps arriving through the listener's sockets. */
static int clj_http3_connect(clj_http3_ctx_t *ctx, h2o_quic_conn_t *conn,
                             const quicly_address_t *local,
                             const quicly_address_t *peer) {
  if (ctx->transport == NULL || ctx->num_connected == CLJ_HTTP3_CONNECTED_MAX ||
      !clj_http3_may_connect(ctx, local, peer))
    return 0;
  uint64_t now = clj_monotonic_millis();
  if (now < ctx->connect_retry_at || !clj_http3_reserve_connected(ctx->transport))
    return 0;
  clj_http3_connected_t *connected = malloc(sizeof(*connected));
  int fd = connected == NULL ? -1 : clj_http3_open_connected_fd(local, peer);
  if (fd == -1) {
    free(connected);
    atomic_fetch_sub(&ctx->transport->num_connected, 1);
    /* Without a pause every forwarded batch would repeat the failing calls */
    ctx->connect_retry_at = now + CLJ_HTTP3_CONNECT_RETRY_MILLIS;
    return 0;
  }
  *connected = (clj_http3_connected_t){
      .ctx = ctx,
      .conn = conn,
      .sock = h2o_evloop_socket_create(ctx->h3_ctx.super.loop, fd,
                                       H2O_SOCKET_FLAG_DONT_READ),
      .local = *local,
      .peer = *peer,
  };
  connected->sock->data = connected;
  ctx->connected[ctx->num_connected++] = connected;
  clj_http3_count_connected(ctx);
  clj_http3_set_conn_update(ctx);
  h2o_socket_read_start(connected->sock, clj_http3_connected_on_read);
  return 1;
}

/* Keeps `conn`'s connected socket on the addresses of the path it uses. When
   the client moves (migration, NAT rebinding), the socket of the old path is
   closed and replaced, whether the new path's datagrams are forwarded here
   or the kernel delivers them here directly; only forwarded datagrams give a
   connection its first socket. */
static void clj_http3_follow_path(clj_http3_ctx_t *ctx, h2o_quic_conn_t *conn,
                                  quicly_address_t *local,
                                  quicly_address_t *peer, int may_create) {
  clj_http3_connected_t **slot = clj_http3_find_connected(ctx, conn);
  if (slot != NULL) {
    if (h2o_socket_compare_address(&(*slot)->peer.sa, &peer->sa, 1) == 0 &&
        h2o_socket_compare_address(&(*slot)->local.sa, &local->sa, 1) == 0)
      return;
    clj_http3_close_connected(ctx, slot);
  } else if (!may_create) {
    return;
  }
  clj_http3_connect(ctx, conn, local, peer);
}

/* Runs after H2O processes datagrams of `conn` from the forwarding channel
   or the listener's socket (clj_http3_set_conn_update); a connected socket
   carries only the path it was opened for. Connects only once the handshake
   is complete: the client has then shown that it receives at its address,
   so a spoofed source address never gets a socket. */
static void clj_on_conn_update(h2o_quic_ctx_t *quic_ctx,
                               h2o_quic_conn_t *conn) {
  clj_http3_ctx_t *ctx =
      H2O_STRUCT_FROM_MEMBER(clj_http3_ctx_t, h3_ctx.super, quic_ctx);
  if (ctx->reading == CLJ_HTTP3_READING_CONNECTED ||
      quicly_get_state(conn->quic) != QUICLY_STATE_CONNECTED ||
      !ptls_handshake_is_complete(quicly_get_tls(conn->quic)))
    return;
  clj_http3_follow_path(ctx, conn,
                        (quicly_address_t *)quicly_get_sockname(conn->quic),
                        (quicly_address_t *)quicly_get_peername(conn->quic),
                        ctx->reading == CLJ_HTTP3_READING_FORWARDED);
}

static void clj_http3_disconnect(clj_http3_ctx_t *ctx, h2o_quic_conn_t *conn) {
  if (ctx->num_connected != 0) {
    clj_http3_connected_t **slot = clj_http3_find_connected(ctx, conn);
    if (slot != NULL)
      clj_http3_close_connected(ctx, slot);
  }
}

/* quicly records a close reason exactly when it calls its `closed`
   callback, on entry to closing or draining; an idle or handshake timeout
   drains without one */
static int clj_http3_reported_closed(quicly_conn_t *quic) {
  const char *reason_phrase = NULL;
  if (quic == NULL || quicly_get_state(quic) < QUICLY_STATE_CLOSING)
    return 0;
  quicly_get_close_reason(quic, NULL, &reason_phrase, NULL);
  return reason_phrase != NULL;
}

/* A connection in its closing period carries no more requests, so it no
   longer counts toward its worker's load; it keeps its connected socket and
   its connection limit slot until destroy */
static void clj_on_quic_closed(quicly_closed_t *self, quicly_conn_t *quic) {
  (void)self;
  h2o_quic_conn_t *conn = *quicly_get_data(quic);
  if (conn != NULL)
    clj_http3_count_close(conn->ctx);
}

static quicly_closed_t clj_quic_closed = {clj_on_quic_closed};

/* HTTP/3 connection destroy wrapper - releases connection limit slot */
static void clj_on_http3_conn_destroy(h2o_quic_conn_t *conn) {
  clj_http3_disconnect(
      H2O_STRUCT_FROM_MEMBER(clj_http3_ctx_t, h3_ctx.super, conn->ctx), conn);
  if (!clj_http3_reported_closed(conn->quic))
    clj_http3_count_close(conn->ctx);
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
  clj_http3_ctx_t *ctx =
      H2O_STRUCT_FROM_MEMBER(clj_http3_ctx_t, h3_ctx.super, quic_ctx);

  /* Enforce global connection limit before accepting */
  if (!clj_h2o_conn_limit_try_acquire())
    return NULL;

  quicly_address_token_plaintext_t token_buf, *token;
  const char *err_desc;
  if (clj_read_address_token(&ctx->address_token, quic_ctx->quic,
                             &srcaddr->sa, packet, &token_buf, &token,
                             &err_desc) != 0) {
    uint8_t payload[QUICLY_MIN_CLIENT_INITIAL_SIZE];
    size_t payload_size = quicly_send_close_invalid_token(
        quic_ctx->quic, packet->version, packet->cid.src,
        packet->cid.dest.encrypted, err_desc, payload);
    if (payload_size != SIZE_MAX) {
      struct iovec vec = {.iov_base = payload, .iov_len = payload_size};
      h2o_quic_send_datagrams(quic_ctx, srcaddr, destaddr, &vec, 1, 0);
    }
    clj_h2o_conn_limit_release();
    return NULL;
  }

  clj_init_http3_conn_callbacks();

  clj_http3_count_accept(quic_ctx);
  h2o_http3_conn_t *conn = h2o_http3_server_accept(
      &ctx->h3_ctx, destaddr, srcaddr, packet, token, &clj_http3_conn_callbacks);

  /* Release if accept failed without creating a connection */
  if (!conn) {
    clj_http3_count_close(quic_ctx);
    clj_h2o_conn_limit_release();
    return NULL;
  }
  /* Not H2O's decryption-failed result: H2O would forward the Initial, whose
     header, packet number and payload the failed decryption has overwritten */
  if (&conn->super == &h2o_quic_accept_conn_decryption_failed) {
    clj_http3_count_close(quic_ctx);
    clj_h2o_conn_limit_release();
    return NULL;
  }
  /* h2o_http3_accept_conn_closed means conn was created then destroyed;
     destroy callback already fired, so don't release again */
  if (conn == &h2o_http3_accept_conn_closed)
    return NULL;

  return &conn->super;
}

static int clj_dup_fd_cloexec(int fd) {
  int dup_fd = dup(fd);
  if (dup_fd == -1)
    return -1;

  if (fcntl(dup_fd, F_SETFD, FD_CLOEXEC) != 0) {
    close(dup_fd);
    return -1;
  }

  return dup_fd;
}

static int clj_set_fd_nonblock(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return -1;

  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

#define CLJ_QUIC_FORWARDED_HEADER_MAX_SIZE (1 + 4 + (1 + 16 + 2) * 2 + 1)
#define CLJ_QUIC_FORWARDED_VERSION 0x91c17000
/* The first datagram of a new flow sent to the worker chosen for it */
#define CLJ_QUIC_FORWARDED_NEW_FLOW_VERSION 0x91c17001

static uint8_t *clj_encode_quic_address(uint8_t *dst, quicly_address_t *addr) {
  switch (addr->sa.sa_family) {
  case AF_INET:
    *dst++ = 4;
    memcpy(dst, &addr->sin.sin_addr.s_addr, 4);
    dst += 4;
    memcpy(dst, &addr->sin.sin_port, 2);
    dst += 2;
    break;
  case AF_INET6:
    *dst++ = 6;
    memcpy(dst, addr->sin6.sin6_addr.s6_addr, 16);
    dst += 16;
    memcpy(dst, &addr->sin6.sin6_port, 2);
    dst += 2;
    break;
  case AF_UNSPEC:
    *dst++ = 0;
    break;
  default:
    return NULL;
  }

  return dst;
}

static int clj_decode_quic_address(quicly_address_t *addr, const uint8_t **src,
                                   const uint8_t *end) {
  memset(addr, 0, sizeof(*addr));

  if (*src >= end)
    return 0;

  switch (*(*src)++) {
  case 4:
    if (end - *src < 6)
      return 0;
    addr->sin.sin_family = AF_INET;
    memcpy(&addr->sin.sin_addr.s_addr, *src, 4);
    *src += 4;
    memcpy(&addr->sin.sin_port, *src, 2);
    *src += 2;
    break;
  case 6:
    if (end - *src < 18)
      return 0;
    addr->sin6.sin6_family = AF_INET6;
    memcpy(addr->sin6.sin6_addr.s6_addr, *src, 16);
    *src += 16;
    memcpy(&addr->sin6.sin6_port, *src, 2);
    *src += 2;
    break;
  case 0:
    addr->sa.sa_family = AF_UNSPEC;
    break;
  default:
    return 0;
  }

  return 1;
}

static size_t clj_encode_quic_forwarded_header(void *buf,
                                               quicly_address_t *destaddr,
                                               quicly_address_t *srcaddr,
                                               uint8_t ttl, int new_flow) {
  uint8_t *dst = buf;

  *dst++ = 0x80;
  dst = quicly_encode32(dst, new_flow ? CLJ_QUIC_FORWARDED_NEW_FLOW_VERSION
                                      : CLJ_QUIC_FORWARDED_VERSION);
  dst = clj_encode_quic_address(dst, destaddr);
  if (dst == NULL)
    return SIZE_MAX;
  dst = clj_encode_quic_address(dst, srcaddr);
  if (dst == NULL)
    return SIZE_MAX;
  *dst++ = ttl;

  return dst - (uint8_t *)buf;
}

static size_t clj_decode_quic_forwarded_header(quicly_address_t *destaddr,
                                               quicly_address_t *srcaddr,
                                               uint8_t *ttl, int *new_flow,
                                               h2o_iovec_t octets) {
  const uint8_t *src = (const uint8_t *)octets.base, *end = src + octets.len;

  if (end - src < 6)
    goto NotForwarded;
  if (*src++ != 0x80)
    goto NotForwarded;
  uint32_t version = quicly_decode32(&src);
  if (version != CLJ_QUIC_FORWARDED_VERSION &&
      version != CLJ_QUIC_FORWARDED_NEW_FLOW_VERSION)
    goto NotForwarded;
  if (new_flow != NULL)
    *new_flow = version == CLJ_QUIC_FORWARDED_NEW_FLOW_VERSION;
  if (!clj_decode_quic_address(destaddr, &src, end))
    goto NotForwarded;
  if (!clj_decode_quic_address(srcaddr, &src, end))
    goto NotForwarded;
  if (end - src < 1)
    goto NotForwarded;
  *ttl = *src++;

  return src - (const uint8_t *)octets.base;

NotForwarded:
  return SIZE_MAX;
}

/* Call with the transport mutex held. The attached workers except `left`,
   plus `extra` unless it is NULL, for lookups without the mutex; NULL when
   allocation fails. */
static clj_http3_worker_set_t *
clj_http3_build_workers(clj_http3_udp_transport_t *transport,
                        clj_http3_worker_load_t *left,
                        clj_http3_worker_load_t *extra) {
  clj_http3_worker_set_t *set =
      calloc(1, sizeof(*set) + (transport->num_attachments + 1) *
                                   sizeof(clj_http3_worker_load_t *));
  if (set == NULL)
    return NULL;
  set->active_node_id = transport->active_node_id;
  for (size_t idx = 0; idx < transport->num_attachments; ++idx) {
    clj_http3_worker_load_t *load = transport->attachments[idx].load;
    if (load != NULL && load != left)
      set->loads[set->num_loads++] = load;
  }
  if (extra != NULL)
    set->loads[set->num_loads++] = extra;
  return set;
}

/* The worker's use of worker sets and of the load records it finds in them
   lies between these two calls */
static void clj_http3_enter_sets(clj_http3_worker_load_t *self) {
  uint64_t marks = atomic_load_explicit(&self->using_sets, memory_order_relaxed);
#if CLJ_DEBUG
  /* A nested use would leave the mark even while in use */
  assert((marks & 1) == 0);
#endif
  atomic_store(&self->using_sets, marks + 1);
}

static void clj_http3_leave_sets(clj_http3_worker_load_t *self) {
  uint64_t marks = atomic_load_explicit(&self->using_sets, memory_order_relaxed);
#if CLJ_DEBUG
  assert((marks & 1) == 1);
#endif
  atomic_store_explicit(&self->using_sets, marks + 1, memory_order_release);
}

/* Call with the transport mutex held, after a change to the set or to a
   descriptor. A worker marks a use odd before it loads the set, and that
   store, the exchange or descriptor store before this call and the loads
   here are sequentially consistent, so a use that saw the old state shows
   its odd mark here. Waiting until the mark changes waits for that use to
   end; its reads happen before anything freed after this returns. */
static void clj_http3_await_workers(clj_http3_udp_transport_t *transport) {
  for (clj_http3_worker_load_t *load = transport->loads; load != NULL;
       load = load->next) {
    uint64_t seen = atomic_load(&load->using_sets);
    while ((seen & 1) != 0 && atomic_load(&load->using_sets) == seen)
      sched_yield();
  }
}

static void clj_http3_free_loads(clj_http3_worker_load_t *load) {
  while (load != NULL) {
    clj_http3_worker_load_t *next = load->next;
    free(load);
    load = next;
  }
}

/* Call with the transport mutex held */
static void clj_http3_publish_workers(clj_http3_udp_transport_t *transport,
                                      clj_http3_worker_set_t *set) {
  clj_http3_worker_set_t *replaced = atomic_exchange(&transport->workers, set);
  clj_http3_await_workers(transport);
  free(replaced);
  clj_http3_free_loads(transport->unlisted);
  transport->unlisted = NULL;
}

static int clj_http3_transport_register_attachment(
    clj_http3_udp_transport_t *transport, uint64_t node_id, uint32_t thread_id,
    int forward_fd, clj_http3_ctx_t *ctx) {
  clj_http3_worker_load_t *load = aligned_alloc(64, sizeof(*load));
  if (load == NULL)
    return 0;
  memset(load, 0, sizeof(*load));
  load->node_id = node_id;
  load->thread_id = thread_id;
  atomic_init(&load->forward_fd, forward_fd);
  if (pthread_mutex_lock(&transport->mutex) != 0) {
    free(load);
    return 0;
  }

  /* A generation id is reused after a failed start; the new worker takes
     the slot, and the old one keeps its record until it unregisters */
  size_t slot = transport->num_attachments;
  for (size_t idx = 0; idx < transport->num_attachments; ++idx) {
    if (transport->attachments[idx].node_id == node_id &&
        transport->attachments[idx].thread_id == thread_id) {
      slot = idx;
      break;
    }
  }

  int new_slot = slot == transport->num_attachments;
  if (new_slot && transport->num_attachments == transport->attachments_capacity) {
    size_t new_capacity =
        transport->attachments_capacity == 0 ? 8 : transport->attachments_capacity * 2;
    clj_http3_transport_attachment_t *new_attachments =
        realloc(transport->attachments,
                new_capacity * sizeof(*new_attachments));
    if (new_attachments == NULL) {
      pthread_mutex_unlock(&transport->mutex);
      free(load);
      return 0;
    }
    transport->attachments = new_attachments;
    transport->attachments_capacity = new_capacity;
  }
  /* Built before anything changes, so a failure leaves no trace */
  clj_http3_worker_set_t *set = clj_http3_build_workers(
      transport, new_slot ? NULL : transport->attachments[slot].load, load);
  if (set == NULL) {
    pthread_mutex_unlock(&transport->mutex);
    free(load);
    return 0;
  }
  if (new_slot)
    transport->num_attachments++;

  load->next = transport->loads;
  transport->loads = load;
  transport->attachments[slot] = (clj_http3_transport_attachment_t){
      .node_id = node_id,
      .thread_id = thread_id,
      .ctx = ctx,
      .load = load,
  };
  if (ctx != NULL)
    ctx->load = load;
  clj_http3_publish_workers(transport, set);
  pthread_mutex_unlock(&transport->mutex);
  return 1;
}

/* Call on the worker's thread, or while its loop does not run. Returns once
   no other worker can still write to its forwarding channel or read its load
   record, which is then freed. */
static void clj_http3_transport_unregister_attachment(
    clj_http3_udp_transport_t *transport, clj_http3_ctx_t *ctx) {
  clj_http3_worker_load_t *load = ctx->load;
  if (load == NULL || pthread_mutex_lock(&transport->mutex) != 0)
    return;

  for (clj_http3_worker_load_t **link = &transport->loads; *link != NULL;
       link = &(*link)->next) {
    if (*link == load) {
      *link = load->next;
      break;
    }
  }
  atomic_store(&load->forward_fd, -1);
  int listed = 0;
  clj_http3_worker_set_t *set = NULL;
  for (size_t idx = 0; idx < transport->num_attachments; ++idx) {
    clj_http3_transport_attachment_t *attachment = transport->attachments + idx;
    if (attachment->load == load) {
      set = clj_http3_build_workers(transport, load, NULL);
      memmove(attachment, attachment + 1,
              (transport->num_attachments - idx - 1) * sizeof(*attachment));
      transport->num_attachments--;
      listed = 1;
      break;
    }
  }
  if (set != NULL)
    clj_http3_publish_workers(transport, set);
  else
    clj_http3_await_workers(transport);
  if (listed && set == NULL) {
    /* The current set still names the worker; with its descriptor -1 it is
       neither chosen nor written to */
    load->next = transport->unlisted;
    transport->unlisted = load;
  } else {
    free(load);
  }
  ctx->load = NULL;
  pthread_mutex_unlock(&transport->mutex);
}

static clj_http3_worker_load_t *
clj_http3_find_worker(clj_http3_worker_set_t *set, uint64_t node_id,
                      uint32_t thread_id) {
  if (set == NULL)
    return NULL;
  for (size_t idx = 0; idx < set->num_loads; ++idx) {
    if (set->loads[idx]->node_id == node_id &&
        set->loads[idx]->thread_id == thread_id)
      return set->loads[idx];
  }
  return NULL;
}

enum { CLJ_HTTP3_SENT = 1, CLJ_HTTP3_CHANNEL_FULL = 0, CLJ_HTTP3_GONE = -1 };

/* Writes an envelope and a datagram to the worker's forwarding channel */
static int clj_http3_send_to_worker(clj_http3_worker_load_t *target,
                                    quicly_address_t *destaddr,
                                    quicly_address_t *srcaddr, uint8_t ttl,
                                    int new_flow, const uint8_t *datagram,
                                    size_t len) {
  char header_buf[CLJ_QUIC_FORWARDED_HEADER_MAX_SIZE];
  if (ttl == 0)
    return CLJ_HTTP3_GONE;
  size_t header_len = clj_encode_quic_forwarded_header(
      header_buf, destaddr, srcaddr, ttl - 1, new_flow);
  if (header_len == SIZE_MAX)
    return CLJ_HTTP3_GONE;
  struct iovec vec[2] = {{header_buf, header_len}, {(uint8_t *)datagram, len}};

  int fd = atomic_load(&target->forward_fd);
  if (fd == -1)
    return CLJ_HTTP3_GONE;
  if (writev(fd, vec, 2) >= 0)
    return CLJ_HTTP3_SENT;
  if (errno == EAGAIN || errno == EWOULDBLOCK)
    return CLJ_HTTP3_CHANNEL_FULL;
  return CLJ_HTTP3_GONE;
}

static int clj_http3_forward(clj_http3_ctx_t *ctx,
                             clj_http3_worker_load_t *target,
                             quicly_address_t *destaddr,
                             quicly_address_t *srcaddr, uint8_t ttl,
                             int new_flow, const uint8_t *datagram,
                             size_t len) {
  int sent = clj_http3_send_to_worker(target, destaddr, srcaddr, ttl, new_flow,
                                      datagram, len);
  if (sent == CLJ_HTTP3_SENT)
    clj_http3_count_forward(ctx);
  return sent;
}

static int clj_http3_may_choose(clj_http3_worker_set_t *set,
                                clj_http3_worker_load_t *load) {
  return load != NULL && load->node_id == set->active_node_id &&
         atomic_load(&load->accepting) && !atomic_load(&load->withdrawn) &&
         atomic_load(&load->forward_fd) != -1;
}

static clj_http3_worker_load_t *
clj_http3_forward_target(clj_http3_ctx_t *ctx, clj_http3_worker_set_t *set,
                         const uint64_t *node_id, uint32_t thread_id) {
  if (set == NULL)
    return NULL;
  if (node_id != NULL)
    return clj_http3_find_worker(set, *node_id, thread_id);
  /* An Initial or 0-RTT packet this worker does not accept goes to the
     active generation; the thread matters only once a CID names it */
  if (atomic_load(&ctx->load->accepting))
    return NULL;
  clj_http3_worker_load_t *target =
      clj_http3_find_worker(set, set->active_node_id, thread_id);
  for (size_t idx = 0;
       !clj_http3_may_choose(set, target) && idx < set->num_loads; ++idx)
    target = set->loads[idx];
  if (!clj_http3_may_choose(set, target) || target == ctx->load)
    return NULL;
  return target;
}

static int clj_forward_quic_packets(h2o_quic_ctx_t *h3ctx,
                                    const uint64_t *node_id,
                                    uint32_t thread_id,
                                    quicly_address_t *destaddr,
                                    quicly_address_t *srcaddr, uint8_t ttl,
                                    quicly_decoded_packet_t *packets,
                                    size_t num_packets) {
  clj_http3_ctx_t *ctx = H2O_STRUCT_FROM_MEMBER(clj_http3_ctx_t, h3_ctx.super,
                                                h3ctx);
  clj_http3_worker_load_t *self = ctx->load;

  if (ttl == 0 || self == NULL ||
      (node_id != NULL && *node_id == ctx->node_id &&
       thread_id == ctx->thread_id))
    return 0;

  clj_http3_enter_sets(self);
  clj_http3_worker_load_t *target = clj_http3_forward_target(
      ctx, atomic_load(&ctx->transport->workers), node_id, thread_id);
  int all_sent = target != NULL;
  for (size_t idx = 0; target != NULL && idx != num_packets; ++idx) {
    if (clj_http3_forward(ctx, target, destaddr, srcaddr, ttl, 0,
                          packets[idx].octets.base,
                          packets[idx].octets.len) != CLJ_HTTP3_SENT)
      all_sent = 0;
  }
  clj_http3_leave_sets(self);

  return all_sent;
}

static uint64_t clj_http3_load_of(clj_http3_worker_load_t *load) {
  return (uint64_t)atomic_load(&load->conns) + atomic_load(&load->pending);
}

/* The accepting worker of the active generation with the fewest QUIC
   connections and pending flows takes a new flow. The deciding worker wins a
   tie; otherwise a cursor shared by the transport's workers spreads ties. */
static clj_http3_worker_load_t *clj_http3_choose_worker(clj_http3_ctx_t *ctx,
                                                       uint64_t turn) {
  clj_http3_worker_set_t *set = atomic_load(&ctx->transport->workers);
  clj_http3_worker_load_t *chosen = NULL;
  uint64_t min_load = UINT64_MAX,
           own_kept = ctx->kept_at == turn ? ctx->kept : 0;
  size_t num_min = 0;

  if (set == NULL)
    return NULL;
  for (size_t idx = 0; idx < set->num_loads; ++idx) {
    clj_http3_worker_load_t *load = set->loads[idx];
    if (!clj_http3_may_choose(set, load))
      continue;
    uint64_t load_now =
        clj_http3_load_of(load) + (load == ctx->load ? own_kept : 0);
    if (load_now < min_load) {
      min_load = load_now;
      num_min = 0;
      chosen = load;
    }
    if (load_now == min_load)
      ++num_min;
  }
  if (clj_http3_may_choose(set, ctx->load) &&
      clj_http3_load_of(ctx->load) + own_kept <= min_load)
    return ctx->load;
  if (num_min > 1) {
    size_t skip = atomic_fetch_add(&ctx->transport->spread_cursor, 1) % num_min;
    for (size_t idx = 0; idx < set->num_loads; ++idx) {
      clj_http3_worker_load_t *load = set->loads[idx];
      if (load != ctx->load && clj_http3_may_choose(set, load) &&
          clj_http3_load_of(load) <= min_load && skip-- == 0)
        return load;
    }
  }
  return chosen;
}

static clj_http3_new_flow_t *
clj_http3_find_new_flow(clj_http3_ctx_t *ctx, uint64_t now,
                        quicly_address_t *srcaddr, const uint8_t *dcid,
                        size_t dcid_len) {
  for (size_t idx = 0; idx < CLJ_HTTP3_NEW_FLOWS; ++idx) {
    clj_http3_new_flow_t *flow = ctx->new_flows + idx;
    if (flow->expires_at > now && flow->dcid_len == dcid_len &&
        memcmp(flow->dcid, dcid, dcid_len) == 0 &&
        h2o_socket_compare_address(&flow->srcaddr.sa, &srcaddr->sa, 1) == 0)
      return flow;
  }
  return NULL;
}

/* An expired entry, or NULL while every entry is live */
static clj_http3_new_flow_t *clj_http3_free_flow(clj_http3_ctx_t *ctx,
                                                 uint64_t now) {
  for (size_t idx = 0; idx < CLJ_HTTP3_NEW_FLOWS; ++idx) {
    if (ctx->new_flows[idx].expires_at <= now)
      return ctx->new_flows + idx;
  }
  return NULL;
}

/* The worker that issued `dcid`, in any generation attached to the transport */
static clj_http3_worker_load_t *clj_http3_cid_worker(clj_http3_ctx_t *ctx,
                                                     const uint8_t *dcid,
                                                     size_t dcid_len) {
  quicly_cid_encryptor_t *encryptor = ctx->h3_ctx.super.quic->cid_encryptor;
  quicly_cid_plaintext_t plaintext;
  if (encryptor == NULL || dcid_len == 0 ||
      encryptor->decrypt_cid(encryptor, &plaintext, dcid, dcid_len) == SIZE_MAX)
    return NULL;
  return clj_http3_find_worker(atomic_load(&ctx->transport->workers),
                               plaintext.node_id, plaintext.thread_id);
}

static void clj_http3_keep_flow(clj_http3_ctx_t *ctx, uint64_t turn) {
  if (ctx->kept_at != turn) {
    ctx->kept_at = turn;
    ctx->kept = 0;
  }
  ++ctx->kept;
}

static void clj_http3_flow_to(clj_http3_new_flow_t *flow,
                              clj_http3_worker_load_t *target) {
  flow->target_node = target->node_id;
  flow->target_thread = target->thread_id;
}

static int clj_http3_steer_in_sets(clj_http3_ctx_t *ctx, uint8_t type,
                                   struct iovec *datagram,
                                   quicly_address_t *destaddr,
                                   quicly_address_t *srcaddr, uint8_t ttl) {
  const uint8_t *bytes = datagram->iov_base;
  size_t len = datagram->iov_len;
  const uint8_t *src = bytes + 1;
  if (!quicly_is_supported_version(quicly_decode32(&src)))
    return 1;
  size_t dcid_len = *src++;
  if (dcid_len > QUICLY_MAX_CID_LEN_V1 || 6 + dcid_len > len)
    return 1;
  const uint8_t *dcid = src;

  uint64_t now = clj_monotonic_millis();
  clj_http3_new_flow_t *flow =
      clj_http3_find_new_flow(ctx, now, srcaddr, dcid, dcid_len);
  if (flow != NULL) {
    if (flow->target_node == ctx->node_id &&
        flow->target_thread == ctx->thread_id)
      return 1;
    clj_http3_worker_load_t *target =
        clj_http3_find_worker(atomic_load(&ctx->transport->workers),
                              flow->target_node, flow->target_thread);
    if (target == NULL || clj_http3_forward(ctx, target, destaddr, srcaddr,
                                            ttl, 0, bytes, len) == CLJ_HTTP3_GONE) {
      /* The worker is gone, and its connections with it */
      clj_http3_flow_to(flow, ctx->load);
      return 1;
    }
    /* Sent, or dropped while the channel is full; the client sends it again
       and the channel drains as the worker reads */
    return 0;
  }

  /* Initial and 0-RTT datagrams that carry a server CID go to the worker
     that issued it. H2O drops such 0-RTT packets, and forwards such Initials
     only after a failed decryption has removed their header protection in
     place, so the receiving worker cannot decrypt them either. */
  clj_http3_worker_load_t *issuer = clj_http3_cid_worker(ctx, dcid, dcid_len);
  if (issuer != NULL)
    return issuer == ctx->load ||
           clj_http3_forward(ctx, issuer, destaddr, srcaddr, ttl, 0, bytes,
                                    len) != CLJ_HTTP3_SENT;

  /* Choose only for datagrams quicly_accept takes */
  if (type != QUICLY_PACKET_TYPE_INITIAL ||
      len < QUICLY_MIN_CLIENT_INITIAL_SIZE ||
      dcid_len < QUICLY_MIN_INITIAL_DCID_LEN || 7 + dcid_len > len ||
      dcid[dcid_len] > QUICLY_MAX_CID_LEN_V1)
    return 1;
  if (!atomic_load(&ctx->load->accepting) ||
      (flow = clj_http3_free_flow(ctx, now)) == NULL)
    return 1;
  /* The loop's time changes only between loop turns, so it marks the receive
     batch whose accepts are still to come */
  uint64_t turn = h2o_now(ctx->h3_ctx.super.loop);
  /* With every connected socket in use a moved connection would be forwarded
     for its whole life; the kernel's choice is even at such numbers */
  clj_http3_worker_load_t *target =
      CLJ_HTTP3_CONNECTED_SOCKETS &&
              atomic_load(&ctx->transport->num_connected) >=
                  CLJ_HTTP3_CONNECTED_MAX
          ? ctx->load
          : clj_http3_choose_worker(ctx, turn);
  if (target == NULL)
    return 1;
  *flow = (clj_http3_new_flow_t){.expires_at = now + CLJ_HTTP3_NEW_FLOW_MILLIS,
                                 .srcaddr = *srcaddr,
                                 .dcid_len = (uint8_t)dcid_len};
  clj_http3_flow_to(flow, target);
  memcpy(flow->dcid, dcid, dcid_len);
  if (target != ctx->load) {
    /* Added before the write, since the target takes it back on reading */
    atomic_fetch_add(&target->pending, 1);
    if (clj_http3_forward(ctx, target, destaddr, srcaddr, ttl, 1, bytes,
                                 len) == CLJ_HTTP3_SENT)
      return 0;
    /* The channel is full or gone: accept here */
    clj_http3_take_pending(target);
    clj_http3_flow_to(flow, ctx->load);
  }
  clj_http3_keep_flow(ctx, turn);
  return 1;
}

/* Sends the first Initial of a new flow to the chosen worker, and the flow's
   later Initial and 0-RTT datagrams after it. Returns 0 when the datagram
   went to another worker or is to be dropped. */
static int clj_http3_steer_new_flow(clj_http3_ctx_t *ctx,
                                    struct iovec *datagram,
                                    quicly_address_t *destaddr,
                                    quicly_address_t *srcaddr, uint8_t ttl) {
  const uint8_t *bytes = datagram->iov_base;
  clj_http3_worker_load_t *self = ctx->load;
  if (datagram->iov_len < 7 || self == NULL)
    return 1;
  uint8_t type = bytes[0] & QUICLY_PACKET_TYPE_BITMASK;
  if (type != QUICLY_PACKET_TYPE_INITIAL && type != QUICLY_PACKET_TYPE_0RTT)
    return 1;
  clj_http3_enter_sets(self);
  int keep =
      clj_http3_steer_in_sets(ctx, type, datagram, destaddr, srcaddr, ttl);
  clj_http3_leave_sets(self);
  return keep;
}

static int clj_rewrite_forwarded_quic_datagram(h2o_quic_ctx_t *h3ctx,
                                               struct msghdr *msg,
                                               quicly_address_t *destaddr,
                                               quicly_address_t *srcaddr,
                                               uint8_t *ttl) {
  struct {
    quicly_address_t destaddr, srcaddr;
    uint8_t ttl;
    size_t offset;
    int new_flow;
  } encapsulated;
  clj_http3_ctx_t *ctx =
      H2O_STRUCT_FROM_MEMBER(clj_http3_ctx_t, h3_ctx.super, h3ctx);

  if (msg->msg_iovlen != 1)
    return 1;
  /* Datagrams from the public socket keep their real sender. A connected
     socket is public too; between its bind and its connect it also takes
     other clients' datagrams, which are treated as on the listener's
     socket. */
  if (ctx->reading == CLJ_HTTP3_READING_PUBLIC ||
      (ctx->reading == CLJ_HTTP3_READING_CONNECTED &&
       h2o_socket_compare_address(&srcaddr->sa, &ctx->reading_peer.sa, 1) !=
           0)) {
    /* A datagram sent to another worker is cut to one byte, which H2O fails
       to decode and skips. Returning 0 would make H2O process the datagrams
       grouped before it in the batch with its addresses. */
    if (!clj_http3_steer_new_flow(ctx, msg->msg_iov, destaddr, srcaddr, *ttl))
      msg->msg_iov[0].iov_len = 1;
    return 1;
  }
  if (ctx->reading == CLJ_HTTP3_READING_CONNECTED)
    return 1;

  encapsulated.offset =
      clj_decode_quic_forwarded_header(&encapsulated.destaddr,
                                       &encapsulated.srcaddr,
                                       &encapsulated.ttl,
                                       &encapsulated.new_flow,
                                       h2o_iovec_init(msg->msg_iov[0].iov_base,
                                                      msg->msg_iov[0].iov_len));
  if (encapsulated.offset == SIZE_MAX)
    return 1;
  if (encapsulated.new_flow && ctx->load != NULL)
    clj_http3_take_pending(ctx->load);

  if (encapsulated.srcaddr.sa.sa_family != AF_INET &&
      encapsulated.srcaddr.sa.sa_family != AF_INET6)
    return 0;

  switch (encapsulated.destaddr.sa.sa_family) {
  case AF_UNSPEC:
    break;
  case AF_INET:
    if (encapsulated.destaddr.sin.sin_port != *h3ctx->sock.port)
      return 1;
    break;
  case AF_INET6:
    if (encapsulated.destaddr.sin6.sin6_port != *h3ctx->sock.port)
      return 1;
    break;
  default:
    return 1;
  }

  msg->msg_iov[0].iov_base =
      (char *)msg->msg_iov[0].iov_base + encapsulated.offset;
  msg->msg_iov[0].iov_len -= encapsulated.offset;
  *destaddr = encapsulated.destaddr;
  *srcaddr = encapsulated.srcaddr;
  /* Forwarding envelopes are unauthenticated; cap their hop limit. */
  *ttl = encapsulated.ttl < h3ctx->default_ttl ? encapsulated.ttl
                                            : h3ctx->default_ttl;

  return 1;
}

static void clj_forwarded_quic_socket_on_read(h2o_socket_t *sock,
                                              const char *err) {
  clj_http3_ctx_t *ctx = sock->data;

  if (err != NULL)
    return;

  ctx->reading = CLJ_HTTP3_READING_FORWARDED;
  clj_http3_set_conn_update(ctx);
  h2o_quic_read_socket(&ctx->h3_ctx.super, sock);
  ctx->reading = CLJ_HTTP3_READING_PUBLIC;
  clj_http3_set_conn_update(ctx);
}

static int clj_h2o_http3_open_udp_fd(const char *host, uint16_t port) {
  char servname[6];
  struct addrinfo hints, *res = NULL, *ai = NULL;
  const char *lookup_host = host != NULL && host[0] != '\0' ? host : NULL;
  int gai_err, fd = -1, saved_errno = EADDRNOTAVAIL;
  int optval = 1;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_protocol = IPPROTO_UDP;
  hints.ai_flags = AI_NUMERICSERV | AI_PASSIVE;

  snprintf(servname, sizeof(servname), "%u", (unsigned)port);
  gai_err = getaddrinfo(lookup_host, servname, &hints, &res);
  if (gai_err != 0) {
    if (gai_err != EAI_SYSTEM)
      errno = EINVAL;
    DEBUG_LOG("getaddrinfo(UDP) failed for %s:%u",
              lookup_host ? lookup_host : "*", (unsigned)port);
    return -1;
  }

  for (ai = res; ai != NULL; ai = ai->ai_next) {
    fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd == -1) {
      saved_errno = errno;
      continue;
    }

#ifdef IPV6_V6ONLY
    if (ai->ai_family == AF_INET6) {
      int flag = 1;
      if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &flag, sizeof(flag)) != 0)
        goto Next;
    }
#endif

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) != 0)
      goto Next;
#ifdef SO_REUSEPORT
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) != 0)
      goto Next;
#endif

#ifdef IP_PKTINFO
    if (ai->ai_family == AF_INET &&
        setsockopt(fd, IPPROTO_IP, IP_PKTINFO, &optval, sizeof(optval)) != 0)
      goto Next;
#endif
#ifdef IPV6_RECVPKTINFO
    if (ai->ai_family == AF_INET6 &&
        setsockopt(fd, IPPROTO_IPV6, IPV6_RECVPKTINFO, &optval,
                   sizeof(optval)) != 0)
      goto Next;
#endif

    if (bind(fd, ai->ai_addr, ai->ai_addrlen) != 0)
      goto Next;

    h2o_socket_set_df_bit(fd, ai->ai_family);
    freeaddrinfo(res);
    return fd;

  Next:
    saved_errno = errno;
    close(fd);
    fd = -1;
  }

  freeaddrinfo(res);
  errno = saved_errno;
  DEBUG_LOG("failed to open UDP socket for %s:%u: %s",
            lookup_host ? lookup_host : "*", (unsigned)port, strerror(errno));
  return -1;
}

static clj_http3_ctx_t *
clj_h2o_http3_create_worker_ctx_from_fd(h2o_context_t *h2o_ctx,
                                        h2o_evloop_t *loop,
                                        quicly_context_t *quic_ctx,
                                        h2o_hostconf_t **hosts, int fd,
                                        uint64_t node_id,
                                        uint32_t thread_id) {
  if (!h2o_ctx || !loop || !quic_ctx || !hosts || fd < 0)
    return NULL;

  clj_http3_ctx_t *ctx = calloc(1, sizeof(clj_http3_ctx_t));
  if (!ctx)
    return NULL;

  ctx->forward_fd = -1;

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
      .node_id = node_id,
  };
  ctx->node_id = node_id;

  ctx->accept_ctx.ctx = h2o_ctx;
  ctx->accept_ctx.hosts = hosts;

  h2o_http3_server_init_context(h2o_ctx, &ctx->h3_ctx.super, loop, ctx->udp_sock,
                                NULL, quic_ctx, &ctx->next_cid, NULL,
                                NULL, h2o_ctx->globalconf->http3.use_gso);

  ctx->h3_ctx.accept_ctx = &ctx->accept_ctx;

  return ctx;
}

clj_http3_udp_transport_t *clj_h2o_http3_open_udp_transport(const char *host,
                                                            uint16_t port) {
  clj_http3_udp_transport_t *transport = calloc(1, sizeof(*transport));
  if (!transport)
    return NULL;

  transport->reservation_fd = -1;
  if (pthread_mutex_init(&transport->mutex, NULL) != 0) {
    free(transport);
    return NULL;
  }
  if (host != NULL && host[0] != '\0') {
    transport->host = strdup(host);
    if (transport->host == NULL) {
      pthread_mutex_destroy(&transport->mutex);
      free(transport);
      return NULL;
    }
  }
  transport->port = port;
  transport->reservation_fd = clj_h2o_http3_open_udp_fd(host, port);
  if (transport->reservation_fd == -1) {
    free(transport->host);
    pthread_mutex_destroy(&transport->mutex);
    free(transport);
    return NULL;
  }

  return transport;
}

clj_http3_ctx_t *
clj_h2o_http3_attach_udp_transport(h2o_context_t *h2o_ctx, h2o_evloop_t *loop,
                                   quicly_context_t *quic_ctx,
                                   h2o_hostconf_t **hosts,
                                   clj_http3_udp_transport_t *transport,
                                   clj_ticket_manager_t *ticket_mgr,
                                   uint64_t node_id, uint32_t thread_id) {
  clj_http3_ctx_t *ctx;
  int fds[2];

  if (!transport)
    return NULL;

  int lock_rc = pthread_mutex_lock(&transport->mutex);
  if (lock_rc != 0) {
    DEBUG_LOG("pthread_mutex_lock(transport) failed: %d", lock_rc);
    return NULL;
  }

  if (!transport->cid_key_initialized) {
    ptls_openssl_random_bytes(transport->cid_key, sizeof(transport->cid_key));
    transport->cid_key_initialized = 1;
  }
  if (transport->reservation_fd != -1) {
    close(transport->reservation_fd);
    transport->reservation_fd = -1;
  }
  pthread_mutex_unlock(&transport->mutex);

  if (!clj_quicly_wrapper_set_cid_key(quic_ctx, transport->cid_key,
                                      sizeof(transport->cid_key))) {
    DEBUG_LOG("failed to set shared CID key on QUIC context");
    return NULL;
  }

  int worker_fd = clj_h2o_http3_open_udp_fd(transport->host, transport->port);
  if (worker_fd == -1) {
    DEBUG_LOG("open(worker UDP listener) failed: %s", strerror(errno));
    return NULL;
  }

  ctx = clj_h2o_http3_create_worker_ctx_from_fd(h2o_ctx, loop, quic_ctx, hosts,
                                                worker_fd, node_id, thread_id);
  if (ctx == NULL)
    return NULL;

  ctx->address_token.mgr = ticket_mgr;

  if (socketpair(AF_UNIX, SOCK_DGRAM, 0, fds) != 0) {
    clj_address_token_keys_dispose(&ctx->address_token);
    h2o_quic_dispose_context(&ctx->h3_ctx.super);
    free(ctx);
    return NULL;
  }
  if (fcntl(fds[0], F_SETFD, FD_CLOEXEC) != 0 ||
      fcntl(fds[1], F_SETFD, FD_CLOEXEC) != 0 ||
      clj_set_fd_nonblock(fds[1]) != 0) {
    close(fds[0]);
    close(fds[1]);
    clj_address_token_keys_dispose(&ctx->address_token);
    h2o_quic_dispose_context(&ctx->h3_ctx.super);
    free(ctx);
    return NULL;
  }

  ctx->transport = transport;
  ctx->thread_id = thread_id;
  ctx->forward_fd = fds[1];
  ctx->forwarded_sock =
      h2o_evloop_socket_create(loop, fds[0], H2O_SOCKET_FLAG_DONT_READ);
  if (ctx->forwarded_sock == NULL ||
      !clj_http3_transport_register_attachment(transport, node_id, thread_id,
                                               ctx->forward_fd, ctx)) {
    if (ctx->forwarded_sock == NULL)
      close(fds[0]);
    if (ctx->forwarded_sock != NULL)
      h2o_socket_close(ctx->forwarded_sock);
    close(ctx->forward_fd);
    clj_address_token_keys_dispose(&ctx->address_token);
    h2o_quic_dispose_context(&ctx->h3_ctx.super);
    free(ctx);
    return NULL;
  }

  ctx->forwarded_sock->data = ctx;
  h2o_socket_read_start(ctx->forwarded_sock, clj_forwarded_quic_socket_on_read);
  h2o_quic_set_forwarding_context(&ctx->h3_ctx.super, 0, 4,
                                  clj_forward_quic_packets,
                                  clj_rewrite_forwarded_quic_datagram);

  return ctx;
}

void clj_h2o_http3_activate_udp_transport_generation(
    clj_http3_udp_transport_t *transport, uint64_t node_id) {
  if (transport == NULL)
    return;

  if (pthread_mutex_lock(&transport->mutex) != 0)
    return;

  transport->active_node_id = node_id;
  for (size_t idx = 0; idx < transport->num_attachments; ++idx) {
    clj_http3_transport_attachment_t *attachment = transport->attachments + idx;
    if (attachment->ctx != NULL) {
      attachment->ctx->h3_ctx.super.acceptor =
          attachment->node_id == node_id ? clj_on_http3_accept : NULL;
    }
    if (attachment->load != NULL)
      atomic_store(&attachment->load->accepting, attachment->node_id == node_id);
  }
  /* Without a new set no worker is eligible for new flows: the old set names
     the previous generation, which no longer accepts */
  clj_http3_worker_set_t *set = clj_http3_build_workers(transport, NULL, NULL);
  if (set != NULL)
    clj_http3_publish_workers(transport, set);

  pthread_mutex_unlock(&transport->mutex);
}

void clj_h2o_http3_release_udp_transport(clj_http3_udp_transport_t *transport) {
  if (!transport)
    return;

  if (transport->reservation_fd != -1)
    close(transport->reservation_fd);
  clj_http3_free_loads(transport->loads);
  clj_http3_free_loads(transport->unlisted);
  free(atomic_load(&transport->workers));
  free(transport->host);
  free(transport->attachments);
  pthread_mutex_destroy(&transport->mutex);
  free(transport);
}

void clj_h2o_http3_stop_accepting(clj_http3_ctx_t *ctx) {
  if (!ctx)
    return;

  /* Following h2o main.c pattern (line 4572): set acceptor to NULL to stop
     accepting new HTTP/3 connections. New Initial packets will be rejected
     with version negotiation. Existing connections (including handshakes in
     progress) continue to completion. */
  ctx->h3_ctx.super.acceptor = NULL;
  if (ctx->load != NULL)
    atomic_store(&ctx->load->accepting, 0);
}

void clj_h2o_http3_withdraw_worker(clj_http3_ctx_t *ctx) {
  if (ctx != NULL && ctx->load != NULL)
    atomic_store(&ctx->load->withdrawn, 1);
}

size_t clj_h2o_http3_num_connections(clj_http3_ctx_t *ctx) {
  if (!ctx)
    return 0;
  return h2o_quic_num_connections(&ctx->h3_ctx.super);
}

/* Call with the transport mutex held; NULL once the worker's context is
   disposed */
static clj_http3_worker_load_t *
clj_http3_lookup_load(clj_http3_udp_transport_t *transport, uint64_t node_id,
                      uint32_t thread_id) {
  for (clj_http3_worker_load_t *load = transport->loads; load != NULL;
       load = load->next) {
    if (load->node_id == node_id && load->thread_id == thread_id)
      return load;
  }
  return NULL;
}

uint64_t clj_h2o_http3_forwarded_datagrams(clj_http3_udp_transport_t *transport,
                                           uint64_t node_id,
                                           uint32_t thread_id) {
  uint64_t forwarded = 0;
  if (transport == NULL || pthread_mutex_lock(&transport->mutex) != 0)
    return 0;
  clj_http3_worker_load_t *load =
      clj_http3_lookup_load(transport, node_id, thread_id);
  if (load != NULL)
    forwarded = atomic_load(&load->forwarded);
  pthread_mutex_unlock(&transport->mutex);
  return forwarded;
}

uint32_t clj_h2o_http3_connected_sockets(clj_http3_udp_transport_t *transport,
                                         uint64_t node_id, uint32_t thread_id) {
  uint32_t connected = 0;
  if (transport == NULL || pthread_mutex_lock(&transport->mutex) != 0)
    return 0;
  clj_http3_worker_load_t *load =
      clj_http3_lookup_load(transport, node_id, thread_id);
  if (load != NULL)
    connected = atomic_load(&load->connected);
  pthread_mutex_unlock(&transport->mutex);
  return connected;
}

void clj_h2o_http3_dispose_worker_ctx(clj_http3_ctx_t *ctx) {
  if (!ctx)
    return;

  while (ctx->num_connected != 0)
    clj_http3_close_connected(ctx, ctx->connected);

  if (ctx->transport != NULL)
    clj_http3_transport_unregister_attachment(ctx->transport, ctx);
  if (ctx->forwarded_sock != NULL)
    h2o_socket_close(ctx->forwarded_sock);
  if (ctx->forward_fd != -1)
    close(ctx->forward_fd);

  /* h2o_quic_dispose_context closes the socket internally */
  h2o_quic_dispose_context(&ctx->h3_ctx.super);
  clj_address_token_keys_dispose(&ctx->address_token);

  free(ctx);
}

void clj_h2o_http3_detach_udp_transport(clj_http3_ctx_t *ctx) {
  clj_h2o_http3_dispose_worker_ctx(ctx);
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
    atomic_init(&mgr->generation, 1);

    mgr->keys = malloc(sizeof(*mgr->keys));
    if (!mgr->keys) {
        pthread_rwlock_destroy(&mgr->rwlock);
        free(mgr);
        return NULL;
    }
    /* Generate initial key so server is ready immediately */
    generate_random_key(mgr->keys, current_time_ms(), ticket_lifetime_seconds);
    mgr->num_keys = 1;
    mgr->capacity = 1;

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
    atomic_fetch_add(&mgr->generation, 1);

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

int clj_ptls_ctx_set_tickets(
    ptls_context_t *ctx,
    clj_encrypt_ticket_t *encrypt_ticket,
    uint32_t ticket_lifetime,
    uint32_t max_early_data_size)
{
    if (!ctx) return -1;

    clj_ptls_wrapper_t *wrapper =
        H2O_STRUCT_FROM_MEMBER(clj_ptls_wrapper_t, ctx, ctx);
    /* Identity cache entries copy the context, so a replaced callback would
       stay in use by them */
    if (wrapper->encrypt_ticket != NULL) return -1;

    if (encrypt_ticket) {
        wrapper->encrypt_ticket = encrypt_ticket;
        ctx->encrypt_ticket = &encrypt_ticket->super;
    }
    ctx->ticket_lifetime = ticket_lifetime;
    ctx->max_early_data_size = max_early_data_size;

    /* Send 2 NewSessionTicket messages (like Cloudflare).
       This gives client redundancy and curl seems to need 2 before sending early data. */
    ctx->ticket_requests.server.max_count = 2;
    return 0;
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
