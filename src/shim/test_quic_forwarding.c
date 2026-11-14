/* Exercise the private callbacks without adding test-only shim exports. */
#include "shim.c"
#include <dirent.h>
#include <netinet/ip.h>
#include <sys/resource.h>

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);          \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

#include "test_heap.h"

/* Why heap checks are skipped, or NULL */
static const char *heap_skip;

static quicly_address_t address(int family, uint16_t port) {
  quicly_address_t addr = {0};
  addr.sa.sa_family = family;
  if (family == AF_INET) {
    addr.sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin.sin_port = htons(port);
  } else if (family == AF_INET6) {
    addr.sin6.sin6_addr = in6addr_loopback;
    addr.sin6.sin6_port = htons(port);
  }
  return addr;
}

static void test_rewrite(int family, uint8_t incoming_ttl,
                         uint8_t expected_ttl) {
  quicly_address_t dest = address(family, 443), src = address(family, 12345);
  uint16_t port = htons(443);
  clj_http3_ctx_t ctx = {.h3_ctx.super.default_ttl = 4,
                         .reading = CLJ_HTTP3_READING_FORWARDED};
  ctx.h3_ctx.super.sock.port = &port;
  char buffer[256];
  size_t offset =
      clj_encode_quic_forwarded_header(buffer, &dest, &src, incoming_ttl, 0);
  CHECK(offset != SIZE_MAX);
  memcpy(buffer + offset, "packet", 6);
  struct iovec vec = {buffer, offset + 6};
  struct msghdr msg = {.msg_iov = &vec, .msg_iovlen = 1};
  quicly_address_t decoded_dest = {0}, decoded_src = {0};
  uint8_t ttl = 99;
  CHECK(clj_rewrite_forwarded_quic_datagram(&ctx.h3_ctx.super, &msg, &decoded_dest,
                                            &decoded_src, &ttl) == 1);
  CHECK(ttl == expected_ttl);
  CHECK(vec.iov_base == buffer + offset && vec.iov_len == 6);
  CHECK(memcmp(vec.iov_base, "packet", 6) == 0);
  CHECK(decoded_dest.sa.sa_family == family);
  CHECK(decoded_src.sa.sa_family == family);
}

static void test_invalid_source(void) {
  quicly_address_t dest = address(AF_INET, 443), src = address(AF_UNSPEC, 0);
  uint16_t port = htons(443);
  clj_http3_ctx_t ctx = {.h3_ctx.super.default_ttl = 4,
                         .reading = CLJ_HTTP3_READING_FORWARDED};
  ctx.h3_ctx.super.sock.port = &port;
  char buffer[256];
  size_t len = clj_encode_quic_forwarded_header(buffer, &dest, &src, 4, 0);
  CHECK(len != SIZE_MAX);
  struct iovec vec = {buffer, len};
  struct msghdr msg = {.msg_iov = &vec, .msg_iovlen = 1};
  uint8_t ttl = 4;
  CHECK(clj_rewrite_forwarded_quic_datagram(&ctx.h3_ctx.super, &msg, &dest, &src, &ttl) ==
        0);
  CHECK(vec.iov_base == buffer && vec.iov_len == len);
}

static void test_other_datagrams(void) {
  quicly_address_t dest = address(AF_INET, 444), src = address(AF_INET, 12345);
  uint16_t port = htons(443);
  clj_http3_ctx_t ctx = {.h3_ctx.super.default_ttl = 4,
                         .reading = CLJ_HTTP3_READING_FORWARDED};
  ctx.h3_ctx.super.sock.port = &port;
  char buffer[256];
  size_t len = clj_encode_quic_forwarded_header(buffer, &dest, &src, 255, 0);
  CHECK(len != SIZE_MAX);
  struct iovec vec = {buffer, len};
  struct msghdr msg = {.msg_iov = &vec, .msg_iovlen = 1};
  uint8_t ttl = 4;
  CHECK(clj_rewrite_forwarded_quic_datagram(&ctx.h3_ctx.super, &msg, &dest, &src, &ttl) ==
        1);
  CHECK(vec.iov_base == buffer && vec.iov_len == len && ttl == 4);
  buffer[0] = 0;
  CHECK(clj_rewrite_forwarded_quic_datagram(&ctx.h3_ctx.super, &msg, &dest, &src, &ttl) ==
        1);
  CHECK(vec.iov_base == buffer && vec.iov_len == len && ttl == 4);
}

static void test_worker_ctx_send_offload(void) {
  h2o_globalconf_t conf;
  h2o_config_init(&conf);
  h2o_context_t h2o_ctx = {.globalconf = &conf};
  h2o_evloop_t *loop = h2o_evloop_create();
  ptls_cipher_suite_t *no_ciphers[] = {NULL};
  ptls_context_t tls = {.cipher_suites = no_ciphers};
  quicly_context_t quic = quicly_spec_context;
  quic.tls = &tls;
  h2o_http3_server_amend_quicly_context(&conf, &quic);
  h2o_hostconf_t *hosts[] = {NULL};

  for (int pass = 0; pass < 2; ++pass) {
    int fd = clj_h2o_http3_open_udp_fd("127.0.0.1", 0);
    CHECK(fd != -1);
    clj_http3_ctx_t *ctx = clj_h2o_http3_create_worker_ctx_from_fd(
        &h2o_ctx, loop, &quic, hosts, fd, 1, 0);
    CHECK(ctx != NULL);
    CHECK(ctx->h3_ctx.super.use_gso == conf.http3.use_gso);
    CHECK(ctx->h3_ctx.super.use_gso == (pass == 0));
    clj_h2o_http3_dispose_worker_ctx(ctx);
    conf.http3.use_gso = 0;
  }
  h2o_config_dispose(&conf);
}

static int accept_calls;
static quicly_address_t accepted_src, accepted_dest;
static uint8_t accepted_ecn;

static h2o_quic_conn_t *record_accept(h2o_quic_ctx_t *ctx,
                                      quicly_address_t *destaddr,
                                      quicly_address_t *srcaddr,
                                      quicly_decoded_packet_t *packet) {
  ++accept_calls;
  accepted_src = *srcaddr;
  accepted_dest = *destaddr;
  accepted_ecn = packet->ecn;
  return NULL;
}

static quicly_error_t open_no_stream(quicly_stream_open_t *self,
                                     quicly_stream_t *stream) {
  return 0;
}

static int bound_udp(quicly_address_t *addr) {
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  CHECK(fd != -1);
  *addr = address(AF_INET, 0);
  CHECK(bind(fd, &addr->sa, sizeof(addr->sin)) == 0);
  socklen_t len = sizeof(addr->sin);
  CHECK(getsockname(fd, &addr->sa, &len) == 0);
  CHECK(clj_set_fd_nonblock(fd) == 0);
  return fd;
}

/* An Initial with an envelope in front claiming another source address */
static size_t enveloped_initial(uint8_t *buf, quicly_address_t *dest,
                                quicly_address_t *claimed_src) {
  size_t len = clj_encode_quic_forwarded_header(buf, dest, claimed_src, 4, 0);
  CHECK(len != SIZE_MAX);
  static const uint8_t initial_head[] = {
      0xc0, 0, 0, 0, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 8, 9, 10, 11, 12,
      13, 14, 15, 16, 0, 0x40, 20};
  memcpy(buf + len, initial_head, sizeof(initial_head));
  len += sizeof(initial_head);
  memset(buf + len, 0, 20);
  return len + 20;
}

static void test_envelope_only_from_forwarding_channel(void) {
  static quicly_stream_open_t stream_open = {open_no_stream};
  ptls_context_t tls = {.random_bytes = ptls_openssl_random_bytes};
  quicly_context_t quic = quicly_spec_context;
  quic.tls = &tls;
  quic.stream_open = &stream_open;
  quicly_cid_plaintext_t next_cid = {.thread_id = 1, .node_id = 1};
  h2o_evloop_t *loop = h2o_evloop_create();

  quicly_address_t server_addr, client_addr, claimed_addr;
  int server_fd = bound_udp(&server_addr);
  int client_fd = bound_udp(&client_addr);
  int claimed_fd = bound_udp(&claimed_addr);

  clj_http3_ctx_t *ctx = calloc(1, sizeof(*ctx));
  CHECK(ctx != NULL);
  ctx->udp_sock =
      h2o_evloop_socket_create(loop, server_fd, H2O_SOCKET_FLAG_DONT_READ);
  h2o_quic_init_context(&ctx->h3_ctx.super, loop, ctx->udp_sock, NULL, &quic,
                        &next_cid, record_accept, NULL, 0, NULL);
  h2o_quic_set_forwarding_context(&ctx->h3_ctx.super, 0, 4,
                                  clj_forward_quic_packets,
                                  clj_rewrite_forwarded_quic_datagram);
  int fds[2], captured[2];
  CHECK(socketpair(AF_UNIX, SOCK_DGRAM, 0, fds) == 0);
  ctx->forwarded_sock =
      h2o_evloop_socket_create(loop, fds[0], H2O_SOCKET_FLAG_DONT_READ);
  ctx->forwarded_sock->data = ctx;
  /* The datagram decodes as a packet without a valid CID, so H2O forwards it
     to the invalid CID's worker; that forward shows the address H2O used. */
  CHECK(socketpair(AF_UNIX, SOCK_DGRAM, 0, captured) == 0);
  CHECK(clj_set_fd_nonblock(captured[1]) == 0);
  clj_http3_udp_transport_t *transport = calloc(1, sizeof(*transport));
  CHECK(transport != NULL);
  transport->reservation_fd = -1;
  CHECK(pthread_mutex_init(&transport->mutex, NULL) == 0);
  CHECK(clj_http3_transport_register_attachment(
      transport, quicly_cid_plaintext_invalid.node_id,
      quicly_cid_plaintext_invalid.thread_id, captured[0], NULL));
  CHECK(clj_http3_transport_register_attachment(transport, 1, 1, fds[1], ctx));
  ctx->transport = transport;
  ctx->node_id = 1;
  ctx->thread_id = 1;

  quicly_address_t no_dest = address(AF_UNSPEC, 0);
  uint8_t datagram[256];
  size_t len = enveloped_initial(datagram, &no_dest, &claimed_addr);
  char reply[512];

  /* From the public socket the envelope is ordinary payload of a datagram
     from its real sender. */
  accept_calls = 0;
  CHECK(sendto(client_fd, datagram, len, 0, &server_addr.sa,
               sizeof(server_addr.sin)) == (ssize_t)len);
  h2o_quic_read_socket(&ctx->h3_ctx.super, ctx->udp_sock);
  CHECK(accept_calls == 0);
  ssize_t reply_len = recv(captured[1], reply, sizeof(reply), 0);
  CHECK(reply_len > 0);
  quicly_address_t seen_dest, seen_src;
  uint8_t seen_ttl;
  size_t offset = clj_decode_quic_forwarded_header(
      &seen_dest, &seen_src, &seen_ttl, NULL,
      h2o_iovec_init(reply, (size_t)reply_len));
  CHECK(offset != SIZE_MAX);
  CHECK(h2o_socket_compare_address(&seen_src.sa, &client_addr.sa, 1) == 0);
  CHECK((size_t)reply_len - offset == len);
  CHECK(memcmp(reply + offset, datagram, len) == 0);

  /* Over the forwarding channel the envelope sets the source address */
  CHECK(write(fds[1], datagram, len) == (ssize_t)len);
  clj_forwarded_quic_socket_on_read(ctx->forwarded_sock, NULL);
  CHECK(accept_calls == 1);
  CHECK(ctx->reading == CLJ_HTTP3_READING_PUBLIC &&
        ctx->h3_ctx.super.notify_conn_update == NULL);
  CHECK(h2o_socket_compare_address(&accepted_src.sa, &claimed_addr.sa, 1) ==
        0);

  h2o_socket_close(ctx->forwarded_sock);
  close(fds[1]);
  close(captured[0]);
  close(captured[1]);
  h2o_quic_dispose_context(&ctx->h3_ctx.super);
  free(ctx);
  close(client_fd);
  close(claimed_fd);
  clj_h2o_http3_release_udp_transport(transport);
}

/* Workers of one transport, each with a real UDP socket and forwarding
   channel. Workers are added per generation and share one port, as in a
   server, unless `separate_ports` is set; `channel[i]` reads what other
   workers forward to worker i. Accepts are recorded, not performed. */
#define SPREAD_WORKERS 6
typedef struct {
  h2o_evloop_t *loop;
  ptls_context_t tls;
  quicly_stream_open_t stream_open;
  quicly_context_t quic;
  clj_http3_udp_transport_t *transport;
  int separate_ports;
  const char *host;
  uint16_t port;
  size_t num_workers;
  clj_http3_ctx_t *workers[SPREAD_WORKERS];
  int channel[SPREAD_WORKERS];
} spread_fixture_t;

static void spread_init(spread_fixture_t *f) {
  *f = (spread_fixture_t){.loop = h2o_evloop_create(),
                          .tls = {.random_bytes = ptls_openssl_random_bytes},
                          .stream_open = {open_no_stream},
                          .host = "127.0.0.1"};
  f->quic = quicly_spec_context;
  f->quic.tls = &f->tls;
  f->quic.stream_open = &f->stream_open;
  static const uint8_t cid_key[32] = {1};
  f->quic.cid_encryptor = quicly_new_default_cid_encryptor(
      &ptls_openssl_aes128ecb, &ptls_openssl_aes128ecb, &ptls_openssl_sha256,
      ptls_iovec_init(cid_key, sizeof(cid_key)));
  CHECK(f->quic.cid_encryptor != NULL);
  f->transport = calloc(1, sizeof(*f->transport));
  CHECK(f->transport != NULL);
  f->transport->reservation_fd = -1;
  CHECK(pthread_mutex_init(&f->transport->mutex, NULL) == 0);
}

static void spread_add(spread_fixture_t *f, uint64_t node, size_t num) {
  for (size_t thread = 0; thread < num; ++thread) {
    CHECK(f->num_workers < SPREAD_WORKERS);
    quicly_address_t addr;
    int fd;
    if (f->separate_ports) {
      fd = bound_udp(&addr);
    } else {
      fd = clj_h2o_http3_open_udp_fd(f->host, f->port);
      CHECK(fd != -1 && clj_set_fd_nonblock(fd) == 0);
      socklen_t len = sizeof(addr);
      CHECK(getsockname(fd, &addr.sa, &len) == 0);
      f->port = ntohs(addr.sa.sa_family == AF_INET ? addr.sin.sin_port
                                                   : addr.sin6.sin6_port);
    }
    clj_http3_ctx_t *ctx = calloc(1, sizeof(*ctx));
    CHECK(ctx != NULL);
    ctx->udp_sock =
        h2o_evloop_socket_create(f->loop, fd, H2O_SOCKET_FLAG_DONT_READ);
    ctx->next_cid = (quicly_cid_plaintext_t){.thread_id = thread,
                                             .node_id = node};
    h2o_quic_init_context(&ctx->h3_ctx.super, f->loop, ctx->udp_sock, NULL,
                          &f->quic, &ctx->next_cid, NULL, NULL, 0, NULL);
    h2o_quic_set_forwarding_context(&ctx->h3_ctx.super, 0, 4,
                                    clj_forward_quic_packets,
                                    clj_rewrite_forwarded_quic_datagram);
    int fds[2];
    CHECK(socketpair(AF_UNIX, SOCK_DGRAM, 0, fds) == 0);
    CHECK(clj_set_fd_nonblock(fds[0]) == 0);
    CHECK(clj_set_fd_nonblock(fds[1]) == 0);
    ctx->forwarded_sock =
        h2o_evloop_socket_create(f->loop, fds[0], H2O_SOCKET_FLAG_DONT_READ);
    ctx->forwarded_sock->data = ctx;
    ctx->transport = f->transport;
    ctx->node_id = node;
    ctx->thread_id = thread;
    ctx->forward_fd = fds[1];
    CHECK(clj_http3_transport_register_attachment(f->transport, node, thread,
                                                  fds[1], ctx));
    f->channel[f->num_workers] = fds[0];
    f->workers[f->num_workers++] = ctx;
  }
}

/* Activates `node`; accepts then only record the sender */
static void spread_activate(spread_fixture_t *f, uint64_t node) {
  clj_h2o_http3_activate_udp_transport_generation(f->transport, node);
  for (size_t i = 0; i < f->num_workers; ++i) {
    if (f->workers[i]->h3_ctx.super.acceptor != NULL)
      f->workers[i]->h3_ctx.super.acceptor = record_accept;
  }
}

static void spread_setup(spread_fixture_t *f, size_t num_workers) {
  spread_init(f);
  spread_add(f, 7, num_workers);
  spread_activate(f, 7);
}

static void spread_teardown(spread_fixture_t *f) {
  for (size_t i = 0; i < f->num_workers; ++i) {
    if (f->workers[i] != NULL)
      clj_h2o_http3_dispose_worker_ctx(f->workers[i]);
  }
  clj_h2o_http3_release_udp_transport(f->transport);
  free(f->quic.cid_encryptor);
}

static void set_conns(clj_http3_ctx_t *ctx, uint32_t conns) {
  atomic_store(&ctx->load->conns, conns);
  atomic_store(&ctx->load->pending, 0);
  ctx->kept = 0;
}

static uint32_t pending(clj_http3_ctx_t *ctx) {
  return atomic_load(&ctx->load->pending);
}

/* A long header datagram of `type` padded to `size` bytes */
static size_t long_header(uint8_t *buf, size_t size, uint8_t type,
                          const uint8_t *dcid, size_t dcid_len,
                          size_t scid_len) {
  size_t off = 0;
  buf[off++] = type;
  off += quicly_encode32(buf + off, QUICLY_PROTOCOL_VERSION_1) - (buf + off);
  buf[off++] = (uint8_t)dcid_len;
  memcpy(buf + off, dcid, dcid_len);
  off += dcid_len;
  buf[off++] = (uint8_t)scid_len;
  memset(buf + off, 0xaa, scid_len);
  off += scid_len;
  if (type == QUICLY_PACKET_TYPE_INITIAL)
    buf[off++] = 0; /* token length */
  size_t rest = size - off - 2;
  buf[off++] = (uint8_t)(0x40 | (rest >> 8));
  buf[off++] = (uint8_t)rest;
  memset(buf + off, 0, rest);
  return size;
}

/* An Initial of the smallest size a server accepts, or a short 0-RTT, with an
   8-byte DCID ending in `dcid_tag` */
static size_t quic_datagram(uint8_t *buf, uint8_t type, uint8_t dcid_tag) {
  const uint8_t dcid[8] = {1, 2, 3, 4, 5, 6, 7, dcid_tag};
  size_t size = type == QUICLY_PACKET_TYPE_INITIAL
                    ? QUICLY_MIN_CLIENT_INITIAL_SIZE
                    : 100;
  return long_header(buf, size, type, dcid, sizeof(dcid), 0);
}

#define DATAGRAM_MAX 1500

/* Worker `worker`'s preprocess on a datagram from the public socket; 0 when
   the datagram went elsewhere and only a stub stays */
static int preprocess(spread_fixture_t *f, size_t worker,
                      quicly_address_t *src, uint8_t *buf, size_t len) {
  struct iovec vec = {buf, len};
  struct msghdr msg = {.msg_iov = &vec, .msg_iovlen = 1};
  quicly_address_t dest = address(src->sa.sa_family, 0);
  if (src->sa.sa_family == AF_INET)
    dest.sin.sin_port = *f->workers[worker]->h3_ctx.super.sock.port;
  else
    dest.sin6.sin6_port = *f->workers[worker]->h3_ctx.super.sock.port;
  uint8_t ttl = 4;
  CHECK(clj_rewrite_forwarded_quic_datagram(&f->workers[worker]->h3_ctx.super,
                                            &msg, &dest, src, &ttl) == 1);
  CHECK(vec.iov_len == len || vec.iov_len == 1);
  return vec.iov_len == len;
}

static int arrive(spread_fixture_t *f, size_t worker, quicly_address_t *src,
                  uint8_t type, uint8_t dcid_tag) {
  uint8_t buf[DATAGRAM_MAX];
  return preprocess(f, worker, src, buf, quic_datagram(buf, type, dcid_tag));
}

static size_t cid_of(spread_fixture_t *f, uint64_t node, uint32_t thread,
                     uint8_t *cid) {
  quicly_cid_plaintext_t plaintext = {
      .master_id = 5, .thread_id = thread, .node_id = node};
  quicly_cid_t encrypted;
  f->quic.cid_encryptor->encrypt_cid(f->quic.cid_encryptor, &encrypted, NULL,
                                     &plaintext);
  memcpy(cid, encrypted.cid, encrypted.len);
  return encrypted.len;
}

/* A datagram whose DCID is a CID issued by worker `thread` of `node` */
static int arrive_with_cid(spread_fixture_t *f, size_t worker,
                           quicly_address_t *src, uint8_t type, uint64_t node,
                           uint32_t thread) {
  uint8_t cid[QUICLY_MAX_CID_LEN_V1], buf[DATAGRAM_MAX];
  size_t cid_len = cid_of(f, node, thread, cid);
  size_t size = type == QUICLY_PACKET_TYPE_INITIAL ? 1200 : 100;
  return preprocess(f, worker, src, buf,
                    long_header(buf, size, type, cid, cid_len, 0));
}

/* Datagrams forwarded to `worker` since the last call; drained unread */
static int forwarded_to(spread_fixture_t *f, size_t worker) {
  char buf[DATAGRAM_MAX + 64];
  int n = 0;
  while (recv(f->channel[worker], buf, sizeof(buf), 0) > 0)
    ++n;
  return n;
}

/* Lets every flow entry of `worker` expire */
static void age_flows(spread_fixture_t *f, size_t worker) {
  clj_http3_new_flow_t *flows = f->workers[worker]->new_flows;
  for (size_t idx = 0; idx < CLJ_HTTP3_NEW_FLOWS; ++idx)
    flows[idx].expires_at = flows[idx].expires_at > CLJ_HTTP3_NEW_FLOW_MILLIS
                                ? flows[idx].expires_at - CLJ_HTTP3_NEW_FLOW_MILLIS
                                : 0;
}

/* Worker `worker` reads its forwarding channel as in production */
static void deliver(spread_fixture_t *f, size_t worker) {
  clj_forwarded_quic_socket_on_read(f->workers[worker]->forwarded_sock, NULL);
}

static void test_forward(uint8_t ttl, int unknown_initial) {
  spread_fixture_t f;
  spread_init(&f);
  spread_add(&f, 1, 2);
  spread_add(&f, 2, 2);
  spread_activate(&f, 2);
  quicly_address_t dest = address(AF_INET, 443), src = address(AF_INET, 12345);
  uint8_t payload[] = "quic packet";
  quicly_decoded_packet_t packet = {.octets = {payload, sizeof(payload)}};
  uint64_t target_node = 2;
  /* From worker 0 of generation 1, to worker 1 of generation 2 by CID, or as
     an Initial this worker does not accept, to the active generation */
  int forwarded = clj_forward_quic_packets(
      &f.workers[0]->h3_ctx.super, unknown_initial ? NULL : &target_node,
      unknown_initial ? 99 : 1, &dest, &src, ttl, &packet, 1);

  char received[256];
  ssize_t len = recv(f.channel[3], received, sizeof(received), 0);
  if (unknown_initial && len < 0)
    len = recv(f.channel[2], received, sizeof(received), 0);
  if (ttl == 0) {
    CHECK(forwarded == 0);
    CHECK(len == -1 && (errno == EAGAIN || errno == EWOULDBLOCK));
  } else {
    CHECK(forwarded == 1 && len > 0);
    quicly_address_t decoded_dest, decoded_src;
    uint8_t decoded_ttl;
    int new_flow = 1;
    size_t offset = clj_decode_quic_forwarded_header(
        &decoded_dest, &decoded_src, &decoded_ttl, &new_flow,
        h2o_iovec_init(received, (size_t)len));
    CHECK(offset != SIZE_MAX && new_flow == 0);
    CHECK(decoded_ttl == ttl - 1);
    CHECK(decoded_dest.sin.sin_port == dest.sin.sin_port);
    CHECK(decoded_src.sin.sin_port == src.sin.sin_port);
    CHECK((size_t)len - offset == sizeof(payload));
    CHECK(memcmp(received + offset, payload, sizeof(payload)) == 0);
  }
  /* A worker of the active generation does not forward Initials it does
     not accept elsewhere */
  CHECK(clj_forward_quic_packets(&f.workers[2]->h3_ctx.super, NULL, 0, &dest,
                                 &src, 4, &packet, 1) == 0);
  spread_teardown(&f);
}

static void test_choose_worker(void) {
  spread_fixture_t f;
  spread_setup(&f, 3);
  clj_http3_ctx_t **w = f.workers;
  uint64_t now = h2o_now(f.loop);

  /* The deciding worker keeps a flow when it is at the minimum */
  CHECK(clj_http3_choose_worker(w[0], now) == w[0]->load);

  /* Otherwise the fewest connections and pending flows; ties are spread */
  set_conns(w[0], 2);
  set_conns(w[1], 1);
  set_conns(w[2], 1);
  clj_http3_worker_load_t *first = clj_http3_choose_worker(w[0], now);
  atomic_fetch_add(&first->pending, 1);
  clj_http3_worker_load_t *second = clj_http3_choose_worker(w[0], now);
  CHECK(first != w[0]->load && second != w[0]->load && first != second);
  set_conns(w[1], 0);
  CHECK(clj_http3_choose_worker(w[2], now) == w[1]->load);

  /* Flows the deciding worker kept in this millisecond count for it */
  set_conns(w[0], 0);
  set_conns(w[1], 1);
  set_conns(w[2], 1);
  w[0]->kept_at = now;
  w[0]->kept = 2;
  CHECK(clj_http3_choose_worker(w[0], now) != w[0]->load);
  CHECK(clj_http3_choose_worker(w[0], now + 1) == w[0]->load);

  /* Large counts do not wrap */
  set_conns(w[0], UINT32_MAX);
  atomic_store(&w[0]->load->pending, UINT32_MAX);
  set_conns(w[1], 3);
  set_conns(w[2], 3);
  CHECK(clj_http3_choose_worker(w[0], now) != w[0]->load);

  /* A withdrawn or stopped worker is never chosen, also after activation */
  set_conns(w[0], 5);
  set_conns(w[1], 0);
  set_conns(w[2], 3);
  clj_h2o_http3_withdraw_worker(w[1]);
  CHECK(clj_http3_choose_worker(w[0], now) == w[2]->load);
  spread_activate(&f, 7);
  CHECK(clj_http3_choose_worker(w[0], now) == w[2]->load);
  clj_h2o_http3_stop_accepting(w[2]);
  CHECK(clj_http3_choose_worker(w[0], now) == w[0]->load);
  clj_h2o_http3_stop_accepting(w[0]);
  CHECK(clj_http3_choose_worker(w[0], now) == NULL);

  /* Only the active generation is chosen */
  spread_activate(&f, 7);
  clj_h2o_http3_activate_udp_transport_generation(f.transport, 8);
  CHECK(clj_http3_choose_worker(w[0], now) == NULL);
  spread_teardown(&f);
}

static void test_accept_counting(void) {
  spread_fixture_t f;
  spread_setup(&f, 1);
  clj_http3_ctx_t *w = f.workers[0];
  atomic_store(&w->load->pending, 1);
  clj_http3_count_accept(&w->h3_ctx.super);
  CHECK(atomic_load(&w->load->conns) == 1 && pending(w) == 1);
  clj_http3_count_close(&w->h3_ctx.super);
  clj_http3_count_close(&w->h3_ctx.super);
  CHECK(atomic_load(&w->load->conns) == 0);

  /* Registering again starts from zero */
  atomic_store(&w->load->conns, 4);
  clj_h2o_http3_withdraw_worker(w);
  CHECK(clj_http3_transport_register_attachment(f.transport, 7, 0,
                                                w->forward_fd, w));
  CHECK(atomic_load(&w->load->conns) == 0 && pending(w) == 0);
  CHECK(!atomic_load(&w->load->withdrawn) && !atomic_load(&w->load->accepting));
  spread_teardown(&f);
}

/* The acceptor gives back its slot when the Initial does not decrypt, and
   does not return H2O's decryption-failed result, which would make H2O
   forward the overwritten bytes; it accepts nothing at the connection limit */
static void test_acceptor(void) {
  h2o_globalconf_t conf;
  h2o_config_init(&conf);
  h2o_config_register_host(&conf, h2o_iovec_init(H2O_STRLIT("default")),
                           65535);
  h2o_evloop_t *loop = h2o_evloop_create();
  h2o_context_t h2o_ctx;
  h2o_context_init(&h2o_ctx, loop, &conf);
  ptls_context_t tls = {.cipher_suites = ptls_openssl_cipher_suites,
                        .random_bytes = ptls_openssl_random_bytes};
  quicly_context_t quic = quicly_spec_context;
  quic.tls = &tls;
  h2o_http3_server_amend_quicly_context(&conf, &quic);
  int fd = clj_h2o_http3_open_udp_fd("127.0.0.1", 0);
  CHECK(fd != -1);
  clj_http3_ctx_t *ctx = clj_h2o_http3_create_worker_ctx_from_fd(
      &h2o_ctx, loop, &quic, conf.hosts, fd, 1, 0);
  CHECK(ctx != NULL);
  clj_http3_worker_load_t load = {.forward_fd = -1};
  ctx->load = &load;

  uint8_t buf[DATAGRAM_MAX];
  size_t len = quic_datagram(buf, QUICLY_PACKET_TYPE_INITIAL, 1);
  quicly_decoded_packet_t packet;
  size_t off = 0;
  CHECK(quicly_decode_packet(&quic, &packet, buf, len, &off) != SIZE_MAX);
  quicly_address_t dest = address(AF_INET, 443), src = address(AF_INET, 5000);

  clj_h2o_conn_limit_set_max(0);
  uint32_t before = clj_h2o_conn_limit_current();
  CHECK(clj_on_http3_accept(&ctx->h3_ctx.super, &dest, &src, &packet) ==
        NULL);
  CHECK(clj_h2o_conn_limit_current() == before);
  CHECK(atomic_load(&load.conns) == 0);

  clj_h2o_conn_limit_set_max(before + 1);
  CHECK(clj_h2o_conn_limit_try_acquire());
  CHECK(clj_on_http3_accept(&ctx->h3_ctx.super, &dest, &src, &packet) == NULL);
  CHECK(clj_h2o_conn_limit_current() == before + 1);
  CHECK(atomic_load(&load.conns) == 0);
  clj_h2o_conn_limit_release();
  clj_h2o_conn_limit_set_max(0);

  ctx->load = NULL;
  clj_h2o_http3_dispose_worker_ctx(ctx);
  h2o_context_dispose(&h2o_ctx);
  h2o_config_dispose(&conf);
}

static void test_new_flow_follows_first(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  quicly_address_t client = address(AF_INET, 5000);
  quicly_address_t other_port = address(AF_INET, 5001);
  quicly_address_t client6 = address(AF_INET6, 5000);
  set_conns(f.workers[0], 1);
  set_conns(f.workers[1], 0);

  /* The first Initial goes to the least loaded worker */
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_INITIAL, 1) == 0);
  CHECK(forwarded_to(&f, 1) == 1 && pending(f.workers[1]) == 1);

  /* Later Initial and 0-RTT datagrams of the flow follow it */
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_INITIAL, 1) == 0);
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_0RTT, 1) == 0);
  CHECK(forwarded_to(&f, 1) == 2 && pending(f.workers[1]) == 1);

  /* Other datagrams are left to H2O */
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_0RTT, 2) == 1);
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_HANDSHAKE, 1) == 1);
  CHECK(arrive(&f, 0, &client, 0x40, 1) == 1);
  CHECK(forwarded_to(&f, 1) == 0);

  /* Another source is another flow, also the same port over IPv6; at a tie
     the worker keeps it */
  CHECK(arrive(&f, 0, &other_port, QUICLY_PACKET_TYPE_INITIAL, 1) == 1);
  CHECK(arrive(&f, 0, &other_port, QUICLY_PACKET_TYPE_0RTT, 1) == 1);
  CHECK(pending(f.workers[0]) == 0 && forwarded_to(&f, 1) == 0);
  set_conns(f.workers[0], 2);
  CHECK(arrive(&f, 0, &client6, QUICLY_PACKET_TYPE_INITIAL, 1) == 0);
  CHECK(arrive(&f, 0, &client6, QUICLY_PACKET_TYPE_0RTT, 1) == 0);
  CHECK(forwarded_to(&f, 1) == 2);

  /* Entry ages follow the monotonic clock, not the loop's time */
  for (size_t idx = 0; idx < CLJ_HTTP3_NEW_FLOWS; ++idx) {
    uint64_t expires_at = f.workers[0]->new_flows[idx].expires_at;
    CHECK(expires_at == 0 ||
          expires_at <= clj_monotonic_millis() + CLJ_HTTP3_NEW_FLOW_MILLIS);
  }
  f.loop->_now_millisec += 1000000;
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_0RTT, 1) == 0);
  CHECK(forwarded_to(&f, 1) == 1);
  f.loop->_now_millisec -= 1000000;

  /* After the flow expires, its next Initial is a new decision */
  set_conns(f.workers[0], 1);
  set_conns(f.workers[1], 5);
  age_flows(&f, 0);
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_INITIAL, 1) == 1);
  CHECK(forwarded_to(&f, 1) == 0);
  spread_teardown(&f);
}

/* Datagrams quicly would never accept make no choice and no pending flow */
static void test_no_choice_for_junk(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  set_conns(f.workers[0], 3);
  set_conns(f.workers[1], 0);
  uint8_t buf[DATAGRAM_MAX];
  const uint8_t dcid[21] = {9};
  for (int i = 0; i < 1000; ++i) {
    quicly_address_t src = address(AF_INET, (uint16_t)(10000 + i));
    uint8_t tiny[7] = {QUICLY_PACKET_TYPE_INITIAL, 0, 0, 0, 1, 1, (uint8_t)i};
    CHECK(preprocess(&f, 0, &src, tiny, sizeof(tiny)) == 1);
    /* too small for a client's first datagram */
    CHECK(preprocess(&f, 0, &src, buf,
                     long_header(buf, 1199, QUICLY_PACKET_TYPE_INITIAL, dcid,
                                 8, 0)) == 1);
    /* DCID shorter than quicly accepts, or longer than QUIC allows */
    CHECK(preprocess(&f, 0, &src, buf,
                     long_header(buf, 1200, QUICLY_PACKET_TYPE_INITIAL, dcid,
                                 7, 0)) == 1);
    CHECK(preprocess(&f, 0, &src, buf,
                     long_header(buf, 1200, QUICLY_PACKET_TYPE_INITIAL, dcid,
                                 21, 0)) == 1);
    /* SCID longer than QUIC allows */
    CHECK(preprocess(&f, 0, &src, buf,
                     long_header(buf, 1200, QUICLY_PACKET_TYPE_INITIAL, dcid,
                                 8, 21)) == 1);
  }
  /* Cut short at the DCID, and without the SCID length */
  quicly_address_t src = address(AF_INET, 9000);
  long_header(buf, 1200, QUICLY_PACKET_TYPE_INITIAL, dcid, 8, 0);
  CHECK(preprocess(&f, 0, &src, buf, 6 + 8) == 1);
  CHECK(preprocess(&f, 0, &src, buf, 6 + 7) == 1);
  CHECK(pending(f.workers[0]) == 0 && pending(f.workers[1]) == 0);
  CHECK(forwarded_to(&f, 1) == 0);
  spread_teardown(&f);
}

/* The chosen worker gives back the pending flow when it reads the datagram,
   whether or not it accepts a connection from it */
static void test_pending_comes_back(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  set_conns(f.workers[0], 3);
  set_conns(f.workers[1], 0);
  quicly_address_t a = address(AF_INET, 5000), b = address(AF_INET, 5001);

  /* Accepted (recorded) */
  accept_calls = 0;
  CHECK(arrive(&f, 0, &a, QUICLY_PACKET_TYPE_INITIAL, 1) == 0);
  CHECK(pending(f.workers[1]) == 1);
  deliver(&f, 1);
  CHECK(pending(f.workers[1]) == 0 && accept_calls == 1);
  CHECK(h2o_socket_compare_address(&accepted_src.sa, &a.sa, 1) == 0);

  /* Refused, as at the connection limit */
  f.workers[1]->h3_ctx.super.acceptor = NULL;
  CHECK(arrive(&f, 0, &b, QUICLY_PACKET_TYPE_INITIAL, 2) == 0);
  CHECK(pending(f.workers[1]) == 1);
  deliver(&f, 1);
  CHECK(pending(f.workers[1]) == 0);
  f.workers[1]->h3_ctx.super.acceptor = record_accept;

  /* Large but undecodable: the length field runs past the datagram */
  uint8_t buf[DATAGRAM_MAX];
  size_t len = quic_datagram(buf, QUICLY_PACKET_TYPE_INITIAL, 3);
  buf[1 + 4 + 1 + 8 + 1 + 1] = 0x7f;
  quicly_address_t c = address(AF_INET, 5002);
  accept_calls = 0;
  CHECK(preprocess(&f, 0, &c, buf, len) == 0);
  CHECK(pending(f.workers[1]) == 1);
  deliver(&f, 1);
  CHECK(pending(f.workers[1]) == 0 && accept_calls == 0);

  /* Follow-up datagrams take nothing back */
  CHECK(arrive(&f, 0, &a, QUICLY_PACKET_TYPE_INITIAL, 1) == 0);
  atomic_store(&f.workers[1]->load->pending, 1);
  deliver(&f, 1);
  CHECK(pending(f.workers[1]) == 1);
  spread_teardown(&f);
}

/* Junk cannot push live flows out of the table; a full table leaves new
   flows to the kernel's choice */
static void test_full_flow_table(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  set_conns(f.workers[0], 3);
  set_conns(f.workers[1], 0);
  quicly_address_t moved = address(AF_INET, 5000);
  CHECK(arrive(&f, 0, &moved, QUICLY_PACKET_TYPE_INITIAL, 200) == 0);
  for (int i = 1; i < CLJ_HTTP3_NEW_FLOWS; ++i) {
    quicly_address_t src = address(AF_INET, (uint16_t)(20000 + i));
    arrive(&f, 0, &src, QUICLY_PACKET_TYPE_INITIAL, 1);
  }
  forwarded_to(&f, 1);
  set_conns(f.workers[0], 3);
  set_conns(f.workers[1], 0);
  quicly_address_t extra = address(AF_INET, 30000);
  CHECK(arrive(&f, 0, &extra, QUICLY_PACKET_TYPE_INITIAL, 1) == 1);
  CHECK(forwarded_to(&f, 1) == 0 && pending(f.workers[1]) == 0);

  /* The moved flow still follows its first datagram */
  set_conns(f.workers[0], 0);
  set_conns(f.workers[1], 5);
  CHECK(arrive(&f, 0, &moved, QUICLY_PACKET_TYPE_INITIAL, 200) == 0);
  CHECK(forwarded_to(&f, 1) == 1);

  /* Expired entries make room again */
  age_flows(&f, 0);
  set_conns(f.workers[0], 3);
  set_conns(f.workers[1], 0);
  CHECK(arrive(&f, 0, &extra, QUICLY_PACKET_TYPE_INITIAL, 1) == 0);
  CHECK(forwarded_to(&f, 1) == 1);
  spread_teardown(&f);
}

static void test_stop_after_decision(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  set_conns(f.workers[0], 3);
  set_conns(f.workers[1], 0);
  quicly_address_t client = address(AF_INET, 5000);
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_INITIAL, 1) == 0);
  forwarded_to(&f, 1);
  clj_h2o_http3_stop_accepting(f.workers[0]);

  /* The moved flow keeps following its first datagram; new flows are left
     to H2O */
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_INITIAL, 1) == 0);
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_0RTT, 1) == 0);
  CHECK(forwarded_to(&f, 1) == 2);
  quicly_address_t other = address(AF_INET, 5001);
  CHECK(arrive(&f, 0, &other, QUICLY_PACKET_TYPE_INITIAL, 2) == 1);
  CHECK(forwarded_to(&f, 1) == 0);
  spread_teardown(&f);
}

/* Initial and 0-RTT datagrams carrying a server CID go to the worker that
   issued it, in any generation */
static void test_server_cids(void) {
  spread_fixture_t f;
  spread_init(&f);
  spread_add(&f, 7, 2);
  spread_add(&f, 8, 2);
  spread_activate(&f, 8);
  quicly_address_t client = address(AF_INET, 5000);
  for (size_t i = 0; i < f.num_workers; ++i)
    set_conns(f.workers[i], 1);

  CHECK(arrive_with_cid(&f, 2, &client, QUICLY_PACKET_TYPE_0RTT, 8, 1) == 0);
  CHECK(forwarded_to(&f, 3) == 1);
  CHECK(arrive_with_cid(&f, 2, &client, QUICLY_PACKET_TYPE_0RTT, 7, 1) == 0);
  CHECK(forwarded_to(&f, 1) == 1);
  CHECK(arrive_with_cid(&f, 2, &client, QUICLY_PACKET_TYPE_0RTT, 8, 0) == 1);
  CHECK(arrive_with_cid(&f, 2, &client, QUICLY_PACKET_TYPE_0RTT, 7, 9) == 1);
  CHECK(arrive_with_cid(&f, 2, &client, QUICLY_PACKET_TYPE_INITIAL, 8, 1) ==
        0);
  CHECK(forwarded_to(&f, 3) == 1);
  CHECK(arrive_with_cid(&f, 2, &client, QUICLY_PACKET_TYPE_INITIAL, 7, 0) ==
        0);
  CHECK(forwarded_to(&f, 0) == 1);
  CHECK(arrive_with_cid(&f, 2, &client, QUICLY_PACKET_TYPE_INITIAL, 8, 0) ==
        1);
  for (size_t i = 0; i < f.num_workers; ++i) {
    CHECK(forwarded_to(&f, i) == 0);
    CHECK(pending(f.workers[i]) == 0);
  }
  spread_teardown(&f);
}

/* During a reload new flows go only to the active generation, and workers of
   the old generation do not choose */
static void test_reload(void) {
  spread_fixture_t f;
  spread_init(&f);
  spread_add(&f, 7, 2);
  spread_activate(&f, 7);
  spread_add(&f, 8, 2);
  spread_activate(&f, 8);
  for (size_t i = 0; i < f.num_workers; ++i)
    set_conns(f.workers[i], 0);
  set_conns(f.workers[2], 3);
  quicly_address_t a = address(AF_INET, 5000), b = address(AF_INET, 5001);
  CHECK(arrive(&f, 0, &a, QUICLY_PACKET_TYPE_INITIAL, 1) == 1);
  CHECK(arrive(&f, 2, &b, QUICLY_PACKET_TYPE_INITIAL, 1) == 0);
  CHECK(forwarded_to(&f, 3) == 1);
  CHECK(forwarded_to(&f, 0) + forwarded_to(&f, 1) == 0);
  spread_teardown(&f);
}

/* A coalesced datagram goes whole */
static void test_coalesced(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  set_conns(f.workers[0], 3);
  set_conns(f.workers[1], 0);
  uint8_t buf[DATAGRAM_MAX];
  const uint8_t dcid[8] = {1, 2, 3, 4, 5, 6, 7, 8};
  size_t len = long_header(buf, 600, QUICLY_PACKET_TYPE_INITIAL, dcid, 8, 0);
  len += long_header(buf + len, 600, QUICLY_PACKET_TYPE_0RTT, dcid, 8, 0);
  quicly_address_t client = address(AF_INET, 5000);
  CHECK(preprocess(&f, 0, &client, buf, len) == 0);
  char got[DATAGRAM_MAX + 64];
  ssize_t n = recv(f.channel[1], got, sizeof(got), 0);
  quicly_address_t d, s;
  uint8_t ttl;
  int new_flow = 0;
  size_t offset = clj_decode_quic_forwarded_header(
      &d, &s, &ttl, &new_flow, h2o_iovec_init(got, (size_t)n));
  CHECK(offset != SIZE_MAX && new_flow == 1);
  CHECK((size_t)n - offset == len && memcmp(got + offset, buf, len) == 0);
  spread_teardown(&f);
}

/* Datagrams steered away in the middle of a batch must not lend their
   addresses to the datagrams H2O still processes */
static void test_batch_keeps_addresses(void) {
  spread_fixture_t f;
  spread_init(&f);
  f.separate_ports = 1;
  spread_add(&f, 7, 2);
  spread_activate(&f, 7);
  clj_http3_ctx_t *w0 = f.workers[0];
  quicly_address_t server, first, second;
  socklen_t len = sizeof(server.sin);
  CHECK(getsockname(h2o_socket_get_fd(w0->udp_sock), &server.sa, &len) == 0);
  int first_fd = bound_udp(&first), second_fd = bound_udp(&second);
  uint8_t buf[DATAGRAM_MAX];
  size_t n = quic_datagram(buf, QUICLY_PACKET_TYPE_INITIAL, 1);
  CHECK(sendto(first_fd, buf, n, 0, &server.sa, sizeof(server.sin)) ==
        (ssize_t)n);
  n = quic_datagram(buf, QUICLY_PACKET_TYPE_INITIAL, 2);
  CHECK(sendto(second_fd, buf, n, 0, &server.sa, sizeof(server.sin)) ==
        (ssize_t)n);

  /* The first flow stays (a tie), the second goes to worker 1 */
  accept_calls = 0;
  h2o_quic_read_socket(&w0->h3_ctx.super, w0->udp_sock);
  CHECK(forwarded_to(&f, 1) == 1);
  CHECK(accept_calls == 1);
  CHECK(h2o_socket_compare_address(&accepted_src.sa, &first.sa, 1) == 0);
  close(first_fd);
  close(second_fd);
  spread_teardown(&f);
}

static void test_failed_forward_accepts_locally(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  quicly_address_t client = address(AF_INET, 5000);
  set_conns(f.workers[0], 1);
  set_conns(f.workers[1], 0);
  char filler[1024] = {0};
  while (write(f.workers[1]->forward_fd, filler, sizeof(filler)) > 0)
    ;
  CHECK(errno == EAGAIN || errno == EWOULDBLOCK);

  /* A full channel: this worker accepts the flow and keeps it */
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_INITIAL, 1) == 1);
  CHECK(pending(f.workers[1]) == 0 && pending(f.workers[0]) == 0);
  forwarded_to(&f, 1);
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_INITIAL, 1) == 1);
  CHECK(arrive(&f, 0, &client, QUICLY_PACKET_TYPE_0RTT, 1) == 1);
  CHECK(forwarded_to(&f, 1) == 0);

  /* A full channel later drops a follow-up for now */
  quicly_address_t moved = address(AF_INET, 5001);
  set_conns(f.workers[0], 3);
  set_conns(f.workers[1], 0);
  CHECK(arrive(&f, 0, &moved, QUICLY_PACKET_TYPE_INITIAL, 2) == 0);
  while (write(f.workers[1]->forward_fd, filler, sizeof(filler)) > 0)
    ;
  CHECK(arrive(&f, 0, &moved, QUICLY_PACKET_TYPE_INITIAL, 2) == 0);
  forwarded_to(&f, 1);
  CHECK(arrive(&f, 0, &moved, QUICLY_PACKET_TYPE_INITIAL, 2) == 0);
  CHECK(forwarded_to(&f, 1) == 1);

  /* When the worker is gone its flows come here */
  clj_http3_transport_unregister_attachment(f.transport, f.workers[1]);
  CHECK(f.workers[1]->load == NULL);
  CHECK(arrive(&f, 0, &moved, QUICLY_PACKET_TYPE_INITIAL, 2) == 1);
  CHECK(arrive(&f, 0, &moved, QUICLY_PACKET_TYPE_0RTT, 2) == 1);
  quicly_address_t next = address(AF_INET, 5002);
  CHECK(arrive(&f, 0, &next, QUICLY_PACKET_TYPE_INITIAL, 4) == 1);
  CHECK(forwarded_to(&f, 1) == 0);
  spread_teardown(&f);
}

/* An Initial the old generation does not accept goes to a worker of the
   active generation that can take it, and a full channel is not a forward */
static void test_fallback_skips_unavailable_workers(void) {
  spread_fixture_t f;
  spread_init(&f);
  spread_add(&f, 1, 2);
  spread_add(&f, 2, 2);
  spread_activate(&f, 2);
  quicly_address_t dest = address(AF_INET, 443), src = address(AF_INET, 5000);
  uint8_t payload[] = "initial";
  quicly_decoded_packet_t packet = {.octets = {payload, sizeof(payload)}};
  h2o_quic_ctx_t *old = &f.workers[1]->h3_ctx.super;

  clj_h2o_http3_withdraw_worker(f.workers[3]);
  CHECK(clj_forward_quic_packets(old, NULL, 1, &dest, &src, 4, &packet, 1) == 1);
  CHECK(forwarded_to(&f, 3) == 0 && forwarded_to(&f, 2) == 1);

  char filler[1024] = {0};
  while (write(f.workers[2]->forward_fd, filler, sizeof(filler)) > 0)
    ;
  CHECK(clj_forward_quic_packets(old, NULL, 1, &dest, &src, 4, &packet, 1) == 0);
  forwarded_to(&f, 2);

  clj_h2o_http3_stop_accepting(f.workers[2]);
  CHECK(clj_forward_quic_packets(old, NULL, 1, &dest, &src, 4, &packet, 1) == 0);
  CHECK(forwarded_to(&f, 2) == 0 && forwarded_to(&f, 3) == 0);
  spread_teardown(&f);
}

/* A register, unregister or activation on another thread, so that the test
   thread can act as a worker in the middle of a use of the worker sets */
typedef struct {
  spread_fixture_t *f;
  clj_http3_ctx_t *unregister; /* NULL: activate generation 7 */
  pthread_t thread;
  _Atomic int returned;
} lifecycle_call_t;

static void *lifecycle_call_run(void *arg) {
  lifecycle_call_t *call = arg;
  if (call->unregister != NULL)
    clj_http3_transport_unregister_attachment(call->f->transport,
                                              call->unregister);
  else
    clj_h2o_http3_activate_udp_transport_generation(call->f->transport, 7);
  atomic_store(&call->returned, 1);
  return NULL;
}

static void lifecycle_call_start(lifecycle_call_t *call, spread_fixture_t *f,
                                 clj_http3_ctx_t *unregister) {
  *call = (lifecycle_call_t){.f = f, .unregister = unregister};
  CHECK(pthread_create(&call->thread, NULL, lifecycle_call_run, call) == 0);
}

/* Whether the call has returned within `millis` */
static int lifecycle_call_returns(lifecycle_call_t *call, int millis) {
  for (int i = 0; i < millis && !atomic_load(&call->returned); ++i)
    usleep(1000);
  return atomic_load(&call->returned);
}

/* Unregistering publishes a set without the worker at once, but returns,
   frees the worker's record and lets its descriptor be closed only after a
   worker that found it in the old set has finished */
static void test_unregister_waits_for_users(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  clj_http3_worker_load_t *self = f.workers[0]->load;
  clj_http3_enter_sets(self);
  clj_http3_worker_set_t *set = atomic_load(&f.transport->workers);
  clj_http3_worker_load_t *target = clj_http3_find_worker(set, 7, 1);
  CHECK(target != NULL);
  lifecycle_call_t call;
  lifecycle_call_start(&call, &f, f.workers[1]);
  CHECK(!lifecycle_call_returns(&call, 200));
  CHECK(clj_http3_find_worker(atomic_load(&f.transport->workers), 7, 1) ==
        NULL);
  /* Freed too early, these would read the free-list links glibc writes
     into freed blocks */
  if (heap_skip == NULL)
    CHECK(set->active_node_id == 7 && set->num_loads == 2 &&
          set->loads[1] == target && target->node_id == 7 &&
          target->thread_id == 1);
  CHECK(atomic_load(&target->forward_fd) == -1);
  quicly_address_t addr = address(AF_INET, 5000);
  uint8_t byte = 0;
  CHECK(clj_http3_send_to_worker(target, &addr, &addr, 4, 0, &byte, 1) ==
        CLJ_HTTP3_GONE);
  clj_http3_leave_sets(self);
  CHECK(lifecycle_call_returns(&call, 5000));
  CHECK(pthread_join(call.thread, NULL) == 0);
  CHECK(f.workers[1]->load == NULL && forwarded_to(&f, 1) == 0);
  spread_teardown(&f);
}

/* A replaced set is freed only after every worker that loaded it has
   finished; a use that begins after the swap does not hold it up */
static void test_publish_waits_for_users(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  clj_http3_worker_load_t *self = f.workers[1]->load;
  clj_http3_worker_load_t *other = f.workers[0]->load;
  clj_http3_enter_sets(self);
  clj_http3_worker_set_t *set = atomic_load(&f.transport->workers);
  lifecycle_call_t call;
  lifecycle_call_start(&call, &f, NULL);
  CHECK(!lifecycle_call_returns(&call, 200));
  CHECK(atomic_load(&f.transport->workers) != set);
  /* As in test_unregister_waits_for_users */
  if (heap_skip == NULL)
    CHECK(set->active_node_id == 7 && set->num_loads == 2 &&
          set->loads[0] == other && set->loads[1] == self);
  clj_http3_leave_sets(self);
  clj_http3_enter_sets(self);
  CHECK(lifecycle_call_returns(&call, 5000));
  clj_http3_leave_sets(self);
  CHECK(pthread_join(call.thread, NULL) == 0);
  spread_teardown(&f);
}

/* Forwarding by CID and steering a new flow each mark one use */
static void test_uses_are_marked(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  clj_http3_worker_load_t *self = f.workers[0]->load;
  uint64_t before = atomic_load(&self->using_sets);
  quicly_address_t dest = address(AF_INET, 443), src = address(AF_INET, 5000);
  uint8_t payload[] = "quic packet";
  quicly_decoded_packet_t packet = {.octets = {payload, sizeof(payload)}};
  uint64_t node = 7;
  CHECK(clj_forward_quic_packets(&f.workers[0]->h3_ctx.super, &node, 1, &dest,
                                 &src, 4, &packet, 1) == 1);
  CHECK(atomic_load(&self->using_sets) == before + 2);
  set_conns(f.workers[0], 1);
  set_conns(f.workers[1], 0);
  CHECK(arrive(&f, 0, &src, QUICLY_PACKET_TYPE_INITIAL, 1) == 0);
  CHECK(atomic_load(&self->using_sets) == before + 4);
  CHECK(forwarded_to(&f, 1) == 2);
  spread_teardown(&f);
}

/* Only Linux shows, through /proc, that the sender sleeps inside its write;
   elsewhere the test could not tell a write in progress from one not begun */
#ifdef __linux__
#include <sys/syscall.h>

/* Worker 0 forwarding to worker 1 on its own thread */
typedef struct {
  spread_fixture_t *f;
  int steer;
  _Atomic int tid;
  _Atomic int result;
  _Atomic int returned;
} sender_t;

static void *sender_run(void *arg) {
  sender_t *sender = arg;
  atomic_store(&sender->tid, (int)syscall(SYS_gettid));
  quicly_address_t dest = address(AF_INET, 443), src = address(AF_INET, 5000);
  uint8_t payload[] = "quic packet";
  quicly_decoded_packet_t packet = {.octets = {payload, sizeof(payload)}};
  uint64_t node = 7;
  atomic_store(&sender->result,
               sender->steer
                   ? arrive(sender->f, 0, &src, QUICLY_PACKET_TYPE_INITIAL, 1)
                   : clj_forward_quic_packets(
                         &sender->f->workers[0]->h3_ctx.super, &node, 1, &dest,
                         &src, 4, &packet, 1));
  atomic_store(&sender->returned, 1);
  return NULL;
}

/* Whether the sender sleeps in the kernel, which only its write can do */
static int sender_blocked(sender_t *sender) {
  for (int i = 0; i < 5000; ++i) {
    char path[64], buf[512];
    snprintf(path, sizeof(path), "/proc/self/task/%d/stat",
             atomic_load(&sender->tid));
    FILE *stat = atomic_load(&sender->tid) != 0 ? fopen(path, "r") : NULL;
    if (stat != NULL) {
      size_t n = fread(buf, 1, sizeof(buf) - 1, stat);
      fclose(stat);
      buf[n] = '\0';
      char *end = strrchr(buf, ')');
      if (end != NULL && end[1] == ' ' && end[2] == 'S')
        return 1;
    }
    usleep(1000);
  }
  return 0;
}

/* A write to another worker's channel happens while the sender is marked, so
   unregistering the target, after which its descriptor closes, waits for it */
static void test_writes_are_marked(int steer) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  set_conns(f.workers[0], 1);
  set_conns(f.workers[1], 0);
  int fd = f.workers[1]->forward_fd;
  char filler[1024] = {0};
  while (write(fd, filler, sizeof(filler)) > 0)
    ;
  CHECK(errno == EAGAIN || errno == EWOULDBLOCK);
  CHECK(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) & ~O_NONBLOCK) == 0);

  sender_t sender = {.f = &f, .steer = steer};
  pthread_t thread;
  CHECK(pthread_create(&thread, NULL, sender_run, &sender) == 0);
  CHECK(sender_blocked(&sender));
  CHECK((atomic_load(&f.workers[0]->load->using_sets) & 1) == 1);
  lifecycle_call_t call;
  lifecycle_call_start(&call, &f, f.workers[1]);
  CHECK(!lifecycle_call_returns(&call, 200));

  forwarded_to(&f, 1);
  CHECK(pthread_join(thread, NULL) == 0);
  CHECK(atomic_load(&sender.result) == (steer ? 0 : 1));
  CHECK(lifecycle_call_returns(&call, 5000));
  CHECK(pthread_join(call.thread, NULL) == 0);
  CHECK((atomic_load(&f.workers[0]->load->using_sets) & 1) == 0);
  spread_teardown(&f);
}
#else
static void test_writes_are_marked(int steer) {
  printf("write marking test (%s) skipped: needs /proc to see a blocked "
         "write\n",
         steer ? "steering" : "forwarding");
}
#endif

static size_t count_loads(clj_http3_udp_transport_t *transport) {
  size_t n = 0;
  for (clj_http3_worker_load_t *load = transport->loads; load != NULL;
       load = load->next)
    ++n;
  return n;
}

/* One reload of N workers: the next generation registers and is activated,
   and the previous one unregisters */
static void reload_round(clj_http3_udp_transport_t *transport,
                         clj_http3_ctx_t *ctxs, uint64_t node, size_t n) {
  clj_http3_ctx_t *next = ctxs + (node % 2) * n, *prev = ctxs + (1 - node % 2) * n;
  for (uint32_t t = 0; t < n; ++t) {
    next[t] = (clj_http3_ctx_t){.node_id = node, .thread_id = t,
                                .forward_fd = (int)(1000 + node % 2 * n + t)};
    CHECK(clj_http3_transport_register_attachment(
        transport, node, t, next[t].forward_fd, next + t));
  }
  clj_h2o_http3_activate_udp_transport_generation(transport, node);
  for (uint32_t t = 0; t < n && node > 1; ++t)
    clj_http3_transport_unregister_attachment(transport, prev + t);
}

/* Heap in use and load records stay level over many reloads */
static void test_memory_per_reload(void) {
  enum { N = 8, RELOADS = 1000 };
  if (heap_skip != NULL) {
    printf("memory per reload test skipped: %s\n", heap_skip);
    return;
  }
  clj_http3_udp_transport_t *transport = calloc(1, sizeof(*transport));
  clj_http3_ctx_t ctxs[2 * N];
  CHECK(transport != NULL);
  transport->reservation_fd = -1;
  CHECK(pthread_mutex_init(&transport->mutex, NULL) == 0);
  uint64_t node = 1;
  for (; node <= 10; ++node)
    reload_round(transport, ctxs, node, N);
  size_t before = heap_now();
  for (; node <= 10 + RELOADS; ++node)
    reload_round(transport, ctxs, node, N);
  size_t after = heap_now();
  fprintf(stderr, "heap in use after %d reloads of %d workers: %zu bytes more\n",
          RELOADS, N, after > before ? after - before : 0);
  CHECK(after <= before + 1024);
  CHECK(count_loads(transport) == N && transport->num_attachments == N);
  CHECK(atomic_load(&transport->workers)->num_loads == N);
  clj_h2o_http3_release_udp_transport(transport);
}

static int udp_client(int family, quicly_address_t *addr) {
  int fd = socket(family, SOCK_DGRAM, 0);
  CHECK(fd != -1);
  *addr = address(family, 0);
  socklen_t len = quicly_get_socklen(&addr->sa);
  CHECK(bind(fd, &addr->sa, len) == 0);
  CHECK(getsockname(fd, &addr->sa, &len) == 0);
  return fd;
}

#if CLJ_HTTP3_CONNECTED_SOCKETS
/* Connections are only keys to the connected sockets */
static h2o_quic_conn_t fake_conns[CLJ_HTTP3_CONNECTED_MAX + 2];

static int count_fds(void) {
  DIR *dir = opendir("/proc/self/fd");
  CHECK(dir != NULL);
  int n = 0;
  while (readdir(dir) != NULL)
    ++n;
  closedir(dir);
  return n;
}

static void send_from(int fd, spread_fixture_t *f, int family) {
  quicly_address_t to = address(family, f->port);
  CHECK(sendto(fd, "x", 1, 0, &to.sa, quicly_get_socklen(&to.sa)) == 1);
}

/* Drains `fd`, counting datagrams from `from` and from others */
static void drain_count(int fd, quicly_address_t *from, int *mine,
                        int *others) {
  char buf[DATAGRAM_MAX];
  quicly_address_t src;
  socklen_t len = sizeof(src);
  while (recvfrom(fd, buf, sizeof(buf), MSG_DONTWAIT, &src.sa, &len) >= 0) {
    if (h2o_socket_compare_address(&src.sa, &from->sa, 1) == 0)
      ++*mine;
    else
      ++*others;
    len = sizeof(src);
  }
}

static void group_count(spread_fixture_t *f, quicly_address_t *from, int *mine,
                        int *others) {
  *mine = *others = 0;
  for (size_t i = 0; i < f->num_workers; ++i)
    drain_count(h2o_socket_get_fd(f->workers[i]->udp_sock), from, mine,
                others);
}

static void connected_count(clj_http3_ctx_t *w, quicly_address_t *from,
                            int *mine, int *others) {
  *mine = *others = 0;
  CHECK(w->num_connected == 1);
  drain_count(h2o_socket_get_fd(w->connected[0]->sock), from, mine, others);
}

static uint32_t transport_connected(spread_fixture_t *f) {
  return atomic_load(&f->transport->num_connected);
}

static void connected_setup(spread_fixture_t *f, int family) {
  spread_init(f);
  f->host = family == AF_INET ? "127.0.0.1" : "::1";
  spread_add(f, 7, 2);
  spread_activate(f, 7);
}

/* The connected socket receives its client's datagrams and no others;
   datagrams queued before the connect stay on the listener's sockets */
static void test_connected_socket_takes_its_flow(int family) {
  spread_fixture_t f;
  connected_setup(&f, family);
  clj_http3_ctx_t *w = f.workers[1];
  quicly_address_t client, other, local = address(family, f.port);
  int client_fd = udp_client(family, &client);
  int other_fd = udp_client(family, &other);
  int fds_before = count_fds(), mine, others;

  for (int i = 0; i < 3; ++i)
    send_from(client_fd, &f, family);
  CHECK(clj_http3_connect(w, fake_conns, &local, &client));
  CHECK(transport_connected(&f) == 1 && clj_h2o_http3_connected_sockets(f.transport, 7, 1) == 1);
  for (int i = 0; i < 5; ++i) {
    send_from(client_fd, &f, family);
    send_from(other_fd, &f, family);
  }
  connected_count(w, &client, &mine, &others);
  CHECK(mine == 5 && others == 0);
  group_count(&f, &client, &mine, &others);
  CHECK(mine == 3 && others == 5);

  /* H2O learns the destination address of a datagram read on it */
  uint8_t initial[DATAGRAM_MAX];
  size_t initial_len = quic_datagram(initial, QUICLY_PACKET_TYPE_INITIAL, 1);
  CHECK(sendto(client_fd, initial, initial_len, 0, &local.sa,
               quicly_get_socklen(&local.sa)) == (ssize_t)initial_len);
  accept_calls = 0;
  clj_http3_connected_on_read(w->connected[0]->sock, NULL);
  CHECK(accept_calls == 1);
  CHECK(h2o_socket_compare_address(&accepted_dest.sa, &local.sa, 1) == 0);

  /* The connection's end closes it, once */
  clj_http3_disconnect(w, fake_conns);
  clj_http3_disconnect(w, fake_conns);
  CHECK(w->num_connected == 0 && transport_connected(&f) == 0 &&
        clj_h2o_http3_connected_sockets(f.transport, 7, 1) == 0);
  CHECK(count_fds() == fds_before);
  send_from(client_fd, &f, family);
  group_count(&f, &client, &mine, &others);
  CHECK(mine == 1);

  close(client_fd);
  close(other_fd);
  spread_teardown(&f);
}

/* A connected socket is public: an envelope on it is payload, its client's
   datagrams are not steered, and other senders' datagrams, which arrive
   between its bind and its connect, are treated as on the listener's
   socket */
static void test_reading_connected_socket(void) {
  spread_fixture_t f;
  connected_setup(&f, AF_INET);
  clj_http3_ctx_t *w = f.workers[1];
  set_conns(f.workers[0], 0);
  set_conns(w, 3);
  quicly_address_t client, local = address(AF_INET, f.port);
  quicly_address_t claimed = address(AF_INET, 7777);
  int client_fd = udp_client(AF_INET, &client);
  CHECK(clj_http3_connect(w, fake_conns, &local, &client));
  h2o_socket_t *sock = w->connected[0]->sock;

  uint8_t buf[DATAGRAM_MAX];
  size_t len = enveloped_initial(buf, &local, &claimed);
  CHECK(sendto(client_fd, buf, len, 0, &local.sa, sizeof(local.sin)) ==
        (ssize_t)len);
  accept_calls = 0;
  clj_http3_connected_on_read(sock, NULL);
  CHECK(accept_calls == 0);
  CHECK(w->reading == CLJ_HTTP3_READING_PUBLIC &&
        w->h3_ctx.super.notify_conn_update == clj_on_conn_update);

  len = quic_datagram(buf, QUICLY_PACKET_TYPE_INITIAL, 1);
  CHECK(sendto(client_fd, buf, len, 0, &local.sa, sizeof(local.sin)) ==
        (ssize_t)len);
  clj_http3_connected_on_read(sock, NULL);
  CHECK(accept_calls == 1 && forwarded_to(&f, 0) == 0);
  CHECK(h2o_socket_compare_address(&accepted_src.sa, &client.sa, 1) == 0);
  CHECK(h2o_socket_compare_address(&accepted_dest.sa, &local.sa, 1) == 0);

  w->reading = CLJ_HTTP3_READING_CONNECTED;
  w->reading_peer = client;
  quicly_address_t other = address(AF_INET, 6000);
  uint64_t forwarded = clj_h2o_http3_forwarded_datagrams(f.transport, 7, 1);
  CHECK(arrive(&f, 1, &other, QUICLY_PACKET_TYPE_INITIAL, 9) == 0);
  CHECK(forwarded_to(&f, 0) == 1);
  CHECK(clj_h2o_http3_forwarded_datagrams(f.transport, 7, 1) == forwarded + 1);
  CHECK(arrive(&f, 1, &client, QUICLY_PACKET_TYPE_INITIAL, 10) == 1);
  CHECK(forwarded_to(&f, 0) == 0);
  w->reading = CLJ_HTTP3_READING_PUBLIC;

  close(client_fd);
  spread_teardown(&f);
}

/* At most CLJ_HTTP3_CONNECTED_MAX per listener over all its workers; above
   that a connection keeps arriving through the listener's sockets. Disposing
   a worker closes its sockets. */
static void test_connected_limit(void) {
  spread_fixture_t f;
  spread_init(&f);
  int fds_before = count_fds();
  spread_add(&f, 7, 2);
  spread_activate(&f, 7);
  quicly_address_t local = address(AF_INET, f.port);
  for (int i = 0; i < CLJ_HTTP3_CONNECTED_MAX; ++i) {
    quicly_address_t peer = address(AF_INET, (uint16_t)(40000 + i));
    CHECK(clj_http3_connect(f.workers[i % 2], fake_conns + i, &local, &peer));
  }
  CHECK(transport_connected(&f) == CLJ_HTTP3_CONNECTED_MAX);
  quicly_address_t peer = address(AF_INET, 41000);
  CHECK(!clj_http3_connect(f.workers[0], fake_conns + CLJ_HTTP3_CONNECTED_MAX,
                           &local, &peer));
  CHECK(!clj_http3_connect(f.workers[1], fake_conns + CLJ_HTTP3_CONNECTED_MAX,
                           &local, &peer));
  CHECK(transport_connected(&f) == CLJ_HTTP3_CONNECTED_MAX);
  CHECK(f.workers[0]->connect_retry_at == 0);

  clj_http3_disconnect(f.workers[1], fake_conns + 1);
  CHECK(clj_http3_connect(f.workers[0], fake_conns + CLJ_HTTP3_CONNECTED_MAX,
                          &local, &peer));
  uint32_t on_first = f.workers[0]->num_connected;
  CHECK(on_first == CLJ_HTTP3_CONNECTED_MAX / 2 + 1);

  int open = count_fds();
  clj_h2o_http3_dispose_worker_ctx(f.workers[0]);
  f.workers[0] = NULL;
  /* its connected sockets, its UDP socket and its forwarding channel */
  CHECK(count_fds() == open - (int)on_first - 3);
  CHECK(transport_connected(&f) == CLJ_HTTP3_CONNECTED_MAX - on_first);
  spread_teardown(&f);
  CHECK(count_fds() == fds_before);
}

/* A client that changes address or port gets a socket for its new path; the
   old one is closed */
static void test_connected_follows_path(void) {
  spread_fixture_t f;
  connected_setup(&f, AF_INET);
  clj_http3_ctx_t *w = f.workers[1];
  quicly_address_t first, second, local = address(AF_INET, f.port);
  int first_fd = udp_client(AF_INET, &first);
  int second_fd = udp_client(AF_INET, &second);
  int fds_before = count_fds(), mine, others;

  /* Only forwarded datagrams give a connection its first socket */
  clj_http3_follow_path(w, fake_conns, &local, &first, 0);
  CHECK(w->num_connected == 0 && count_fds() == fds_before);
  clj_http3_follow_path(w, fake_conns, &local, &first, 1);
  clj_http3_follow_path(w, fake_conns, &local, &first, 1);
  CHECK(w->num_connected == 1 && count_fds() == fds_before + 1);
  send_from(first_fd, &f, AF_INET);
  connected_count(w, &first, &mine, &others);
  CHECK(mine == 1);

  clj_http3_follow_path(w, fake_conns, &local, &second, 0);
  CHECK(w->num_connected == 1 && transport_connected(&f) == 1 &&
        count_fds() == fds_before + 1);
  CHECK(h2o_socket_compare_address(&w->connected[0]->peer.sa, &second.sa, 1) ==
        0);
  send_from(first_fd, &f, AF_INET);
  send_from(second_fd, &f, AF_INET);
  connected_count(w, &second, &mine, &others);
  CHECK(mine == 1 && others == 0);
  group_count(&f, &first, &mine, &others);
  CHECK(mine == 1 && others == 0);

  clj_http3_disconnect(w, fake_conns);
  CHECK(count_fds() == fds_before);
  close(first_fd);
  close(second_fd);
  spread_teardown(&f);
}

static void lowest_fd_limit(struct rlimit *saved) {
  CHECK(getrlimit(RLIMIT_NOFILE, saved) == 0);
  int probe = dup(0);
  CHECK(probe != -1);
  close(probe);
  struct rlimit low = *saved;
  low.rlim_cur = (rlim_t)probe;
  CHECK(setrlimit(RLIMIT_NOFILE, &low) == 0);
}

/* After a failed call: no descriptor, no reservation, and a pause before the
   worker tries again. Addresses a connected socket cannot use make no call
   and no pause. */
static void expect_failure(spread_fixture_t *f, clj_http3_ctx_t *w,
                           quicly_address_t *local, quicly_address_t *peer,
                           int expected_errno, int open) {
  errno = 0;
  CHECK(!clj_http3_connect(w, fake_conns, local, peer));
  CHECK(errno == expected_errno);
  CHECK(count_fds() == open && transport_connected(f) == 0 &&
        w->num_connected == 0);
  CHECK((w->connect_retry_at != 0) == (expected_errno != 0));
  w->connect_retry_at = 0;
}

static int port_bindable(int family, uint16_t port) {
  quicly_address_t addr = address(family, port);
  int fd = socket(family, SOCK_DGRAM, 0);
  CHECK(fd != -1);
  int bound = bind(fd, &addr.sa, quicly_get_socklen(&addr.sa)) == 0;
  close(fd);
  return bound;
}

/* A kept worker's sockets on the port close, and its context can still be
   disposed, which closes the stand-in socket */
static void test_close_worker_sockets(int family) {
  spread_fixture_t f;
  connected_setup(&f, family);
  quicly_address_t local = address(family, f.port);
  quicly_address_t peer = address(family, 40000);
  CHECK(clj_http3_connect(f.workers[1], fake_conns, &local, &peer));
  int open = count_fds();

  CHECK(clj_h2o_http3_close_worker_sockets(f.workers[1]) == 0);
  CHECK(f.workers[1]->num_connected == 0 && transport_connected(&f) == 0 &&
        clj_h2o_http3_connected_sockets(f.transport, 7, 1) == 0);
  /* the connected socket, while the stand-in replaces the listener socket */
  CHECK(count_fds() == open - 1);
  CHECK(!port_bindable(family, f.port));

  /* Without a stand-in the listener socket stays open */
  struct rlimit saved;
  lowest_fd_limit(&saved);
  CHECK(clj_h2o_http3_close_worker_sockets(f.workers[0]) == -1);
  CHECK(setrlimit(RLIMIT_NOFILE, &saved) == 0);
  CHECK(!port_bindable(family, f.port));
  CHECK(clj_h2o_http3_close_worker_sockets(f.workers[0]) == 0);
  CHECK(port_bindable(family, f.port));

  clj_h2o_http3_dispose_worker_ctx(f.workers[1]);
  f.workers[1] = NULL;
  /* the stand-in and the forwarding channel */
  CHECK(count_fds() == open - 4);
  spread_teardown(&f);
}

static void test_connect_failures(int family) {
  spread_fixture_t f;
  connected_setup(&f, family);
  clj_http3_ctx_t *w = f.workers[1];
  quicly_address_t local = address(family, f.port), peer = address(family, 40000);
  int open = count_fds();

  quicly_address_t any = local, other_port = address(family, f.port + 1),
                   no_port = address(family, 0),
                   other_family = address(family == AF_INET ? AF_INET6 : AF_INET,
                                          f.port);
  if (family == AF_INET)
    any.sin.sin_addr.s_addr = htonl(INADDR_ANY);
  else
    any.sin6.sin6_addr = in6addr_any;
  expect_failure(&f, w, &any, &peer, 0, open);
  expect_failure(&f, w, &other_port, &peer, 0, open);
  expect_failure(&f, w, &local, &no_port, 0, open);
  expect_failure(&f, w, &other_family, &peer, 0, open);
  expect_failure(&f, w, &local, &other_family, 0, open);
  quicly_address_t multicast = local;
  if (family == AF_INET) {
    quicly_address_t broadcast_local = local;
    inet_pton(AF_INET, "224.0.0.1", &multicast.sin.sin_addr);
    broadcast_local.sin.sin_addr.s_addr = htonl(INADDR_BROADCAST);
    expect_failure(&f, w, &broadcast_local, &peer, 0, open);
  } else {
    inet_pton(AF_INET6, "ff02::1", &multicast.sin6.sin6_addr);
  }
  expect_failure(&f, w, &multicast, &peer, 0, open);
  if (family == AF_INET6) {
    quicly_address_t link_local = local, link_peer = peer, mapped = peer;
    inet_pton(AF_INET6, "fe80::1", &link_local.sin6.sin6_addr);
    inet_pton(AF_INET6, "fe80::2", &link_peer.sin6.sin6_addr);
    inet_pton(AF_INET6, "::ffff:127.0.0.1", &mapped.sin6.sin6_addr);
    expect_failure(&f, w, &link_local, &peer, 0, open);
    expect_failure(&f, w, &local, &link_peer, 0, open);
    expect_failure(&f, w, &local, &mapped, 0, open);
  }

  /* socket: no descriptor left */
  struct rlimit saved;
  lowest_fd_limit(&saved);
  errno = 0;
  CHECK(!clj_http3_connect(w, fake_conns, &local, &peer));
  int socket_errno = errno;
  CHECK(setrlimit(RLIMIT_NOFILE, &saved) == 0);
  CHECK(socket_errno == EMFILE && w->connect_retry_at != 0);
  CHECK(count_fds() == open && transport_connected(&f) == 0);
  /* paused: the same call that now works does not happen yet */
  CHECK(!clj_http3_connect(w, fake_conns, &local, &peer));
  CHECK(count_fds() == open);
  w->connect_retry_at = 0;

  /* bind: an address that is not on this host */
  quicly_address_t foreign = local;
  if (family == AF_INET)
    inet_pton(AF_INET, "192.0.2.1", &foreign.sin.sin_addr);
  else
    inet_pton(AF_INET6, "2001:db8::1", &foreign.sin6.sin6_addr);
  expect_failure(&f, w, &foreign, &peer, EADDRNOTAVAIL, open);

  /* connect: broadcast without SO_BROADCAST */
  if (family == AF_INET) {
    quicly_address_t broadcast = peer;
    broadcast.sin.sin_addr.s_addr = htonl(INADDR_BROADCAST);
    expect_failure(&f, w, &local, &broadcast, EACCES, open);
  }

  CHECK(clj_http3_connect(w, fake_conns, &local, &peer));
  clj_http3_disconnect(w, fake_conns);
  CHECK(count_fds() == open);
  spread_teardown(&f);
}

/* SO_REUSEPORT puts the socket in the listener's group: before its connect
   it takes only a share of the port's datagrams, where a socket bound with
   SO_REUSEADDR alone takes them all */
static void test_bound_socket_joins_group(int family) {
  spread_fixture_t f;
  connected_setup(&f, family);
  quicly_address_t local = address(family, f.port);
  int fd = clj_http3_bound_fd(&local);
  CHECK(fd != -1);
  for (int i = 0; i < 64; ++i) {
    quicly_address_t from;
    int client_fd = udp_client(family, &from);
    send_from(client_fd, &f, family);
    close(client_fd);
  }
  int on_new = 0, unused = 0, on_group, more;
  quicly_address_t nobody = address(family, 1);
  drain_count(fd, &nobody, &unused, &on_new);
  group_count(&f, &nobody, &more, &on_group);
  CHECK(on_new > 0 && on_new < 64 && on_new + on_group == 64);
  close(fd);
  spread_teardown(&f);
}

/* With every connected socket of the listener in use, a moved connection
   would be forwarded for its whole life: new flows stay where the kernel
   put them, and their later Initials follow */
static void test_full_listener_keeps_flows(void) {
  spread_fixture_t f;
  spread_setup(&f, 2);
  set_conns(f.workers[0], 3);
  set_conns(f.workers[1], 0);
  atomic_store(&f.transport->num_connected, CLJ_HTTP3_CONNECTED_MAX);
  quicly_address_t kept = address(AF_INET, 5000), moved = address(AF_INET, 5001);
  CHECK(arrive(&f, 0, &kept, QUICLY_PACKET_TYPE_INITIAL, 1) == 1);
  CHECK(forwarded_to(&f, 1) == 0 && pending(f.workers[1]) == 0);
  atomic_store(&f.transport->num_connected, CLJ_HTTP3_CONNECTED_MAX - 1);
  CHECK(arrive(&f, 0, &kept, QUICLY_PACKET_TYPE_INITIAL, 1) == 1);
  CHECK(forwarded_to(&f, 1) == 0);
  CHECK(arrive(&f, 0, &moved, QUICLY_PACKET_TYPE_INITIAL, 2) == 0);
  CHECK(forwarded_to(&f, 1) == 1);
  atomic_store(&f.transport->num_connected, 0);
  spread_teardown(&f);
}
#endif

/* Sends an Initial marked `ecn` from `client_fd` to `to` */
static void send_marked(int client_fd, int family, int ecn,
                        quicly_address_t *to, uint8_t dcid_tag) {
  int tos = ecn;
  CHECK(family == AF_INET
            ? setsockopt(client_fd, IPPROTO_IP, IP_TOS, &tos, sizeof(tos)) == 0
            : setsockopt(client_fd, IPPROTO_IPV6, IPV6_TCLASS, &tos,
                         sizeof(tos)) == 0);
  uint8_t buf[DATAGRAM_MAX];
  size_t len = quic_datagram(buf, QUICLY_PACKET_TYPE_INITIAL, dcid_tag);
  CHECK(sendto(client_fd, buf, len, 0, &to->sa, quicly_get_socklen(&to->sa)) ==
        (ssize_t)len);
  accept_calls = 0;
  accepted_ecn = 0xff;
}

/* H2O passes the ECN bits of a datagram read on the listener's socket, or on
   a connected socket, to quicly with its packets */
static void test_ecn_marks(int family) {
  spread_fixture_t f;
  spread_init(&f);
  f.host = family == AF_INET ? "127.0.0.1" : "::1";
  spread_add(&f, 7, 1);
  spread_activate(&f, 7);
  clj_http3_ctx_t *w = f.workers[0];
  quicly_address_t client, local = address(family, f.port);
  int client_fd = udp_client(family, &client);
  static const int marks[] = {IPTOS_ECN_ECT0, IPTOS_ECN_CE};

  for (size_t i = 0; i < 2; ++i) {
    send_marked(client_fd, family, marks[i], &local, (uint8_t)(1 + i));
    h2o_quic_read_socket(&w->h3_ctx.super, w->udp_sock);
    CHECK(accept_calls == 1 && accepted_ecn == marks[i]);
  }
#if CLJ_HTTP3_CONNECTED_SOCKETS
  CHECK(clj_http3_connect(w, fake_conns, &local, &client));
  for (size_t i = 0; i < 2; ++i) {
    send_marked(client_fd, family, marks[i], &local, (uint8_t)(3 + i));
    clj_http3_connected_on_read(w->connected[0]->sock, NULL);
    CHECK(accept_calls == 1 && accepted_ecn == marks[i]);
  }
  clj_http3_disconnect(w, fake_conns);
#endif

  close(client_fd);
  spread_teardown(&f);
}

int main(void) {
  heap_skip = heap_skip_reason();
  if (heap_skip != NULL)
    printf("QUIC forwarding heap checks skipped: %s\n", heap_skip);
  test_worker_ctx_send_offload();
  for (int unknown_initial = 0; unknown_initial <= 1; ++unknown_initial) {
    test_forward(0, unknown_initial);
    test_forward(1, unknown_initial);
    test_forward(4, unknown_initial);
  }
  for (size_t i = 0; i < 2; ++i) {
    int family = i == 0 ? AF_INET : AF_INET6;
    test_rewrite(family, 0, 0);
    test_rewrite(family, 1, 1);
    test_rewrite(family, 4, 4);
    test_rewrite(family, 255, 4);
  }
  test_invalid_source();
  test_other_datagrams();
  test_envelope_only_from_forwarding_channel();
  test_choose_worker();
  test_accept_counting();
  test_acceptor();
  test_new_flow_follows_first();
  test_no_choice_for_junk();
  test_pending_comes_back();
  test_full_flow_table();
  test_stop_after_decision();
  test_server_cids();
  test_reload();
  test_coalesced();
  test_batch_keeps_addresses();
  test_failed_forward_accepts_locally();
  test_fallback_skips_unavailable_workers();
  test_unregister_waits_for_users();
  test_publish_waits_for_users();
  test_uses_are_marked();
  test_writes_are_marked(0);
  test_writes_are_marked(1);
  test_memory_per_reload();
  test_ecn_marks(AF_INET);
  test_ecn_marks(AF_INET6);
#if CLJ_HTTP3_CONNECTED_SOCKETS
  for (int i = 0; i < 2; ++i) {
    int family = i == 0 ? AF_INET : AF_INET6;
    test_connected_socket_takes_its_flow(family);
    test_connect_failures(family);
    test_bound_socket_joins_group(family);
    test_close_worker_sockets(family);
  }
  test_full_listener_keeps_flows();
  test_reading_connected_socket();
  test_connected_limit();
  test_connected_follows_path();
#endif
  puts("QUIC forwarding tests passed");
  return 0;
}
