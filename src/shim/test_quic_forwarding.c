/* Exercise the private callbacks without adding test-only shim exports. */
#include "shim.c"

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);          \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

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

static void test_forward(uint8_t ttl, int unknown_initial) {
  int sockets[2];
  CHECK(socketpair(AF_UNIX, SOCK_DGRAM, 0, sockets) == 0);
  CHECK(clj_set_fd_nonblock(sockets[1]) == 0);
  clj_http3_transport_attachment_t attachment = {
      .node_id = 2, .thread_id = 1, .forward_fd = sockets[0]};
  clj_http3_udp_transport_t transport = {.mutex = PTHREAD_MUTEX_INITIALIZER,
                                         .attachments = &attachment,
                                         .num_attachments = 1,
                                         .active_node_id = 2};
  clj_http3_ctx_t ctx = {.transport = &transport, .node_id = 1, .thread_id = 1};
  quicly_address_t dest = address(AF_INET, 443), src = address(AF_INET, 12345);
  char payload[] = "quic packet";
  quicly_decoded_packet_t packet = {.octets = {payload, sizeof(payload)}};
  uint64_t target_node = 2;
  int forwarded = clj_forward_quic_packets(
      &ctx.h3_ctx.super, unknown_initial ? NULL : &target_node,
      unknown_initial ? 99 : 1, &dest, &src, ttl, &packet, 1);

  char received[256];
  ssize_t len = recv(sockets[1], received, sizeof(received), 0);
  if (ttl == 0) {
    CHECK(forwarded == 0);
    CHECK(len == -1 && (errno == EAGAIN || errno == EWOULDBLOCK));
  } else {
    CHECK(forwarded == 1 && len > 0);
    quicly_address_t decoded_dest, decoded_src;
    uint8_t decoded_ttl;
    size_t offset = clj_decode_quic_forwarded_header(
        &decoded_dest, &decoded_src, &decoded_ttl,
        h2o_iovec_init(received, (size_t)len));
    CHECK(offset != SIZE_MAX);
    CHECK(decoded_ttl == ttl - 1);
    CHECK(decoded_dest.sin.sin_port == dest.sin.sin_port);
    CHECK(decoded_src.sin.sin_port == src.sin.sin_port);
    CHECK((size_t)len - offset == sizeof(payload));
    CHECK(memcmp(received + offset, payload, sizeof(payload)) == 0);
  }
  close(sockets[0]);
  close(sockets[1]);
  CHECK(pthread_mutex_destroy(&transport.mutex) == 0);
}

static void test_rewrite(int family, uint8_t incoming_ttl,
                         uint8_t expected_ttl) {
  quicly_address_t dest = address(family, 443), src = address(family, 12345);
  uint16_t port = htons(443);
  clj_http3_ctx_t ctx = {.h3_ctx.super.default_ttl = 4,
                         .reading_forwarded = 1};
  ctx.h3_ctx.super.sock.port = &port;
  char buffer[256];
  size_t offset =
      clj_encode_quic_forwarded_header(buffer, &dest, &src, incoming_ttl);
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
                         .reading_forwarded = 1};
  ctx.h3_ctx.super.sock.port = &port;
  char buffer[256];
  size_t len = clj_encode_quic_forwarded_header(buffer, &dest, &src, 4);
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
                         .reading_forwarded = 1};
  ctx.h3_ctx.super.sock.port = &port;
  char buffer[256];
  size_t len = clj_encode_quic_forwarded_header(buffer, &dest, &src, 255);
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
static quicly_address_t accepted_src;

static h2o_quic_conn_t *record_accept(h2o_quic_ctx_t *ctx,
                                      quicly_address_t *destaddr,
                                      quicly_address_t *srcaddr,
                                      quicly_decoded_packet_t *packet) {
  ++accept_calls;
  accepted_src = *srcaddr;
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
  size_t len = clj_encode_quic_forwarded_header(buf, dest, claimed_src, 4);
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
  clj_http3_transport_attachment_t attachment = {
      .node_id = quicly_cid_plaintext_invalid.node_id,
      .thread_id = quicly_cid_plaintext_invalid.thread_id,
      .forward_fd = captured[0]};
  clj_http3_udp_transport_t transport = {.mutex = PTHREAD_MUTEX_INITIALIZER,
                                         .attachments = &attachment,
                                         .num_attachments = 1};
  ctx->transport = &transport;
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
      &seen_dest, &seen_src, &seen_ttl,
      h2o_iovec_init(reply, (size_t)reply_len));
  CHECK(offset != SIZE_MAX);
  CHECK(h2o_socket_compare_address(&seen_src.sa, &client_addr.sa, 1) == 0);
  CHECK((size_t)reply_len - offset == len);
  CHECK(memcmp(reply + offset, datagram, len) == 0);

  /* Over the forwarding channel the envelope sets the source address */
  CHECK(write(fds[1], datagram, len) == (ssize_t)len);
  clj_forwarded_quic_socket_on_read(ctx->forwarded_sock, NULL);
  CHECK(accept_calls == 1);
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
  CHECK(pthread_mutex_destroy(&transport.mutex) == 0);
}

int main(void) {
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
  puts("QUIC forwarding tests passed");
  return 0;
}
