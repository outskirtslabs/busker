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
  h2o_quic_ctx_t ctx = {.default_ttl = 4};
  ctx.sock.port = &port;
  char buffer[256];
  size_t offset =
      clj_encode_quic_forwarded_header(buffer, &dest, &src, incoming_ttl);
  CHECK(offset != SIZE_MAX);
  memcpy(buffer + offset, "packet", 6);
  struct iovec vec = {buffer, offset + 6};
  struct msghdr msg = {.msg_iov = &vec, .msg_iovlen = 1};
  quicly_address_t decoded_dest = {0}, decoded_src = {0};
  uint8_t ttl = 99;
  CHECK(clj_rewrite_forwarded_quic_datagram(&ctx, &msg, &decoded_dest,
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
  h2o_quic_ctx_t ctx = {.default_ttl = 4};
  ctx.sock.port = &port;
  char buffer[256];
  size_t len = clj_encode_quic_forwarded_header(buffer, &dest, &src, 4);
  CHECK(len != SIZE_MAX);
  struct iovec vec = {buffer, len};
  struct msghdr msg = {.msg_iov = &vec, .msg_iovlen = 1};
  uint8_t ttl = 4;
  CHECK(clj_rewrite_forwarded_quic_datagram(&ctx, &msg, &dest, &src, &ttl) ==
        0);
  CHECK(vec.iov_base == buffer && vec.iov_len == len);
}

static void test_other_datagrams(void) {
  quicly_address_t dest = address(AF_INET, 444), src = address(AF_INET, 12345);
  uint16_t port = htons(443);
  h2o_quic_ctx_t ctx = {.default_ttl = 4};
  ctx.sock.port = &port;
  char buffer[256];
  size_t len = clj_encode_quic_forwarded_header(buffer, &dest, &src, 255);
  CHECK(len != SIZE_MAX);
  struct iovec vec = {buffer, len};
  struct msghdr msg = {.msg_iov = &vec, .msg_iovlen = 1};
  uint8_t ttl = 4;
  CHECK(clj_rewrite_forwarded_quic_datagram(&ctx, &msg, &dest, &src, &ttl) ==
        1);
  CHECK(vec.iov_base == buffer && vec.iov_len == len && ttl == 4);
  buffer[0] = 0;
  CHECK(clj_rewrite_forwarded_quic_datagram(&ctx, &msg, &dest, &src, &ttl) ==
        1);
  CHECK(vec.iov_base == buffer && vec.iov_len == len && ttl == 4);
}

int main(void) {
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
  puts("QUIC forwarding tests passed");
  return 0;
}
