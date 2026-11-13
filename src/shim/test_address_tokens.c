/* Exercise the private token callbacks without adding test-only shim exports.
   Arguments: certificate and private key PEM files for the accept tests. */
#include "shim.c"
#include <openssl/aead.h>
#include <openssl/hkdf.h>
#include <openssl/sha.h>
#include <dirent.h>

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);          \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

#define NOW 1000000000
#define MINUTE (60 * 1000)

static uint8_t client_cid[8] = {0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8};
static uint8_t server_cid[8] = {0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58};

static int64_t clock_ms = NOW;
static void test_now(quicly_now_t *self, double *now) {
  (void)self;
  *now = clock_ms;
}
static quicly_now_t test_clock = {test_now};
static quicly_context_t clock_ctx = {.now = &test_clock};

static quicly_address_t address(uint32_t ip, uint16_t port) {
  quicly_address_t addr = {0};
  addr.sin.sin_family = AF_INET;
  addr.sin.sin_addr.s_addr = htonl(ip);
  addr.sin.sin_port = htons(port);
  return addr;
}

static quicly_address_t address6(uint8_t last, uint16_t port) {
  quicly_address_t addr = {0};
  addr.sin6.sin6_family = AF_INET6;
  addr.sin6.sin6_addr.s6_addr[0] = 0x20;
  addr.sin6.sin6_addr.s6_addr[1] = 0x01;
  addr.sin6.sin6_addr.s6_addr[15] = last;
  addr.sin6.sin6_port = htons(port);
  return addr;
}

static quicly_address_t client(void) { return address(0x7f000001, 40000); }

static clj_session_ticket_t ticket(uint8_t name, uint8_t fill) {
  clj_session_ticket_t key = {.not_before = 0, .not_after = UINT64_MAX};
  memset(key.name, name, sizeof(key.name));
  for (size_t i = 0; i < sizeof(key.aes_key); ++i)
    key.aes_key[i] = (uint8_t)(fill + i);
  for (size_t i = 0; i < sizeof(key.hmac_key); ++i)
    key.hmac_key[i] = (uint8_t)(fill * 3 + i);
  return key;
}

static clj_ticket_manager_t *manager(const clj_session_ticket_t *keys,
                                     size_t num_keys) {
  clj_ticket_manager_t *mgr = clj_ticket_manager_create(3600);
  CHECK(mgr != NULL);
  CHECK(clj_ticket_manager_set_keys(mgr, keys, num_keys) == 0);
  return mgr;
}

static clj_address_token_keys_t worker(clj_ticket_manager_t *mgr) {
  return (clj_address_token_keys_t){.mgr = mgr};
}

static quicly_address_token_plaintext_t resumption_from(quicly_address_t remote,
                                                        int64_t issued_at) {
  quicly_address_token_plaintext_t token = {
      .type = QUICLY_ADDRESS_TOKEN_TYPE_RESUMPTION,
      .issued_at = (uint64_t)issued_at};
  token.remote = remote;
  return token;
}

static quicly_address_token_plaintext_t resumption(int64_t issued_at) {
  return resumption_from(client(), issued_at);
}

static quicly_address_token_plaintext_t retry(int64_t issued_at) {
  quicly_address_token_plaintext_t token = {
      .type = QUICLY_ADDRESS_TOKEN_TYPE_RETRY,
      .issued_at = (uint64_t)issued_at};
  token.remote = client();
  quicly_set_cid(&token.retry.original_dcid, ptls_iovec_init(server_cid, 8));
  quicly_set_cid(&token.retry.client_cid, ptls_iovec_init(client_cid, 8));
  quicly_set_cid(&token.retry.server_cid, ptls_iovec_init(server_cid, 8));
  return token;
}

typedef struct {
  quicly_error_t ret;
  quicly_address_token_plaintext_t buf;
  quicly_address_token_plaintext_t *token;
} result_t;

static result_t present_to(clj_address_token_keys_t *keys, uint8_t *token,
                           size_t len, quicly_address_t from, int64_t now,
                           uint8_t *dest_cid) {
  quicly_decoded_packet_t packet = {0};
  packet.token = ptls_iovec_init(token, len);
  packet.cid.src = ptls_iovec_init(client_cid, sizeof(client_cid));
  packet.cid.dest.encrypted = ptls_iovec_init(dest_cid, 8);
  result_t r;
  const char *err_desc;
  clock_ms = now;
  r.ret = clj_read_address_token(keys, &clock_ctx, &from.sa, &packet, &r.buf,
                                 &r.token, &err_desc);
  return r;
}

static result_t present(clj_address_token_keys_t *keys, uint8_t *token,
                        size_t len, quicly_address_t from, int64_t now) {
  return present_to(keys, token, len, from, now, server_cid);
}

static int accepted(clj_address_token_keys_t *keys, uint8_t *token,
                    size_t len) {
  result_t r = present(keys, token, len, client(), NOW + MINUTE);
  CHECK(r.ret == 0);
  return r.token != NULL && !r.token->address_mismatch;
}

static int ignored(clj_address_token_keys_t *keys, uint8_t *token, size_t len) {
  result_t r = present(keys, token, len, client(), NOW + MINUTE);
  return r.ret == 0 && r.token == NULL;
}

static size_t issue(clj_address_token_keys_t *keys,
                    quicly_address_token_plaintext_t plaintext, uint8_t *dst) {
  ptls_buffer_t buf;
  ptls_buffer_init(&buf, dst, 256);
  CHECK(clj_encrypt_address_token(keys, &buf, &plaintext) == 0);
  CHECK(!buf.is_allocated);
  return buf.off;
}

/* Computes h2o's token key with BoringSSL's HKDF and AEAD instead of picotls:
   PRK = SHA-256(cipher key || HMAC key) (ssl.c:1223-1226), secret =
   HKDF-Expand(PRK, HkdfLabel(32, "address-token", "")), and picotls's AEAD key
   = HKDF-Expand(secret, HkdfLabel(16, "key", "")); h2o passes an empty label
   prefix, so the labels carry no "tls13 ". The token nonce is the IV it
   carries, and the associated data is the name, type and IV. */
static void test_key_matches_h2o(void) {
  clj_session_ticket_t key = ticket(0x31, 7);
  clj_ticket_manager_t *mgr = manager(&key, 1);
  clj_address_token_keys_t keys = worker(mgr);
  uint8_t token[256];
  size_t len = issue(&keys, resumption(NOW), token);
  CHECK(token[0] == 0x31 && token[1] == QUICLY_ADDRESS_TOKEN_TYPE_RESUMPTION);

  uint8_t keybuf[96], prk[32], secret[32], aead_key[16];
  memcpy(keybuf, key.aes_key, 32);
  memcpy(keybuf + 32, key.hmac_key, 64);
  SHA256(keybuf, sizeof(keybuf), prk);
  static const uint8_t secret_info[] = {0, 32, 13, 'a', 'd', 'd', 'r', 'e',
                                        's', 's', '-', 't', 'o', 'k', 'e',
                                        'n', 0};
  static const uint8_t key_info[] = {0, 16, 3, 'k', 'e', 'y', 0};
  CHECK(HKDF_expand(secret, sizeof(secret), EVP_sha256(), prk, sizeof(prk),
                    secret_info, sizeof(secret_info)) == 1);
  CHECK(HKDF_expand(aead_key, sizeof(aead_key), EVP_sha256(), secret,
                    sizeof(secret), key_info, sizeof(key_info)) == 1);

  EVP_AEAD_CTX aead;
  CHECK(EVP_AEAD_CTX_init(&aead, EVP_aead_aes_128_gcm(), aead_key,
                          sizeof(aead_key), 16, NULL) == 1);
  uint8_t plaintext[256];
  size_t plaintext_len;
  CHECK(EVP_AEAD_CTX_open(&aead, plaintext, &plaintext_len, sizeof(plaintext),
                          token + 2, 12, token + 14, len - 14, token,
                          14) == 1);
  CHECK(plaintext_len >= 8 && quicly_decode64(&(const uint8_t *){plaintext}) ==
                                  (uint64_t)NOW);
  EVP_AEAD_CTX_cleanup(&aead);

  clj_address_token_keys_dispose(&keys);
  clj_ticket_manager_destroy(mgr);
}

static void test_validation(void) {
  clj_session_ticket_t key = ticket(0x40, 1);
  clj_ticket_manager_t *mgr = manager(&key, 1);
  clj_address_token_keys_t issuer = worker(mgr), other = worker(mgr);
  uint8_t token[256];
  size_t len = issue(&issuer, resumption(NOW), token);
  result_t r;

  r = present(&other, token, len, address(0x7f000001, 50000), NOW + 1000);
  CHECK(r.ret == 0 && r.token != NULL && !r.token->address_mismatch);
  CHECK(r.token->type == QUICLY_ADDRESS_TOKEN_TYPE_RESUMPTION);
  r = present(&other, token, len, address(0x7f000002, 40000), NOW + 1000);
  CHECK(r.ret == 0 && r.token != NULL && r.token->address_mismatch);
  r = present(&other, token, len, client(), NOW + 10 * MINUTE);
  CHECK(r.ret == 0 && r.token != NULL && !r.token->address_mismatch);
  r = present(&other, token, len, client(), NOW + 10 * MINUTE + 1);
  CHECK(r.ret == 0 && r.token != NULL && r.token->address_mismatch);
  r = present(&other, token, len, client(), NOW - 5000);
  CHECK(r.ret == 0 && r.token != NULL && !r.token->address_mismatch);

  CHECK(ignored(&other, token, 10));
  token[len - 1] ^= 1;
  CHECK(ignored(&other, token, len));

  quicly_address_t loopback6 = address6(1, 40000);
  memset(loopback6.sin6.sin6_addr.s6_addr, 0, 15);
  len = issue(&issuer, resumption_from(loopback6, NOW), token);
  quicly_address_t loopback6_port = loopback6;
  loopback6_port.sin6.sin6_port = htons(50000);
  r = present(&other, token, len, loopback6_port, NOW + 1000);
  CHECK(r.ret == 0 && r.token != NULL && !r.token->address_mismatch);
  r = present(&other, token, len, address6(1, 40000), NOW + 1000);
  CHECK(r.ret == 0 && r.token != NULL && r.token->address_mismatch);
  r = present(&other, token, len, client(), NOW + 1000);
  CHECK(r.ret == 0 && r.token != NULL && r.token->address_mismatch);
  len = issue(&issuer, resumption_from(address6(1, 40000), NOW), token);
  r = present(&other, token, len, address6(1, 40000), NOW + 1000);
  CHECK(r.ret == 0 && r.token != NULL && !r.token->address_mismatch);
  r = present(&other, token, len, loopback6, NOW + 1000);
  CHECK(r.ret == 0 && r.token != NULL && r.token->address_mismatch);

  len = issue(&issuer, retry(NOW), token);
  r = present(&other, token, len, client(), NOW + 30 * 1000);
  CHECK(r.ret == 0 && r.token != NULL);
  CHECK(r.token->type == QUICLY_ADDRESS_TOKEN_TYPE_RETRY);
  r = present(&other, token, len, client(), NOW + 30 * 1000 + 1);
  CHECK(r.ret == 0 && r.token == NULL);
  r = present(&other, token, len, address(0x7f000001, 40001), NOW);
  CHECK(r.ret == 0 && r.token == NULL);
  uint8_t other_cid[8] = {0x99};
  r = present_to(&other, token, len, client(), NOW, other_cid);
  CHECK(r.ret == 0 && r.token == NULL);
  token[len - 1] ^= 1;
  r = present(&other, token, len, client(), NOW);
  CHECK(r.ret == QUICLY_TRANSPORT_ERROR_INVALID_TOKEN && r.token == NULL);

  clj_address_token_keys_dispose(&issuer);
  clj_address_token_keys_dispose(&other);
  clj_ticket_manager_destroy(mgr);
}

/* Workers notice key replacement through the manager's generation, encrypt
   with the newest key and decrypt with whichever live key the name selects. */
static void test_rotation_and_reload(void) {
  clj_session_ticket_t old_key = ticket(0x10, 1), new_key = ticket(0x20, 2);
  clj_ticket_manager_t *mgr = manager(&old_key, 1);
  clj_address_token_keys_t a = worker(mgr), b = worker(mgr);
  uint8_t old_token[256], new_token[256];
  size_t old_len = issue(&a, resumption(NOW), old_token);
  CHECK(old_token[0] == 0x10);

  clj_session_ticket_t rotated[] = {new_key, old_key};
  CHECK(clj_ticket_manager_set_keys(mgr, rotated, 2) == 0);
  size_t new_len = issue(&a, resumption(NOW), new_token);
  CHECK(new_token[0] == 0x20);
  CHECK(accepted(&b, old_token, old_len));
  CHECK(accepted(&b, new_token, new_len));

  /* A later generation gets a new manager with the same keys. */
  clj_ticket_manager_t *reloaded = manager(rotated, 2);
  clj_address_token_keys_t c = worker(reloaded);
  CHECK(accepted(&c, old_token, old_len));
  CHECK(accepted(&c, new_token, new_len));

  CHECK(clj_ticket_manager_set_keys(mgr, &new_key, 1) == 0);
  CHECK(ignored(&b, old_token, old_len));
  CHECK(accepted(&b, new_token, new_len));

  clj_session_ticket_t same_name = ticket(0x20, 9);
  CHECK(clj_ticket_manager_set_keys(mgr, &same_name, 1) == 0);
  CHECK(ignored(&b, new_token, new_len));

  clj_address_token_keys_dispose(&a);
  clj_address_token_keys_dispose(&b);
  clj_address_token_keys_dispose(&c);
  clj_ticket_manager_destroy(mgr);
  clj_ticket_manager_destroy(reloaded);
}

typedef struct {
  clj_ticket_manager_t *mgr;
  atomic_int stop;
} swapper_t;

static void *swap_keys(void *arg) {
  swapper_t *swapper = arg;
  clj_session_ticket_t one[] = {ticket(0x01, 1), ticket(0x02, 2)},
                       two[] = {ticket(0x02, 2), ticket(0x01, 1)};
  for (unsigned i = 0; !atomic_load(&swapper->stop); ++i)
    CHECK(clj_ticket_manager_set_keys(swapper->mgr, i % 2 ? one : two, 2) ==
          0);
  return NULL;
}

static void test_rebuild_during_swaps(void) {
  clj_session_ticket_t initial[] = {ticket(0x01, 1), ticket(0x02, 2)};
  swapper_t swapper = {.mgr = manager(initial, 2)};
  clj_address_token_keys_t issuer = worker(swapper.mgr),
                           reader = worker(swapper.mgr);
  pthread_t thread;
  CHECK(pthread_create(&thread, NULL, swap_keys, &swapper) == 0);
  uint64_t rebuilds = 0, seen = issuer.generation;
  for (int i = 0; i < 5000; ++i) {
    uint8_t token[256];
    size_t len = issue(&issuer, resumption(NOW), token);
    CHECK(token[0] == 0x01 || token[0] == 0x02);
    CHECK(accepted(&reader, token, len));
    if (issuer.generation != seen) {
      seen = issuer.generation;
      ++rebuilds;
    }
  }
  atomic_store(&swapper.stop, 1);
  CHECK(pthread_join(thread, NULL) == 0);
  CHECK(rebuilds > 0);
  clj_address_token_keys_dispose(&issuer);
  clj_address_token_keys_dispose(&reader);
  clj_ticket_manager_destroy(swapper.mgr);
}

static void test_without_keys(void) {
  clj_session_ticket_t key = ticket(0x55, 3);
  clj_ticket_manager_t *mgr = manager(&key, 1), *empty = manager(NULL, 0);
  clj_address_token_keys_t keyed = worker(mgr), none = worker(NULL),
                           emptied = worker(empty);
  uint8_t token[256], real[256];

  clj_address_token_keys_t *keyless[] = {&none, &emptied};
  quicly_address_t from[] = {client(), address6(1, 40000)};
  for (size_t i = 0; i < 2; ++i) {
    for (size_t f = 0; f < 2; ++f) {
      size_t len = issue(keyless[i], resumption_from(from[f], NOW), token);
      CHECK(len == (f == 0 ? 47u : 63u));
      CHECK(len == issue(&keyed, resumption_from(from[f], NOW), real));
      CHECK(token[1] == QUICLY_ADDRESS_TOKEN_TYPE_RESUMPTION);
      CHECK(keyless[i]->num_keys == 0 && keyless[i]->keys == NULL);
      token[0] = 0x55;
      CHECK(ignored(&keyed, token, len));
    }
  }

  size_t len = issue(&keyed, resumption(NOW), token);
  CHECK(ignored(&none, token, len));
  CHECK(ignored(&emptied, token, len));

  clj_address_token_keys_dispose(&keyed);
  clj_address_token_keys_dispose(&none);
  clj_address_token_keys_dispose(&emptied);
  clj_ticket_manager_destroy(mgr);
  clj_ticket_manager_destroy(empty);
}

/* clj_on_http3_accept with real Initial packets from a quicly client. */
typedef struct {
  h2o_globalconf_t config;
  h2o_context_t context;
  h2o_hostconf_t *hosts[2];
  clj_http3_ctx_t *worker;
  quicly_address_t server_addr, client_addr;
  int client_fd;
  ptls_context_t client_tls;
  quicly_context_t client_quic;
} accept_fixture_t;

static int bound_udp(quicly_address_t *addr) {
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  CHECK(fd >= 0);
  *addr = address(0x7f000001, 0);
  CHECK(bind(fd, &addr->sa, sizeof(addr->sin)) == 0);
  socklen_t len = sizeof(addr->sin);
  CHECK(getsockname(fd, &addr->sa, &len) == 0);
  return fd;
}

static void accept_fixture_init(accept_fixture_t *f, const char *cert,
                                const char *key, clj_ticket_manager_t *mgr) {
  h2o_config_init(&f->config);
  f->hosts[0] = h2o_config_register_host(
      &f->config, h2o_iovec_init(H2O_STRLIT("default")), 65535);
  f->hosts[1] = NULL;
  h2o_evloop_t *loop = h2o_evloop_create();
  h2o_context_init(&f->context, loop, &f->config);
  ptls_context_t *server_tls = clj_h2o_create_ptls_ctx(cert, key, NULL, NULL);
  CHECK(server_tls != NULL);
  quicly_context_t *quic = clj_h2o_create_quicly_ctx(server_tls, &f->config);
  CHECK(quic != NULL);
  int server_fd = clj_h2o_http3_open_udp_fd("127.0.0.1", 0);
  CHECK(server_fd != -1 && clj_set_fd_nonblock(server_fd) == 0);
  socklen_t len = sizeof(f->server_addr);
  CHECK(getsockname(server_fd, &f->server_addr.sa, &len) == 0);
  f->worker = clj_h2o_http3_create_worker_ctx_from_fd(
      &f->context, loop, quic, f->hosts, server_fd, 1, 0);
  CHECK(f->worker != NULL);
  f->worker->address_token.mgr = mgr;
  f->client_fd = bound_udp(&f->client_addr);
  CHECK(clj_set_fd_nonblock(f->client_fd) == 0);

  f->client_tls = (ptls_context_t){
      .random_bytes = ptls_openssl_random_bytes,
      .get_time = &ptls_get_time,
      .key_exchanges = ptls_openssl_key_exchanges,
      .cipher_suites = ptls_openssl_cipher_suites};
  quicly_amend_ptls_context(&f->client_tls);
  f->client_quic = quicly_spec_context;
  f->client_quic.tls = &f->client_tls;
}

/* Sends one client Initial carrying `token` to clj_on_http3_accept. */
static h2o_quic_conn_t *client_initial(accept_fixture_t *f, ptls_iovec_t token) {
  static quicly_cid_plaintext_t next_cid;
  ptls_iovec_t alpn = ptls_iovec_init("h3", 2);
  ptls_handshake_properties_t props = {
      .client = {.negotiated_protocols = {.list = &alpn, .count = 1}}};
  quicly_conn_t *conn;
  CHECK(quicly_connect(&conn, &f->client_quic, "localhost",
                       &f->server_addr.sa, NULL, &next_cid, token, &props,
                       NULL, NULL) == 0);
  ++next_cid.master_id;
  quicly_address_t dest, src;
  struct iovec datagrams[10];
  size_t num_datagrams = 10;
  static uint8_t buf[10 * 1500];
  CHECK(quicly_send(conn, &dest, &src, datagrams, &num_datagrams, buf,
                    sizeof(buf)) == 0);
  CHECK(num_datagrams >= 1);
  quicly_decoded_packet_t packet;
  size_t off = 0;
  CHECK(quicly_decode_packet(f->worker->h3_ctx.super.quic, &packet,
                             datagrams[0].iov_base, datagrams[0].iov_len,
                             &off) != SIZE_MAX);
  return clj_on_http3_accept(&f->worker->h3_ctx.super, &f->server_addr,
                             &f->client_addr, &packet);
}

static int validated(h2o_quic_conn_t *conn) {
  return ((struct _st_quicly_conn_public_t *)conn->quic)
      ->remote.address_validation.validated;
}

static size_t drain(accept_fixture_t *f) {
  uint8_t buf[2048];
  size_t count = 0;
  while (recv(f->client_fd, buf, sizeof(buf), 0) > 0)
    ++count;
  return count;
}

static void test_accept(const char *cert, const char *key) {
  clj_session_ticket_t ticket_key = ticket(0x66, 4);
  clj_ticket_manager_t *mgr = manager(&ticket_key, 1);
  accept_fixture_t f;
  accept_fixture_init(&f, cert, key, mgr);
  quicly_context_t *quic = f.worker->h3_ctx.super.quic;

  h2o_quic_conn_t *conn = client_initial(&f, ptls_iovec_init(NULL, 0));
  CHECK(conn != NULL && !validated(conn));

  /* The generator finds the worker's keys through the connection's data. */
  uint8_t token[256];
  ptls_buffer_t buf;
  ptls_buffer_init(&buf, token, sizeof(token));
  double now;
  quic->now->cb(quic->now, &now);
  quicly_address_token_plaintext_t plaintext =
      resumption_from(f.client_addr, (int64_t)now);
  CHECK(quic->generate_resumption_token->cb(quic->generate_resumption_token,
                                            conn->quic, &buf, &plaintext) == 0);
  CHECK(!buf.is_allocated && token[0] == 0x66);
  size_t len = buf.off;

  conn = client_initial(&f, ptls_iovec_init(token, len));
  CHECK(conn != NULL && validated(conn));

  uint8_t garbage[sizeof(token)];
  memcpy(garbage, token, len);
  garbage[len - 1] ^= 1;
  conn = client_initial(&f, ptls_iovec_init(garbage, len));
  CHECK(conn != NULL && !validated(conn));

  drain(&f);
  uint32_t before = clj_h2o_conn_limit_current();
  garbage[1] = QUICLY_ADDRESS_TOKEN_TYPE_RETRY;
  CHECK(client_initial(&f, ptls_iovec_init(garbage, len)) == NULL);
  CHECK(drain(&f) == 1);
  CHECK(clj_h2o_conn_limit_current() == before);

  garbage[0] = 0x67;
  conn = client_initial(&f, ptls_iovec_init(garbage, len));
  CHECK(conn != NULL && !validated(conn));
  CHECK(clj_h2o_conn_limit_current() == before + 1);
}

#if CLJ_HTTP3_CONNECTED_SOCKETS
static void receive_all(quicly_conn_t *conn, quicly_context_t *ctx,
                        quicly_address_t *dest, quicly_address_t *src,
                        struct iovec *datagrams, size_t num_datagrams) {
  for (size_t i = 0; i < num_datagrams; ++i) {
    size_t off = 0;
    while (off < datagrams[i].iov_len) {
      quicly_decoded_packet_t packet;
      if (quicly_decode_packet(ctx, &packet, datagrams[i].iov_base,
                               datagrams[i].iov_len, &off) == SIZE_MAX)
        break;
      quicly_receive(conn, &dest->sa, &src->sa, &packet);
    }
  }
}

static size_t send_all(quicly_conn_t *conn, struct iovec *datagrams,
                       uint8_t *buf, size_t size) {
  quicly_address_t dest, src;
  size_t num_datagrams = 10;
  CHECK(quicly_send(conn, &dest, &src, datagrams, &num_datagrams, buf, size) ==
        0);
  return num_datagrams;
}

/* The server sends through H2O, on its UDP socket, to the client's socket */
static void client_reads(accept_fixture_t *f, quicly_conn_t *client) {
  static uint8_t buf[1500];
  ssize_t len;
  while ((len = recv(f->client_fd, buf, sizeof(buf), 0)) > 0) {
    struct iovec datagram = {buf, (size_t)len};
    receive_all(client, &f->client_quic, &f->client_addr, &f->server_addr,
                &datagram, 1);
  }
}

static int connected_sockets(clj_http3_ctx_t *ctx) {
  return (int)ctx->num_connected;
}

static int count_fds(void) {
  int n = 0;
  DIR *dir = opendir("/proc/self/fd");
  CHECK(dir != NULL);
  while (readdir(dir) != NULL)
    ++n;
  closedir(dir);
  return n;
}

/* The worker's hook as H2O calls it during a read of the forwarding channel
   (clj_forwarded_quic_socket_on_read) */
static void forwarded_update(clj_http3_ctx_t *w, h2o_quic_conn_t *conn) {
  w->reading = CLJ_HTTP3_READING_FORWARDED;
  clj_http3_set_conn_update(w);
  w->h3_ctx.super.notify_conn_update(&w->h3_ctx.super, conn);
  w->reading = CLJ_HTTP3_READING_PUBLIC;
  clj_http3_set_conn_update(w);
}

static clj_http3_udp_transport_t *new_transport(void) {
  clj_http3_udp_transport_t *transport = calloc(1, sizeof(*transport));
  CHECK(transport != NULL);
  transport->reservation_fd = -1;
  CHECK(pthread_mutex_init(&transport->mutex, NULL) == 0);
  return transport;
}

/* Accepts a quicly client's connection on the fixture's worker; with
   `complete`, runs the handshake to its end, calling the hook as after a
   forwarded read at every step and checking that it connects nothing
   before the end */
static h2o_quic_conn_t *handshake(accept_fixture_t *f, quicly_conn_t **client,
                                  int complete) {
  clj_http3_ctx_t *w = f->worker;
  h2o_quic_ctx_t *quic_ctx = &w->h3_ctx.super;
  static quicly_cid_plaintext_t next_cid;
  ptls_iovec_t alpn = ptls_iovec_init("h3", 2);
  ptls_handshake_properties_t props = {
      .client = {.negotiated_protocols = {.list = &alpn, .count = 1}}};
  CHECK(quicly_connect(client, &f->client_quic, "localhost",
                       &f->server_addr.sa, NULL, &next_cid,
                       ptls_iovec_init(NULL, 0), &props, NULL, NULL) == 0);
  ++next_cid.master_id;
  static uint8_t buf[10 * 1500];
  struct iovec datagrams[10];
  size_t n = send_all(*client, datagrams, buf, sizeof(buf));
  quicly_decoded_packet_t first;
  size_t off = 0;
  CHECK(quicly_decode_packet(quic_ctx->quic, &first, datagrams[0].iov_base,
                             datagrams[0].iov_len, &off) != SIZE_MAX);
  h2o_quic_conn_t *conn =
      clj_on_http3_accept(quic_ctx, &f->server_addr, &f->client_addr, &first);
  CHECK(conn != NULL);
  receive_all(conn->quic, quic_ctx->quic, &f->server_addr, &f->client_addr,
              datagrams + 1, n - 1);
  forwarded_update(w, conn);
  CHECK(connected_sockets(w) == 0);
  for (int round = 0; complete && round < 8 &&
                      !ptls_handshake_is_complete(quicly_get_tls(conn->quic));
       ++round) {
    h2o_quic_send(conn);
    client_reads(f, *client);
    forwarded_update(w, conn);
    CHECK(connected_sockets(w) == 0);
    n = send_all(*client, datagrams, buf, sizeof(buf));
    receive_all(conn->quic, quic_ctx->quic, &f->server_addr, &f->client_addr,
                datagrams, n);
    if (!ptls_handshake_is_complete(quicly_get_tls(conn->quic))) {
      forwarded_update(w, conn);
      CHECK(connected_sockets(w) == 0);
    }
  }
  CHECK(!complete || ptls_handshake_is_complete(quicly_get_tls(conn->quic)));
  return conn;
}

/* A connection whose datagrams arrive forwarded gets a connected socket only
   once its handshake is complete, so a client that has not shown that it
   receives at its address never gets one; the destroy wrapper closes it */
static void test_connect_after_handshake(const char *cert, const char *key) {
  accept_fixture_t f;
  accept_fixture_init(&f, cert, key, NULL);
  clj_http3_ctx_t *w = f.worker;
  clj_http3_udp_transport_t *transport = new_transport();
  w->transport = transport;
  quicly_conn_t *client;
  h2o_quic_conn_t *conn = handshake(&f, &client, 1);
  int fds_before = count_fds();

  CHECK(w->h3_ctx.super.notify_conn_update == NULL);
  forwarded_update(w, conn);
  CHECK(connected_sockets(w) == 1 && count_fds() == fds_before + 1);
  CHECK(w->h3_ctx.super.notify_conn_update == clj_on_conn_update);
  CHECK(h2o_socket_compare_address(&w->connected[0]->peer.sa,
                                   &f.client_addr.sa, 1) == 0);
  CHECK(sendto(f.client_fd, "x", 1, 0, &f.server_addr.sa,
               sizeof(f.server_addr.sin)) == 1);
  char got[8];
  CHECK(recv(h2o_socket_get_fd(w->connected[0]->sock), got, sizeof(got),
             MSG_DONTWAIT) == 1);

  uint32_t limit_before = clj_h2o_conn_limit_current();
  clj_on_http3_conn_destroy(conn);
  CHECK(connected_sockets(w) == 0 &&
        atomic_load(&transport->num_connected) == 0);
  CHECK(clj_h2o_conn_limit_current() == limit_before - 1);
  CHECK(count_fds() == fds_before);
  CHECK(w->h3_ctx.super.notify_conn_update == NULL);
  quicly_free(client);
  w->transport = NULL;
  clj_h2o_http3_release_udp_transport(transport);
}

/* A client whose new path the kernel delivers to the connection's worker
   directly: H2O reads it from the listener's socket, the old path's socket
   closes and one for the new path replaces it, so a later sender on the old
   address and port reaches the listener's socket, not a stale one */
static void test_path_change_on_listener_socket(const char *cert,
                                                const char *key) {
  accept_fixture_t f;
  accept_fixture_init(&f, cert, key, NULL);
  clj_http3_ctx_t *w = f.worker;
  static const uint8_t cid_key[32] = {7};
  CHECK(clj_quicly_wrapper_set_cid_key(w->h3_ctx.super.quic, cid_key,
                                       sizeof(cid_key)));
  clj_http3_udp_transport_t *transport = new_transport();
  w->transport = transport;
  quicly_conn_t *client;
  h2o_quic_conn_t *conn = handshake(&f, &client, 1);
  forwarded_update(w, conn);
  CHECK(connected_sockets(w) == 1);

  quicly_address_t rebound;
  int rebound_fd = bound_udp(&rebound);
  CHECK(clj_set_fd_nonblock(rebound_fd) == 0);
  int fds_before = count_fds();
  /* quicly has validated and promoted the new path */
  memcpy(quicly_get_peername(conn->quic), &rebound, sizeof(rebound.sin));

  /* A short header datagram with the connection's CID, as a 1-RTT packet
     on the new path; it does not decrypt, H2O still reports the connection */
  const quicly_cid_t *dcid = quicly_get_remote_cid(client);
  uint8_t packet[64] = {0x40};
  memcpy(packet + 1, dcid->cid, dcid->len);
  memset(packet + 1 + dcid->len, 0x5a, 40);
  size_t len = 1 + dcid->len + 40;
  CHECK(sendto(rebound_fd, packet, len, 0, &f.server_addr.sa,
               sizeof(f.server_addr.sin)) == (ssize_t)len);
  h2o_quic_read_socket(&w->h3_ctx.super, w->udp_sock);

  CHECK(connected_sockets(w) == 1 && count_fds() == fds_before);
  CHECK(atomic_load(&transport->num_connected) == 1);
  CHECK(h2o_socket_compare_address(&w->connected[0]->peer.sa, &rebound.sa,
                                   1) == 0);
  char got[8];
  CHECK(sendto(f.client_fd, "o", 1, 0, &f.server_addr.sa,
               sizeof(f.server_addr.sin)) == 1);
  CHECK(sendto(rebound_fd, "n", 1, 0, &f.server_addr.sa,
               sizeof(f.server_addr.sin)) == 1);
  CHECK(recv(h2o_socket_get_fd(w->connected[0]->sock), got, sizeof(got),
             MSG_DONTWAIT) == 1 &&
        got[0] == 'n');
  CHECK(recv(h2o_socket_get_fd(w->udp_sock), got, sizeof(got), MSG_DONTWAIT) ==
            1 &&
        got[0] == 'o');

  clj_on_http3_conn_destroy(conn);
  CHECK(connected_sockets(w) == 0 && count_fds() == fds_before - 1);
  close(rebound_fd);
  quicly_free(client);
  w->transport = NULL;
  clj_h2o_http3_release_udp_transport(transport);
}

/* Sends the client's pending datagrams to the server connection */
static void server_reads(accept_fixture_t *f, quicly_conn_t *client,
                         h2o_quic_conn_t *conn) {
  static uint8_t buf[10 * 1500];
  struct iovec datagrams[10];
  size_t n = send_all(client, datagrams, buf, sizeof(buf));
  receive_all(conn->quic, f->worker->h3_ctx.super.quic, &f->server_addr,
              &f->client_addr, datagrams, n);
}

static uint32_t counted(clj_http3_ctx_t *w) {
  return atomic_load(&w->load->conns);
}

/* A worker counts the connections that can still carry requests: one is
   counted off when quicly reports it closed (closing or draining), and at
   destroy only if that did not happen, so connections in their closing
   period do not make the worker look loaded to new flows */
static void test_count_follows_close(const char *cert, const char *key) {
  accept_fixture_t f;
  accept_fixture_init(&f, cert, key, NULL);
  clj_http3_ctx_t *w0 = f.worker;
  h2o_quic_ctx_t *quic_ctx = &w0->h3_ctx.super;
  clj_http3_udp_transport_t *transport = new_transport();
  w0->transport = transport;
  int pair0[2], pair1[2];
  CHECK(socketpair(AF_UNIX, SOCK_DGRAM, 0, pair0) == 0);
  CHECK(socketpair(AF_UNIX, SOCK_DGRAM, 0, pair1) == 0);
  CHECK(clj_http3_transport_register_attachment(transport, 1, 0, pair0[1], w0));
  int fd1 = clj_h2o_http3_open_udp_fd("127.0.0.1", ntohs(f.server_addr.sin.sin_port));
  CHECK(fd1 != -1);
  clj_http3_ctx_t *w1 = clj_h2o_http3_create_worker_ctx_from_fd(
      &f.context, quic_ctx->loop, quic_ctx->quic, f.hosts, fd1, 1, 1);
  CHECK(w1 != NULL);
  w1->transport = transport;
  w1->forward_fd = pair1[1];
  CHECK(clj_http3_transport_register_attachment(transport, 1, 1, pair1[1], w1));
  clj_h2o_http3_activate_udp_transport_generation(transport, 1);

  quicly_conn_t *clients[6];
  h2o_quic_conn_t *conns[6];
  for (int i = 0; i < 4; ++i)
    conns[i] = handshake(&f, clients + i, 1);
  CHECK(counted(w0) == 4 && h2o_quic_num_connections(quic_ctx) == 4);

  /* The client closes: draining, no longer counted, still a connection */
  for (int i = 0; i < 2; ++i) {
    CHECK(quicly_close(clients[i], 0, "") == 0);
    server_reads(&f, clients[i], conns[i]);
    CHECK(quicly_get_state(conns[i]->quic) == QUICLY_STATE_DRAINING);
  }
  CHECK(counted(w0) == 2 && h2o_quic_num_connections(quic_ctx) == 4);

  /* A new flow read by this worker while those drain stays with it when its
     live connections are as few as the other worker's */
  atomic_store(&w1->load->conns, 2);
  uint64_t turn = h2o_now(quic_ctx->loop);
  CHECK(clj_http3_choose_worker(w0, turn) == w0->load);
  atomic_store(&w1->load->conns, 0);

  /* Destroy after the close counts nothing again */
  clj_on_http3_conn_destroy(conns[0]);
  clj_on_http3_conn_destroy(conns[1]);
  CHECK(counted(w0) == 2 && h2o_quic_num_connections(quic_ctx) == 2);

  /* The server closes: closing, no longer counted; destroy counts nothing */
  h2o_quic_close_connection(conns[2], 0, NULL);
  CHECK(quicly_get_state(conns[2]->quic) == QUICLY_STATE_CLOSING);
  CHECK(counted(w0) == 1);
  clj_on_http3_conn_destroy(conns[2]);
  CHECK(counted(w0) == 1);

  /* Destroyed without a close: counted off once, at destroy */
  conns[4] = handshake(&f, clients + 4, 1);
  CHECK(counted(w0) == 2);
  clj_on_http3_conn_destroy(conns[3]);
  CHECK(counted(w0) == 1);

  /* An idle timeout drains without a close reason: counted off at destroy */
  quicly_context_t *quic = quic_ctx->quic;
  uint64_t idle = quic->transport_params.max_idle_timeout;
  quic->transport_params.max_idle_timeout = 50;
  conns[5] = handshake(&f, clients + 5, 1);
  quic->transport_params.max_idle_timeout = idle;
  CHECK(counted(w0) == 2);
  usleep(400 * 1000);
  h2o_quic_send(conns[5]);
  CHECK(counted(w0) == 1 && h2o_quic_num_connections(quic_ctx) == 1);

  clj_on_http3_conn_destroy(conns[4]);
  CHECK(counted(w0) == 0 && h2o_quic_num_connections(quic_ctx) == 0);
  for (int i = 0; i < 6; ++i)
    quicly_free(clients[i]);
  clj_h2o_http3_dispose_worker_ctx(w1);
  clj_http3_transport_unregister_attachment(transport, 1, 0, pair0[1]);
  w0->transport = NULL;
  w0->load = NULL;
  close(pair0[0]);
  close(pair0[1]);
  close(pair1[0]);
  clj_h2o_http3_release_udp_transport(transport);
}
#endif

int main(int argc, char **argv) {
  CHECK(argc == 3);
  test_key_matches_h2o();
  test_validation();
  test_rotation_and_reload();
  test_rebuild_during_swaps();
  test_without_keys();
  test_accept(argv[1], argv[2]);
#if CLJ_HTTP3_CONNECTED_SOCKETS
  test_connect_after_handshake(argv[1], argv[2]);
  test_path_change_on_listener_socket(argv[1], argv[2]);
  test_count_follows_close(argv[1], argv[2]);
#endif
  puts("QUIC address token tests passed");
  return 0;
}
