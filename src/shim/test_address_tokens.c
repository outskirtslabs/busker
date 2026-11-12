/* Exercise the private token callbacks without adding test-only shim exports.
   Arguments: certificate and private key PEM files for the accept tests. */
#include "shim.c"
#include <openssl/aead.h>
#include <openssl/hkdf.h>
#include <openssl/sha.h>

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
static int64_t test_now(quicly_now_t *self) {
  (void)self;
  return clock_ms;
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

  /* quicly decodes an IPv6 token's scope ID from the first four bytes of the
     address (deps/quicly/lib/quicly.c:8171-8172 does not advance past the
     address), so only addresses starting with four zero bytes, such as ::1,
     match; h2o's server behaves the same. */
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
  int server_fd = bound_udp(&f->server_addr);
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
  quicly_address_token_plaintext_t plaintext =
      resumption_from(f.client_addr, quic->now->cb(quic->now));
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

int main(int argc, char **argv) {
  CHECK(argc == 3);
  test_key_matches_h2o();
  test_validation();
  test_rotation_and_reload();
  test_rebuild_during_swaps();
  test_without_keys();
  test_accept(argv[1], argv[2]);
  puts("QUIC address token tests passed");
  return 0;
}
