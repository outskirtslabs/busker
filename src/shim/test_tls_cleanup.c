/* Check that TLS and QUIC context setup and release return all heap memory,
   also when setup fails part-way. Fails when the heap statistics do not
   follow a probe allocation. Skips, printing why, only on a libc without heap
   statistics or when CLJ_SKIP_HEAP_TESTS=1 asks for it (valgrind, a replaced
   allocator). The macOS statistics have not been checked on a Mac.
   Arguments: certificate and private key PEM files. */
#include "shim.c"
#if defined(__GLIBC__)
#include <malloc.h>
#elif defined(__APPLE__)
#include <malloc/malloc.h>
#endif

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);          \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

#define WARMUP_ROUNDS 32
#define ROUNDS 1000
#define ALLOWED_GROWTH 4096

static const char *cert_file;
static const char *key_file;
static char junk_file[] = "/tmp/busker-tls-cleanup-XXXXXX";
static char missing_file[sizeof(junk_file) + 8];
static h2o_globalconf_t globalconf;
static clj_ticket_manager_t *ticket_mgr;
static int failures;

/* glibc counts mmapped blocks in hblkhd, not uordblks */
static int heap_in_use(size_t *in_use) {
#if defined(__GLIBC__)
  struct mallinfo2 info = mallinfo2();
  *in_use = info.uordblks + info.hblkhd;
  return 1;
#elif defined(__APPLE__)
  malloc_statistics_t stats;
  malloc_zone_statistics(NULL, &stats);
  *in_use = stats.size_in_use;
  return 1;
#else
  (void)in_use;
  return 0;
#endif
}

static size_t heap_now(void) {
  size_t in_use = 0;
  CHECK(heap_in_use(&in_use));
  return in_use;
}

/* One size above glibc's tcache and below its default mmap threshold, one
   above that threshold, so the probe sees both kinds of block */
static int heap_follows(size_t size) {
  size_t before = heap_now();
  char *volatile probe = malloc(size);
  CHECK(probe != NULL);
  memset(probe, 1, size);
  size_t allocated = heap_now();
  free(probe);
  size_t released = heap_now();
  /* glibc rounds an mmapped block up to whole pages */
  size_t slack = ALLOWED_GROWTH + (size_t)sysconf(_SC_PAGESIZE);
  if (allocated >= before + size && allocated <= before + size + slack &&
      released <= before + ALLOWED_GROWTH)
    return 1;
  fprintf(stderr,
          "heap statistics do not follow a %zu byte block (before %zu, "
          "allocated %zu, released %zu); set CLJ_SKIP_HEAP_TESTS=1 under "
          "valgrind or a replaced allocator\n",
          size, before, allocated, released);
  return 0;
}

/* Warm-up rounds let OpenSSL finish its lazy per-thread setup first */
static void check_no_growth(const char *name, void (*round)(void)) {
  for (int i = 0; i < WARMUP_ROUNDS; ++i) {
    round();
    ERR_clear_error();
  }
  size_t before = heap_now();
  for (int i = 0; i < ROUNDS; ++i) {
    round();
    ERR_clear_error();
  }
  size_t after = heap_now();
  if (after > before + ALLOWED_GROWTH) {
    fprintf(stderr, "%s: heap grew by %zu bytes over %d rounds\n", name,
            after - before, ROUNDS);
    ++failures;
  }
}

static void ptls_with_junk_key(void) {
  CHECK(clj_h2o_create_ptls_ctx(cert_file, junk_file, NULL, NULL) == NULL);
}

static void ptls_with_missing_key(void) {
  CHECK(clj_h2o_create_ptls_ctx(cert_file, missing_file, NULL, NULL) == NULL);
}

static void ptls_with_junk_cert(void) {
  CHECK(clj_h2o_create_ptls_ctx(junk_file, key_file, NULL, NULL) == NULL);
}

static void ptls_with_static_identity(void) {
  ptls_context_t *ptls = clj_h2o_create_ptls_ctx(cert_file, key_file, NULL, NULL);
  CHECK(ptls != NULL);
  clj_h2o_free_ptls_ctx(ptls);
}

static void ptls_with_session_tickets(void) {
  ptls_context_t *ptls = clj_h2o_create_ptls_ctx(NULL, NULL, NULL, NULL);
  CHECK(ptls != NULL);
  clj_encrypt_ticket_t *encrypt_ticket =
      clj_ticket_manager_create_encrypt_ticket(ticket_mgr, 1);
  CHECK(encrypt_ticket != NULL);
  CHECK(clj_ptls_ctx_set_tickets(ptls, encrypt_ticket, 3600, 8192) == 0);
  clj_h2o_free_ptls_ctx(ptls);
}

/* A second call is refused and leaves the first callback in place */
static void ptls_with_session_tickets_set_twice(void) {
  ptls_context_t *ptls = clj_h2o_create_ptls_ctx(NULL, NULL, NULL, NULL);
  CHECK(ptls != NULL);
  clj_encrypt_ticket_t *first =
      clj_ticket_manager_create_encrypt_ticket(ticket_mgr, 1);
  clj_encrypt_ticket_t *second =
      clj_ticket_manager_create_encrypt_ticket(ticket_mgr, 1);
  CHECK(first != NULL && second != NULL);
  CHECK(clj_ptls_ctx_set_tickets(ptls, first, 3600, 8192) == 0);
  CHECK(clj_ptls_ctx_set_tickets(ptls, first, 60, 0) == -1);
  CHECK(clj_ptls_ctx_set_tickets(ptls, second, 60, 0) == -1);
  CHECK(ptls->encrypt_ticket == &first->super);
  CHECK(ptls->ticket_lifetime == 3600);
  free(second);
  clj_h2o_free_ptls_ctx(ptls);
}

static int lookup_status;
static size_t lookup_key_len;

static int lookup_returning_copies(const uint8_t *sni, size_t sni_len,
                                   uint8_t **cert_out, size_t *cert_len_out,
                                   uint8_t **key_out, size_t *key_len_out,
                                   void *user_ctx) {
  (void)sni;
  (void)sni_len;
  (void)user_ctx;
  *cert_out = clj_h2o_tls_memdup("CERT", 4);
  *cert_len_out = 4;
  *key_out = clj_h2o_tls_memdup("KEY", 3);
  *key_len_out = lookup_key_len;
  return lookup_status;
}

/* Outputs the callback filled are freed on every status but a usable 1 */
static void lookup_rejected(void) {
  static const struct {
    int status;
    size_t key_len;
  } cases[] = {{-1, 3}, {0, 3}, {2, 3}, {1, 0}};
  for (size_t i = 0; i != sizeof(cases) / sizeof(cases[0]); ++i) {
    uint8_t *cert = NULL, *key = NULL;
    size_t cert_len = 0, key_len = 0;
    lookup_status = cases[i].status;
    lookup_key_len = cases[i].key_len;
    int status = clj_tls_lookup_pem_for_sni(lookup_returning_copies, NULL,
                                            (const uint8_t *)"a.test", 6,
                                            &cert, &cert_len, &key, &key_len);
    CHECK(status == (cases[i].key_len == 0 ? -1 : cases[i].status));
    CHECK(cert == NULL && key == NULL);
  }
}

/* Each worker attach replaces the CID encryptor of the shared quicly context */
static void quicly_with_cid_key_replacements(void) {
  uint8_t cid_key[32] = {1, 2, 3};
  ptls_context_t *ptls = clj_h2o_create_ptls_ctx(NULL, NULL, NULL, NULL);
  CHECK(ptls != NULL);
  quicly_context_t *quic = clj_h2o_create_quicly_ctx(ptls, &globalconf);
  CHECK(quic != NULL);
  CHECK(clj_quicly_wrapper_set_cid_key(quic, cid_key, sizeof(cid_key)));
  CHECK(clj_quicly_wrapper_set_cid_key(quic, cid_key, sizeof(cid_key)));
  clj_h2o_free_quicly_ctx(quic);
  clj_h2o_free_ptls_ctx(ptls);
}

int main(int argc, char **argv) {
  CHECK(argc == 3);
  cert_file = argv[1];
  key_file = argv[2];

  const char *skip = getenv("CLJ_SKIP_HEAP_TESTS");
  size_t in_use;
  if (skip != NULL && strcmp(skip, "1") == 0) {
    puts("TLS cleanup tests skipped: CLJ_SKIP_HEAP_TESTS=1");
    return 0;
  }
  if (!heap_in_use(&in_use)) {
    puts("TLS cleanup tests skipped: no heap statistics for this libc");
    return 0;
  }
  CHECK(heap_follows(16 * 1024));
  CHECK(heap_follows(256 * 1024));

  int fd = mkstemp(junk_file);
  CHECK(fd != -1);
  static const char junk[] = "not a PEM object\n";
  CHECK(write(fd, junk, sizeof(junk) - 1) == (ssize_t)(sizeof(junk) - 1));
  close(fd);
  snprintf(missing_file, sizeof(missing_file), "%s.missing", junk_file);

  h2o_config_init(&globalconf);
  ticket_mgr = clj_ticket_manager_create(3600);
  CHECK(ticket_mgr != NULL);

  check_no_growth("ptls context with a junk key file", ptls_with_junk_key);
  check_no_growth("ptls context with a missing key file", ptls_with_missing_key);
  check_no_growth("ptls context with a junk certificate file",
                  ptls_with_junk_cert);
  check_no_growth("ptls context with a static identity",
                  ptls_with_static_identity);
  check_no_growth("ptls context with session tickets",
                  ptls_with_session_tickets);
  check_no_growth("ptls context with session tickets set twice",
                  ptls_with_session_tickets_set_twice);
  check_no_growth("TLS lookup outputs on a rejected status", lookup_rejected);
  check_no_growth("quicly context with CID key replacements",
                  quicly_with_cid_key_replacements);

  clj_ticket_manager_destroy(ticket_mgr);
  unlink(junk_file);
  if (failures != 0)
    return 1;
  puts("TLS cleanup tests passed");
  return 0;
}
