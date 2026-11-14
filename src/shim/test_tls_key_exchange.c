/* Drive picotls clients against the HTTP/3 TLS context: the key exchange
   groups it offers, whether a client needs a HelloRetryRequest, whether a
   ticket from an earlier handshake resumes with early data, and that every
   resumption has a new key exchange.
   Arguments: certificate and private key PEM files. */
#include "shim.c"

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);          \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

typedef struct {
  int server_flights;
  int resumed;
  ptls_early_data_acceptance_t early_data;
  uint16_t group;
} handshake_result_t;

static uint8_t saved_ticket[4096];
static size_t saved_ticket_len;

static int save_ticket_cb(ptls_save_ticket_t *self, ptls_t *tls,
                          ptls_iovec_t input,
                          const ptls_save_ticket_properties_t *properties) {
  (void)self;
  (void)tls;
  (void)properties;
  CHECK(input.len <= sizeof(saved_ticket));
  memcpy(saved_ticket, input.base, input.len);
  saved_ticket_len = input.len;
  return 0;
}

static ptls_save_ticket_t save_ticket = {save_ticket_cb};

/* The client sends a key share for the first of its groups only. With
   require_dhe it lists psk_dhe_ke alone, as curl and browsers do; without, it
   lists psk_ke and psk_dhe_ke. */
static ptls_context_t client_ctx(ptls_key_exchange_algorithm_t **groups,
                                 int require_dhe) {
  return (ptls_context_t){
      .random_bytes = ptls_openssl_random_bytes,
      .get_time = &ptls_get_time,
      .key_exchanges = groups,
      .cipher_suites = ptls_openssl_cipher_suites,
      .save_ticket = &save_ticket,
      .require_dhe_on_psk = require_dhe,
  };
}

/* A server flight that is not the last one is a HelloRetryRequest */
static handshake_result_t handshake(ptls_context_t *server_ctx,
                                    ptls_context_t *client_ctx,
                                    int with_ticket) {
  handshake_result_t result = {0};
  ptls_t *client = ptls_new(client_ctx, 0);
  ptls_t *server = ptls_new(server_ctx, 1);
  CHECK(client != NULL && server != NULL);
  ptls_set_server_name(client, "localhost", 0);

  size_t max_early_data_size = 0;
  ptls_handshake_properties_t client_props = {0};
  if (with_ticket) {
    CHECK(saved_ticket_len != 0);
    client_props.client.session_ticket =
        ptls_iovec_init(saved_ticket, saved_ticket_len);
    client_props.client.max_early_data_size = &max_early_data_size;
  }
  saved_ticket_len = 0;

  ptls_buffer_t cbuf, sbuf, decbuf;
  ptls_buffer_init(&cbuf, "", 0);
  ptls_buffer_init(&sbuf, "", 0);
  ptls_buffer_init(&decbuf, "", 0);

  CHECK(ptls_handshake(client, &cbuf, NULL, NULL, &client_props) ==
        PTLS_ERROR_IN_PROGRESS);
  int client_ret = PTLS_ERROR_IN_PROGRESS;
  size_t consumed;
  while (client_ret == PTLS_ERROR_IN_PROGRESS) {
    CHECK(result.server_flights < 4);
    consumed = cbuf.off;
    int server_ret = ptls_handshake(server, &sbuf, cbuf.base, &consumed, NULL);
    CHECK(server_ret == 0 || server_ret == PTLS_ERROR_IN_PROGRESS);
    CHECK(consumed == cbuf.off);
    cbuf.off = 0;
    ++result.server_flights;

    consumed = sbuf.off;
    client_ret =
        ptls_handshake(client, &cbuf, sbuf.base, &consumed, &client_props);
    CHECK(client_ret == 0 || client_ret == PTLS_ERROR_IN_PROGRESS);
    CHECK(client_ret == 0 || consumed == sbuf.off);
    CHECK(client_ret == 0 || server_ret == PTLS_ERROR_IN_PROGRESS);
    if (client_ret != 0)
      sbuf.off = 0;
  }

  /* NewSessionTicket follows the server's Finished in the same flight */
  size_t ticket_off = consumed;
  consumed = sbuf.off - ticket_off;
  CHECK(ptls_receive(client, &decbuf, sbuf.base + ticket_off, &consumed) == 0);
  CHECK(saved_ticket_len != 0);

  consumed = cbuf.off;
  CHECK(ptls_receive(server, &decbuf, cbuf.base, &consumed) == 0);
  CHECK(consumed == cbuf.off);
  CHECK(ptls_handshake_is_complete(server));

  result.resumed = ptls_is_psk_handshake(server);
  result.early_data = client_props.client.early_data_acceptance;
  result.group = ptls_get_negotiated_group(server);

  ptls_buffer_dispose(&cbuf);
  ptls_buffer_dispose(&sbuf);
  ptls_buffer_dispose(&decbuf);
  ptls_free(client);
  ptls_free(server);
  return result;
}

static void offers_h2o_server_groups(ptls_context_t *server) {
  ptls_key_exchange_algorithm_t *expected[] = {
      &ptls_openssl_x25519, &ptls_openssl_secp256r1, &ptls_openssl_secp384r1,
      NULL};
  for (size_t i = 0; i != sizeof(expected) / sizeof(expected[0]); ++i)
    CHECK(server->key_exchanges[i] == expected[i]);
}

/* curl and browsers send their x25519 key share in the first ClientHello */
static void x25519_client_needs_one_flight_and_resumes(ptls_context_t *server) {
  ptls_key_exchange_algorithm_t *groups[] = {&ptls_openssl_x25519,
                                             &ptls_openssl_secp256r1, NULL};
  ptls_context_t client = client_ctx(groups, 1);

  handshake_result_t full = handshake(server, &client, 0);
  CHECK(full.server_flights == 1);
  CHECK(!full.resumed);
  CHECK(full.group == PTLS_GROUP_X25519);

  handshake_result_t resumed = handshake(server, &client, 1);
  CHECK(resumed.server_flights == 1);
  CHECK(resumed.resumed);
  CHECK(resumed.early_data == PTLS_EARLY_DATA_ACCEPTED);
  CHECK(resumed.group == PTLS_GROUP_X25519);
}

static void secp384r1_client_needs_one_flight(ptls_context_t *server) {
  ptls_key_exchange_algorithm_t *groups[] = {&ptls_openssl_secp384r1, NULL};
  ptls_context_t client = client_ctx(groups, 1);

  handshake_result_t full = handshake(server, &client, 0);
  CHECK(full.server_flights == 1);
  CHECK(full.group == PTLS_GROUP_SECP384R1);
}

/* A first key share in a group the server lacks still gets a retry, which
   picks the client's first group the server supports */
static void unsupported_first_share_gets_a_retry(ptls_context_t *server) {
  ptls_key_exchange_algorithm_t *groups[] = {
      &ptls_openssl_secp521r1, &ptls_openssl_secp256r1, &ptls_openssl_x25519,
      NULL};
  ptls_context_t client = client_ctx(groups, 1);

  handshake_result_t full = handshake(server, &client, 0);
  CHECK(full.server_flights == 2);
  CHECK(full.group == PTLS_GROUP_SECP256R1);
}

/* picotls picks the mode without a key exchange whenever the client lists it */
static void psk_ke_beside_psk_dhe_ke_resumes_with_a_key_exchange(
    ptls_context_t *server) {
  ptls_key_exchange_algorithm_t *groups[] = {&ptls_openssl_x25519, NULL};
  ptls_context_t client = client_ctx(groups, 0);

  handshake(server, &client, 0);
  handshake_result_t resumed = handshake(server, &client, 1);
  CHECK(resumed.resumed);
  CHECK(resumed.group == PTLS_GROUP_X25519);
}

/* Rewrites the modes list psk_ke, psk_dhe_ke of a ClientHello record to
   psk_ke, psk_ke and recomputes the binder from the ticket's PSK, since a
   picotls client cannot list psk_ke alone */
static void list_psk_ke_alone(uint8_t *record, size_t len,
                              const uint8_t *ticket, size_t ticket_len) {
  static const uint8_t modes[] = {0x00, 0x2d, 0x00, 0x03, 0x02, 0x00, 0x01};
  uint8_t *found = memmem(record, len, modes, sizeof(modes));
  CHECK(found != NULL);
  found[sizeof(modes) - 1] = 0x00;

  /* A stored ticket: obtained_at(8), group(2), cipher suite(2),
     NewSessionTicket<3>, PSK<2> */
  CHECK(ticket_len > 15);
  uint16_t csid = (uint16_t)(ticket[10] << 8 | ticket[11]);
  const uint8_t *psk = ticket + 12;
  psk += 3 + ((size_t)psk[0] << 16 | (size_t)psk[1] << 8 | psk[2]);
  CHECK(psk + 2 <= ticket + ticket_len);
  size_t psk_len = (size_t)psk[0] << 8 | psk[1];
  psk += 2;
  CHECK(psk + psk_len == ticket + ticket_len);
  ptls_cipher_suite_t *suite = NULL;
  for (ptls_cipher_suite_t **c = ptls_openssl_cipher_suites; *c != NULL; ++c)
    if ((*c)->id == csid)
      suite = *c;
  CHECK(suite != NULL);
  ptls_hash_algorithm_t *hash = suite->hash;
  size_t hash_len = hash->digest_size;

  /* One ClientHello in one record, the binder of the only identity last */
  CHECK(len > 9 && record[0] == 22 /* handshake */);
  CHECK(len == 9 + ((size_t)record[6] << 16 | (size_t)record[7] << 8 |
                    record[8]));
  CHECK(record[len - hash_len - 1] == hash_len);
  size_t binders_len = 2 + 1 + hash_len;

  uint8_t zeros[PTLS_MAX_DIGEST_SIZE] = {0};
  uint8_t early_secret[PTLS_MAX_DIGEST_SIZE];
  uint8_t binder_key[PTLS_MAX_DIGEST_SIZE];
  uint8_t finished_key[PTLS_MAX_DIGEST_SIZE];
  uint8_t transcript[PTLS_MAX_DIGEST_SIZE];
  CHECK(ptls_hkdf_extract(hash, early_secret, ptls_iovec_init(zeros, hash_len),
                          ptls_iovec_init(psk, psk_len)) == 0);
  CHECK(ptls_hkdf_expand_label(hash, binder_key, hash_len,
                               ptls_iovec_init(early_secret, hash_len),
                               "res binder",
                               ptls_iovec_init(hash->empty_digest, hash_len),
                               NULL) == 0);
  CHECK(ptls_hkdf_expand_label(hash, finished_key, hash_len,
                               ptls_iovec_init(binder_key, hash_len),
                               "finished", ptls_iovec_init(NULL, 0),
                               NULL) == 0);
  CHECK(ptls_calc_hash(hash, transcript, record + 5,
                       len - 5 - binders_len) == 0);
  ptls_hash_context_t *hmac = ptls_hmac_create(hash, finished_key, hash_len);
  CHECK(hmac != NULL);
  hmac->update(hmac, transcript, hash_len);
  hmac->final(hmac, record + len - hash_len, PTLS_HASH_FINAL_MODE_FREE);
}

/* With psk_ke alone the server cannot resume and does a full handshake */
static void psk_ke_alone_gets_a_full_handshake(ptls_context_t *server_ctx) {
  ptls_key_exchange_algorithm_t *groups[] = {&ptls_openssl_x25519, NULL};
  ptls_context_t client_context = client_ctx(groups, 0);
  handshake(server_ctx, &client_context, 0);

  ptls_t *client = ptls_new(&client_context, 0);
  ptls_t *server = ptls_new(server_ctx, 1);
  CHECK(client != NULL && server != NULL);
  ptls_set_server_name(client, "localhost", 0);
  ptls_handshake_properties_t client_props = {0};
  client_props.client.session_ticket =
      ptls_iovec_init(saved_ticket, saved_ticket_len);

  ptls_buffer_t cbuf, sbuf;
  ptls_buffer_init(&cbuf, "", 0);
  ptls_buffer_init(&sbuf, "", 0);
  CHECK(ptls_handshake(client, &cbuf, NULL, NULL, &client_props) ==
        PTLS_ERROR_IN_PROGRESS);
  list_psk_ke_alone(cbuf.base, cbuf.off, saved_ticket, saved_ticket_len);

  size_t consumed = cbuf.off;
  int ret = ptls_handshake(server, &sbuf, cbuf.base, &consumed, NULL);
  CHECK(ret == 0 || ret == PTLS_ERROR_IN_PROGRESS);
  CHECK(!ptls_is_psk_handshake(server));
  CHECK(ptls_get_negotiated_group(server) == PTLS_GROUP_X25519);

  ptls_buffer_dispose(&cbuf);
  ptls_buffer_dispose(&sbuf);
  ptls_free(client);
  ptls_free(server);
}

int main(int argc, char **argv) {
  CHECK(argc == 3);
  clj_ticket_manager_t *ticket_mgr = clj_ticket_manager_create(3600);
  CHECK(ticket_mgr != NULL);
  ptls_context_t *server = clj_h2o_create_ptls_ctx(argv[1], argv[2], NULL, NULL);
  CHECK(server != NULL);
  clj_encrypt_ticket_t *encrypt_ticket =
      clj_ticket_manager_create_encrypt_ticket(ticket_mgr, 1);
  CHECK(encrypt_ticket != NULL);
  CHECK(clj_ptls_ctx_set_tickets(server, encrypt_ticket, 3600, 8192) == 0);

  x25519_client_needs_one_flight_and_resumes(server);
  secp384r1_client_needs_one_flight(server);
  unsupported_first_share_gets_a_retry(server);
  offers_h2o_server_groups(server);
  psk_ke_beside_psk_dhe_ke_resumes_with_a_key_exchange(server);
  psk_ke_alone_gets_a_full_handshake(server);

  clj_h2o_free_ptls_ctx(server);
  clj_ticket_manager_destroy(ticket_mgr);
  puts("TLS key exchange tests passed");
  return 0;
}
