/* Drive picotls clients against the HTTP/3 TLS context: the key exchange
   groups it offers, whether a client needs a HelloRetryRequest, and whether
   a ticket from an earlier handshake resumes with early data.
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

/* The client sends a key share for the first of its groups only and, like
   curl and browsers, offers resumption only with a new key exchange */
static ptls_context_t client_ctx(ptls_key_exchange_algorithm_t **groups) {
  return (ptls_context_t){
      .random_bytes = ptls_openssl_random_bytes,
      .get_time = &ptls_get_time,
      .key_exchanges = groups,
      .cipher_suites = ptls_openssl_cipher_suites,
      .save_ticket = &save_ticket,
      .require_dhe_on_psk = 1,
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
  ptls_context_t client = client_ctx(groups);

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
  ptls_context_t client = client_ctx(groups);

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
  ptls_context_t client = client_ctx(groups);

  handshake_result_t full = handshake(server, &client, 0);
  CHECK(full.server_flights == 2);
  CHECK(full.group == PTLS_GROUP_SECP256R1);
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

  clj_h2o_free_ptls_ctx(server);
  clj_ticket_manager_destroy(ticket_mgr);
  puts("TLS key exchange tests passed");
  return 0;
}
