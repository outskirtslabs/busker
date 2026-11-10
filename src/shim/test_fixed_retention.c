/* Exercise slot lifetime with a real H2O final-send callback. */
#include "shim.c"

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);            \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

typedef struct {
  h2o_ostream_t super;
  clj_req_ctx_t *ctx;
  clj_mt_receiver_t *receiver;
  uint64_t replacement;
  int reentrant;
  int allows_body;
} test_output_t;

static uint64_t publish(clj_mt_receiver_t *receiver, uint64_t sequence,
                        int status);

static void final_send(h2o_ostream_t *stream, h2o_req_t *req,
                       h2o_sendvec_t *bufs, size_t count,
                       h2o_send_state_t state) {
  (void)req;
  test_output_t *output = (test_output_t *)stream;
  clj_response_slot_t *slot = output->ctx->fixed_slot;
  CHECK(state == H2O_SEND_STATE_FINAL);
  CHECK(clj_h2o_response_ring_ready(output->receiver) == 0);
  CHECK(clj_h2o_response_ring_retained(output->receiver) == 1);
  CHECK(clj_h2o_response_try_claim(output->receiver) == 0);
  CHECK(count == (size_t)output->allows_body);
  if (count != 0) {
    CHECK(bufs[0].len == 3);
    CHECK(bufs[0].raw == (char *)response_slot_payload(&slot->data));
    CHECK(memcmp(bufs[0].raw, "cat", 3) == 0);
  }
  if (output->reentrant) {
    cleanup_request(output->ctx);
    output->replacement = output->reentrant == 2
                              ? publish(output->receiver, 99, 200)
                              : clj_h2o_response_try_claim(output->receiver);
    CHECK(output->replacement != 0);
  }
}

static uint64_t publish(clj_mt_receiver_t *receiver, uint64_t sequence,
                        int status) {
  uint64_t claim = clj_h2o_response_try_claim(receiver);
  CHECK(claim != 0);
  clj_response_slot_t *slot = response_slot_from_handle(receiver, claim);
  slot->data.module_id = 1;
  slot->data.request_seq = sequence;
  slot->data.status = status;
  slot->data.headers_len = 0;
  slot->data.content_length = 3;
  slot->data.body_offset = 0;
  slot->data.body_len = 3;
  slot->data.payload_len = 3;
  memcpy(response_slot_payload(&slot->data), "cat", 3);
  CHECK(clj_h2o_response_publish(receiver, claim) == 1);
  return claim;
}

static void test_final(clj_mt_receiver_t *receiver, int reentrant,
                       const char *method, int status) {
  h2o_req_t req = {0};
  h2o_mem_init_pool(&req.pool);
  req.input.method = h2o_iovec_init(method, strlen(method));
  clj_req_ctx_t ctx = {.req = &req,
                      .response_receiver = receiver,
                      .dispatch_module_id = 1,
                      .dispatch_request_seq = 1};
  test_output_t output = {.super = {.do_send = final_send},
                          .ctx = &ctx,
                          .receiver = receiver,
                          .reentrant = reentrant,
                          .allows_body = strcmp(method, "HEAD") != 0 &&
                                         status != 204 && status != 205 &&
                                         status != 304};
  req._ostr_top = &output.super;
  CHECK(register_response_request(receiver, &ctx, 1));
  CHECK(clj_h2o_response_use_acquire(receiver) == 1);
  uint64_t claim = publish(receiver, 1, status);
  CHECK(clj_h2o_response_ring_drain(receiver) == 1);
  if (!reentrant) {
    CHECK(clj_h2o_response_ring_retained(receiver) == 1);
    CHECK(clj_h2o_response_try_claim(receiver) == 0);
    cleanup_request(&ctx);
    output.replacement = clj_h2o_response_try_claim(receiver);
  }
  CHECK(output.replacement != 0 && output.replacement != claim);
  cleanup_request(&ctx);
  CHECK(clj_h2o_response_abort(receiver, claim) == 0);
  CHECK(clj_h2o_response_ring_retained(receiver) == 0);
  if (reentrant == 2) {
    CHECK(clj_h2o_response_ring_claimed(receiver) == 0);
    CHECK(clj_h2o_response_ring_ready(receiver) == 1);
    CHECK(clj_h2o_response_ring_drain(receiver) == 1);
    CHECK(clj_h2o_response_ring_ready(receiver) == 0);
    CHECK(clj_h2o_response_ring_drain(receiver) == 0);
  } else {
    CHECK(clj_h2o_response_ring_claimed(receiver) == 1);
    CHECK(clj_h2o_response_abort(receiver, output.replacement) == 1);
  }
  CHECK(clj_h2o_mt_response_pending(receiver) == 1);
  clj_h2o_response_use_release(receiver);
  CHECK(clj_h2o_mt_response_pending(receiver) == 0);
  h2o_mem_clear_pool(&req.pool);
}

static void test_independent_release(h2o_context_t *context) {
  clj_mt_receiver_t *receiver = clj_h2o_mt_create_response_receiver(context, 256, 64);
  CHECK(receiver != NULL);
  clj_req_ctx_t contexts[256] = {0};
  uint64_t claims[256];
  for (size_t i = 0; i < 256; ++i) {
    claims[i] = publish(receiver, i + 1, 200);
    clj_response_slot_t *slot = response_slot_from_handle(receiver, claims[i]);
    contexts[i].response_receiver = receiver;
    contexts[i].fixed_slot = slot;
    contexts[i].fixed_claim = claims[i];
    /* Model the worker detaching discovery before retaining this slot. */
    atomic_fetch_and(&receiver->ready_words[i / 64],
                     ~(UINT64_C(1) << (i % 64)));
    atomic_store(&slot->state, CLJ_RESPONSE_SLOT_IN_NATIVE);
    atomic_fetch_add(&receiver->ring_retained, 1);
    atomic_fetch_sub(&receiver->ring_ready, 1);
  }
  CHECK(clj_h2o_response_try_claim(receiver) == 0);
  const size_t order[] = {255, 17, 0, 128};
  for (size_t i = 0; i < sizeof(order) / sizeof(order[0]); ++i) {
    size_t index = order[i];
    release_fixed_slot(&contexts[index]);
    uint64_t replacement = clj_h2o_response_try_claim(receiver);
    CHECK((replacement & CLJ_RESPONSE_HANDLE_INDEX_MASK) == index + 1);
    CHECK(clj_h2o_response_abort(receiver, claims[index]) == 0);
    CHECK(clj_h2o_response_abort(receiver, replacement) == 1);
    /* Keep the other released entries unavailable for the next exact reuse. */
    claims[index] = clj_h2o_response_try_claim(receiver);
  }
  for (size_t i = 0; i < 256; ++i) {
    if (contexts[i].fixed_slot != NULL)
      release_fixed_slot(&contexts[i]);
    else
      CHECK(clj_h2o_response_abort(receiver, claims[i]) == 1);
  }
  CHECK(clj_h2o_mt_response_pending(receiver) == 0);
  clj_h2o_mt_destroy_response_receiver(receiver);
}

static void test_discovery(h2o_context_t *context, size_t capacity) {
  clj_mt_receiver_t *receiver =
      clj_h2o_mt_create_response_receiver(context, capacity, 3);
  CHECK(receiver != NULL);
  CHECK(receiver->ready_word_count == (capacity + 63) / 64);
  CHECK(clj_h2o_response_use_acquire(receiver) == 1);
  atomic_store(&receiver->claim_cursor, capacity - 1);
  uint64_t last = publish(receiver, 99, 200);
  CHECK((last & CLJ_RESPONSE_HANDLE_INDEX_MASK) == capacity);
  CHECK(atomic_load(&receiver->ready_words[(capacity - 1) / 64]) ==
        (UINT64_C(1) << ((capacity - 1) % 64)));
  CHECK(clj_h2o_response_ring_drain(receiver) == 1);
  CHECK(clj_h2o_response_abort(receiver, last) == 0);
  CHECK(clj_h2o_response_ring_ready(receiver) == 0);
  CHECK(clj_h2o_mt_response_pending(receiver) == 1);

  /* Pause a publication after READY, before its discovery bit. No worker may
     consume the slot early and leave a late bit referring to a recycled slot. */
  uint64_t late = clj_h2o_response_try_claim(receiver);
  clj_response_slot_t *slot = response_slot_from_handle(receiver, late);
  CHECK(slot != NULL);
  slot->data.module_id = 0; /* Invalid identity is discarded without native send. */
  atomic_fetch_add_explicit(&receiver->ring_ready, 1, memory_order_release);
  atomic_store_explicit(&slot->state, CLJ_RESPONSE_SLOT_READY, memory_order_release);
  CHECK(clj_h2o_response_ring_drain(receiver) == 0);
  CHECK(atomic_load(&slot->state) == CLJ_RESPONSE_SLOT_READY);
  size_t index = (late & CLJ_RESPONSE_HANDLE_INDEX_MASK) - 1;
  atomic_fetch_or_explicit(&receiver->ready_words[index / 64],
                           UINT64_C(1) << (index % 64), memory_order_release);
  atomic_fetch_sub_explicit(&receiver->ring_claimed, 1, memory_order_release);
  atomic_store(&receiver->accepts_responses, false);
  CHECK(clj_h2o_response_use_acquire(receiver) == 0);
  CHECK(clj_h2o_response_ring_drain(receiver) == 1);
  CHECK(clj_h2o_response_ring_drain(receiver) == 0);
  CHECK(clj_h2o_mt_response_pending(receiver) == 1);
  clj_h2o_response_use_release(receiver);
  CHECK(clj_h2o_mt_response_pending(receiver) == 0);
  clj_h2o_mt_destroy_response_receiver(receiver);
}

static void test_cancel_full(h2o_context_t *context) {
  clj_mt_receiver_t *receiver = clj_h2o_mt_create_response_receiver(context, 2, 3);
  CHECK(receiver != NULL);
  CHECK(clj_h2o_response_use_acquire(receiver) == 1);
  publish(receiver, 99, 200);
  uint64_t private = clj_h2o_response_try_claim(receiver);
  CHECK(private != 0);
  CHECK(clj_h2o_response_try_claim(receiver) == 0);
  CHECK(clj_h2o_response_ring_ready(receiver) == 1);
  CHECK(clj_h2o_mt_response_pending(receiver) == 3);
  atomic_store(&receiver->accepts_responses, false);
  CHECK(clj_h2o_response_publish(receiver, private) == 0);
  CHECK(clj_h2o_response_abort(receiver, private) == 1);
  CHECK(clj_h2o_response_abort(receiver, private) == 0);
  CHECK(clj_h2o_response_ring_drain(receiver) == 1);
  CHECK(clj_h2o_mt_response_pending(receiver) == 1);
  clj_h2o_response_use_release(receiver);
  CHECK(clj_h2o_mt_response_pending(receiver) == 0);
  clj_h2o_mt_destroy_response_receiver(receiver);
}

int main(void) {
  h2o_evloop_t *loop = h2o_evloop_create();
  h2o_context_t context = {.queue = h2o_multithread_create_queue(loop)};
  clj_mt_receiver_t *receiver = clj_h2o_mt_create_response_receiver(&context, 1, 64);
  CHECK(receiver != NULL);
  CHECK(clj_h2o_response_use_acquire(receiver) == 1);
  CHECK(clj_h2o_mt_response_pending(receiver) == 1);
  clj_h2o_response_use_release(receiver);
  test_final(receiver, 0, "GET", 200);
  test_final(receiver, 1, "GET", 200);
  test_final(receiver, 2, "GET", 200);
  test_final(receiver, 1, "HEAD", 200);
  test_final(receiver, 0, "GET", 204);
  test_final(receiver, 0, "GET", 205);
  test_final(receiver, 0, "GET", 304);
  publish(receiver, 99, 200);
  CHECK(clj_h2o_response_ring_drain(receiver) == 1);
  CHECK(clj_h2o_mt_response_pending(receiver) == 0);
  clj_h2o_mt_destroy_response_receiver(receiver);
  test_independent_release(&context);
  test_discovery(&context, 65);
  test_discovery(&context, UINT16_MAX);
  test_cancel_full(&context);
  h2o_multithread_destroy_queue(context.queue);
  h2o_evloop_destroy(loop);
  return 0;
}
