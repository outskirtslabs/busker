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

#define BATCH_REQUESTS (CLJ_CLEANUP_BATCH_CAPACITY + 1)

static struct {
  clj_mt_receiver_t *receiver;
  clj_req_ctx_t *contexts;
  size_t calls;
  size_t counts[8];
  uint64_t seqs[BATCH_REQUESTS];
  size_t delivered;
  int reenter;
  int nest;
  int in_timer;
  size_t upcall_calls;
  uint64_t upcall_seq;
  size_t upcalls;
} cleanups;

/* Native cleanup is complete before the JVM part arrives: the request has left
   the receiver table and its slot is free. */
static void record_cleanups(uint64_t module_id, const uint64_t *request_seqs,
                            size_t count) {
  CHECK(module_id == 1);
  CHECK(!cleanups.in_timer);
  if (cleanups.calls < 8)
    cleanups.counts[cleanups.calls] = count;
  ++cleanups.calls;
  for (size_t i = 0; i < count; ++i) {
    uint64_t seq = request_seqs[i];
    clj_req_ctx_t *ctx = &cleanups.contexts[seq - 1];
    CHECK(ctx->cleanup && ctx->fixed_slot == NULL);
    CHECK(find_response_request(cleanups.receiver, seq) == NULL);
    cleanups.seqs[cleanups.delivered++] = seq;
  }
  if (cleanups.reenter) {
    cleanups.reenter = 0;
    size_t calls = cleanups.calls;
    cleanup_request(&cleanups.contexts[BATCH_REQUESTS - 1]);
    CHECK(cleanups.calls == calls + 1);
  }
  if (cleanups.nest) {
    cleanups.nest = 0;
    size_t calls = cleanups.calls;
    bool started = begin_cleanup_batch();
    CHECK(started);
    cleanup_request(&cleanups.contexts[BATCH_REQUESTS - 1]);
    end_cleanup_batch(started);
    CHECK(cleanups.calls == calls + 1);
  }
}

static void reset_cleanups(clj_mt_receiver_t *receiver,
                           clj_req_ctx_t *contexts) {
  memset(&cleanups, 0, sizeof(cleanups));
  cleanups.receiver = receiver;
  cleanups.contexts = contexts;
  for (size_t i = 0; i < BATCH_REQUESTS; ++i) {
    uint64_t claim = publish(receiver, i + 1, 200);
    clj_response_slot_t *slot = response_slot_from_handle(receiver, claim);
    contexts[i] = (clj_req_ctx_t){.on_request_cleanup = record_cleanups,
                                  .response_receiver = receiver,
                                  .fixed_slot = slot,
                                  .fixed_claim = claim,
                                  .dispatch_module_id = 1,
                                  .dispatch_request_seq = i + 1};
    CHECK(register_response_request(receiver, &contexts[i], i + 1));
    size_t index = (claim & CLJ_RESPONSE_HANDLE_INDEX_MASK) - 1;
    atomic_fetch_and(&receiver->ready_words[index / 64],
                     ~(UINT64_C(1) << (index % 64)));
    atomic_store(&slot->state, CLJ_RESPONSE_SLOT_IN_NATIVE);
    atomic_fetch_add(&receiver->ring_retained, 1);
    atomic_fetch_sub(&receiver->ring_ready, 1);
  }
}

static void check_delivered_once(size_t count) {
  CHECK(cleanups.delivered == count);
  for (size_t i = 0; i < count; ++i)
    for (size_t j = i + 1; j < count; ++j)
      CHECK(cleanups.seqs[i] != cleanups.seqs[j]);
}

static void release_all(clj_mt_receiver_t *receiver,
                              clj_req_ctx_t *contexts) {
  for (size_t i = 0; i < BATCH_REQUESTS; ++i)
    cleanup_request(&contexts[i]);
  CHECK(clj_h2o_response_ring_retained(receiver) == 0);
  CHECK(receiver->request_size == 0);
}

static void cleanup_on_timer(h2o_timer_t *timer) {
  (void)timer;
  cleanups.in_timer = 1;
  for (size_t i = 0; i < 3; ++i)
    cleanup_request(&cleanups.contexts[i]);
  cleanups.in_timer = 0;
}

/* Every upcall that names a request sees the cleanups that waited before it,
   and collection is off while it runs. */
static void expect_cleanups_delivered(void) {
  CHECK(cleanups.calls == cleanups.upcall_calls + 1);
  CHECK(cleanups.seqs[cleanups.delivered - 1] == cleanups.upcall_seq);
  CHECK(!tls_cleanup_batch.collecting && tls_cleanup_batch.count == 0);
  ++cleanups.upcalls;
}

static int request_upcall(clj_req_ctx_t *ctx) {
  (void)ctx;
  expect_cleanups_delivered();
  return CLJ_HANDLER_DECLINED;
}

static int body_upcall(uint64_t module_id, uint64_t request_seq, char *chunk,
                       size_t chunk_len, int is_end_stream) {
  (void)module_id, (void)request_seq, (void)chunk, (void)chunk_len,
      (void)is_end_stream;
  expect_cleanups_delivered();
  return 0;
}

static void proceed_upcall(uint64_t module_id, uint64_t request_seq) {
  (void)module_id, (void)request_seq;
  expect_cleanups_delivered();
}

static void stop_upcall(uint64_t module_id, uint64_t request_seq,
                        clj_complete_reason_t reason) {
  (void)module_id, (void)request_seq, (void)reason;
  expect_cleanups_delivered();
}

/* Leaves the request of `seq` cleaned up and waiting in an open batch. */
static void wait_cleanup(uint64_t seq) {
  CHECK(tls_cleanup_batch.collecting);
  cleanups.upcall_calls = cleanups.calls;
  cleanups.upcall_seq = seq;
  cleanup_request(&cleanups.contexts[seq - 1]);
  CHECK(cleanups.calls == cleanups.upcall_calls);
}

static void test_upcalls_receive_waiting_cleanups(clj_mt_receiver_t *receiver,
                                                  clj_req_ctx_t *contexts) {
  reset_cleanups(receiver, contexts);
  bool started = begin_cleanup_batch();

  h2o_globalconf_t globalconf = {0};
  h2o_context_t context = {.globalconf = &globalconf};
  clj_entrypoint_hosts_t entrypoint = {.entrypoint = 0};
  entrypoint.self = &entrypoint;
  h2o_conn_callbacks_t callbacks = {0};
  h2o_conn_t conn = {.ctx = &context,
                     .hosts = entrypoint.hosts,
                     .callbacks = &callbacks};
  h2o_req_t req = {.conn = &conn};
  h2o_mem_init_pool(&req.pool);
  clj_h2o_handler_t handler = {.on_request = request_upcall};
  wait_cleanup(1);
  CHECK(request_handler(&handler.super, &req) == -1);

  clj_req_ctx_t named = {.req = &req,
                         .dispatch_module_id = 1,
                         .dispatch_request_seq = 1000,
                         .on_request_body_chunk = body_upcall,
                         .on_response_generator_proceed = proceed_upcall,
                         .on_response_generator_stop = stop_upcall};
  wait_cleanup(2);
  CHECK(deliver_body_chunk(&named, 1) == 0);
  wait_cleanup(3);
  clj_generator_proceed(&named.generator, &req);
  wait_cleanup(4);
  clj_generator_stop(&named.generator, &req);
  CHECK(cleanups.upcalls == 4);
  CHECK(tls_cleanup_batch.collecting);

  end_cleanup_batch(started);
  h2o_mem_clear_pool(&req.pool);
  release_all(receiver, contexts);
  check_delivered_once(BATCH_REQUESTS);
}

static void test_cleanup_batch(h2o_evloop_t *loop, h2o_context_t *context) {
  clj_mt_receiver_t *receiver =
      clj_h2o_mt_create_response_receiver(context, 128, 3);
  CHECK(receiver != NULL);
  clj_req_ctx_t contexts[BATCH_REQUESTS];

  /* Outside a native turn each cleanup goes out at once. */
  reset_cleanups(receiver, contexts);
  cleanup_request(&contexts[0]);
  cleanup_request(&contexts[0]);
  CHECK(cleanups.calls == 1 && cleanups.counts[0] == 1);
  release_all(receiver, contexts);
  check_delivered_once(BATCH_REQUESTS);

  /* A turn delivers its cleanups once, before it returns. */
  reset_cleanups(receiver, contexts);
  h2o_timer_t timer;
  h2o_timer_init(&timer, cleanup_on_timer);
  h2o_timer_link(loop, 0, &timer);
  CHECK(clj_h2o_evloop_run(loop, 0) == 0);
  CHECK(!h2o_timer_is_linked(&timer));
  CHECK(cleanups.calls == 1 && cleanups.counts[0] == 3);
  CHECK(!tls_cleanup_batch.collecting && tls_cleanup_batch.count == 0);
  release_all(receiver, contexts);
  check_delivered_once(BATCH_REQUESTS);

  /* A nested begin neither starts nor ends the batch. An upcall that names a
     request first receives the pending cleanups, and a cleanup during that
     upcall or during a delivery goes out at once. A full batch goes out
     before the next cleanup is added. */
  reset_cleanups(receiver, contexts);
  bool started = begin_cleanup_batch();
  CHECK(started);
  cleanup_request(&contexts[0]);
  bool nested = begin_cleanup_batch();
  CHECK(!nested);
  end_cleanup_batch(nested);
  CHECK(cleanups.calls == 0);
  bool collecting = pause_cleanup_batch();
  CHECK(cleanups.calls == 1 && cleanups.counts[0] == 1);
  cleanup_request(&contexts[1]);
  CHECK(cleanups.calls == 2 && cleanups.counts[1] == 1);
  resume_cleanup_batch(collecting);
  for (size_t i = 2; i < BATCH_REQUESTS - 1; ++i)
    cleanup_request(&contexts[i]);
  CHECK(cleanups.calls == 2);
  cleanups.reenter = 1;
  end_cleanup_batch(started);
  CHECK(cleanups.calls == 4);
  CHECK(cleanups.counts[2] == BATCH_REQUESTS - 3 && cleanups.counts[3] == 1);
  check_delivered_once(BATCH_REQUESTS);

  reset_cleanups(receiver, contexts);
  started = begin_cleanup_batch();
  for (size_t i = 0; i < BATCH_REQUESTS; ++i)
    cleanup_request(&contexts[i]);
  CHECK(cleanups.calls == 1 &&
        cleanups.counts[0] == CLJ_CLEANUP_BATCH_CAPACITY);
  end_cleanup_batch(started);
  CHECK(cleanups.calls == 2 && cleanups.counts[1] == 1);
  check_delivered_once(BATCH_REQUESTS);
  CHECK(clj_h2o_response_ring_retained(receiver) == 0);

  /* A batch started inside a delivery neither delivers that delivery's
     sequences again nor writes into the array the callback reads. */
  reset_cleanups(receiver, contexts);
  started = begin_cleanup_batch();
  for (size_t i = 0; i < 3; ++i)
    cleanup_request(&contexts[i]);
  cleanups.nest = 1;
  end_cleanup_batch(started);
  CHECK(cleanups.calls == 2 && cleanups.counts[0] == 3 &&
        cleanups.counts[1] == 1);
  release_all(receiver, contexts);
  check_delivered_once(BATCH_REQUESTS);

  test_upcalls_receive_waiting_cleanups(receiver, contexts);

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
  test_cleanup_batch(loop, &context);
  h2o_multithread_destroy_queue(context.queue);
  h2o_evloop_destroy(loop);
  return 0;
}
