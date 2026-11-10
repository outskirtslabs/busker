#include "h2o/socket.h"
#include "h2o/socket/evloop.h"
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);          \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

#if defined(__linux__)
typedef struct {
  int completed;
  int errors;
  int rearm;
  int close_on_completion;
  int closed;
  char payload[26000];
} test_state_t;

static void complete(h2o_socket_t *sock, const char *error) {
  test_state_t *state = sock->data;
  ++state->completed;
  state->errors += error != NULL;
  if (state->completed == 2 && state->rearm) {
    CHECK(error == NULL);
    h2o_iovec_t vec = h2o_iovec_init(state->payload + 14000, state->rearm);
    h2o_socket_write(sock, &vec, 1, complete);
  } else if (state->close_on_completion) {
    state->closed = 1;
    h2o_socket_close(sock);
  }
}

static h2o_socket_t *create_socket(h2o_evloop_t *loop, int fds[2],
                                   test_state_t *state) {
  int size = 4096;
  CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds) == 0);
  CHECK(setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) == 0);
  h2o_socket_t *sock = h2o_evloop_socket_create(loop, fds[0], 0);
  CHECK(sock != NULL);
  sock->data = state;
  for (size_t i = 0; i < sizeof(state->payload); ++i)
    state->payload[i] = (char)(i % 251);
  return sock;
}

static size_t drain(int fd, char *received, size_t capacity) {
  size_t total = 0;
  for (;;) {
    CHECK(total < capacity);
    ssize_t n = read(fd, received + total, capacity - total);
    if (n < 0) {
      CHECK(errno == EAGAIN || errno == EWOULDBLOCK);
      return total;
    }
    if (n == 0)
      return total;
    total += (size_t)n;
  }
}

static int available(int fd) {
  int bytes = -1;
  CHECK(ioctl(fd, FIONREAD, &bytes) == 0);
  return bytes;
}

static void test_between_turns(int rearm, int close_on_completion) {
  test_state_t state = {.rearm = rearm};
  char received[26001];
  int fds[2];
  h2o_evloop_t *loop = h2o_evloop_create();
  CHECK(loop != NULL);
  h2o_socket_t *sock = create_socket(loop, fds, &state);
  h2o_iovec_t vec = h2o_iovec_init(state.payload, 12000);
  h2o_socket_write(sock, &vec, 1, complete);
  CHECK(state.completed == 0 && h2o_socket_is_writing(sock));
  CHECK(available(fds[1]) == 8064);
  CHECK(h2o_evloop_run(loop, 0) == 0);
  CHECK(state.completed == 0);
  size_t total = drain(fds[1], received, sizeof(received));
  CHECK(total == 8064 && memcmp(received, state.payload, total) == 0);
  CHECK(h2o_evloop_run(loop, 0) == 0);
  CHECK(state.completed == 1 && !h2o_socket_is_writing(sock));
  CHECK(available(fds[1]) == 3936);

  /* No intervening poll: the next completion must record a fresh interest
   * update. */
  vec = h2o_iovec_init(state.payload + 12000, 2000);
  h2o_socket_write(sock, &vec, 1, complete);
  CHECK(state.completed == 1 && h2o_socket_is_writing(sock));
  CHECK(available(fds[1]) == 5936);
  state.close_on_completion = close_on_completion;
  CHECK(h2o_evloop_run(loop, 0) == 0);
  CHECK(state.completed == 2);
  if (!state.closed)
    CHECK(h2o_socket_is_writing(sock) == (rearm != 0));
  total += drain(fds[1], received + total, sizeof(received) - total);
  CHECK(total >= 14000);
  CHECK(memcmp(received, state.payload, total) == 0);
  fprintf(stderr,
          "before fourth turn: rearm=%d close=%d callbacks=%d bytes=%zu\n",
          rearm, close_on_completion, state.completed, total);
  CHECK(h2o_evloop_run(loop, 0) == 0);
  total += drain(fds[1], received + total, sizeof(received) - total);
  if (rearm == 12000) {
    CHECK(total == 22064 && state.completed == 2);
    CHECK(h2o_socket_is_writing(sock));
    CHECK(h2o_evloop_run(loop, 0) == 0);
    total += drain(fds[1], received + total, sizeof(received) - total);
  }
  CHECK(total == (size_t)(14000 + rearm));
  CHECK(memcmp(received, state.payload, total) == 0);
  CHECK(state.completed == (rearm ? 3 : 2) && state.errors == 0);
  if (!state.closed) {
    CHECK(!h2o_socket_is_writing(sock));
    h2o_socket_close(sock);
  }
  CHECK(h2o_evloop_run(loop, 0) == 0);
  CHECK(close(fds[1]) == 0);
  h2o_evloop_destroy(loop);
}

static void test_immediate_rearm(void) {
  test_state_t state = {.rearm = 16};
  char received[2017];
  int fds[2];
  h2o_evloop_t *loop = h2o_evloop_create();
  CHECK(loop != NULL);
  h2o_socket_t *sock = create_socket(loop, fds, &state);
  h2o_iovec_t vec = h2o_iovec_init(state.payload, 16);
  h2o_socket_write(sock, &vec, 1, complete);
  CHECK(h2o_evloop_run(loop, 0) == 0);
  CHECK(state.completed == 1);
  CHECK(drain(fds[1], received, sizeof(received)) == 16);
  CHECK(memcmp(received, state.payload, 16) == 0);
  vec = h2o_iovec_init(state.payload + 12000, 2000);
  h2o_socket_write(sock, &vec, 1, complete);
  CHECK(h2o_evloop_run(loop, 0) == 0);
  CHECK(state.completed == 3 && state.errors == 0);
  CHECK(!h2o_socket_is_writing(sock));
  CHECK(drain(fds[1], received, sizeof(received)) == 2016);
  CHECK(memcmp(received, state.payload + 12000, 2016) == 0);
  CHECK(h2o_evloop_run(loop, 0) == 0);
  CHECK(state.completed == 3);
  h2o_socket_close(sock);
  CHECK(h2o_evloop_run(loop, 0) == 0);
  CHECK(close(fds[1]) == 0);
  h2o_evloop_destroy(loop);
}

static void test_error(int deferred, int close_on_completion) {
  test_state_t state = {.close_on_completion = close_on_completion};
  int fds[2];
  h2o_evloop_t *loop = h2o_evloop_create();
  CHECK(loop != NULL);
  h2o_socket_t *sock = create_socket(loop, fds, &state);
  h2o_iovec_t vec = h2o_iovec_init(state.payload, 12000);
  if (!deferred)
    CHECK(close(fds[1]) == 0);
  h2o_socket_write(sock, &vec, 1, complete);
  CHECK(state.completed == 0 && h2o_socket_is_writing(sock));
  if (deferred) {
    CHECK(available(fds[1]) == 8064);
    CHECK(h2o_evloop_run(loop, 0) == 0);
    CHECK(state.completed == 0);
    CHECK(close(fds[1]) == 0);
  }
  CHECK(h2o_evloop_run(loop, 0) == 0);
  CHECK(state.completed == 1 && state.errors == 1);
  CHECK(h2o_evloop_run(loop, 0) == 0);
  CHECK(state.completed == 1 && state.errors == 1);
  if (!state.closed) {
    CHECK(!h2o_socket_is_writing(sock));
    h2o_socket_close(sock);
  }
  CHECK(h2o_evloop_run(loop, 0) == 0);
  h2o_evloop_destroy(loop);
}
#endif

int main(void) {
#if defined(__linux__)
  CHECK(signal(SIGPIPE, SIG_IGN) != SIG_ERR);
  test_between_turns(0, 0);
  test_between_turns(16, 0);
  test_between_turns(12000, 0);
  test_between_turns(0, 1);
  test_immediate_rearm();
  test_error(0, 0);
  test_error(0, 1);
  test_error(1, 0);
  test_error(1, 1);
  fprintf(stderr, "socket write completion: nine Linux cases passed\n");
#else
  fprintf(stderr, "socket write completion: Linux buffer preconditions; other "
                  "backends not covered\n");
#endif
  return 0;
}
