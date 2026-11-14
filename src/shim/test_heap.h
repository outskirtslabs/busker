/* Heap in use, for native tests that check memory is returned. Include after
   defining CHECK. */
#ifndef CLJ_TEST_HEAP_H
#define CLJ_TEST_HEAP_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#if defined(__GLIBC__)
#include <malloc.h>
#elif defined(__APPLE__)
#include <malloc/malloc.h>
#endif

#define HEAP_ALLOWED_GROWTH 4096

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
  size_t slack = HEAP_ALLOWED_GROWTH + (size_t)sysconf(_SC_PAGESIZE);
  if (allocated >= before + size && allocated <= before + size + slack &&
      released <= before + HEAP_ALLOWED_GROWTH)
    return 1;
  fprintf(stderr,
          "heap statistics do not follow a %zu byte block (before %zu, "
          "allocated %zu, released %zu); set CLJ_SKIP_HEAP_TESTS=1 under "
          "valgrind or a replaced allocator\n",
          size, before, allocated, released);
  return 0;
}

/* Why heap checks are skipped: CLJ_SKIP_HEAP_TESTS=1 (valgrind, a replaced
   allocator) or a libc without heap statistics; NULL when they run. Fails
   when the statistics do not follow a probe allocation. */
static const char *heap_skip_reason(void) {
  const char *skip = getenv("CLJ_SKIP_HEAP_TESTS");
  size_t in_use;
  if (skip != NULL && strcmp(skip, "1") == 0)
    return "CLJ_SKIP_HEAP_TESTS=1";
  if (!heap_in_use(&in_use))
    return "no heap statistics for this libc";
  CHECK(heap_follows(16 * 1024));
  CHECK(heap_follows(256 * 1024));
  return NULL;
}

#endif
