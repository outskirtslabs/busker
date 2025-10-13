#include "shim.h"

void clj_req_meta_print_offsets(void) {
  fprintf(stderr, "clj_req_meta_t field offsets:\n");
  fprintf(stderr, "  sizeof(clj_req_meta_t) = %zu\n", sizeof(clj_req_meta_t));
  fprintf(stderr, "  authority = %zu\n", offsetof(clj_req_meta_t, authority));
  fprintf(stderr, "  charset = %zu\n", offsetof(clj_req_meta_t, charset));
  fprintf(stderr, "  method = %zu\n", offsetof(clj_req_meta_t, method));
  fprintf(stderr, "  path = %zu\n", offsetof(clj_req_meta_t, path));
  fprintf(stderr, "  remote_addr = %zu\n",
          offsetof(clj_req_meta_t, remote_addr));
  fprintf(stderr, "  scheme = %zu\n", offsetof(clj_req_meta_t, scheme));
  fprintf(stderr, "  headers = %zu\n", offsetof(clj_req_meta_t, headers));
  fprintf(stderr, "  authority_len = %zu\n",
          offsetof(clj_req_meta_t, authority_len));
  fprintf(stderr, "  charset_len = %zu\n",
          offsetof(clj_req_meta_t, charset_len));
  fprintf(stderr, "  method_len = %zu\n", offsetof(clj_req_meta_t, method_len));
  fprintf(stderr, "  path_len = %zu\n", offsetof(clj_req_meta_t, path_len));
  fprintf(stderr, "  remote_addr_len = %zu\n",
          offsetof(clj_req_meta_t, remote_addr_len));
  fprintf(stderr, "  scheme_len = %zu\n", offsetof(clj_req_meta_t, scheme_len));
  fprintf(stderr, "  headers_len = %zu\n",
          offsetof(clj_req_meta_t, headers_len));
  fprintf(stderr, "  http_version = %zu\n",
          offsetof(clj_req_meta_t, http_version));
  fprintf(stderr, "  has_body = %zu\n", offsetof(clj_req_meta_t, has_body));
}
void clj_req_ctx_print_offsets(void) {
  fprintf(stderr, "clj_req_ctx_t field offsets:\n");
  fprintf(stderr, "  sizeof(clj_req_ctx_t) = %zu\n", sizeof(clj_req_ctx_t));
  fprintf(stderr, "  req = %zu\n", offsetof(clj_req_ctx_t, req));
  fprintf(stderr, "  meta = %zu\n", offsetof(clj_req_ctx_t, meta));
  fprintf(stderr, "  cleanup = %zu\n", offsetof(clj_req_ctx_t, cleanup));
  fprintf(stderr, "  on_cleanup = %zu\n", offsetof(clj_req_ctx_t, on_cleanup));
  fprintf(stderr, "  generator = %zu\n", offsetof(clj_req_ctx_t, generator));

  fprintf(stderr, "  generator = %zu\n", offsetof(clj_req_ctx_t, generator));
}
