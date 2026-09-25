import { deepStrictEqual, throws } from "node:assert/strict";
import { summarizeProfile } from "../extensions/models/_lib/profile.ts";

Deno.test("counts recursive frames once and preserves caller stacks", () => {
  const summary = summarizeProfile({
    event: "ctimer",
    completedRequests: 100,
    collapsed:
      "[h2o-evloop-0 tid=10];root;repeat;repeat;sendmsg 3\n[h2o-evloop-0 tid=10];root;other 1\n",
  });
  deepStrictEqual(summary.inclusiveFrames, [
    { name: "root", weight: 4, percent: 100, perMillionRequests: 40000 },
    { name: "repeat", weight: 3, percent: 75, perMillionRequests: 30000 },
    { name: "sendmsg", weight: 3, percent: 75, perMillionRequests: 30000 },
    { name: "other", weight: 1, percent: 25, perMillionRequests: 10000 },
  ]);
  deepStrictEqual(summary.completeStacks[0], {
    threadRole: "event-loop",
    frames: ["root", "repeat", "repeat", "sendmsg"],
    weight: 3,
    percent: 75,
    perMillionRequests: 30000,
  });
  deepStrictEqual(summary.leaves, [
    { name: "sendmsg", weight: 3, percent: 75, perMillionRequests: 30000 },
    { name: "other", weight: 1, percent: 25, perMillionRequests: 10000 },
  ]);
});

Deno.test("does not infer a virtual-thread call from the carrier name alone", () => {
  const summary = summarizeProfile({
    event: "cpu",
    completedRequests: 100,
    collapsed: [
      "[ForkJoinPool-1-worker-1 tid=20];scheduler;h2o_send 5",
      "[ForkJoinPool-1-worker-1 tid=20];java/lang/VirtualThread.run;DowncallStub 2",
      "[VirtualThread tid=21];java.lang.VirtualThread.run;clj_h2o_send 3",
    ].join("\n"),
  });
  deepStrictEqual(summary.quality.virtualThreadNative, {
    weight: 5,
    percent: 50,
    perMillionRequests: 50000,
  });
  deepStrictEqual(summary.threadRoles, [
    {
      name: "virtual-carrier",
      weight: 7,
      percent: 70,
      perMillionRequests: 70000,
    },
    {
      name: "virtual-thread",
      weight: 3,
      percent: 30,
      perMillionRequests: 30000,
    },
  ]);
});

Deno.test("reports missing attribution and incomplete stacks without inventing names", () => {
  const summary = summarizeProfile({
    event: "wall",
    completedRequests: 10,
    collapsed: "root;[unknown];unknown_Java 2\nroot;frame_buffer_overflow 2\n",
  });
  deepStrictEqual(summary.quality, {
    unknownFrames: { weight: 2, percent: 50, perMillionRequests: 200000 },
    missingThread: { weight: 4, percent: 100, perMillionRequests: 400000 },
    stackOverflow: { weight: 2, percent: 50, perMillionRequests: 200000 },
    virtualThreadNative: { weight: 0, percent: 0, perMillionRequests: 0 },
  });
});

Deno.test("allocation weights remain separate from CPU samples and exact bytes", () => {
  const summary = summarizeProfile({
    event: "alloc",
    completedRequests: 10,
    collapsed: "allocate 2097152",
  });
  deepStrictEqual([
    summary.unit,
    summary.diagnosticOnly,
    "requestsPerSecond" in summary,
  ], ["allocation-weight", true, false]);
});

Deno.test("rejects empty, malformed, unsafe and zero-weight recordings", () => {
  for (
    const collapsed of [
      "",
      "a nope",
      "a -1",
      "a 0",
      "a 1.5",
      "a;;b 1",
      "[thread tid=1] 1",
      "a 9007199254740992",
      "a 9007199254740991\nb 1",
    ]
  ) {
    throws(() =>
      summarizeProfile({ event: "cpu", completedRequests: 1, collapsed })
    );
  }
  for (const completedRequests of [0, -1, NaN, Infinity, 0.5]) {
    throws(() =>
      summarizeProfile({ event: "cpu", completedRequests, collapsed: "a 1" })
    );
  }
});

Deno.test("ordering is stable and top limit does not truncate totals", () => {
  const input = { event: "cpu" as const, completedRequests: 10, top: 1 };
  const first = summarizeProfile({ ...input, collapsed: "z 1\na 1" });
  const second = summarizeProfile({ ...input, collapsed: "a 1\nz 1" });
  deepStrictEqual(first, second);
  deepStrictEqual([first.totalWeight, first.leaves.map((row) => row.name)], [
    2,
    ["a"],
  ]);
});
