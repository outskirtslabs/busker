import { deepStrictEqual, throws } from "node:assert/strict";
import {
  profileRequestSchema,
  validateProfile,
} from "../extensions/models/_lib/profile-runner.ts";

const revision = "a".repeat(40),
  hash = "b".repeat(64),
  native = "/repo/native.so";
const request = profileRequestSchema.parse({
  runId: "fixture",
  repository: "/repo",
  directory: "/output",
  collectorDirectory: "/support",
  protocol: "h1",
  event: "ctimer",
  smoke: false,
  parameters: {
    warmup: 1,
    duration: 1,
    connections: 128,
    streams: 64,
    threads: 2,
  },
});
function fixture() {
  const fields = ["S", ...Array(49).fill("0")];
  fields[11] = "10";
  fields[12] = "5";
  fields[19] = "100";
  const snapshot = {
    "captured-at-ms": 1000,
    "allocated-bytes": 100,
    process: {
      stat: `123 (server) ${fields.join(" ")}`,
      status: "Cpus_allowed_list:\t0-1",
      schedstat: null,
    },
    threads: {},
    "host-pressure": null,
    "cgroup-path": "/fixture",
    "cgroup-stat": null,
    "cgroup-pressure": null,
  };
  return {
    report: {
      kind: "profile",
      score: null,
      event: "ctimer",
      protocol: "h1",
      parameters: request.parameters,
      "profiler-version": "1.6.2",
      "clock-ticks-per-second": 100,
      load: { requests: 100, "requests-per-second": 100 },
      environment: {
        "busker-mode": "local",
        "busker-checkout": "/repo",
        "busker-revision": revision,
        "busker-dirty?": false,
        "native-version": null,
        "native-sha256": hash,
        "native-resource": "file:/repo/native.so",
        "busker-resource": "file:/repo/src/main/clojure/ol/busker.clj",
        "java-version": "25",
        "h2load-version": "fixture",
        processors: 2,
        "max-heap-bytes": 2147483648,
        "native-mappings": [`mapping ${native}`],
        "garbage-collectors": ["G1"],
        "jvm-options": [
          "-Xms512m",
          "-Xmx2g",
          "-XX:ActiveProcessorCount=2",
          `-Dol.libh2oclj.path=${native}`,
        ],
      },
    },
    before: snapshot,
    after: {
      ...structuredClone(snapshot),
      "captured-at-ms": 2000,
      "allocated-bytes": 400,
    },
    client: {
      userSeconds: 0.1,
      systemSeconds: 0.1,
      voluntarySwitches: 1,
      involuntarySwitches: 0,
    },
    collapsed: "[h2o-evloop-0 tid=123];h2o_send 10\n",
  };
}
function validate(f: ReturnType<typeof fixture>) {
  return validateProfile(
    f.report,
    f.before,
    f.after,
    f.client,
    f.collapsed,
    request,
    revision,
    hash,
    native,
  );
}
Deno.test("profile validation keeps CPU, sampled weights, and allocated bytes distinct", () => {
  const result = validate(fixture());
  deepStrictEqual(result.stacks?.diagnosticOnly, true);
  deepStrictEqual(result.allocatedBytesPerRequest, 3);
  deepStrictEqual(result.stacks?.unit, "samples");
  deepStrictEqual(result.before.cgroupCpu, null);
});
Deno.test("profile rejects stale identity, changed affinity, missing mapped image, decreasing counters, and empty recordings", () => {
  const changes = [
    (f: ReturnType<typeof fixture>) => {
      f.report.environment["busker-revision"] = "c".repeat(40);
    },
    (f: ReturnType<typeof fixture>) => {
      f.report.environment["native-mappings"] = ["mapping /wrong.so"];
    },
    (f: ReturnType<typeof fixture>) => {
      f.after.process.status = "Cpus_allowed_list:\t2-3";
    },
    (f: ReturnType<typeof fixture>) => {
      f.after["allocated-bytes"] = 0;
    },
    (f: ReturnType<typeof fixture>) => {
      f.collapsed = "";
    },
  ];
  for (const change of changes) {
    const f = fixture();
    change(f);
    throws(() => validate(f));
  }
});
Deno.test("JFR summaries require actual events and remain separate from sampled-stack weights", () => {
  const f = fixture();
  f.report.event = "jfr";
  const recording = {
    recording: {
      events: [{ type: "jdk.ThreadAllocationStatistics", values: {} }],
    },
  };
  const result = validateProfile(
    f.report,
    f.before,
    f.after,
    f.client,
    recording,
    { ...request, event: "jfr" },
    revision,
    hash,
    native,
  );
  deepStrictEqual(result.stacks, null);
  deepStrictEqual(result.jfr?.eventCounts["jdk.ThreadAllocationStatistics"], 1);
  throws(() =>
    validateProfile(
      f.report,
      f.before,
      f.after,
      f.client,
      { recording: { events: [] } },
      { ...request, event: "jfr" },
      revision,
      hash,
      native,
    )
  );
});
