import { deepStrictEqual } from "node:assert/strict";
import {
  runArgumentsSchema,
  runConfigSchema,
} from "../extensions/models/_lib/run.ts";
import {
  defaultPrecision,
  scorePolicyVersion,
} from "../extensions/models/_lib/benchmark.ts";
import { benchmarkMethod } from "../extensions/models/_lib/placement.ts";
function input() {
  return {
    confirmLive: true,
    runId: "fixture",
    repository: "/repo",
    revision: "a".repeat(40),
    directory: "/output",
    supportDirectory: "/support",
    supportSha256: "b".repeat(64),
    supportRevision: "c".repeat(40),
    credentialDirectory: "/credentials",
    referenceRoots: [],
    limits: {
      maxAttempts: 1,
      maxRepairsPerAttempt: 1,
      maxPiCalls: 10,
      maxDurationMs: 1000,
    },
    policy: {
      goal: { kind: "improve-baseline", multiplier: 1.5, workloads: ["h1"] },
      protections: [{ workload: "tls-h2", maxDecreaseFraction: 0.05 }],
      minimumImprovementFraction: 0.05,
    },
    benchmarkMethod,
    parameters: {
      warmup: 10,
      duration: 30,
      repetitions: 3,
      connections: 128,
      streams: 64,
      threads: 2,
    },
  };
}
Deno.test("live configuration requires explicit confirmation, H2 protection, compatible workload settings, and bounded identifiers", () => {
  deepStrictEqual(runArgumentsSchema.safeParse(input()).success, true);
  deepStrictEqual(
    runArgumentsSchema.safeParse({ ...input(), benchmarkMethod: "unpinned" })
      .success,
    false,
  );
  const invalid = [
    (value: ReturnType<typeof input>) => {
      value.confirmLive = false;
    },
    (value: ReturnType<typeof input>) => {
      value.policy.protections = [];
    },
    (value: ReturnType<typeof input>) => {
      value.parameters.connections = 129;
    },
    (value: ReturnType<typeof input>) => {
      value.runId = "a".repeat(81);
    },
    (value: ReturnType<typeof input>) => {
      value.limits.maxAttempts = 0;
    },
    (value: ReturnType<typeof input>) => {
      value.policy.minimumImprovementFraction = 0.01;
    },
    (value: ReturnType<typeof input>) => {
      value.policy.protections[0].maxDecreaseFraction = 1;
    },
  ];
  for (const change of invalid) {
    const value = input();
    change(value);
    deepStrictEqual(runArgumentsSchema.safeParse(value).success, false);
  }
});
Deno.test("precision configuration defaults, overrides, and legacy config rejection", () => {
  const parsed = runArgumentsSchema.parse(input());
  deepStrictEqual(parsed.policy.precision, defaultPrecision);
  deepStrictEqual(parsed.policy.scorePolicyVersion, scorePolicyVersion);
  const custom = runArgumentsSchema.parse({
    ...input(),
    policy: {
      ...input().policy,
      precision: { relativeHalfWidth: 0.03, confidenceLevel: 0.99 },
    },
  });
  deepStrictEqual(custom.policy.precision, {
    relativeHalfWidth: 0.03,
    confidenceLevel: 0.99,
  });
  for (
    const precision of [
      { relativeHalfWidth: 0 },
      { relativeHalfWidth: -0.1 },
      { relativeHalfWidth: Infinity },
      { confidenceLevel: 0 },
      { confidenceLevel: 1 },
      { confidenceLevel: NaN },
      { confidenceLevel: 0.95, unknown: true },
    ]
  ) {
    deepStrictEqual(
      runArgumentsSchema.safeParse({
        ...input(),
        policy: { ...input().policy, precision },
      }).success,
      false,
    );
  }
  deepStrictEqual(
    runArgumentsSchema.safeParse({
      ...input(),
      parameters: { ...input().parameters, repetitions: 2 },
    }).success,
    false,
  );
  deepStrictEqual(
    runArgumentsSchema.safeParse({
      ...input(),
      parameters: { ...input().parameters, repetitions: 6 },
    }).success,
    true,
  );
  deepStrictEqual(
    runArgumentsSchema.safeParse({
      ...input(),
      parameters: { ...input().parameters, repetitions: 4 },
    }).success,
    false,
  );
  const config = {
    arguments: parsed,
    scorePolicyVersion,
    startedAtMs: 1,
    supportManifest: { sha256: "d".repeat(64), files: [] },
    cliVersion: "fixture",
    dataDirectory: "/data",
  };
  deepStrictEqual(runConfigSchema.safeParse(config).success, true);
  const oldConfig: Partial<typeof config> = structuredClone(config);
  delete oldConfig.scorePolicyVersion;
  deepStrictEqual(runConfigSchema.safeParse(oldConfig).success, false);
});

Deno.test("beat-all requires every supported competitor, rather than silently benchmarking a chosen subset", () => {
  const value = input();
  const goal = {
    kind: "beat-all",
    competitors: {
      h1: [
        "aleph",
        "capra",
        "hirundo",
        "http-exchange",
        "http-kit",
        "jetty",
        "undertow",
      ],
    },
  };
  deepStrictEqual(
    runArgumentsSchema.safeParse({
      ...value,
      policy: { ...value.policy, goal },
    }).success,
    true,
  );
  goal.competitors.h1.pop();
  deepStrictEqual(
    runArgumentsSchema.safeParse({
      ...value,
      policy: { ...value.policy, goal },
    }).success,
    false,
  );
});
