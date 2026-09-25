import { deepStrictEqual, rejects } from "node:assert/strict";
import {
  defaultPrecision,
  precisionSummary,
  scorePolicyVersion,
} from "../extensions/models/_lib/benchmark.ts";
import { Store } from "./fixtures/store.ts";
import {
  compareSavedMeasurements,
  readMeasurement,
} from "../extensions/models/_lib/measurement.ts";
import { runSavedStep } from "../extensions/models/_lib/driver.ts";
import { newRun } from "../extensions/models/_lib/state.ts";
import { benchmarkMethod } from "../extensions/models/_lib/placement.ts";
const initialRevision = "a".repeat(40);
const revision = "b".repeat(40);
const goal = {
  kind: "improve-baseline" as const,
  multiplier: 1.5,
  workloads: ["h1"],
};
const protections = [{ workload: "tls-h2", maxDecreaseFraction: 0.05 }];
const limits = {
  maxAttempts: 1,
  maxRepairsPerAttempt: 1,
  maxPiCalls: 3,
  maxDurationMs: 1000,
};
function record(
  protocol: "h1" | "tls-h2",
  score: number,
  commit = revision,
  repetitions = 3,
) {
  const rates = Array<number>(2 * repetitions).fill(score);
  const pooled = precisionSummary(rates, defaultPrecision);
  return {
    runId: "run",
    purpose: "measurement",
    adapter: "busker",
    protocol,
    revision: commit,
    nativeSha256: "c".repeat(64),
    hostname: "fixture-host",
    method: benchmarkMethod,
    scorePolicyVersion,
    precision: defaultPrecision,
    plannedSamples: 2 * repetitions,
    placement: {
      topology: [10, 11, 12, 13].map((cpu) => ({
        cpu,
        core: String(cpu),
        package: "0",
        l3: "1",
        l3Size: "32768K",
        siblings: `${cpu},${cpu + 16}`,
      })),
      realH2load: "/nix/h2load",
    },
    valid: true,
    inconclusive: false,
    error: null,
    files: [],
    omitted: [],
    logs: [],
    exitCode: 0,
    cleanup: "absent",
    batches: [{ samples: rates.slice(0, repetitions) }, {
      samples: rates.slice(repetitions),
    }],
    summary: {
      kind: "benchmark",
      adapter: "busker",
      protocol,
      score: pooled.mean,
      diagnosticRate: pooled.mean,
      samples: pooled.rates,
      sampleSD: pooled.sampleSD,
      tCritical: pooled.tCritical,
      ciHalfWidth: pooled.ciHalfWidth,
      relativeHalfWidth: pooled.relativeHalfWidth,
      javaVersion: "25",
      h2loadVersion: "1.69",
      parameters: {
        warmup: 10,
        duration: 30,
        repetitions,
        connections: 128,
        streams: 64,
        threads: 2,
      },
    },
  };
}
async function fixture(h2: number) {
  const store = new Store();
  await store.writeResource("state", "run-state", {
    ...newRun("run", initialRevision),
    phase: "measure",
    workingRevision: revision,
  });
  const initial = [
    await store.writeResource(
      "benchmark",
      "initial-h1",
      record("h1", 100, initialRevision),
    ),
    await store.writeResource(
      "benchmark",
      "initial-h2",
      record("tls-h2", 200, initialRevision),
    ),
  ];
  const current = [
    await store.writeResource("benchmark", "current-h1", record("h1", 160)),
    await store.writeResource("benchmark", "current-h2", record("tls-h2", h2)),
  ];
  return { store, references: { initial, best: initial, current } };
}

Deno.test("saved comparison sends an H1 win with excessive H2 loss to discard, not review", async () => {
  const { store, references } = await fixture(189);
  const result = await runSavedStep(
    store,
    "run",
    limits,
    0,
    (state) =>
      compareSavedMeasurements(
        store,
        state,
        goal,
        protections,
        references,
        0.05,
        defaultPrecision,
      ),
  );
  deepStrictEqual([
    result.state.phase,
    result.state.targetMet,
    result.state.bestRevision,
  ], ["discard", false, initialRevision]);
  const comparison = await store.readResource("run-comparison-1", 1);
  deepStrictEqual(comparison?.current, references.current);
  deepStrictEqual(result.record.evidence, [{
    name: "run-comparison-1",
    version: 1,
  }]);
});
Deno.test("comparison reads selected versions and permits review, but cannot keep code itself", async () => {
  const { store, references } = await fixture(190);
  await store.writeResource("benchmark", "current-h2", record("tls-h2", 100));
  const result = await runSavedStep(
    store,
    "run",
    limits,
    0,
    (state) =>
      compareSavedMeasurements(
        store,
        state,
        goal,
        protections,
        references,
        0.05,
        defaultPrecision,
      ),
  );
  deepStrictEqual([
    result.state.phase,
    result.state.targetMet,
    result.state.bestRevision,
  ], ["review", true, initialRevision]);
});
Deno.test("smoke results never become measurements, even with valid=true and fast diagnostic rates", async () => {
  const { store, references } = await fixture(200);
  const smoke = record("h1", 999999);
  const reference = await store.writeResource("benchmark", "smoke", {
    ...smoke,
    purpose: "smoke",
    summary: { ...smoke.summary, kind: "smoke", score: null },
  });
  references.current[0] = reference;
  const result = await runSavedStep(
    store,
    "run",
    limits,
    0,
    (state) =>
      compareSavedMeasurements(
        store,
        state,
        goal,
        protections,
        references,
        0.05,
        defaultPrecision,
      ),
  );
  deepStrictEqual([
    result.state.phase,
    result.state.targetMet,
    result.state.stopReason,
  ], ["finish", false, "invalid-measurement"]);
  const saved = await store.readResource("run-comparison-1", 1);
  deepStrictEqual(saved?.result, null);
  deepStrictEqual(typeof saved?.error, "string");
});
Deno.test("measurement assembly rejects mixed revisions, protocols, hosts, and native libraries", async () => {
  const { store, references } = await fixture(200);
  await rejects(
    () =>
      readMeasurement(
        store,
        "run",
        initialRevision,
        references.current,
        defaultPrecision,
      ),
    /revision/,
  );
  await rejects(
    () =>
      readMeasurement(
        store,
        "another-run",
        revision,
        references.current,
        defaultPrecision,
      ),
    /run/,
  );
  await rejects(
    () =>
      readMeasurement(store, "run", revision, [
        references.current[0],
        references.current[0],
      ], defaultPrecision),
    /Duplicate/,
  );
  for (
    const change of [{ nativeSha256: "d".repeat(64) }, {
      hostname: "another-host",
    }]
  ) {
    const changed = await store.writeResource("benchmark", "changed", {
      ...record("tls-h2", 200),
      ...change,
    });
    await rejects(
      () =>
        readMeasurement(store, "run", revision, [
          references.current[0],
          changed,
        ], defaultPrecision),
      /differs|hosts/,
    );
  }
});
Deno.test("saved mean and interval require exactly six supported samples and matching settings", async () => {
  const store = new Store();
  const base = record("h1", 100);
  const invalid = [
    { ...base, batches: [base.batches[0]] },
    { ...base, summary: { ...base.summary, score: 105 } },
    { ...base, summary: { ...base.summary, ciHalfWidth: 1 } },
    { ...base, scorePolicyVersion: "median-v0" },
    { ...base, precision: { ...defaultPrecision, confidenceLevel: 0.9 } },
    {
      ...base,
      summary: {
        ...base.summary,
        parameters: { ...base.summary.parameters, repetitions: 2 },
      },
    },
  ];
  for (const [index, item] of invalid.entries()) {
    const saved = { name: `invalid-${index}`, version: 1 };
    store.resources.set(saved.name, [item]);
    await rejects(
      () => readMeasurement(store, "run", revision, [saved], defaultPrecision),
    );
  }
});
Deno.test("a precise first batch still needs its second batch", async () => {
  const store = new Store();
  const saved = await store.writeResource(
    "benchmark",
    "complete",
    record("h1", 100),
  );
  const result = await readMeasurement(
    store,
    "run",
    revision,
    [saved],
    defaultPrecision,
  );
  deepStrictEqual(result.scores[0].requestsPerSecond, 100);
  const previous = { ...record("h1", 100) } as Record<string, unknown>;
  delete previous.scorePolicyVersion;
  const old = { name: "old", version: 1 };
  store.resources.set(old.name, [previous]);
  await rejects(() =>
    readMeasurement(store, "run", revision, [old], defaultPrecision)
  );
});

Deno.test("twelve scored samples require two complete six-sample batches and matching provenance", async () => {
  const store = new Store();
  const complete = record("h1", 100, revision, 6);
  const saved = await store.writeResource("benchmark", "twelve", complete);
  const measured = await readMeasurement(
    store,
    "run",
    revision,
    [saved],
    defaultPrecision,
  );
  deepStrictEqual(measured.scores[0].requestsPerSecond, 100);
  deepStrictEqual(
    complete.summary.tCritical,
    precisionSummary(Array(12).fill(100), defaultPrecision).tCritical,
  );
  const invalid = [
    { ...complete, plannedSamples: 6 },
    {
      ...complete,
      batches: [
        { samples: complete.batches[0].samples.slice(0, 5) },
        complete.batches[1],
      ],
    },
    {
      ...complete,
      summary: {
        ...complete.summary,
        samples: complete.summary.samples.slice(0, 11),
      },
    },
    {
      ...complete,
      summary: {
        ...complete.summary,
        parameters: { ...complete.summary.parameters, repetitions: 3 },
      },
    },
  ];
  for (const [index, item] of invalid.entries()) {
    const reference = { name: `invalid-twelve-${index}`, version: 1 };
    store.resources.set(reference.name, [item]);
    await rejects(() =>
      readMeasurement(store, "run", revision, [reference], defaultPrecision)
    );
  }
});

Deno.test("measurement requires the configured confidence and half-width", async () => {
  const store = new Store();
  const customPrecision = { relativeHalfWidth: 0.02, confidenceLevel: 0.9 };
  const changed = record("h1", 100);
  changed.precision = customPrecision;
  const recomputed = precisionSummary(changed.summary.samples, customPrecision);
  changed.summary.tCritical = recomputed.tCritical;
  const saved = await store.writeResource("benchmark", "custom", changed);
  const matching = await readMeasurement(
    store,
    "run",
    revision,
    [saved],
    customPrecision,
  );
  deepStrictEqual(matching.scores[0].requestsPerSecond, 100);
  await rejects(
    () => readMeasurement(store, "run", revision, [saved], defaultPrecision),
    /precision differs/,
  );
});

Deno.test("changed benchmark settings produce an invalid comparison instead of a speed claim", async () => {
  const { store, references } = await fixture(200);
  const changed = record("h1", 160);
  changed.summary.parameters.duration = 1;
  references.current[0] = await store.writeResource(
    "benchmark",
    "changed-settings",
    changed,
  );
  const result = await runSavedStep(
    store,
    "run",
    limits,
    0,
    (state) =>
      compareSavedMeasurements(
        store,
        state,
        goal,
        protections,
        references,
        0.05,
        defaultPrecision,
      ),
  );
  deepStrictEqual([result.state.phase, result.state.targetMet], [
    "finish",
    false,
  ]);
});

Deno.test("unpinned historical benchmarks cannot supply a measurement", async () => {
  const store = new Store();
  const historical: Record<string, unknown> = { ...record("h1", 100) };
  delete historical.method;
  delete historical.placement;
  store.resources.set("historical", [historical]);
  await rejects(
    () =>
      readMeasurement(store, "run", revision, [{
        name: "historical",
        version: 1,
      }], defaultPrecision),
    /method|placement/,
  );
});

Deno.test("changed CPU placement cannot compare against the initial anchor", async () => {
  const { store, references } = await fixture(200);
  const changed = record("h1", 160);
  changed.placement.realH2load = "/different/h2load";
  references.current[0] = await store.writeResource(
    "benchmark",
    "changed-placement",
    changed,
  );
  const result = await runSavedStep(
    store,
    "run",
    limits,
    0,
    (state) =>
      compareSavedMeasurements(
        store,
        state,
        goal,
        protections,
        references,
        0.05,
        defaultPrecision,
      ),
  );
  deepStrictEqual([result.state.phase, result.state.targetMet], [
    "finish",
    false,
  ]);
});
