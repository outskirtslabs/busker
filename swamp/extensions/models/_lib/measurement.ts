import { z } from "npm:zod@4.4.3";
import {
  adapterSchema,
  benchmarkParametersSchema,
  meetsPrecision,
  precisionSchema,
  precisionSummary,
  protocolSchema,
  scoredRepetitionsSchema,
  scorePolicyVersion,
} from "./benchmark.ts";
import {
  assessCandidate,
  type Goal,
  type Measurement,
  type Protection,
} from "./control.ts";
import type { StepEffect } from "./driver.ts";
import { benchmarkMethod, placementProvenanceSchema } from "./placement.ts";
import { type OutputReference, type ResourceStore } from "./inference.ts";
import type { RunState } from "./state.ts";

const savedBenchmarkSchema = z.object({
  runId: z.string(),
  purpose: z.literal("measurement"),
  protocol: protocolSchema,
  adapter: adapterSchema,
  revision: z.string(),
  nativeSha256: z.string().regex(/^[a-f0-9]{64}$/),
  hostname: z.string().min(1),
  method: z.literal(benchmarkMethod),
  scorePolicyVersion: z.literal(scorePolicyVersion),
  precision: precisionSchema,
  plannedSamples: z.union([z.literal(6), z.literal(12)]),
  placement: placementProvenanceSchema,
  valid: z.literal(true),
  inconclusive: z.literal(false),
  exitCode: z.literal(0),
  cleanup: z.enum(["absent", "stopped"]),
  summary: z.object({
    kind: z.literal("benchmark"),
    protocol: protocolSchema,
    adapter: adapterSchema,
    score: z.number().finite().positive(),
    diagnosticRate: z.number().finite().positive(),
    javaVersion: z.string().min(1),
    h2loadVersion: z.string().min(1),
    parameters: benchmarkParametersSchema,
    samples: z.array(z.number().finite().positive()).min(6).max(12),
    sampleSD: z.number().finite().nonnegative(),
    tCritical: z.number().finite().positive(),
    ciHalfWidth: z.number().finite().nonnegative(),
    relativeHalfWidth: z.number().finite().nonnegative(),
  }),
  batches: z.array(z.object({
    samples: z.array(z.number().finite().positive()).min(3).max(6),
  })).length(2),
});
const nullableNumber = z.number().nullable();
const referenceSchema = z.strictObject({
  name: z.string().min(1),
  version: z.number().int().positive(),
});
export const comparisonSchema = z.strictObject({
  runId: z.string(),
  revision: z.string(),
  error: z.string().nullable(),
  policy: z.strictObject({
    goal: z.union([
      z.strictObject({
        kind: z.literal("improve-baseline"),
        multiplier: z.number(),
        workloads: z.array(z.string()),
      }),
      z.strictObject({
        kind: z.literal("beat-all"),
        competitors: z.record(z.string(), z.array(z.string())),
      }),
    ]),
    protections: z.array(
      z.strictObject({ workload: z.string(), maxDecreaseFraction: z.number() }),
    ),
    minimumImprovementFraction: z.number().min(0.03).max(0.05),
    scorePolicyVersion: z.literal(scorePolicyVersion),
    precision: precisionSchema,
  }),
  initial: z.array(referenceSchema),
  best: z.array(referenceSchema),
  current: z.array(referenceSchema),
  parent: z.strictObject({
    runId: z.string(),
    sha256: z.string().regex(/^[a-f0-9]{64}$/),
  }).optional(),
  result: z.strictObject({
    kind: z.literal("goal-result"),
    initialMeasurement: z.string(),
    measurement: z.string(),
    revision: z.string(),
    bestMeasurement: z.string(),
    status: z.enum(["valid", "invalid"]),
    targetMet: z.boolean().nullable(),
    improved: z.boolean().nullable(),
    protectedPerformance: z.boolean().nullable(),
    errors: z.array(z.string()),
    comparisons: z.array(
      z.strictObject({
        workload: z.string(),
        initial: nullableNumber,
        measured: nullableNumber,
        target: nullableNumber,
        met: z.boolean(),
      }),
    ),
    protections: z.array(
      z.strictObject({
        workload: z.string(),
        initial: nullableNumber,
        measured: nullableNumber,
        minimum: nullableNumber,
        passed: z.boolean(),
      }),
    ),
    improvements: z.array(
      z.strictObject({
        workload: z.string(),
        best: nullableNumber,
        measured: nullableNumber,
        nondecreasing: z.boolean(),
        increased: z.boolean(),
        meetsFloor: z.boolean(),
      }),
    ),
  }).nullable(),
});

export async function readMeasurement(
  store: Pick<ResourceStore, "readResource">,
  runId: string,
  revision: string,
  references: OutputReference[],
  precision: z.infer<typeof precisionSchema>,
): Promise<Measurement> {
  if (!references.length) {
    throw new Error("A measurement needs saved benchmark outputs");
  }
  const records = [];
  for (const reference of references) {
    referenceSchema.parse(reference);
    const record = savedBenchmarkSchema.parse(
      await store.readResource(reference.name, reference.version),
    );
    if (
      record.runId !== runId || record.revision !== revision ||
      record.protocol !== record.summary.protocol ||
      record.adapter !== record.summary.adapter
    ) {
      throw new Error(
        "Benchmark output does not match the run, revision, or protocol",
      );
    }
    if (JSON.stringify(record.precision) !== JSON.stringify(precision)) {
      throw new Error("Benchmark precision differs from run configuration");
    }
    const rates = record.batches.flatMap((batch) => batch.samples);
    const pooled = precisionSummary(rates, precision);
    if (
      !scoredRepetitionsSchema.safeParse(
        record.summary.parameters.repetitions,
      ).success ||
      record.plannedSamples !== 2 * record.summary.parameters.repetitions ||
      record.batches.some((batch) =>
        batch.samples.length !== record.summary.parameters.repetitions
      ) ||
      record.summary.samples.length !== record.plannedSamples ||
      !meetsPrecision(pooled, precision) ||
      pooled.mean !== record.summary.score ||
      pooled.mean !== record.summary.diagnosticRate ||
      pooled.sampleSD !== record.summary.sampleSD ||
      pooled.tCritical !== record.summary.tCritical ||
      pooled.ciHalfWidth !== record.summary.ciHalfWidth ||
      pooled.relativeHalfWidth !== record.summary.relativeHalfWidth ||
      JSON.stringify(pooled.rates) !== JSON.stringify(record.summary.samples)
    ) throw new Error("Benchmark samples do not support the saved score");
    records.push({ reference, record });
  }
  records.sort((a, b) => a.record.protocol.localeCompare(b.record.protocol));
  if (
    new Set(records.map(({ record }) => `${record.protocol}/${record.adapter}`))
      .size !==
      records.length
  ) throw new Error("Duplicate benchmark protocols");
  if (new Set(records.map(({ record }) => record.nativeSha256)).size !== 1) {
    throw new Error("Native library differs within the measurement");
  }
  if (new Set(records.map(({ record }) => record.hostname)).size !== 1) {
    throw new Error("Measurement spans different hosts");
  }
  const settings = new Map<string, string>();
  for (const { record } of records) {
    const signature = JSON.stringify({
      hostname: record.hostname,
      method: record.method,
      scorePolicyVersion: record.scorePolicyVersion,
      precision: record.precision,
      plannedSamples: record.plannedSamples,
      placement: record.placement,
      javaVersion: record.summary.javaVersion,
      h2loadVersion: record.summary.h2loadVersion,
      parameters: record.summary.parameters,
    });
    if (
      settings.has(record.protocol) &&
      settings.get(record.protocol) !== signature
    ) throw new Error("Benchmark settings differ within a protocol");
    settings.set(record.protocol, signature);
  }
  const protocolId = JSON.stringify(
    [...settings].sort(([a], [b]) => a.localeCompare(b)),
  );
  return {
    kind: "benchmark",
    id: records.map(({ reference }) => `${reference.name}@${reference.version}`)
      .join(","),
    revision,
    protocolId,
    nativeSha256: records[0].record.nativeSha256,
    scores: records.map(({ record }) => ({
      workload: record.protocol,
      adapter: record.adapter,
      requestsPerSecond: record.summary.score,
      valid: true,
    })),
  };
}

export async function compareSavedMeasurements(
  store: ResourceStore,
  state: Readonly<RunState>,
  goal: Goal,
  protections: Protection[],
  references: {
    initial: OutputReference[];
    best: OutputReference[];
    current: OutputReference[];
  },
  minimumImprovementFraction: number,
  precision: z.infer<typeof precisionSchema>,
  parent: {
    initial: Measurement | null;
    best: Measurement | null;
    runId: string;
    sha256: string;
  } | null = null,
): Promise<StepEffect> {
  if (
    !["baseline", "measure"].includes(state.phase) ||
    state.activeStep === null || state.activeStep !== state.sequence
  ) {
    throw new Error("Comparison requires an active measurement step");
  }
  let result: ReturnType<typeof assessCandidate> | null = null;
  let error: string | null = null;
  try {
    const initial = parent?.initial ?? await readMeasurement(
      store,
      state.runId,
      state.initialRevision,
      references.initial,
      precision,
    );
    const best = parent?.best ?? await readMeasurement(
      store,
      state.runId,
      state.bestRevision,
      references.best,
      precision,
    );
    const current = await readMeasurement(
      store,
      state.runId,
      state.workingRevision,
      references.current,
      precision,
    );
    if (
      state.phase === "baseline" && parent?.best && (
        current.revision !== parent.best.revision ||
        !current.nativeSha256 ||
        current.nativeSha256 !== parent.best.nativeSha256 ||
        current.protocolId !== parent.best.protocolId
      )
    ) throw new Error("Best requalification changed native bytes or settings");
    result = assessCandidate(
      goal,
      protections,
      initial,
      best,
      current,
      minimumImprovementFraction,
    );
  } catch (failure) {
    error = String(failure);
  }
  const saved = await store.writeResource(
    "comparison",
    `${state.runId}-comparison-${state.sequence}`,
    {
      runId: state.runId,
      revision: state.workingRevision,
      ...references,
      ...(parent
        ? { parent: { runId: parent.runId, sha256: parent.sha256 } }
        : {}),
      policy: {
        goal,
        protections,
        minimumImprovementFraction,
        scorePolicyVersion,
        precision,
      },
      result,
      error,
    },
  );
  if (
    state.phase === "baseline" && parent?.initial &&
    result?.protectedPerformance !== true
  ) {
    return {
      outcome: {
        kind: "error",
        message:
          "Best requalification failed original H2 floor or comparability",
      },
      evidence: [{ name: saved.name, version: saved.version }],
    };
  }
  return {
    outcome: {
      kind: "measurement",
      revision: state.workingRevision,
      valid: result?.status === "valid",
      targetMet: result?.targetMet === true,
      improved: result?.improved === true,
    },
    evidence: [{ name: saved.name, version: saved.version }],
  };
}
