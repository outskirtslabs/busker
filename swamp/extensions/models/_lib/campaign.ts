import { createHash } from "node:crypto";
import { join, resolve } from "node:path";
import { z } from "npm:zod@4.4.3";
import {
  adapterSchema,
  benchmarkParametersSchema,
  protocolSchema,
  validateBenchmark,
} from "./benchmark.ts";
import { comparisonSchema, readMeasurement } from "./measurement.ts";
import { runStateSchema, stepResultSchema } from "./schemas.ts";
import { assessCandidate, type Measurement } from "./control.ts";
import type { OutputReference } from "./inference.ts";
import { beginStep, finishStep, newRun, type RunState } from "./state.ts";
import { supportManifest } from "./support.ts";

const shaSchema = z.string().regex(/^[0-9a-f]{64}$/);
const refSchema = z.strictObject({
  name: z.string().min(1),
  version: z.number().int().positive(),
});
const recordSchema = z.object({
  name: z.string().min(1),
  version: z.number().int().positive(),
  modelId: z.string().min(1),
  modelName: z.literal("tempo"),
  modelType: z.literal("busker/tempo"),
  checksum: shaSchema,
  contentEncoding: z.string().optional(),
  content: z.unknown(),
});
const abandonmentSchema = z.strictObject({
  approvedBy: z.literal("tempo-leader@busker"),
  runId: z.string(),
  step: z.number().int().positive(),
  stateChecksum: shaSchema,
  controllerStopped: z.literal(true),
  childrenStopped: z.literal(true),
  refsAndLogsPreserved: z.literal(true),
});
export const parentEvidenceSchema = z.strictObject({
  repository: z.string().min(1),
  runId: z.string().regex(/^[a-zA-Z0-9_-]+$/),
  config: refSchema.extend({ checksum: shaSchema }),
  state: refSchema.extend({ checksum: shaSchema }),
  abandoned: z.strictObject({
    receipt: z.string().min(1),
    receiptSha256: shaSchema,
    step: z.number().int().positive(),
  }).nullable(),
  records: z.array(recordSchema),
  logs: z.array(z.strictObject({
    path: z.string().regex(/^step-[1-9][0-9]*\/(stdout\.jsonl|stderr\.log)$/),
    sha256: shaSchema,
    size: z.number().int().nonnegative(),
  })),
});
export type ParentEvidence = z.infer<typeof parentEvidenceSchema>;

export async function assertNotAbandoned(directory: string) {
  try {
    await Deno.stat(join(directory, "abandoned.json"));
    throw new Error("Abandoned run cannot be resumed or recovered");
  } catch (error) {
    if (!(error instanceof Deno.errors.NotFound)) throw error;
  }
}

async function assertParentStopped(runId: string) {
  const scopes = await new Deno.Command("systemctl", {
    args: [
      "--user",
      "list-units",
      "--all",
      "--plain",
      "--no-legend",
      "--no-pager",
      `tempo-controller-${runId}.scope`,
      `tempo-${runId}-*.scope`,
    ],
  }).output();
  if (
    !scopes.success || /\s(active|activating|reloading)\s/.test(
      new TextDecoder().decode(scopes.stdout),
    )
  ) throw new Error("Parent controller or child scope is still active");
}

function sha256(bytes: Uint8Array | string) {
  return createHash("sha256").update(bytes).digest("hex");
}
function contentBytes(record: z.infer<typeof recordSchema>) {
  if (
    record.contentEncoding === "base64" && typeof record.content === "string"
  ) {
    return Uint8Array.from(
      atob(record.content),
      (letter) => letter.charCodeAt(0),
    );
  }
  if (
    record.contentEncoding === "utf-8" && typeof record.content === "string"
  ) {
    return new TextEncoder().encode(record.content);
  }
  if (
    record.contentEncoding === undefined || record.contentEncoding === "utf-8"
  ) {
    const compact = JSON.stringify(record.content);
    const pretty = JSON.stringify(record.content, null, 2);
    for (const text of [compact, `${compact}\n`, pretty, `${pretty}\n`]) {
      const bytes = new TextEncoder().encode(text);
      if (sha256(bytes) === record.checksum) return bytes;
    }
  }
  throw new Error("Unsupported or unverified Swamp evidence encoding");
}
function catalog(evidence: ParentEvidence) {
  const records = new Map<string, z.infer<typeof recordSchema>>();
  const ids = new Set<string>();
  for (const item of evidence.records) {
    const key = `${item.name}@${item.version}`;
    if (records.has(key) || sha256(contentBytes(item)) !== item.checksum) {
      throw new Error("Duplicate or changed parent evidence");
    }
    ids.add(item.modelId);
    records.set(key, item);
  }
  if (ids.size !== 1) throw new Error("Parent evidence spans different models");
  const get = (reference: OutputReference) => {
    const record = records.get(`${reference.name}@${reference.version}`);
    if (!record) {
      throw new Error(
        `Missing exact parent evidence ${reference.name}@${reference.version}`,
      );
    }
    return record;
  };
  return { records, get };
}

type CampaignConfig = {
  arguments: {
    runId: string;
    revision: string;
    repository: string;
    supportDirectory: string;
    directory: string;
    parentEvidence?: { path: string; sha256: string };
    experimentBrief?: string;
    policy: unknown;
    parameters: unknown;
    limits: {
      maxAttempts: number;
      maxRepairsPerAttempt: number;
      maxPiCalls: number;
      maxDurationMs: number;
    };
  };
  startedAtMs: number;
};
type CampaignResult = {
  config: CampaignConfig;
  state: RunState;
  initial: Measurement | null;
  best: Measurement | null;
  bestRevision: string;
  startedAtMs: number;
  attemptsStarted: number;
  piCallsStarted: number;
  sourceRunId: string;
  sourceDigest: string;
};

export function verifyAcceptedComparison(
  policy: z.infer<typeof comparisonSchema>["policy"],
  initial: Measurement,
  best: Measurement,
  current: Measurement,
  saved: z.infer<typeof comparisonSchema>["result"],
  outcome: { valid: boolean; targetMet: boolean; improved: boolean },
) {
  const assessed = assessCandidate(
    policy.goal,
    policy.protections,
    initial,
    best,
    current,
    policy.minimumImprovementFraction,
  );
  if (
    assessed.status !== "valid" || assessed.improved !== true ||
    JSON.stringify(comparisonSchema.shape.result.parse(saved)) !==
      JSON.stringify(comparisonSchema.shape.result.parse(assessed)) ||
    !outcome.valid || !outcome.improved ||
    outcome.targetMet !== (assessed.targetMet === true)
  ) {
    throw new Error("Parent accepted comparison differs from verified scores");
  }
}

export async function verifyParentEvidence(
  path: string,
  digest: string,
  parseConfig: (value: unknown) => CampaignConfig,
  visited = new Set<string>(),
): Promise<CampaignResult> {
  if (resolve(path) !== path || !shaSchema.safeParse(digest).success) {
    throw new Error("Parent bundle path and digest must be exact");
  }
  if (visited.has(path)) {
    throw new Error("Parent evidence cycle");
  }
  visited.add(path);
  const bytes = await Deno.readFile(path);
  if (sha256(bytes) !== digest) throw new Error("Parent bundle changed");
  const evidence = parentEvidenceSchema.parse(
    JSON.parse(new TextDecoder().decode(bytes)),
  );
  await assertParentStopped(evidence.runId);
  const versions = await new Deno.Command("swamp", {
    args: [
      "data",
      "versions",
      "tempo",
      `${evidence.runId}-state`,
      "--repo-dir",
      evidence.repository,
      "--json",
    ],
    env: { SWAMP_SERVE_URL: "", SWAMP_SERVER_URL: "" },
  }).output();
  if (
    !versions.success ||
    z.object({
        versions: z.array(z.object({
          version: z.number(),
          isLatest: z.boolean(),
        })),
      }).parse(JSON.parse(new TextDecoder().decode(versions.stdout)))
        .versions.find((item) => item.isLatest)?.version !==
      evidence.state.version
  ) {
    throw new Error("Parent state changed since the evidence export");
  }
  const { records, get } = catalog(evidence);
  const configRecord = get(evidence.config);
  const stateRecord = get(evidence.state);
  for (const selected of [configRecord, stateRecord]) {
    const live = await getSwampRecord(
      evidence.repository,
      selected.name,
      selected.version,
    );
    if (
      live.checksum !== selected.checksum || live.modelId !== selected.modelId
    ) {
      throw new Error("Parent source record differs from exported evidence");
    }
  }
  if (
    configRecord.checksum !== evidence.config.checksum ||
    stateRecord.checksum !== evidence.state.checksum ||
    evidence.config.name !== `${evidence.runId}-config` ||
    evidence.state.name !== `${evidence.runId}-state`
  ) {
    throw new Error("Parent config or state reference changed");
  }
  const config = parseConfig(configRecord.content);
  const seenLogs = new Set<string>();
  for (const log of evidence.logs) {
    if (seenLogs.has(log.path)) throw new Error("Duplicate parent log");
    seenLogs.add(log.path);
    const full = join(config.arguments.directory, log.path);
    const stat = await Deno.lstat(full);
    if (
      !stat.isFile || stat.isSymlink || stat.size !== log.size ||
      sha256(await Deno.readFile(full)) !== log.sha256
    ) {
      throw new Error("Parent log changed after abandonment");
    }
  }
  if (
    (await supportManifest(evidence.repository)).sha256 !==
      z.object({ supportManifest: z.object({ sha256: shaSchema }) }).parse(
        configRecord.content,
      ).supportManifest.sha256
  ) {
    throw new Error("Parent support snapshot changed");
  }
  const state = runStateSchema.parse(stateRecord.content);
  const preceding = config.arguments.parentEvidence
    ? await verifyParentEvidence(
      config.arguments.parentEvidence.path,
      config.arguments.parentEvidence.sha256,
      parseConfig,
      visited,
    )
    : null;
  if (
    config.arguments.runId !== evidence.runId ||
    state.runId !== evidence.runId ||
    evidence.repository !== config.arguments.supportDirectory ||
    (preceding
      ? config.arguments.revision !== preceding.bestRevision ||
        state.initialRevision !==
          (preceding.initial?.revision ?? preceding.state.initialRevision) ||
        config.startedAtMs !== preceding.startedAtMs ||
        config.arguments.repository !== preceding.config.arguments.repository ||
        JSON.stringify(config.arguments.policy) !==
          JSON.stringify(preceding.config.arguments.policy) ||
        JSON.stringify(config.arguments.parameters) !==
          JSON.stringify(preceding.config.arguments.parameters) ||
        JSON.stringify(config.arguments.limits) !==
          JSON.stringify(preceding.config.arguments.limits) ||
        config.arguments.experimentBrief !==
          preceding.config.arguments.experimentBrief
      : config.arguments.revision !== state.initialRevision)
  ) {
    throw new Error("Parent run identity changed");
  }
  if (state.activeStep !== null) {
    if (
      !evidence.abandoned || state.activeStep !== evidence.abandoned.step ||
      records.has(`${evidence.runId}-step-${state.activeStep}@1`) ||
      (["analyze", "research", "implement", "repair", "review"].includes(
        state.phase,
      ) && ["stdout.jsonl", "stderr.log"].some(
        (name) => !seenLogs.has(`step-${state.activeStep}/${name}`),
      ))
    ) {
      throw new Error(
        "Active parent step needs verified abandonment, not replay",
      );
    }
    if (
      evidence.abandoned.receipt !== join(
        config.arguments.directory,
        "abandoned.json",
      )
    ) throw new Error("Abandonment receipt is outside the parent run");
    if (
      sha256(await Deno.readFile(evidence.abandoned.receipt)) !==
        evidence.abandoned.receiptSha256
    ) {
      throw new Error("Abandonment receipt changed");
    }
    const receipt = abandonmentSchema.parse(
      JSON.parse(await Deno.readTextFile(evidence.abandoned.receipt)),
    );
    if (
      receipt.runId !== evidence.runId || receipt.step !== state.activeStep ||
      receipt.stateChecksum !== stateRecord.checksum
    ) {
      throw new Error(
        "Abandonment receipt does not match the interrupted state",
      );
    }
  } else if (evidence.abandoned || state.phase !== "done") {
    throw new Error("Parent run is not terminal");
  }
  const lastCompleted = state.sequence - (state.activeStep === null ? 0 : 1);
  const steps: z.infer<typeof stepResultSchema>[] = [];
  for (let index = 1; index <= lastCompleted; index++) {
    const result = stepResultSchema.parse(
      get({ name: `${evidence.runId}-step-${index}`, version: 1 }).content,
    );
    if (result.runId !== evidence.runId || result.step !== index) {
      throw new Error("Parent step sequence is inconsistent");
    }
    steps.push(result);
    for (const reference of result.evidence) get(reference);
  }
  const attempts = (preceding?.attemptsStarted ?? 0) +
    steps.filter(({ phase }) => phase === "start-attempt").length +
    (state.activeStep !== null && state.phase === "start-attempt" ? 1 : 0);
  const piPhases = new Set([
    "analyze",
    "research",
    "implement",
    "repair",
    "review",
  ]);
  const piCalls = steps.filter(({ phase }) => piPhases.has(phase)).length +
    (preceding?.piCallsStarted ?? 0) +
    (state.activeStep !== null && piPhases.has(state.phase) ? 1 : 0);
  if (
    attempts !== state.attemptsStarted || piCalls !== state.piCallsStarted ||
    attempts > config.arguments.limits.maxAttempts ||
    piCalls > config.arguments.limits.maxPiCalls
  ) {
    throw new Error("Parent campaign reservations are inconsistent");
  }
  const baseline = steps.find(({ phase, outcome }) =>
    phase === "baseline" &&
    outcome.kind === "measurement" && outcome.valid
  );
  if (
    steps.length === 0
      ? state.activeStep !== 1 || state.phase !== "prepare" ||
        state.bestRevision !== state.initialRevision
      : steps[0].phase !== "prepare" || steps[0].outcome.kind !== "prepared"
  ) {
    throw new Error("Parent initial tests did not pass or are still ambiguous");
  }
  let replay: RunState = {
    ...newRun(evidence.runId, config.arguments.revision),
    initialRevision: preceding?.initial?.revision ?? config.arguments.revision,
    bestRevision: config.arguments.revision,
    attemptsStarted: preceding?.attemptsStarted ?? 0,
    piCallsStarted: preceding?.piCallsStarted ?? 0,
  };
  for (const step of steps) {
    const elapsed = step.step === steps.length
      ? state.elapsedMs
      : replay.elapsedMs;
    let active = beginStep(replay, config.arguments.limits, elapsed);
    if (active.activeStep === null) {
      active = beginStep(active, config.arguments.limits, elapsed);
    }
    if (
      step.step !== active.sequence || step.phase !== active.phase ||
      step.revision !== active.workingRevision
    ) {
      throw new Error("Parent step does not follow the previous result");
    }
    replay = finishStep(
      active,
      step.step,
      step.outcome,
      { name: `${evidence.runId}-step-${step.step}`, version: 1 },
    );
  }
  if (
    state.sequence !== steps.length + (state.activeStep === null ? 0 : 1) ||
    state.phase !== replay.phase ||
    state.workingRevision !== replay.workingRevision ||
    state.bestRevision !== replay.bestRevision ||
    state.targetMet !== replay.targetMet ||
    (state.activeStep !== null && replay.phase === "done")
  ) {
    throw new Error("Parent state does not follow saved step results");
  }
  const store = {
    readResource: (name: string, version?: number) =>
      Promise.resolve(
        z.record(z.string(), z.unknown()).parse(
          get({ name, version: version ?? 1 }).content,
        ),
      ),
  };
  const measurements = async (
    step: (typeof steps)[number],
    revision: string,
  ) => {
    const ref = step.evidence.find((item) =>
      item.name === `${evidence.runId}-comparison-${step.step}`
    );
    if (!ref) throw new Error("Missing parent comparison");
    const result = comparisonSchema.parse(get(ref).content);
    if (
      result.runId !== evidence.runId || result.revision !== revision ||
      JSON.stringify(result.policy) !==
        JSON.stringify(config.arguments.policy) ||
      result.error !== null || result.result?.status !== "valid"
    ) {
      throw new Error("Parent comparison policy or result changed");
    }
    return {
      result,
      measurement: await readMeasurement(
        store,
        evidence.runId,
        revision,
        result.current,
        comparisonSchema.shape.policy.parse(config.arguments.policy).precision,
      ),
    };
  };
  let initial: Measurement | null = preceding?.initial ?? null;
  const requalified = baseline
    ? (await measurements(baseline, baseline.revision)).measurement
    : null;
  if (!initial) initial = requalified;
  if (preceding?.initial && requalified) {
    if (
      requalified.protocolId !== preceding.initial.protocolId ||
      requalified.revision !== preceding.bestRevision
    ) {
      throw new Error("Fresh best measurements are not comparable");
    }
  } else if (preceding?.initial && !requalified && state.activeStep === null) {
    throw new Error("Restart has no best requalification");
  } else if (!initial && state.bestRevision !== state.initialRevision) {
    throw new Error("Parent has no verified initial baseline");
  }
  const kept = steps.filter(({ phase, outcome }) =>
    phase === "keep" && outcome.kind === "kept"
  );
  const lastKeep = kept.at(-1);
  const bestRevision = lastKeep?.outcome.kind === "kept"
    ? lastKeep.outcome.revision
    : (preceding?.bestRevision ?? state.initialRevision);
  if (state.bestRevision !== bestRevision) {
    throw new Error("Parent best is not a kept revision");
  }
  let best = preceding?.best ?? initial;
  const verifiedTests = (step: (typeof steps)[number]) => {
    const ref = step.evidence.find((item) =>
      item.name === `${evidence.runId}-test-${step.step}-result`
    );
    if (!ref) throw new Error("Missing saved QA result");
    const execution = z.object({
      runId: z.string(),
      step: z.number(),
      revision: z.string(),
      command: z.literal("bb"),
      args: z.tuple([z.literal("qa")]),
      valid: z.literal(true),
      passed: z.literal(true),
      sourceUnchanged: z.literal(true),
      exitCode: z.literal(0),
      artifacts: z.array(refSchema).min(1),
    }).parse(get(ref).content);
    if (
      execution.runId !== evidence.runId || execution.step !== step.step ||
      execution.revision !== step.revision
    ) throw new Error("QA result identity changed");
    for (const file of execution.artifacts) get(file);
  };
  if (steps.length) verifiedTests(steps[0]);
  if (preceding?.initial && preceding.best && requalified) {
    if (preceding.best.nativeSha256 !== requalified.nativeSha256) {
      throw new Error("Best revision native bytes differ after restart");
    }
    const policy = comparisonSchema.shape.policy.parse(config.arguments.policy);
    const assessment = assessCandidate(
      policy.goal,
      policy.protections,
      preceding.initial,
      preceding.best,
      requalified,
      policy.minimumImprovementFraction,
    );
    if (assessment.status !== "valid" || !assessment.protectedPerformance) {
      throw new Error(
        "Requalified best is incompatible or fails original H2 floor",
      );
    }
    const oldH1 = preceding.best.scores.find((cell) =>
      cell.workload === "h1" && cell.adapter === "busker"
    );
    const freshH1 = requalified.scores.find((cell) =>
      cell.workload === "h1" && cell.adapter === "busker"
    );
    if (
      !oldH1 || !freshH1 ||
      Math.abs(freshH1.requestsPerSecond - oldH1.requestsPerSecond) /
            oldH1.requestsPerSecond > 0.05
    ) {
      throw new Error(
        "Requalified best H1 differs beyond allowed repeatability",
      );
    }
    if (freshH1.requestsPerSecond > oldH1.requestsPerSecond) best = requalified;
  }
  let previousKeep = 0;
  for (const accepted of kept) {
    if (accepted.outcome.kind !== "kept" || !initial || !best) {
      throw new Error("Parent best lacks a verified comparison");
    }
    const revision = accepted.outcome.revision;
    const before = steps.filter((item) =>
      item.step > previousKeep && item.step < accepted.step
    );
    const tests = before.findLast((item) =>
      item.phase === "test" && item.outcome.kind === "tests" &&
      item.outcome.passed && item.outcome.revision === revision
    );
    const measured = before.findLast((item) =>
      item.phase === "measure" && item.outcome.kind === "measurement" &&
      item.outcome.valid && item.outcome.revision === revision
    );
    const reviewed = before.findLast((item) =>
      item.phase === "review" && item.outcome.kind === "review" &&
      item.outcome.passed && item.outcome.revision === revision
    );
    if (
      !tests || !measured || !reviewed ||
      !(tests.step < measured.step && measured.step < reviewed.step &&
        reviewed.step < accepted.step)
    ) {
      throw new Error(
        "Parent best lacks passing tests, measurement, review and keep",
      );
    }
    verifiedTests(tests);
    const reviewRef = reviewed.evidence.find((item) =>
      item.name === `${evidence.runId}-pi-${reviewed.step}-result`
    );
    if (!reviewRef) throw new Error("Parent review has no saved Pi result");
    const reviewResult = z.object({
      runId: z.string(),
      step: z.number(),
      stage: z.literal("review"),
      revision: z.string(),
      status: z.literal("valid"),
      inputs: z.array(refSchema),
      result: z.object({
        verdict: z.literal("pass"),
        evidence: z.array(refSchema),
      }),
    }).parse(get(reviewRef).content);
    if (
      reviewResult.runId !== evidence.runId ||
      reviewResult.step !== reviewed.step ||
      reviewResult.revision !== revision ||
      [
        `${evidence.runId}-test-${tests.step}-result`,
        `${evidence.runId}-comparison-${measured.step}`,
      ].some((name) =>
        !reviewResult.inputs.some((item) => item.name === name) ||
        !reviewResult.result.evidence.some((item) => item.name === name)
      )
    ) throw new Error("Parent review did not inspect the tested measurement");
    for (const input of reviewResult.inputs) get(input);
    const saved = await measurements(measured, revision);
    const policy = comparisonSchema.shape.policy.parse(config.arguments.policy);
    if (measured.outcome.kind !== "measurement") {
      throw new Error("Parent accepted comparison lacks a measurement result");
    }
    verifyAcceptedComparison(
      policy,
      initial,
      best,
      saved.measurement,
      saved.result.result,
      measured.outcome,
    );
    if (
      preceding
        ? saved.result.parent?.runId !== preceding.sourceRunId ||
          saved.result.parent?.sha256 !== preceding.sourceDigest
        : saved.result.parent !== undefined
    ) {
      throw new Error("Parent accepted comparison provenance changed");
    }
    best = saved.measurement;
    previousKeep = accepted.step;
  }
  for (const item of [initial, best, requalified]) {
    if (!item) continue;
    if (!item.id.split(",").some((id) => id.startsWith(`${evidence.runId}-`))) {
      continue;
    }
    for (const id of item.id.split(",")) {
      const match = /^(.+)@([1-9][0-9]*)$/.exec(id);
      if (!match) throw new Error("Invalid parent benchmark reference");
      const result = get({ name: match[1], version: Number(match[2]) });
      const benchmark = z.object({
        purpose: z.literal("measurement"),
        revision: z.string(),
        nativeSha256: shaSchema,
        protocol: protocolSchema,
        adapter: adapterSchema,
        summary: z.object({ parameters: benchmarkParametersSchema }),
        batches: z.array(z.object({ samples: z.array(z.number()) })).min(1).max(
          2,
        ),
        files: z.array(z.object({ path: z.string(), reference: refSchema })),
      }).parse(result.content);
      for (const [index, batch] of benchmark.batches.entries()) {
        const file = benchmark.files.find(({ path }) =>
          path === `${index + 1}/results.json`
        );
        if (!file) throw new Error("Missing parent raw benchmark results");
        const raw = z.string().parse(get(file.reference).content);
        const report = JSON.parse(raw);
        const environment = z.object({
          environment: z.object({
            "busker-checkout": z.string(),
            "native-resource": z.string(),
            "busker-resource": z.string(),
          }),
        }).parse(report).environment;
        const checked = validateBenchmark(report, {
          checkout: environment["busker-checkout"],
          revision: benchmark.revision,
          nativeSha256: benchmark.nativeSha256,
          nativeResource: new URL(environment["native-resource"]).href,
          sourceResource: new URL(environment["busker-resource"]).href,
          protocol: benchmark.protocol,
          adapter: benchmark.adapter,
          purpose: "measurement",
          parameters: benchmark.summary.parameters,
        });
        if (JSON.stringify(checked.samples) !== JSON.stringify(batch.samples)) {
          throw new Error(
            "Parent raw benchmark samples differ from saved result",
          );
        }
      }
    }
  }
  return {
    config,
    state,
    initial,
    best,
    bestRevision,
    startedAtMs: config.startedAtMs,
    attemptsStarted: attempts,
    piCallsStarted: piCalls,
    sourceRunId: evidence.runId,
    sourceDigest: digest,
  };
}

export async function getSwampRecord(
  repository: string,
  name: string,
  version: number,
) {
  if (
    !refSchema.safeParse({ name, version }).success ||
    resolve(repository) !== repository
  ) {
    throw new Error("Swamp data lookup needs an exact local reference");
  }
  const process = await new Deno.Command("swamp", {
    args: [
      "data",
      "get",
      "tempo",
      name,
      "--version",
      String(version),
      "--repo-dir",
      repository,
      "--json",
    ],
    env: { SWAMP_SERVE_URL: "", SWAMP_SERVER_URL: "" },
  }).output();
  if (!process.success) {
    throw new Error(`Missing Swamp data ${name}@${version}`);
  }
  const value = recordSchema.parse(
    JSON.parse(new TextDecoder().decode(process.stdout)),
  );
  if (
    value.name !== name || value.version !== version ||
    sha256(contentBytes(value)) !== value.checksum
  ) {
    throw new Error(
      `Swamp returned another version or changed content: ${name}@${version}`,
    );
  }
  return value;
}

export async function exportParentEvidence(
  repository: string,
  runId: string,
  stateVersion: number,
  destination: string,
  receiptPath: string | null = null,
) {
  if (
    resolve(repository) !== repository ||
    resolve(destination) !== destination ||
    !parentEvidenceSchema.shape.runId.safeParse(runId).success ||
    !Number.isSafeInteger(stateVersion) || stateVersion < 1
  ) {
    throw new Error("Parent export needs exact local paths and state version");
  }
  await assertParentStopped(runId);
  const versions = await new Deno.Command("swamp", {
    args: [
      "data",
      "versions",
      "tempo",
      `${runId}-state`,
      "--repo-dir",
      repository,
      "--json",
    ],
    env: { SWAMP_SERVE_URL: "", SWAMP_SERVER_URL: "" },
  }).output();
  if (!versions.success) {
    throw new Error("Cannot verify latest parent state version");
  }
  const listing = z.object({
    modelId: z.string(),
    versions: z.array(z.object({ version: z.number(), isLatest: z.boolean() })),
  }).parse(JSON.parse(new TextDecoder().decode(versions.stdout)));
  if (
    listing.versions.find((item) => item.isLatest)?.version !== stateVersion
  ) {
    throw new Error("Selected parent state is not the latest saved state");
  }
  const records = new Map<string, z.infer<typeof recordSchema>>();
  const fetch = async (name: string, version: number) => {
    const cached = records.get(`${name}@${version}`);
    if (cached) return cached;
    const item = await getSwampRecord(repository, name, version);
    if (item.modelId !== listing.modelId) {
      throw new Error("Parent model identity changed");
    }
    records.set(`${name}@${version}`, item);
    return item;
  };
  const fetchMany = async (references: OutputReference[]) => {
    const unique = [...new Map(references.map((reference) => [
      `${reference.name}@${reference.version}`,
      reference,
    ])).values()];
    for (let index = 0; index < unique.length; index += 4) {
      await Promise.all(
        unique.slice(index, index + 4).map((reference) =>
          records.get(`${reference.name}@${reference.version}`) ??
            fetch(reference.name, reference.version)
        ),
      );
    }
    return unique.map((reference) =>
      records.get(`${reference.name}@${reference.version}`)!
    );
  };
  const config = await fetch(`${runId}-config`, 1);
  if (
    (await supportManifest(repository)).sha256 !==
      z.object({ supportManifest: z.object({ sha256: shaSchema }) }).parse(
        config.content,
      ).supportManifest.sha256
  ) {
    throw new Error("Parent support snapshot changed before export");
  }
  const parentDirectory = z.object({
    arguments: z.object({
      directory: z.string(),
    }),
  }).parse(config.content).arguments.directory;
  if (resolve(parentDirectory) !== parentDirectory) {
    throw new Error("Parent run directory must be absolute");
  }
  const stateRecord = await fetch(`${runId}-state`, stateVersion);
  const state = runStateSchema.parse(stateRecord.content);
  if (state.runId !== runId) {
    throw new Error("Parent state belongs to another run");
  }
  const logs: ParentEvidence["logs"] = [];
  if (
    state.activeStep !== null &&
    ["analyze", "research", "implement", "repair", "review"].includes(
      state.phase,
    )
  ) {
    for (const name of ["stdout.jsonl", "stderr.log"]) {
      const path = `step-${state.activeStep}/${name}`;
      const full = join(parentDirectory, path);
      const stat = await Deno.lstat(full);
      if (!stat.isFile || stat.isSymlink) {
        throw new Error("Interrupted Pi log is not a regular file");
      }
      logs.push({
        path,
        sha256: sha256(await Deno.readFile(full)),
        size: stat.size,
      });
    }
  }
  const lastCompleted = state.sequence - (state.activeStep === null ? 0 : 1);
  const stepRecords = await fetchMany(Array.from(
    { length: lastCompleted },
    (_, index) => ({
      name: `${runId}-step-${index + 1}`,
      version: 1,
    }),
  ));
  const steps = stepRecords.map((item) => stepResultSchema.parse(item.content));
  await fetchMany(steps.flatMap((step) => step.evidence));
  if (state.activeStep !== null) {
    const probe = await new Deno.Command("swamp", {
      args: [
        "data",
        "get",
        "tempo",
        `${runId}-step-${state.activeStep}`,
        "--version",
        "1",
        "--repo-dir",
        repository,
        "--json",
      ],
      env: { SWAMP_SERVE_URL: "", SWAMP_SERVER_URL: "" },
    }).output();
    const missing = z.object({ error: z.string() }).safeParse(
      JSON.parse(
        new TextDecoder().decode(probe.stderr) ||
          new TextDecoder().decode(probe.stdout),
      ),
    );
    if (
      probe.success || !missing.success ||
      !missing.data.error.startsWith("Data not found:")
    ) {
      throw new Error(
        "Ambiguous parent step has a saved result or lookup failed",
      );
    }
  }
  const kept = steps.filter((step) =>
    step.phase === "keep" && step.outcome.kind === "kept"
  );
  const accepted = kept.map((keep) =>
    steps.findLast((step) =>
      step.step < keep.step && step.phase === "measure" &&
      step.outcome.kind === "measurement" && step.outcome.valid &&
      step.outcome.improved && keep.outcome.kind === "kept" &&
      step.outcome.revision === keep.outcome.revision
    )
  );
  const selected = [
    steps.find((step) =>
      step.phase === "baseline" &&
      step.outcome.kind === "measurement" && step.outcome.valid
    ),
    ...accepted,
  ];
  for (const step of selected) {
    if (!step) continue;
    const reference = step.evidence.find((item) =>
      item.name === `${runId}-comparison-${step.step}`
    );
    if (!reference) throw new Error("Missing parent comparison reference");
    const comparison = comparisonSchema.parse(
      (await fetch(reference.name, reference.version)).content,
    );
    for (const benchmark of comparison.current) {
      const result = await fetch(benchmark.name, benchmark.version);
      const files = z.object({
        files: z.array(z.object({ path: z.string(), reference: refSchema })),
      }).parse(result.content).files;
      for (
        const file of files.filter(({ path }) => path.endsWith("/results.json"))
      ) {
        await fetch(file.reference.name, file.reference.version);
      }
    }
  }
  const acceptedTests = kept.map((keep) =>
    steps.findLast((step) =>
      step.step < keep.step && step.phase === "test" &&
      step.outcome.kind === "tests" && step.outcome.passed &&
      keep.outcome.kind === "kept" &&
      step.outcome.revision === keep.outcome.revision
    )
  );
  for (const step of [steps[0], ...acceptedTests]) {
    if (!step) continue;
    const resultRef = step.evidence.find((item) =>
      item.name === `${runId}-test-${step.step}-result`
    );
    if (!resultRef) throw new Error("Missing parent test result");
    const result = await fetch(resultRef.name, resultRef.version);
    const logs = z.object({ artifacts: z.array(refSchema) }).parse(
      result.content,
    );
    for (const file of logs.artifacts) await fetch(file.name, file.version);
  }
  const acceptedReviews = kept.map((keep) =>
    steps.findLast((step) =>
      step.step < keep.step && step.phase === "review" &&
      step.outcome.kind === "review" && step.outcome.passed &&
      keep.outcome.kind === "kept" &&
      step.outcome.revision === keep.outcome.revision
    )
  );
  for (const acceptedReview of acceptedReviews) {
    if (!acceptedReview) continue;
    const ref = acceptedReview.evidence.find((item) =>
      item.name === `${runId}-pi-${acceptedReview.step}-result`
    );
    if (!ref) throw new Error("Missing accepted review result");
    const result = z.object({ inputs: z.array(refSchema) }).parse(
      (await fetch(ref.name, ref.version)).content,
    );
    for (const input of result.inputs) {
      if (!records.has(`${input.name}@${input.version}`)) {
        await fetch(input.name, input.version);
      }
    }
  }
  let abandoned: ParentEvidence["abandoned"] = null;
  if (state.activeStep !== null) {
    if (
      !receiptPath || receiptPath !== join(parentDirectory, "abandoned.json")
    ) {
      throw new Error(
        "Interrupted parent requires an approved abandonment receipt",
      );
    }
    const receipt = await Deno.readFile(receiptPath);
    const approved = abandonmentSchema.parse(
      JSON.parse(new TextDecoder().decode(receipt)),
    );
    if (
      approved.runId !== runId || approved.step !== state.activeStep ||
      approved.stateChecksum !== stateRecord.checksum
    ) {
      throw new Error("Abandonment receipt does not match parent state");
    }
    abandoned = {
      receipt: receiptPath,
      receiptSha256: sha256(receipt),
      step: state.activeStep,
    };
  } else if (receiptPath) {
    throw new Error("Terminal parent does not require abandonment");
  }
  const savedRecords = [...records.values()];
  for (let index = 0; index < savedRecords.length; index += 4) {
    await Promise.all(
      savedRecords.slice(index, index + 4).map(async (item) => {
        const latest = await getSwampRecord(
          repository,
          item.name,
          item.version,
        );
        if (
          latest.modelId !== item.modelId || latest.checksum !== item.checksum
        ) {
          throw new Error("Parent evidence changed while exporting");
        }
      }),
    );
  }
  const evidence: ParentEvidence = {
    repository,
    runId,
    config: {
      name: config.name,
      version: config.version,
      checksum: config.checksum,
    },
    state: {
      name: stateRecord.name,
      version: stateRecord.version,
      checksum: stateRecord.checksum,
    },
    abandoned,
    records: [...records.values()],
    logs,
  };
  const bytes = new TextEncoder().encode(JSON.stringify(evidence));
  const file = await Deno.open(destination, {
    createNew: true,
    write: true,
    mode: 0o600,
  });
  try {
    for (let offset = 0; offset < bytes.length;) {
      const written = await file.write(bytes.subarray(offset));
      if (!written) throw new Error("Could not write parent evidence");
      offset += written;
    }
    await file.sync();
  } finally {
    file.close();
  }
  await Deno.chmod(destination, 0o400);
  return sha256(bytes);
}

export { sha256 };
