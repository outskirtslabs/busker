import { createHash } from "node:crypto";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { z } from "npm:zod@4.4.3";
import { archiveLogs, type ArtifactStore } from "./artifacts.ts";
import {
  adapterSchema,
  benchmarkParametersSchema,
  defaultPrecision,
  precisionSchema,
  protocolSchema,
  scoredRepetitionsSchema,
  scorePolicyVersion,
} from "./benchmark.ts";
import { runBenchmark } from "./benchmark-runner.ts";
import { assertNotAbandoned, verifyParentEvidence } from "./campaign.ts";
import { runSavedStep, type StepEffect } from "./driver.ts";
import { type OutputReference } from "./inference.ts";
import {
  compareSavedMeasurements,
  comparisonSchema,
  readMeasurement,
} from "./measurement.ts";
import { runPiEffect } from "./pi-runner.ts";
import { profileRequestSchema, runProfile } from "./profile-runner.ts";
import { benchmarkMethod } from "./placement.ts";
import { runCommand, stopProcessScope } from "./process.ts";
import { startRepl, stopRepl } from "./repl.ts";
import {
  inferenceContextSchema,
  revisionSchema,
  runIdSchema,
  runStateSchema,
  stepResultSchema,
} from "./schemas.ts";
import { newRun, type RunState } from "./state.ts";
import {
  supportManifest,
  verifyReviewedSupport,
  verifySupport,
} from "./support.ts";
import { runTestEffect } from "./test-runner.ts";
import {
  createWorkspace,
  inspectCheckout,
  markBestRevision,
  selectWorkspaceRevision,
  snapshotWorkspace,
  verifyWorkspaceRevision,
  type Workspace,
} from "./workspace.ts";

export const limitsSchema = z.strictObject({
  maxAttempts: z.number().int().positive(),
  maxRepairsPerAttempt: z.number().int().nonnegative(),
  maxPiCalls: z.number().int().positive(),
  maxDurationMs: z.number().int().positive(),
});
export const runArgumentsSchema = z.strictObject({
  confirmLive: z.literal(true),
  runId: runIdSchema.max(80),
  repository: z.string().min(1),
  revision: revisionSchema,
  directory: z.string().min(1),
  supportDirectory: z.string().min(1),
  supportSha256: z.string().regex(/^[a-f0-9]{64}$/),
  supportRevision: revisionSchema,
  parentEvidence: z.strictObject({
    path: z.string().min(1),
    sha256: z.string().regex(/^[a-f0-9]{64}$/),
  }).optional(),
  experimentBrief: z.string().min(1).max(32_768).nullish().transform((value) =>
    value ?? undefined
  ),
  credentialDirectory: z.string().min(1),
  referenceRoots: z.array(z.string()),
  limits: limitsSchema,
  policy: comparisonSchema.shape.policy.omit({
    scorePolicyVersion: true,
    precision: true,
  }).extend({
    scorePolicyVersion: z.literal(scorePolicyVersion).default(
      scorePolicyVersion,
    ),
    precision: precisionSchema.default(defaultPrecision),
  }),
  benchmarkMethod: z.literal(benchmarkMethod),
  parameters: benchmarkParametersSchema,
}).superRefine((value, context) => {
  if (
    !scoredRepetitionsSchema.safeParse(value.parameters.repetitions).success
  ) {
    context.addIssue({
      code: "custom",
      message: "Scored measurements require three or six samples per batch",
    });
  }
  const workloads = value.policy.goal.kind === "beat-all"
    ? Object.keys(value.policy.goal.competitors)
    : value.policy.goal.workloads;
  if (
    workloads.includes("h1") &&
    !value.policy.protections.some((item) => item.workload === "tls-h2")
  ) {
    context.addIssue({
      code: "custom",
      message: "H1 optimization requires an explicit TLS H2 allowance",
    });
  }
  if (
    value.parameters.connections % value.parameters.streams !== 0 ||
    (value.parameters.connections / value.parameters.streams) %
          value.parameters.threads !== 0
  ) {
    context.addIssue({
      code: "custom",
      message: "Connections must divide into streams and client threads",
    });
  }
  if (!workloads.length || new Set(workloads).size !== workloads.length) {
    context.addIssue({ code: "custom", message: "Select unique workloads" });
  }
  for (
    const name of [
      ...workloads,
      ...value.policy.protections.map((item) => item.workload),
    ]
  ) {
    if (!protocolSchema.safeParse(name).success) {
      context.addIssue({
        code: "custom",
        message: `Unsupported workload: ${name}`,
      });
    }
  }
  if (
    value.policy.goal.kind === "improve-baseline" &&
    value.policy.goal.multiplier <= 1
  ) context.addIssue({ code: "custom", message: "Multiplier must exceed one" });
  if (
    new Set(value.policy.protections.map((item) => item.workload)).size !==
      value.policy.protections.length ||
    value.policy.protections.some((item) =>
      item.maxDecreaseFraction < 0 || item.maxDecreaseFraction >= 1
    )
  ) {
    context.addIssue({
      code: "custom",
      message: "Invalid protected workloads",
    });
  }
  if (value.policy.goal.kind === "beat-all") {
    for (
      const [protocol, adapters] of Object.entries(
        value.policy.goal.competitors,
      )
    ) {
      const supported = adapterSchema.options.filter((adapter) =>
        adapter !== "busker" &&
        (protocol === "h1" ||
          (adapter !== "capra" &&
            (protocol !== "tls-h2" ||
              !["http-exchange", "http-kit"].includes(adapter)))) &&
        !(protocol === "tls-h1" && adapter === "http-kit")
      );
      if (
        JSON.stringify([...adapters].sort((a, b) => a.localeCompare(b))) !==
          JSON.stringify([...supported].sort((a, b) => a.localeCompare(b)))
      ) {
        context.addIssue({
          code: "custom",
          message:
            `Beat-all requires all supported competitors for ${protocol}: ${
              supported.join(", ")
            }`,
        });
      }
    }
  }
});
export const runConfigSchema = z.strictObject({
  arguments: runArgumentsSchema,
  scorePolicyVersion: z.literal(scorePolicyVersion),
  startedAtMs: z.number().int().positive(),
  supportManifest: z.strictObject({
    sha256: z.string().regex(/^[a-f0-9]{64}$/),
    files: z.array(z.strictObject({
      path: z.string(),
      sha256: z.string().regex(/^[a-f0-9]{64}$/),
    })),
  }),
  cliVersion: z.string().min(1),
  dataDirectory: z.string().min(1),
});
export const workspaceSchema = z.strictObject({
  repository: z.string(),
  path: z.string(),
  runId: runIdSchema,
  initialRevision: revisionSchema,
});
export const agentsContextSchema = z.strictObject({
  runId: runIdSchema,
  source: z.literal("untracked project AGENTS.md"),
  sha256: z.string().regex(/^[0-9a-f]{64}$/),
  text: z.string(),
});
export const experimentBriefSchema = z.strictObject({
  runId: runIdSchema,
  source: z.literal("prepared experiment brief"),
  sha256: z.string().regex(/^[0-9a-f]{64}$/),
  text: z.string().min(1).max(32_768),
});
export const runDetailSchema = z.object({ runId: runIdSchema }).catchall(
  z.json(),
);
const ref = ({ name, version }: OutputReference) => ({ name, version });
type Configuration = z.infer<typeof runConfigSchema>;

async function currentCliVersion() {
  const result = await new Deno.Command("swamp", { args: ["--version"] })
    .output();
  if (!result.success) throw new Error("Cannot read Swamp CLI version");
  return new TextDecoder().decode(result.stdout).trim();
}
async function checkRunSupport(config: Configuration) {
  const directory = config.arguments.supportDirectory;
  const selectedRepo = Deno.env.get("SWAMP_REPO_DIR");
  if (
    await Deno.realPath(directory) !== directory ||
    await Deno.realPath(Deno.cwd()) !== directory ||
    Deno.env.get("SWAMP_SERVE_URL") || Deno.env.get("SWAMP_SERVER_URL") ||
    (selectedRepo && await Deno.realPath(selectedRepo) !== directory)
  ) throw new Error("Run must use its local support snapshot");
  await verifySupport(directory, config.supportManifest);
  if (config.arguments.parentEvidence) {
    await verifyParentEvidence(
      config.arguments.parentEvidence.path,
      config.arguments.parentEvidence.sha256,
      (value) => runConfigSchema.parse(value),
    );
  }
}
async function stepResource(
  store: ArtifactStore,
  state: Readonly<RunState>,
  phase: keyof RunState["outputs"],
) {
  const reference = state.outputs[phase];
  if (!reference) throw new Error(`Missing saved ${phase} output`);
  const step = stepResultSchema.parse(
    await store.readResource(reference.name, reference.version),
  );
  if (step.runId !== state.runId || step.phase !== phase) {
    throw new Error("Saved step does not match its run");
  }
  const resource = step.evidence.at(-1);
  if (!resource) throw new Error(`Missing ${phase} evidence`);
  return {
    reference: resource,
    value: await store.readResource(resource.name, resource.version),
  };
}
async function measurementReferences(
  store: ArtifactStore,
  state: Readonly<RunState>,
  phase: "baseline" | "measure",
) {
  return comparisonSchema.parse((await stepResource(store, state, phase)).value)
    .current;
}
function measurementRefs(measurement: { id: string }): OutputReference[] {
  return measurement.id.split(",").map((label) => {
    const match = /^(.+)@([1-9][0-9]*)$/.exec(label);
    if (!match) throw new Error("Invalid parent benchmark reference");
    return { name: match[1], version: Number(match[2]) };
  });
}

export async function startRun(
  store: ArtifactStore,
  input: z.input<typeof runArgumentsSchema>,
  now = Date.now(),
) {
  const args = runArgumentsSchema.parse(input);
  if (
    await store.readResource(`${args.runId}-state`) ||
    await store.readResource(`${args.runId}-config`) ||
    await store.readResource(`${args.runId}-agents-context`) ||
    await store.readResource(`${args.runId}-experiment-brief`)
  ) throw new Error("Run already exists");
  const checkout = await inspectCheckout(args.repository);
  args.repository = checkout.path;
  if (
    ![
      args.directory,
      args.supportDirectory,
      args.credentialDirectory,
      ...args.referenceRoots,
    ].every(isAbsolute)
  ) throw new Error("Run paths must be absolute");
  if (
    join(
      await Deno.realPath(dirname(args.directory)),
      basename(args.directory),
    ) !== resolve(args.directory)
  ) throw new Error("Run directory must not traverse a symlink");
  if (
    await Deno.realPath(args.supportDirectory) !== args.supportDirectory ||
    await Deno.realPath(Deno.cwd()) !== args.supportDirectory
  ) throw new Error("Run the workflow from its selected support snapshot");
  const selectedRepo = Deno.env.get("SWAMP_REPO_DIR");
  if (
    Deno.env.get("SWAMP_SERVE_URL") || Deno.env.get("SWAMP_SERVER_URL") ||
    (selectedRepo &&
      await Deno.realPath(selectedRepo) !== args.supportDirectory)
  ) throw new Error("Run requires its selected local Swamp repository");
  const manifest = await supportManifest(args.supportDirectory);
  if (manifest.sha256 !== args.supportSha256) {
    throw new Error("Support snapshot does not match the approved manifest");
  }
  await verifyReviewedSupport(
    args.repository,
    args.supportRevision,
    args.supportDirectory,
    manifest,
  );
  const parent = args.parentEvidence
    ? await verifyParentEvidence(
      args.parentEvidence.path,
      args.parentEvidence.sha256,
      (value) => runConfigSchema.parse(value),
    )
    : null;
  if (parent) {
    if (
      parent.config.arguments.repository !== args.repository ||
      JSON.stringify(parent.config.arguments.policy) !==
        JSON.stringify(args.policy) ||
      args.experimentBrief !== parent.config.arguments.experimentBrief ||
      JSON.stringify(parent.config.arguments.parameters) !==
        JSON.stringify(args.parameters) ||
      JSON.stringify(parent.config.arguments.limits) !==
        JSON.stringify(args.limits) ||
      args.revision !== parent.bestRevision ||
      now - parent.startedAtMs >= args.limits.maxDurationMs ||
      parent.attemptsStarted >= args.limits.maxAttempts ||
      parent.piCallsStarted >= args.limits.maxPiCalls
    ) {
      throw new Error(
        "Parent campaign identity, policy or remaining budget differs",
      );
    }
  }
  const agentsPath = join(args.repository, "AGENTS.md");
  const agentsStat = await Deno.lstat(agentsPath);
  if (!agentsStat.isFile || agentsStat.isSymlink) {
    throw new Error("Project AGENTS.md must be a regular file");
  }
  const agentsBytes = await Deno.readFile(agentsPath);
  const agentsText = new TextDecoder("utf-8", { fatal: true }).decode(
    agentsBytes,
  );
  const agentsHash = createHash("sha256").update(agentsBytes).digest("hex");
  await Deno.mkdir(args.directory, { mode: 0o700 });
  const config = runConfigSchema.parse({
    arguments: args,
    scorePolicyVersion,
    startedAtMs: parent?.startedAtMs ?? now,
    supportManifest: manifest,
    cliVersion: await currentCliVersion(),
    dataDirectory: join(args.supportDirectory, ".swamp"),
  });
  const agentsContext = await store.writeResource(
    "inferenceInput",
    `${args.runId}-agents-context`,
    {
      runId: args.runId,
      source: "untracked project AGENTS.md",
      sha256: agentsHash,
      text: agentsText,
    },
  );
  const brief = args.experimentBrief === undefined
    ? null
    : await store.writeResource(
      "inferenceInput",
      `${args.runId}-experiment-brief`,
      experimentBriefSchema.parse({
        runId: args.runId,
        source: "prepared experiment brief",
        sha256: createHash("sha256").update(args.experimentBrief).digest("hex"),
        text: args.experimentBrief,
      }),
    );
  const saved = await store.writeResource(
    "runConfig",
    `${args.runId}-config`,
    config,
  );
  const state = await store.writeResource("state", `${args.runId}-state`, {
    ...newRun(args.runId, args.revision),
    initialRevision: parent?.initial?.revision ?? args.revision,
    bestRevision: parent?.bestRevision ?? args.revision,
    attemptsStarted: parent?.attemptsStarted ?? 0,
    piCallsStarted: parent?.piCallsStarted ?? 0,
    elapsedMs: parent
      ? Math.max(parent.state.elapsedMs, now - parent.startedAtMs)
      : 0,
  });
  return {
    config,
    dataHandles: [agentsContext, ...(brief ? [brief] : []), saved, state],
  };
}

export async function executeRun(
  store: ArtifactStore,
  runId: string,
  perform: (
    state: Readonly<RunState>,
    config: Configuration,
  ) => Promise<StepEffect>,
  now = Date.now,
) {
  const config = runConfigSchema.parse(
    await store.readResource(`${runId}-config`, 1),
  );
  if (config.arguments.runId !== runId) {
    throw new Error("Run configuration identity differs");
  }
  let state = runStateSchema.parse(await store.readResource(`${runId}-state`));
  await checkRunSupport(config);
  if (await currentCliVersion() !== config.cliVersion) {
    throw new Error("Swamp CLI version changed before run or resume");
  }
  if (state.runId !== runId) {
    throw new Error("Saved progress belongs to another run");
  }
  await assertNotAbandoned(config.arguments.directory);
  if (state.activeStep !== null) {
    throw new Error(
      "Interrupted step requires explicit recovery before resuming",
    );
  }
  let latest: OutputReference[] = [];
  while (state.phase !== "done") {
    await checkRunSupport(config);
    const result = await runSavedStep(
      store,
      runId,
      config.arguments.limits,
      Math.max(state.elapsedMs, now() - config.startedAtMs),
      async (active) => {
        await checkRunSupport(config);
        const effect = await perform(active, config);
        await checkRunSupport(config);
        return effect;
      },
    );
    state = result.state;
    latest = result.dataHandles;
    if (result.record.phase === "finish" && state.phase !== "done") {
      throw new Error(
        "Cleanup failed; inspect the saved finish result before resuming",
      );
    }
  }
  const finish = (await stepResource(store, state, "finish")).value;
  await checkRunSupport(config);
  const unfinished = revisionSchema.nullable().parse(
    finish?.unfinishedRevision ?? null,
  );
  const preparation = state.outputs.prepare
    ? stepResultSchema.parse(
      await store.readResource(
        state.outputs.prepare.name,
        state.outputs.prepare.version,
      ),
    )
    : null;
  const baseline = state.outputs.baseline
    ? stepResultSchema.parse(
      await store.readResource(
        state.outputs.baseline.name,
        state.outputs.baseline.version,
      ),
    )
    : null;
  const bestVerified = preparation?.outcome.kind === "prepared" &&
    baseline?.outcome.kind === "measurement" && baseline.outcome.valid;
  const report = await store.writeResource("runReport", `${runId}-report`, {
    runId,
    state: { ...state },
    diagnosticOnly: false,
    valid: bestVerified && state.stopReason !== "invalid-measurement" &&
      !state.stopReason?.startsWith("error:"),
    bestRevision: state.bestRevision,
    bestVerified,
    unfinishedRevision: unfinished === state.bestRevision ? null : unfinished,
    workspace: finish?.workspace ?? null,
    directory: config.arguments.directory,
    stopReason: state.stopReason,
    targetMet: state.targetMet,
  });
  return {
    state,
    dataHandles: [
      ...new Map([...latest, report].map((handle) => [handle.name, handle]))
        .values(),
    ],
  };
}

export async function performLiveStep(
  store: ArtifactStore,
  state: Readonly<RunState>,
  config: Configuration,
): Promise<StepEffect> {
  const args = config.arguments;
  const directory = join(args.directory, `step-${state.sequence}`);
  let workspace: Workspace | null = null;
  const existing = await store.readResource(`${state.runId}-workspace`, 1);
  if (existing) workspace = workspaceSchema.parse(existing);
  const detail = async (kind: string, value: Record<string, unknown>) =>
    ref(
      await store.writeResource(
        "runDetail",
        `${state.runId}-${kind}-${state.sequence}`,
        { runId: state.runId, ...value },
      ),
    );
  if (state.phase === "prepare") {
    await Deno.mkdir(join(args.repository, ".worktrees"), { recursive: true });
    workspace = await createWorkspace(
      args.repository,
      state.runId,
      args.revision,
    );
    await store.writeResource("workspace", `${state.runId}-workspace`, {
      ...workspace,
    });
    const tested = await runTestEffect(store, state, workspace, directory);
    if (tested.outcome.kind !== "tests" || !tested.outcome.passed) {
      return {
        outcome: {
          kind: "error",
          message: "Initial build and tests did not pass",
        },
        evidence: tested.evidence,
      };
    }
    await markBestRevision(workspace, state.bestRevision);
    return { outcome: { kind: "prepared" }, evidence: tested.evidence };
  }
  if (state.phase === "finish") {
    if (workspace) await stopRepl(state.runId, workspace.path);
    for (
      const phase of [
        "analyze",
        "research",
        "implement",
        "repair",
        "review",
        "test",
        "prepare",
        "start-attempt",
      ] as const
    ) {
      const reference = state.outputs[phase];
      if (!reference) continue;
      const step = stepResultSchema.parse(
        await store.readResource(reference.name, reference.version),
      );
      let suffix = "pi";
      if (["test", "prepare"].includes(phase)) suffix = "test";
      else if (phase === "start-attempt") suffix = "build";
      await stopProcessScope(`${state.runId}-${suffix}-${step.step}`);
    }
    for (const adapter of adapterSchema.options) {
      for (const protocol of protocolSchema.options) {
        await stopProcessScope(`${state.runId}-${adapter}-${protocol}`);
      }
    }
    for (const event of profileRequestSchema.shape.event.options) {
      for (const protocol of protocolSchema.options) {
        await stopProcessScope(`${state.runId}-${protocol}-${event}`);
      }
    }
    let unfinishedRevision: string | null = null;
    if (workspace) {
      unfinishedRevision = await snapshotWorkspace(
        workspace,
        `unfinished-${state.sequence}`,
      );
      await selectWorkspaceRevision(workspace, state.bestRevision);
    }
    return {
      outcome: { kind: "finished" },
      evidence: [
        await detail("finish", {
          unfinishedRevision,
          bestRevision: state.bestRevision,
          workspace,
        }),
      ],
    };
  }
  if (!workspace) throw new Error("Missing experiment worktree");
  await verifyWorkspaceRevision(workspace, state.workingRevision);
  if (state.phase === "start-attempt") {
    await Deno.mkdir(directory, { mode: 0o700 });
    const command = {
      id: `${state.runId}-build-${state.sequence}`,
      command: "bb",
      args: ["build"],
      cwd: workspace.path,
      stdoutPath: join(directory, "stdout.log"),
      stderrPath: join(directory, "stderr.log"),
    };
    const execution = await runCommand(command);
    const logs = await archiveLogs(
      store,
      "testLog",
      command.id,
      command,
      !execution.success,
    );
    await verifyWorkspaceRevision(workspace, state.workingRevision);
    const saved = await detail("build", {
      revision: state.workingRevision,
      success: execution.success,
      cleanup: execution.cleanup,
      exitCode: execution.status?.code ?? null,
      logs: logs.map(ref),
    });
    return {
      outcome: execution.success
        ? { kind: "attempt-started" }
        : { kind: "error", message: "Experiment build failed" },
      evidence: [...logs.map(ref), saved],
    };
  }
  if (state.phase === "test") {
    await stopRepl(state.runId, workspace.path);
    return runTestEffect(store, state, workspace, directory);
  }
  if (state.phase === "profile") {
    const result = await runProfile(store, {
      runId: state.runId,
      repository: workspace.path,
      directory,
      collectorDirectory: join(args.supportDirectory, "clojure"),
      protocol: protocolSchema.parse(
        args.policy.goal.kind === "beat-all"
          ? Object.keys(args.policy.goal.competitors)[0]
          : args.policy.goal.workloads[0],
      ),
      event: profileRequestSchema.shape.event.parse(state.requestedProfile),
      smoke: false,
      parameters: {
        warmup: args.parameters.warmup,
        duration: args.parameters.duration,
        connections: args.parameters.connections,
        streams: args.parameters.streams,
        threads: args.parameters.threads,
      },
    });
    return {
      outcome: result.valid
        ? { kind: "profile" }
        : { kind: "error", message: "Diagnostic recording was invalid" },
      evidence: [ref(result.saved)],
    };
  }
  if (state.phase === "baseline" || state.phase === "measure") {
    await stopRepl(state.runId, workspace.path);
    await Deno.mkdir(directory, { mode: 0o700 });
    const workloads = args.policy.goal.kind === "beat-all"
      ? Object.keys(args.policy.goal.competitors)
      : args.policy.goal.workloads;
    const current: OutputReference[] = [];
    for (
      const workload of new Set([
        ...workloads,
        ...args.policy.protections.map((item) => item.workload),
      ])
    ) {
      const competitors =
        state.phase === "baseline" && args.policy.goal.kind === "beat-all"
          ? args.policy.goal.competitors[workload] ?? []
          : [];
      for (const adapter of ["busker", ...competitors]) {
        const result = await runBenchmark(store, {
          runId: state.runId,
          repository: workspace.path,
          directory: join(directory, `${adapter}-${workload}`),
          purpose: "measurement",
          adapter: adapterSchema.parse(adapter),
          protocol: protocolSchema.parse(workload),
          parameters: args.parameters,
          precision: args.policy.precision,
          deadlineMs: config.startedAtMs + args.limits.maxDurationMs,
        });
        current.push(ref(result.saved));
        if (!result.valid) {
          return {
            outcome: {
              kind: "measurement",
              revision: state.workingRevision,
              valid: false,
              inconclusive: state.phase === "baseline"
                ? result.inconclusive
                : result.precisionFailure,
              targetMet: false,
              improved: false,
            },
            evidence: current,
          };
        }
      }
    }
    const parent = args.parentEvidence
      ? await verifyParentEvidence(
        args.parentEvidence.path,
        args.parentEvidence.sha256,
        (value) => runConfigSchema.parse(value),
      )
      : null;
    const initial = parent?.initial
      ? measurementRefs(parent.initial)
      : state.phase === "baseline"
      ? current
      : await measurementReferences(store, state, "baseline");
    const freshBest =
      state.phase === "measure" && !state.outputs.keep && parent?.best
        ? await measurementReferences(store, state, "baseline")
        : null;
    const requalified = freshBest
      ? await readMeasurement(
        store,
        state.runId,
        state.bestRevision,
        freshBest,
        args.policy.precision,
      )
      : null;
    const oldH1 = parent?.best?.scores.find((cell) =>
      cell.adapter === "busker" && cell.workload === "h1"
    )?.requestsPerSecond;
    const freshH1 = requalified?.scores.find((cell) =>
      cell.adapter === "busker" && cell.workload === "h1"
    )?.requestsPerSecond;
    const useFreshBest = oldH1 !== undefined && freshH1 !== undefined &&
      freshH1 > oldH1;
    const best = state.outputs.keep
      ? z.array(
        z.strictObject({
          name: z.string(),
          version: z.number().int().positive(),
        }),
      ).parse((await stepResource(store, state, "keep")).value?.measurements)
      : useFreshBest && freshBest
      ? freshBest
      : parent?.best
      ? measurementRefs(parent.best)
      : initial;
    return compareSavedMeasurements(
      store,
      state,
      args.policy.goal,
      args.policy.protections,
      { initial, best, current },
      args.policy.minimumImprovementFraction,
      args.policy.precision,
      parent
        ? {
          initial: parent.initial,
          best: state.outputs.keep || useFreshBest ? null : parent.best,
          runId: parent.sourceRunId,
          sha256: parent.sourceDigest,
        }
        : null,
    );
  }
  if (state.phase === "keep") {
    const measurements = await measurementReferences(store, state, "measure");
    await markBestRevision(workspace, state.workingRevision);
    return {
      outcome: { kind: "kept", revision: state.workingRevision },
      evidence: [
        await detail("keep", { revision: state.workingRevision, measurements }),
      ],
    };
  }
  if (state.phase === "discard") {
    await snapshotWorkspace(workspace, `discarded-${state.sequence}`);
    await selectWorkspaceRevision(workspace, state.bestRevision);
    return {
      outcome: { kind: "discarded", revision: state.bestRevision },
      evidence: [
        await detail("discard", {
          rejectedRevision: state.workingRevision,
          restoredRevision: state.bestRevision,
        }),
      ],
    };
  }
  const history = [];
  for (
    let step = Math.max(1, state.sequence - 20);
    step < state.sequence;
    step++
  ) {
    const saved = stepResultSchema.parse(
      await store.readResource(`${state.runId}-step-${step}`, 1),
    );
    history.push({
      step,
      phase: saved.phase,
      revision: saved.revision,
      outcome: saved.outcome,
    });
  }
  const handoff = await detail("handoff", {
    revision: state.workingRevision,
    policy: args.policy,
    limits: args.limits,
    history,
    attemptsStarted: state.attemptsStarted,
    repairsThisAttempt: state.repairsThisAttempt,
  });
  const inputs = [
    { name: `${state.runId}-agents-context`, version: 1 },
    handoff,
  ];
  if (state.phase === "analyze" && args.experimentBrief !== undefined) {
    inputs.push({ name: `${state.runId}-experiment-brief`, version: 1 });
  }
  for (
    const phase of [
      "baseline",
      "profile",
      "analyze",
      "research",
      "implement",
      "repair",
      "test",
      "measure",
      "review",
    ] as const
  ) {
    if (state.outputs[phase]) {
      inputs.push((await stepResource(store, state, phase)).reference);
    }
  }
  const context = inferenceContextSchema.parse({
    runId: state.runId,
    step: state.sequence,
    stage: state.phase,
    revision: state.workingRevision,
    snapshotSha256: createHash("sha256").update(
      JSON.stringify({ revision: state.workingRevision, inputs }),
    ).digest("hex"),
    inputs,
  });
  const usingRepl = state.phase === "implement" || state.phase === "repair";
  if (usingRepl) await startRepl(state.runId, workspace.path);
  let result: StepEffect;
  try {
    result = await runPiEffect(store, state, {
      context,
      workspace: workspace.path,
      reviewBaseRevision: state.phase === "review"
        ? state.bestRevision
        : undefined,
      directory,
      promptsDirectory: join(args.supportDirectory, "prompts"),
      extensionPath: join(args.supportDirectory, "pi/restricted-tools.js"),
      credentialDirectory: args.credentialDirectory,
      referenceRoots: args.referenceRoots,
    }, () => snapshotWorkspace(workspace, `edit-${state.sequence}`));
  } finally {
    if (usingRepl) await stopRepl(state.runId, workspace.path);
  }
  if (!usingRepl) {
    await verifyWorkspaceRevision(workspace, state.workingRevision);
  }
  return result;
}
