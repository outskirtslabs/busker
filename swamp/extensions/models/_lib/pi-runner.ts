import { createHash } from "node:crypto";
import {
  basename,
  dirname,
  isAbsolute,
  join,
  relative,
  resolve,
} from "node:path";
import { z } from "npm:zod@4.4.3";
import {
  type InferenceContext,
  type OutputReference,
  saveInference,
} from "./inference.ts";
import { inferenceContextSchema } from "./schemas.ts";
import { parsePiEvents, piArguments } from "./pi.ts";
import { runCommand } from "./process.ts";
import type { Policy } from "../../../pi/guard.ts";
import { archiveLogs, type ArtifactStore } from "./artifacts.ts";
import { readInferenceEffect, type StepEffect } from "./driver.ts";
import type { RunState } from "./state.ts";

export type PiStore = ArtifactStore;
export interface PiRequest {
  context: InferenceContext;
  reviewBaseRevision?: string;
  workspace: string;
  referenceRoots: string[];
  directory: string;
  promptsDirectory: string;
  extensionPath: string;
  credentialDirectory: string;
}

export interface PiExecution {
  success: boolean;
  error: string | null;
  status?: { code: number; signal: string | null } | null;
  cleanup?: "absent" | "stopped" | "failed";
}

export async function preparePiStep(store: PiStore, request: PiRequest) {
  let context = inferenceContextSchema.parse(request.context);
  const workspace = await Deno.realPath(request.workspace);
  if (!isAbsolute(request.directory)) {
    throw new Error("Pi step directory must be absolute");
  }
  if (
    ![
      request.credentialDirectory,
      request.promptsDirectory,
      request.extensionPath,
    ].every(isAbsolute)
  ) throw new Error("Pi configuration paths must be absolute");
  const canonicalDirectory = join(
    await Deno.realPath(dirname(request.directory)),
    basename(request.directory),
  );
  if (canonicalDirectory !== resolve(request.directory)) {
    throw new Error("Pi step directory must not traverse a symlink");
  }
  const suffix = relative(workspace, request.directory);
  if (!suffix.startsWith("../") && !isAbsolute(suffix)) {
    throw new Error("Pi state must be outside the editable worktree");
  }
  let credentialRoot = request.credentialDirectory;
  try {
    credentialRoot = await Deno.realPath(credentialRoot);
  } catch (error) {
    if (!(error instanceof Deno.errors.NotFound)) throw error;
  }
  const referenceRoots = await Promise.all(
    request.referenceRoots.map((path) => Deno.realPath(path)),
  );
  for (const root of referenceRoots) {
    for (const privatePath of [request.directory, credentialRoot]) {
      const relationships = [
        relative(root, privatePath),
        relative(privatePath, root),
      ];
      if (
        relationships.some((path) =>
          path === "" ||
          (!isAbsolute(path) && path !== ".." && !path.startsWith("../"))
        )
      ) {
        throw new Error(
          "Reference directories must not expose Pi configuration or credentials",
        );
      }
    }
  }
  const inputs = [];
  for (const reference of context.inputs) {
    const data = await store.readResource(reference.name, reference.version);
    if (data === null || data.runId !== context.runId) {
      throw new Error("Input is missing or belongs to another run");
    }
    if (data.status === "invalid") {
      throw new Error("Invalid inference output cannot be supplied to Pi");
    }
    inputs.push({ reference, data });
    if (reference.name === `${context.runId}-agents-context`) {
      const frozen = z.object({
        source: z.literal("untracked project AGENTS.md"),
        sha256: z.string().regex(/^[0-9a-f]{64}$/),
        text: z.string(),
      }).parse(data);
      if (
        reference.version !== 1 ||
        createHash("sha256").update(frozen.text).digest("hex") !== frozen.sha256
      ) {
        throw new Error("Frozen AGENTS.md input differs from its saved hash");
      }
    }
    if (reference.name === `${context.runId}-experiment-brief`) {
      const frozen = z.object({
        source: z.literal("prepared experiment brief"),
        sha256: z.string().regex(/^[0-9a-f]{64}$/),
        text: z.string().min(1).max(32_768),
      }).parse(data);
      if (
        reference.version !== 1 ||
        createHash("sha256").update(frozen.text).digest("hex") !== frozen.sha256
      ) {
        throw new Error("Frozen experiment brief differs from its saved hash");
      }
    }
  }
  let reviewDiff:
    | { baseRevision: string; bytes: Uint8Array; sha256: string }
    | null = null;
  if (context.stage === "review") {
    const baseRevision = request.reviewBaseRevision;
    if (
      !baseRevision || !/^[0-9a-f]{40}$/.test(baseRevision) ||
      baseRevision === context.revision
    ) {
      throw new Error(
        "Review needs distinct full base and candidate revisions",
      );
    }
    for (const revision of [baseRevision, context.revision]) {
      const checked = await new Deno.Command("git", {
        cwd: workspace,
        args: ["cat-file", "-t", revision],
      }).output();
      if (
        !checked.success || new TextDecoder().decode(checked.stdout).trim() !==
          "commit"
      ) {
        throw new Error("Review revision is not a Git commit");
      }
    }
    const diff = await new Deno.Command("git", {
      cwd: workspace,
      args: [
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--binary",
        "--full-index",
        baseRevision,
        context.revision,
        "--",
      ],
    }).output();
    if (!diff.success || diff.stdout.length === 0) {
      throw new Error("Review needs a nonempty committed revision diff");
    }
    reviewDiff = {
      baseRevision,
      bytes: diff.stdout,
      sha256: createHash("sha256").update(diff.stdout).digest("hex"),
    };
  }
  await Deno.mkdir(request.directory, { mode: 0o700 });
  const inputDirectory = join(request.directory, "inputs");
  const agentDirectory = join(request.directory, "agent");
  await Deno.mkdir(inputDirectory, { mode: 0o700 });
  await Deno.mkdir(agentDirectory, { mode: 0o700 });
  await Deno.writeTextFile(
    join(agentDirectory, "settings.json"),
    JSON.stringify({
      compaction: { enabled: false },
      retry: { enabled: false, provider: { maxRetries: 0 } },
      cacheWarming: "off",
      defaultTools: [],
      defaultProjectTrust: "never",
      enableAnalytics: false,
      enableInstallTelemetry: false,
      packages: [],
      extensions: [],
      skills: [],
      prompts: [],
      themes: [],
    }),
    { createNew: true, mode: 0o600 },
  );
  const policy: Policy = {
    workspace,
    readRoots: [inputDirectory, ...referenceRoots],
    stage: context.stage,
  };
  const paths = {
    manifest: join(inputDirectory, "manifest.json"),
    policy: join(request.directory, "policy.json"),
    extension: request.extensionPath,
    systemPrompt: join(inputDirectory, "system.md"),
    taskPrompt: join(inputDirectory, `${context.stage}.md`),
  };
  const promptHandles: OutputReference[] = [];
  for (
    const [name, destination] of [["system", paths.systemPrompt], [
      context.stage,
      paths.taskPrompt,
    ]]
  ) {
    const text = await Deno.readTextFile(
      join(request.promptsDirectory, `${name}.md`),
    );
    await Deno.writeTextFile(destination, text, {
      createNew: true,
      mode: 0o400,
    });
    promptHandles.push(
      await store.createFileWriter(
        "piPrompt",
        `${context.runId}-pi-${context.step}-${name}-prompt`,
      ).writeText(text),
    );
  }
  const reviewDiffPath = join(inputDirectory, "revision.diff");
  let reviewDiffHandle: OutputReference | null = null;
  if (reviewDiff) {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(
      reviewDiff.bytes,
    );
    await Deno.writeFile(reviewDiffPath, reviewDiff.bytes, {
      createNew: true,
      mode: 0o400,
    });
    const reviewDiffInput = {
      baseRevision: reviewDiff.baseRevision,
      candidateRevision: context.revision,
      sha256: reviewDiff.sha256,
      diff: text,
    };
    reviewDiffHandle = await store.createFileWriter(
      "piInput",
      `${context.runId}-pi-${context.step}-review-diff`,
    ).writeText(JSON.stringify(reviewDiffInput));
    const reference = {
      name: reviewDiffHandle.name,
      version: reviewDiffHandle.version,
    };
    context = inferenceContextSchema.parse({
      ...context,
      inputs: [...context.inputs, reference],
    });
    inputs.push({ reference, data: reviewDiffInput });
  }
  const manifest = JSON.stringify(
    {
      context,
      inputs,
      prompts: promptHandles.map(({ name, version }) => ({ name, version })),
      reviewDiff: reviewDiff && reviewDiffHandle
        ? {
          baseRevision: reviewDiff.baseRevision,
          candidateRevision: context.revision,
          sha256: reviewDiff.sha256,
          path: reviewDiffPath,
          reference: reviewDiffHandle,
        }
        : undefined,
    },
    null,
    2,
  ) + "\n";
  await Deno.writeTextFile(paths.manifest, manifest, {
    createNew: true,
    mode: 0o400,
  });
  await Deno.writeTextFile(paths.policy, JSON.stringify(policy), {
    createNew: true,
    mode: 0o400,
  });
  const args = piArguments(context.stage, paths);
  const inputHandle = await store.createFileWriter(
    "piInput",
    `${context.runId}-pi-${context.step}-input`,
  ).writeText(manifest);
  const auth = join(request.credentialDirectory, "auth.json");
  try {
    if (!(await Deno.stat(auth)).isFile) {
      throw new Error("Pi credentials are not a regular file");
    }
    await Deno.symlink(auth, join(agentDirectory, "auth.json"));
  } catch (error) {
    if (!(error instanceof Deno.errors.NotFound)) throw error;
  }
  return {
    context,
    policy,
    inputHandle,
    reviewDiffHandle,
    promptHandles,
    agentDirectory,
    command: {
      id: `${context.runId}-pi-${context.step}`,
      command: "pi",
      args,
      cwd: workspace,
      stdoutPath: join(request.directory, "stdout.jsonl"),
      stderrPath: join(request.directory, "stderr.log"),
      env: {
        PI_CODING_AGENT_DIR: agentDirectory,
        PI_CODING_AGENT_SESSION_DIR: join(agentDirectory, "sessions"),
        PI_SKIP_VERSION_CHECK: "1",
        PI_TELEMETRY: "0",
      },
    },
  };
}

export async function finishPiStep(
  store: PiStore,
  prepared: Awaited<ReturnType<typeof preparePiStep>>,
  execution: PiExecution,
) {
  const { context, command, policy } = prepared;
  const handles: OutputReference[] = [
    prepared.inputHandle,
    ...prepared.promptHandles,
  ];
  if (prepared.reviewDiffHandle) handles.push(prepared.reviewDiffHandle);
  handles.push(
    ...await archiveLogs(
      store,
      "piLog",
      `${context.runId}-pi-${context.step}`,
      command,
      !execution.success,
    ),
  );
  let text = "";
  let error = execution.error;
  let parsed: ReturnType<typeof parsePiEvents> | null = null;
  try {
    text = await Deno.readTextFile(command.stdoutPath);
    if (!execution.success) {
      throw new Error(execution.error ?? "Pi process or cleanup failed");
    }
    parsed = parsePiEvents(text, policy);
  } catch (failure) {
    error = String(failure);
  }
  const executionHandle = await store.writeResource(
    "piExecution",
    `${context.runId}-pi-${context.step}-execution`,
    {
      runId: context.runId,
      step: context.step,
      revision: context.revision,
      success: parsed !== null,
      error,
      modelTurns: parsed?.modelTurns ?? null,
      totalTokens: parsed?.totalTokens ?? null,
      exitCode: execution.status?.code ?? null,
      signal: execution.status?.signal ?? null,
      cleanup: execution.cleanup ?? null,
      artifacts: handles.map(({ name, version }) => ({ name, version })),
    },
  );
  const saved = await saveInference(
    store,
    context,
    parsed?.response ?? text,
    execution.status?.code ?? null,
    parsed === null ? error ?? "Pi output was not validated" : undefined,
  );
  return {
    ...saved,
    execution,
    dataHandles: [...handles, executionHandle, ...saved.dataHandles],
  };
}

export async function runPiStep(store: PiStore, request: PiRequest) {
  const prepared = await preparePiStep(store, request);
  let execution: PiExecution;
  try {
    execution = await runCommand(prepared.command);
  } catch (failure) {
    execution = { success: false, error: String(failure) };
  }
  try {
    return await finishPiStep(store, prepared, execution);
  } finally {
    if (execution.cleanup !== "failed") {
      await Deno.remove(prepared.agentDirectory, { recursive: true });
    }
  }
}

export async function runPiEffect(
  store: PiStore,
  state: Readonly<RunState>,
  request: PiRequest,
  saveChanges?: () => Promise<string>,
): Promise<StepEffect> {
  const context = inferenceContextSchema.parse(request.context);
  if (
    state.activeStep !== state.sequence || state.activeStep !== context.step ||
    state.runId !== context.runId || state.phase !== context.stage ||
    state.workingRevision !== context.revision
  ) {
    throw new Error("Pi request does not match the active step");
  }
  const writable = context.stage === "implement" || context.stage === "repair";
  if (writable && !saveChanges) {
    throw new Error("Editing requires a Swamp Git snapshot operation");
  }
  const saved = await runPiStep(store, request);
  const evidence = saved.dataHandles.map(({ name, version }) => ({
    name,
    version,
  }));
  try {
    if (saved.execution.cleanup === "failed") {
      throw new Error(
        "Pi processes are still active; preserve the worktree without snapshotting it",
      );
    }
    // Preserve edits even when Pi returned malformed or unsuccessful output.
    const revision = writable && saveChanges ? await saveChanges() : undefined;
    const effect = await readInferenceEffect(
      store,
      state,
      saved.record,
      saved.saved,
      revision,
    );
    return { ...effect, evidence };
  } catch (failure) {
    return { outcome: { kind: "error", message: String(failure) }, evidence };
  }
}
