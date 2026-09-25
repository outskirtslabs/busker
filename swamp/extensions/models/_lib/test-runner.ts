import {
  basename,
  dirname,
  isAbsolute,
  join,
  relative,
  resolve,
} from "node:path";
import { archiveLogs, type ArtifactStore, logExcerpt } from "./artifacts.ts";
import type { StepEffect } from "./driver.ts";
import { runCommand } from "./process.ts";
import type { RunState } from "./state.ts";
import { verifyWorkspaceRevision, type Workspace } from "./workspace.ts";

export async function runTestEffect(
  store: ArtifactStore,
  state: Readonly<RunState>,
  workspace: Workspace,
  directory: string,
): Promise<StepEffect> {
  if (
    !["prepare", "test"].includes(state.phase) || state.activeStep === null ||
    state.activeStep !== state.sequence || state.runId !== workspace.runId
  ) {
    throw new Error("Tests require the matching active test step");
  }
  if (!Deno.env.get("IN_NIX_SHELL")) {
    throw new Error("Tests require the Nix devshell");
  }
  await verifyWorkspaceRevision(workspace, state.workingRevision);
  if (
    !isAbsolute(directory) ||
    join(await Deno.realPath(dirname(directory)), basename(directory)) !==
      resolve(directory)
  ) {
    throw new Error(
      "Test log directory must be absolute and must not traverse a symlink",
    );
  }
  const suffix = relative(workspace.path, directory);
  if (
    suffix === "" ||
    (!isAbsolute(suffix) && suffix !== ".." && !suffix.startsWith("../"))
  ) {
    throw new Error("Test logs must be outside the experiment worktree");
  }
  await Deno.mkdir(directory, { mode: 0o700 });
  const prefix = `${state.runId}-test-${state.sequence}`;
  const command = {
    id: prefix,
    command: "bb",
    args: ["qa"],
    cwd: workspace.path,
    stdoutPath: join(directory, "stdout.log"),
    stderrPath: join(directory, "stderr.log"),
  };
  let execution: Awaited<ReturnType<typeof runCommand>> | null = null;
  let error: string | null = null;
  try {
    execution = await runCommand(command);
    error = execution.error;
  } catch (failure) {
    error = String(failure);
  }
  let sourceUnchanged = false;
  try {
    await verifyWorkspaceRevision(workspace, state.workingRevision);
    sourceUnchanged = true;
  } catch (failure) {
    error = [error, String(failure)].filter(Boolean).join("; ");
  }
  const artifacts = await archiveLogs(
    store,
    "testLog",
    prefix,
    command,
    execution?.success !== true,
  );
  const valid = sourceUnchanged && execution?.status != null &&
    execution.cleanup !== "failed" && error === null;
  const passed = valid && execution?.success === true;
  const saved = await store.writeResource("testExecution", `${prefix}-result`, {
    runId: state.runId,
    step: state.sequence,
    revision: state.workingRevision,
    command: command.command,
    args: command.args,
    valid,
    passed,
    sourceUnchanged,
    error,
    exitCode: execution?.status?.code ?? null,
    signal: execution?.status?.signal ?? null,
    cleanup: execution?.cleanup ?? null,
    failureOutput: valid && !passed
      ? {
        stdout: await logExcerpt(command.stdoutPath),
        stderr: await logExcerpt(command.stderrPath),
      }
      : null,
    artifacts: artifacts.map(({ name, version }) => ({ name, version })),
  });
  return {
    outcome: valid
      ? { kind: "tests", revision: state.workingRevision, passed }
      : {
        kind: "error",
        message: error ?? "Test execution or process cleanup failed",
      },
    evidence: [...artifacts, saved].map(({ name, version }) => ({
      name,
      version,
    })),
  };
}
