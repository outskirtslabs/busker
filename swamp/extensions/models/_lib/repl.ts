import { join } from "node:path";
import { processScope, stopProcessScope } from "./process.ts";

function replId(runId: string) {
  return `${runId}-repl`;
}

export async function startRepl(runId: string, workspace: string) {
  const portFile = join(workspace, ".nrepl-port");
  try {
    await Deno.lstat(portFile);
    throw new Error("Experiment worktree already has a REPL port file");
  } catch (error) {
    if (!(error instanceof Deno.errors.NotFound)) throw error;
  }
  const scope = processScope(replId(runId));
  const started = await new Deno.Command("tmux", {
    args: [
      "new-window",
      "-d",
      "-t",
      "busker",
      "-c",
      workspace,
      "-n",
      `tempo-${runId}-repl`,
      "--",
      `exec systemd-run --user --scope --quiet --collect --expand-environment=no --unit=${scope.unit} --description='${scope.description}' --property=TimeoutStopSec=5s -- bb dev`,
    ],
  }).output();
  if (!started.success) {
    throw new Error(
      `Cannot start experiment REPL: ${
        new TextDecoder().decode(started.stderr)
      }`,
    );
  }
  try {
    for (let attempt = 0; attempt < 60; attempt++) {
      try {
        if ((await Deno.stat(portFile)).isFile) {
          const probe = await new Deno.Command("brepl", {
            cwd: workspace,
            args: ["(+ 1 2)"],
            env: { BREPL_PORT: "" },
          }).output();
          if (
            probe.success &&
            new TextDecoder().decode(probe.stdout).trim() === "3"
          ) {
            return;
          }
        }
      } catch (error) {
        if (!(error instanceof Deno.errors.NotFound)) throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    throw new Error("Experiment REPL did not become ready");
  } catch (error) {
    await stopRepl(runId, workspace);
    throw error;
  }
}

export async function stopRepl(runId: string, workspace: string) {
  const stopped = await stopProcessScope(replId(runId));
  const portFile = join(workspace, ".nrepl-port");
  try {
    const stat = await Deno.lstat(portFile);
    if (!stat.isFile || stat.isSymlink) {
      throw new Error("Unexpected REPL port file");
    }
    await Deno.remove(portFile);
  } catch (error) {
    if (!(error instanceof Deno.errors.NotFound)) throw error;
  }
  return stopped;
}
