import { resolve } from "node:path";
export interface CommandSpec {
  id: string;
  command: string;
  args: string[];
  cwd: string;
  env?: Record<string, string>;
  stdoutPath: string;
  stderrPath: string;
}

export function processScope(id: string) {
  if (!/^[a-zA-Z0-9_-]{1,180}$/.test(id)) throw new Error("Invalid process ID");
  return { unit: `tempo-${id}.scope`, description: `Tempo process ${id}` };
}

async function scopeDescription(unit: string) {
  const result = await new Deno.Command("systemctl", {
    args: ["--user", "show", unit, "--property=LoadState,Description"],
  }).output();
  const text = new TextDecoder().decode(result.stdout);
  const fields = new Map(
    text.trim().split("\n").map((line) => {
      const split = line.indexOf("=");
      return [line.slice(0, split), line.slice(split + 1)];
    }),
  );
  if (fields.get("LoadState") === "not-found") return null;
  if (!result.success || fields.get("LoadState") !== "loaded") {
    throw new Error(
      `Cannot inspect process scope: ${
        new TextDecoder().decode(result.stderr)
      }`,
    );
  }
  return fields.get("Description");
}

export async function stopProcessScope(id: string) {
  const scope = processScope(id);
  const description = await scopeDescription(scope.unit);
  if (description === null) return "absent" as const;
  if (description !== scope.description) {
    throw new Error("Refusing to stop a scope with a different description");
  }
  const result = await new Deno.Command("systemctl", {
    args: ["--user", "stop", scope.unit],
  }).output();
  if (!result.success) {
    if (await scopeDescription(scope.unit) === null) return "absent" as const;
    throw new Error(
      `Scope cleanup failed: ${new TextDecoder().decode(result.stderr)}`,
    );
  }
  return "stopped" as const;
}

export async function runCommand(spec: CommandSpec) {
  const scope = processScope(spec.id);
  if (await scopeDescription(scope.unit) !== null) {
    throw new Error("Process ID already exists");
  }
  const stdoutPath = resolve(spec.stdoutPath);
  const stderrPath = resolve(spec.stderrPath);
  const stdout = await Deno.open(stdoutPath, {
    createNew: true,
    write: true,
    mode: 0o600,
  });
  stdout.close();
  try {
    const stderr = await Deno.open(stderrPath, {
      createNew: true,
      write: true,
      mode: 0o600,
    });
    stderr.close();
  } catch (error) {
    await Deno.remove(stdoutPath);
    throw error;
  }
  const startedPath = `${stdoutPath}.started`;
  try {
    await Deno.stat(startedPath);
    throw new Error("Scoped command start record already exists");
  } catch (failure) {
    if (!(failure instanceof Deno.errors.NotFound)) throw failure;
  }
  let child: Deno.ChildProcess;
  try {
    child = new Deno.Command("bash", {
      cwd: spec.cwd,
      env: spec.env,
      args: [
        "-c",
        'out=$1; err=$2; shift 2; exec "$@" >> "$out" 2>> "$err"',
        "tempo-launch",
        stdoutPath,
        stderrPath,
        "systemd-run",
        "--user",
        "--scope",
        "--quiet",
        "--collect",
        "--expand-environment=no",
        `--unit=${scope.unit}`,
        `--description=${scope.description}`,
        "--property=TimeoutStopSec=5s",
        "--",
        "bash",
        "-c",
        'out=$1; err=$2; started=$3; shift 3; command -v "$1" >/dev/null || { printf "Scoped executable unavailable: %s\\n" "$1" >&2; exit 127; }; umask 077; set -C; printf "started\\n" > "$started" || exit 126; exec "$@" >> "$out" 2>> "$err"',
        "tempo-command",
        stdoutPath,
        stderrPath,
        startedPath,
        spec.command,
        ...spec.args,
      ],
      stdin: "null",
      stdout: "null",
      stderr: "null",
    }).spawn();
  } catch (failure) {
    throw new Error(
      `Cannot launch scoped command; logs remain at ${stderrPath}`,
      {
        cause: failure,
      },
    );
  }
  let status: Deno.CommandStatus | null = null;
  let error: string | null = null;
  let cleanup: "absent" | "stopped" | "failed" = "failed";
  try {
    status = await child.status;
  } catch (failure) {
    error = String(failure);
  }
  try {
    cleanup = await stopProcessScope(spec.id);
    status ??= await child.status;
  } catch (failure) {
    error = [error, String(failure)].filter(Boolean).join("; ");
    child.unref();
  }
  try {
    await Deno.stat(startedPath);
  } catch (failure) {
    error = [error, `Scoped executable did not start: ${String(failure)}`]
      .filter(Boolean).join("; ");
  }
  return {
    unit: scope.unit,
    status,
    cleanup,
    error,
    success: status?.success === true && cleanup !== "failed" && error === null,
    stdoutPath: spec.stdoutPath,
    stderrPath: spec.stderrPath,
  };
}
