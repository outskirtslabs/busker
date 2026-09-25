import { deepStrictEqual, rejects } from "node:assert/strict";
import { join } from "node:path";
import {
  runCommand,
  stopProcessScope,
} from "../../extensions/models/_lib/process.ts";

Deno.test("command scope drains logs and stops descendants after their parent exits", async () => {
  const directory = await Deno.makeTempDir({
    prefix: "tempo-command-fixture-",
  });
  const id = `fixture-${crypto.randomUUID()}`;
  try {
    const result = await runCommand({
      id,
      cwd: directory,
      command: "sh",
      args: [
        "-c",
        "sleep 60 & printf '%s\\n' \"$!\"; printf 'diagnostic\\n' >&2; exit 7",
      ],
      stdoutPath: join(directory, "stdout"),
      stderrPath: join(directory, "stderr"),
    });
    deepStrictEqual([result.success, result.status?.code, result.error], [
      false,
      7,
      null,
    ]);
    deepStrictEqual(await Deno.readTextFile(result.stderrPath), "diagnostic\n");
    const pid = (await Deno.readTextFile(result.stdoutPath)).trim();
    if (!/^\d+$/.test(pid)) {
      throw new Error("Fixture did not report its child PID");
    }
    try {
      const stat = await Deno.readTextFile(`/proc/${pid}/stat`);
      const state = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0];
      deepStrictEqual(state, "Z");
    } catch (error) {
      if (!(error instanceof Deno.errors.NotFound)) throw error;
    }
  } finally {
    await stopProcessScope(id);
    await Deno.remove(directory, { recursive: true });
  }
});

Deno.test("command scope passes arguments literally and inherits the devshell environment", async () => {
  const directory = await Deno.makeTempDir({
    prefix: "tempo-command-fixture-",
  });
  const id = `fixture-${crypto.randomUUID()}`;
  try {
    const result = await runCommand({
      id,
      cwd: directory,
      command: "sh",
      args: [
        "-c",
        'printf \'%s|%s\' "$1" "$IN_NIX_SHELL"',
        "fixture",
        "$HOME; $(false)",
      ],
      stdoutPath: join(directory, "stdout"),
      stderrPath: join(directory, "stderr"),
    });
    deepStrictEqual(result.success, true, JSON.stringify(result));
    deepStrictEqual(
      await Deno.readTextFile(result.stdoutPath),
      `$HOME; $(false)|${Deno.env.get("IN_NIX_SHELL") ?? ""}`,
    );
  } finally {
    await stopProcessScope(id);
    await Deno.remove(directory, { recursive: true });
  }
});

Deno.test("scoped command retains launcher errors and refuses old output files", async () => {
  const directory = await Deno.makeTempDir({ prefix: "tempo-command-errors-" });
  const id = `fixture-${crypto.randomUUID()}`;
  try {
    const spec = {
      id,
      cwd: directory,
      command: "tempo-no-such-executable",
      args: ["literal; $HOME"],
      stdoutPath: join(directory, "stdout"),
      stderrPath: join(directory, "stderr"),
    };
    const result = await runCommand(spec);
    deepStrictEqual(result.success, false);
    deepStrictEqual(result.error?.includes("did not start"), true);
    deepStrictEqual(result.cleanup !== "failed", true);
    deepStrictEqual(await Deno.readTextFile(result.stdoutPath), "");
    const stderr = await Deno.readTextFile(result.stderrPath);
    deepStrictEqual(stderr.includes("tempo-no-such-executable"), true);
    deepStrictEqual((await Deno.stat(result.stderrPath)).mode! & 0o077, 0);
    await rejects(() => runCommand(spec), Deno.errors.AlreadyExists);
    deepStrictEqual(await Deno.readTextFile(result.stderrPath), stderr);
  } finally {
    await stopProcessScope(id);
    await Deno.remove(directory, { recursive: true });
  }
});

Deno.test("launcher failure retains diagnostics and cannot count as failed candidate tests", async () => {
  const directory = await Deno.makeTempDir({ prefix: "tempo-launch-error-" });
  const id = `fixture-${crypto.randomUUID()}`;
  try {
    const located = await new Deno.Command("which", { args: ["bash"] })
      .output();
    deepStrictEqual(located.success, true);
    await Deno.symlink(
      new TextDecoder().decode(located.stdout).trim(),
      join(directory, "bash"),
    );
    const result = await runCommand({
      id,
      cwd: directory,
      command: "bash",
      args: ["-c", "exit 7"],
      env: { PATH: directory },
      stdoutPath: join(directory, "stdout"),
      stderrPath: join(directory, "stderr"),
    });
    deepStrictEqual(result.success, false);
    deepStrictEqual(result.error?.includes("did not start"), true);
    deepStrictEqual(
      (await Deno.readTextFile(result.stderrPath)).includes("systemd-run"),
      true,
    );
  } finally {
    await stopProcessScope(id);
    await Deno.remove(directory, { recursive: true });
  }
});

Deno.test("cleanup failure is reported without discarding child output", async () => {
  const directory = await Deno.makeTempDir({ prefix: "tempo-cleanup-error-" });
  const id = `fixture-${crypto.randomUUID()}`;
  const priorPath = Deno.env.get("PATH") ?? "";
  try {
    const located = await new Deno.Command("which", { args: ["systemctl"] })
      .output();
    deepStrictEqual(located.success, true);
    const real = new TextDecoder().decode(located.stdout).trim();
    const bin = join(directory, "bin");
    await Deno.mkdir(bin);
    await Deno.writeTextFile(
      join(bin, "systemctl"),
      `#!/bin/sh\nif [ "$2" = stop ]; then echo 'fixture stop refused' >&2; exit 9; fi\nexec '${real}' "$@"\n`,
    );
    await Deno.chmod(join(bin, "systemctl"), 0o700);
    Deno.env.set("PATH", `${bin}:${priorPath}`);
    const result = await runCommand({
      id,
      cwd: directory,
      command: "sh",
      args: ["-c", "printf 'raw output\\n'; tail -f /dev/null & exit 0"],
      stdoutPath: join(directory, "stdout"),
      stderrPath: join(directory, "stderr"),
    });
    deepStrictEqual(result.cleanup, "failed");
    deepStrictEqual(result.success, false);
    deepStrictEqual(result.error?.includes("Scope cleanup failed"), true);
    deepStrictEqual(await Deno.readTextFile(result.stdoutPath), "raw output\n");
  } finally {
    Deno.env.set("PATH", priorPath);
    await stopProcessScope(id);
    await Deno.remove(directory, { recursive: true });
  }
});
