import { deepStrictEqual, match, rejects } from "node:assert/strict";
import { join } from "node:path";
import { Store } from "../fixtures/store.ts";
import { runSavedStep } from "../../extensions/models/_lib/driver.ts";
import { runTestEffect } from "../../extensions/models/_lib/test-runner.ts";
import { newRun } from "../../extensions/models/_lib/state.ts";
import {
  createWorkspace,
  verifyWorkspaceRevision,
} from "../../extensions/models/_lib/workspace.ts";

Deno.test("test actions archive output and route failure to repair, but reject changed source or lost archives", async () => {
  const priorPath = Deno.env.get("PATH") ?? "";
  for (const mode of ["pass", "fail", "mutate", "archive"]) {
    const root = await Deno.makeTempDir({ prefix: "tempo-test-action-" });
    try {
      const repository = join(root, "repo");
      await Deno.mkdir(repository);
      const git = async (...args: string[]) => {
        const result = await new Deno.Command("git", {
          cwd: repository,
          args: [
            "-c",
            "user.name=Tempo Fixture",
            "-c",
            "user.email=tempo@localhost",
            "-c",
            "commit.gpgsign=false",
            ...args,
          ],
        }).output();
        if (!result.success) {
          throw new Error(new TextDecoder().decode(result.stderr));
        }
        return new TextDecoder().decode(result.stdout).trim();
      };
      await git("init", "--initial-branch=main");
      await Deno.mkdir(join(repository, ".worktrees"));
      await Deno.writeTextFile(join(repository, ".gitignore"), ".worktrees/\n");
      await Deno.writeTextFile(join(repository, "source.txt"), "initial\n");
      await git("add", ".");
      await git("commit", "-m", "fixture: create test source");
      const revision = await git("rev-parse", "HEAD");
      const workspace = await createWorkspace(repository, "run", revision);
      const store = new Store();
      store.failArchive = mode === "archive";
      await store.writeResource("state", "run-state", {
        ...newRun("run", revision),
        phase: "test",
      });
      const bin = join(root, "bin");
      await Deno.mkdir(bin);
      const script =
        '#!/bin/sh\nprintf "task=%s\\n" "$1"\nprintf "fixture stderr\\n" >&2\n' +
        (mode === "mutate" ? 'printf "changed\\n" > source.txt\n' : "") +
        `exit ${mode === "fail" ? 7 : 0}\n`;
      await Deno.writeTextFile(join(bin, "bb"), script);
      await Deno.chmod(join(bin, "bb"), 0o700);
      Deno.env.set("PATH", `${bin}:${priorPath}`);
      const directory = join(root, "logs");
      const limits = {
        maxAttempts: 1,
        maxRepairsPerAttempt: 1,
        maxPiCalls: 3,
        maxDurationMs: 1000,
      };
      const result = await runSavedStep(
        store,
        "run",
        limits,
        0,
        (active) => runTestEffect(store, active, workspace, directory),
      );
      const expectedPhase = {
        pass: "measure",
        fail: "repair",
        mutate: "finish",
        archive: "finish",
      }[mode];
      deepStrictEqual(result.state.phase, expectedPhase);
      deepStrictEqual(result.state.bestRevision, revision);
      deepStrictEqual(result.state.targetMet, false);
      deepStrictEqual(
        await Deno.readTextFile(join(repository, "source.txt")),
        "initial\n",
      );
      deepStrictEqual(
        await Deno.readTextFile(join(directory, "stdout.log")),
        "task=qa\n",
      );
      if (mode === "archive") {
        match(result.state.stopReason ?? "", /Archive unavailable/);
        deepStrictEqual(await store.readResource("run-test-1-result"), null);
      } else {
        const record = await store.readResource("run-test-1-result", 1);
        deepStrictEqual(record?.passed, mode === "pass");
        deepStrictEqual(record?.valid, mode !== "mutate");
        deepStrictEqual(record?.exitCode, mode === "fail" ? 7 : 0);
        deepStrictEqual(
          new TextDecoder().decode(store.files.get("run-test-1-trace")),
          "task=qa\n",
        );
        deepStrictEqual(
          new TextDecoder().decode(store.files.get("run-test-1-stderr")),
          "fixture stderr\n",
        );
        deepStrictEqual(
          result.record.evidence.some((ref) =>
            ref.name === "run-test-1-result"
          ),
          true,
        );
      }
      if (mode === "mutate") {
        match(result.state.stopReason ?? "", /unsaved source/);
        await rejects(
          () => verifyWorkspaceRevision(workspace, revision),
          /unsaved source/,
        );
        const active = {
          ...newRun("run", revision),
          phase: "test" as const,
          sequence: 1,
          activeStep: 1,
        };
        await rejects(
          () =>
            runTestEffect(
              store,
              active,
              workspace,
              join(root, "must-not-start"),
            ),
          /unsaved source/,
        );
        await rejects(
          () => Deno.stat(join(root, "must-not-start")),
          Deno.errors.NotFound,
        );
      } else {
        await verifyWorkspaceRevision(workspace, revision);
        await rejects(
          () => verifyWorkspaceRevision(workspace, "0".repeat(40)),
          /revision does not match/,
        );
      }
    } finally {
      Deno.env.set("PATH", priorPath);
      await Deno.remove(root, { recursive: true });
    }
  }
});
