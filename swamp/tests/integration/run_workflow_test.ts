import {
  deepStrictEqual,
  match,
  notStrictEqual,
  rejects,
} from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { z } from "npm:zod@4.4.3";
import {
  defaultPrecision,
  precisionSummary,
  scorePolicyVersion,
} from "../../extensions/models/_lib/benchmark.ts";

const support = fileURLToPath(new URL("../../", import.meta.url));
async function copyTree(from: string, to: string) {
  await Deno.mkdir(to, { recursive: true });
  for await (const entry of Deno.readDir(from)) {
    if (entry.isDirectory) {
      await copyTree(join(from, entry.name), join(to, entry.name));
    } else if (entry.isFile) {
      await Deno.copyFile(join(from, entry.name), join(to, entry.name));
    }
  }
}

Deno.test("Swamp exercises complete orchestration with fixed tools: repairs, rejection, expiry, malformed output, lost archives, cleanup recovery, and competitors", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-workflow-fixture-" });
  let passed = false;
  try {
    const repository = join(root, "repo");
    const swampRoot = join(root, "swamp");
    const bin = join(root, "bin");
    await Deno.mkdir(repository);
    await Deno.mkdir(swampRoot);
    await Deno.mkdir(bin);
    for (const name of ["bb", "pi", "clojure", "tmux", "brepl"]) {
      await Deno.copyFile(
        join(support, "tests/fixtures/tempo_tools.py"),
        join(bin, name),
      );
      await Deno.chmod(join(bin, name), 0o700);
    }
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
    await Deno.mkdir(join(repository, "src/main/clojure/ol"), {
      recursive: true,
    });
    await Deno.writeTextFile(
      join(repository, "src/main/clojure/ol/busker.clj"),
      "; initial\n",
    );
    await Deno.writeTextFile(
      join(repository, ".gitignore"),
      ".worktrees/\n*.so\n.nrepl-port\n",
    );
    await git("add", ".");
    await git("commit", "-m", "fixture: initial source");
    const revision = await git("rev-parse", "HEAD");
    await Deno.writeTextFile(
      join(repository, "AGENTS.md"),
      "Local fixture guidance; untracked and read-only.\n",
    );
    const env = {
      PATH: `${bin}:${Deno.env.get("PATH")}`,
      TEMPO_FIXTURE_BIN: bin,
    };
    const swamp = async (args: string[], caseName = "repair") => {
      const result = await new Deno.Command("swamp", {
        cwd: swampRoot,
        args: [...args, "--json"],
        env: { ...env, TEMPO_FIXTURE_CASE: caseName },
      }).output();
      if (!result.success) {
        const report = await new Deno.Command("swamp", {
          cwd: swampRoot,
          args: [
            "report",
            "get",
            "@swamp/workflow-summary",
            "--workflow",
            "verify",
            "--json",
          ],
        }).output();
        throw new Error(
          `${new TextDecoder().decode(result.stderr)}\n${
            new TextDecoder().decode(result.stdout)
          }\n${new TextDecoder().decode(report.stdout)}`,
        );
      }
      return z.record(z.string(), z.unknown()).parse(
        JSON.parse(new TextDecoder().decode(result.stdout)),
      );
    };
    await swamp(["init", "--tool", "none"]);
    await copyTree(join(support, "extensions"), join(swampRoot, "extensions"));
    await copyTree(join(support, "pi"), join(swampRoot, "pi"));
    for (const name of ["prompts", "clojure"]) {
      await copyTree(join(support, name), join(swampRoot, name));
    }
    await Deno.mkdir(join(swampRoot, "tests/fixtures"), { recursive: true });
    await Deno.copyFile(
      join(support, "tests/fixtures/run-model.ts"),
      join(swampRoot, "tests/fixtures/run-model.ts"),
    );
    await Deno.writeTextFile(
      join(swampRoot, "extensions/models/fixture.ts"),
      'import { model as fixture } from "../../tests/fixtures/run-model.ts";\nexport const model = fixture;\n',
    );
    await swamp(["model", "create", "busker/tempo-fixture", "fixture"]);
    const created = await swamp(["workflow", "create", "verify"]);
    await Deno.writeTextFile(
      String(created.path),
      `id: ${created.id}\nname: verify\nversion: 1\ninputs:\n  type: object\n  properties:\n    runId: {type: string}\n    caseName: {type: string}\n  required: [runId, caseName]\njobs:\n  - name: fixture\n    steps:\n      - name: run\n        task:\n          type: model_method\n          modelIdOrName: fixture\n          methodName: run\n          inputs:\n            runId: \${{ inputs.runId }}\n            caseName: \${{ inputs.caseName }}\n            repository: ${
        JSON.stringify(repository)
      }\n            revision: ${revision}\n            supportDirectory: ${
        JSON.stringify(swampRoot)
      }\n            directory: \${{ '${root}/' + inputs.runId }}\n      - name: check\n        dependsOn:\n          - step: run\n            condition: {type: succeeded}\n        task:\n          type: assert\n          expr: data.latest('fixture', inputs.runId + '-report').attributes.diagnosticOnly && !data.latest('fixture', inputs.runId + '-report').attributes.targetMet && data.latest('fixture', inputs.runId + '-report').attributes.state.phase == 'done'\n          message: Fixture results must finish without claiming a performance goal.\n          severity: high\n`,
    );
    await swamp(["workflow", "validate", "verify"]);
    const reviewedSupport = join(repository, "swamp");
    await Deno.mkdir(reviewedSupport);
    await Deno.copyFile(
      join(swampRoot, ".swamp.yaml"),
      join(reviewedSupport, ".swamp.yaml"),
    );
    for (
      const name of [
        "extensions",
        "models",
        "workflows",
        "prompts",
        "pi",
        "clojure",
      ]
    ) {
      await copyTree(join(swampRoot, name), join(reviewedSupport, name));
    }
    await git("add", "swamp");
    await git("add", "-f", "swamp/prompts");
    await git("commit", "-m", "fixture reviewed support tree");
    const supportRevision = await git("rev-parse", "HEAD");
    for (
      const caseName of [
        "repair",
        "reject",
        "malformed",
        "expiry",
        "archive",
        "cleanup",
        "competitors",
        "candidate-noise",
        "candidate-failure",
        "noise",
        "noise-h2",
        "noise-stabilizes",
        "recovery",
      ]
    ) {
      const runId = `fixture-${caseName}`;
      await swamp([
        "workflow",
        "run",
        "verify",
        "--input",
        `runId=${runId}`,
        "--input",
        `caseName=${caseName}`,
      ], caseName);
      const data = await swamp(
        ["data", "get", "fixture", `${runId}-report`],
        caseName,
      );
      const report = z.object({
        diagnosticOnly: z.literal(true),
        fixture: z.literal(true),
        targetMet: z.literal(false),
        verificationTargetMet: z.boolean(),
        bestRevision: z.string(),
        bestVerified: z.boolean(),
        unfinishedRevision: z.string().nullable(),
        stopReason: z.string(),
        state: z.object({
          phase: z.literal("done"),
          repairsThisAttempt: z.number(),
          piCallsStarted: z.number(),
        }),
      }).parse(data.content);
      deepStrictEqual(
        report.bestVerified,
        !["noise", "noise-h2"].includes(caseName),
      );
      if (caseName === "repair") {
        const benchmark = await swamp(
          [
            "data",
            "get",
            "fixture",
            `${runId}-busker-h1-result`,
            "--version",
            "1",
          ],
          caseName,
        );
        const result = z.object({
          valid: z.literal(true),
          plannedSamples: z.literal(6),
          batches: z.array(z.object({ samples: z.array(z.number()).length(3) }))
            .length(2),
          summary: z.object({
            score: z.number(),
            samples: z.array(z.number()).length(6),
          }),
        }).parse(benchmark.content);
        deepStrictEqual(
          result.summary.score,
          precisionSummary(
            result.batches.flatMap((batch) => batch.samples),
            defaultPrecision,
          ).mean,
        );
      }
      if (["repair", "competitors", "recovery"].includes(caseName)) {
        if (caseName === "recovery") {
          deepStrictEqual(report.state.piCallsStarted, 3);
        }
        deepStrictEqual(report.verificationTargetMet, true);
        notStrictEqual(report.bestRevision, revision);
        deepStrictEqual(report.unfinishedRevision, null);
        match(
          await git(
            "show",
            `${report.bestRevision}:src/main/clojure/ol/busker.clj`,
          ),
          /good/,
        );
        if (caseName === "repair") {
          deepStrictEqual(report.state.repairsThisAttempt, 2);
        }
      } else {
        deepStrictEqual([report.verificationTargetMet, report.bestRevision], [
          false,
          revision,
        ]);
        if (
          !["reject", "candidate-noise"].includes(caseName) &&
          !caseName.startsWith("noise")
        ) {
          notStrictEqual(report.unfinishedRevision, null);
          notStrictEqual(report.unfinishedRevision, revision);
        }
        if (caseName === "expiry") match(report.stopReason, /duration/);
        if (caseName === "cleanup") match(report.stopReason, /cleanup/);
        if (["noise", "noise-h2"].includes(caseName)) {
          deepStrictEqual(report.stopReason, "measurement-inconclusive");
          deepStrictEqual(report.state.piCallsStarted, 0);
          const protocol = caseName === "noise-h2" ? "tls-h2" : "h1";
          const benchmark = await swamp(
            [
              "data",
              "get",
              "fixture",
              `${runId}-busker-${protocol}-result`,
              "--version",
              "1",
            ],
            caseName,
          );
          const content = z.object({
            valid: z.literal(false),
            scorePolicyVersion: z.literal(scorePolicyVersion),
            precision: z.object({
              relativeHalfWidth: z.literal(defaultPrecision.relativeHalfWidth),
              confidenceLevel: z.literal(defaultPrecision.confidenceLevel),
            }),
            plannedSamples: z.literal(6),
            inconclusive: z.literal(true),
            batches: z.array(z.object({ samples: z.array(z.number()) })).length(
              2,
            ),
            summary: z.object({
              score: z.null(),
              samples: z.array(z.number()).length(6),
            }),
            files: z.array(z.object({ path: z.string() })),
            logs: z.array(z.object({ name: z.string() })),
          }).parse(benchmark.content);
          deepStrictEqual(
            content.files.filter(({ path }) => path.endsWith("results.json"))
              .length,
            2,
          );
          deepStrictEqual(content.logs.length >= 4, true);
        }
        if (["candidate-noise", "candidate-failure"].includes(caseName)) {
          const benchmark = await swamp(
            ["data", "get", "fixture", `${runId}-busker-tls-h2-result`],
            caseName,
          );
          const result = z.object({
            valid: z.literal(false),
            inconclusive: z.literal(true),
            error: z.string().nullable(),
            batches: z.array(z.object({ samples: z.array(z.number()) })),
          }).parse(benchmark.content);
          const measured = await swamp(
            ["data", "get", "fixture", `${runId}-step-8`],
            caseName,
          );
          const outcome = z.object({
            outcome: z.object({
              kind: z.literal("measurement"),
              valid: z.literal(false),
              inconclusive: z.boolean(),
            }),
          }).parse(measured.content).outcome;
          const noisy = caseName === "candidate-noise";
          deepStrictEqual(outcome.inconclusive, noisy);
          deepStrictEqual(result.error === null, noisy);
          deepStrictEqual(result.batches.length, noisy ? 2 : 0);
          deepStrictEqual(
            report.stopReason,
            noisy ? "limit:attempts" : "invalid-measurement",
          );
          if (noisy) {
            const discarded = await swamp(
              ["data", "get", "fixture", `${runId}-step-9`],
              caseName,
            );
            z.object({ phase: z.literal("discard") }).parse(discarded.content);
          }
        }
        if (caseName === "noise-stabilizes") {
          const benchmark = await swamp(
            [
              "data",
              "get",
              "fixture",
              `${runId}-busker-h1-result`,
              "--version",
              "1",
            ],
            caseName,
          );
          const result = z.object({
            valid: z.literal(true),
            inconclusive: z.literal(false),
            batches: z.array(z.object({ samples: z.array(z.number()) })).length(
              2,
            ),
            summary: z.object({
              score: z.number(),
              samples: z.array(z.number()).length(6),
            }),
          }).parse(benchmark.content);
          deepStrictEqual(result.batches[0].samples, [100, 100, 105.01]);
          const expected = precisionSummary(
            [100, 100, 105.01, 102, 102, 102],
            defaultPrecision,
          );
          deepStrictEqual(result.summary.score, expected.mean);
          deepStrictEqual(result.batches[1].samples, [102, 102, 102]);
        }
      }
      deepStrictEqual(await git("rev-parse", "HEAD"), supportRevision);
      deepStrictEqual(await git("status", "--porcelain"), "?? AGENTS.md");
      const head = await new Deno.Command("git", {
        cwd: join(repository, ".worktrees", `tempo-${runId}`),
        args: ["rev-parse", "HEAD"],
      }).output();
      deepStrictEqual(
        new TextDecoder().decode(head.stdout).trim(),
        report.bestRevision,
      );
    }
    const hold = "fixture-hold";
    const controller = `tempo-controller-${hold}.scope`;
    const child = `tempo-${hold}-test-1.scope`;
    const sentinel = "tempo-unrelated-sentinel-04602.scope";
    const ready = join(root, "hold-ready");
    const nextPhase = join(root, "next-phase");
    const show = async (unit: string, property: string) => {
      const output = await new Deno.Command("systemctl", {
        args: ["--user", "show", unit, `--property=${property}`, "--value"],
      }).output();
      return new TextDecoder().decode(output.stdout).trim();
    };
    const stop = async (unit: string) => {
      const output = await new Deno.Command("systemctl", {
        args: ["--user", "stop", unit],
      }).output();
      if (!output.success && await show(unit, "LoadState") !== "not-found") {
        throw new Error(new TextDecoder().decode(output.stderr));
      }
    };
    for (const unit of [controller, child, sentinel]) {
      deepStrictEqual(await show(unit, "LoadState"), "not-found");
    }
    try {
      new Deno.Command("systemd-run", {
        cwd: swampRoot,
        env,
        args: [
          "--user",
          "--scope",
          "--quiet",
          "--collect",
          "--expand-environment=no",
          `--unit=${sentinel}`,
          "--description=Unrelated Tempo fixture sentinel",
          "--",
          "tail",
          "-f",
          "/dev/null",
        ],
        stdin: "null",
        stdout: "null",
        stderr: "null",
      }).spawn().unref();
      new Deno.Command("systemd-run", {
        cwd: swampRoot,
        env: {
          ...env,
          TEMPO_FIXTURE_CASE: "hold",
          TEMPO_FIXTURE_HOLD_READY: ready,
          TEMPO_FIXTURE_NEXT_PHASE: nextPhase,
        },
        args: [
          "--user",
          "--scope",
          "--quiet",
          "--collect",
          "--expand-environment=no",
          `--unit=${controller}`,
          `--description=Tempo controller ${hold}`,
          "--property=TimeoutStopSec=2s",
          "--",
          "swamp",
          "workflow",
          "run",
          "verify",
          "--input",
          `runId=${hold}`,
          "--input",
          "caseName=hold",
        ],
        stdin: "null",
        stdout: "null",
        stderr: "null",
      }).spawn().unref();
      const deadline = Date.now() + 20000;
      while (Date.now() < deadline) {
        try {
          await Deno.stat(ready);
          break;
        } catch (error) {
          if (!(error instanceof Deno.errors.NotFound)) throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      const workloadPid = Number(await Deno.readTextFile(ready));
      deepStrictEqual(await show(controller, "ActiveState"), "active");
      deepStrictEqual(await show(child, "ActiveState"), "active");
      deepStrictEqual(await show(sentinel, "ActiveState"), "active");
      deepStrictEqual(
        await show(child, "Description"),
        `Tempo process ${hold}-test-1`,
      );
      const controllerCgroup = await show(controller, "ControlGroup");
      const controllerPids = (await Deno.readTextFile(
        `/sys/fs/cgroup${controllerCgroup}/cgroup.procs`,
      )).trim().split("\n").filter(Boolean);
      deepStrictEqual(controllerPids.length > 0, true);
      const starts = await Promise.all(controllerPids.map(async (pid) => {
        const stat = await Deno.readTextFile(`/proc/${pid}/stat`);
        return {
          pid,
          start: stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19],
        };
      }));
      const childCgroup = await show(child, "ControlGroup");
      notStrictEqual(childCgroup, await show(controller, "ControlGroup"));
      const pidCgroup = await Deno.readTextFile(`/proc/${workloadPid}/cgroup`);
      match(pidCgroup, new RegExp(child.replaceAll(".", "\\.")));
      const stoppedAt = Date.now();
      const stopRecord = join(root, "stop-actions.log");
      await Deno.writeTextFile(
        stopRecord,
        JSON.stringify({ controllerCgroup, starts }) + "\n",
      );
      for (
        const args of [["freeze", controller], [
          "kill",
          "--signal=SIGKILL",
          "--kill-whom=all",
          controller,
        ]]
      ) {
        await Deno.writeTextFile(stopRecord, `${args[0]} requested\n`, {
          append: true,
        });
        const response = await new Deno.Command("systemctl", {
          args: ["--user", ...args],
        }).output();
        await Deno.writeTextFile(
          stopRecord,
          `${args[0]} result ${response.code}\n`,
          { append: true },
        );
        if (!response.success) {
          throw new Error(new TextDecoder().decode(response.stderr));
        }
        if (args[0] === "freeze") {
          deepStrictEqual(await show(controller, "FreezerState"), "frozen");
        }
      }
      deepStrictEqual(Date.now() - stoppedAt < 8000, true);
      const stillRunning = async () => {
        for (const { pid, start } of starts) {
          try {
            const stat = await Deno.readTextFile(`/proc/${pid}/stat`);
            const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
            if (fields[19] === start && fields[0] !== "Z") return true;
          } catch (error) {
            if (!(error instanceof Deno.errors.NotFound)) throw error;
          }
        }
        return false;
      };
      while (await stillRunning() && Date.now() - stoppedAt < 8000) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      deepStrictEqual(await stillRunning(), false);
      deepStrictEqual(await show(child, "ActiveState"), "active");
      deepStrictEqual(await show(sentinel, "ActiveState"), "active");
      await rejects(() => Deno.stat(nextPhase), Deno.errors.NotFound);
      await stop(child);
      deepStrictEqual(await show(child, "LoadState"), "not-found");
      deepStrictEqual(await show(sentinel, "ActiveState"), "active");
      await rejects(() => Deno.stat(nextPhase), Deno.errors.NotFound);
      const state = await swamp(
        ["data", "get", "fixture", `${hold}-state`],
        "hold",
      );
      deepStrictEqual(
        z.object({ activeStep: z.literal(1), phase: z.literal("prepare") })
          .parse(state.content).phase,
        "prepare",
      );
      match(
        await Deno.readTextFile(join(root, hold, "step-1/stdout.log")),
        /Fixture paused after reserving/,
      );
      match(
        await Deno.readTextFile(join(root, hold, "step-1/stderr.log")),
        /Fixture stderr survives controller termination/,
      );
      const missingStep = await new Deno.Command("swamp", {
        cwd: swampRoot,
        args: ["data", "get", "fixture", `${hold}-step-1`, "--json"],
      }).output();
      deepStrictEqual(missingStep.success, false);
      deepStrictEqual(await git("rev-parse", "HEAD"), supportRevision);
    } finally {
      if (await show(controller, "LoadState") === "loaded") {
        await new Deno.Command("systemctl", {
          args: [
            "--user",
            "kill",
            "--signal=SIGKILL",
            "--kill-whom=all",
            controller,
          ],
        }).output();
      }
      for (const unit of [child, sentinel]) {
        if (await show(unit, "LoadState") === "loaded") await stop(unit);
      }
    }
    passed = true;
  } finally {
    if (passed) await Deno.remove(root, { recursive: true });
    else console.error(`Preserved failed fixture workspace: ${root}`);
  }
});
