import { deepStrictEqual, match, rejects } from "node:assert/strict";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "npm:zod@4.4.3";
import {
  assertNotAbandoned,
  exportParentEvidence,
  getSwampRecord,
  verifyParentEvidence,
} from "../../extensions/models/_lib/campaign.ts";
import {
  runArgumentsSchema,
  runConfigSchema,
  startRun,
} from "../../extensions/models/_lib/run.ts";
import { supportManifest } from "../../extensions/models/_lib/support.ts";
import { benchmarkMethod } from "../../extensions/models/_lib/placement.ts";
import { stepResultSchema } from "../../extensions/models/_lib/schemas.ts";
import { Store } from "../fixtures/store.ts";

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

Deno.test("fixed Swamp runs export exact parent evidence and inherit the original baseline, best and budget", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-campaign-fixture-" });
  const originalCwd = Deno.cwd();
  let passed = false;
  try {
    const repository = join(root, "repo");
    const first = join(root, "first");
    const second = join(root, "second");
    const bin = join(root, "bin");
    await Deno.mkdir(repository);
    await Deno.mkdir(first);
    await Deno.mkdir(second);
    await Deno.mkdir(bin);
    for (const name of ["bb", "pi", "clojure", "tmux", "brepl"]) {
      const fixture = await Deno.readTextFile(
        join(support, "tests/fixtures/tempo_tools.py"),
      );
      match(fixture, /this program never invokes Pi or a provider/);
      await Deno.writeTextFile(join(bin, name), fixture, { mode: 0o700 });
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
    await git("config", "user.name", "Tempo Fixture");
    await git("config", "user.email", "tempo-fixture@example.test");
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
    await git("commit", "-m", "fixture initial source");
    let revision = await git("rev-parse", "HEAD");
    await Deno.writeTextFile(
      join(repository, "AGENTS.md"),
      "Local fixture guidance; untracked and read-only.\n",
    );
    const environment = {
      PATH: `${bin}:${Deno.env.get("PATH")}`,
      TEMPO_FIXTURE_BIN: bin,
    };
    const swamp = async (cwd: string, args: string[], caseName: string) => {
      const result = await new Deno.Command("swamp", {
        cwd,
        args: [...args, "--json"],
        env: { ...environment, TEMPO_FIXTURE_CASE: caseName },
      }).output();
      if (!result.success) {
        throw new Error(
          `${new TextDecoder().decode(result.stdout)}\n${
            new TextDecoder().decode(result.stderr)
          }`,
        );
      }
      return z.record(z.string(), z.unknown()).parse(
        JSON.parse(new TextDecoder().decode(result.stdout)),
      );
    };
    for (const directory of [first, second]) {
      await swamp(directory, ["init", "--tool", "none"], "analysis-stop");
      for (
        const name of ["extensions", "pi", "prompts", "clojure", "workflows"]
      ) {
        await copyTree(join(support, name), join(directory, name));
      }
      if (directory === first) {
        await swamp(
          directory,
          ["model", "create", "busker/tempo", "tempo"],
          "analysis-stop",
        );
      } else {
        await Deno.copyFile(
          join(first, ".swamp.yaml"),
          join(directory, ".swamp.yaml"),
        );
        await copyTree(join(first, "models"), join(directory, "models"));
      }
      await swamp(
        directory,
        ["workflow", "validate", "tempo"],
        "analysis-stop",
      );
    }
    const reviewedSupport = join(repository, "swamp");
    await Deno.mkdir(reviewedSupport);
    await Deno.copyFile(
      join(first, ".swamp.yaml"),
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
      await copyTree(join(first, name), join(reviewedSupport, name));
    }
    await git("add", "swamp");
    await git("add", "-f", "swamp/prompts");
    await git("commit", "-m", "fixture reviewed support tree");
    revision = await git("rev-parse", "HEAD");
    const policy = {
      goal: { kind: "improve-baseline", multiplier: 1.5, workloads: ["h1"] },
      protections: [{ workload: "tls-h2", maxDecreaseFraction: 0.05 }],
      minimumImprovementFraction: 0.05,
    };
    const limits = {
      maxAttempts: 10,
      maxRepairsPerAttempt: 2,
      maxPiCalls: 40,
      maxDurationMs: 4 * 60 * 60 * 1000,
    };
    const parameters = {
      warmup: 10,
      duration: 30,
      repetitions: 3,
      connections: 128,
      streams: 64,
      threads: 2,
    };
    const inputs = async (
      directory: string,
      runId: string,
      selectedRevision: string,
      parentEvidence?: { path: string; sha256: string },
    ) => ({
      confirmLive: true,
      runId,
      repository,
      revision: selectedRevision,
      directory: join(root, `output-${runId}`),
      supportDirectory: directory,
      supportSha256: (await supportManifest(directory)).sha256,
      supportRevision: revision,
      ...(parentEvidence ? { parentEvidence } : {}),
      credentialDirectory: join(root, "credentials"),
      referenceRoots: [],
      benchmarkMethod,
      policy,
      limits,
      parameters,
    });
    const parentId = "fixture-campaign-parent";
    const briefText =
      "Prioritize completion scans; exclude worker sizing. Change one hypothesis per attempt.";
    const firstInputs = {
      ...await inputs(first, parentId, revision),
      experimentBrief: briefText,
    };
    const firstFile = join(root, "first.json");
    await Deno.writeTextFile(firstFile, JSON.stringify(firstInputs));
    await swamp(
      first,
      ["workflow", "run", "tempo", "--input-file", firstFile],
      "repair",
    );
    const brief = await swamp(first, [
      "data",
      "get",
      "tempo",
      `${parentId}-experiment-brief`,
    ], "repair");
    deepStrictEqual(
      z.object({
        content: z.object({
          runId: z.string(),
          source: z.string(),
          sha256: z.string(),
          text: z.string(),
        }),
      }).parse(brief).content,
      {
        runId: parentId,
        source: "prepared experiment brief",
        sha256: createHash("sha256").update(briefText).digest("hex"),
        text: briefText,
      },
    );
    const analysis = await swamp(first, [
      "data",
      "get",
      "tempo",
      `${parentId}-pi-5-result`,
    ], "repair");
    const accepted = z.object({
      content: z.object({
        status: z.literal("valid"),
        inputs: z.array(z.object({ name: z.string(), version: z.number() })),
        result: z.object({
          evidence: z.array(
            z.object({ name: z.string(), version: z.number() }),
          ),
        }),
      }),
    }).parse(analysis).content;
    const briefReference = { name: `${parentId}-experiment-brief`, version: 1 };
    deepStrictEqual({
      saved: accepted.inputs.filter((item) =>
        item.name === briefReference.name
      ),
      cited: accepted.result.evidence.filter((item) =>
        item.name === briefReference.name
      ),
    }, { saved: [briefReference], cited: [briefReference] });
    const stateVersions = await swamp(first, [
      "data",
      "versions",
      "tempo",
      `${parentId}-state`,
    ], "repair");
    const latest = z.object({
      versions: z.array(
        z.object({ version: z.number(), isLatest: z.boolean() }),
      ),
    })
      .parse(stateVersions).versions.find((item) => item.isLatest)?.version;
    if (!latest) throw new Error("Missing latest fixture state");
    await rejects(
      () => getSwampRecord(first, `${parentId}-state`, latest + 99),
      /Missing Swamp data/,
    );
    const bundle = join(root, "parent-evidence.json");
    const digest = await exportParentEvidence(first, parentId, latest, bundle);
    const parent = await verifyParentEvidence(
      bundle,
      digest,
      (value) => runConfigSchema.parse(value),
    );
    deepStrictEqual([
      parent.initial?.scores.find((cell) => cell.workload === "h1")
        ?.requestsPerSecond,
      parent.best?.scores.find((cell) => cell.workload === "h1")
        ?.requestsPerSecond,
    ], [100, 160]);
    deepStrictEqual([parent.attemptsStarted, parent.piCallsStarted > 0], [
      1,
      true,
    ]);
    const multiId = "fixture-campaign-two-keeps";
    const multiFile = join(root, "two-keeps.json");
    await Deno.writeTextFile(
      multiFile,
      JSON.stringify({
        ...await inputs(first, multiId, revision),
        policy: { ...policy, goal: { ...policy.goal, multiplier: 2 } },
        limits: { ...limits, maxAttempts: 2 },
      }),
    );
    await swamp(
      first,
      ["workflow", "run", "tempo", "--input-file", multiFile],
      "two-keeps",
    );
    const noBriefAnalysis = await swamp(first, [
      "data",
      "get",
      "tempo",
      `${multiId}-pi-5-result`,
    ], "two-keeps");
    const noBriefRecord = z.object({
      content: z.object({
        status: z.literal("valid"),
        inputs: z.array(z.object({ name: z.string() })),
      }),
    }).parse(noBriefAnalysis).content;
    deepStrictEqual(
      noBriefRecord.inputs.some((item) =>
        item.name === `${multiId}-experiment-brief`
      ),
      false,
    );
    const multiVersions = await swamp(first, [
      "data",
      "versions",
      "tempo",
      `${multiId}-state`,
    ], "two-keeps");
    const multiVersion = z.object({
      versions: z.array(z.object({
        version: z.number(),
        isLatest: z.boolean(),
      })),
    }).parse(multiVersions).versions.find((item) => item.isLatest)?.version;
    if (!multiVersion) throw new Error("Missing two-keeps state");
    const multiBundle = join(root, "two-keeps-evidence.json");
    const multiDigest = await exportParentEvidence(
      first,
      multiId,
      multiVersion,
      multiBundle,
    );
    const multiProof = await verifyParentEvidence(
      multiBundle,
      multiDigest,
      (value) => runConfigSchema.parse(value),
    );
    const multiRecords = z.object({
      records: z.array(z.object({
        content: z.unknown(),
      })),
    }).parse(JSON.parse(await Deno.readTextFile(multiBundle))).records;
    deepStrictEqual(
      multiRecords.filter((item) =>
        stepResultSchema.safeParse(item.content).data?.outcome.kind === "kept"
      ).length,
      2,
    );
    deepStrictEqual(
      multiProof.best?.scores.find((cell) => cell.workload === "h1")
        ?.requestsPerSecond,
      180,
    );
    const childId = "fixture-campaign-child";
    const childInputs = {
      ...await inputs(second, childId, parent.bestRevision, {
        path: bundle,
        sha256: digest,
      }),
      experimentBrief: briefText,
    };
    const childFile = join(root, "second.json");
    await Deno.writeTextFile(childFile, JSON.stringify(childInputs));
    Deno.chdir(second);
    const seeded = runArgumentsSchema.parse({
      ...childInputs,
      runId: "fixture-campaign-seed",
      directory: join(root, "seed"),
    });
    const started = await startRun(new Store(), seeded);
    deepStrictEqual(started.config.startedAtMs, parent.startedAtMs);
    Deno.chdir(originalCwd);
    await swamp(
      second,
      ["workflow", "run", "tempo-restart", "--input-file", childFile],
      "requal-higher",
    );
    const child = await swamp(second, [
      "data",
      "get",
      "tempo",
      `${childId}-report`,
    ], "analysis-stop");
    const report = z.object({
      content: z.object({
        state: z.object({
          initialRevision: z.string(),
          bestRevision: z.string(),
          attemptsStarted: z.number(),
          piCallsStarted: z.number(),
          phase: z.literal("done"),
        }),
        targetMet: z.boolean(),
      }),
    }).parse(child).content;
    deepStrictEqual([
      report.state.initialRevision,
      report.state.bestRevision,
      report.state.attemptsStarted,
      report.state.piCallsStarted,
      report.targetMet,
    ], [
      revision,
      parent.bestRevision,
      parent.attemptsStarted + 1,
      parent.piCallsStarted + 1,
      false,
    ]);
    const childVersions = await swamp(second, [
      "data",
      "versions",
      "tempo",
      `${childId}-state`,
    ], "requal-higher");
    const childVersion = z.object({
      versions: z.array(z.object({
        version: z.number(),
        isLatest: z.boolean(),
      })),
    }).parse(childVersions).versions.find((item) => item.isLatest)?.version;
    if (!childVersion) throw new Error("Missing requalified child state");
    const childBundle = join(root, "child-evidence.json");
    const childDigest = await exportParentEvidence(
      second,
      childId,
      childVersion,
      childBundle,
    );
    const childProof = await verifyParentEvidence(
      childBundle,
      childDigest,
      (value) => runConfigSchema.parse(value),
    );
    deepStrictEqual(
      childProof.best?.scores.find((cell) => cell.workload === "h1")
        ?.requestsPerSecond,
      167,
    );
    deepStrictEqual(
      childProof.initial?.scores.find((cell) => cell.workload === "h1")
        ?.requestsPerSecond,
      100,
    );
    const third = join(root, "third");
    await Deno.mkdir(third);
    await swamp(third, ["init", "--tool", "none"], "analysis-stop");
    for (
      const name of ["extensions", "pi", "prompts", "clojure", "workflows"]
    ) {
      await copyTree(join(support, name), join(third, name));
    }
    await Deno.copyFile(join(first, ".swamp.yaml"), join(third, ".swamp.yaml"));
    await copyTree(join(first, "models"), join(third, "models"));
    const grandchildId = "fixture-campaign-grandchild";
    const grandchildInputs = {
      ...await inputs(
        third,
        grandchildId,
        parent.bestRevision,
        { path: childBundle, sha256: childDigest },
      ),
      experimentBrief: briefText,
    };
    const grandchildFile = join(root, "grandchild.json");
    await Deno.writeTextFile(grandchildFile, JSON.stringify(grandchildInputs));
    await swamp(third, [
      "workflow",
      "run",
      "tempo-restart",
      "--input-file",
      grandchildFile,
    ], "analysis-stop");
    const grandchildVersions = await swamp(third, [
      "data",
      "versions",
      "tempo",
      `${grandchildId}-state`,
    ], "analysis-stop");
    const grandchildVersion = z.object({
      versions: z.array(z.object({
        version: z.number(),
        isLatest: z.boolean(),
      })),
    }).parse(grandchildVersions).versions.find((item) => item.isLatest)
      ?.version;
    if (!grandchildVersion) throw new Error("Missing grandchild state");
    const grandchildBundle = join(root, "grandchild-evidence.json");
    const grandchildDigest = await exportParentEvidence(
      third,
      grandchildId,
      grandchildVersion,
      grandchildBundle,
    );
    const grandchildProof = await verifyParentEvidence(
      grandchildBundle,
      grandchildDigest,
      (value) => runConfigSchema.parse(value),
    );
    deepStrictEqual(
      grandchildProof.best?.scores.find((cell) => cell.workload === "h1")
        ?.requestsPerSecond,
      167,
    );
    deepStrictEqual([
      grandchildProof.initial?.scores.find((cell) => cell.workload === "h1")
        ?.requestsPerSecond,
      grandchildProof.attemptsStarted,
      grandchildProof.piCallsStarted,
    ], [100, childProof.attemptsStarted + 1, childProof.piCallsStarted + 1]);
    const interruptedId = "fixture-campaign-abandoned";
    const interruptedSupport = join(root, "interrupted");
    await Deno.mkdir(interruptedSupport);
    await swamp(interruptedSupport, ["init", "--tool", "none"], "hold-edit");
    for (
      const name of ["extensions", "pi", "prompts", "clojure", "workflows"]
    ) {
      await copyTree(join(support, name), join(interruptedSupport, name));
    }
    await Deno.copyFile(
      join(first, ".swamp.yaml"),
      join(interruptedSupport, ".swamp.yaml"),
    );
    await copyTree(join(first, "models"), join(interruptedSupport, "models"));
    const interruptedInputs = await inputs(
      interruptedSupport,
      interruptedId,
      revision,
    );
    const interruptedFile = join(root, "interrupted.json");
    await Deno.writeTextFile(
      interruptedFile,
      JSON.stringify(interruptedInputs),
    );
    const ready = join(root, "edit-ready");
    const controller = `tempo-controller-${interruptedId}.scope`;
    const show = async (unit: string, property: string) => {
      const output = await new Deno.Command("systemctl", {
        args: ["--user", "show", unit, `--property=${property}`, "--value"],
      }).output();
      return new TextDecoder().decode(output.stdout).trim();
    };
    deepStrictEqual(await show(controller, "LoadState"), "not-found");
    new Deno.Command("systemd-run", {
      cwd: interruptedSupport,
      env: {
        ...environment,
        TEMPO_FIXTURE_CASE: "hold-edit",
        TEMPO_FIXTURE_HOLD_READY: ready,
      },
      args: [
        "--user",
        "--scope",
        "--quiet",
        "--collect",
        "--expand-environment=no",
        `--unit=${controller}`,
        `--description=Tempo controller ${interruptedId}`,
        "--property=TimeoutStopSec=2s",
        "--",
        "swamp",
        "workflow",
        "run",
        "tempo",
        "--input-file",
        interruptedFile,
        "--json",
      ],
      stdin: "null",
      stdout: "null",
      stderr: "null",
    }).spawn().unref();
    const deadline = Date.now() + 60000;
    while (Date.now() < deadline) {
      try {
        await Deno.stat(ready);
        break;
      } catch (error) {
        if (!(error instanceof Deno.errors.NotFound)) throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    const effectPid = Number(await Deno.readTextFile(ready));
    deepStrictEqual(await show(controller, "ActiveState"), "active");
    const stateVersions2 = await swamp(interruptedSupport, [
      "data",
      "versions",
      "tempo",
      `${interruptedId}-state`,
    ], "hold-edit");
    const version = z.object({
      versions: z.array(z.object({
        version: z.number(),
        isLatest: z.boolean(),
      })),
    }).parse(stateVersions2).versions.find((item) => item.isLatest)?.version;
    if (!version) throw new Error("Interrupted fixture state is missing");
    const interruptedState = await getSwampRecord(
      interruptedSupport,
      `${interruptedId}-state`,
      version,
    );
    const active = z.object({
      activeStep: z.number(),
      phase: z.literal("implement"),
      attemptsStarted: z.number(),
      piCallsStarted: z.number(),
    }).parse(interruptedState.content);
    const childUnit = `tempo-${interruptedId}-pi-${active.activeStep}.scope`;
    deepStrictEqual(await show(childUnit, "ActiveState"), "active");
    const replUnit = `tempo-${interruptedId}-repl.scope`;
    const replReadyUntil = Date.now() + 8000;
    while (
      await show(replUnit, "ActiveState") !== "active" &&
      Date.now() < replReadyUntil
    ) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    deepStrictEqual(await show(replUnit, "ActiveState"), "active");
    deepStrictEqual(
      await show(replUnit, "Description"),
      `Tempo process ${interruptedId}-repl`,
    );
    const replCgroup = await show(replUnit, "ControlGroup");
    deepStrictEqual(replCgroup !== await show(childUnit, "ControlGroup"), true);
    const childCgroup = await show(childUnit, "ControlGroup");
    match(
      await Deno.readTextFile(`/proc/${effectPid}/cgroup`),
      new RegExp(childUnit.replaceAll(".", "\\.")),
    );
    const controllerCgroup = await show(controller, "ControlGroup");
    deepStrictEqual(controllerCgroup !== childCgroup, true);
    const controllerPids = (await Deno.readTextFile(
      `/sys/fs/cgroup${controllerCgroup}/cgroup.procs`,
    )).trim().split("\n").filter(Boolean);
    const identities = await Promise.all(controllerPids.map(async (pid) => {
      const stat = await Deno.readTextFile(`/proc/${pid}/stat`);
      return {
        pid,
        start: stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19],
      };
    }));
    const stopRecord = join(root, "abandonment-stop.log");
    await Deno.writeTextFile(
      stopRecord,
      JSON.stringify({
        controllerCgroup,
        identities,
        childCgroup,
        replCgroup,
        effectPid,
      }) +
        "\n",
    );
    const systemctl = async (...args: string[]) => {
      const result = await new Deno.Command("systemctl", {
        args: ["--user", ...args],
      }).output();
      await Deno.writeTextFile(
        stopRecord,
        JSON.stringify({ args, code: result.code }) + "\n",
        { append: true },
      );
      if (!result.success) {
        throw new Error(new TextDecoder().decode(result.stderr));
      }
    };
    await systemctl("freeze", controller);
    deepStrictEqual(await show(controller, "FreezerState"), "frozen");
    await systemctl("kill", "--signal=SIGKILL", "--kill-whom=all", controller);
    const originalLive = async () => {
      for (const { pid, start } of identities) {
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
    const stoppedAt = Date.now();
    while (await originalLive() && Date.now() - stoppedAt < 8000) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    deepStrictEqual(await originalLive(), false);
    deepStrictEqual(await show(childUnit, "ActiveState"), "active");
    deepStrictEqual(await show(replUnit, "ActiveState"), "active");
    await systemctl("stop", childUnit);
    deepStrictEqual(await show(childUnit, "LoadState"), "not-found");
    deepStrictEqual(await show(replUnit, "ActiveState"), "active");
    await rejects(() =>
      exportParentEvidence(
        interruptedSupport,
        interruptedId,
        version,
        join(root, "must-not-export-with-repl.json"),
      ), /Parent controller or child scope is still active/);
    await systemctl("stop", replUnit);
    deepStrictEqual(await show(replUnit, "LoadState"), "not-found");
    const partial = join(
      interruptedInputs.directory,
      `step-${active.activeStep}`,
    );
    match(await Deno.readTextFile(join(partial, "stdout.jsonl")), /session/);
    match(
      await Deno.readTextFile(join(partial, "stderr.log")),
      /Fixed fixture Pi stderr/,
    );
    match(
      await Deno.readTextFile(
        join(
          repository,
          ".worktrees",
          `tempo-${interruptedId}`,
          "src/main/clojure/ol/busker.clj",
        ),
      ),
      /interrupted fixture edit/,
    );
    const receiptPath = join(interruptedInputs.directory, "abandoned.json");
    await Deno.writeTextFile(
      receiptPath,
      JSON.stringify({
        approvedBy: "tempo-leader@busker",
        runId: interruptedId,
        step: active.activeStep,
        stateChecksum: interruptedState.checksum,
        controllerStopped: true,
        childrenStopped: true,
        refsAndLogsPreserved: true,
      }),
      { createNew: true, mode: 0o400 },
    );
    const abandonedBundle = join(root, "abandoned-evidence.json");
    const abandonedDigest = await exportParentEvidence(
      interruptedSupport,
      interruptedId,
      version,
      abandonedBundle,
      receiptPath,
    );
    const abandoned = await verifyParentEvidence(
      abandonedBundle,
      abandonedDigest,
      (value) => runConfigSchema.parse(value),
    );
    deepStrictEqual([
      abandoned.bestRevision,
      abandoned.attemptsStarted,
      abandoned.piCallsStarted,
    ], [revision, active.attemptsStarted, active.piCallsStarted]);
    const resumedId = "fixture-campaign-from-abandonment";
    const resumedInputs = await inputs(second, resumedId, revision, {
      path: abandonedBundle,
      sha256: abandonedDigest,
    });
    const resumedFile = join(root, "resumed.json");
    await Deno.writeTextFile(resumedFile, JSON.stringify(resumedInputs));
    await swamp(second, [
      "workflow",
      "run",
      "tempo-restart",
      "--input-file",
      resumedFile,
    ], "analysis-stop");
    const resumed = await swamp(second, [
      "data",
      "get",
      "tempo",
      `${resumedId}-report`,
    ], "analysis-stop");
    const resumedState = z.object({
      content: z.object({
        state: z.object({
          attemptsStarted: z.number(),
          piCallsStarted: z.number(),
          initialRevision: z.string(),
          bestRevision: z.string(),
        }),
      }),
    }).parse(resumed).content.state;
    deepStrictEqual([
      resumedState.attemptsStarted,
      resumedState.piCallsStarted,
      resumedState.initialRevision,
      resumedState.bestRevision,
    ], [
      active.attemptsStarted + 1,
      active.piCallsStarted + 1,
      revision,
      revision,
    ]);
    await rejects(
      () => assertNotAbandoned(interruptedInputs.directory),
      /Abandoned run cannot be resumed or recovered/,
    );
    const archived = z.object({
      records: z.array(
        z.object({
          name: z.string(),
          checksum: z.string(),
          content: z.unknown(),
        }).passthrough(),
      ),
      logs: z.array(
        z.object({ path: z.string(), sha256: z.string() }).passthrough(),
      ),
      abandoned: z.unknown(),
    }).passthrough().parse(
      JSON.parse(await Deno.readTextFile(abandonedBundle)),
    );
    const refuseVariant = async (
      label: string,
      update: (copy: typeof archived) => void,
      expected: RegExp,
      source = archived,
    ) => {
      const changed = structuredClone(source);
      update(changed);
      const path = join(root, `${label}-evidence.json`);
      const bytes = JSON.stringify(changed);
      await Deno.writeTextFile(path, bytes);
      await rejects(
        () =>
          verifyParentEvidence(
            path,
            createHash("sha256").update(bytes).digest("hex"),
            (value) => runConfigSchema.parse(value),
          ),
        expected,
      );
    };
    await refuseVariant("missing-parent-ref", (copy) => {
      copy.records = copy.records.filter((record) =>
        !record.name.endsWith("-comparison-2")
      );
    }, /Missing exact parent evidence/);
    await refuseVariant("changed-partial-log", (copy) => {
      copy.logs[0].sha256 = "0".repeat(64);
    }, /Parent log changed/);
    await refuseVariant("missing-abandonment", (copy) => {
      copy.abandoned = null;
    }, /Active parent step needs verified abandonment/);
    const completedArchive = z.object({
      records: z.array(
        z.object({
          name: z.string(),
          checksum: z.string(),
          content: z.unknown(),
        }).passthrough(),
      ),
      logs: z.array(
        z.object({ path: z.string(), sha256: z.string() }).passthrough(),
      ),
      abandoned: z.unknown(),
    }).passthrough().parse(JSON.parse(await Deno.readTextFile(bundle)));
    await refuseVariant(
      "unreviewed-best",
      (copy) => {
        const review = copy.records.find((record) =>
          record.name.endsWith("-step-19")
        );
        if (!review) throw new Error("Missing fixture review step");
        const content = z.object({
          outcome: z.object({
            kind: z.literal("review"),
            revision: z.string(),
            passed: z.literal(true),
          }),
        }).passthrough()
          .parse(review.content);
        review.content = {
          ...content,
          outcome: { ...content.outcome, passed: false },
        };
        review.checksum = createHash("sha256").update(
          JSON.stringify(review.content),
        ).digest("hex");
      },
      /Parent step does not follow the previous result/,
      completedArchive,
    );
    Deno.chdir(second);
    const expiredInputs = runArgumentsSchema.parse({
      ...childInputs,
      runId: "fixture-campaign-expired",
      directory: join(root, "expired"),
    });
    await rejects(
      () =>
        startRun(
          new Store(),
          expiredInputs,
          parent.startedAtMs + limits.maxDurationMs,
        ),
      /remaining budget differs/,
    );
    const changedPolicy = runArgumentsSchema.parse({
      ...childInputs,
      runId: "fixture-campaign-policy-change",
      directory: join(root, "policy-change"),
      policy: { ...policy, minimumImprovementFraction: 0.03 },
    });
    await rejects(
      () => startRun(new Store(), changedPolicy),
      /policy or remaining budget differs/,
    );
    await rejects(
      () =>
        startRun(
          new Store(),
          runArgumentsSchema.parse({
            ...childInputs,
            runId: "fixture-campaign-brief-change",
            directory: join(root, "brief-change"),
            experimentBrief: "A changed hypothesis",
          }),
        ),
      /policy or remaining budget differs/,
    );
    await rejects(
      () =>
        startRun(
          new Store(),
          runArgumentsSchema.parse({
            ...childInputs,
            runId: "fixture-campaign-brief-removed",
            directory: join(root, "brief-removed"),
            experimentBrief: undefined,
          }),
        ),
      /policy or remaining budget differs/,
    );
    Deno.chdir(originalCwd);
    await rejects(
      () =>
        verifyParentEvidence(
          bundle,
          "0".repeat(64),
          (value) => runConfigSchema.parse(value),
        ),
      /Parent bundle changed/,
    );
    const changed = await Deno.readTextFile(bundle);
    await Deno.chmod(bundle, 0o600);
    await Deno.writeTextFile(
      bundle,
      changed.replace("fixture-campaign-parent", "wrong-parent"),
    );
    await rejects(
      () =>
        verifyParentEvidence(
          bundle,
          digest,
          (value) => runConfigSchema.parse(value),
        ),
      /Parent bundle changed/,
    );
    passed = true;
  } finally {
    Deno.chdir(originalCwd);
    if (passed) await Deno.remove(root, { recursive: true });
    else console.error(`Campaign fixture retained at ${root}`);
  }
});
