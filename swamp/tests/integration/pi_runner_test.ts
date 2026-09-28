import { createHash } from "node:crypto";
import { deepStrictEqual, rejects } from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import {
  readInference,
  saveInference,
} from "../../extensions/models/_lib/inference.ts";
import {
  finishPiStep,
  type PiRequest,
  preparePiStep,
  runPiEffect,
  runPiStep,
} from "../../extensions/models/_lib/pi-runner.ts";
import { newRun } from "../../extensions/models/_lib/state.ts";
import { runSavedStep } from "../../extensions/models/_lib/driver.ts";
import { recoverSavedStep } from "../../extensions/models/_lib/driver.ts";
import type { ArtifactStore } from "../../extensions/models/_lib/artifacts.ts";
import {
  createWorkspace,
  snapshotWorkspace,
} from "../../extensions/models/_lib/workspace.ts";

import { Store } from "../fixtures/store.ts";
import { authorizePath } from "../../pi/guard.ts";

async function fixture(root: string) {
  const store = new Store();
  const revision = "a".repeat(40);
  await store.writeResource("state", "run-state", {
    ...newRun("run", revision),
  });
  await store.writeResource("state", "run-state", {
    ...newRun("run", revision),
    phase: "baseline",
  });
  const workspace = join(root, "work");
  const credentials = join(root, "credentials");
  await Deno.mkdir(workspace);
  await Deno.mkdir(credentials);
  await Deno.writeTextFile(
    join(credentials, "auth.json"),
    "fixture-credential-not-for-inference",
  );
  const request: PiRequest = {
    context: {
      runId: "run",
      step: 1,
      stage: "analyze",
      revision,
      snapshotSha256: "b".repeat(64),
      inputs: [{ name: "run-state", version: 1 }],
    },
    workspace,
    referenceRoots: [],
    directory: join(root, "step"),
    credentialDirectory: credentials,
    promptsDirectory: fileURLToPath(new URL("../../prompts", import.meta.url)),
    extensionPath: fileURLToPath(
      new URL("../../pi/restricted-tools.js", import.meta.url),
    ),
  };
  return { store, request };
}

function transcript(
  request: PiRequest,
  responseOverride?: Record<string, unknown>,
) {
  const response = {
    action: "stop",
    reason: "Fixture only",
    evidence: request.context.inputs,
  };
  return [
    { type: "session" },
    {
      type: "entry_appended",
      entry: {
        customType: "tempo-policy",
        data: {
          workspace: request.workspace,
          readRoots: [join(request.directory, "inputs")],
          stage: request.context.stage,
        },
      },
    },
    {
      type: "message_end",
      message: {
        role: "assistant",
        provider: "openai-codex",
        model: "gpt-6-astra",
        stopReason: "stop",
        content: [{
          type: "text",
          text: JSON.stringify(responseOverride ?? response),
        }],
        usage: { totalTokens: 10 },
      },
    },
    { type: "agent_settled" },
  ].map((event) => JSON.stringify(event)).join("\n") + "\n";
}

Deno.test("Pi launcher handoff uses saved versions and archives output without a provider call", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-pi-fixture-" });
  const priorPath = Deno.env.get("PATH") ?? "";
  const priorTrace = Deno.env.get("TEMPO_FIXTURE_TRACE");
  try {
    const { store, request } = await fixture(root);
    const bin = join(root, "bin");
    await Deno.mkdir(bin);
    const fakePi = join(bin, "pi");
    await Deno.writeTextFile(
      fakePi,
      '#!/bin/sh\ncat "$TEMPO_FIXTURE_TRACE"\nprintf "fixture diagnostic\\n" >&2\n',
    );
    await Deno.chmod(fakePi, 0o700);
    const trace = join(root, "fixture.jsonl");
    await Deno.writeTextFile(trace, transcript(request));
    Deno.env.set("PATH", `${bin}:${priorPath}`);
    Deno.env.set("TEMPO_FIXTURE_TRACE", trace);
    const result = await runPiStep(store, request);
    deepStrictEqual(result.record.status, "valid");
    deepStrictEqual(await readInference(store, result.saved, request.context), {
      action: "stop",
      reason: "Fixture only",
      evidence: request.context.inputs,
    });
    const manifest = new TextDecoder().decode(
      store.files.get("run-pi-1-input"),
    );
    deepStrictEqual(
      new TextDecoder().decode(store.files.get("run-pi-1-system-prompt")),
      await Deno.readTextFile(join(request.promptsDirectory, "system.md")),
    );
    deepStrictEqual(
      new TextDecoder().decode(store.files.get("run-pi-1-analyze-prompt")),
      await Deno.readTextFile(join(request.promptsDirectory, "analyze.md")),
    );
    deepStrictEqual(manifest.includes('"phase": "prepare"'), true);
    deepStrictEqual(manifest.includes('"phase": "baseline"'), false);
    deepStrictEqual(manifest.includes("fixture-credential"), false);
    deepStrictEqual(
      new TextDecoder().decode(store.files.get("run-pi-1-trace")),
      transcript(request),
    );
    deepStrictEqual(
      new TextDecoder().decode(store.files.get("run-pi-1-stderr")),
      "fixture diagnostic\n",
    );
    await rejects(
      () => Deno.stat(join(request.directory, "agent")),
      Deno.errors.NotFound,
    );
    deepStrictEqual(
      await Deno.readTextFile(join(request.credentialDirectory, "auth.json")),
      "fixture-credential-not-for-inference",
    );
  } finally {
    Deno.env.set("PATH", priorPath);
    if (priorTrace === undefined) Deno.env.delete("TEMPO_FIXTURE_TRACE");
    else Deno.env.set("TEMPO_FIXTURE_TRACE", priorTrace);
    await Deno.remove(root, { recursive: true });
  }
});

Deno.test("review receives archived exact-commit diff through a readable file", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-review-diff-" });
  const priorPath = Deno.env.get("PATH") ?? "";
  const priorTrace = Deno.env.get("TEMPO_FIXTURE_TRACE");
  try {
    const { store, request } = await fixture(root);
    const git = async (...args: string[]) => {
      const result = await new Deno.Command("git", {
        cwd: request.workspace,
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
      return new TextDecoder().decode(result.stdout);
    };
    await git("init", "--initial-branch=main");
    const source = join(request.workspace, "source.txt");
    await Deno.writeTextFile(source, "initial\n");
    await git("add", "source.txt");
    await git("commit", "-m", "initial");
    const baseRevision = (await git("rev-parse", "HEAD")).trim();
    await Deno.writeTextFile(source, "candidate\n");
    await git("add", "source.txt");
    await git("commit", "-m", "candidate");
    const candidateRevision = (await git("rev-parse", "HEAD")).trim();
    request.context = {
      ...request.context,
      stage: "review",
      revision: candidateRevision,
    };
    request.reviewBaseRevision = baseRevision;
    const expected = await git(
      "diff",
      "--no-ext-diff",
      "--no-textconv",
      "--binary",
      "--full-index",
      baseRevision,
      candidateRevision,
      "--",
    );
    await Deno.writeTextFile(source, "unsaved change\n");
    const prepared = await preparePiStep(store, request);
    const manifest = JSON.parse(
      await Deno.readTextFile(join(request.directory, "inputs/manifest.json")),
    );
    const path = join(request.directory, "inputs/revision.diff");
    const sha256 = createHash("sha256").update(expected).digest("hex");
    deepStrictEqual(manifest.reviewDiff, {
      baseRevision,
      candidateRevision,
      sha256,
      path,
      reference: { name: "run-pi-1-review-diff", version: 1 },
    });
    deepStrictEqual(manifest.context.inputs, [
      ...request.context.inputs,
      { name: "run-pi-1-review-diff", version: 1 },
    ]);
    deepStrictEqual(manifest.inputs.at(-1), {
      reference: { name: "run-pi-1-review-diff", version: 1 },
      data: { baseRevision, candidateRevision, sha256, diff: expected },
    });
    deepStrictEqual(await Deno.readTextFile(path), expected);
    deepStrictEqual(
      await authorizePath(prepared.policy, "tempo_read", path),
      path,
    );
    deepStrictEqual(
      JSON.parse(
        new TextDecoder().decode(store.files.get("run-pi-1-review-diff")),
      ),
      { baseRevision, candidateRevision, sha256, diff: expected },
    );
    await Deno.writeTextFile(
      prepared.command.stdoutPath,
      transcript(request, {
        verdict: "pass",
        findings: [],
        evidence: manifest.context.inputs,
      }),
    );
    await Deno.writeTextFile(prepared.command.stderrPath, "");
    const finished = await finishPiStep(store, prepared, {
      success: true,
      error: null,
      status: { code: 0, signal: null },
      cleanup: "absent",
    });
    deepStrictEqual(
      finished.dataHandles.some((item) => item.name === "run-pi-1-review-diff"),
      true,
    );
    deepStrictEqual(finished.record.status, "valid");
    deepStrictEqual(finished.record.inputs, manifest.context.inputs);
    const response = (evidence: { name: string; version: number }[]) =>
      JSON.stringify({ verdict: "pass", findings: [], evidence });
    const citedDiff = await saveInference(
      store,
      prepared.context,
      response([{ name: "run-pi-1-review-diff", version: 1 }]),
      0,
    );
    deepStrictEqual(citedDiff.record.status, "valid");
    const unknown = await saveInference(
      store,
      prepared.context,
      response([{ name: "run-pi-1-unknown", version: 1 }]),
      0,
    );
    deepStrictEqual(unknown.record.status, "invalid");
    deepStrictEqual(
      unknown.record.error,
      "Pi cited an output that was not in its saved inputs",
    );
    const bin = join(root, "bin");
    await Deno.mkdir(bin);
    const fakePi = join(bin, "pi");
    await Deno.writeTextFile(
      fakePi,
      '#!/bin/sh\ncat "$TEMPO_FIXTURE_TRACE"\n',
    );
    await Deno.chmod(fakePi, 0o700);
    request.directory = join(root, "review-effect");
    const trace = join(root, "review-trace.jsonl");
    await Deno.writeTextFile(
      trace,
      transcript(request, {
        verdict: "pass",
        findings: [],
        evidence: manifest.context.inputs,
      }),
    );
    Deno.env.set("PATH", `${bin}:${priorPath}`);
    Deno.env.set("TEMPO_FIXTURE_TRACE", trace);
    const reviewed = await runPiEffect(
      store,
      {
        ...newRun("run", candidateRevision),
        phase: "review",
        sequence: 1,
        activeStep: 1,
      },
      request,
    );
    deepStrictEqual(reviewed.outcome, {
      kind: "review",
      revision: candidateRevision,
      passed: true,
    });
    deepStrictEqual(
      (await store.readResource("run-pi-1-result"))?.status,
      "valid",
    );
    request.directory = join(root, "missing-base");
    delete request.reviewBaseRevision;
    await rejects(() => preparePiStep(store, request), /distinct full base/);
    await rejects(() => Deno.stat(request.directory), Deno.errors.NotFound);
    request.directory = join(root, "unknown-base");
    request.reviewBaseRevision = "f".repeat(40);
    await rejects(() => preparePiStep(store, request), /not a Git commit/);
    await rejects(() => Deno.stat(request.directory), Deno.errors.NotFound);
    request.directory = join(root, "same-revision");
    request.reviewBaseRevision = candidateRevision;
    await rejects(() => preparePiStep(store, request), /distinct full base/);
    await rejects(() => Deno.stat(request.directory), Deno.errors.NotFound);
    await git("commit", "--allow-empty", "-m", "same tree");
    request.context.revision = (await git("rev-parse", "HEAD")).trim();
    request.reviewBaseRevision = candidateRevision;
    request.directory = join(root, "empty-diff");
    await rejects(
      () => preparePiStep(store, request),
      /nonempty committed revision diff/,
    );
    await rejects(() => Deno.stat(request.directory), Deno.errors.NotFound);
  } finally {
    await Deno.remove(root, { recursive: true });
    Deno.env.set("PATH", priorPath);
    if (priorTrace === undefined) Deno.env.delete("TEMPO_FIXTURE_TRACE");
    else Deno.env.set("TEMPO_FIXTURE_TRACE", priorTrace);
  }
});

Deno.test("missing input versions fail before any Pi files are prepared", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-pi-fixture-" });
  try {
    const { store, request } = await fixture(root);
    request.context.inputs[0].version = 99;
    await rejects(() => preparePiStep(store, request), /missing/);
    await rejects(() => Deno.stat(request.directory), Deno.errors.NotFound);
    request.context.inputs[0].version = 1;
    request.referenceRoots = [root];
    await rejects(() => preparePiStep(store, request), /must not expose/);
    request.referenceRoots = [];
    await Deno.symlink(request.workspace, join(root, "workspace-link"));
    request.directory = join(root, "workspace-link", "step");
    await rejects(() => preparePiStep(store, request), /symlink/);
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});

Deno.test("malformed Pi streams stay invalid even with zero exit status; archive failures are not hidden", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-pi-fixture-" });
  try {
    const { store, request } = await fixture(root);
    const prepared = await preparePiStep(store, request);
    const settings = await Deno.readTextFile(
      join(prepared.agentDirectory, "settings.json"),
    );
    deepStrictEqual(settings.includes('"compaction":{"enabled":false}'), true);
    deepStrictEqual(settings.includes('"retry":{"enabled":false'), true);
    deepStrictEqual(settings.includes('"cacheWarming":"off"'), true);
    const text = JSON.stringify({
      action: "stop",
      reason: "Not an event stream",
      evidence: request.context.inputs,
    });
    await Deno.writeTextFile(prepared.command.stdoutPath, text);
    await Deno.writeTextFile(prepared.command.stderrPath, "");
    const execution = {
      success: true,
      error: null,
      status: { code: 0, signal: null },
      cleanup: "absent" as const,
    };
    const saved = await finishPiStep(store, prepared, execution);
    deepStrictEqual(saved.record.status, "invalid");
    deepStrictEqual(
      (await store.readResource(saved.raw.name, saved.raw.version))?.exitCode,
      0,
    );
    deepStrictEqual(
      (await store.readResource(saved.raw.name, saved.raw.version))?.text,
      text,
    );
    store.failArchive = true;
    await rejects(
      () => finishPiStep(store, prepared, execution),
      /Archive unavailable/,
    );
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});

Deno.test("saved Pi effects preserve Git edits and traces after malformed output or interrupted step write", async () => {
  const priorPath = Deno.env.get("PATH") ?? "";
  const priorTrace = Deno.env.get("TEMPO_FIXTURE_TRACE");
  for (const scenario of ["valid", "malformed", "interrupted"] as const) {
    const valid = scenario !== "malformed";
    const root = await Deno.makeTempDir({ prefix: "tempo-pi-effect-" });
    try {
      const { store, request } = await fixture(root);
      const repository = request.workspace;
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
      await Deno.mkdir(join(repository, ".worktrees"));
      await Deno.mkdir(join(repository, "src/main"), { recursive: true });
      await Deno.writeTextFile(join(repository, ".gitignore"), ".worktrees/\n");
      await Deno.writeTextFile(
        join(repository, "src/main/source.txt"),
        "initial\n",
      );
      await git("add", ".");
      await git("commit", "-m", "fixture: create initial source");
      const revision = await git("rev-parse", "HEAD");
      const workspace = await createWorkspace(repository, "run", revision);
      request.workspace = workspace.path;
      const input = await store.writeResource("state", "run-state", {
        ...newRun("run", revision),
        phase: "implement",
      });
      request.context = {
        ...request.context,
        stage: "implement",
        revision,
        inputs: [input],
      };
      const bin = join(root, "bin");
      await Deno.mkdir(bin);
      await Deno.writeTextFile(
        join(bin, "pi"),
        '#!/bin/sh\nprintf "edited\\n" > src/main/source.txt\ncat "$TEMPO_FIXTURE_TRACE"\n',
      );
      await Deno.chmod(join(bin, "pi"), 0o700);
      const trace = join(root, "trace.jsonl");
      await Deno.writeTextFile(
        trace,
        valid
          ? transcript(request, {
            summary: "Edited fixture",
            evidence: [input],
          })
          : "malformed response\n",
      );
      Deno.env.set("PATH", `${bin}:${priorPath}`);
      Deno.env.set("TEMPO_FIXTURE_TRACE", trace);
      const active = {
        ...newRun("run", revision),
        phase: "implement" as const,
        sequence: 1,
        activeStep: 1,
      };
      await rejects(
        () => runPiEffect(store, active, request),
        /snapshot operation/,
      );
      await rejects(
        () => runPiEffect(store, { ...active, activeStep: 2 }, request),
        /active step/,
      );
      await rejects(() => Deno.stat(request.directory), Deno.errors.NotFound);
      const limits = {
        maxAttempts: 1,
        maxRepairsPerAttempt: 1,
        maxPiCalls: 1,
        maxDurationMs: 1000,
      };
      const effectStore: ArtifactStore = scenario === "interrupted"
        ? {
          readResource: (name, version) => store.readResource(name, version),
          writeResource: (spec, name, value) =>
            spec === "step" && name === "run-step-1"
              ? Promise.reject(new Error("Fixture interrupted after snapshot"))
              : store.writeResource(spec, name, value),
          createFileWriter: (spec, name) => store.createFileWriter(spec, name),
        }
        : store;
      if (scenario === "interrupted") {
        await rejects(
          () =>
            runSavedStep(
              effectStore,
              "run",
              limits,
              0,
              (active) =>
                runPiEffect(
                  effectStore,
                  active,
                  request,
                  () => snapshotWorkspace(workspace, "pi-1"),
                ),
            ),
          /Fixture interrupted after snapshot/,
        );
        const interrupted = await store.readResource("run-state");
        deepStrictEqual(interrupted?.activeStep, 1);
        deepStrictEqual(interrupted?.phase, "implement");
        deepStrictEqual(await store.readResource("run-step-1"), null);
        await rejects(() => recoverSavedStep(store, "run"), /no saved result/);
        await rejects(
          () =>
            runSavedStep(
              store,
              "run",
              limits,
              0,
              () => Promise.reject(new Error("Effect must not replay")),
            ),
          /step is active/,
        );
        deepStrictEqual(store.files.has("run-pi-1-trace"), true);
        deepStrictEqual(
          await git("show", "refs/tempo/run/pi-1:src/main/source.txt"),
          "edited",
        );
        deepStrictEqual(
          await Deno.readTextFile(join(repository, "src/main/source.txt")),
          "initial\n",
        );
        continue;
      }
      const result = await runSavedStep(
        store,
        "run",
        limits,
        0,
        (active) =>
          runPiEffect(
            store,
            active,
            request,
            () => snapshotWorkspace(workspace, "pi-1"),
          ),
      );
      const savedRevision = await git("rev-parse", "refs/tempo/run/pi-1");
      deepStrictEqual(
        await git("show", `${savedRevision}:src/main/source.txt`),
        "edited",
      );
      deepStrictEqual(
        await Deno.readTextFile(join(repository, "src/main/source.txt")),
        "initial\n",
      );
      deepStrictEqual(result.state.phase, valid ? "test" : "finish");
      deepStrictEqual(
        result.state.workingRevision,
        valid ? savedRevision : revision,
      );
      deepStrictEqual(result.state.bestRevision, revision);
      deepStrictEqual(
        result.record.evidence.some((ref) => ref.name === "run-pi-1-trace"),
        true,
      );
      deepStrictEqual(
        result.record.evidence.some((ref) => ref.name === "run-pi-1-result"),
        true,
      );
    } finally {
      Deno.env.set("PATH", priorPath);
      if (priorTrace === undefined) Deno.env.delete("TEMPO_FIXTURE_TRACE");
      else Deno.env.set("TEMPO_FIXTURE_TRACE", priorTrace);
      await Deno.remove(root, { recursive: true });
    }
  }
});
