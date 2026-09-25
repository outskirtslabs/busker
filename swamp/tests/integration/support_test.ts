import { deepStrictEqual, rejects } from "node:assert/strict";
import { createHash } from "node:crypto";
import { join } from "node:path";
import {
  supportManifest,
  verifySupport,
} from "../../extensions/models/_lib/support.ts";
import { executeRun, startRun } from "../../extensions/models/_lib/run.ts";
import { benchmarkMethod } from "../../extensions/models/_lib/placement.ts";
import { Store } from "../fixtures/store.ts";

Deno.test("support manifest detects changed, added, removed, and linked runtime files", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-support-" });
  try {
    for (
      const directory of [
        "extensions/models",
        "models",
        "workflows",
        "prompts",
        "pi",
        "clojure",
      ]
    ) {
      await Deno.mkdir(join(root, directory), { recursive: true });
      await Deno.writeTextFile(join(root, directory, "input.txt"), directory);
    }
    await Deno.writeTextFile(join(root, ".swamp.yaml"), "tools: []\n");
    const initial = await supportManifest(root);
    deepStrictEqual(initial.files.length, 7);
    await verifySupport(root, initial);
    await Deno.mkdir(join(root, ".swamp"));
    await Deno.writeTextFile(join(root, ".swamp", "runtime.db"), "mutable");
    await verifySupport(root, initial);
    const prompt = join(root, "prompts/input.txt");
    await Deno.writeTextFile(prompt, "changed");
    await rejects(() => verifySupport(root, initial), /Support code changed/);
    await Deno.writeTextFile(prompt, "prompts");
    await Deno.writeTextFile(join(root, "pi/new.js"), "extra");
    await rejects(() => verifySupport(root, initial), /Support code changed/);
    await Deno.remove(join(root, "pi/new.js"));
    await Deno.remove(join(root, "clojure/input.txt"));
    await rejects(() => verifySupport(root, initial), /Support code changed/);
    await Deno.symlink(prompt, join(root, "clojure/link"));
    await rejects(() => supportManifest(root), /symlink is not permitted/);
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});

Deno.test("run refuses a different support repository or changed runtime files before any effect", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-support-run-" });
  const previous = Deno.cwd();
  try {
    const repository = join(root, "repo");
    const support = join(root, "support");
    await Deno.mkdir(repository);
    await Deno.mkdir(support);
    for (
      const name of [
        "extensions/models",
        "models",
        "workflows",
        "prompts",
        "pi",
        "clojure",
      ]
    ) {
      await Deno.mkdir(join(support, name), { recursive: true });
      await Deno.writeTextFile(join(support, name, "input.txt"), name);
    }
    await Deno.writeTextFile(join(support, ".swamp.yaml"), "tools: []\n");
    const git = async (...args: string[]) => {
      const result = await new Deno.Command("git", {
        cwd: repository,
        args: [
          "-c",
          "user.name=Fixture",
          "-c",
          "user.email=fixture@localhost",
          ...args,
        ],
      }).output();
      if (!result.success) {
        throw new Error(new TextDecoder().decode(result.stderr));
      }
      return new TextDecoder().decode(result.stdout).trim();
    };
    await git("init", "--initial-branch=main");
    await Deno.writeTextFile(join(repository, "source.txt"), "initial");
    await Deno.mkdir(join(repository, "swamp"));
    for (
      const name of [
        "extensions/models",
        "models",
        "workflows",
        "prompts",
        "pi",
        "clojure",
      ]
    ) {
      await Deno.mkdir(join(repository, "swamp", name), { recursive: true });
      await Deno.copyFile(
        join(support, name, "input.txt"),
        join(repository, "swamp", name, "input.txt"),
      );
    }
    await Deno.copyFile(
      join(support, ".swamp.yaml"),
      join(repository, "swamp/.swamp.yaml"),
    );
    await git("add", "source.txt", "swamp");
    await git("add", "-f", "swamp/prompts/input.txt");
    await git("commit", "-m", "fixture");
    const revision = await git("rev-parse", "HEAD");
    await Deno.writeTextFile(
      join(repository, "AGENTS.md"),
      "Local fixture guidance; untracked and read-only.\n",
    );
    const manifest = await supportManifest(support);
    const args = {
      confirmLive: true as const,
      runId: "fixture",
      repository,
      revision,
      directory: join(root, "output"),
      supportDirectory: support,
      supportSha256: manifest.sha256,
      supportRevision: revision,
      credentialDirectory: join(root, "credentials"),
      referenceRoots: [],
      benchmarkMethod,
      parameters: {
        warmup: 10,
        duration: 30,
        repetitions: 3,
        connections: 128,
        streams: 64,
        threads: 2,
      },
      limits: {
        maxAttempts: 1,
        maxRepairsPerAttempt: 0,
        maxPiCalls: 1,
        maxDurationMs: 1000,
      },
      policy: {
        goal: {
          kind: "improve-baseline" as const,
          multiplier: 1.5,
          workloads: ["h1"],
        },
        protections: [{ workload: "tls-h2", maxDecreaseFraction: 0.05 }],
        minimumImprovementFraction: 0.05,
      },
    };
    const store = new Store();
    await rejects(() => startRun(store, args), /selected support snapshot/);
    Deno.chdir(support);
    await rejects(
      () => startRun(store, { ...args, supportSha256: "f".repeat(64) }),
      /approved manifest/,
    );
    const previousRepo = Deno.env.get("SWAMP_REPO_DIR");
    try {
      Deno.env.set("SWAMP_REPO_DIR", repository);
      await rejects(
        () => startRun(store, args),
        /selected local Swamp repository/,
      );
    } finally {
      if (previousRepo === undefined) Deno.env.delete("SWAMP_REPO_DIR");
      else Deno.env.set("SWAMP_REPO_DIR", previousRepo);
    }
    await rejects(
      () => startRun(store, { ...args, supportRevision: "0".repeat(40) }),
      /Cannot inspect reviewed support commit/,
    );
    await Deno.writeTextFile(join(support, "pi/extra.js"), "extra");
    await rejects(
      async () =>
        startRun(store, {
          ...args,
          supportSha256: (await supportManifest(support)).sha256,
        }),
      /Support file set differs from reviewed Git commit/,
    );
    await Deno.remove(join(support, "pi/extra.js"));
    await Deno.writeTextFile(join(support, "prompts/input.txt"), "changed");
    await rejects(
      async () =>
        startRun(store, {
          ...args,
          supportSha256: (await supportManifest(support)).sha256,
        }),
      /Support file differs from reviewed Git commit/,
    );
    await Deno.writeTextFile(join(support, "prompts/input.txt"), "prompts");
    await Deno.remove(join(support, "clojure/input.txt"));
    await rejects(
      async () =>
        startRun(store, {
          ...args,
          supportSha256: (await supportManifest(support)).sha256,
        }),
      /Support file set differs from reviewed Git commit/,
    );
    await Deno.writeTextFile(join(support, "clojure/input.txt"), "clojure");
    const started = await startRun(store, args);
    deepStrictEqual(started.config.supportManifest.sha256, manifest.sha256);
    deepStrictEqual(started.config.dataDirectory, join(support, ".swamp"));
    const frozen = await store.readResource("fixture-agents-context", 1);
    deepStrictEqual([frozen?.source, frozen?.text, frozen?.sha256], [
      "untracked project AGENTS.md",
      "Local fixture guidance; untracked and read-only.\n",
      createHash("sha256").update(
        "Local fixture guidance; untracked and read-only.\n",
      ).digest("hex"),
    ]);
    await Deno.writeTextFile(join(repository, "AGENTS.md"), "Changed later\n");
    deepStrictEqual(
      (await store.readResource("fixture-agents-context", 1))?.text,
      frozen?.text,
    );
    let effects = 0;
    await Deno.writeTextFile(join(support, "prompts/input.txt"), "modified");
    await rejects(() =>
      executeRun(store, args.runId, () => {
        effects++;
        return Promise.reject(new Error("Effect must not run"));
      }), /Support code changed/);
    deepStrictEqual(effects, 0);
    deepStrictEqual(
      (await store.readResource("fixture-state"))?.activeStep,
      null,
    );
  } finally {
    Deno.chdir(previous);
    await Deno.remove(root, { recursive: true });
  }
});
