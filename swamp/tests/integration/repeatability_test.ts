import { deepStrictEqual } from "node:assert/strict";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  defaultPrecision,
  precisionSummary,
  scorePolicyVersion,
} from "../../extensions/models/_lib/benchmark.ts";

const fixture = fileURLToPath(
  new URL("../fixtures/tempo_tools.py", import.meta.url),
);
const script = fileURLToPath(
  new URL("../../extensions/models/_lib/repeatability.ts", import.meta.url),
);

Deno.test("unscored repeatability runs exactly two three-sample batches for both protocols and retains unstable results", async () => {
  const root = await Deno.makeTempDir({
    prefix: "tempo-repeatability-fixture-",
  });
  try {
    const repository = join(root, "repo");
    const bin = join(root, "bin");
    await Deno.mkdir(join(repository, "src/main/clojure/ol"), {
      recursive: true,
    });
    await Deno.mkdir(bin);
    await Deno.copyFile(fixture, join(bin, "bb"));
    await Deno.chmod(join(bin, "bb"), 0o700);
    await Deno.writeTextFile(
      join(repository, "src/main/clojure/ol/busker.clj"),
      "; initial\n",
    );
    await Deno.writeTextFile(join(repository, ".gitignore"), "*.so\n");
    const git = async (...args: string[]) => {
      const result = await new Deno.Command("git", { cwd: repository, args })
        .output();
      deepStrictEqual(result.success, true);
    };
    await git("init", "--initial-branch=main");
    await git(
      "-c",
      "user.name=Fixture",
      "-c",
      "user.email=fixture@localhost",
      "add",
      ".",
    );
    await git(
      "-c",
      "user.name=Fixture",
      "-c",
      "user.email=fixture@localhost",
      "commit",
      "-m",
      "fixture initial",
    );
    const artifact = `linux-${
      Deno.build.arch === "x86_64" ? "x86-64" : Deno.build.arch
    }`;
    const native = join(
      repository,
      "shim",
      artifact,
      "resources",
      artifact,
      "libh2oclj.so",
    );
    await Deno.mkdir(
      join(repository, "shim", artifact, "resources", artifact),
      { recursive: true },
    );
    await Deno.writeTextFile(native, "fixture native bytes");
    const run = async (
      name: string,
      condition = name,
      precision: Partial<typeof defaultPrecision> = defaultPrecision,
    ) => {
      const directory = join(root, name);
      const result = await new Deno.Command("deno", {
        args: [
          "run",
          "--allow-all",
          script,
          repository,
          directory,
          JSON.stringify(precision),
        ],
        env: {
          PATH: `${bin}:${Deno.env.get("PATH")}`,
          IN_NIX_SHELL: "1",
          TEMPO_FIXTURE_CASE: condition,
        },
      }).output();
      const report = JSON.parse(
        await Deno.readTextFile(join(directory, "repeatability.json")),
      );
      const stderr = await Deno.readTextFile(
        join(directory, "logs", "h1-1.stderr.log"),
      );
      deepStrictEqual(
        report.batches.map((
          batch: { protocol: string; batch: number; samples: number[] },
        ) => [batch.protocol, batch.batch, batch.samples.length]),
        [["h1", 1, 3], ["h1", 2, 3], ["tls-h2", 1, 3], ["tls-h2", 2, 3]],
        `${report.error ?? new TextDecoder().decode(result.stderr)}\n${stderr}`,
      );
      return { result, report };
    };
    const stable = await run("repair");
    deepStrictEqual(stable.result.success, true);
    deepStrictEqual(stable.report.passed, true);
    deepStrictEqual(stable.report.scorePolicyVersion, scorePolicyVersion);
    deepStrictEqual(stable.report.precision, defaultPrecision);
    deepStrictEqual(stable.report.plannedSamples, 6);
    const unstable = await run("noise");
    deepStrictEqual(unstable.result.success, false);
    deepStrictEqual(unstable.report.passed, false);
    deepStrictEqual(
      unstable.report.pooled.h1.relativeHalfWidth >
        defaultPrecision.relativeHalfWidth,
      true,
    );
    deepStrictEqual(unstable.report.batches.length, 4);
    const custom = { relativeHalfWidth: 0.045, confidenceLevel: 0.99 };
    const passing = await run("custom-pass", "noise-stabilizes", custom);
    deepStrictEqual(passing.report.passed, true);
    deepStrictEqual(passing.report.precision, custom);
    const partial = await run("partial-precision", "repair", {
      confidenceLevel: 0.99,
    });
    deepStrictEqual(partial.report.passed, true);
    deepStrictEqual(partial.report.precision, {
      relativeHalfWidth: defaultPrecision.relativeHalfWidth,
      confidenceLevel: 0.99,
    });
    const failing = await run(
      "custom-fail",
      "noise-stabilizes",
      { ...custom, relativeHalfWidth: 0.04 },
    );
    deepStrictEqual(failing.report.passed, false);
    deepStrictEqual(failing.result.success, false);
    deepStrictEqual(
      failing.report.pooled.h1.relativeHalfWidth >
        failing.report.precision.relativeHalfWidth,
      true,
    );
    deepStrictEqual(
      failing.report.pooled.h1.mean,
      precisionSummary(
        failing.report.batches.filter((batch: { protocol: string }) =>
          batch.protocol === "h1"
        )
          .flatMap((batch: { samples: number[] }) => batch.samples),
        failing.report.precision,
      ).mean,
    );
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});
