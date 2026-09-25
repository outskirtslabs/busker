import { basename, dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { hashFile } from "./benchmark-runner.ts";
import {
  defaultPrecision,
  meetsPrecision,
  precisionSchema,
  precisionSummary,
  scorePolicyVersion,
  validateBenchmark,
} from "./benchmark.ts";
import {
  benchmarkMethod,
  placementEnvironment,
  validatePlacementEvidence,
} from "./placement.ts";
import { runCommand } from "./process.ts";
import { inspectCheckout } from "./workspace.ts";

const parameters = {
  warmup: 10,
  duration: 30,
  repetitions: 3,
  connections: 128,
  streams: 64,
  threads: 2,
};
const [repository, directory, precisionJson] = Deno.args;
const precision = precisionSchema.parse(
  precisionJson ? JSON.parse(precisionJson) : defaultPrecision,
);
if (!repository || !directory || !Deno.env.get("IN_NIX_SHELL")) {
  throw new Error(
    "Usage in the Nix devshell: deno run -A repeatability.ts <clean-checkout> <new-absolute-output-directory> [precision-json]",
  );
}
const outputRoot = resolve(directory);
if (
  outputRoot !== directory || !/^[a-z0-9-]{1,50}$/.test(basename(directory)) ||
  await Deno.realPath(dirname(directory)) !== dirname(directory)
) {
  throw new Error(
    "Repeatability needs a new absolute directory without symlinks",
  );
}
const checkout = await inspectCheckout(repository);
if (checkout.dirty) {
  throw new Error("Repeatability requires a clean, frozen checkout");
}
const artifact = `linux-${
  Deno.build.arch === "x86_64" ? "x86-64" : Deno.build.arch
}`;
const native = join(
  checkout.path,
  "shim",
  artifact,
  "resources",
  artifact,
  "libh2oclj.so",
);
const nativeSha256 = await hashFile(native);
const placement = await placementEnvironment(outputRoot);
await Deno.mkdir(outputRoot, { mode: 0o700 });
await Deno.mkdir(join(outputRoot, "logs"), { mode: 0o700 });
const report: {
  method: typeof benchmarkMethod;
  scorePolicyVersion: typeof scorePolicyVersion;
  precision: typeof precision;
  plannedSamples: 6;
  revision: string;
  nativeSha256: string;
  placement: { topology: typeof placement.topology; realH2load: string };
  parameters: typeof parameters;
  batches: {
    protocol: string;
    batch: number;
    samples: number[];
    directory: string;
  }[];
  pooled: Record<string, ReturnType<typeof precisionSummary>>;
  passed: boolean;
  error: string | null;
} = {
  method: benchmarkMethod,
  scorePolicyVersion,
  precision,
  plannedSamples: 6,
  revision: checkout.revision,
  nativeSha256,
  placement: { topology: placement.topology, realH2load: placement.realH2load },
  parameters,
  batches: [],
  pooled: {},
  passed: false,
  error: null,
};
try {
  let versions: { java: string; h2load: string } | null = null;
  for (const protocol of ["h1", "tls-h2"] as const) {
    for (const batch of [1, 2]) {
      const output = join(outputRoot, protocol, String(batch));
      await Deno.mkdir(dirname(output), { recursive: true, mode: 0o700 });
      const execution = await runCommand({
        id: `${basename(outputRoot)}-${protocol}-${batch}`,
        command: "taskset",
        args: [
          "-c",
          "10,11",
          "bb",
          "bench:local",
          "--adapter",
          "busker",
          "--protocol",
          protocol,
          "--output",
          output,
          ...Object.entries(parameters).flatMap((
            [key, value],
          ) => [`--${key}`, String(value)]),
        ],
        cwd: checkout.path,
        env: {
          ...placement.env,
          TEMPO_AFFINITY_LOG: join(output, "client-affinity.tsv"),
        },
        stdoutPath: join(outputRoot, "logs", `${protocol}-${batch}.stdout.log`),
        stderrPath: join(outputRoot, "logs", `${protocol}-${batch}.stderr.log`),
      });
      if (!execution.success) {
        throw new Error(
          `Repeatability ${protocol}/${batch} failed: ${
            execution.error ?? execution.status?.code
          }`,
        );
      }
      await validatePlacementEvidence(
        output,
        Array.from(
          { length: 3 },
          (_, index) => join(output, "busker", protocol, String(index + 1)),
        ),
        parameters.warmup,
        parameters.duration,
        placement.realH2load,
      );
      const raw = JSON.parse(
        await Deno.readTextFile(join(output, "results.json")),
      );
      const validated = validateBenchmark(raw, {
        checkout: checkout.path,
        revision: checkout.revision,
        nativeSha256,
        nativeResource: pathToFileURL(native).href,
        sourceResource:
          pathToFileURL(join(checkout.path, "src/main/clojure/ol/busker.clj"))
            .href,
        protocol,
        adapter: "busker",
        purpose: "measurement",
        parameters,
      });
      const currentVersions = {
        java: validated.javaVersion,
        h2load: validated.h2loadVersion,
      };
      if (
        versions && JSON.stringify(versions) !== JSON.stringify(currentVersions)
      ) {
        throw new Error(
          "Repeatability batches used different JVM or h2load versions",
        );
      }
      versions = currentVersions;
      const after = await inspectCheckout(checkout.path);
      if (
        after.revision !== checkout.revision || after.dirty ||
        await hashFile(native) !== nativeSha256
      ) {
        throw new Error(
          "Source or native library changed during repeatability",
        );
      }
      report.batches.push({
        protocol,
        batch,
        directory: output,
        samples: raw.results[0].protocols[protocol].samples.map(
          (sample: { "requests-per-second": number }) =>
            sample["requests-per-second"],
        ),
      });
    }
  }
  for (const protocol of ["h1", "tls-h2"]) {
    const rates = report.batches.filter((batch) => batch.protocol === protocol)
      .flatMap((batch) => batch.samples);
    if (rates.length !== report.plannedSamples) {
      throw new Error("Repeatability needs six samples per protocol");
    }
    report.pooled[protocol] = precisionSummary(rates, precision);
  }
  for (const protocol of ["h1", "tls-h2"]) {
    if (!meetsPrecision(report.pooled[protocol], precision)) {
      throw new Error(
        `Pooled ${protocol} precision exceeds configured relative half-width; do not launch or retry adaptively`,
      );
    }
  }
  report.passed = true;
} catch (failure) {
  report.error = String(failure);
} finally {
  await Deno.writeTextFile(
    join(outputRoot, "repeatability.json"),
    JSON.stringify(report, null, 2) + "\n",
    { createNew: true },
  );
}
if (!report.passed) throw new Error(report.error ?? "Repeatability failed");
