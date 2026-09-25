import { createHash } from "node:crypto";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { z } from "npm:zod@4.4.3";
import { archiveFile, archiveLogs, type ArtifactStore } from "./artifacts.ts";
import {
  adapterSchema,
  benchmarkParametersSchema,
  defaultPrecision,
  meetsPrecision,
  precisionSchema,
  precisionSummary,
  protocolSchema,
  scoredRepetitionsSchema,
  scorePolicyVersion,
  validateBenchmark,
} from "./benchmark.ts";
import { runCommand } from "./process.ts";
import {
  benchmarkMethod,
  placementEnvironment,
  validatePlacementEvidence,
} from "./placement.ts";
import { runIdSchema } from "./schemas.ts";
import { inspectCheckout } from "./workspace.ts";
import type { OutputReference } from "./inference.ts";

export const benchmarkRequestSchema = z.strictObject({
  runId: runIdSchema,
  repository: z.string().min(1),
  directory: z.string().min(1),
  protocol: protocolSchema,
  adapter: adapterSchema,
  purpose: z.enum(["smoke", "measurement"]),
  parameters: benchmarkParametersSchema,
  precision: precisionSchema.default(defaultPrecision),
  deadlineMs: z.number().int().positive().optional(),
});
export async function hashFile(path: string) {
  const hash = createHash("sha256");
  const file = await Deno.open(path, { read: true });
  for await (const chunk of file.readable) hash.update(chunk);
  return hash.digest("hex");
}

export async function runBenchmark(
  store: ArtifactStore,
  input: z.input<typeof benchmarkRequestSchema>,
) {
  const request = benchmarkRequestSchema.parse(input);
  if (!Deno.env.get("IN_NIX_SHELL")) {
    throw new Error("Benchmark requires the Nix devshell");
  }
  const checkout = await inspectCheckout(request.repository);
  if (request.purpose === "measurement" && checkout.dirty) {
    throw new Error("Cannot measure dirty source");
  }
  if (
    !isAbsolute(request.directory) ||
    join(
        await Deno.realPath(dirname(request.directory)),
        basename(request.directory),
      ) !== resolve(request.directory)
  ) {
    throw new Error(
      "Benchmark directory must be absolute without symlink traversal",
    );
  }
  const platform = Deno.build.os === "darwin" ? "macos" : Deno.build.os;
  const arch = Deno.build.arch === "x86_64" ? "x86-64" : Deno.build.arch;
  const artifact = `${platform}-${arch}`;
  const native = join(
    checkout.path,
    "shim",
    artifact,
    "resources",
    artifact,
    `libh2oclj.${platform === "macos" ? "dylib" : "so"}`,
  );
  const expected = {
    checkout: checkout.path,
    revision: checkout.revision,
    nativeSha256: await hashFile(native),
    nativeResource: pathToFileURL(native).href,
    sourceResource:
      pathToFileURL(join(checkout.path, "src/main/clojure/ol/busker.clj")).href,
    protocol: request.protocol,
    adapter: request.adapter,
    purpose: request.purpose,
    parameters: request.parameters,
  };
  if (
    request.purpose === "smoke" &&
    (request.parameters.warmup !== 1 || request.parameters.duration !== 1 ||
      request.parameters.repetitions !== 1)
  ) {
    throw new Error(
      "Smoke checks require one one-second warmup and measurement",
    );
  }
  if (
    request.purpose === "measurement" &&
    !scoredRepetitionsSchema.safeParse(request.parameters.repetitions).success
  ) {
    throw new Error(
      "Scored measurements require exactly two batches of three or six samples",
    );
  }
  await Deno.mkdir(request.directory, { mode: 0o700 });
  const base = `${request.runId}-${request.adapter}-${request.protocol}`;
  const placement = await placementEnvironment(request.directory);
  const handles: OutputReference[] = [];
  const logs: OutputReference[] = [];
  const files: { path: string; reference: OutputReference }[] = [];
  const omitted: string[] = [];
  let summary: ReturnType<typeof validateBenchmark> | null = null;
  let execution: Awaited<ReturnType<typeof runCommand>> | null = null;
  let error: string | null = null;
  let inconclusive = false;
  const batches: { output: string; samples: number[] }[] = [];
  for (let batch = 1; batch <= 2; batch++) {
    if (request.deadlineMs !== undefined && Date.now() >= request.deadlineMs) {
      inconclusive = true;
      break;
    }
    const prefix = batch === 1 ? base : `${base}-repeat`;
    const output = join(
      request.directory,
      batch === 1 ? "benchmark" : "benchmark-repeat",
    );
    const command = {
      id: prefix,
      command: "taskset",
      args: [
        "-c",
        "10,11",
        "bb",
        "bench:local",
        "--adapter",
        request.adapter,
        "--protocol",
        request.protocol,
        "--output",
        output,
        ...Object.entries(request.parameters).flatMap(([key, value]) => [
          `--${key}`,
          String(value),
        ]),
      ],
      env: {
        ...placement.env,
        TEMPO_AFFINITY_LOG: join(output, "client-affinity.tsv"),
      },
      cwd: checkout.path,
      stdoutPath: join(request.directory, `stdout-${batch}.log`),
      stderrPath: join(request.directory, `stderr-${batch}.log`),
    };
    execution = null;
    try {
      execution = await runCommand(command);
    } catch (failure) {
      error = String(failure);
    }
    const archivedLogs = await archiveLogs(
      store,
      "benchmarkFile",
      prefix,
      command,
      !execution?.success,
    );
    for (const handle of archivedLogs) {
      const reference = { name: handle.name, version: handle.version };
      handles.push(reference);
      logs.push(reference);
    }
    let outputExists = true;
    try {
      await Deno.stat(output);
    } catch (failure) {
      if (!(failure instanceof Deno.errors.NotFound)) throw failure;
      outputExists = false;
      error = [error, "Benchmark produced no output directory"].filter(Boolean)
        .join("; ");
    }
    const archive = async (directory: string, relative = "") => {
      for await (const entry of Deno.readDir(directory)) {
        const path = join(directory, entry.name);
        const label = relative ? `${relative}/${entry.name}` : entry.name;
        if (entry.isSymlink) {
          throw new Error("Benchmark artifacts must not contain symlinks");
        }
        if (entry.isDirectory) await archive(path, label);
        else if (["server.key", "server.p12"].includes(label)) {
          omitted.push(`${batch}/${label}`);
        } else if (entry.isFile) {
          const handle = await archiveFile(
            store,
            "benchmarkFile",
            `${prefix}-file-${files.length + 1}`,
            path,
          );
          if (handle) {
            const reference = { name: handle.name, version: handle.version };
            handles.push(reference);
            files.push({ path: `${batch}/${label}`, reference });
          }
        } else throw new Error("Unexpected benchmark artifact type");
      }
    };
    if (outputExists) await archive(output);
    try {
      if (!execution?.success || error !== null) {
        throw new Error(
          error ?? execution?.error ?? "Benchmark command or cleanup failed",
        );
      }
      const after = await inspectCheckout(checkout.path);
      if (
        after.revision !== checkout.revision ||
        (request.purpose === "measurement" && after.dirty)
      ) {
        throw new Error("Source changed during benchmark");
      }
      if (await hashFile(native) !== expected.nativeSha256) {
        throw new Error("Native library changed during benchmark");
      }
      await validatePlacementEvidence(
        output,
        Array.from(
          { length: request.parameters.repetitions },
          (_, index) =>
            join(output, request.adapter, request.protocol, String(index + 1)),
        ),
        request.parameters.warmup,
        request.parameters.duration,
        placement.realH2load,
      );
      const measured = validateBenchmark(
        JSON.parse(await Deno.readTextFile(join(output, "results.json"))),
        expected,
      );
      if (
        summary && (summary.javaVersion !== measured.javaVersion ||
          summary.h2loadVersion !== measured.h2loadVersion ||
          JSON.stringify(summary.parameters) !==
            JSON.stringify(measured.parameters))
      ) {
        throw new Error("Repeat batch environment differs");
      }
      const samples: number[] = [
        ...(summary?.samples ?? []),
        ...measured.samples,
      ];
      const scale = Math.max(...samples);
      const total = samples.reduce((sum, rate) => sum + rate, 0);
      const mean = Number.isFinite(total)
        ? total / samples.length
        : samples.reduce((sum, rate) => sum + rate / scale, 0) /
          samples.length * scale;
      summary = { ...measured, samples, diagnosticRate: mean };
      batches.push({ output, samples: measured.samples });
      if (
        request.deadlineMs !== undefined && Date.now() >= request.deadlineMs
      ) {
        inconclusive = true;
        break;
      }
      if (request.purpose === "smoke") break;
      if (
        batch === 1 && request.deadlineMs !== undefined &&
        request.deadlineMs - Date.now() <
          request.parameters.repetitions *
                (request.parameters.warmup + request.parameters.duration) *
                1000 + 10000
      ) {
        inconclusive = true;
        break;
      }
    } catch (failure) {
      summary = null;
      error = String(failure);
      break;
    }
  }
  if (request.deadlineMs !== undefined && Date.now() >= request.deadlineMs) {
    inconclusive = true;
  }
  let pooled: ReturnType<typeof precisionSummary> | null = null;
  if (summary && request.purpose === "measurement" && batches.length === 2) {
    try {
      pooled = precisionSummary(summary.samples, request.precision);
    } catch (failure) {
      summary = null;
      error = String(failure);
    }
  }
  const precisionFailure = request.purpose === "measurement" &&
    error === null &&
    batches.length === 2 && pooled !== null &&
    !meetsPrecision(pooled, request.precision);
  if (
    request.purpose === "measurement" &&
    (batches.length !== 2 || precisionFailure)
  ) {
    inconclusive = true;
  }
  const valid = summary !== null && !inconclusive && error === null &&
    (request.purpose === "smoke" || pooled !== null);
  const finalSummary = summary && pooled
    ? {
      ...summary,
      diagnosticRate: pooled.mean,
      score: valid ? pooled.mean : null,
      samples: pooled.rates,
      sampleSD: pooled.sampleSD,
      tCritical: pooled.tCritical,
      ciHalfWidth: pooled.ciHalfWidth,
      relativeHalfWidth: pooled.relativeHalfWidth,
    }
    : summary && { ...summary, score: null };
  const saved = await store.writeResource("benchmark", `${base}-result`, {
    runId: request.runId,
    purpose: request.purpose,
    protocol: request.protocol,
    adapter: request.adapter,
    revision: checkout.revision,
    nativeSha256: expected.nativeSha256,
    hostname: Deno.hostname(),
    method: benchmarkMethod,
    scorePolicyVersion,
    precision: request.precision,
    plannedSamples: request.purpose === "measurement"
      ? 2 * request.parameters.repetitions
      : 1,
    placement: {
      topology: placement.topology,
      realH2load: placement.realH2load,
    },
    valid,
    inconclusive,
    error,
    summary: finalSummary,
    batches,
    files,
    omitted,
    exitCode: execution?.status?.code ?? null,
    cleanup: execution?.cleanup ?? null,
    logs,
  });
  return {
    valid,
    inconclusive,
    precisionFailure,
    saved,
    dataHandles: [...handles, saved],
  };
}
