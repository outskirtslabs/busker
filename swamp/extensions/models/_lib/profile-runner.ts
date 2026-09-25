import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { z } from "npm:zod@4.4.3";
import { archiveDirectory, type ArtifactStore } from "./artifacts.ts";
import { hashFile } from "./benchmark-runner.ts";
import {
  benchmarkParametersSchema,
  protocolSchema,
  resourceURL,
} from "./benchmark.ts";
import {
  parseCpuStat,
  parsePressure,
  parseProcess,
  processCost,
} from "./counters.ts";
import { runCommand } from "./process.ts";
import {
  benchmarkMethod,
  placementEnvironment,
  placementProvenanceSchema,
  validatePlacementEvidence,
} from "./placement.ts";
import { summarizeProfile } from "./profile.ts";
import { revisionSchema, runIdSchema } from "./schemas.ts";
import { inspectCheckout } from "./workspace.ts";

export const profileRequestSchema = z.strictObject({
  runId: runIdSchema,
  repository: z.string().min(1),
  directory: z.string().min(1),
  collectorDirectory: z.string().min(1),
  protocol: protocolSchema,
  event: z.enum(["ctimer", "wall", "alloc", "jfr"]),
  smoke: z.boolean(),
  parameters: benchmarkParametersSchema.omit({ repetitions: true }),
});
export const profileRecordSchema = z.strictObject({
  runId: runIdSchema,
  revision: revisionSchema,
  event: profileRequestSchema.shape.event,
  protocol: protocolSchema,
  method: z.literal(benchmarkMethod),
  placement: placementProvenanceSchema,
  diagnosticOnly: z.literal(true),
  score: z.null(),
  valid: z.boolean(),
  error: z.string().nullable(),
  nativeSha256: z.string(),
  hostname: z.string(),
  summary: z.record(z.string(), z.json()).nullable(),
  exitCode: z.number().int().nullable(),
  cleanup: z.enum(["absent", "stopped", "failed"]).nullable(),
  files: z.array(
    z.strictObject({
      path: z.string(),
      reference: z.strictObject({
        name: z.string(),
        version: z.number().int().positive(),
      }),
    }),
  ),
  omitted: z.array(z.string()),
});
const rawProcessSchema = z.object({
  stat: z.string().nullable(),
  status: z.string().nullable(),
  schedstat: z.string().nullable(),
});
const snapshotSchema = z.object({
  "captured-at-ms": z.number().int().positive(),
  "allocated-bytes": z.number().int().nonnegative().nullable(),
  process: rawProcessSchema,
  threads: z.record(z.string(), rawProcessSchema),
  "host-pressure": z.string().nullable(),
  "cgroup-path": z.string().nullable(),
  "cgroup-stat": z.string().nullable(),
  "cgroup-pressure": z.string().nullable(),
});
const reportSchema = z.object({
  kind: z.literal("profile"),
  score: z.null(),
  event: profileRequestSchema.shape.event,
  protocol: protocolSchema,
  parameters: profileRequestSchema.shape.parameters,
  "profiler-version": z.literal("1.6.2"),
  "clock-ticks-per-second": z.number().int().positive(),
  load: z.object({
    requests: z.number().int().positive(),
    "requests-per-second": z.number().positive(),
  }),
  environment: z.object({
    "busker-mode": z.literal("local"),
    "busker-checkout": z.string(),
    "busker-revision": revisionSchema,
    "busker-dirty?": z.boolean(),
    "native-version": z.null(),
    "native-sha256": z.string(),
    "native-resource": z.string(),
    "busker-resource": z.string(),
    "java-version": z.string(),
    "h2load-version": z.string().min(1),
    "jvm-options": z.array(z.string()),
    processors: z.literal(2),
    "max-heap-bytes": z.literal(2147483648),
    "native-mappings": z.array(z.string()).min(1),
    "garbage-collectors": z.array(z.string()).min(1),
  }),
});

export function validateProfile(
  reportValue: unknown,
  beforeValue: unknown,
  afterValue: unknown,
  clientValue: unknown,
  recording: unknown,
  request: z.infer<typeof profileRequestSchema>,
  revision: string,
  nativeSha256: string,
  native: string,
) {
  const report = reportSchema.parse(reportValue);
  const before = snapshotSchema.parse(beforeValue);
  const after = snapshotSchema.parse(afterValue);
  const environment = report.environment;
  if (
    report.event !== request.event || report.protocol !== request.protocol ||
    Object.entries(request.parameters).some(([key, value]) =>
      report.parameters[key as keyof typeof report.parameters] !== value
    )
  ) throw new Error("Profile workload does not match");
  if (
    environment["busker-checkout"] !== request.repository ||
    environment["busker-revision"] !== revision ||
    environment["native-sha256"] !== nativeSha256 ||
    resourceURL(environment["native-resource"]) !==
      pathToFileURL(native).href ||
    resourceURL(environment["busker-resource"]) !==
      pathToFileURL(join(request.repository, "src/main/clojure/ol/busker.clj"))
        .href ||
    (!request.smoke && environment["busker-dirty?"])
  ) throw new Error("Profile source or native identity does not match");
  for (
    const flag of [
      "-Xms512m",
      "-Xmx2g",
      "-XX:ActiveProcessorCount=2",
      `-Dol.libh2oclj.path=${native}`,
    ]
  ) {
    if (!environment["jvm-options"].includes(flag)) {
      throw new Error(`Missing profile JVM option: ${flag}`);
    }
  }
  if (
    after["captured-at-ms"] < before["captured-at-ms"] ||
    before["cgroup-path"] !== after["cgroup-path"]
  ) throw new Error("Profile counter context changed");
  const parse = (raw: z.infer<typeof rawProcessSchema>) => {
    if (raw.stat === null) throw new Error("Missing process counters");
    return parseProcess(raw.stat, raw.status, raw.schedstat);
  };
  const requests = report.load.requests;
  const ticks = report["clock-ticks-per-second"];
  if (parse(before.process).allowedCpus !== parse(after.process).allowedCpus) {
    throw new Error("Server CPU affinity changed during recording");
  }
  if (
    !environment["native-mappings"].some((line) => line.endsWith(` ${native}`))
  ) throw new Error("Expected native file is absent from live mappings");
  const serverCpu = processCost(
    parse(before.process),
    parse(after.process),
    ticks,
    requests,
  );
  const threads = [];
  const unavailableThreads = [];
  for (
    const tid of new Set([
      ...Object.keys(before.threads),
      ...Object.keys(after.threads),
    ])
  ) {
    const a = before.threads[tid], b = after.threads[tid];
    if (!a?.stat || !b?.stat) {
      unavailableThreads.push(tid);
      continue;
    }
    const first = parse(a), last = parse(b);
    if (first.startTicks !== last.startTicks) {
      unavailableThreads.push(tid);
      continue;
    }
    threads.push({
      name: first.name,
      ...processCost(first, last, ticks, requests),
    });
  }
  const clientCpu = z.strictObject({
    userSeconds: z.number().nonnegative(),
    systemSeconds: z.number().nonnegative(),
    voluntarySwitches: z.number().int().nonnegative(),
    involuntarySwitches: z.number().int().nonnegative(),
  }).parse(clientValue);
  const context = (snapshot: z.infer<typeof snapshotSchema>) => ({
    capturedAtMs: snapshot["captured-at-ms"],
    cgroupPath: snapshot["cgroup-path"],
    cgroupCpu: snapshot["cgroup-stat"] === null
      ? null
      : parseCpuStat(snapshot["cgroup-stat"]),
    cgroupPressure: snapshot["cgroup-pressure"] === null
      ? null
      : parsePressure(snapshot["cgroup-pressure"]),
    hostPressure: snapshot["host-pressure"] === null
      ? null
      : parsePressure(snapshot["host-pressure"]),
  });
  const allocatedBytes =
    before["allocated-bytes"] === null || after["allocated-bytes"] === null
      ? null
      : after["allocated-bytes"] - before["allocated-bytes"];
  if (allocatedBytes !== null && allocatedBytes < 0) {
    throw new Error("Allocation counter decreased");
  }
  let jfr: { events: number; eventCounts: Record<string, number> } | null =
    null;
  if (report.event === "jfr") {
    const data = z.object({
      recording: z.object({
        events: z.array(
          z.object({
            type: z.string().min(1),
            values: z.record(z.string(), z.json()),
          }),
        ).min(1),
      }),
    }).parse(recording);
    const eventCounts: Record<string, number> = Object.create(null);
    for (const event of data.recording.events) {
      eventCounts[event.type] = (eventCounts[event.type] ?? 0) + 1;
    }
    jfr = { events: data.recording.events.length, eventCounts };
  }
  return {
    stacks: report.event === "jfr" ? null : summarizeProfile({
      event: report.event,
      completedRequests: requests,
      collapsed: z.string().parse(recording),
    }),
    jfr,
    allocatedBytes,
    allocatedBytesPerRequest: allocatedBytes === null
      ? null
      : allocatedBytes / requests,
    serverCpu,
    threads,
    unavailableThreads,
    clientCpu,
    before: context(before),
    after: context(after),
    environment,
    parameters: report.parameters,
    profilerVersion: report["profiler-version"],
    limitations: [
      "JVM-reported allocated bytes include collector work, not native allocations.",
      "Absent JFR events do not prove absence of contention or pinning.",
      "Counters include load-generator startup and collector overhead.",
      "Main-thread status and scheduler counters are not process-wide totals.",
      "Cgroup counters can include both server and client; process CPU is reported separately.",
      "Client CPU seconds have GNU time's reporting precision.",
    ],
  };
}

export async function runProfile(
  store: ArtifactStore,
  input: z.infer<typeof profileRequestSchema>,
) {
  const request = profileRequestSchema.parse(input);
  if (!Deno.env.get("IN_NIX_SHELL") || Deno.build.os !== "linux") {
    throw new Error("Profiling requires the Linux Nix devshell");
  }
  const checkout = await inspectCheckout(request.repository);
  request.repository = checkout.path;
  if (!request.smoke && checkout.dirty) {
    throw new Error("Cannot profile dirty source");
  }
  if (
    request.smoke &&
    (request.parameters.warmup !== 1 || request.parameters.duration !== 1)
  ) throw new Error("Profile smoke checks require one-second windows");
  if (
    !isAbsolute(request.directory) ||
    join(
        await Deno.realPath(dirname(request.directory)),
        basename(request.directory),
      ) !== resolve(request.directory)
  ) {
    throw new Error(
      "Profile directory must be absolute without symlink traversal",
    );
  }
  if (!isAbsolute(request.collectorDirectory)) {
    throw new Error("Collector directory must be absolute");
  }
  const arch = Deno.build.arch === "x86_64" ? "x86-64" : Deno.build.arch;
  const platform = `linux-${arch}`;
  const native = join(
    checkout.path,
    "shim",
    platform,
    "resources",
    platform,
    "libh2oclj.so",
  );
  const nativeSha256 = await hashFile(native);
  const collector = join(
    resolve(request.collectorDirectory),
    "tempo/profile.clj",
  );
  if (await Deno.realPath(collector) !== collector) {
    throw new Error("Collector source must not traverse symlinks");
  }
  await Deno.mkdir(request.directory, { mode: 0o700 });
  const support = join(request.directory, "support");
  await Deno.mkdir(join(support, "tempo"), { recursive: true, mode: 0o700 });
  await Deno.copyFile(
    join(request.collectorDirectory, "tempo/profile.clj"),
    join(support, "tempo/profile.clj"),
  );
  const prefix = `${request.runId}-${request.protocol}-${request.event}`;
  const options = `{:directory ${
    JSON.stringify(request.directory)
  } :protocol :${request.protocol} :event :${request.event} ${
    Object.entries(request.parameters).map(([k, v]) => `:${k} ${v}`).join(" ")
  }}`;
  const requestFile = join(request.directory, "request.edn");
  await Deno.writeTextFile(requestFile, options, {
    createNew: true,
    mode: 0o600,
  });
  const flags = [
    "-Xms512m",
    "-Xmx2g",
    "-XX:ActiveProcessorCount=2",
    "-Djdk.attach.allowAttachSelf",
    "-XX:+EnableDynamicAgentLoading",
    `-Dol.libh2oclj.path=${native}`,
    `-Dclj-async-profiler.output-dir=${request.directory}`,
  ];
  const config = `{:aliases {:tempo-profile {:extra-paths [${
    JSON.stringify(support)
  }] :extra-deps {com.clojure-goes-fast/clj-async-profiler {:mvn/version "1.6.2"}} :jvm-opts [${
    flags.map((flag) => JSON.stringify(flag)).join(" ")
  }] :main-opts ["-m" "tempo.profile"]}}}`;
  await Deno.writeTextFile(join(request.directory, "deps.edn"), config, {
    createNew: true,
    mode: 0o600,
  });
  const placement = await placementEnvironment(request.directory);
  const command = {
    id: prefix,
    command: "taskset",
    args: [
      "-c",
      "10,11",
      "env",
      "-u",
      "JAVA_TOOL_OPTIONS",
      "-u",
      "JDK_JAVA_OPTIONS",
      "-u",
      "_JAVA_OPTIONS",
      "-u",
      "CLJ_JVM_OPTS",
      "timeout",
      "--foreground",
      String(request.parameters.warmup + request.parameters.duration + 90),
      "clojure",
      "-Sthreads",
      "1",
      "-Sdeps",
      config,
      "-M:bench:bench-local:tempo-profile",
      requestFile,
    ],
    env: placement.env,
    cwd: checkout.path,
    stdoutPath: join(request.directory, "stdout.log"),
    stderrPath: join(request.directory, "stderr.log"),
  };
  let execution: Awaited<ReturnType<typeof runCommand>> | null = null;
  let error: string | null = null;
  try {
    execution = await runCommand(command);
  } catch (failure) {
    error = String(failure);
  }
  const archived = await archiveDirectory(
    store,
    "profileFile",
    prefix,
    request.directory,
  );
  let summary: ReturnType<typeof validateProfile> | null = null;
  try {
    if (!execution?.success || error) {
      throw new Error(
        error ?? execution?.error ?? "Profile command or cleanup failed",
      );
    }
    const after = await inspectCheckout(checkout.path);
    if (
      after.revision !== checkout.revision || (!request.smoke && after.dirty) ||
      await hashFile(native) !== nativeSha256
    ) throw new Error("Source or native library changed during profile");
    await validatePlacementEvidence(
      request.directory,
      [request.directory],
      request.parameters.warmup,
      request.parameters.duration,
      placement.realH2load,
    );
    const json = async (name: string) =>
      JSON.parse(await Deno.readTextFile(join(request.directory, name)));
    summary = validateProfile(
      await json("profile.json"),
      await json("before.json"),
      await json("after.json"),
      await json("client-cpu.json"),
      request.event === "jfr"
        ? await json("jfr-events.json")
        : await Deno.readTextFile(join(request.directory, "profile.collapsed")),
      request,
      checkout.revision,
      nativeSha256,
      native,
    );
  } catch (failure) {
    error = String(failure);
  }
  const saved = await store.writeResource("profile", `${prefix}-result`, {
    runId: request.runId,
    revision: checkout.revision,
    event: request.event,
    protocol: request.protocol,
    method: benchmarkMethod,
    placement: {
      topology: placement.topology,
      realH2load: placement.realH2load,
    },
    diagnosticOnly: true,
    score: null,
    valid: summary !== null,
    error,
    nativeSha256,
    hostname: Deno.hostname(),
    summary,
    exitCode: execution?.status?.code ?? null,
    cleanup: execution?.cleanup ?? null,
    files: archived.files,
    omitted: archived.omitted,
  });
  return {
    valid: summary !== null,
    saved,
    dataHandles: [...archived.handles, saved],
  };
}
