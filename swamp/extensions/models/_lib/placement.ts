import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "npm:zod@4.4.3";

export const benchmarkMethod = "pinned-server-10-11-client-12-13" as const;
export const placementProvenanceSchema = z.strictObject({
  topology: z.array(z.strictObject({
    cpu: z.number().int(),
    core: z.string(),
    package: z.string(),
    l3: z.string(),
    l3Size: z.string(),
    siblings: z.string(),
  })),
  realH2load: z.string().min(1),
});
const serverMask = "10-11";
const clientMask = "12-13";
const cpus = [10, 11, 12, 13];
const wrapper = fileURLToPath(
  new URL("../../../clojure/h2load", import.meta.url),
);

const hostFile = async (path: string) => {
  const result = await new Deno.Command("cat", { args: [path] }).output();
  if (!result.success) {
    throw new Error(`Cannot read host CPU metadata: ${path}`);
  }
  return new TextDecoder().decode(result.stdout).trim();
};
export async function placementEnvironment(directory: string) {
  if (Deno.build.os !== "linux") {
    throw new Error("Pinned benchmark requires Linux");
  }
  const allowed = /^Cpus_allowed_list:\s*(\S+)/m.exec(
    await hostFile("/proc/self/status"),
  )?.[1];
  if (!allowed) throw new Error("Cannot read permitted CPUs");
  const permitted = new Set<number>();
  for (const range of allowed.split(",")) {
    const [start, end = start] = range.split("-").map(Number);
    for (let cpu = start; cpu <= end; cpu++) permitted.add(cpu);
  }
  if (!cpus.every((cpu) => permitted.has(cpu))) {
    throw new Error("Benchmark CPUs are not permitted by this process");
  }
  const topology = await Promise.all(cpus.map(async (cpu) => {
    const base = `/sys/devices/system/cpu/cpu${cpu}`;
    const read = (name: string) => hostFile(join(base, name));
    return {
      cpu,
      core: await read("topology/core_id"),
      package: await read("topology/physical_package_id"),
      l3: await read("cache/index3/id"),
      l3Size: await read("cache/index3/size"),
      siblings: await read("topology/thread_siblings_list"),
    };
  }));
  if (
    new Set(topology.map((cpu) => `${cpu.package}/${cpu.core}`)).size !== 4 ||
    new Set(topology.map((cpu) => `${cpu.package}/${cpu.l3}`)).size !== 1 ||
    topology.some((cpu) =>
      cpu.l3Size !== "32768K" ||
      cpu.siblings !== `${cpu.cpu},${cpu.cpu + 16}`
    )
  ) {
    throw new Error(
      "Benchmark cores are not distinct physical cores on the expected L3",
    );
  }
  const found = await new Deno.Command("bash", {
    args: ["-c", "command -v h2load"],
  }).output();
  if (!found.success) throw new Error("Real h2load is unavailable");
  const realH2load = await Deno.realPath(
    new TextDecoder().decode(found.stdout).trim(),
  );
  if (realH2load === wrapper || !(await Deno.stat(wrapper)).isFile) {
    throw new Error("Reviewed h2load wrapper is unavailable");
  }
  return {
    topology,
    realH2load,
    env: {
      PATH: `${dirname(wrapper)}:${Deno.env.get("PATH") ?? ""}`,
      TEMPO_BENCHMARK_METHOD: benchmarkMethod,
      TEMPO_H2LOAD_REAL: realH2load,
      TEMPO_AFFINITY_LOG: join(directory, "client-affinity.tsv"),
    },
  };
}

export async function validatePlacementEvidence(
  directory: string,
  samples: string[],
  warmup: number,
  duration: number,
  realH2load: string,
) {
  const rows = (await Deno.readTextFile(join(directory, "client-affinity.tsv")))
    .trim().split("\n").map((row) => row.split("\t"));
  const commands = samples.flatMap(() => [`-D ${warmup} `, `-D ${duration} `]);
  if (
    rows.length !== commands.length ||
    rows.some((row, index) =>
      row.length !== 5 || !/^\d{4}-\d\d-\d\dT/.test(row[0]) ||
      !/^\d+$/.test(row[1]) || row[2] !== realH2load ||
      !/^\d+:12-13(?:,\d+:12-13)*$/.test(row[3]) ||
      !row[3].split(",").includes(`${row[1]}:${clientMask}`) ||
      !row[4].startsWith(commands[index])
    )
  ) throw new Error("Client affinity evidence is missing or inconsistent");
  for (const sample of samples) {
    for (const phase of ["ready", "measurement-start", "measurement-end"]) {
      const evidence = JSON.parse(
        await Deno.readTextFile(
          join(sample, `server-affinity-${phase}.json`),
        ),
      );
      if (
        evidence.method !== benchmarkMethod || evidence.phase !== phase ||
        !Array.isArray(evidence.threads) || evidence.threads.length === 0 ||
        evidence.threads.some((thread: { tid: number; allowed: string }) =>
          !Number.isInteger(thread.tid) || thread.allowed !== serverMask
        )
      ) throw new Error(`Server affinity evidence differs: ${sample}/${phase}`);
    }
  }
}
