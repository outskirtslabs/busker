export interface ProcessCounters {
  pid: number;
  name: string;
  startTicks: number;
  userTicks: number;
  systemTicks: number;
  voluntarySwitches: number | null;
  involuntarySwitches: number | null;
  schedulerRunNs: number | null;
  schedulerWaitNs: number | null;
  allowedCpus: string | null;
}

function count(value: string | undefined, field: string): number {
  if (value === undefined || !/^\d+$/.test(value)) {
    throw new Error(`Missing or invalid ${field}`);
  }
  const result = Number(value);
  if (!Number.isSafeInteger(result)) {
    throw new Error(`${field} exceeds safe integer range`);
  }
  return result;
}

export function parseProcess(
  stat: string,
  status: string | null,
  schedstat: string | null,
): ProcessCounters {
  const open = stat.indexOf("(");
  const close = stat.lastIndexOf(")");
  if (open < 1 || close <= open) throw new Error("Invalid process stat");
  const fields = stat.slice(close + 1).trim().split(/\s+/);
  const statuses = new Map(
    (status ?? "").split("\n").map((line) => {
      const colon = line.indexOf(":");
      return [line.slice(0, colon), line.slice(colon + 1).trim()];
    }),
  );
  const statusCount = (key: string) =>
    statuses.has(key) ? count(statuses.get(key), key) : null;
  const scheduler = schedstat?.trim().split(/\s+/);
  return {
    pid: count(stat.slice(0, open).trim(), "pid"),
    name: stat.slice(open + 1, close),
    startTicks: count(fields[19], "startTicks"),
    userTicks: count(fields[11], "userTicks"),
    systemTicks: count(fields[12], "systemTicks"),
    voluntarySwitches: statusCount("voluntary_ctxt_switches"),
    involuntarySwitches: statusCount("nonvoluntary_ctxt_switches"),
    schedulerRunNs: scheduler ? count(scheduler[0], "schedulerRunNs") : null,
    schedulerWaitNs: scheduler ? count(scheduler[1], "schedulerWaitNs") : null,
    allowedCpus: statuses.get("Cpus_allowed_list") ?? null,
  };
}

export function parseCpuStat(text: string): Record<string, number> {
  const result: Record<string, number> = {};
  for (const line of text.trim().split("\n")) {
    const fields = line.trim().split(/\s+/);
    if (fields.length !== 2 || Object.hasOwn(result, fields[0])) {
      throw new Error("Invalid cgroup CPU stat");
    }
    result[fields[0]] = count(fields[1], fields[0]);
  }
  if (!Object.hasOwn(result, "usage_usec")) {
    throw new Error("Missing cgroup CPU usage");
  }
  return result;
}

export function parsePressure(
  text: string,
): Record<
  string,
  { totalUs: number; avg10: number; avg60: number; avg300: number }
> {
  const result: Record<
    string,
    { totalUs: number; avg10: number; avg60: number; avg300: number }
  > = {};
  for (const line of text.trim().split("\n")) {
    const [kind, ...pairs] = line.trim().split(/\s+/);
    if (!["some", "full"].includes(kind) || Object.hasOwn(result, kind)) {
      throw new Error("Invalid CPU pressure row");
    }
    const values = new Map(pairs.map((pair) => {
      const match = /^(avg10|avg60|avg300|total)=([0-9.]+)$/.exec(pair);
      if (!match) throw new Error("Invalid CPU pressure value");
      return [match[1], match[2]];
    }));
    if (values.size !== 4 || pairs.length !== 4) {
      throw new Error("Missing or duplicate CPU pressure values");
    }
    const average = (name: string) => {
      const value = Number(values.get(name));
      if (!Number.isFinite(value) || value < 0 || value > 100) {
        throw new Error("Invalid pressure average");
      }
      return value;
    };
    result[kind] = {
      totalUs: count(values.get("total"), "pressure total"),
      avg10: average("avg10"),
      avg60: average("avg60"),
      avg300: average("avg300"),
    };
  }
  return result;
}

function delta(
  before: number | null | undefined,
  after: number | null | undefined,
) {
  if (before == null || after == null) return null;
  if (after < before) {
    throw new Error("Counter decreased; samples are not comparable");
  }
  return after - before;
}

export function processCost(
  before: ProcessCounters,
  after: ProcessCounters,
  ticksPerSecond: number,
  requests: number,
) {
  if (before.pid !== after.pid || before.startTicks !== after.startTicks) {
    throw new Error("Process identity changed");
  }
  if (
    !Number.isSafeInteger(ticksPerSecond) || ticksPerSecond <= 0 ||
    !Number.isSafeInteger(requests) || requests <= 0
  ) {
    throw new Error("Expected positive clock frequency and request count");
  }
  const userSeconds = delta(before.userTicks, after.userTicks)! /
    ticksPerSecond;
  const systemSeconds = delta(before.systemTicks, after.systemTicks)! /
    ticksPerSecond;
  return {
    pid: before.pid,
    startTicks: before.startTicks,
    userSeconds,
    systemSeconds,
    cpuMicrosPerRequest: (userSeconds + systemSeconds) * 1_000_000 / requests,
    voluntarySwitches: delta(before.voluntarySwitches, after.voluntarySwitches),
    involuntarySwitches: delta(
      before.involuntarySwitches,
      after.involuntarySwitches,
    ),
    schedulerRunNs: delta(before.schedulerRunNs, after.schedulerRunNs),
    schedulerWaitNs: delta(before.schedulerWaitNs, after.schedulerWaitNs),
  };
}

export async function captureProcess(pid: number) {
  if (!Number.isSafeInteger(pid) || pid <= 0) {
    throw new Error("Invalid process id");
  }
  const missing: string[] = [];
  async function optional(path: string) {
    try {
      return await Deno.readTextFile(path);
    } catch (error) {
      if (
        error instanceof Deno.errors.NotFound ||
        error instanceof Deno.errors.PermissionDenied
      ) {
        missing.push(path);
        return null;
      }
      throw error;
    }
  }
  async function sample(base: string) {
    const stat = await optional(`${base}/stat`);
    if (stat === null) return null;
    return parseProcess(
      stat,
      await optional(`${base}/status`),
      await optional(`${base}/schedstat`),
    );
  }
  const startedAt = new Date().toISOString();
  const process = await sample(`/proc/${pid}`);
  if (process === null) {
    throw new Error("Target process is absent or unreadable");
  }
  const threads: ProcessCounters[] = [];
  for await (const entry of Deno.readDir(`/proc/${pid}/task`)) {
    if (!/^\d+$/.test(entry.name)) continue;
    const thread = await sample(`/proc/${pid}/task/${entry.name}`);
    if (thread !== null) threads.push(thread);
  }
  const cgroup = await Deno.readTextFile(`/proc/${pid}/cgroup`);
  const unified = /^0::(\/[^\r\n]*)$/m.exec(cgroup)?.[1];
  if (
    !unified || unified.split("/").some((part) => part === ".." || part === ".")
  ) throw new Error("Unified process cgroup is unavailable");
  const base = `/sys/fs/cgroup${unified}`;
  const cpu = await optional(`${base}/cpu.stat`);
  const pressure = await optional(`${base}/cpu.pressure`);
  const memoryEvents = await optional(`${base}/memory.events`);
  const finalProcess = await sample(`/proc/${pid}`);
  if (!finalProcess || finalProcess.startTicks !== process.startTicks) {
    throw new Error("Target process changed during capture");
  }
  return {
    startedAt,
    finishedAt: new Date().toISOString(),
    process,
    threads: threads.sort((a, b) => a.pid - b.pid),
    cgroupPath: unified,
    cgroupCpu: cpu === null ? null : parseCpuStat(cpu),
    cgroupPressure: pressure === null ? null : parsePressure(pressure),
    memoryEvents,
    missing,
  };
}
