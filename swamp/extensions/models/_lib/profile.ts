export type ProfileEvent = "cpu" | "ctimer" | "wall" | "alloc";
export type WeightUnit = "samples" | "allocation-weight";
export type ThreadRole =
  | "event-loop"
  | "virtual-carrier"
  | "virtual-thread"
  | "wake-notifier"
  | "lifecycle"
  | "gc"
  | "compiler"
  | "other-jvm"
  | "unattributed";

export interface ProfileInput {
  event: ProfileEvent;
  completedRequests: number;
  collapsed: string;
  top?: number;
}

function role(name: string | undefined): ThreadRole {
  if (name === undefined) return "unattributed";
  if (name.startsWith("h2o-evloop")) return "event-loop";
  if (name.startsWith("ForkJoinPool")) return "virtual-carrier";
  if (name.startsWith("VirtualThread")) return "virtual-thread";
  if (name.startsWith("busker-wake")) return "wake-notifier";
  if (name.startsWith("busker-lifecycl")) return "lifecycle";
  if (/^(GC Thread|G1 |ZGC|ZWorker)/.test(name)) return "gc";
  if (name.includes("Compiler")) return "compiler";
  return "other-jvm";
}

function add(counts: Map<string, number>, key: string, weight: number) {
  const sum = (counts.get(key) ?? 0) + weight;
  if (!Number.isSafeInteger(sum)) {
    throw new Error("Profile weight exceeds safe integer range");
  }
  counts.set(key, sum);
}

export function summarizeProfile(input: ProfileInput) {
  if (!["cpu", "ctimer", "wall", "alloc"].includes(input.event)) {
    throw new Error("Unsupported profile event");
  }
  if (
    !Number.isSafeInteger(input.completedRequests) ||
    input.completedRequests <= 0
  ) {
    throw new Error("completedRequests must be a positive safe integer");
  }
  const top = input.top ?? 20;
  if (!Number.isSafeInteger(top) || top < 1 || top > 100) {
    throw new Error("top must be an integer from 1 to 100");
  }
  const leaves = new Map<string, number>();
  const inclusive = new Map<string, number>();
  const roles = new Map<string, number>();
  const stacks = new Map<string, number>();
  const stackDetails = new Map<
    string,
    { threadRole: ThreadRole; frames: string[] }
  >();
  let totalWeight = 0;
  let stackLines = 0;
  let maxDepth = 0;
  let unknownFrameWeight = 0;
  let missingThreadWeight = 0;
  let overflowWeight = 0;
  let virtualThreadNativeWeight = 0;
  for (const [index, raw] of input.collapsed.split(/\r?\n/).entries()) {
    if (!raw.trim()) continue;
    const match = /^(\S.*) ([0-9]+)$/.exec(raw.trimEnd());
    if (!match) {
      throw new Error(`Malformed collapsed stack at line ${index + 1}`);
    }
    const weight = Number(match[2]);
    if (!Number.isSafeInteger(weight) || weight <= 0) {
      throw new Error(`Invalid profile weight at line ${index + 1}`);
    }
    const frames = match[1].split(";");
    if (frames.some((frame) => !frame.trim())) {
      throw new Error(`Empty frame at line ${index + 1}`);
    }
    const thread = /^\[(.+) tid=\d+\]$/.exec(frames[0]);
    if (thread) frames.shift();
    if (!frames.length) {
      throw new Error(`Thread without stack at line ${index + 1}`);
    }
    const threadRole = role(thread?.[1]);
    totalWeight += weight;
    if (!Number.isSafeInteger(totalWeight)) {
      throw new Error("Profile total exceeds safe integer range");
    }
    stackLines++;
    maxDepth = Math.max(maxDepth, frames.length);
    add(roles, threadRole, weight);
    add(leaves, frames[frames.length - 1], weight);
    for (const frame of new Set(frames)) add(inclusive, frame, weight);
    const stackKey = JSON.stringify([threadRole, frames]);
    add(stacks, stackKey, weight);
    stackDetails.set(stackKey, { threadRole, frames });
    if (!thread) missingThreadWeight += weight;
    if (
      frames.some((frame) => /\[unknown|unknown_Java|not_walkable/.test(frame))
    ) {
      unknownFrameWeight += weight;
    }
    if (frames.some((frame) => frame.includes("frame_buffer_overflow"))) {
      overflowWeight += weight;
    }
    const virtual = frames.some((frame) =>
      /java[/.]lang[/.]VirtualThread(?:[.$/]|$)/.test(frame)
    );
    const native = frames.some((frame) =>
      /DowncallStub|^clj_h2o_|^h2o_/.test(frame)
    );
    if (virtual && native) virtualThreadNativeWeight += weight;
  }
  if (!totalWeight) throw new Error("Profile has no samples");
  const metric = (weight: number) => ({
    weight,
    percent: weight / totalWeight * 100,
    perMillionRequests: weight / input.completedRequests * 1_000_000,
  });
  const ranked = (counts: Map<string, number>, limit: number) =>
    [...counts].sort(([a, x], [b, y]) => y - x || (a < b ? -1 : a > b ? 1 : 0))
      .slice(0, limit).map(([name, weight]) => ({ name, ...metric(weight) }));
  const unit: WeightUnit = input.event === "alloc"
    ? "allocation-weight"
    : "samples";
  return {
    kind: "profile-summary" as const,
    diagnosticOnly: true as const,
    event: input.event,
    unit,
    completedRequests: input.completedRequests,
    totalWeight,
    weightPerMillionRequests: metric(totalWeight).perMillionRequests,
    stackLines,
    maxDepth,
    quality: {
      unknownFrames: metric(unknownFrameWeight),
      missingThread: metric(missingThreadWeight),
      stackOverflow: metric(overflowWeight),
      virtualThreadNative: metric(virtualThreadNativeWeight),
    },
    threadRoles: ranked(roles, roles.size),
    leaves: ranked(leaves, top),
    inclusiveFrames: ranked(inclusive, top),
    completeStacks: ranked(stacks, top).map(({ name, ...weight }) => ({
      ...stackDetails.get(name)!,
      ...weight,
    })),
    limitations: [
      "Inclusive frame weights overlap; do not add them.",
      "A profile is diagnostic evidence, not a benchmark score.",
      "No observed native call does not prove virtual-thread safety.",
      ...(input.event === "alloc"
        ? ["Allocation weights are not exact bytes per request."]
        : []),
      ...(input.event === "wall"
        ? ["Wall samples include idle waits; they are not CPU time."]
        : []),
    ],
  };
}
