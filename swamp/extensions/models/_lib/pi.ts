import { isAbsolute } from "node:path";
import { z } from "npm:zod@4.4.3";
import type { Policy } from "../../../pi/guard.ts";

export function piTools(stage: Policy["stage"]) {
  return stage === "implement" || stage === "repair"
    ? ["tempo_read", "tempo_edit", "tempo_write", "tempo_repl_eval"]
    : ["tempo_read"];
}

export function piArguments(stage: Policy["stage"], paths: {
  extension: string;
  policy: string;
  systemPrompt: string;
  taskPrompt: string;
  manifest: string;
}) {
  if (Object.values(paths).some((path) => !isAbsolute(path))) {
    throw new Error("Pi input paths must be absolute");
  }
  return [
    "--provider",
    "openai-codex",
    "--model",
    "gpt-6-astra",
    "--thinking",
    "medium",
    "--mode",
    "json",
    "--no-session",
    "--no-approve",
    "--no-context-files",
    "--no-extensions",
    "--no-skills",
    "--no-prompt-templates",
    "--no-themes",
    "--no-builtin-tools",
    "--tools",
    piTools(stage).join(","),
    "--extension",
    paths.extension,
    "--tempo-policy",
    paths.policy,
    "--system-prompt",
    paths.systemPrompt,
    "--",
    `@${paths.taskPrompt}`,
    `@${paths.manifest}`,
  ];
}

const eventSchema = z.looseObject({ type: z.string() });
const policySchema = z.strictObject({
  workspace: z.string(),
  readRoots: z.array(z.string()),
  stage: z.enum(["analyze", "research", "implement", "repair", "review"]),
});
const assistantSchema = z.looseObject({
  role: z.literal("assistant"),
  provider: z.string(),
  model: z.string(),
  stopReason: z.string(),
  content: z.array(
    z.looseObject({ type: z.string(), text: z.string().optional() }),
  ),
  usage: z.looseObject({ totalTokens: z.number().int().nonnegative() }),
});

export function parsePiEvents(text: string, expected: Policy) {
  if (!text.endsWith("\n")) throw new Error("Incomplete Pi JSONL stream");
  let header = false;
  let guarded = false;
  let settled = false;
  let last: z.infer<typeof assistantSchema> | null = null;
  let modelTurns = 0;
  let totalTokens = 0;
  for (const line of text.slice(0, -1).split("\n")) {
    let event: z.infer<typeof eventSchema>;
    try {
      event = eventSchema.parse(
        JSON.parse(line.endsWith("\r") ? line.slice(0, -1) : line),
      );
    } catch (cause) {
      throw new Error("Invalid Pi JSONL record", { cause });
    }
    if (!header && event.type !== "session") {
      throw new Error("Pi session header is missing");
    }
    if (event.type === "session") {
      if (header) throw new Error("Multiple Pi sessions in one step");
      header = true;
    }
    if (event.type === "entry_appended") {
      const entry = z.looseObject({
        customType: z.string().optional(),
        data: z.unknown(),
      }).parse(event.entry);
      if (entry.customType === "tempo-policy") {
        const actual = policySchema.parse(entry.data);
        if (
          actual.workspace !== expected.workspace ||
          actual.stage !== expected.stage ||
          JSON.stringify(actual.readRoots) !==
            JSON.stringify(expected.readRoots)
        ) throw new Error("Pi loaded a different file policy");
        guarded = true;
      }
    }
    if (event.type === "tool_execution_start") {
      if (
        !guarded || !piTools(expected.stage).includes(String(event.toolName))
      ) throw new Error("Pi used a tool outside the assigned policy");
    }
    if (event.type === "agent_start") settled = false;
    if (event.type === "agent_settled") settled = true;
    if (event.type === "message_end") {
      const message = z.looseObject({ role: z.string() }).parse(event.message);
      if (message.role === "assistant") {
        last = assistantSchema.parse(message);
        if (last.provider !== "openai-codex" || last.model !== "gpt-6-astra") {
          throw new Error("Pi used a different provider or model");
        }
        modelTurns++;
        totalTokens += last.usage.totalTokens;
        settled = false;
      }
    }
  }
  if (
    !header || !guarded || !settled || last === null ||
    last.stopReason !== "stop"
  ) throw new Error("Pi did not finish a successful guarded response");
  if (
    last.content.some((block) => !["text", "thinking"].includes(block.type))
  ) throw new Error("Pi ended with unfinished tool work");
  const response = last.content.filter((block) => block.type === "text").map(
    (block) => {
      if (block.text === undefined) {
        throw new Error("Pi text block has no text");
      }
      return block.text;
    },
  ).join("");
  if (!response.trim()) throw new Error("Pi response is empty");
  return { response, modelTurns, totalTokens };
}
