import { deepStrictEqual, throws } from "node:assert/strict";
import { parsePiEvents, piArguments } from "../extensions/models/_lib/pi.ts";

const policy = {
  workspace: "/work",
  readRoots: ["/evidence"],
  stage: "analyze" as const,
};
const message = {
  role: "assistant",
  provider: "openai-codex",
  model: "gpt-6-astra",
  stopReason: "stop",
  content: [{
    type: "text",
    text: '{"action":"stop","reason":"done\u2028now"}',
  }],
  usage: { totalTokens: 42 },
};
function stream(events: unknown[]) {
  return events.map((event) => JSON.stringify(event)).join("\n") + "\n";
}
const prefix = [
  { type: "session", version: 3 },
  {
    type: "entry_appended",
    entry: { type: "custom", customType: "tempo-policy", data: policy },
  },
  { type: "agent_start" },
];
const suffix = [{ type: "message_end", message }, {
  type: "agent_end",
  willRetry: false,
}, { type: "agent_settled" }];

Deno.test("Pi invocation disables built-ins, discovery, inherited instructions, and old sessions", () => {
  const paths = {
    extension: "/tools/restricted.js",
    policy: "/inputs/policy.json",
    systemPrompt: "/prompts/system.md",
    taskPrompt: "/prompts/task.md",
    manifest: "/inputs/manifest.json",
  };
  const args = piArguments("review", paths);
  deepStrictEqual(args.slice(0, 6), [
    "--provider",
    "openai-codex",
    "--model",
    "gpt-6-astra",
    "--thinking",
    "medium",
  ]);
  for (
    const flag of [
      "--no-builtin-tools",
      "--no-extensions",
      "--no-context-files",
      "--no-approve",
      "--no-session",
    ]
  ) deepStrictEqual(args.includes(flag), true);
  deepStrictEqual(args[args.indexOf("--tools") + 1], "tempo_read");
  const editing = piArguments("repair", paths);
  deepStrictEqual(
    editing[editing.indexOf("--tools") + 1],
    "tempo_read,tempo_edit,tempo_write,tempo_repl_eval",
  );
  deepStrictEqual(
    piArguments("implement", paths)[editing.indexOf("--tools") + 1],
    "tempo_read,tempo_edit,tempo_write,tempo_repl_eval",
  );
  throws(
    () => piArguments("analyze", { ...paths, policy: "relative" }),
    /absolute/,
  );
});

Deno.test("JSONL framing preserves Unicode separators and returns only authoritative final text", () => {
  deepStrictEqual(parsePiEvents(stream([...prefix, ...suffix]), policy), {
    response: message.content[0].text,
    modelTurns: 1,
    totalTokens: 42,
  });
});

Deno.test("zero process exit cannot make an aborted, truncated, unguarded, or unsettled response valid", () => {
  for (const reason of ["error", "aborted", "length", "deferred", "toolUse"]) {
    throws(
      () =>
        parsePiEvents(
          stream([...prefix, {
            type: "message_end",
            message: { ...message, stopReason: reason },
          }, { type: "agent_settled" }]),
          policy,
        ),
      /did not finish/,
    );
  }
  throws(
    () => parsePiEvents(stream([...prefix, ...suffix]).trimEnd(), policy),
    /Incomplete/,
  );
  throws(
    () => parsePiEvents(stream([{ type: "session" }, ...suffix]), policy),
    /guarded/,
  );
  throws(
    () => parsePiEvents(stream([...prefix, ...suffix.slice(0, -1)]), policy),
    /did not finish/,
  );
  throws(() => parsePiEvents("not-json\n", policy), /Invalid/);
});

Deno.test("wrong model, wrong policy, and forbidden tools invalidate the step", () => {
  throws(
    () =>
      parsePiEvents(
        stream([
          ...prefix,
          { type: "tool_execution_start", toolName: "bash" },
          ...suffix,
        ]),
        policy,
      ),
    /outside/,
  );
  throws(
    () =>
      parsePiEvents(stream([...prefix, ...suffix]), {
        ...policy,
        workspace: "/another",
      }),
    /different file policy/,
  );
  throws(
    () =>
      parsePiEvents(
        stream([...prefix, {
          type: "message_end",
          message: { ...message, model: "other" },
        }, { type: "agent_settled" }]),
        policy,
      ),
    /different provider or model/,
  );
});
