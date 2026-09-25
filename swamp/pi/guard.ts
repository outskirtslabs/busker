import { lstat, readFile, realpath } from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";

export interface Policy {
  workspace: string;
  readRoots: string[];
  stage: "analyze" | "research" | "implement" | "repair" | "review";
}
const stages = ["analyze", "research", "implement", "repair", "review"];
const writePrefixes = [
  "src/main/",
  "src/shim/",
  "src/test/clojure/",
  "design/",
];

export async function loadPolicy(path: string): Promise<Policy> {
  let raw: unknown;
  try {
    raw = JSON.parse(await readFile(path, "utf8"));
  } catch (cause) {
    throw new Error("Cannot load Tempo policy", { cause });
  }
  if (raw === null || typeof raw !== "object") {
    throw new Error("Invalid Tempo policy");
  }
  const value = raw as Partial<Policy>;
  if (
    typeof value.workspace !== "string" || !isAbsolute(value.workspace) ||
    !Array.isArray(value.readRoots) ||
    value.readRoots.some((root) =>
      typeof root !== "string" || !isAbsolute(root)
    ) || !stages.includes(value.stage ?? "")
  ) {
    throw new Error("Invalid Tempo policy");
  }
  return {
    workspace: await realpath(value.workspace),
    readRoots: await Promise.all(value.readRoots.map((root) => realpath(root))),
    stage: value.stage as Policy["stage"],
  };
}

function contained(root: string, path: string) {
  const suffix = relative(root, path);
  return suffix === "" ||
    (!isAbsolute(suffix) && suffix !== ".." && !suffix.startsWith(`..${sep}`));
}

export async function authorizePath(
  policy: Policy,
  tool: string,
  input: unknown,
): Promise<string> {
  if (!["tempo_read", "tempo_write", "tempo_edit"].includes(tool)) {
    throw new Error("Tool is not permitted");
  }
  if (
    typeof input !== "string" || input.length === 0 ||
    [...input].some((character) => character.charCodeAt(0) < 32) ||
    /^[~@]/.test(input)
  ) throw new Error("Use an ordinary file path");
  const writing = tool !== "tempo_read";
  if (writing && policy.stage !== "implement" && policy.stage !== "repair") {
    throw new Error("This step is read-only");
  }
  const path = resolve(policy.workspace, input);
  const roots = writing
    ? [policy.workspace]
    : [policy.workspace, ...policy.readRoots];
  const root = roots.find((candidate) => contained(candidate, path));
  if (root === undefined) {
    throw new Error("Path is outside the permitted directories");
  }
  const suffix = relative(root, path);
  const pieces = suffix.split(sep).filter(Boolean);
  if (pieces.some((piece) => piece.startsWith("."))) {
    throw new Error("Hidden files and Git metadata are not available");
  }
  if (
    writing &&
    !writePrefixes.some((prefix) =>
      relative(policy.workspace, path).startsWith(prefix)
    )
  ) {
    throw new Error(
      "Only application source, tests, and design documents can be changed",
    );
  }
  let current = root;
  for (const [index, piece] of pieces.entries()) {
    current = join(current, piece);
    let stat;
    try {
      stat = await lstat(current);
    } catch (error) {
      if (
        writing && tool === "tempo_write" &&
        (error as NodeJS.ErrnoException).code === "ENOENT"
      ) break;
      throw error;
    }
    if (stat.isSymbolicLink()) {
      throw new Error("Symlinks are not available to Tempo tools");
    }
    if (!stat.isFile() && !stat.isDirectory()) {
      throw new Error("Only regular files and directories are available");
    }
    if (index < pieces.length - 1 && !stat.isDirectory()) {
      throw new Error("Parent is not a directory");
    }
    if (writing && stat.isFile() && stat.nlink !== 1) {
      throw new Error("Hard-linked files cannot be changed");
    }
  }
  return path;
}
