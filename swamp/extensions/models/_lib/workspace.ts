import { join } from "node:path";

export interface Workspace {
  repository: string;
  path: string;
  runId: string;
  initialRevision: string;
}

async function git(cwd: string, args: string[]) {
  const env = Object.fromEntries(
    Object.entries(Deno.env.toObject()).filter(([name]) =>
      !name.startsWith("GIT_")
    ),
  );
  const result = await new Deno.Command("git", {
    cwd,
    args,
    env,
    clearEnv: true,
  }).output();
  if (!result.success) {
    throw new Error(
      `git ${args[0]} failed: ${
        new TextDecoder().decode(result.stderr).trim()
      }`,
    );
  }
  return new TextDecoder().decode(result.stdout).trim();
}
function revision(value: string) {
  if (!/^[0-9a-f]{40}$/.test(value)) {
    throw new Error("Expected a full Git commit ID");
  }
  return value;
}
function reference(workspace: Workspace, name: string) {
  if (
    !/^[a-zA-Z0-9_-]+$/.test(workspace.runId) || !/^[a-zA-Z0-9_-]+$/.test(name)
  ) throw new Error("Invalid snapshot name");
  return `refs/tempo/${workspace.runId}/${name}`;
}

async function verify(workspace: Workspace) {
  const root = await Deno.realPath(workspace.repository);
  const expected = join(root, ".worktrees", `tempo-${workspace.runId}`);
  if (
    workspace.path !== expected ||
    await Deno.realPath(workspace.path) !== expected
  ) throw new Error("Unexpected worktree location");
  if (
    await git(workspace.path, ["rev-parse", "--show-toplevel"]) !== expected
  ) throw new Error("Not the experiment worktree");
  const common = await git(workspace.path, [
    "rev-parse",
    "--path-format=absolute",
    "--git-common-dir",
  ]);
  if (
    common !==
      await git(root, [
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
      ])
  ) throw new Error("Worktree belongs to another repository");
  if (
    await git(root, [
      "rev-parse",
      "--verify",
      reference(workspace, "initial"),
    ]) !== workspace.initialRevision
  ) throw new Error("Run identity does not match its saved revision");
  const head = await git(workspace.path, [
    "rev-parse",
    "--symbolic-full-name",
    "HEAD",
  ]);
  if (head !== "HEAD") throw new Error("Experiment must use detached HEAD");
}

export async function createWorkspace(
  repository: string,
  runId: string,
  requestedRevision: string,
): Promise<Workspace> {
  const root = await Deno.realPath(repository);
  if (await git(root, ["rev-parse", "--show-toplevel"]) !== root) {
    throw new Error("Repository must be its top-level directory");
  }
  const directory = join(root, ".worktrees");
  if (await Deno.realPath(directory) !== directory) {
    throw new Error("Worktree directory must not be a symlink");
  }
  await git(root, ["check-ignore", "--quiet", "--", ".worktrees/"]);
  const initialRevision = revision(
    await git(root, [
      "rev-parse",
      "--verify",
      "--end-of-options",
      `${requestedRevision}^{commit}`,
    ]),
  );
  const workspace = {
    repository: root,
    path: join(directory, `tempo-${runId}`),
    runId,
    initialRevision,
  };
  const initial = reference(workspace, "initial");
  await git(root, ["update-ref", initial, initialRevision, "0".repeat(40)]);
  try {
    await git(root, [
      "worktree",
      "add",
      "--detach",
      workspace.path,
      initialRevision,
    ]);
  } catch (cause) {
    await git(root, ["update-ref", "-d", initial, initialRevision]);
    throw cause;
  }
  await verify(workspace);
  return workspace;
}

export async function inspectCheckout(repository: string) {
  const path = await Deno.realPath(repository);
  if (await git(path, ["rev-parse", "--show-toplevel"]) !== path) {
    throw new Error("Expected a checkout root");
  }
  return {
    path,
    revision: revision(await git(path, ["rev-parse", "HEAD"])),
    dirty:
      (await git(path, ["status", "--porcelain", "--untracked-files=all"])) !==
        "",
  };
}

export async function verifyWorkspaceRevision(
  workspace: Workspace,
  expected: string,
) {
  await verify(workspace);
  revision(expected);
  if (await git(workspace.path, ["rev-parse", "HEAD"]) !== expected) {
    throw new Error("Worktree revision does not match the saved step");
  }
  if (
    await git(workspace.path, [
      "status",
      "--porcelain",
      "--untracked-files=all",
    ])
  ) {
    throw new Error("Worktree has unsaved source changes");
  }
}

export async function snapshotWorkspace(
  workspace: Workspace,
  name: string,
): Promise<string> {
  await verify(workspace);
  const target = reference(workspace, name);
  if (name === "initial" || name === "best") {
    throw new Error("Use an experiment snapshot name");
  }
  const parent = revision(await git(workspace.path, ["rev-parse", "HEAD"]));
  await git(workspace.path, ["add", "--all", "--", "."]);
  const tree = await git(workspace.path, ["write-tree"]);
  const parentTree = await git(workspace.path, ["rev-parse", "HEAD^{tree}"]);
  const commit = tree === parentTree ? parent : revision(
    await git(workspace.path, [
      "-c",
      "user.name=Tempo",
      "-c",
      "user.email=tempo@localhost",
      "commit-tree",
      tree,
      "-p",
      parent,
      "-m",
      "tempo: preserve experiment changes",
    ]),
  );
  await git(workspace.path, ["update-ref", target, commit, "0".repeat(40)]);
  await git(workspace.path, ["update-ref", "HEAD", commit, parent]);
  return commit;
}

export async function selectWorkspaceRevision(
  workspace: Workspace,
  commit: string,
) {
  await verify(workspace);
  revision(commit);
  if (
    await git(workspace.path, [
      "status",
      "--porcelain",
      "--untracked-files=all",
    ])
  ) {
    throw new Error(
      "Save unfinished changes before selecting another revision",
    );
  }
  const saved = (await git(workspace.repository, [
    "for-each-ref",
    "--format=%(objectname)",
    `refs/tempo/${workspace.runId}/`,
  ])).split("\n");
  if (!saved.includes(commit)) {
    throw new Error("Revision is not a saved part of this run");
  }
  await git(workspace.path, ["switch", "--detach", commit]);
}

export async function markBestRevision(workspace: Workspace, commit: string) {
  await verify(workspace);
  revision(commit);
  if (await git(workspace.path, ["rev-parse", "HEAD"]) !== commit) {
    throw new Error("Best revision must match the worktree");
  }
  if (
    await git(workspace.path, [
      "status",
      "--porcelain",
      "--untracked-files=all",
    ])
  ) throw new Error("Best revision must be clean");
  await git(workspace.repository, [
    "update-ref",
    reference(workspace, "best"),
    commit,
  ]);
}
