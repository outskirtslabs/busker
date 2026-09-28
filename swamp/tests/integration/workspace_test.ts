import { deepStrictEqual, notStrictEqual, rejects } from "node:assert/strict";
import { join } from "node:path";
import {
  createWorkspace,
  markBestRevision,
  selectWorkspaceRevision,
  snapshotWorkspace,
} from "../../extensions/models/_lib/workspace.ts";

async function git(cwd: string, ...args: string[]) {
  const result = await new Deno.Command("git", {
    cwd,
    args: [
      "-c",
      "user.name=Tempo Fixture",
      "-c",
      "user.email=tempo-fixture@localhost",
      "-c",
      "commit.gpgsign=false",
      ...args,
    ],
  }).output();
  if (!result.success) throw new Error(new TextDecoder().decode(result.stderr));
  return new TextDecoder().decode(result.stdout).trim();
}

Deno.test("worktree snapshots preserve rejected and unfinished code without touching the main checkout", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-workspace-fixture-" });
  try {
    await git(root, "init", "--initial-branch=main");
    await git(root, "config", "user.name", "Workspace Author");
    await git(root, "config", "user.email", "workspace@example.test");
    await Deno.mkdir(join(root, ".worktrees"));
    await Deno.writeTextFile(join(root, ".gitignore"), ".worktrees/\n");
    await Deno.writeTextFile(join(root, "source.txt"), "initial\n");
    await git(root, "add", ".");
    await git(root, "commit", "-m", "fixture: create initial source");
    const initial = await git(root, "rev-parse", "HEAD");
    await Deno.writeTextFile(
      join(root, "source.txt"),
      "unrelated main edits\n",
    );
    const before = await git(root, "status", "--porcelain");
    const workspace = await createWorkspace(root, "fixture", initial);
    deepStrictEqual(
      await Deno.readTextFile(join(workspace.path, "source.txt")),
      "initial\n",
    );
    await rejects(
      () => createWorkspace(root, "fixture", initial),
      /update-ref failed/,
    );
    await Deno.writeTextFile(
      join(workspace.path, "source.txt"),
      "experiment\n",
    );
    await Deno.writeTextFile(join(workspace.path, "new.txt"), "new source\n");
    await rejects(
      () => selectWorkspaceRevision(workspace, initial),
      /Save unfinished changes/,
    );
    const previousIndex = Deno.env.get("GIT_INDEX_FILE");
    Deno.env.set("GIT_INDEX_FILE", join(root, ".git", "index"));
    let edited: string;
    try {
      edited = await snapshotWorkspace(workspace, "attempt-1");
    } finally {
      if (previousIndex === undefined) Deno.env.delete("GIT_INDEX_FILE");
      else Deno.env.set("GIT_INDEX_FILE", previousIndex);
    }
    notStrictEqual(edited, initial);
    deepStrictEqual(
      await git(root, "show", "-s", "--format=%an <%ae>%n%cn <%ce>", edited),
      "Workspace Author <workspace@example.test>\nWorkspace Author <workspace@example.test>",
    );
    await markBestRevision(workspace, edited);
    deepStrictEqual(
      await git(root, "rev-parse", "refs/tempo/fixture/best"),
      edited,
    );
    await Deno.writeTextFile(
      join(workspace.path, "source.txt"),
      "unfinished edit\n",
    );
    await rejects(() => markBestRevision(workspace, edited), /must be clean/);
    const unfinished = await snapshotWorkspace(workspace, "unfinished");
    await selectWorkspaceRevision(workspace, edited);
    deepStrictEqual(
      await Deno.readTextFile(join(workspace.path, "source.txt")),
      "experiment\n",
    );
    deepStrictEqual(
      await git(root, "show", `${unfinished}:source.txt`),
      "unfinished edit",
    );
    deepStrictEqual(await git(root, "show", `${edited}:new.txt`), "new source");
    await selectWorkspaceRevision(workspace, initial);
    await rejects(
      () => snapshotWorkspace({ ...workspace, path: root }, "wrong-location"),
      /Unexpected worktree location/,
    );
    deepStrictEqual(await git(root, "status", "--porcelain"), before);
    deepStrictEqual(await git(root, "rev-parse", "HEAD"), initial);
    deepStrictEqual(
      await Deno.readTextFile(join(root, "source.txt")),
      "unrelated main edits\n",
    );
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});
