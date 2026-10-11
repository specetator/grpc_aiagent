/** Explicitly loaded LAST; no discovered extensions may mutate approved inputs. */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { Type } from "typebox";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export function permittedPath(root: string, value: unknown): boolean {
  if (!path.isAbsolute(root) || typeof value !== "string" || !value || value.includes("\0")) return false;
  const target = path.resolve(root, value);
  const relative = path.relative(root, target);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) return false;
  const parts = relative.split(path.sep);
  if (parts.some(p => p.startsWith("."))) return false;
  let current = root;
  for (const part of parts) {
    current = path.join(current, part);
    try { if (fs.lstatSync(current).isSymbolicLink()) return false; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return false; }
  }
  return true;
}

export default function (pi: ExtensionAPI) {
  const root = process.env.SPARK_ROOM_WORKSPACE || "";
  const canDevelop = process.env.SPARK_ROOM_CAN_DEVELOP === "1";
  // Pi may issue parallel tool calls within ONE turn. Serialize edits and
  // tests as well as the outer per-room turns, so receipts name real inputs.
  let tail = Promise.resolve();
  const releases = new Map<string, () => void>();
  const acquire = async (id: string) => {
    const previous = tail;
    tail = new Promise<void>(resolve => releases.set(id, resolve));
    await previous;
  };
  pi.on("tool_result", async event => {
    const release = releases.get(event.toolCallId);
    if (release) { releases.delete(event.toolCallId); release(); }
  });
  pi.on("tool_call", async (event) => {
    if (["cann_knowledge_search", "cann_knowledge_get", "cann_knowledge_neighbors", "cann_knowledge_status"].includes(event.toolName)) return;
    if (event.toolName === "room_run_tests") {
      if (canDevelop) { await acquire(event.toolCallId); return; }
    } else if (["read", "write", "edit"].includes(event.toolName)) {
      if ((event.toolName === "read" || canDevelop) &&
          permittedPath(root, (event.input as Record<string, unknown>).path)) {
        if (event.toolName !== "read") await acquire(event.toolCallId);
        return;
      }
    }
    return { block: true, reason: "房间工具权限不足，或路径不在任务工作区内。" };
  });
  pi.registerTool({
    name: "room_run_tests", label: "Room operator tests",
    description: "Run the administrator-configured test runner in a network-free sandbox; return a versioned receipt. No command/path arguments.",
    parameters: Type.Object({}),
    async execute() {
      const runner = process.env.SPARK_ROOM_TEST_RUNNER;
      if (!canDevelop || !runner) return { content: [{ type: "text", text: "未授权或管理员尚未配置测试执行器；不能宣称测试通过。" }], details: {} };
      const completed = spawnSync("/usr/bin/python3", [process.env.SPARK_ROOM_HELPER!,
        "--workspace", root, "--receipts", process.env.SPARK_ROOM_RECEIPTS!, "--runner", runner],
        { timeout: 135000, maxBuffer: 1024 * 1024, encoding: "utf8" });
      return { content: [{ type: "text", text: completed.stdout || "测试执行失败，未生成成功证据。" }], details: {} };
    }
  });
}
