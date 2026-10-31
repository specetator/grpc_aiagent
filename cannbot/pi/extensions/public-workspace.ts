/** Public Pi loads only this extension; the original native read tool remains in use. */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { createReadToolDefinition, type ExtensionAPI } from "@earendil-works/pi-coding-agent";

const denied = "工作区授权已失效或文件不在批准的只读快照中。";
export function approvedFile(root: string, manifest: any, value: unknown): boolean {
  if (typeof value !== "string" || !value || value.length > 512 || /[\\\0:]/.test(value)) return false;
  const parts = value.split("/");
  if (parts.some(p => !p || p.startsWith("."))) return false;
  const expected = value === "PROJECT_FILES.txt" ? manifest.index_sha256 :
    manifest.files.find((row: any) => row.path === value)?.sha256;
  if (!expected) return false;
  let target = root;
  try {
    if (fs.lstatSync(root).isSymbolicLink()) return false;
    for (const part of parts) {
      target = path.join(target, part);
      if (fs.lstatSync(target).isSymbolicLink()) return false;
    }
    const info = fs.statSync(target);
    return info.isFile() && info.nlink === 1 && info.size <= 1024 * 1024 &&
      crypto.createHash("sha256").update(fs.readFileSync(target)).digest("hex") === expected;
  } catch { return false; }
}

export default function(pi: ExtensionAPI) {
  const root = process.env.SPARK_PUBLIC_SNAPSHOT || "";
  const scope = JSON.parse(process.env.SPARK_PUBLIC_SCOPE || "null");
  const manifest = JSON.parse(fs.readFileSync(process.env.SPARK_PUBLIC_MANIFEST_PATH!, "utf8"));
  const expected = process.env.SPARK_PUBLIC_SCOPE_CANONICAL || "";
  const canonical = (value: any): any => Array.isArray(value) ? value.map(canonical) :
    value && typeof value === "object" ? Object.fromEntries(Object.keys(value).sort().map(k => [k, canonical(value[k])])) : value;
  const authorize = async () => {
    if (!scope || !manifest || !root || !expected) throw new Error(denied);
    const response = await fetch(process.env.SPARK_WORKSPACE_AUTH_ORIGIN + "/internal/room-agent/authorize", {
      method: "POST", redirect: "error", signal: AbortSignal.timeout(3000),
      headers: {"Content-Type": "application/json", "Authorization": "Bearer " + process.env.SPARK_PUSH_WORKSPACE_AUTH_KEY},
      body: JSON.stringify({request_id: scope.request_id})
    });
    const raw = await response.text();
    if (raw.length > 65536) throw new Error(denied);
    const value = JSON.parse(raw);
    if (!response.ok || value.code !== 0 || value.data?.request_id !== scope.request_id ||
        JSON.stringify(canonical(value.data?.collaboration)) !== expected) throw new Error(denied);
  };
  const nativeRead = createReadToolDefinition(root);
  // The CLI keeps --no-builtin-tools. If this extension cannot load, built-in
  // tools stay inactive. Both tool execution paths use this registered handler.
  pi.registerTool({ ...nativeRead,
    async execute(id, input, signal, _onUpdate, ctx) {
      try {
        await authorize();
        if (!approvedFile(root,manifest,input.path)) throw new Error(denied);
        const result = await nativeRead.execute(id,input,signal,undefined,ctx);
        if (!approvedFile(root,manifest,input.path)) throw new Error(denied);
        await authorize();
        return result;
      } catch { throw new Error(denied); }
    }
  });
}
