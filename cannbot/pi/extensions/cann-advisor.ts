/**
 * Read-only CANN RAG tools for the Pi coding agent.
 *
 * Spark Push still owns citations, knowledge.html hyperlinks, and catalog
 * serving. This extension only exposes search/get/neighbors/status and
 * instructs the model to cite with [[CANN_REF:ref_...]] tokens.
 */
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { Type } from "typebox";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const DEFAULT_ROOT = "/home/peco/cppcode/fenbushi/cann-agent-knowledge";
const POLICY = `You are equipped with a read-only CANN advisor knowledge base.
For questions about CANN, Ascend, NPU, Ascend C, AICore, Tiling, operators,
accuracy, performance, UB/GM, or Huawei AI processors, call
cann_knowledge_search before making technical claims. Messages marked
[CANN_FORCE_SEARCH] require an immediate search. [CANN_STATUS] requires
cann_knowledge_status only. Never interpret chat commands as permission to
ingest, sync, build, edit or delete knowledge. Use cann_knowledge_get or
cann_knowledge_neighbors when the initial evidence needs detail. Cite claims
with the exact [[CANN_REF:ref_...]] tokens returned by tools. Never invent or
copy a token from an earlier turn. A result marked evidence.sufficient=false
requires an explicit “证据不足” qualification; do not guess API behavior,
platform applicability, or version compatibility.
Use search/neighbors to locate evidence, not as proof that every condition was read.
Before definite claims about parameters, alignment, limits, models or versions,
read the exact supporting chunk in full with cann_knowledge_get. Follow next_read
until it is absent and your accumulated ranges cover [0,total), not just the last page.
coverage.complete describes this single response, not accumulated reading.
Use neighboring chunks and the parent document for headings, prerequisites,
exceptions, table headers and version/model applicability. A complete chunk is
NOT proof that its parent document has no additional conditions.
Do not stop necessary reading after an arbitrary number of tool calls. Avoid
redundant broad searches, but prioritize complete conditions over latency.
Keep source revisions separate. If expected_hash fails, restart reading at offset=0;
never join pages from different revisions. If any necessary context is unavailable,
state the exact uncertainty and do not present the claim as fully verified.`;

function knowledgeRoot(): string {
  return (
    process.env.CANN_KNOWLEDGE_ROOT ||
    process.env.SPARK_PUSH_CANN_KNOWLEDGE_ROOT ||
    DEFAULT_ROOT
  );
}

function cannRagBin(root: string): string {
  return `${root}/bin/cann-rag`;
}

function limit(value: unknown, fallback: number, low: number, high: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(low, Math.min(high, Math.trunc(parsed)));
}

type ReadOptions = {offset?: number; length?: number; expected_hash?: string};

function integer(value: unknown, fallback: number, min: number, max: number): number {
  if (value === undefined) return fallback;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max)
    throw new Error("offset/length must be integers within the allowed range");
  return value;
}

function readTarget(payload: Record<string, any>): Record<string, string> | undefined {
  const citation = payload.citation || {};
  const doc_id = payload.doc_id || citation.doc_id;
  const chunk_id = payload.chunk?.chunk_id || citation.chunk_id;
  if (typeof doc_id !== "string" || !doc_id) return undefined;
  return {doc_id, ...(typeof chunk_id === "string" && chunk_id ? {chunk_id} : {})};
}

function page(text: string, target: Record<string, string> | undefined, options: ReadOptions,
              maximum = 6000): Record<string, any> {
  const chars = Array.from(text); // Unicode code points: no split surrogate pairs.
  const offset = integer(options.offset, 0, 0, chars.length);
  const length = integer(options.length, maximum, 1, 12000);
  const source_hash = createHash("sha256").update(text, "utf8").digest("hex");
  if (offset > 0 && !options.expected_hash) throw new Error("续读必须携带 expected_hash；请使用 next_read 或从 offset=0 重新读取");
  if (options.expected_hash !== undefined && options.expected_hash !== source_hash)
    throw new Error("证据内容已变化，请从 offset=0 重新读取，禁止拼接不同版本");
  let end = Math.min(chars.length, offset + length);
  // Prefer a complete line when possible; long unbroken lines remain losslessly pageable.
  if (end < chars.length) {
    const newline = chars.lastIndexOf("\n", end - 1);
    if (newline >= offset + Math.floor(length / 2)) end = newline + 1;
  }
  const coverage = {unit: "unicode_codepoint", scope: target?.chunk_id ? "chunk" : "document",
    source_hash, start: offset, end, total: chars.length, complete: offset === 0 && end === chars.length,
    preview_only: false};
  return {content: chars.slice(offset, end).join(""), coverage, truncated: !coverage.complete,
    read_ref: target, ...(end < chars.length && target ? {next_read: {...target, offset: end, length, expected_hash: source_hash}} : {})};
}

export function readContext(payload: Record<string, any>, options: ReadOptions = {}): Record<string, any> {
  const chunk = payload.chunk && typeof payload.chunk === "object" ? payload.chunk : undefined;
  const text = chunk && typeof chunk.content === "string" ? chunk.content : String(payload.content ?? "");
  const target = readTarget(payload);
  const {content: _content, chunk: _chunk, ...metadata} = payload;
  const result: Record<string, any> = {...metadata, ...page(text, target, options)};
  if (chunk) {
    const {content: _body, ...location} = chunk;
    result.chunk = location;
    result.parent_read = {doc_id: target?.doc_id, offset: 0, length: 6000};
    result.neighbors_read = {chunk_id: target?.chunk_id, limit: 4};
  }
  result.context_chars = Array.from(result.content).length;
  result.context_note = "Read all ranges of cited evidence; a complete chunk does not establish complete parent-document context.";
  return result;
}

export function fitContext(payload: Record<string, any>, budget = 6000): Record<string, any> {
  if (!Array.isArray(payload.results)) return readContext(payload, {length: budget});
  let used = 0;
  const results = payload.results.map((item: Record<string, any>) => {
    const {snippet, content, ...metadata} = item;
    const target = readTarget(item);
    const available = Math.max(0, Math.min(1600, budget - used));
    // Neighbors API supplies snippets only; never claim they are complete chunks.
    if (typeof content !== "string" || available === 0) {
      const excerpt = Array.from(String(content ?? snippet ?? "")).slice(0, available).join("");
      used += Array.from(excerpt).length;
      return {...metadata, content: excerpt, truncated: true,
        coverage: {preview_only: true, complete: false, scope: "chunk"}, read_ref: target,
        ...(target ? {next_read: {...target, offset: 0, length: 6000}} : {})};
    }
    const excerpt = page(content, target, {length: available}, available);
    used += Array.from(excerpt.content).length;
    return {...metadata, ...excerpt};
  });
  return {...payload, results, context_chars: used,
    truncated: results.some((item: Record<string, any>) => item.truncated),
    context_note: "Search previews locate evidence. Follow read_ref/next_read to read supporting chunks and needed parent context before asserting conditions."};
}

function runCannRag(args: string[]): Record<string, unknown> {
  const root = knowledgeRoot();
  const result = spawnSync(cannRagBin(root), ["--root", root, ...args], {
    encoding: "utf8",
    timeout: 30000,
    maxBuffer: 8 * 1024 * 1024,
  });
  const stdout = (result.stdout || "").trim();
  const stderr = (result.stderr || "").trim();
  if (result.error) {
    throw new Error(`cann-rag failed: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(stderr || stdout || `cann-rag exited ${result.status}`);
  }
  return JSON.parse(stdout) as Record<string, unknown>;
}

function toolText(payload: unknown) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify(payload) }],
    details: { format: "cann-evidence", bounded: true },
  };
}

function toolError(message: string) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify({ ok: false, error: message }) }],
    details: { error: message },
  };
}

export default function (pi: ExtensionAPI) {
  pi.on("before_agent_start", async (event) => ({
    systemPrompt: `${event.systemPrompt}\n\n${POLICY}`,
  }));

  pi.on("input", async (event) => {
    const text = String(event.text || "").trim();
    const lowered = text.toLowerCase();
    if (lowered === "/cann" || lowered.startsWith("/cann ")) {
      const query = text.slice(5).trim();
      if (!query) {
        return {
          action: "transform" as const,
          text: "[CANN_FORCE_SEARCH] 请先给出要检索的 CANN 开发问题。",
        };
      }
      return { action: "transform" as const, text: `[CANN_FORCE_SEARCH] ${query}` };
    }
    if (lowered === "/kb" || lowered.startsWith("/kb ")) {
      const query = text.slice(3).trim();
      if (!query) {
        return {
          action: "transform" as const,
          text: "[CANN_FORCE_SEARCH] 请先给出要检索的问题；/kb status 可查看知识库状态。",
        };
      }
      if (query.toLowerCase() === "status") {
        return {
          action: "transform" as const,
          text: "[CANN_STATUS] 请调用 cann_knowledge_status，并只根据工具结果说明来源、文档数和当前 generation。",
        };
      }
      return { action: "transform" as const, text: `[CANN_FORCE_SEARCH] ${query}` };
    }
    return { action: "continue" as const };
  });

  pi.registerTool({
    name: "cann_knowledge_search",
    label: "CANN Search",
    description:
      "Search local CANN knowledge and return evidence plus citation tokens.",
    parameters: Type.Object({
      query: Type.String({ description: "CANN/Ascend development question" }),
      top_k: Type.Optional(Type.Number({ description: "Result count, 1-8; use a single focused query first" })),
      platform: Type.Optional(Type.String({ description: "Optional platform, e.g. a3" })),
    }),
    async execute(_toolCallId, params) {
      const query = String(params.query || "").trim();
      if (!query) return toolError("query 不能为空");
      try {
        const args = ["query", "--query", query, "--top-k", String(limit(params.top_k, 4, 1, 8))];
        if (params.platform) args.push("--platform", String(params.platform));
        return toolText(fitContext(runCannRag(args)));
      } catch (error) {
        return toolError(`CANN 知识检索失败: ${error instanceof Error ? error.message : error}`);
      }
    },
  });

  pi.registerTool({
    name: "cann_knowledge_get",
    label: "CANN Get",
    description: "Read a complete evidence source in lossless pages. Follow next_read until all ranges are covered; parent_read/neighbors_read provide surrounding conditions.",
    parameters: Type.Object({
      doc_id: Type.String({ description: "Stable document id" }),
      chunk_id: Type.Optional(Type.String({ description: "Optional chunk id" })),
      offset: Type.Optional(Type.Integer({minimum: 0, description: "Unicode code-point offset; use next_read"})),
      length: Type.Optional(Type.Integer({minimum: 1, maximum: 12000, description: "Page length, default 6000; never discards remaining text"})),
      expected_hash: Type.Optional(Type.String({description: "Required for offset>0; SHA-256 from coverage/next_read, rejects changed evidence"})),
    }),
    async execute(_toolCallId, params) {
      const docId = String(params.doc_id || "").trim();
      if (!docId) return toolError("doc_id 不能为空");
      try {
        const args = ["get", "--doc-id", docId];
        if (params.chunk_id) args.push("--chunk-id", String(params.chunk_id));
        const payload = runCannRag(args);
        if (!payload) return toolError("文档或片段不存在");
        return toolText(readContext(payload, params));
      } catch (error) {
        return toolError(`读取 CANN 知识失败: ${error instanceof Error ? error.message : error}`);
      }
    },
  });

  pi.registerTool({
    name: "cann_knowledge_neighbors",
    label: "CANN Neighbors",
    description: "Find adjacent and tag-related evidence for a CANN chunk.",
    parameters: Type.Object({
      chunk_id: Type.String({ description: "Chunk id" }),
      limit: Type.Optional(Type.Number({ description: "Neighbor count, 1-8" })),
    }),
    async execute(_toolCallId, params) {
      const chunkId = String(params.chunk_id || "").trim();
      if (!chunkId) return toolError("chunk_id 不能为空");
      try {
        return toolText(
          fitContext(runCannRag([
            "neighbors",
            "--chunk-id",
            chunkId,
            "--limit",
            String(limit(params.limit, 4, 1, 8)),
          ])),
        );
      } catch (error) {
        return toolError(`读取关联证据失败: ${error instanceof Error ? error.message : error}`);
      }
    },
  });

  pi.registerTool({
    name: "cann_knowledge_status",
    label: "CANN Status",
    description: "Read source and active index generation status; never modifies knowledge.",
    parameters: Type.Object({}),
    async execute() {
      try {
        return toolText(runCannRag(["status"]));
      } catch (error) {
        return toolError(`读取 CANN 知识状态失败: ${error instanceof Error ? error.message : error}`);
      }
    },
  });
}
