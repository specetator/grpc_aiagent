/**
 * Parent-owned child lifecycle. Codex's reservation / wait-any semantics are
 * described in docs/codex-subagent-reference.md; the executor is the Pi SDK.
 */
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export const SUBAGENT_TOOL_NAMES = ["spawn_agent", "send_input", "wait_agent", "close_agent", "list_agents"];
export const ROLES = ["researcher", "architect", "developer", "reviewer", "tester"] as const;
export type Role = typeof ROLES[number];
export type Status = "starting" | "running" | "completed" | "failed" | "cancelled" | "timed_out" | "closed";
export const CANN_TOOLS = ["cann_knowledge_search", "cann_knowledge_get", "cann_knowledge_neighbors", "cann_knowledge_status"];
export const MAX_LAUNCHES = 16;
export const MAX_ROUNDS = 4;
export const MAX_TOOL_CALLS = 48;
export const OUTPUT_LIMIT = 12000;

export function bounded(text: string, limit = OUTPUT_LIMIT): string {
  if (text.length <= limit) return text;
  let head = text.slice(0, limit);
  if (/[\uD800-\uDBFF]$/.test(head)) head = head.slice(0, -1);
  return head + "\n[truncated; ask the child for a narrower summary]";
}

export function envInteger(name: string, fallback: number, min: number, max: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`${name} must be an integer in [${min}, ${max}]`);
  return value;
}

/** No hidden files, traversal, symlinks or hard-linked files in child tools. */
export function scopedPath(root: string, value: unknown): string {
  if (typeof value !== "string" || !value || value.includes("\0")) throw new Error("A workspace file path is required");
  const target = path.resolve(root, value);
  const relative = path.relative(root, target);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) throw new Error("Path escapes the child workspace");
  const parts = relative.split(path.sep);
  if (parts.some(part => part.startsWith("."))) throw new Error("Hidden paths are unavailable to child agents");
  let current = root;
  for (const part of parts) {
    current = path.join(current, part);
    try {
      const stat = fs.lstatSync(current);
      if (stat.isSymbolicLink() || (stat.isFile() && stat.nlink > 1)) throw new Error("Linked paths are unavailable to child agents");
      if (current === target && !stat.isFile()) throw new Error("Child tools accept files, not directories");
    } catch (error: any) {
      if (error.code !== "ENOENT") throw error;
    }
  }
  return target;
}

export type ChildSpec = { id: string; role: Role; message: string; root: string; tools: string[]; allowedWrites: string[]; cancelled?: boolean };
export interface ChildSession {
  prompt(message: string, options?: any): Promise<void>;
  abort(): Promise<void>;
  dispose(): void;
  subscribe(listener: (event: any) => void): () => void;
  readonly messages: readonly any[];
}
export type ChildFactory = (spec: ChildSpec) => Promise<ChildSession>;
type RecordState = ChildSpec & { status: Status; rounds: number; toolCalls: number; output: string; error?: string;
  session?: ChildSession; task?: Promise<void>; timer?: ReturnType<typeof setTimeout>; generation: number;
  unsubscribe?: () => void; cleanup?: Promise<void>; released?: boolean; controlling?: boolean };
export type Snapshot = { id: string; role: Role; status: Status; rounds: number; tool_calls: number; output: string; error?: string };

export class AgentControl {
  private records = new Map<string, RecordState>();
  private listeners = new Set<() => void>();
  private launches = 0;
  private disposed = false;
  readonly owner: string;
  readonly root: string;
  private parentTools: Set<string>;
  constructor(options: { owner: string; root: string; parentTools: string[]; room?: boolean; factory: ChildFactory;
    maxChildren?: number; timeoutMs?: number; onChange?: (snapshot: Snapshot) => void }) {
    this.options = options;
    this.owner = options.owner;
    this.root = fs.realpathSync(options.root);
    this.parentTools = new Set(options.parentTools);
    this.maxChildren = options.maxChildren ?? 4;
    this.timeoutMs = options.timeoutMs ?? 120000;
  }
  private options;
  private maxChildren: number;
  private timeoutMs: number;
  private snapshot(record: RecordState): Snapshot {
    return { id: record.id, role: record.role, status: record.status, rounds: record.rounds,
      tool_calls: record.toolCalls, output: record.output, ...(record.error ? { error: record.error } : {}) };
  }
  private changed(record: RecordState) {
    this.options.onChange?.(this.snapshot(record));
    for (const notify of [...this.listeners]) notify();
  }
  private get(id: string): RecordState {
    const record = this.records.get(id);
    if (!record || this.disposed) throw new Error("Unknown child in this parent turn");
    return record;
  }
  list(): Snapshot[] { return this.disposed ? [] : [...this.records.values()].map(record => this.snapshot(record)); }
  spawn(input: { role?: string; message: string; allowed_writes?: string[] }): Snapshot {
    if (this.disposed) throw new Error("Parent turn is closed");
    const role = input.role ?? "researcher";
    if (!(ROLES as readonly string[]).includes(role)) throw new Error("Unknown child role");
    this.validateMessage(input.message);
    if (this.launches >= MAX_LAUNCHES) throw new Error("Parent turn child launch limit reached");
    if ([...this.records.values()].filter(record => !record.released).length >= this.maxChildren)
      throw new Error("Child slots are full; collect and close a child before spawning another");
    const writes = input.allowed_writes ?? [];
    if (!Array.isArray(writes) || writes.length > 32) throw new Error("allowed_writes must contain at most 32 exact file paths");
    if (writes.length && (role !== "developer" || this.options.room)) throw new Error("Only private developer children may receive write scopes");
    const allowedWrites = [...new Set(writes.map(value => scopedPath(this.root, value)))];
    if (role === "developer" && !this.options.room && !allowedWrites.length)
      throw new Error("Developer children require explicit allowed_writes file paths");
    if (allowedWrites.length && !this.parentTools.has("write") && !this.parentTools.has("edit"))
      throw new Error("Parent has no file mutation tools to delegate");
    for (const record of this.records.values()) {
      if (!record.released && record.allowedWrites.some(file => allowedWrites.some(other =>
        file === other || file.startsWith(other + path.sep) || other.startsWith(file + path.sep))))
        throw new Error("Write scope overlaps another child; collect and close it first");
    }
    const tools = ["read", ...CANN_TOOLS, ...(allowedWrites.length ? ["write", "edit"] : [])].filter(name => this.parentTools.has(name));
    const record: RecordState = { id: randomUUID(), role: role as Role, message: input.message,
      root: this.root, tools, allowedWrites, status: "starting", rounds: 1, toolCalls: 0, output: "", generation: 0 };
    // Reserve BEFORE the first await, as in Codex's slot reservation guard.
    this.records.set(record.id, record);
    this.launches++;
    this.changed(record);
    this.armTimeout(record);
    record.task = this.start(record);
    return this.snapshot(record);
  }
  private validateMessage(message: string) {
    if (typeof message !== "string" || !message.trim() || message.length > 16000)
      throw new Error("Child message must contain 1..16000 characters");
  }
  private armTimeout(record: RecordState) {
    clearTimeout(record.timer);
    record.timer = setTimeout(() => { void this.finish(record, "timed_out", "Child deadline exceeded"); }, this.timeoutMs);
    record.timer.unref?.();
  }
  private async start(record: RecordState) {
    try {
      const session = await this.options.factory(record);
      if (record.status !== "starting" || this.disposed) {
        try { await session.abort(); } finally { session.dispose(); }
        return;
      }
      record.session = session;
      record.unsubscribe = session.subscribe((event: any) => {
        if (event.type === "tool_execution_start") {
          record.toolCalls++;
          if (record.toolCalls > MAX_TOOL_CALLS) void this.finish(record, "failed", "Child tool-call budget exceeded");
        }
      });
      await this.run(record, record.message);
    } catch {
      if (record.status === "starting" || record.status === "running") {
        await this.finish(record, "failed", "Child initialization or execution failed");
      }
    }
  }
  private async run(record: RecordState, message: string) {
    const generation = ++record.generation;
    record.status = "running"; record.output = ""; delete record.error;
    this.changed(record);
    try {
      await record.session!.prompt(message, { expandPromptTemplates: false });
      if (generation !== record.generation || record.status !== "running") return;
      const last = [...record.session!.messages].reverse().find(item => item.role === "assistant");
      if (!last || last.stopReason !== "stop") {
        record.status = "failed"; record.error = "Child did not produce a successful final answer";
      } else {
        record.output = bounded((last.content ?? []).filter((item: any) => item.type === "text").map((item: any) => item.text).join("\n"));
        record.status = "completed";
      }
    } catch {
      if (generation !== record.generation || record.status !== "running") return;
      record.status = "failed"; record.error = "Child execution failed";
    } finally {
      if (generation === record.generation) { clearTimeout(record.timer); this.changed(record); }
    }
  }
  async send(id: string, message: string, interrupt = false): Promise<Snapshot> {
    this.validateMessage(message);
    const record = this.get(id);
    if (record.controlling) throw new Error("Another child control operation is in progress");
    if (record.rounds >= MAX_ROUNDS) throw new Error("Child follow-up limit reached");
    if (record.status === "starting") throw new Error("Child is starting; wait before sending follow-up");
    if (!["running", "completed"].includes(record.status)) throw new Error("Only running or completed children accept follow-up");
    if (record.status === "running" && !interrupt) throw new Error("Child is running; wait or set interrupt=true");
    record.controlling = true;
    try {
    if (record.status === "running") {
      record.generation++;
      await record.session!.abort();
      await record.task;
      if (this.disposed || record.status !== "running") throw new Error("Child closed during interruption");
    }
    record.rounds++;
    this.armTimeout(record);
    record.task = this.run(record, message);
    return this.snapshot(record);
    } finally { record.controlling = false; }
  }
  /** Wait-any: a wait timeout leaves the child running. */
  async wait(ids: string[], timeoutMs = 10000, signal?: AbortSignal): Promise<{ agents: Snapshot[]; timed_out: boolean }> {
    if (!Array.isArray(ids) || !ids.length || ids.length > this.maxChildren) throw new Error("Provide 1..maxChildren child IDs");
    if (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 60000) throw new Error("Wait timeout must be in [0, 60000] ms");
    const records = [...new Set(ids)].map(id => this.get(id));
    const terminal = () => records.some(record => !["starting", "running"].includes(record.status));
    if (!terminal() && timeoutMs && !signal?.aborted) {
      await new Promise<void>(resolve => {
        let timer: ReturnType<typeof setTimeout>;
        const done = () => { clearTimeout(timer); this.listeners.delete(changed); signal?.removeEventListener("abort", done); resolve(); };
        const changed = () => { if (terminal() || this.disposed) done(); };
        timer = setTimeout(done, timeoutMs);
        this.listeners.add(changed);
        signal?.addEventListener("abort", done, { once: true });
        if (signal?.aborted || terminal() || this.disposed) done();
      });
    }
    if (signal?.aborted) throw new Error("Parent cancelled the wait");
    return { agents: records.map(record => this.snapshot(record)), timed_out: !terminal() };
  }
  private async finish(record: RecordState, status: Status, error?: string) {
    record.generation++;
    record.cancelled = true;
    clearTimeout(record.timer);
    record.status = status;
    if (error) record.error = error;
    this.changed(record);
    if (record.cleanup) return record.cleanup;
    record.cleanup = (async () => {
      let confirmed = true;
      try { await record.session?.abort(); }
      catch { confirmed = false; record.status = "failed"; record.error = "Child cancellation could not be confirmed"; }
      finally {
        record.unsubscribe?.();
        try { record.session?.dispose(); } catch { confirmed = false; record.error = "Child disposal failed"; }
        record.session = undefined;
        // Do not grant another writer the slot while failed cancellation still
        // leaves the old prompt unsettled. Future child tools are revoked above.
        record.released = confirmed;
        if (!confirmed && record.task) {
          const release = () => { record.released = true; this.changed(record); };
          record.task.then(release, release);
        }
        this.changed(record);
      }
    })();
    await record.cleanup;
  }
  async close(id: string): Promise<Snapshot> {
    const record = this.get(id);
    await this.finish(record, "closed");
    return this.snapshot(record);
  }
  async dispose() {
    if (this.disposed) return;
    this.disposed = true;
    await Promise.allSettled([...this.records.values()].map(record => this.finish(record, "cancelled", "Parent turn ended")));
    this.records.clear();
    for (const notify of [...this.listeners]) notify();
    this.listeners.clear();
  }
}
