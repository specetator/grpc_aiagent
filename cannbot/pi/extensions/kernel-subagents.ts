/**
 * Kernel Agent orchestration, backed by Pi's real SDK sessions and tools.
 * SDK wiring adapts Pi's MIT-licensed examples/sdk/12-full-control.ts.
 * See third_party/pi/LICENSE and docs/codex-subagent-reference.md.
 */
import path from "node:path";
import {
  createAgentSession, DefaultResourceLoader, getAgentDir, ModelRuntime,
  SessionManager, SettingsManager,
  type ExtensionAPI, type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import cannAdvisor from "./cann-advisor.ts";
import { AgentControl, CANN_TOOLS, envInteger, scopedPath, SUBAGENT_TOOL_NAMES, MAX_TOOL_CALLS,
  type ChildSpec, type ChildSession, type Role } from "./lib/kernel-agent-control.ts";

export { SUBAGENT_TOOL_NAMES } from "./lib/kernel-agent-control.ts";

const ROLE_PROMPTS: Record<Role, string> = {
  researcher: "Research the assigned question using source files and CANN evidence. Return precise references and uncertainties. Do not edit files.",
  architect: "Inspect current implementation, propose a concrete plan with affected files, boundaries and validation. Do not edit files.",
  developer: "Implement only the delegated change within the exact allowed_writes files. Preserve unrelated user changes. Return changed paths and remaining verification. You have no shell; ask the parent to run tests.",
  reviewer: "Independently inspect the specified implementation. Return findings with severity and file evidence, or state which paths you reviewed and found no issue. Do not edit files.",
  tester: "Independently inspect test coverage and propose exact reproducible checks. You cannot execute commands or a device runner; clearly mark every unexecuted check and ask the parent to run it. Do not claim tests passed without actual supplied evidence.",
};

export function childGuard(spec: ChildSpec) {
  return (pi: ExtensionAPI) => {
    let calls = 0;
    pi.on("tool_call", event => {
      if (spec.cancelled) return { block: true, reason: "Parent revoked this child" };
      if (++calls > MAX_TOOL_CALLS) return { block: true, reason: "Child tool-call budget exhausted" };
      if (!spec.tools.includes(event.toolName)) return { block: true, reason: "Tool is outside delegated capabilities" };
      if (CANN_TOOLS.includes(event.toolName)) return;
      try {
        const target = scopedPath(spec.root, (event.input as Record<string, unknown>).path);
        if (event.toolName !== "read" && !spec.allowedWrites.includes(target))
          throw new Error("File is outside allowed_writes");
      } catch (error: any) { return { block: true, reason: error.message }; }
    });
  };
}

/** Exported to exercise the real SDK against a controlled local model in tests. */
export async function createChildSession(spec: ChildSpec, ctx: ExtensionContext): Promise<ChildSession> {
  if (!ctx.model) throw new Error("Parent model is unavailable");
  const auth = await ctx.modelRegistry.getApiKeyAndHeaders(ctx.model);
  if (!auth.ok) throw new Error("Parent model credentials are unavailable");
  const agentDir = getAgentDir();
  // Use the same provider configuration and credentials store as the parent;
  // only public registry APIs are used, never private runtime internals.
  const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"),
    modelsPath: path.join(agentDir, "models.json"), allowModelNetwork: false });
  for (const provider of ctx.modelRegistry.getRegisteredProviderIds()) {
    const native = ctx.modelRegistry.getRegisteredNativeProvider(provider);
    const config = ctx.modelRegistry.getRegisteredProviderConfig(provider);
    if (native) modelRuntime.registerNativeProvider(native);
    else if (config) modelRuntime.registerProvider(provider, config);
  }
  // Pi's RuntimeCredentials keeps this override in memory; it does not write
  // auth.json. Preserve native OAuth identity instead of converting it to a key.
  if (auth.apiKey && !ctx.modelRegistry.isUsingOAuth(ctx.model))
    await modelRuntime.setRuntimeApiKey(ctx.model.provider, auth.apiKey);
  const inheritedModel = { ...ctx.model, ...(auth.baseUrl ? { baseUrl: auth.baseUrl } : {}),
    headers: { ...ctx.model.headers, ...auth.headers } };
  const settingsManager = SettingsManager.inMemory({ defaultProjectTrust: "never",
    retry: { enabled: false, provider: { maxRetries: 0 } }, compaction: { enabled: false },
    enableAnalytics: false, enableInstallTelemetry: false });
  const resourceLoader = new DefaultResourceLoader({ cwd: spec.root, agentDir, settingsManager,
    noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
    extensionFactories: [cannAdvisor, childGuard(spec)],
    systemPrompt: `You are a Kernel Agent child, role=${spec.role}, depth=1.
${ROLE_PROMPTS[spec.role]}
Your context is independent: use only the task and facts explicitly supplied by your parent.
Workspace: ${spec.root}
Allowed writes: ${JSON.stringify(spec.allowedWrites)}
Tools are restricted by the runtime. Do not delegate, bypass paths, or request credentials.
Shared workspace is not an OS sandbox. Other agents may work on different files.
For CANN evidence, return doc_id/chunk_id and source locations so the parent can retrieve it in
its own gateway turn. Child citation tokens alone are not valid parent citation evidence.
Report evidence, changed files, executed versus unexecuted checks, and blockers to your parent.
Your result is advisory: the parent must inspect changes and verify tests before claiming success.`,
  });
  await resourceLoader.reload();
  if (resourceLoader.getExtensions().errors.length) throw new Error("Child extension initialization failed");
  const { session } = await createAgentSession({ cwd: spec.root, agentDir,
    model: inheritedModel, thinkingLevel: ctx.thinkingLevel ?? "off", modelRuntime, settingsManager,
    resourceLoader, tools: spec.tools, sessionManager: SessionManager.inMemory(spec.root) });
  return session;
}

const result = (value: unknown, isError = false) => ({
  content: [{ type: "text" as const, text: JSON.stringify(value) }], details: { kind: "kernel_subagents", value },
  ...(isError ? { isError: true } : {}),
});

export default function kernelSubagents(pi: ExtensionAPI) {
  if (process.env.KERNEL_AGENT_SUBAGENTS_ENABLED === "false") return;
  const maxChildren = envInteger("KERNEL_AGENT_MAX_CHILDREN", 4, 1, 8);
  const timeoutMs = envInteger("KERNEL_AGENT_CHILD_TIMEOUT_MS", 120000, 1000, 600000);
  let control: AgentControl | undefined;
  let removeAbort: (() => void) | undefined;
  let cleanupTask: Promise<void> = Promise.resolve();
  let cleaning = false;
  const cleanup = async () => {
    const previous = control; control = undefined;
    removeAbort?.(); removeAbort = undefined;
    if (previous) {
      cleaning = true;
      cleanupTask = Promise.allSettled([cleanupTask, previous.dispose()]).then(() => { cleaning = false; });
    }
    await cleanupTask;
  };
  const current = (ctx: ExtensionContext) => {
    if (ctx.signal?.aborted) throw new Error("Parent turn has been cancelled");
    if (cleaning) throw new Error("Previous child cleanup is still in progress");
    const owner = ctx.sessionManager.getSessionId();
    if (control && control.owner !== owner) throw new Error("Child registry belongs to another session");
    if (!control) {
      const room = Boolean(process.env.SPARK_ROOM_WORKSPACE);
      const root = room ? process.env.SPARK_ROOM_WORKSPACE! : ctx.cwd;
      control = new AgentControl({ owner, root, room, maxChildren, timeoutMs,
        parentTools: pi.getActiveTools(), factory: spec => createChildSession(spec, ctx) });
      const signal = ctx.signal;
      const abort = () => { void cleanup(); };
      signal?.addEventListener("abort", abort, { once: true });
      removeAbort = () => signal?.removeEventListener("abort", abort);
    }
    return control;
  };
  pi.on("session_before_switch", cleanup);
  pi.on("session_before_fork", cleanup);
  pi.on("session_shutdown", cleanup);
  pi.on("session_start", cleanup);
  // agent_end can precede Pi auto-compaction or queued continuation. settled is
  // the parent operation boundary; abort and session replacement also clean up.
  pi.on("agent_settled", cleanup);
  pi.on("before_agent_start", event => ({ systemPrompt: `${event.systemPrompt}\n
You are Kernel Agent. For independent research, architecture, implementation or review, use spawn_agent
to create real child contexts; provide the goal, relevant file paths, constraints and expected evidence.
Use allowed_writes (exact files) only for private developer children; overlapping writes are rejected.
Children inherit a restricted subset of your tools and cannot run shell commands or spawn grandchildren.
Room children are read-only. Tester children inspect coverage; you execute the tests.
Use wait_agent to collect results (wait timeout does not cancel). send_input continues a child;
interrupt=true aborts its current run before follow-up. close_agent releases a slot after collecting results.
Before your final answer, wait for relevant children and inspect their work; report failures honestly.
Child CANN citation tokens are not registered in your gateway turn. Retrieve their doc_id/chunk_id
using your own CANN tools before citing evidence in the final answer; never blindly copy child tokens.
Children are turn-scoped: remaining work is cancelled when you finish, abort, or switch sessions.
Do not claim a child was dispatched unless spawn_agent returned a child ID.\n` }));

  function register(name: string, description: string, parameters: any,
    execute: (params: any, ctx: ExtensionContext, signal?: AbortSignal) => unknown | Promise<unknown>) {
    pi.registerTool({ name, label: `Kernel ${name}`, description, parameters,
      async execute(_id, params, signal, onUpdate, ctx) {
        try {
          if (signal?.aborted) throw new Error("Parent cancelled the tool");
          if (name === "wait_agent") onUpdate?.(result({ status: "waiting", ids: params.ids }));
          return result(await execute(params, ctx, signal));
        } catch (error: any) {
          return result({ error: error.message || "Kernel child operation failed" }, true);
        }
      },
    });
  }
  register("spawn_agent", "Start an independent Kernel Agent child. Returns immediately with its ID. Roles researcher/architect/reviewer/tester are read-only; developer requires allowed_writes. No shell, no nested children.",
    Type.Object({ message: Type.String({ minLength: 1, maxLength: 16000 }),
      role: Type.Optional(Type.Union(["researcher", "architect", "developer", "reviewer", "tester"].map(value => Type.Literal(value)))),
      allowed_writes: Type.Optional(Type.Array(Type.String(), { maxItems: 32 })) }),
    (params, ctx) => current(ctx).spawn(params));
  register("send_input", "Continue a child with additional instructions in its isolated context. Wait for completion first, or set interrupt=true to abort the running child before follow-up. Maximum four runs per child.",
    Type.Object({ id: Type.String(), message: Type.String({ minLength: 1, maxLength: 16000 }), interrupt: Type.Optional(Type.Boolean()) }),
    (params, ctx) => current(ctx).send(params.id, params.message, params.interrupt));
  register("wait_agent", "Wait for any selected child to finish; timeout returns current statuses without cancelling. Collect output and verify evidence before finalizing.",
    Type.Object({ ids: Type.Array(Type.String(), { minItems: 1, maxItems: 8 }), timeout_ms: Type.Optional(Type.Integer({ minimum: 0, maximum: 60000 })) }),
    (params, ctx, signal) => current(ctx).wait(params.ids, params.timeout_ms, signal));
  register("close_agent", "Cancel and dispose a child, preserving its bounded result in the registry and releasing its slot after cleanup. Closing does not undo writes.",
    Type.Object({ id: Type.String() }), (params, ctx) => current(ctx).close(params.id));
  register("list_agents", "List this parent turn's child states and bounded outputs; cannot access other conversations or old turns.",
    Type.Object({}), (_params, ctx) => ({ agents: current(ctx).list() }));
}
