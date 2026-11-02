/** Explicit public extension: request identity is captured from trusted launch configuration. */
import crypto from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import definitions from "../../room-context-tools.json";

const schemas: any = definitions;
const maximum = 2 * 1024 * 1024;
const hex = /^[a-f0-9]{64}$/;
const errors: Record<number, string> = {
  400: "Invalid bounded room tool parameters.", 401: "Room tool authentication is unavailable.",
  403: "Room access is revoked or authorization is unavailable.",
  404: "The requested source is not available in this room.",
  409: "Room context or source changed; start a new authorized request.",
  413: "Source response is too large; read a smaller range.", 416: "Requested range is outside the source.",
  429: "Request source budget exhausted; reuse an already-read range.",
  501: "Image input is unavailable for the active model/runtime. Use a verified vision-capable configuration; do not infer image contents from metadata.",
  503: "Room sources are temporarily unavailable; a read can be retried.",
};
class RoomError extends Error {
  constructor(code = 503) { super(errors[code] || errors[503]); }
}
const object = (value: any) => value !== null && typeof value === "object" && !Array.isArray(value);
const canonical = (value: any): any => Array.isArray(value) ? value.map(canonical) : object(value) ?
  Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value;
const hash = (data: string | Buffer) => crypto.createHash("sha256").update(data).digest("hex");

export function createRoomTools(inputScope: any, origin: string, key: string, transport = fetch) {
  const scope = JSON.parse(JSON.stringify(inputScope));
  const url = new URL(origin);
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash ||
      !["", "/"].includes(url.pathname) || typeof key !== "string" || key.length < 32 || key.length > 256 || /[\r\n]/.test(key))
    throw new RoomError(401);
  if (!object(scope) || scope.kind !== "room_agent_v1" || scope.policy !== "read_only" ||
      !/^ra_[a-f0-9]{64}$/.test(scope.request_id) || !/^[a-z][a-z0-9_-]{0,47}$/.test(scope.agent_id) ||
      typeof scope.project_id !== "string" ||
      !["room_id", "actor_user_id", "generation", "workspace_generation", "context_generation"].every(name =>
        Number.isSafeInteger(scope[name]) && scope[name] >= (["room_id", "actor_user_id"].includes(name) ? 1 : 0)) ||
      !new RegExp(`^r_${scope.room_id}-[1-9][0-9]*$`).test(scope.source_msg_id)) throw new RoomError(403);
  const expected = JSON.stringify(canonical(scope));
  const ceiling = Number(scope.source_msg_id.split("-").at(-1));

  async function post(route: string, body: any, bound: number, signal?: AbortSignal) {
    const serialized = JSON.stringify(body);
    if (Buffer.byteLength(serialized) > 4096) throw new RoomError(400);
    const timeout = AbortSignal.timeout(10000);
    const response = await transport(url.origin + route, {method: "POST", redirect: "error",
      signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
      headers: {"Content-Type": "application/json", "Authorization": "Bearer " + key}, body: serialized});
    if (!response.ok) throw new RoomError(response.status);
    if (!response.body) throw new RoomError();
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        size += next.value.length;
        if (size > bound) { await reader.cancel(); throw new RoomError(413); }
        chunks.push(next.value);
      }
    } finally { reader.releaseLock(); }
    const value = JSON.parse(new TextDecoder("utf-8", {fatal: true}).decode(Buffer.concat(chunks)));
    if (!object(value) || value.code !== 0) throw new RoomError(value?.code);
    return value.data;
  }
  async function authorize(signal?: AbortSignal) {
    const value = await post("/internal/room-agent/authorize", {request_id: scope.request_id}, 65536, signal);
    if (value?.request_id !== scope.request_id || JSON.stringify(canonical(value?.collaboration)) !== expected)
      throw new RoomError(403);
  }
  function parameters(name: string, args: any) {
    const schema = schemas[name].parameters;
    if (!object(args) || Object.keys(args).some(k => !Object.hasOwn(schema.properties, k)) ||
        (schema.required || []).some((k: string) => !Object.hasOwn(args, k))) throw new RoomError(400);
    for (const [name, value] of Object.entries(args)) {
      const rule = schema.properties[name];
      if (rule.type === "integer") {
        if (!Number.isSafeInteger(value) || (value as number) < rule.minimum || (value as number) > rule.maximum)
          throw new RoomError(400);
      } else if (typeof value !== "string" || value.includes("\0") || [...value].length < (rule.minLength || 0) ||
                 [...value].length > (rule.maxLength || 4096)) throw new RoomError(400);
      if ((rule.enum && !rule.enum.includes(value)) || (rule.pattern && !new RegExp(rule.pattern).test(value as string)))
        throw new RoomError(400);
    }
    if (Buffer.byteLength(args.query || "") > 240) throw new RoomError(400);
  }
  function result(name: string, args: any, value: any) {
    if (!object(value) || value.request_id !== scope.request_id || !Number.isSafeInteger(value.context_generation) ||
        value.context_generation !== scope.context_generation || !Array.isArray(value.items) || value.items.length > 60 ||
        (name === "room_read_media" && value.items.length !== 1)) throw new RoomError();
    const allowed: Record<string, string[]> = {all: ["message", "media", "fragment"], messages: ["message"], media: ["media"], fragments: ["fragment"]};
    for (const item of value.items) {
      const ref = item?.reference;
      if (!object(item) || !/^src_[a-f0-9]{64}$/.test(item.source_id) || !object(ref) || ref.room_id !== scope.room_id ||
          !["message", "media", "fragment", "image_metadata", "image"].includes(ref.kind) || !hex.test(ref.sha256))
        throw new RoomError();
      if ((name === "room_read" && ref.kind !== args.kind) ||
          (name === "room_search" && !allowed[args.type || "all"].includes(ref.kind))) throw new RoomError();
      if (ref.kind === "message") {
        if (!Number.isSafeInteger(ref.msg_seq) || ref.msg_seq <= 0 || ref.msg_seq > ceiling ||
            ref.msg_id !== `r_${scope.room_id}-${ref.msg_seq}` || typeof item.text !== "string" ||
            ref.length !== [...item.text].length || ref.length > (name === "room_read" ? (args.limit || 4096) : 300) ||
            !Number.isSafeInteger(ref.offset) || ref.offset < 0 || hash(item.text) !== ref.sha256 ||
            (name === "room_read" && (ref.msg_seq !== args.msg_seq || ref.offset !== (args.offset || 0)))) throw new RoomError();
      } else if (name !== "room_search" && (ref.asset_id !== args.asset_id || ref.revision !== args.revision)) throw new RoomError();
      if (name === "room_read" && ref.kind !== "message" && (!Number.isSafeInteger(ref.index) ||
          ref.index < (args.offset || 0) || ref.index >= (args.offset || 0) + (args.limit || 5))) throw new RoomError();
      if (name === "room_read_media") {
        if (ref.kind !== "image" || ref.index !== args.index || item.mime !== "image/jpeg" || typeof item.data !== "string" ||
            !object(item.metadata) || item.metadata.sha256 !== ref.sha256 ||
            JSON.stringify(canonical(item.metadata.anchor)) !== JSON.stringify(canonical(ref.anchor))) throw new RoomError();
        const bytes = Buffer.from(item.data, "base64");
        if (bytes.length > maximum || bytes.toString("base64") !== item.data || bytes.length < 5 ||
            bytes[0] !== 255 || bytes[1] !== 216 || bytes[2] !== 255 || bytes.at(-2) !== 255 || bytes.at(-1) !== 217 ||
            hash(bytes) !== ref.sha256) throw new RoomError();
      } else if (Object.hasOwn(item, "data")) throw new RoomError();
    }
    return value;
  }
  return Object.entries(schemas).map(([name, spec]: [string, any]) => ({
    name, label: name, description: spec.description, parameters: spec.parameters,
    async execute(_id: string, input: any, signal?: AbortSignal, _update?: unknown,
                  context?: Pick<ExtensionContext, "model">) {
      try {
        parameters(name, input);
        // Runtime-owned model metadata, never a model-supplied tool argument.
        if (name === "room_read_media" && (!Array.isArray(context?.model?.input) ||
            !context.model.input.includes("image"))) throw new RoomError(501);
        const args = JSON.parse(JSON.stringify(input));
        await authorize(signal);
        const value = result(name, args, await post("/internal/room-agent/context/" + spec.operation,
          {...args, request_id: scope.request_id}, maximum, signal));
        await authorize(signal);
        const clean = {...value, items: value.items.map(({data, ...rest}: any) => rest)};
        return {content: [{type: "text", text: JSON.stringify(clean)}, ...value.items.filter((item: any) => item.data)
          .map((item: any) => ({type: "image", mimeType: item.mime, data: item.data}))],
          details: {request_id: scope.request_id, source_ids: value.items.map((item: any) => item.source_id)}};
      } catch (error) { throw error instanceof RoomError ? error : new RoomError(); }
    }
  }));
}

export default function(pi: ExtensionAPI) {
  const tools = createRoomTools(JSON.parse(process.env.SPARK_PUBLIC_SCOPE || "null"),
    process.env.SPARK_WORKSPACE_AUTH_ORIGIN || "", process.env.SPARK_PUSH_WORKSPACE_AUTH_KEY || "");
  for (const tool of tools) pi.registerTool(tool as any);
}
