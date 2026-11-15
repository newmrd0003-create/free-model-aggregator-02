// free-model-aggregator-02 entry: one server, two client wires, a real free model behind
// every answer, routed by the brief's published ratings.
//
// Built from the kit starter (starter/main.ts + starter/lib/): the starter's wires, catalog,
// register and poll are kept as-is. What changed, and why:
//   - quality (60%): candidates are ranked by rating_milli (best first, ties by model id), so
//     every prompt is served by the best-rated free model that meets its requirements:
//     inkling-2-small (1475) for plain/tool prompts, nemotron-3.5-super-vl (1303) for JSON-mode.
//   - failover (10%): a refused gateway call (429/4xx/5xx/unreachable) fails over to the next
//     best candidate; a 5xx is retried once on the same model first, because a refusal can be
//     one failing call. Every refusal is reported in x-makefun-upstream-failures, so the
//     failover column sees recovered refusals.
//   - tie-break (upstream calls): only gateway calls the provider ANSWERED are counted, so the
//     server dials exactly once per prompt when the first model answers with the prompt's
//     token. A refused call costs nothing on the tie-break; a second SUCCESSFUL call for one
//     prompt costs one. So a re-dial happens only when the answer is unusable (no token),
//     where rescuing the prompt's quality (60% of the score) is worth far more than one call.
//   - streams are buffered, then re-emitted in order, so a failed stream can still fail over
//     and the token check runs before anything is sent to the client.
//   - determinism: no wall clock, no unseeded randomness. Routing derives only from the brief,
//     the request bodies and the gateway's answers, so a replay leaves the same state export.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import {
  buildCatalog,
  capabilitiesOf,
  freeModels,
  isRecord,
  meetsRequirements,
  readBrief,
  type CatalogReport,
  type Json,
  type ModelRow,
  type Requirement,
} from './lib/providers.ts';
import {
  anthropicError,
  anthropicEventsFromOpenAI,
  anthropicModelList,
  anthropicToOpenAI,
  openAIError,
  openAIJsonWithModel,
  openAIModelList,
  openAIStreamWithModel,
  openAIToAnthropic,
  type ServedModel,
} from './lib/wire.ts';

/** The wire protocol's version, in the register body. */
const PROTOCOL_VERSION = 1;
/** How often GET /work is polled, in milliseconds. */
const POLL_INTERVAL_MS = 100;
/** The most bytes of one client request body this baseline reads. */
const MAX_BODY_BYTES = 8 * 1024 * 1024;
/** How long one upstream dial may take; the client waits 30 s per prompt, so 4 attempts fit. */
const UPSTREAM_TIMEOUT_MS = 7000;
/** Most dials tried for one prompt (first choice + failovers + one same-model retry). */
const MAX_ATTEMPTS = 4;
/** The env reads recovered refusals from this response header (at most 8 rows). */
const FAILURES_HEADER = 'x-makefun-upstream-failures';
const MAX_TRAIL_ROWS = 8;

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const PROMPT_FILE = process.env.MAKEFUN_PROMPT_FILE ?? '';
const GATEWAY_URL = (process.env.OPENAI_BASE_URL ?? '').replace(/\/+$/, '');
/** The trial's dummy key for the entry's gateway lane; the gateway swaps it for the entrant's own key. */
const GATEWAY_KEY = process.env.OPENAI_API_KEY ?? '';

const brief = readBrief(PROMPT_FILE);
const catalog: CatalogReport = buildCatalog(brief);
/** Which prompt row the request now arriving is: the brief's prompts are in the client's arrival order. */
let manifestIndex = 0;

function log(message: string): void {
  process.stderr.write(`aggregator: ${message}\n`);
}

/**
 * The gateway's Chat Completions route. OPENAI_BASE_URL is the OpenAI-compatible root (`…/v1` on the
 * trial network); a base that does not already end in `/v1` gets one, so both spellings reach the route.
 */
function chatCompletionsUrl(): string {
  if (GATEWAY_URL === '') return '';
  return GATEWAY_URL.endsWith('/v1') ? `${GATEWAY_URL}/chat/completions` : `${GATEWAY_URL}/v1/chat/completions`;
}

/** The free models the brief lists, as GET /v1/models names them. */
function servedModels(): ServedModel[] {
  return freeModels(brief).map((model) => ({ id: model.id, provider: model.provider, display_name: model.display_name }));
}

/**
 * Every free model that meets the request's requirements, best-rated first (ties by model id,
 * so the order is the same on a replay). Models with no sealed rating are skipped: the
 * grader cannot score them.
 */
function candidatesFor(requires: Requirement): ModelRow[] {
  const out: ModelRow[] = [];
  for (const model of freeModels(brief)) {
    if (model.rating_milli <= 0) continue;
    if (!meetsRequirements(requires, capabilitiesOf(model))) continue;
    out.push(model);
  }
  out.sort((a, b) => b.rating_milli - a.rating_milli || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  return out;
}

/** The requirements of the prompt now arriving: the brief's row by arrival order, else the body's own. */
function requirementsFor(body: Json): Requirement {
  const entry = brief.prompts[manifestIndex];
  manifestIndex += 1;
  if (entry !== undefined) return entry.requires;
  // The brief's prompt list is the declared source of each request's requirements; this
  // fallback is a guess for a brief that holds none.
  return requirementsFromBody(body);
}

/** Requirements read off a request body, when the brief has no row for it. */
function requirementsFromBody(body: Json): Requirement {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  let vision = false;
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (isRecord(block) && (block.type === 'image' || block.type === 'image_url')) vision = true;
    }
  }
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  return { tools: Array.isArray(body.tools) && body.tools.length > 0, vision, json: format === 'json_object' || format === 'json_schema' };
}

/**
 * The prompt's unique token(s), read off the request body: the user instruction carries one
 * (e.g. COMET-3884), and the answer must carry it back. No token found means no check.
 */
function tokensOf(body: Json): string[] {
  const seen = new Set<string>();
  for (const m of JSON.stringify(body).matchAll(/[A-Z]{2,}-[0-9]{3,}/g)) seen.add(m[0]);
  return [...seen];
}

/** One client request body, or null when it is not one JSON object or is too large. */
async function readJson(req: IncomingMessage): Promise<Json | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buffer.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(buffer);
  }
  if (chunks.length === 0) return null;
  try {
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown, extraHeaders: Record<string, string> = {}): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text), ...extraHeaders });
  res.end(text);
}

function sendError(res: ServerResponse, wire: 'openai' | 'anthropic', status: number, type: string, message: string): void {
  if (wire === 'anthropic') sendJson(res, status, anthropicError(type, message));
  else sendJson(res, status, openAIError(message, type, null));
}

/** GET /v1/models, in the shape of the auth header the client sent. */
function models(req: IncomingMessage, res: ServerResponse): void {
  const list = servedModels();
  if (typeof req.headers['x-api-key'] === 'string') sendJson(res, 200, anthropicModelList(list));
  else sendJson(res, 200, openAIModelList(list));
}

/** The refusal trail, as the x-makefun-upstream-failures header value (empty string when none). */
function trailHeader(failures: { status: number; code: string; model: string | null }[]): Record<string, string> {
  if (failures.length === 0) return {};
  return { [FAILURES_HEADER]: JSON.stringify(failures.slice(0, MAX_TRAIL_ROWS)) };
}

/** POST one request to the gateway's Chat Completions route, naming the model this entry dialled. */
async function dial(model: string, body: Json): Promise<Response> {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    authorization: `Bearer ${GATEWAY_KEY}`,
  };
  if (body.stream === true) headers.accept = 'text/event-stream';
  return fetch(chatCompletionsUrl(), {
    method: 'POST',
    headers,
    body: JSON.stringify({ ...body, model }),
    signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
  });
}

/** The error code of a refused gateway call, from its body when it names one. */
function refusalCode(text: string, status: number): string {
  try {
    const value: unknown = JSON.parse(text);
    if (isRecord(value) && isRecord(value.error) && typeof value.error.code === 'string' && value.error.code !== '') {
      return value.error.code.slice(0, 64);
    }
  } catch {
    // Not JSON: fall through to the status-based code.
  }
  if (status === 429) return 'rate_limited';
  if (status === 402) return 'payment_required';
  if (status >= 500) return 'server_error';
  return 'upstream_error';
}

/** Text of a Chat Completions JSON answer (the provider's message content). */
function openAIAnswerText(body: Json): string {
  const choices = Array.isArray(body.choices) ? body.choices : [];
  const choice = isRecord(choices[0]) ? choices[0] : {};
  const message = isRecord(choice.message) ? choice.message : {};
  if (typeof message.content === 'string') return message.content;
  if (Array.isArray(message.content)) {
    return message.content.filter((p): p is Json => isRecord(p) && typeof p.text === 'string').map((p) => p.text as string).join('');
  }
  if (typeof choice.text === 'string') return choice.text;
  return '';
}

/** Text of an Anthropic JSON answer (its text blocks joined). */
function anthropicAnswerText(body: Json): string {
  if (!Array.isArray(body.content)) return '';
  return body.content.filter((b): b is Json => isRecord(b) && b.type === 'text' && typeof b.text === 'string').map((b) => b.text as string).join('');
}

/** Text carried by buffered OpenAI SSE frames (choice deltas joined). */
function openAIStreamText(frames: string[]): string {
  let out = '';
  for (const frame of frames) {
    for (const line of frame.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data:')) continue;
      const payload = trimmed.slice(5).trim();
      if (payload === '' || payload === '[DONE]') continue;
      try {
        const chunk: unknown = JSON.parse(payload);
        if (!isRecord(chunk) || !Array.isArray(chunk.choices)) continue;
        const delta = isRecord(chunk.choices[0]) ? (chunk.choices[0] as Json).delta : null;
        if (isRecord(delta) && typeof delta.content === 'string') out += delta.content;
      } catch {
        // Not JSON: ignore the line.
      }
    }
  }
  return out;
}

/** Text carried by buffered Anthropic SSE frames (text deltas joined). */
function anthropicStreamText(frames: string[]): string {
  let out = '';
  for (const frame of frames) {
    for (const line of frame.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data:')) continue;
      const payload = trimmed.slice(5).trim();
      if (payload === '') continue;
      try {
        const event: unknown = JSON.parse(payload);
        if (!isRecord(event) || event.type !== 'content_block_delta') continue;
        const delta = isRecord(event.delta) ? event.delta : {};
        if (delta.type === 'text_delta' && typeof delta.text === 'string') out += delta.text;
      } catch {
        // Not JSON: ignore the line.
      }
    }
  }
  return out;
}

interface FailureRow {
  status: number;
  code: string;
  model: string | null;
}

interface ServedAnswer {
  /** The translated answer, ready to send: a JSON body, or buffered SSE frames. */
  json: Json | null;
  frames: string[] | null;
  /** The answer's text, for the token check. */
  text: string;
}

/**
 * One successful upstream answer, translated into the client's wire and buffered whole, so a
 * failed stream can still fail over and the token check runs before anything is sent.
 * Returns null when the answer cannot be read or translated.
 */
async function translateAnswer(
  wire: 'openai' | 'anthropic',
  stream: boolean,
  upstream: Response,
  model: string,
): Promise<ServedAnswer | null> {
  try {
    if (!stream) {
      const answer: unknown = await upstream.json();
      if (!isRecord(answer)) return null;
      if (wire === 'openai') {
        const body = openAIJsonWithModel(answer, model);
        return { json: body, frames: null, text: openAIAnswerText(body) };
      }
      const body = openAIToAnthropic(answer, model);
      return { json: body, frames: null, text: anthropicAnswerText(body) };
    }
    if (upstream.body === null) return null;
    if (wire === 'openai') {
      const frames: string[] = [];
      for await (const frame of openAIStreamWithModel(upstream.body, model)) frames.push(frame);
      return { json: null, frames, text: openAIStreamText(frames) };
    }
    const frames: string[] = [];
    for await (const event of anthropicEventsFromOpenAI(upstream.body, model)) frames.push(event);
    return { json: null, frames, text: anthropicStreamText(frames) };
  } catch {
    return null;
  }
}

/**
 * Serve one client request: dial the best-rated free model that meets it, fail over on a
 * refusal, and answer only with a completion that carries the prompt's token. A refused call
 * costs nothing on the tie-break (only answered calls count), so retries and failovers are
 * free there; a second ANSWERED call for one prompt costs one, so it happens only when the
 * first answer is unusable.
 */
async function serve(wire: 'openai' | 'anthropic', body: Json, res: ServerResponse): Promise<void> {
  const requires = requirementsFor(body);
  const ranked = candidatesFor(requires);
  if (ranked.length === 0) {
    return sendError(res, wire, 503, wire === 'anthropic' ? 'overloaded_error' : 'no_free_model', 'no free model in the brief meets this request');
  }
  const tokens = tokensOf(body);
  const stream = body.stream === true;
  const upstreamBody: Json = wire === 'anthropic' ? anthropicToOpenAI(body, '') : { ...body };
  const failures: FailureRow[] = [];
  const tried = new Set<string>();
  const retried = new Set<string>();
  let attempts = 0;
  while (attempts < MAX_ATTEMPTS) {
    const cand = ranked.find((c) => !tried.has(c.id)) ?? null;
    if (cand === null) break;
    attempts += 1;
    let upstream: Response;
    try {
      upstream = await dial(cand.id, { ...upstreamBody, model: cand.id });
    } catch {
      // Unreachable or timed out: record it and fail over; the call never reached the
      // provider, so it costs nothing on the tie-break.
      failures.push({ status: 502, code: 'upstream_unreachable', model: cand.id });
      tried.add(cand.id);
      continue;
    }
    tried.add(cand.id);
    if (!upstream.ok || upstream.body === null) {
      const text = await upstream.text().catch(() => '');
      failures.push({ status: upstream.status, code: refusalCode(text, upstream.status), model: cand.id });
      // A 5xx can be one failing call: retry the same model once before failing over.
      // A 429 or 4xx will not recover on an immediate retry: fail over at once.
      if (upstream.status >= 500 && !retried.has(cand.id)) {
        retried.add(cand.id);
        tried.delete(cand.id);
      }
      continue;
    }
    const served = await translateAnswer(wire, stream, upstream, cand.id);
    if (served === null) {
      // The provider answered, so the call counts — but its body is unusable, and an
      // answer without a token earns nothing: try the next candidate.
      failures.push({ status: 502, code: 'bad_upstream_body', model: cand.id });
      continue;
    }
    if (tokens.length > 0 && !tokens.some((t) => served.text.includes(t))) {
      // Answered, but the provider's body does not carry the prompt's token: the prompt
      // would earn nothing on quality, so spend one more counted call on the next model.
      log(`token missing from ${cand.id} answer for ${tokens.join(',')}: failing over`);
      continue;
    }
    if (stream && served.frames !== null) {
      res.writeHead(200, {
        'content-type': 'text/event-stream',
        'cache-control': 'no-cache',
        connection: 'keep-alive',
        ...trailHeader(failures),
      });
      for (const frame of served.frames) res.write(frame);
      res.end();
      return;
    }
    if (served.json !== null) {
      sendJson(res, 200, served.json, trailHeader(failures));
      return;
    }
    // Unreachable: a translated answer is always one of the two shapes above.
    return sendError(res, wire, 500, 'internal_error', 'the server failed to answer');
  }
  return sendError(res, wire, 502, wire === 'anthropic' ? 'api_error' : 'upstream_error', 'every free model that meets this request refused it');
}

/** POST /v1/chat/completions: an OpenAI-shaped client, answered by a real free model. */
async function chat(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  if (body === null) return sendError(res, 'openai', 400, 'invalid_request_error', 'the request body is not one JSON object');
  await serve('openai', body, res);
}

/** POST /v1/messages: an Anthropic-shaped client, rewritten to Chat Completions and answered back. */
async function messages(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  if (body === null) return sendError(res, 'anthropic', 400, 'invalid_request_error', 'the request body is not one JSON object');
  await serve('anthropic', body, res);
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') {
    models(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') {
    await chat(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/messages') {
    await messages(req, res);
    return;
  }
  sendError(res, url.pathname === '/v1/messages' ? 'anthropic' : 'openai', 404, 'not_found', `no route ${req.method ?? ''} ${url.pathname}`);
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '0.0.0.0', () => resolve());
  });
}

/** POST /register with the port and the one catalog; the env answers with the trial's prompt count. */
async function register(port: number, report: CatalogReport): Promise<void> {
  const res = await fetch(`${ENV_URL}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog: report }),
    signal: AbortSignal.timeout(5000),
  });
  if (!res.ok) throw new Error(`POST ${ENV_URL}/register answered HTTP ${res.status}: ${(await res.text()).slice(0, 500)}`);
  const reply: unknown = await res.json();
  if (isRecord(reply) && typeof reply.requests === 'number') log(`registered: the trial holds ${reply.requests} prompts`);
}

/** Poll GET /work every 100 ms; exit 0 the moment it says done. */
function poll(server: Server): void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status !== 200) return;
        const work: unknown = await res.json();
        if (isRecord(work) && work.done === true) {
          clearInterval(timer);
          server.close();
          process.exit(0);
        }
      } catch {
        // The env is not reachable yet: try again on the next tick.
      } finally {
        inFlight = false;
      }
    })();
  }, POLL_INTERVAL_MS);
}

async function start(): Promise<void> {
  if (brief.error !== null) log(brief.error);
  if (process.argv.includes('--list')) {
    // The same document the register body carries, byte for byte: print the one built object and stop
    // before any server starts. A process whose stdout is not flushed loses the tail, so wait for it.
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  if (ENV_URL === '') throw new Error('MAKEFUN_ENV_URL is not set');
  if (chatCompletionsUrl() === '') throw new Error('OPENAI_BASE_URL is not set');
  const server = createServer((req, res) => {
    void handle(req, res).catch((err: unknown) => {
      log((err as Error).stack ?? String(err));
      if (!res.headersSent) sendError(res, req.url?.startsWith('/v1/messages') === true ? 'anthropic' : 'openai', 500, 'internal_error', 'the server failed to answer');
    });
  });
  await listen(server);
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('the server has no port');
  const port = address.port;
  // 8080 is the env's on one layout; the OS's ephemeral range never holds it, and the guard says so.
  if (port === 8080) throw new Error('the OS picked 8080, which the env owns on one layout: restart the trial');
  log(`listening on 0.0.0.0:${port} with ${servedModels().length} free models`);
  await register(port, catalog);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
  poll(server);
}

try {
  await start();
} catch (err) {
  log((err as Error).stack ?? String(err));
  process.exit(1);
}
