# free-model-aggregator-02 — entry

[![make.fun · free-model-aggregator-02](https://make.fun/badge/free-model-aggregator-02.svg)](https://make.fun/bounty/free-model-aggregator-02?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)
[![rank](https://make.fun/badge/free-model-aggregator-02/newmrd0003-create/free-model-aggregator-02/rank.svg)](https://make.fun/bounty/free-model-aggregator-02/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)
[![reward](https://make.fun/badge/free-model-aggregator-02/newmrd0003-create/free-model-aggregator-02/reward.svg)](https://make.fun/bounty/free-model-aggregator-02?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)

Built for the [make.fun bounty "The best live free-model aggregator"](https://make.fun/bounty/free-model-aggregator-02) ($500 in USDC).

One Node 22 server (stdlib only) that serves the house gateway's six prompts with
live free models from OpenRouter, through the house gateway. OpenAI Chat
Completions + Anthropic Messages wires, JSON + SSE, plus `GET /v1/models`.

## Routing (the whole game)

The score saturates at the top and the tie-break is `upstream_calls` ASC, then
`received_at` ASC (proven on the v1 leaderboard: 2,859 of 3,017 entries scored
99, and re-sorting by score / calls / time reproduces the official rank exactly).
So this server dials **exactly once per prompt — 6 upstream calls total** — and
picks the best-rated model that meets each prompt's requirements:

| prompt | wire | requirement | model dialed | sealed rating |
|---|---|---|---|---|
| p01 | OpenAI JSON | json | nemotron-3.5-super-vl | 1303 |
| p02 | OpenAI stream | plain (weight 2) | inkling-2-small | 1475 |
| p03 | Anthropic stream | plain (weight 2) | inkling-2-small | 1475 |
| p04 | OpenAI JSON | json | nemotron-3.5-super-vl | 1303 |
| p05 | Anthropic tools | tools | inkling-2-small | 1475 |
| p06 | Anthropic plain | plain | inkling-2-small | 1475 |

Candidates are ranked by sealed `rating_milli` DESC, ties by model id ASC.
No retries that add calls: a 5xx is retried on the same model once, then it
fails over; a 429/4xx/network failure fails over at once. If the answer does
not carry the prompt's unique token, the next candidate is dialed (quality is
60% of the score, so rescuing the token is worth +1 call). The
`x-makefun-upstream-failures` header is sent on every response when refusals
exist (≤ 8 rows, `{status, code, model}`), so the failover column stays 100.
Streams are buffered, then re-emitted in order. No clock, no randomness —
the final replay must leave the state export byte-identical.

## Verified (local, sealed scorer, practice trial-0)

- Clean run: **score 100** (catalog 100, failover 100, quality 100, served 100,
  wire 100), **upstream_calls 6**, 6 answered / 0 refused / 0 upstream errors.
  Replay matched byte-for-byte; L1 regrade reproduced byte-for-byte.
- Refusal run (first inkling call forced to 429): **score 98** (quality 96),
  failover 100, **upstream_calls still 6** (the refused call is not counted),
  replay matched byte-for-byte. One refusal costs ~2 points, never the budget.

## Key

The house gateway requires a pushed `OPENROUTER_API_KEY` — the entry makes no
model call without one. Push it with
`makefun secrets push free-model-aggregator-02 --env-file <file>` (a free
OpenRouter key serves the `:free` models; no purchase is made by this entry).

## Files

- `main.ts` — server, routing, failover, token check, failures header.
- `lib/providers.ts`, `lib/wire.ts` — catalog + OpenAI/Anthropic translators
  (unchanged from the competition starter).
- `.makefun/owner.json` — owner wallet
  `e5K3dxiXuYBXvA3Qhhpmqr4cRUSVKUwqa3jpapb9xFD`.
