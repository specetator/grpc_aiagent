# Spark Push Agent Instructions

## Project truth

- Treat root `comet/`, `logic/`, `job/`, `common/`, `proto/`,
  `hermes_bridge/`, `web_demo/`, `tests/`, and `docs/` as the current
  implementation.
- `02_auth_subset/` through `06/` and `5.1_room_subset/` are historical
  teaching snapshots. Read them only for an explicit evolution comparison.
- Start project questions with `spark-push-knowledge`. Prefer current code and
  protocol definitions over prose, and identify any mismatch.

## Reliability contracts

- Keep `accepted_ack` and `delivered_ack` distinct. The former confirms the
  persistence event reached Kafka; the latter confirms a target WebSocket
  connection accepted downstream delivery.
- Changes to `msg_seq`, `client_msg_id`, delivered cursors, gap sync, or offline
  sync must be checked against persistence, reconnect, duplicate delivery, and
  history behavior.
- `ai_delta` is ephemeral UI output: it is not stored and cannot advance a
  cursor. Only final `ai_reply` enters normal message persistence and delivery.
- Spark Push owns authentication, membership, sequencing, idempotency, history,
  and delivery. Pi Agent owns reasoning, tools, Skills, and model access.

## Pi Agent workflow

- Use `spark-push-agent` for non-trivial design, implementation, review, or test
  work. Simple questions can be answered directly after loading the knowledge
  Skill.
- Keep provider-specific HTTP/SSE handling in `hermes_bridge`; it talks to the
  local Pi gateway at `/v1/chat/completions`, not a raw model endpoint.
- Keep API keys in untracked environment files. Never write keys to source,
  examples, prompts, tests, logs, or Skill references.
- When changing code, inspect first, make the smallest coherent change, and run
  the narrowest relevant tests before broader validation.
- Do not edit generated build output or vendored Muduo/nlohmann sources unless
  the user explicitly places them in scope.

## Common validation

```bash
python3 cannbot/scripts/validate_knowledge.py --strict
python3 cannbot/scripts/sync_pi_agent.py --check
python3 cannbot/scripts/test_pi_citations.py
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure
```
