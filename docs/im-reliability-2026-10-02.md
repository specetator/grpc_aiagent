# IM reliability upgrade (2026-10-02)

This document describes the canonical root implementation. Historical subset
directories retain their original teaching behavior.

## Reliable delivery

Ordinary single/room messages and final AI replies use one reliable path:
Logic reserves an immutable message identity, waits for Kafka's persistence
delivery report, and returns `accepted_ack`. Job commits the SQL message, delivery
task and session metadata in one transaction. A task becomes dispatchable only
when its message exists in MySQL. Independent message keys persist in bounded
parallel batches; each successful batch commits the highest durable offset plus
one per partition in one Kafka request. Multi-partition offset commits are not a
global atomic transaction; idempotent SQL replay makes partial commits safe.
Temporary SQL failures retry with bounded backoff; cancellation leaves the
offset for replay. Malformed events and conflicting immutable identities retain
the existing durable dead-letter policy.

The reservation and per-session sequence ledger commit in one SQL transaction.
They preserve the first payload, timestamp and sequence even when Kafka has
accepted a message but Job has not yet inserted history and Redis is lost.
Client IDs compare as bytes, including case. Redis is an unread/frontier cache;
it no longer independently allocates new messages in the canonical hot path.
The reservation alone does not generate an accepted ACK.

Eight bounded dispatch workers (configurable 1–32) claim SQL tasks with expiring ownership tokens. Completion,
renewal and retry require the same unexpired token. A failed Comet RPC/backpressure
retries automatically; a stopped worker's task is reclaimed after its lease expires.
An unfinished persisted message blocks later messages in its own session; other
sessions can proceed. A malformed task without a corresponding message does not
block that session. Duplicate pushes are allowed and clients deduplicate `msg_id`.
Recipient membership is captured at acceptance; each attempt resolves current
leased user routes. Offline recipients recover through device sync.

Sender synchronization is a separate push without delivery ACK fields.
`delivered_ack` still means a receiver's WebSocket queue accepted the message.
Neither this ACK nor a server push advances a device receive cursor or user read
cursor. AI deltas remain ephemeral and never enter the reliable database path.

## Device receipts

New clients connect with `device_id=<stable-id>&receive_ack=1`. IDs contain
1–64 ASCII letters/digits or `._-`. Comet supplies the authenticated user and
device identity; client fields cannot select another user's receipt state.

After a local transaction commits, clients send:

```json
{"type":"received_ack","session_id":"s_1_2","msg_seq":1,"received_seqs":[3]}
```

`msg_seq` is a strictly contiguous prefix; `received_seqs` contains at most 256
individually persisted messages. SQL stores the monotonic prefix and sparse
receipts together. The example does not hide a late-arriving sequence 2. Sparse
receipts also prevent permanent allocation gaps from causing endless duplicate
downloads. They compact only beneath an explicitly confirmed prefix. The client
retries receipts until `received_ack_ok`. Membership and persisted sequence bounds
are checked server-side. A new device starts at zero even if recent history is loaded.

Comet syncs a device after handshake, every three seconds, and after receipt ACK
when an earlier sync had another page. Sync targets that connection alone.
Legacy clients without the opt-in retain the existing user-level delivery cursor.

## Android local state

`SparkMessageStore` uses SQLite transactions for messages, contiguous cursors,
pending receipts and a durable send queue. Database scopes include endpoint and
user. Messages use the server message identity; outgoing retries keep one stable
client ID across disconnects and process restart. Local attachment bytes are
saved before network upload. An accepted ACK saves the authoritative message and
removes the outgoing item atomically. Delivery state advances monotonically;
self synchronization only implies accepted. AI deltas are excluded from storage
and receipt generation.

The timeline and conversation list load from SQLite offline. Queue capacity and
retry limits are bounded; rejected or exhausted sends remain visible. Drafts are
cleared only after successful durable enqueue. Account changes close old sockets.

## Connection limits and routes

User connection registries have 64 independent buckets and room locks are
separate. Authentication and receipt RPCs execute in a bounded worker queue.
Each connection budgets EventLoop-queued frames plus published socket-buffer
bytes. Default budget is 8 MiB/1024 frames, accommodating the existing 4 MiB wire
limit. High-water or admission failures close slow connections; push RPCs return
503 so the SQL task remains retryable. The RPC executor defaults to 256 tasks and
16 MiB; stream queues and pending requests are also bounded.

User routes are 60-second leases refreshed every ten seconds in batches of at
most 256. Generations contain a Comet boot identity and connection counter. Old
disconnects cannot remove a newer route, and older authentication completions
cannot replace a newer generation from the same boot. Closing a second handshake
preserves the route of an existing live connection.
If registration disappears, heartbeat requests require a reconnect through
token authentication; they cannot recreate revoked routes without credentials.

## Validation and deployment

Build and CTest run under WSL in Release mode. MySQL tests require explicit
`SPARK_PUSH_RUN_MYSQL_TESTS=1` and a database distinct from `spark_push`.
`tests/prepare_im_lab.py` prepares localhost-only configurations, a separate test
database, Redis DB 14 and Kafka ports 19092/19093. Its temporary manifest contains
credentials and must remain outside Git. `tests/im_e2e_performance.py` records
recipient-observed latency, raw samples, connection tests and recovery assertions.
The performance report states measured results and limitations separately.

Deploy Logic/Job/Comet together from the same proto revision. Job must use the
same Redis database as Logic. New schema is additive and startup ensures it;
`sql/migrations/005_device_delivery.sql` supports managed deployments. Keep Kafka,
Redis and MySQL persistence configured and monitored. Completed delivery tasks
are retained for replay idempotency; production retention/archival is an operational
choice. An ACK cannot promise user reading or recovery after deleting every
durable copy.

When upgrading an existing deployment, stop old Logic acceptance first and let
old Job drain the persistence-topic backlog before switching all three services.
The durable reservation contract starts with messages allocated by the new
Logic; a pre-upgrade Redis-only identity cannot be reconstructed from an
unconsumed Kafka event using a database lookup alone.
