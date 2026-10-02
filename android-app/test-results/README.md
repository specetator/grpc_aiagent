# Android local reliability and SQLite evidence

These artifacts use synthetic local data only. Tests run on WSL with JDK 17,
Robolectric 4.16.1, Android 28 and native SQLite. The APK compiles against Android 36.
They do not measure a physical phone, the Compose UI or end-to-end network latency.

Reproduce on a machine with an Android SDK and JDK 17:

```bash
cd android-app
./gradlew assembleDebug testDebugUnitTest --no-daemon --console=plain
```

Each benchmark writes 5,000 messages in 100 transactions of 50 messages, then
reads the most recent 200 messages 100 times. Final runs use exactly 1,024 UTF-8
payload bytes per message. JSON includes every transaction sample, write throughput,
p50/p95 transaction latency, mean timeline read latency and environment metadata.
No performance threshold is asserted: these are measurements on one local machine.

Artifacts:

- `android-sqlite-benchmark.json`: final implementation with separate indexed
  `msg_id`, `client_msg_id` and sequence equality lookups, within the same transaction.
- `android-sqlite-1kib-before-index.json`: same 1 KiB workload with the original
  combined OR identity predicate and no `msg_id` index.
- `android-sqlite-1kib-index-only.json`: same 1 KiB workload after adding the index.
  SQLite still selected a `(scope, session)` scan for the combined OR predicate;
  this isolated why adding an index alone did not fix growing conversation latency.
- `android-sqlite-short-text-initial.json`: first successful smoke benchmark with
  short synthetic text. Its throughput is not directly comparable to the 1 KiB runs.
- `android-*-junit.xml`: the actual eight test cases and failures/errors/skips counts.
- `android-*-build-test.log`: corresponding unedited Gradle build/test output.
- `android-build-summary.json`: final build/test counts, APK hash and source hashes.

The eight tests cover missing history prefixes, late gap filling, persistent sparse
receipts, process restart with a stable outbox ID, accepted-ACK transaction rollback,
retry backoff and explicit failure, account/server isolation and background sessions,
ephemeral delta rejection, self-sync ACK semantics and ACK reordering, plus the benchmark.
Repeated frames reconcile to a single local row. Both the received prefix and sparse
receipt queue survive process restart; only a server confirmation removes receipts.

No ADB device was attached and no emulator or phone UI exercise was performed.
