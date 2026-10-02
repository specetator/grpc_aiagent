package com.peco.sparkim

import android.content.Context
import org.json.JSONArray
import org.json.JSONObject
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import org.robolectric.annotation.SQLiteMode
import java.io.File

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
@SQLiteMode(SQLiteMode.Mode.NATIVE)
class SparkMessageStoreTest {
    private lateinit var context: Context
    private lateinit var store: SparkMessageStore
    private val scope = "http://server:9101|42"
    private val session = "s_42_43"

    @Before fun setup() {
        context = RuntimeEnvironment.getApplication()
        context.deleteDatabase("spark_messages.db")
        store = SparkMessageStore(context)
    }
    @After fun cleanup() { store.close(); context.deleteDatabase("spark_messages.db") }
    private fun wire(seq: Long, id: String = "client-$seq", sid: String = session): JSONObject = JSONObject()
        .put("type", "single_chat").put("session_id", sid).put("msg_id", if (seq > 0) "msg-$seq" else "")
        .put("client_msg_id", id).put("msg_seq", seq).put("sender_id", 42)
        .put("content", JSONObject().put("text", "message $seq"))
    private fun frame(id: String): JSONObject = JSONObject().put("type", "single_chat")
        .put("client_msg_id", id).put("to_user_id", 43).put("content", JSONObject().put("text", "offline"))

    @Test fun recentHistoryCannotSkipMissingPrefixAndLateMessagesFillGap() {
        store.save(scope, (51L..100L).map { wire(it) })
        assertEquals(0L, store.cursor(scope, session))
        assertEquals(50, store.receipts(scope).getValue(session).size)
        store.save(scope, (1L..49L).map { wire(it) })
        assertEquals(49L, store.cursor(scope, session))
        store.save(scope, listOf(wire(50)))
        assertEquals(100L, store.cursor(scope, session))
        assertEquals(100, store.timeline(scope, session).size)
    }

    @Test fun sparseReceiptsSurviveRestartAndOnlyServerConfirmationRemovesThem() {
        store.save(scope, listOf(wire(8), wire(2)))
        store.close(); store = SparkMessageStore(context)
        assertEquals(0L, store.cursor(scope, session))
        assertEquals(listOf(2L, 8L), store.receipts(scope).getValue(session))
        store.confirmReceipts(scope, session, 0L, listOf(8L))
        assertEquals(listOf(2L), store.receipts(scope).getValue(session))
        // A repeated frame is acknowledged again and remains one local row.
        store.save(scope, listOf(wire(8)))
        assertEquals(2, store.timeline(scope, session).size)
        assertEquals(listOf(2L, 8L), store.receipts(scope).getValue(session))
    }

    @Test fun processRestartReplaysSameIdAndAcceptedAckReconcilesOptimisticIdentity() {
        val id = "stable-id"
        store.enqueue(scope, frame(id), wire(0, id))
        store.close(); store = SparkMessageStore(context)
        val pending = store.due(scope, 0).single()
        assertEquals(id, pending.frame.getString("client_msg_id"))
        val cursor = store.accepted(scope, JSONObject().put("client_msg_id", id).put("msg_id", "accepted-id").put("msg_seq", 1))
        assertEquals(session to 1L, cursor)
        assertTrue(store.due(scope, Long.MAX_VALUE).isEmpty())
        store.save(scope, listOf(wire(1, id).put("msg_id", "accepted-id")))
        val messages = store.timeline(scope, session)
        assertEquals(1, messages.size)
        assertEquals("accepted-id", messages.single().wire.getString("msg_id"))
    }

    @Test fun malformedAckRollsBackAndTransientFailureKeepsStableRetryFrame() {
        val id = "retry-id"
        store.enqueue(scope, frame(id), wire(0, id))
        try {
            store.accepted(scope, JSONObject().put("client_msg_id", id).put("msg_id", "id-without-seq"))
            fail("malformed ack must fail")
        } catch (_: org.json.JSONException) { }
        val entry = store.due(scope, 0).single()
        store.recordAttempt(scope, entry, 1000)
        assertTrue(store.due(scope, 2999).isEmpty())
        assertEquals(id, store.due(scope, 3000).single().clientId)
        assertEquals(0L, store.timeline(scope, session).single().wire.getLong("msg_seq"))
        store.setState(scope, id, DeliveryState.FAILED)
        assertTrue(store.due(scope, Long.MAX_VALUE).isEmpty())
        assertEquals(DeliveryState.FAILED, store.timeline(scope, session).single().state)
        assertEquals(60_000L, outboxRetryDelay(1000))
    }

    @Test fun endpointAndAccountIsolationAndNonSelectedSessionPersistence() {
        val otherAccount = "http://server:9101|99"
        val otherServer = "http://other:9101|42"
        store.enqueue(scope, frame("a"), wire(0, "a"))
        store.enqueue(otherAccount, frame("a"), wire(0, "a"))
        store.save(otherServer, listOf(wire(1)))
        store.save(scope, listOf(wire(1, "background", "s_42_999")))
        assertEquals(0L, store.cursor(scope, session))
        assertEquals(1L, store.cursor(otherServer, session))
        assertEquals(1L, store.cursor(scope, "s_42_999"))
        assertEquals(1, store.due(otherAccount, 0).size)
        assertTrue(store.due(otherServer, 0).isEmpty())
        assertEquals(1, store.timeline(scope, "s_42_999").size)
    }

    @Test fun selfSyncOnlyMeansAcceptedAndDeliveredAckSurvivesAckReordering() {
        val id = "own-message-with-offline-receiver"
        store.enqueue(scope, frame(id), wire(0, id))
        // Sender-sync is the sender's durable local copy; the receiver is still offline.
        store.save(scope, listOf(wire(1, id)), ownUserId = 42L)
        assertEquals(DeliveryState.ACCEPTED, store.timeline(scope, session).single().state)
        store.setState(scope, id, DeliveryState.DELIVERED)
        store.accepted(scope, JSONObject().put("client_msg_id", id).put("msg_id", "msg-1").put("msg_seq", 1))
        store.save(scope, listOf(wire(1, id)), ownUserId = 42L)
        assertEquals(DeliveryState.DELIVERED, store.timeline(scope, session).single().state)
        assertTrue(store.due(scope, Long.MAX_VALUE).isEmpty())
    }

    @Test fun ephemeralDeltaCannotEnterDurableStoreAndBatchFailureRollsBackCursor() {
        try {
            store.save(scope, listOf(wire(1), wire(2).put("type", "ai_delta")))
            fail("delta must be rejected")
        } catch (_: IllegalArgumentException) { }
        assertTrue(store.timeline(scope, session).isEmpty())
        assertEquals(0L, store.cursor(scope, session))
        assertTrue(store.receipts(scope).isEmpty())
    }

    @Test fun benchmarkDurableTransactionsAndTimelineReads() {
        val elapsed = mutableListOf<Double>()
        val payload = "测".repeat(340) + "IMok" // Exactly 1024 UTF-8 bytes, no compression.
        val start = System.nanoTime()
        for (batch in 0 until 100) {
            val batchStart = System.nanoTime()
            store.save(scope, (1L..50L).map { wire(batch * 50 + it).put("content", JSONObject().put("text", payload)) })
            elapsed += (System.nanoTime() - batchStart) / 1_000_000.0
        }
        val totalMs = (System.nanoTime() - start) / 1_000_000.0
        assertEquals(5000L, store.cursor(scope, session))
        val readStart = System.nanoTime()
        repeat(100) { assertEquals(200, store.timeline(scope, session).size) }
        val readMs = (System.nanoTime() - readStart) / 1_000_000.0
        val sorted = elapsed.sorted()
        val hasIdentityIndex = store.readableDatabase.rawQuery(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='message_ids'", null
        ).use { it.moveToFirst() }
        val result = JSONObject().put("environment", "Robolectric Android 28 native SQLite on WSL; not a physical phone")
            .put("generated_at_utc", java.time.Instant.now().toString())
            .put("java_version", System.getProperty("java.version")).put("os_arch", System.getProperty("os.arch"))
            .put("messages", 5000).put("batch_size", 50).put("transactions", 100)
            .put("payload_bytes_per_message", payload.toByteArray(Charsets.UTF_8).size)
            .put("has_msg_id_index", hasIdentityIndex)
            .put("identity_lookup", "separate indexed equality predicates")
            .put("total_write_ms", totalMs).put("messages_per_second", 5_000_000.0 / totalMs)
            .put("transaction_p50_ms", sorted[49]).put("transaction_p95_ms", sorted[94])
            .put("timeline_read_200_average_ms", readMs / 100).put("transaction_samples_ms", JSONArray(elapsed))
        println("ANDROID_SQLITE_BENCHMARK=$result")
        System.getProperty("spark.results.dir")?.let { dir ->
            File(dir).mkdirs(); File(dir, "android-sqlite-benchmark.json").writeText(result.toString(2))
        }
    }
}
