package com.peco.sparkim

import android.content.ContentValues
import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import org.json.JSONObject

data class StoredWire(val wire: JSONObject, val state: DeliveryState?)
data class OutboxEntry(val clientId: String, val sessionId: String, val frame: JSONObject, val attempts: Int)

/** SQLite is the source of truth for receive progress and sends awaiting accepted_ack.
 * Every method is serialized; callers run on the ordered event worker, never the UI thread.
 * Scope is endpoint + account. A stable installation device ID is maintained separately.
 */
class SparkMessageStore(context: Context) : SQLiteOpenHelper(context, "spark_messages.db", null, 1) {
    override fun onConfigure(db: SQLiteDatabase) { db.enableWriteAheadLogging() }
    override fun onCreate(db: SQLiteDatabase) {
        db.execSQL("CREATE TABLE messages(scope TEXT NOT NULL, session TEXT NOT NULL, identity TEXT NOT NULL, msg_id TEXT NOT NULL, client_id TEXT NOT NULL, seq INTEGER NOT NULL, wire TEXT NOT NULL, state TEXT, PRIMARY KEY(scope,session,identity))")
        db.execSQL("CREATE INDEX message_sequences ON messages(scope,session,seq)")
        db.execSQL("CREATE INDEX message_ids ON messages(scope,session,msg_id)")
        db.execSQL("CREATE INDEX message_clients ON messages(scope,client_id)")
        db.execSQL("CREATE TABLE device_cursors(scope TEXT NOT NULL,session TEXT NOT NULL,received_seq INTEGER NOT NULL,PRIMARY KEY(scope,session))")
        db.execSQL("CREATE TABLE receive_receipts(scope TEXT NOT NULL,session TEXT NOT NULL,seq INTEGER NOT NULL,PRIMARY KEY(scope,session,seq))")
        db.execSQL("CREATE TABLE outbox(scope TEXT NOT NULL,client_id TEXT NOT NULL,session TEXT NOT NULL,frame TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_at INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'PENDING',PRIMARY KEY(scope,client_id))")
    }
    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) = Unit

    private fun <T> transaction(block: (SQLiteDatabase) -> T): T {
        val db = writableDatabase
        db.beginTransaction()
        try { val result = block(db); db.setTransactionSuccessful(); return result }
        finally { db.endTransaction() }
    }

    private fun upsert(db: SQLiteDatabase, scope: String, wire: JSONObject, state: DeliveryState?) {
        require(wire.optString("type") !in listOf("hermes_delta", "ai_delta"))
        val session = wire.getString("session_id")
        val msgId = wire.optString("msg_id")
        val clientId = wire.optString("client_msg_id")
        val seq = wire.optLong("msg_seq")
        require(msgId.isNotBlank() || clientId.isNotBlank() || seq > 0)
        val aliases = buildList<Pair<String, Array<String>>> {
            if (msgId.isNotBlank()) add("scope=? AND session=? AND msg_id=?" to arrayOf(scope, session, msgId))
            if (clientId.isNotBlank()) add("scope=? AND session=? AND client_id=?" to arrayOf(scope, session, clientId))
            if (seq > 0) add("scope=? AND session=? AND seq=?" to arrayOf(scope, session, seq.toString()))
        }
        val effectiveState = if (state == DeliveryState.ACCEPTED && aliases.any { (where, args) ->
            db.rawQuery("SELECT 1 FROM messages WHERE ($where) AND state='DELIVERED' LIMIT 1", args).use { it.moveToFirst() }
        }) DeliveryState.DELIVERED else state
        // Resolve optimistic, live and history aliases before inserting the canonical row.
        // A combined OR makes SQLite choose a (scope,session) scan on cold statistics;
        // separate equalities guarantee point lookups even for a very large conversation.
        aliases.forEach { (where, args) -> db.delete("messages", where, args) }
        val key = if (msgId.isNotBlank()) "m:$msgId" else if (clientId.isNotBlank()) "c:$clientId" else "s:$seq"
        db.insertOrThrow("messages", null, ContentValues().apply {
            put("scope", scope); put("session", session); put("identity", key)
            put("msg_id", msgId); put("client_id", clientId); put("seq", seq)
            put("wire", wire.toString()); put("state", effectiveState?.name)
        })
        if (seq > 0) db.insertWithOnConflict("receive_receipts", null, ContentValues().apply {
            put("scope", scope); put("session", session); put("seq", seq)
        }, SQLiteDatabase.CONFLICT_IGNORE)
    }

    private fun advance(db: SQLiteDatabase, scope: String, session: String): Long {
        var cursor = cursor(scope, session)
        db.rawQuery("SELECT DISTINCT seq FROM messages WHERE scope=? AND session=? AND seq>? ORDER BY seq", arrayOf(scope, session, cursor.toString())).use { rows ->
            while (rows.moveToNext()) {
                val seq = rows.getLong(0)
                if (seq != cursor + 1) break
                cursor = seq
            }
        }
        db.insertWithOnConflict("device_cursors", null, ContentValues().apply {
            put("scope", scope); put("session", session); put("received_seq", cursor)
        }, SQLiteDatabase.CONFLICT_REPLACE)
        return cursor
    }

    @Synchronized fun cursor(scope: String, session: String): Long = readableDatabase.rawQuery(
        "SELECT received_seq FROM device_cursors WHERE scope=? AND session=?", arrayOf(scope, session)
    ).use { if (it.moveToFirst()) it.getLong(0) else 0L }

    @Synchronized fun cursors(scope: String): Map<String, Long> = readableDatabase.rawQuery(
        "SELECT session,received_seq FROM device_cursors WHERE scope=?", arrayOf(scope)
    ).use { rows -> buildMap { while (rows.moveToNext()) put(rows.getString(0), rows.getLong(1)) } }

    @Synchronized fun sessions(scope: String): List<String> = readableDatabase.rawQuery(
        "SELECT DISTINCT session FROM messages WHERE scope=?", arrayOf(scope)
    ).use { rows -> buildList { while (rows.moveToNext()) add(rows.getString(0)) } }

    @Synchronized fun receipts(scope: String): Map<String, List<Long>> {
        val sessions = readableDatabase.rawQuery(
            "SELECT DISTINCT session FROM receive_receipts WHERE scope=? ORDER BY session", arrayOf(scope)
        ).use { rows -> buildList { while (rows.moveToNext()) add(rows.getString(0)) } }
        // Limit every frame independently so one large conversation cannot starve another.
        return sessions.associateWith { session -> readableDatabase.rawQuery(
            "SELECT seq FROM receive_receipts WHERE scope=? AND session=? ORDER BY seq LIMIT 256", arrayOf(scope, session)
        ).use { rows -> buildList { while (rows.moveToNext()) add(rows.getLong(0)) } } }
    }

    @Synchronized fun confirmReceipts(scope: String, session: String, prefix: Long, sequences: List<Long>) = transaction { db ->
        db.delete("receive_receipts", "scope=? AND session=? AND seq<=?", arrayOf(scope, session, prefix.toString()))
        sequences.forEach { db.delete("receive_receipts", "scope=? AND session=? AND seq=?", arrayOf(scope, session, it.toString())) }
    }

    @Synchronized fun save(scope: String, wires: List<JSONObject>, ownUserId: Long? = null): Map<String, Long> = transaction { db ->
        wires.forEach { upsert(db, scope, it, if (ownUserId != null && it.optLong("sender_id", it.optLong("from_user_id")) == ownUserId) DeliveryState.ACCEPTED else DeliveryState.DELIVERED) }
        wires.map { it.getString("session_id") }.distinct().associateWith { advance(db, scope, it) }
    }

    @Synchronized fun enqueue(scope: String, frame: JSONObject, optimistic: JSONObject) = transaction { db ->
        val bytes = frame.toString().toByteArray(Charsets.UTF_8).size
        db.rawQuery("SELECT COUNT(*),COALESCE(SUM(LENGTH(CAST(frame AS BLOB))),0) FROM outbox WHERE scope=? AND status='PENDING'", arrayOf(scope)).use {
            it.moveToFirst(); check(it.getInt(0) < 200 && it.getLong(1) + bytes <= 32L * 1024 * 1024) { "本地发送队列已满，请等待连接恢复" }
        }
        upsert(db, scope, optimistic, DeliveryState.SENDING)
        db.insertOrThrow("outbox", null, ContentValues().apply {
            put("scope", scope); put("client_id", frame.getString("client_msg_id"))
            put("session", optimistic.getString("session_id")); put("frame", frame.toString())
        })
    }

    @Synchronized fun due(scope: String, now: Long, limit: Int = 8): List<OutboxEntry> = readableDatabase.rawQuery(
        "SELECT client_id,session,frame,attempts FROM outbox WHERE scope=? AND status='PENDING' AND next_at<=? ORDER BY rowid LIMIT ?", arrayOf(scope, now.toString(), limit.toString())
    ).use { rows -> buildList { while (rows.moveToNext()) add(OutboxEntry(rows.getString(0), rows.getString(1), JSONObject(rows.getString(2)), rows.getInt(3))) } }

    @Synchronized fun recordAttempt(scope: String, entry: OutboxEntry, now: Long) {
        writableDatabase.update("outbox", ContentValues().apply {
            put("attempts", (entry.attempts + 1).coerceAtMost(1000)); put("next_at", now + outboxRetryDelay(entry.attempts))
        }, "scope=? AND client_id=? AND status='PENDING'", arrayOf(scope, entry.clientId))
    }

    @Synchronized fun replaceFrame(scope: String, clientId: String, frame: JSONObject) = transaction { db ->
        db.update("outbox", ContentValues().apply { put("frame", frame.toString()) }, "scope=? AND client_id=?", arrayOf(scope, clientId))
        db.rawQuery("SELECT wire,state FROM messages WHERE scope=? AND client_id=? LIMIT 1", arrayOf(scope, clientId)).use { rows ->
            if (rows.moveToFirst()) {
                val wire = JSONObject(rows.getString(0)).put("content", frame.getJSONObject("content"))
                upsert(db, scope, wire, rows.getString(1)?.let { DeliveryState.valueOf(it) })
            }
        }
    }

    @Synchronized fun accepted(scope: String, ack: JSONObject): Pair<String, Long>? = transaction { db ->
        val clientId = ack.optString("client_msg_id")
        val stored = db.rawQuery("SELECT session,wire FROM messages WHERE scope=? AND client_id=? LIMIT 1", arrayOf(scope, clientId)).use {
            if (it.moveToFirst()) it.getString(0) to JSONObject(it.getString(1)) else null
        } ?: return@transaction null
        // Save the accepted identity before removing the replayable send, in one transaction.
        val wire = stored.second.put("msg_id", ack.getString("msg_id")).put("msg_seq", ack.getLong("msg_seq"))
        upsert(db, scope, wire, DeliveryState.ACCEPTED)
        db.delete("outbox", "scope=? AND client_id=?", arrayOf(scope, clientId))
        stored.first to advance(db, scope, stored.first)
    }

    @Synchronized fun setState(scope: String, clientId: String, state: DeliveryState) = transaction { db ->
        db.update("messages", ContentValues().apply { put("state", state.name) }, "scope=? AND client_id=?", arrayOf(scope, clientId))
        if (state == DeliveryState.FAILED) db.update("outbox", ContentValues().apply { put("status", "FAILED") }, "scope=? AND client_id=?", arrayOf(scope, clientId))
    }

    @Synchronized fun timeline(scope: String, session: String, limit: Int = 200): List<StoredWire> = readableDatabase.rawQuery(
        "SELECT wire,state FROM messages WHERE scope=? AND session=? ORDER BY CASE WHEN seq=0 THEN 1 ELSE 0 END DESC,seq DESC,rowid DESC LIMIT ?", arrayOf(scope, session, limit.toString())
    ).use { rows -> buildList {
        while (rows.moveToNext()) add(StoredWire(JSONObject(rows.getString(0)), rows.getString(1)?.let { DeliveryState.valueOf(it) }))
    }.reversed() }
}

fun outboxRetryDelay(attempts: Int): Long = minOf(60_000L, 2_000L shl attempts.coerceIn(0, 5))
