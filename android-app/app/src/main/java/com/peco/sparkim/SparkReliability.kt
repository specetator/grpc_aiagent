package com.peco.sparkim

import java.util.LinkedHashSet
import java.util.TreeMap
import kotlin.math.roundToInt

const val MAX_EVENT_QUEUE = 512
const val MAX_STREAM_ACCUMULATORS = 32
const val MAX_PENDING_FRAMES = 256
const val MAX_PENDING_BYTES = 512 * 1024
const val MAX_SEEN_EVENT_IDS = 4096
const val MAX_TIMING_SAMPLES = 64

enum class SessionRestoreKind { VALID, INVALID_TOKEN, UNREACHABLE }

data class SessionRestoreResult(val kind: SessionRestoreKind, val message: String)

object SessionRestoreClassifier {
    fun classify(
        networkFailure: Boolean,
        httpStatus: Int,
        bodyCode: Int,
        bodyMessage: String = ""
    ): SessionRestoreResult {
        if (networkFailure || httpStatus < 0) {
            return SessionRestoreResult(
                SessionRestoreKind.UNREACHABLE,
                "服务器暂时不可达，已保留登录状态，可稍后重试"
            )
        }
        if (httpStatus == 401 || bodyCode == 401) {
            return SessionRestoreResult(SessionRestoreKind.INVALID_TOKEN, "登录已失效，请重新登录")
        }
        if (httpStatus == 403 || bodyCode == 403) {
            return SessionRestoreResult(SessionRestoreKind.INVALID_TOKEN, "登录已失效，请重新登录")
        }
        if (httpStatus >= 500 || bodyCode >= 500) {
            return SessionRestoreResult(
                SessionRestoreKind.UNREACHABLE,
                "服务暂时异常，已保留登录状态，可稍后重试"
            )
        }
        if (bodyCode == 0) return SessionRestoreResult(SessionRestoreKind.VALID, "")
        return SessionRestoreResult(
            SessionRestoreKind.UNREACHABLE,
            bodyMessage.ifBlank { "服务暂时异常，已保留登录状态，可稍后重试" }
        )
    }
}

data class AgentDeltaEnvelope(
    val schema: String? = null,
    val type: String? = null,
    val dataText: String? = null,
    val envelopeSchema: String? = null,
    val requestId: String? = null,
    val conversationId: String? = null,
    val sequence: Int? = null,
    val terminal: Boolean? = null,
    val eventId: String? = null
)

sealed class DeltaDecision {
    data class Ordered(val sequence: Int) : DeltaDecision()
    object Unordered : DeltaDecision()
    object Drop : DeltaDecision()
}

object AgentDeltaValidator {
    fun decide(
        requestId: String,
        sessionId: String,
        delta: String,
        progress: Boolean,
        deltaIndex: Int,
        event: AgentDeltaEnvelope?
    ): DeltaDecision {
        if (requestId.isBlank() || delta.isEmpty() || sessionId.isBlank()) return DeltaDecision.Drop
        if (event == null) {
            return if (deltaIndex >= 0) DeltaDecision.Ordered(deltaIndex) else DeltaDecision.Unordered
        }
        val expectedType = if (progress) "assistant_progress" else "assistant_delta"
        val sequence = event.sequence
        if (event.schema != "sparkpush.agent_event.v1" ||
            event.type != expectedType ||
            event.dataText != delta ||
            event.envelopeSchema != "sparkpush.agent_envelope.v1" ||
            event.requestId != requestId ||
            event.conversationId != sessionId ||
            sequence == null || sequence < 0 ||
            event.terminal != false
        ) {
            return DeltaDecision.Drop
        }
        return DeltaDecision.Ordered(sequence)
    }
}

data class StreamDelta(val text: String, val progress: Boolean)

enum class ReorderStatus { APPLIED, BUFFERED, DUPLICATE, DROP, OVERFLOW }

data class ReorderResult(
    val status: ReorderStatus,
    val applied: List<StreamDelta> = emptyList()
)

class BoundedReorderBuffer(
    private val maxFrames: Int = MAX_PENDING_FRAMES,
    private val maxBytes: Int = MAX_PENDING_BYTES
) {
    val pending = TreeMap<Int, StreamDelta>()
    var nextIndex: Int? = null
        private set
    var pendingBytes: Int = 0
        private set
    var overflowed: Boolean = false
        private set

    fun accept(decision: DeltaDecision, delta: StreamDelta): ReorderResult {
        if (overflowed) return ReorderResult(ReorderStatus.OVERFLOW)
        return when (decision) {
            DeltaDecision.Drop -> ReorderResult(ReorderStatus.DROP)
            DeltaDecision.Unordered -> ReorderResult(ReorderStatus.APPLIED, listOf(delta))
            is DeltaDecision.Ordered -> acceptOrdered(decision.sequence, delta)
        }
    }

    fun markOverflow() {
        overflowed = true
        pending.clear()
        pendingBytes = 0
    }

    private fun acceptOrdered(index: Int, delta: StreamDelta): ReorderResult {
        if (nextIndex == null) nextIndex = 0
        val expected = nextIndex ?: 0
        if (index < expected || pending.containsKey(index)) {
            return ReorderResult(ReorderStatus.DUPLICATE)
        }
        val incomingBytes = utf8Len(delta.text)
        if (pending.size >= maxFrames || pendingBytes + incomingBytes > maxBytes) {
            markOverflow()
            return ReorderResult(ReorderStatus.OVERFLOW)
        }
        pending[index] = delta
        pendingBytes += incomingBytes
        val applied = ArrayList<StreamDelta>()
        var cursor = expected
        while (true) {
            val next = pending.remove(cursor) ?: break
            pendingBytes = (pendingBytes - utf8Len(next.text)).coerceAtLeast(0)
            applied += next
            cursor++
        }
        nextIndex = cursor
        return if (applied.isEmpty()) ReorderResult(ReorderStatus.BUFFERED)
        else ReorderResult(ReorderStatus.APPLIED, applied)
    }
}

class BoundedEventIdSet(private val limit: Int = MAX_SEEN_EVENT_IDS) {
    private val ids = LinkedHashSet<String>()

    fun addIfNew(eventId: String): Boolean {
        if (eventId.isBlank()) return true
        if (!ids.add(eventId)) return false
        while (ids.size > limit) {
            val iterator = ids.iterator()
            if (!iterator.hasNext()) break
            iterator.next()
            iterator.remove()
        }
        return true
    }
}

object TimingPercentiles {
    fun percentile(sortedMillis: List<Long>, p: Double): Long {
        if (sortedMillis.isEmpty()) return -1L
        val rank = ((sortedMillis.size - 1) * p).roundToInt().coerceIn(0, sortedMillis.lastIndex)
        return sortedMillis[rank]
    }

    fun summarize(samples: List<Long>): String {
        if (samples.isEmpty()) return "n=0"
        val sorted = samples.sorted()
        return "n=${sorted.size} p50=${percentile(sorted, 0.50)} p95=${percentile(sorted, 0.95)} max=${sorted.last()}"
    }
}

fun looksLikeStreamDelta(raw: String): Boolean = raw.contains("\"hermes_delta\"")

private fun utf8Len(value: String): Int = value.toByteArray(Charsets.UTF_8).size
