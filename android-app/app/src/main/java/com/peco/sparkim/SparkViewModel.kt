package com.peco.sparkim

import android.app.Application
import android.content.Context
import android.os.SystemClock
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.util.TreeMap
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

private val DisplayPrefixRegex = Regex("^\\s*【[^】]{1,100}】")

private const val STREAM_FLUSH_INTERVAL_MS = 40L
private const val STREAM_TIMEOUT_NANOS = 150_000_000_000L
private const val CLIENT_TIMING_TAG = "SparkTiming"

private fun utf8Bytes(value: String): Int = value.toByteArray(Charsets.UTF_8).size
private fun unicodeChars(value: String): Int =
    value.codePointCount(0, value.length)

private data class PendingStreamDelta(val text: String, val progress: Boolean)

private class StreamAccumulator(
    val requestId: String,
    val sessionId: String,
    val senderId: Long
) {
    val messageKey: String = "stream:$requestId"
    val pending = TreeMap<Int, PendingStreamDelta>()
    val text = StringBuilder()
    var nextDeltaIndex: Int? = null
    var progress: String? = null
    var dirty = false
    var lastActivityAtNanos = 0L
    var lastFlushAtNanos = 0L
    var publishedText = ""
    var publishedProgress: String? = null
    var messageIndex = -1
}

private data class ClientRequestTiming(
    var clientMsgId: String,
    var sessionId: String,
    var agent: Boolean,
    var clientTraceId: String,
    var sendAtNanos: Long,
    var requestId: String? = null,
    var acceptedAtNanos: Long? = null,
    var firstDeltaAtNanos: Long? = null,
    var completedAtNanos: Long? = null,
    var chunkCount: Int = 0,
    var deltaBytes: Int = 0,
    var deltaChars: Int = 0,
    var finalBytes: Int = 0,
    var finalChars: Int = 0,
    var displayBytes: Int = 0,
    var displayChars: Int = 0,
    var stream: Boolean = false,
    var provider: String = "unknown",
    var model: String = "unknown"
)

class SparkViewModel(app: Application) : AndroidViewModel(app), SparkClient.Listener {
    private val prefs = app.getSharedPreferences("spark_native", Context.MODE_PRIVATE)
    private val client = SparkClient()
    private val _ui = MutableStateFlow(SparkUiState())
    val ui = _ui.asStateFlow()
    private var reconnectJob: Job? = null
    // One ordered worker parses WebSocket frames and mutates stream state.
    // This avoids one coroutine and one JSON parse racing for every delta.
    private val eventDispatcher = Dispatchers.Default.limitedParallelism(1)
    private val eventQueue = Channel<String>(Channel.UNLIMITED)
    private var eventJob: Job? = null
    private var streamFlushJob: Job? = null
    private val terminalStreamRequests = LinkedHashSet<String>()
    private val streamAccumulators = LinkedHashMap<String, StreamAccumulator>()
    private val timingsByClientId = ConcurrentHashMap<String, ClientRequestTiming>()
    private val timingsByRequestId = ConcurrentHashMap<String, ClientRequestTiming>()
    private val cursors = mutableMapOf<String, Long>()
    private val seenSequences = mutableMapOf<String, MutableSet<Long>>()

    init {
        eventJob = viewModelScope.launch(eventDispatcher) {
            for (raw in eventQueue) {
                try {
                    handleEvent(JSONObject(raw))
                } catch (_: Exception) {
                    _ui.update { it.copy(notice = "收到无法解析的实时消息") }
                }
            }
        }
        ensureStreamFlushLoop()
        val server = prefs.getString("server", null)
        val uid = prefs.getLong("user_id", 0)
        val token = prefs.getString("token", null)
        val name = prefs.getString("name", "") ?: ""
        if (!server.isNullOrBlank()) {
            _ui.update { it.copy(serverInput = server, screen = if (uid > 0 && !token.isNullOrBlank()) Screen.HOME else Screen.AUTH) }
            if (uid > 0 && !token.isNullOrBlank() && client.configure(server)) {
                val auth = AuthSession(uid, token, name.ifBlank { "用户 $uid" })
                _ui.update { it.copy(auth = auth, screen = Screen.HOME, notice = "正在恢复登录状态…") }
                viewModelScope.launch {
                    if (verifySession(auth)) onAuthenticated(auth) else clearStoredAuth()
                }
            }
        }
    }

    fun setServer(value: String) = _ui.update { it.copy(serverInput = value, error = null) }
    fun setAccount(value: String) = _ui.update { it.copy(account = value, error = null) }
    fun setPassword(value: String) = _ui.update { it.copy(password = value, error = null) }
    fun setNickname(value: String) = _ui.update { it.copy(nickname = value, error = null) }
    fun setDraft(value: String) = _ui.update { it.copy(draft = value) }
    fun setNewPeer(value: String) = _ui.update { it.copy(newPeerInput = value) }
    fun toggleRegister() = _ui.update { it.copy(registerMode = !it.registerMode, error = null) }

    fun continueToAuth() {
        val value = _ui.value.serverInput.trim()
        if (!client.configure(value)) {
            _ui.update { it.copy(error = "请输入有效的服务器地址，例如 http://100.89.19.125") }
            return
        }
        val normalized = client.currentEndpoint()?.input ?: value
        prefs.edit().putString("server", normalized).apply()
        _ui.update { it.copy(serverInput = normalized, screen = Screen.AUTH, error = null) }
    }

    fun authenticate() {
        val snapshot = _ui.value
        if (snapshot.account.trim().isEmpty() || snapshot.password.isEmpty()) {
            _ui.update { it.copy(error = "请输入账号和密码") }
            return
        }
        if (snapshot.password.length < 6) {
            _ui.update { it.copy(error = "密码至少 6 位") }
            return
        }
        if (client.currentEndpoint() == null && !client.configure(snapshot.serverInput)) {
            _ui.update { it.copy(error = "服务器地址无效") }
            return
        }
        _ui.update { it.copy(loading = true, error = null, notice = "正在登录…") }
        viewModelScope.launch {
            try {
                val path = if (snapshot.registerMode) "/api/register" else "/api/login"
                val body = JSONObject().put("account", snapshot.account.trim()).put("password", snapshot.password)
                if (snapshot.registerMode && snapshot.nickname.trim().isNotEmpty()) body.put("name", snapshot.nickname.trim())
                val response = client.post(path, body)
                val auth = parseAuth(response, snapshot.nickname)
                persistAuth(auth)
                onAuthenticated(auth)
            } catch (e: Exception) {
                _ui.update { it.copy(loading = false, error = e.message ?: "登录失败", notice = null) }
            }
        }
    }

    private suspend fun verifySession(auth: AuthSession): Boolean = try {
        val response = client.post("/api/session/list_single", JSONObject().put("user_id", auth.userId), auth.token)
        response.optInt("code", -1) == 0
    } catch (_: Exception) { false }

    private fun parseAuth(response: JSONObject, fallbackName: String): AuthSession {
        val code = response.optInt("code", -1)
        if (code != 0) throw IllegalStateException(response.optString("message", "服务端拒绝请求"))
        val data = response.optJSONObject("data") ?: throw IllegalStateException("登录响应缺少 data")
        val uid = data.optLong("user_id", 0)
        val token = data.optString("token", "")
        if (uid <= 0 || token.isBlank()) throw IllegalStateException("登录响应缺少凭证")
        return AuthSession(uid, token, data.optString("name", fallbackName).ifBlank { "用户 $uid" })
    }

    private fun persistAuth(auth: AuthSession) {
        prefs.edit().putLong("user_id", auth.userId).putString("token", auth.token).putString("name", auth.name).apply()
    }

    private fun nowNanos(): Long = SystemClock.elapsedRealtimeNanos()

    private fun registerTiming(clientMsgId: String, sessionId: String, agent: Boolean, sendAtNanos: Long = nowNanos()): ClientRequestTiming {
        val timing = ClientRequestTiming(
            clientMsgId = clientMsgId,
            sessionId = sessionId,
            agent = agent,
            clientTraceId = UUID.randomUUID().toString().replace("-", "").take(16),
            sendAtNanos = sendAtNanos
        )
        timingsByClientId[clientMsgId] = timing
        ensureStreamFlushLoop()
        return timing
    }

    private fun findTimingByRequest(requestId: String): ClientRequestTiming? {
        if (requestId.isBlank()) return null
        timingsByRequestId[requestId]?.let { return it }
        return timingsByRequestId.entries.firstOrNull { (key, _) ->
            key.substringBefore("@") == requestId
        }?.value
    }

    private fun timingForRequest(requestId: String, sessionId: String): ClientRequestTiming {
        timingsByRequestId[requestId]?.let {
            it.stream = true
            return it
        }
        // Logic derives the Hermes request id from the accepted Spark message
        // id (message_id@comet_id). Reuse that timing object so send_at and
        // accepted_at remain valid even when several Agent requests overlap.
        val baseRequestId = requestId.substringBefore("@")
        val inherited = findTimingByRequest(baseRequestId)
        if (inherited != null && inherited.sessionId == sessionId && inherited.agent) {
            timingsByRequestId.entries
                .filter { it.value === inherited && it.key != requestId }
                .forEach { timingsByRequestId.remove(it.key, inherited) }
            inherited.requestId = requestId
            inherited.stream = true
            timingsByRequestId[requestId] = inherited
            return inherited
        }
        val created = ClientRequestTiming(
            clientMsgId = "",
            sessionId = sessionId,
            agent = true,
            clientTraceId = "",
            sendAtNanos = 0L,
            requestId = requestId,
            stream = true
        )
        return timingsByRequestId.putIfAbsent(requestId, created) ?: created
    }

    private fun recordAccepted(event: JSONObject) {
        val clientId = event.optString("client_msg_id").trim()
        if (clientId.isBlank()) return
        val requestId = event.optString("msg_id").trim()
        val pending = timingsByClientId[clientId]
        val existing = requestId.takeIf { it.isNotBlank() }?.let { findTimingByRequest(it) }
        if (pending == null && existing == null) return
        val timing = existing ?: pending ?: ClientRequestTiming(
            clientMsgId = clientId,
            sessionId = event.optString("session_id"),
            agent = false,
            clientTraceId = "",
            sendAtNanos = 0L
        )
        if (existing != null && pending != null && existing !== pending) {
            if (existing.sendAtNanos == 0L) existing.sendAtNanos = pending.sendAtNanos
            if (existing.clientTraceId.isBlank()) existing.clientTraceId = pending.clientTraceId
            existing.agent = existing.agent || pending.agent
            timingsByClientId.remove(pending.clientMsgId, pending)
        }
        timing.clientMsgId = clientId
        timing.acceptedAtNanos = nowNanos()
        if (requestId.isNotBlank()) {
            // If a delta already arrived, keep its full request id; otherwise
            // retain the accepted message id as the alias used by the next
            // delta. Both aliases are removed on completion.
            if (timing.requestId.isNullOrBlank() || timing.requestId == requestId) {
                timing.requestId = requestId
            }
            timingsByRequestId[requestId] = timing
        }
        timingsByClientId[clientId] = timing
    }

    private fun completeTiming(timing: ClientRequestTiming, reason: String, completedAt: Long = nowNanos()) {
        if (timing.completedAtNanos != null) return
        timing.completedAtNanos = completedAt
        fun elapsed(end: Long?, start: Long): Long =
            if (end == null || start <= 0L) -1L else (end - start) / 1_000_000L
        Log.i(
            CLIENT_TIMING_TAG,
            "client_timing request_id=${timing.requestId.orEmpty()} " +
                "client_msg_id=${timing.clientMsgId} client_trace_id=${timing.clientTraceId} " +
                "send_at=${timing.sendAtNanos} " +
                "accepted_at=${timing.acceptedAtNanos ?: 0L} " +
                "first_delta_at=${timing.firstDeltaAtNanos ?: 0L} " +
                "completed_at=${timing.completedAtNanos ?: 0L} " +
                "accepted_ms=${elapsed(timing.acceptedAtNanos, timing.sendAtNanos)} " +
                "first_delta_ms=${elapsed(timing.firstDeltaAtNanos, timing.sendAtNanos)} " +
                "total_ms=${elapsed(timing.completedAtNanos, timing.sendAtNanos)} " +
                "provider=${timing.provider} model=${timing.model} " +
                "stream=${timing.stream} max_tokens=unset max_output_tokens=unset " +
                "chunk_count=${timing.chunkCount} delta_bytes=${timing.deltaBytes} " +
                "delta_chars=${timing.deltaChars} final_bytes=${timing.finalBytes} " +
                "final_chars=${timing.finalChars} display_bytes=${timing.displayBytes} " +
                "display_chars=${timing.displayChars} reason=${reason}"
        )
        if (timing.clientMsgId.isNotBlank()) timingsByClientId.remove(timing.clientMsgId, timing)
        timing.requestId?.let { timingsByRequestId.remove(it, timing) }
        timingsByRequestId.entries
            .filter { it.value === timing }
            .forEach { timingsByRequestId.remove(it.key, timing) }
    }

    private fun completeTimingForClient(clientId: String, reason: String) {
        timingsByClientId[clientId]?.let { completeTiming(it, reason) }
    }

    private fun completeTimingForRequest(requestId: String, reason: String) {
        timingsByRequestId[requestId]?.let { completeTiming(it, reason) }
    }

    private fun ensureStreamFlushLoop() {
        synchronized(this) {
            if (streamFlushJob?.isActive == true) return
            streamFlushJob = viewModelScope.launch(eventDispatcher) {
                while (isActive) {
                    delay(STREAM_FLUSH_INTERVAL_MS)
                    val now = nowNanos()
                    for (accumulator in streamAccumulators.values.toList()) {
                        if (!streamAccumulators.containsKey(accumulator.requestId)) continue
                        if (now - accumulator.lastActivityAtNanos >= STREAM_TIMEOUT_NANOS) {
                            discardStream(accumulator, "响应超时，请重试", "timeout", showFailure = true)
                        } else if (accumulator.dirty &&
                            (accumulator.lastFlushAtNanos == 0L || now - accumulator.lastFlushAtNanos >= STREAM_FLUSH_INTERVAL_MS * 1_000_000L)) {
                            flushAccumulator(accumulator, now, force = false)
                        }
                    }
                    for (timing in timingsByClientId.values.toList()) {
                        if (timing.sendAtNanos > 0L && now - timing.sendAtNanos >= STREAM_TIMEOUT_NANOS) {
                            completeTiming(timing, "timeout", now)
                        }
                    }
                }
            }
        }
    }

    private fun markTerminalStream(requestId: String) {
        if (requestId.isBlank()) return
        terminalStreamRequests.add(requestId)
        while (terminalStreamRequests.size > 4096) {
            val iterator = terminalStreamRequests.iterator()
            if (!iterator.hasNext()) break
            iterator.next()
            iterator.remove()
        }
    }

    private fun applyDelta(accumulator: StreamAccumulator, delta: PendingStreamDelta) {
        if (delta.progress) {
            if (accumulator.progress != delta.text) {
                accumulator.progress = delta.text
                accumulator.dirty = true
            }
        } else {
            if (delta.text.isNotEmpty()) {
                accumulator.text.append(delta.text)
                accumulator.dirty = true
            }
            if (accumulator.progress != "正在生成回答") {
                accumulator.progress = "正在生成回答"
                accumulator.dirty = true
            }
        }
    }

    private fun acceptDelta(accumulator: StreamAccumulator, index: Int, delta: PendingStreamDelta): Boolean {
        if (index < 0) {
            applyDelta(accumulator, delta)
            return true
        }
        // Spark Push numbers the first progress/text delta from zero. Keep a
        // gap in the small pending map instead of silently discarding a late
        // lower index; the durable final message remains authoritative.
        if (accumulator.nextDeltaIndex == null) accumulator.nextDeltaIndex = 0
        val expected = accumulator.nextDeltaIndex ?: 0
        if (index < expected || accumulator.pending.containsKey(index)) return false
        accumulator.pending[index] = delta
        var cursor = expected
        while (true) {
            val next = accumulator.pending.remove(cursor) ?: break
            applyDelta(accumulator, next)
            cursor++
        }
        accumulator.nextDeltaIndex = cursor
        return true
    }

    private fun streamIndex(state: SparkUiState, accumulator: StreamAccumulator): Int {
        val hint = accumulator.messageIndex
        if (hint in state.messages.indices && state.messages[hint].clientMsgId == accumulator.messageKey) return hint
        val found = state.messages.indexOfFirst { it.clientMsgId == accumulator.messageKey }
        accumulator.messageIndex = found
        return found
    }

    private fun flushAccumulator(accumulator: StreamAccumulator, now: Long, force: Boolean) {
        if (!accumulator.dirty) return
        if (!force && accumulator.lastFlushAtNanos > 0L && now - accumulator.lastFlushAtNanos < STREAM_FLUSH_INTERVAL_MS * 1_000_000L) return
        val renderedText = if (accumulator.text.length != accumulator.publishedText.length) accumulator.text.toString() else accumulator.publishedText
        val renderedProgress = accumulator.progress
        _ui.update { state ->
            if (state.selected?.sessionId != accumulator.sessionId) return@update state
            val index = streamIndex(state, accumulator)
            if (index >= 0) {
                val old = state.messages[index]
                val updated = old.copy(text = renderedText, progress = renderedProgress, streaming = true)
                if (updated == old) state else state.copy(messages = state.messages.toMutableList().also { it[index] = updated })
            } else {
                state.copy(messages = state.messages + ChatMessage(
                    clientMsgId = accumulator.messageKey,
                    sessionId = accumulator.sessionId,
                    senderId = accumulator.senderId,
                    text = renderedText,
                    streaming = true,
                    progress = renderedProgress
                ))
            }
        }
        accumulator.publishedText = renderedText
        accumulator.publishedProgress = renderedProgress
        accumulator.lastFlushAtNanos = now
        accumulator.dirty = false
    }

    private fun markStreamFailed(accumulator: StreamAccumulator, message: String) {
        _ui.update { state ->
            if (state.selected?.sessionId != accumulator.sessionId) return@update state
            val index = streamIndex(state, accumulator)
            if (index < 0) state
            else {
                val old = state.messages[index]
                val updated = old.copy(streaming = false, state = DeliveryState.FAILED, progress = message)
                if (updated == old) state else state.copy(messages = state.messages.toMutableList().also { it[index] = updated })
            }
        }
    }

    private fun discardStream(accumulator: StreamAccumulator, message: String, reason: String, showFailure: Boolean) {
        if (!streamAccumulators.containsKey(accumulator.requestId)) return
        val now = nowNanos()
        if (showFailure) {
            flushAccumulator(accumulator, now, force = true)
            markStreamFailed(accumulator, message)
        }
        markTerminalStream(accumulator.requestId)
        streamAccumulators.remove(accumulator.requestId)
        completeTimingForRequest(accumulator.requestId, reason)
    }

    private fun clearStreams(sessionId: String?, reason: String, showFailure: Boolean) {
        for (accumulator in streamAccumulators.values.toList()) {
            if (sessionId == null || accumulator.sessionId == sessionId) {
                discardStream(accumulator, "连接中断，等待重连补齐", reason, showFailure)
            }
        }
        for (timing in timingsByClientId.values.toList()) {
            if (sessionId == null || timing.sessionId == sessionId) completeTiming(timing, reason)
        }
        for (timing in timingsByRequestId.values.toList()) {
            if (sessionId == null || timing.sessionId == sessionId) completeTiming(timing, reason)
        }
    }

    private fun clearStoredAuth() {
        viewModelScope.launch(eventDispatcher) { clearStreams(null, "logout", showFailure = false) }
        client.close()
        prefs.edit().remove("user_id").remove("token").remove("name").apply()
        _ui.update { it.copy(auth = null, screen = Screen.AUTH, connection = ConnectionState.DISCONNECTED, conversations = emptyList(), selected = null, messages = emptyList(), hasHistory = false, loading = false, notice = null) }
    }

    private fun onAuthenticated(auth: AuthSession) {
        _ui.update { it.copy(auth = auth, screen = Screen.HOME, loading = false, error = null, notice = "正在连接 Spark Push…") }
        connectSocket(auth)
        loadSessions(auth)
    }

    private fun connectSocket(auth: AuthSession) {
        reconnectJob?.cancel()
        client.connect(auth.token, this)
    }

    private fun scheduleReconnect() {
        if (reconnectJob?.isActive == true) return
        reconnectJob = viewModelScope.launch {
            delay(1800)
            val auth = _ui.value.auth ?: return@launch
            client.connect(auth.token, this@SparkViewModel)
        }
    }

    private fun loadSessions(auth: AuthSession? = null) {
        val session = auth ?: _ui.value.auth ?: return
        viewModelScope.launch {
            try {
                val response = client.post("/api/session/list_single", JSONObject().put("user_id", session.userId), session.token)
                if (response.optInt("code", -1) != 0) throw IllegalStateException(response.optString("message", "无法读取会话列表"))
                val data = response.optJSONObject("data")
                val serverList = mutableListOf<Conversation>()
                val array = data?.optJSONArray("sessions") ?: JSONArray()
                for (i in 0 until array.length()) {
                    val item = array.optJSONObject(i) ?: continue
                    val peer = item.optLong("peer_user_id", 0)
                    // Do not surface the legacy PC/WebDemo Agent contacts in
                    // the Android workspace. Ordinary Spark Push contacts
                    // remain available through the shared transport.
                    if (peer > 0 && !isDesktopAgent(peer)) serverList += knownConversation(session.userId, peer)
                }
                val fixed = listOf(knownConversation(session.userId, ANDROID_HERMES_ID), knownConversation(session.userId, ANDROID_PI_ID))
                val conversations = (fixed + serverList).distinctBy { it.sessionId }
                _ui.update { current ->
                    if (current.auth?.userId != session.userId) current
                    else current.copy(conversations = conversations, notice = if (current.connection == ConnectionState.CONNECTED) null else current.notice)
                }
            } catch (e: Exception) {
                _ui.update { current ->
                    if (current.auth?.userId != session.userId) current
                    else current.copy(notice = "会话列表暂不可用：${e.message ?: "网络错误"}")
                }
            }
        }
    }

    fun openConversation(conversation: Conversation) {
        val previous = _ui.value.selected?.sessionId
        if (previous != null && previous != conversation.sessionId) {
            viewModelScope.launch(eventDispatcher) { clearStreams(previous, "session_switch", showFailure = false) }
        }
        _ui.update { it.copy(screen = Screen.CHAT, selected = conversation, messages = emptyList(), hasHistory = false, loading = true, loadingMore = false, error = null, draft = "") }
        loadHistory(conversation, true)
        refreshUnread()
        if (_ui.value.connection != ConnectionState.CONNECTED) _ui.value.auth?.let { connectSocket(it) }
    }

    fun openKnownAgent(peerId: Long) {
        val auth = _ui.value.auth ?: return
        openConversation(knownConversation(auth.userId, peerId))
    }

    fun goHome() {
        viewModelScope.launch(eventDispatcher) { clearStreams(_ui.value.selected?.sessionId, "session_switch", showFailure = false) }
        _ui.update { it.copy(screen = Screen.HOME, selected = null, messages = emptyList(), hasHistory = false, error = null) }
        loadSessions()
    }

    fun openKnowledge(docId: String = _ui.value.knowledgeDocId, chunkId: String = _ui.value.knowledgeChunkId) {
        _ui.update { it.copy(screen = Screen.KNOWLEDGE, knowledgeDocId = docId, knowledgeChunkId = chunkId, knowledge = null, knowledgeError = null) }
        if (docId.isNotBlank()) loadKnowledge()
    }

    fun setKnowledgeDocId(value: String) = _ui.update { it.copy(knowledgeDocId = value, knowledgeError = null) }
    fun setKnowledgeChunkId(value: String) = _ui.update { it.copy(knowledgeChunkId = value, knowledgeError = null) }

    fun loadKnowledge() {
        val auth = _ui.value.auth ?: return
        val docId = _ui.value.knowledgeDocId.trim()
        val chunkId = _ui.value.knowledgeChunkId.trim()
        if (docId.isBlank()) {
            _ui.update { it.copy(knowledgeError = "请输入 doc_id") }
            return
        }
        _ui.update { it.copy(knowledgeLoading = true, knowledgeError = null) }
        viewModelScope.launch {
            try {
                val body = JSONObject().put("doc_id", docId)
                if (chunkId.isNotBlank()) body.put("chunk_id", chunkId)
                val response = client.post("/api/knowledge/document", body, auth.token)
                if (response.optInt("code", -1) != 0) throw IllegalStateException(response.optString("message", "知识原文读取失败"))
                val data = response.optJSONObject("data") ?: JSONObject()
                val doc = data.optJSONObject("document") ?: JSONObject()
                val chunk = data.optJSONObject("chunk")
                val locator = chunk?.optJSONObject("locator")?.let { loc ->
                    "${loc.optString("type")} ${loc.optString("value")}".trim()
                }.orEmpty()
                val result = KnowledgeDocument(
                    docId = doc.optString("doc_id", docId),
                    title = doc.optString("title", "CANN 知识原文"),
                    sourceId = doc.optString("source_id"),
                    authority = doc.optString("authority"),
                    revision = doc.optString("source_revision"),
                    generation = data.optString("generation"),
                    locator = locator,
                    content = doc.optString("content"),
                    chunkId = chunk?.optString("chunk_id", chunkId).orEmpty()
                )
                _ui.update { it.copy(knowledge = result, knowledgeLoading = false, knowledgeError = null) }
            } catch (e: Exception) {
                _ui.update { it.copy(knowledgeLoading = false, knowledgeError = e.message ?: "知识原文读取失败") }
            }
        }
    }

    fun openAdmin() = _ui.update { it.copy(screen = Screen.ADMIN, adminError = null, adminNotice = null) }
    fun setAdminAccount(value: String) = _ui.update { it.copy(adminAccount = value, adminError = null) }
    fun setAdminPassword(value: String) = _ui.update { it.copy(adminPassword = value, adminError = null) }
    fun setAdminSection(value: AdminSection) = _ui.update { it.copy(adminSection = value, adminError = null) }
    fun setAdminSearch(value: String) = _ui.update { it.copy(adminSearch = value) }
    fun setAdminRoomName(value: String) = _ui.update { it.copy(adminRoomName = value, adminError = null) }
    fun setAdminRoomOwner(value: String) = _ui.update { it.copy(adminRoomOwner = value, adminError = null) }
    fun setAdminBroadcastScope(value: String) = _ui.update { it.copy(adminBroadcastScope = value, adminError = null) }
    fun setAdminBroadcastRoom(value: String) = _ui.update { it.copy(adminBroadcastRoom = value, adminError = null) }
    fun setAdminBroadcastText(value: String) = _ui.update { it.copy(adminBroadcastText = value, adminError = null) }

    fun adminLogin() {
        val snapshot = _ui.value
        if (snapshot.adminAccount.trim().isEmpty() || snapshot.adminPassword.isEmpty()) {
            _ui.update { it.copy(adminError = "请输入管理员账号和密码") }
            return
        }
        _ui.update { it.copy(adminBusy = true, adminError = null, adminNotice = "正在验证管理员会话…") }
        viewModelScope.launch {
            try {
                val response = client.post("/api/login", JSONObject().put("account", snapshot.adminAccount.trim()).put("password", snapshot.adminPassword))
                val auth = parseAuth(response, "管理员")
                _ui.update { it.copy(adminToken = auth.token, adminUserId = auth.userId, adminSection = AdminSection.OVERVIEW, adminBusy = false, adminError = null, adminNotice = "管理员已登录") }
                loadAdminAll()
            } catch (e: Exception) {
                _ui.update { it.copy(adminBusy = false, adminError = e.message ?: "管理员登录失败", adminNotice = null) }
            }
        }
    }

    fun adminLogout() = _ui.update { it.copy(adminToken = null, adminUserId = 0, adminSection = AdminSection.LOGIN, adminUsers = emptyList(), adminRooms = emptyList(), adminAudit = emptyList(), adminNotice = null) }

    private fun adminPost(path: String, body: JSONObject = JSONObject(), onSuccess: (JSONObject) -> Unit) {
        val token = _ui.value.adminToken ?: run {
            _ui.update { it.copy(adminError = "请先登录管理员账号") }
            return
        }
        _ui.update { it.copy(adminBusy = true, adminError = null) }
        viewModelScope.launch {
            try {
                val response = client.post(path, body, token)
                if (response.optInt("code", -1) != 0) throw IllegalStateException(response.optString("message", "管理员请求失败"))
                onSuccess(response.optJSONObject("data") ?: JSONObject())
                _ui.update { it.copy(adminBusy = false) }
            } catch (e: Exception) {
                _ui.update { it.copy(adminBusy = false, adminError = e.message ?: "管理员请求失败") }
            }
        }
    }

    fun loadAdminAll() {
        loadAdminUsers()
        loadAdminRooms()
        loadAdminAudit()
    }

    fun loadAdminUsers() = adminPost("/api/admin/user/list", JSONObject().put("offset", 0).put("limit", 100).put("status", 0)) { data ->
        val array = data.optJSONArray("users") ?: JSONArray()
        val users = buildList {
            for (i in 0 until array.length()) {
                val item = array.optJSONObject(i) ?: continue
                add(AdminUser(item.optLong("user_id"), item.optString("account"), item.optString("name"), item.optString("status"), item.optString("created_at"), item.optString("deleted_at")))
            }
        }
        _ui.update { it.copy(adminUsers = users, adminNotice = "已更新用户列表，共 ${data.optInt("total", users.size)} 个账号") }
    }

    fun loadAdminRooms() = adminPost("/api/admin/chatroom/list", JSONObject().put("offset", 0).put("limit", 100)) { data ->
        val array = data.optJSONArray("rooms") ?: JSONArray()
        val rooms = buildList {
            for (i in 0 until array.length()) {
                val item = array.optJSONObject(i) ?: continue
                add(AdminRoom(item.optLong("room_id"), item.optString("name"), item.optLong("owner_id")))
            }
        }
        _ui.update { it.copy(adminRooms = rooms) }
    }

    fun loadAdminAudit(targetUserId: Long = 0) = adminPost("/api/admin/audit/list", JSONObject().put("offset", 0).put("limit", 100).put("target_user_id", targetUserId)) { data ->
        val array = data.optJSONArray("logs") ?: JSONArray()
        val logs = buildList {
            for (i in 0 until array.length()) {
                val item = array.optJSONObject(i) ?: continue
                add(AuditLog(item.optLong("id"), item.optString("action"), item.optLong("actor_user_id"), item.optLong("target_user_id"), item.optString("reason"), item.optString("metadata"), item.optString("created_at")))
            }
        }
        _ui.update { it.copy(adminAudit = logs) }
    }

    fun adminChangeStatus(userId: Long, action: String) = adminPost("/api/admin/user/$action", JSONObject().put("user_id", userId).put("reason", "Android 管理员控制台")) {
        _ui.update { it.copy(adminNotice = "用户 #$userId 状态已更新") }
        loadAdminUsers(); loadAdminAudit()
    }

    fun adminUpdateName(userId: Long, name: String) {
        if (name.trim().isBlank()) return
        adminPost("/api/admin/user/update", JSONObject().put("user_id", userId).put("name", name.trim())) {
            _ui.update { it.copy(adminNotice = "用户 #$userId 昵称已更新") }
            loadAdminUsers(); loadAdminAudit()
        }
    }

    fun adminRevokeTokens(userId: Long) = adminPost("/api/admin/user/revoke_tokens", JSONObject().put("user_id", userId)) {
        _ui.update { it.copy(adminNotice = "已撤销用户 #$userId 的登录 Token") }
        loadAdminAudit()
    }

    fun adminCreateRoom() {
        val name = _ui.value.adminRoomName.trim()
        val owner = _ui.value.adminRoomOwner.trim().toLongOrNull() ?: _ui.value.adminUserId
        if (name.isBlank() || owner <= 0) {
            _ui.update { it.copy(adminError = "请填写房间名称和有效房主 ID") }
            return
        }
        adminPost("/api/admin/chatroom/create", JSONObject().put("name", name).put("owner_id", owner)) {
            _ui.update { it.copy(adminRoomName = "", adminNotice = "聊天室创建成功") }
            loadAdminRooms()
        }
    }

    fun adminBroadcast() {
        val text = _ui.value.adminBroadcastText.trim()
        if (text.isBlank()) {
            _ui.update { it.copy(adminError = "请填写广播内容") }
            return
        }
        val body = JSONObject().put("scope", "all").put("text", text)
        adminPost("/api/admin/broadcast", body) {
            _ui.update { it.copy(adminBroadcastText = "", adminNotice = "广播任务已提交") }
        }
    }

    fun showNewConversation() = _ui.update { it.copy(showNewConversation = true, newPeerInput = "", error = null) }
    fun hideNewConversation() = _ui.update { it.copy(showNewConversation = false) }
    fun createNewConversation() {
        val peer = _ui.value.newPeerInput.trim().toLongOrNull()
        val auth = _ui.value.auth
        if (peer == null || peer <= 0 || auth == null) {
            _ui.update { it.copy(error = "请输入有效的数字 UserID") }
            return
        }
        if (isDesktopAgent(peer)) {
            _ui.update { it.copy(error = "PC Agent 通道已隔离，请使用 Android 专用 Agent 联系人") }
            return
        }
        hideNewConversation()
        openConversation(knownConversation(auth.userId, peer))
    }

    fun loadMoreHistory() {
        val selected = _ui.value.selected ?: return
        if (_ui.value.loadingMore || _ui.value.messages.isEmpty()) return
        loadHistory(selected, false)
    }

    fun refreshUnread() {
        val auth = _ui.value.auth ?: return
        val selected = _ui.value.selected ?: return
        viewModelScope.launch {
            try {
                val response = client.post("/api/session/unread", JSONObject().put("user_id", auth.userId).put("session_id", selected.sessionId), auth.token)
                if (response.optInt("code", -1) == 0) {
                    _ui.update { current ->
                        if (current.selected?.sessionId != selected.sessionId) current
                        else current.copy(unreadCount = response.optJSONObject("data")?.optLong("unread", 0) ?: 0)
                    }
                }
            } catch (_: Exception) { }
        }
    }

    fun markSelectedRead() {
        val auth = _ui.value.auth ?: return
        val selected = _ui.value.selected ?: return
        val readSeq = _ui.value.messages.maxOfOrNull { it.seq } ?: return
        if (readSeq <= 0) return
        viewModelScope.launch {
            try {
                val response = client.post("/api/session/mark_read", JSONObject().put("user_id", auth.userId).put("session_id", selected.sessionId).put("read_seq", readSeq), auth.token)
                if (response.optInt("code", -1) == 0) _ui.update { it.copy(unreadCount = 0, notice = "已标记为已读") }
            } catch (e: Exception) { _ui.update { it.copy(error = e.message ?: "标记已读失败") } }
        }
    }

    private fun loadHistory(conversation: Conversation, initial: Boolean) {
        val auth = _ui.value.auth ?: return
        val anchor = if (initial) 0 else (_ui.value.messages.firstOrNull { it.seq > 0 }?.seq ?: return)
        _ui.update { it.copy(loading = initial, loadingMore = !initial, error = null) }
        viewModelScope.launch {
            try {
                val response = client.post("/api/session/history", JSONObject().put("session_id", conversation.sessionId).put("anchor_seq", anchor).put("limit", 50), auth.token)
                val code = response.optInt("code", -1)
                if (code != 0 && !(initial && code == 404)) throw IllegalStateException(response.optString("message", "历史消息读取失败"))
                val array = response.optJSONObject("data")?.optJSONArray("messages") ?: JSONArray()
                val parsed = withContext(Dispatchers.Default) {
                    buildList {
                        for (i in 0 until array.length()) {
                            parseMessage(array.optJSONObject(i), conversation.sessionId)?.let { add(it) }
                        }
                    }
                }
                val historyBytes = parsed.sumOf { utf8Bytes(it.text) }
                val historyChars = parsed.sumOf { unicodeChars(it.text) }
                Log.i(
                    CLIENT_TIMING_TAG,
                    "length_audit stage=android_history_display session=${conversation.sessionId} " +
                        "message_count=${parsed.size} bytes=${historyBytes} chars=${historyChars}"
                )
                parsed.filter { it.seq > 0 }.maxOfOrNull { it.seq }?.let { cursors[conversation.sessionId] = maxOf(cursors[conversation.sessionId] ?: 0, it) }
                _ui.update { current ->
                    if (current.selected?.sessionId != conversation.sessionId) current
                    else {
                        val merged = if (initial) parsed else (parsed + current.messages).distinctBy { messageKey(it) }.sortedWith(compareBy<ChatMessage> { it.seq == 0L }.thenBy { it.seq })
                        current.copy(messages = merged, hasHistory = merged.any { it.seq > 0L }, loading = false, loadingMore = false, notice = if (parsed.isEmpty() && initial) "暂无历史消息" else current.notice)
                    }
                }
            } catch (e: Exception) {
                _ui.update { current ->
                    if (current.selected?.sessionId != conversation.sessionId) current
                    else current.copy(loading = false, loadingMore = false, error = e.message ?: "历史消息读取失败")
                }
            }
        }
    }

    fun sendDraft() {
        val text = _ui.value.draft.trim()
        if (text.isNotEmpty()) sendMessage(text)
    }

    fun sendMessage(text: String, action: AgentAction? = null): Boolean {
        val current = _ui.value
        val auth = current.auth ?: return false
        val selected = current.selected ?: run {
            _ui.update { it.copy(error = "请先选择会话") }
            return false
        }
        if (text.isBlank()) return false
        if (current.connection != ConnectionState.CONNECTED) {
            _ui.update { it.copy(error = "实时连接尚未建立，请稍候重试") }
            return false
        }
        val clientId = "${System.currentTimeMillis()}-${UUID.randomUUID().toString().take(8)}"
        val content = JSONObject().put("text", text)
        if (action != null) {
            val actionJson = JSONObject().put("kind", action.id)
            action.value?.let { actionJson.put("value", it) }
            action.provider?.let { actionJson.put("provider", it) }
            action.modelId?.let { actionJson.put("modelId", it) }
            content.put("agent_action", actionJson)
        }
        val payload = JSONObject().put("type", "single_chat").put("to_user_id", selected.peerId).put("client_msg_id", clientId).put("content", content)
        val optimistic = ChatMessage(clientMsgId = clientId, sessionId = selected.sessionId, senderId = auth.userId, timestampMs = System.currentTimeMillis(), text = text, state = DeliveryState.SENDING)
        _ui.update { it.copy(messages = it.messages + optimistic, draft = "", error = null) }
        registerTiming(clientId, selected.sessionId, selected.agent, nowNanos())
        val sent = client.send(payload)
        if (!sent) {
            updateMessage(clientId) { it.copy(state = DeliveryState.FAILED) }
            completeTimingForClient(clientId, "send_failed")
        }
        return sent
    }

    fun sendAgentAction(action: AgentAction) {
        val command = when (action.id) {
            "agent_set" -> "/agent ${action.value.orEmpty()}"
            "reasoning_set" -> "/reasoning ${action.value.orEmpty()}"
            "select_model" -> "/model ${action.provider}:${action.modelId}"
            "list_provider_models" -> "/model ${action.provider.orEmpty()}:"
            "list_models" -> "/model"
            "refresh_models" -> "/model refresh"
            "reset_model" -> "/model default"
            "restart_confirm" -> "/restart now"
            "new_confirm" -> "/new now"
            "retry_confirm" -> "/retry now"
            else -> "/${action.id}"
        }
        sendMessage(command, action)
    }

    fun activateAgentAction(action: AgentAction) {
        if (action.id == "agent_open") {
            val peer = action.value?.toLongOrNull()
            if (peer != null && peer > 0) openKnownAgent(peer) else _ui.update { it.copy(error = "Agent 联系人 ID 无效") }
        } else {
            sendAgentAction(action)
        }
    }

    private fun updateMessage(clientId: String, transform: (ChatMessage) -> ChatMessage) {
        _ui.update { state ->
            val index = state.messages.indexOfFirst { it.clientMsgId == clientId }
            if (index < 0) state
            else {
                val old = state.messages[index]
                val updated = transform(old)
                if (updated == old) state else state.copy(messages = state.messages.toMutableList().also { it[index] = updated })
            }
        }
    }

    override fun onState(state: ConnectionState) {
        viewModelScope.launch(eventDispatcher) {
            if (state == ConnectionState.RECONNECTING || state == ConnectionState.DISCONNECTED) {
                clearStreams(null, "disconnect", showFailure = true)
            }
            _ui.update { it.copy(connection = state, notice = if (state == ConnectionState.CONNECTED) null else it.notice) }
            if (state == ConnectionState.CONNECTED) {
                val selected = _ui.value.selected
                if (selected != null) client.sync(selected.sessionId, cursors[selected.sessionId] ?: 0)
                _ui.value.auth?.let { loadSessions(it) }
            } else if (state == ConnectionState.RECONNECTING && _ui.value.auth != null) scheduleReconnect()
        }
    }

    override fun onEvent(raw: String) {
        eventQueue.trySend(raw)
    }

    override fun onFailure(message: String) {
        viewModelScope.launch(eventDispatcher) {
            clearStreams(null, "failure", showFailure = true)
            _ui.update { it.copy(notice = message.take(180)) }
        }
    }

    private fun handleEvent(event: JSONObject) {
        when (event.optString("type")) {
            "accepted_ack" -> {
                val clientId = event.optString("client_msg_id")
                if (clientId.isNotBlank()) {
                    recordAccepted(event)
                    updateMessage(clientId) { it.copy(msgId = event.optString("msg_id"), seq = event.optLong("msg_seq", it.seq), state = DeliveryState.ACCEPTED) }
                }
            }
            "delivered_ack" -> {
                val clientId = event.optString("client_msg_id")
                if (clientId.isNotBlank()) {
                    timingsByClientId[clientId]?.let { if (!it.agent) completeTiming(it, "delivered") }
                    updateMessage(clientId) { it.copy(state = DeliveryState.DELIVERED) }
                }
            }
            "hermes_delta" -> handleDelta(event)
            "error" -> {
                val clientId = event.optString("client_msg_id")
                val requestId = event.optString("request_id")
                if (requestId.isNotBlank()) discardStreamById(requestId, "Hermes 请求失败，请重试", "error")
                if (clientId.isNotBlank()) {
                    completeTimingForClient(clientId, "error")
                    updateMessage(clientId) { it.copy(state = DeliveryState.FAILED) }
                }
                _ui.update { it.copy(error = event.optString("message", "服务端发送失败")) }
            }
            "sync_end", "ack" -> Unit
            else -> parseMessage(event, event.optString("session_id").ifBlank { null })?.let { incoming ->
                observeSequence(incoming)
                val requestId = incoming.clientMsgId
                    .removePrefix("hermes:")
                    .takeIf { incoming.clientMsgId.startsWith("hermes:") && it.isNotBlank() }
                requestId?.let { id ->
                    val existingTiming = timingsByRequestId[id]
                    val timing = if (existingTiming != null || streamAccumulators.containsKey(id)) {
                        existingTiming ?: timingForRequest(id, incoming.sessionId)
                    } else {
                        null
                    }
                    if (timing != null) {
                        val rawText = extractRawMessageText(event)
                        timing.finalBytes = utf8Bytes(rawText)
                        timing.finalChars = unicodeChars(rawText)
                        timing.displayBytes = utf8Bytes(incoming.text)
                        timing.displayChars = unicodeChars(incoming.text)
                    }
                    val raw = event.opt("content")
                    val outer = when (raw) {
                        is JSONObject -> raw
                        is String -> try { JSONObject(raw) } catch (_: Exception) { null }
                        else -> null
                    }
                    val nested = outer?.optJSONObject("content")
                    val content = nested ?: outer
                    if (timing != null) {
                        timing.provider = content?.optString("hermes_provider", "").orEmpty().ifBlank { timing.provider }
                        timing.model = content?.optString("hermes_model", "").orEmpty().ifBlank { timing.model }
                    }
                }
                mergeIncoming(incoming)
            }
        }
    }

    private fun handleDelta(event: JSONObject) {
        val selected = _ui.value.selected ?: return
        val sessionId = event.optString("session_id")
        if (sessionId.isNotBlank() && sessionId != selected.sessionId) return
        val requestId = event.optString("request_id").trim()
        if (terminalStreamRequests.contains(requestId)) return
        val delta = event.optString("delta")
        if (requestId.isBlank() || delta.isEmpty()) return
        val now = nowNanos()
        val timing = timingForRequest(requestId, selected.sessionId)
        timing.stream = true
        val isProgress = event.optBoolean("progress", false)
        if (!isProgress && timing.firstDeltaAtNanos == null) timing.firstDeltaAtNanos = now
        val existing = streamAccumulators[requestId]
        val isNew = existing == null
        val active = existing ?: StreamAccumulator(
            requestId = requestId,
            sessionId = selected.sessionId,
            senderId = event.optLong("from_user_id", selected.peerId).takeIf { it > 0L } ?: selected.peerId
        ).also {
            it.lastActivityAtNanos = now
            streamAccumulators[requestId] = it
        }
        active.lastActivityAtNanos = now
        val accepted = acceptDelta(
            active,
            event.optInt("delta_index", -1),
            PendingStreamDelta(delta, isProgress)
        )
        if (accepted) {
            timing.chunkCount += 1
            timing.deltaBytes += utf8Bytes(delta)
            timing.deltaChars += unicodeChars(delta)
        }
        ensureStreamFlushLoop()
        // Make the first visible token appear as soon as the background worker
        // receives it; all subsequent deltas are coalesced by the 40 ms loop.
        if (isNew) flushAccumulator(active, now, force = true)
    }

    private fun discardStreamById(requestId: String, message: String, reason: String) {
        streamAccumulators[requestId]?.let { discardStream(it, message, reason, showFailure = true) }
        if (!streamAccumulators.containsKey(requestId)) {
            markTerminalStream(requestId)
            completeTimingForRequest(requestId, reason)
        }
    }

    private fun mergeIncoming(message: ChatMessage) {
        val requestId = message.clientMsgId.removePrefix("hermes:").takeIf { message.clientMsgId.startsWith("hermes:") && it.isNotBlank() }
        val selected = _ui.value.selected
        if (selected == null || message.sessionId != selected.sessionId) {
            requestId?.let { discardStreamById(it, "连接已切换", "session_switch") }
            return
        }
        val accumulator = requestId?.let { streamAccumulators[it] }
        val now = nowNanos()
        // The final durable message is authoritative, but any buffered deltas
        // must be published before it replaces the temporary stream bubble.
        accumulator?.let { flushAccumulator(it, now, force = true) }
        _ui.update { state ->
            val streamKey = requestId?.let { "stream:$it" }.orEmpty()
            val idx = state.messages.indexOfFirst { messageKey(it) == messageKey(message) || (streamKey.isNotEmpty() && it.clientMsgId == streamKey) }
            val delivered = message.copy(state = DeliveryState.DELIVERED, streaming = false)
            if (idx >= 0) {
                state.copy(messages = state.messages.toMutableList().also { it[idx] = delivered }, hasHistory = state.hasHistory || message.seq > 0L)
            } else {
                state.copy(messages = state.messages + delivered, hasHistory = state.hasHistory || message.seq > 0L)
            }
        }
        if (requestId != null) {
            markTerminalStream(requestId)
            streamAccumulators.remove(requestId)
            completeTimingForRequest(requestId, "completed")
        }
    }

    private fun observeSequence(message: ChatMessage) {
        if (message.seq <= 0) return
        val sid = message.sessionId
        val seen = seenSequences.getOrPut(sid) { mutableSetOf() }
        seen += message.seq
        var cursor = cursors[sid] ?: 0
        while (seen.remove(cursor + 1)) cursor++
        cursors[sid] = cursor
        if (message.seq > cursor + 1) client.sync(sid, cursor)
    }

    private fun extractRawMessageText(obj: JSONObject): String {
        val raw = obj.opt("content")
        val outer = when (raw) {
            is JSONObject -> raw
            is String -> try { JSONObject(raw) } catch (_: Exception) { null }
            else -> null
        }
        val nested = outer?.opt("content")
        val content = when (nested) {
            is JSONObject -> nested
            is String -> try { JSONObject(nested) } catch (_: Exception) { outer }
            else -> outer
        }
        return sequenceOf(
            content?.optString("text", ""),
            when (nested) {
                is String -> if (nested.trim().startsWith("{")) "" else nested
                else -> ""
            },
            outer?.optString("text", ""),
            obj.optString("text", ""),
            if (raw is String && outer == null) raw else ""
        ).filterNotNull().firstOrNull { it.isNotBlank() }.orEmpty()
    }

    private fun parseMessage(obj: JSONObject?, sessionOverride: String? = null): ChatMessage? {
        if (obj == null) return null
        val session = sessionOverride?.takeIf { it.isNotBlank() } ?: obj.optString("session_id")
        if (session.isBlank()) return null
        val raw = obj.opt("content")
        val outer = when (raw) {
            is JSONObject -> raw
            is String -> try { JSONObject(raw) } catch (_: Exception) { null }
            else -> null
        }
        val nested = outer?.opt("content")
        val content = when (nested) {
            is JSONObject -> nested
            is String -> try { JSONObject(nested) } catch (_: Exception) { outer }
            else -> outer
        }
        val text = extractRawMessageText(obj)
        val displayText = cleanDisplayText(text)
        val card = (content?.optJSONObject("agent_event") ?: outer?.optJSONObject("agent_event"))?.let { parseAgentCard(it) }
        val citations = parseCitations(content?.optJSONArray("citations") ?: outer?.optJSONArray("citations"))
        // 控制帧或只有包装元数据的记录没有可见内容，不能交给 Compose 绘制；
        // 否则圆角 Surface 会退化成空白小圆点。
        if (displayText.isBlank() && card == null && citations.isEmpty()) return null
        val sender = obj.optLong("sender_id", obj.optLong("from_user_id", 0)).takeIf { it > 0 }
            ?: outer?.optLong("from_user_id", 0)?.takeIf { it > 0 } ?: 0
        val clientId = obj.optString("client_msg_id").ifBlank { outer?.optString("client_msg_id").orEmpty() }
        val sequence = obj.optLong("msg_seq", 0).takeIf { it > 0 } ?: outer?.optLong("msg_seq", 0) ?: 0
        val timestamp = obj.optLong("timestamp_ms", 0).takeIf { it > 0 }
            ?: outer?.optLong("create_time", 0)?.takeIf { it > 0 } ?: System.currentTimeMillis()
        return ChatMessage(
            obj.optString("msg_id").ifBlank { outer?.optString("msg_id").orEmpty() },
            clientId,
            session,
            sequence,
            sender,
            timestamp,
            displayText,
            content?.optString("format") == "markdown" || outer?.optString("format") == "markdown",
            agentCard = card,
            citations = citations
        )
    }

    private fun parseCitations(array: JSONArray?): List<Citation> {
        if (array == null) return emptyList()
        return buildList {
            for (i in 0 until minOf(array.length(), 20)) {
                val item = array.optJSONObject(i) ?: continue
                val docId = item.optString("doc_id")
                val chunkId = item.optString("chunk_id")
                if (docId.isBlank() || chunkId.isBlank()) continue
                val locator = item.optJSONObject("locator")?.let { loc -> "${loc.optString("type")} ${loc.optString("value")}".trim() }.orEmpty()
                add(Citation(item.optString("citation_id", "ref_${i + 1}"), docId, chunkId, item.optString("title", "CANN 资料"), item.optString("authority"), locator))
            }
        }
    }

    private fun parseAgentCard(event: JSONObject): AgentCard? {
        val data = event.optJSONObject("data") ?: return null
        val presentation = data.optJSONObject("presentation") ?: return null
        val kind = presentation.optString("kind", "command_card")
        val title = presentation.optString("title").ifBlank {
            if (kind == "model_picker") "选择模型" else "Agent 操作"
        }
        val summary = when (kind) {
            "creative_workspace" -> cleanDisplayText(data.optString("text", "写作工作区已更新"))
            else -> cleanDisplayText(data.optString("text", ""))
        }
        val actions = mutableListOf<AgentAction>()
        val array = presentation.optJSONArray("actions") ?: JSONArray()
        for (i in 0 until minOf(array.length(), 16)) {
            val item = array.optJSONObject(i) ?: continue
            val id = item.optString("id")
            if (id.isNotBlank()) actions += AgentAction(id, item.optString("label", id), item.optString("value").ifBlank { null }, item.optString("provider").ifBlank { null }, item.optString("modelId").ifBlank { null })
        }
        val artifactName = presentation.optJSONObject("artifact")?.optString("name").orEmpty().ifBlank { null }
        val currentModel = if (kind == "model_picker") {
            parseAgentModel(presentation.optJSONObject("current")
                ?: data.optJSONObject("model_state")?.optJSONObject("model"))
        } else null
        val models = if (kind == "model_picker") {
            val modelArray = presentation.optJSONArray("models") ?: JSONArray()
            buildList {
                val seen = HashSet<String>()
                for (i in 0 until minOf(modelArray.length(), 512)) {
                    parseAgentModel(modelArray.optJSONObject(i))?.let { model ->
                        val key = model.provider + "|" + model.id
                        if (seen.add(key)) add(model)
                    }
                }
            }
        } else emptyList()
        val providers = if (kind == "model_picker") {
            val providerArray = presentation.optJSONArray("providers") ?: JSONArray()
            buildList {
                val seen = HashSet<String>()
                for (i in 0 until minOf(providerArray.length(), 128)) {
                    val item = providerArray.optJSONObject(i) ?: continue
                    val id = item.optString("id").trim()
                    val name = item.optString("name").trim().ifBlank { id }
                    if (id.isBlank() || name.isBlank() || !seen.add(id)) continue
                    add(AgentProvider(id, name, item.optInt("model_count", 0).coerceAtLeast(0), item.optBoolean("current", false)))
                }
            }
        } else emptyList()
        val selectedProvider = presentation.optString("selected_provider").trim().ifBlank { null }
        return AgentCard(
            title = title,
            kind = kind,
            summary = summary,
            actions = actions,
            artifactName = artifactName,
            currentModel = currentModel,
            models = models,
            providers = providers,
            selectedProvider = selectedProvider,
            truncated = presentation.optBoolean("truncated", false)
        )
    }

    private fun parseAgentModel(item: JSONObject?): AgentModel? {
        if (item == null) return null
        val provider = item.optString("provider").trim()
        val id = item.optString("id").trim()
        if (provider.isBlank() || id.isBlank() || provider.contains(":") || provider.length > 128 || id.length > 128) return null
        val name = item.optString("name").trim().ifBlank { id }.take(640)
        return AgentModel(provider, id, name, item.optBoolean("reasoning", false))
    }

    private fun cleanDisplayText(value: String): String {
        val match = DisplayPrefixRegex.find(value) ?: return value
        var start = match.range.last + 1
        // AgentRouter adds exactly two line breaks after its display label.
        // Remove only that transport decoration; preserve any additional
        // leading blank lines belonging to the model's Markdown.
        if (value.startsWith("\r\n\r\n", start)) start += 4
        else if (value.startsWith("\n\n", start)) start += 2
        else if (value.startsWith("\r\n", start)) start += 2
        else if (value.startsWith("\n", start)) start += 1
        return value.substring(start)
    }

    private fun messageKey(message: ChatMessage): String = when {
        message.msgId.isNotBlank() -> "m:${message.msgId}"
        message.clientMsgId.isNotBlank() -> "c:${message.clientMsgId}"
        message.seq > 0 -> "s:${message.sessionId}:${message.seq}"
        else -> "t:${message.sessionId}:${message.senderId}:${message.timestampMs}:${message.text}"
    }

    fun logout() { adminLogout(); clearStoredAuth() }
    fun reconnect() { _ui.value.auth?.let { connectSocket(it) } }

    override fun onCleared() {
        eventQueue.close()
        eventJob?.cancel()
        streamFlushJob?.cancel()
        streamAccumulators.clear()
        timingsByClientId.clear()
        timingsByRequestId.clear()
        terminalStreamRequests.clear()
        client.close()
        super.onCleared()
    }
}
