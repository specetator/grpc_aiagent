package com.peco.sparkim

import android.app.Application
import android.content.Context
import android.os.SystemClock
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CoroutineScope
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
import java.util.ArrayDeque
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private val DisplayPrefixRegex = Regex("^\\s*【[^】]{1,100}】")

private const val STREAM_FLUSH_INTERVAL_MS = 40L
private const val STREAM_TIMEOUT_NANOS = 150_000_000_000L
private const val CLIENT_TIMING_TAG = "SparkTiming"

private fun utf8Bytes(value: String): Int = value.toByteArray(Charsets.UTF_8).size
private fun unicodeChars(value: String): Int =
    value.codePointCount(0, value.length)

private class StreamAccumulator(
    val requestId: String,
    val sessionId: String,
    val senderId: Long
) {
    val messageKey: String = "stream:$requestId"
    val reorder = BoundedReorderBuffer()
    val seenEventIds = BoundedEventIdSet()
    val text = StringBuilder()
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
    var agentStartAtNanos: Long? = null,
    var firstDeltaAtNanos: Long? = null,
    var historyReadableAtNanos: Long? = null,
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
    private val messageStore = SparkMessageStore(app)
    private val deviceId = prefs.getString("device_id", null) ?: UUID.randomUUID().toString().also {
        check(prefs.edit().putString("device_id", it).commit()) { "设备身份保存失败" }
    }
    @Volatile private var activeScope = ""
    private var outboxJob: Job? = null
    private var lastReceiptSendAtMs = 0L
    private var lastReceiptScope = ""
    private val _ui = MutableStateFlow(SparkUiState())
    val ui = _ui.asStateFlow()
    private var reconnectJob: Job? = null
    // One ordered worker parses WebSocket frames and mutates stream state.
    // This avoids one coroutine and one JSON parse racing for every delta.
    private val eventDispatcher = Dispatchers.Default.limitedParallelism(1)
    private val eventQueue = Channel<Pair<String, String>>(MAX_EVENT_QUEUE)
    private var eventJob: Job? = null
    private var streamFlushJob: Job? = null
    private val terminalStreamRequests = LinkedHashSet<String>()
    private val streamAccumulators = LinkedHashMap<String, StreamAccumulator>()
    private val timingsByClientId = ConcurrentHashMap<String, ClientRequestTiming>()
    private val timingsByRequestId = ConcurrentHashMap<String, ClientRequestTiming>()
    private val timingSamples = ArrayDeque<Long>()
    private val realtimeBacklog = AtomicBoolean(false)

    init {
        eventJob = viewModelScope.launch(eventDispatcher) {
            for ((scope, raw) in eventQueue) {
                if (scope.isBlank() || scope != activeScope) continue
                try {
                    handleEvent(JSONObject(raw), scope)
                } catch (error: Exception) {
                    if (scope != activeScope) continue
                    if (error is android.database.SQLException) {
                        // No receive ACK was emitted. Reconnect requests the missing durable frame
                        // again once storage recovers, even if this was the last live message.
                        client.close()
                        _ui.update { it.copy(connection = ConnectionState.RECONNECTING, notice = "本地消息保存失败，等待恢复后重新同步") }
                        scheduleReconnect()
                    } else {
                        _ui.update { it.copy(notice = "收到无法解析的实时消息") }
                    }
                }
            }
        }
        ensureStreamFlushLoop()
        outboxJob = viewModelScope.launch(eventDispatcher) {
            while (isActive) {
                delay(1000)
                val scope = activeScope
                if (scope.isBlank() || _ui.value.connection != ConnectionState.CONNECTED) continue
                try {
                    flushReceiveReceipts(scope)
                    flushOutbox(scope)
                } catch (e: Exception) {
                    _ui.update { it.copy(notice = "本地消息恢复等待重试：${e.message.orEmpty().take(120)}") }
                }
            }
        }
        val server = prefs.getString("server", null)
        val uid = prefs.getLong("user_id", 0)
        val token = prefs.getString("token", null)
        val name = prefs.getString("name", "") ?: ""
        if (!server.isNullOrBlank()) {
            _ui.update { it.copy(serverInput = server, screen = if (uid > 0 && !token.isNullOrBlank()) Screen.HOME else Screen.AUTH) }
            if (uid > 0 && !token.isNullOrBlank() && client.configure(server)) {
                val auth = AuthSession(uid, token, name.ifBlank { "用户 $uid" })
                activeScope = "${client.currentEndpoint()?.logicBase}|${auth.userId}"
                _ui.update { it.copy(auth = auth, screen = Screen.HOME, notice = "正在恢复登录状态…") }
                viewModelScope.launch {
                    applySessionRestore(auth, verifySession(auth))
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
        val previousEndpoint = client.currentEndpoint()?.logicBase
        if (!client.configure(value)) {
            _ui.update { it.copy(error = "请输入有效的服务器地址，例如 http://100.89.19.125") }
            return
        }
        if (previousEndpoint != null && previousEndpoint != client.currentEndpoint()?.logicBase) clearStoredAuth()
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

    fun retrySessionRestore() {
        val auth = _ui.value.auth ?: return
        _ui.update { it.copy(notice = "正在重新校验登录状态…", restoreRetryAvailable = false, error = null) }
        viewModelScope.launch {
            applySessionRestore(auth, verifySession(auth))
        }
    }

    private suspend fun verifySession(auth: AuthSession): SessionRestoreResult = try {
        val response = client.post("/api/session/list_single", JSONObject().put("user_id", auth.userId), auth.token)
        SessionRestoreClassifier.classify(
            networkFailure = false,
            httpStatus = 200,
            bodyCode = response.optInt("code", -1),
            bodyMessage = response.optString("message")
        )
    } catch (error: SparkHttpException) {
        SessionRestoreClassifier.classify(
            networkFailure = error.networkFailure,
            httpStatus = error.statusCode,
            bodyCode = error.bodyCode,
            bodyMessage = error.message ?: ""
        )
    } catch (error: Exception) {
        SessionRestoreClassifier.classify(true, -1, -1, error.message ?: "")
    }

    private suspend fun applySessionRestore(auth: AuthSession, result: SessionRestoreResult) {
        withContext(eventDispatcher) {
            if (_ui.value.auth != auth) return@withContext
            when (result.kind) {
                SessionRestoreKind.VALID -> onAuthenticated(auth)
                SessionRestoreKind.INVALID_TOKEN -> {
                    clearStoredAuth()
                    _ui.update { it.copy(error = result.message, restoreRetryAvailable = false) }
                }
                SessionRestoreKind.UNREACHABLE -> {
                    _ui.update {
                        it.copy(
                            auth = auth,
                            screen = Screen.HOME,
                            loading = false,
                            error = null,
                            notice = result.message,
                            restoreRetryAvailable = true,
                            connection = ConnectionState.DISCONNECTED
                        )
                    }
                    loadLocalSessions(auth, activeScope)
                    scheduleReconnect()
                }
            }
        }
    }

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
        val totalMs = elapsed(timing.completedAtNanos, timing.sendAtNanos)
        if (timing.agent && totalMs >= 0) {
            synchronized(timingSamples) {
                timingSamples.addLast(totalMs)
                while (timingSamples.size > MAX_TIMING_SAMPLES) timingSamples.removeFirst()
            }
        }
        val samples = synchronized(timingSamples) { timingSamples.toList() }
        Log.i(
            CLIENT_TIMING_TAG,
            "client_timing request_id=${timing.requestId.orEmpty()} " +
                "client_msg_id=${timing.clientMsgId} client_trace_id=${timing.clientTraceId} " +
                "send_at=${timing.sendAtNanos} " +
                "accepted_at=${timing.acceptedAtNanos ?: 0L} " +
                "agent_start_at=${timing.agentStartAtNanos ?: 0L} " +
                "first_delta_at=${timing.firstDeltaAtNanos ?: 0L} " +
                "history_readable_at=${timing.historyReadableAtNanos ?: 0L} " +
                "completed_at=${timing.completedAtNanos ?: 0L} " +
                "accepted_ms=${elapsed(timing.acceptedAtNanos, timing.sendAtNanos)} " +
                "agent_start_ms=${elapsed(timing.agentStartAtNanos, timing.sendAtNanos)} " +
                "first_text_ms=${elapsed(timing.firstDeltaAtNanos, timing.sendAtNanos)} " +
                "final_ms=${elapsed(timing.completedAtNanos, timing.sendAtNanos)} " +
                "history_ms=${elapsed(timing.historyReadableAtNanos, timing.sendAtNanos)} " +
                "total_ms=$totalMs " +
                "provider=${timing.provider} model=${timing.model} " +
                "stream=${timing.stream} " +
                "chunk_count=${timing.chunkCount} delta_bytes=${timing.deltaBytes} " +
                "delta_chars=${timing.deltaChars} final_bytes=${timing.finalBytes} " +
                "final_chars=${timing.finalChars} display_bytes=${timing.displayBytes} " +
                "display_chars=${timing.displayChars} reason=${reason} " +
                "agent_latency=${TimingPercentiles.summarize(samples)}"
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

    private fun applyDelta(accumulator: StreamAccumulator, delta: StreamDelta) {
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

    private fun acceptDelta(
        accumulator: StreamAccumulator,
        decision: DeltaDecision,
        delta: StreamDelta
    ): Boolean {
        val result = accumulator.reorder.accept(decision, delta)
        when (result.status) {
            ReorderStatus.DROP, ReorderStatus.DUPLICATE -> return false
            ReorderStatus.OVERFLOW -> {
                discardStream(accumulator, "流式预览积压，已改为等待最终消息", "stream_overflow", showFailure = false)
                return false
            }
            ReorderStatus.BUFFERED -> return false
            ReorderStatus.APPLIED -> {
                for (item in result.applied) applyDelta(accumulator, item)
                return true
            }
        }
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
        activeScope = ""
        reconnectJob?.cancel()
        viewModelScope.launch(eventDispatcher) { clearStreams(null, "logout", showFailure = false) }
        client.close()
        prefs.edit().remove("user_id").remove("token").remove("name").apply()
        _ui.update { it.copy(auth = null, screen = Screen.AUTH, connection = ConnectionState.DISCONNECTED, conversations = emptyList(), selected = null, messages = emptyList(), hasHistory = false, loading = false, notice = null, restoreRetryAvailable = false) }
    }

    private fun onAuthenticated(auth: AuthSession) {
        activeScope = "${client.currentEndpoint()?.logicBase}|${auth.userId}"
        val scope = activeScope
        viewModelScope.launch(eventDispatcher) { loadLocalSessions(auth, scope) }
        _ui.update { it.copy(auth = auth, screen = Screen.HOME, loading = false, error = null, notice = "正在连接 Spark Push…", restoreRetryAvailable = false) }
        connectSocket(auth)
        loadSessions(auth)
    }

    private fun loadLocalSessions(auth: AuthSession, scope: String) {
        if (scope != activeScope || scope.isBlank()) return
        val peers = messageStore.sessions(scope).mapNotNull { sid ->
            val ids = sid.removePrefix("s_").split('_').mapNotNull { it.toLongOrNull() }
            if (!sid.startsWith("s_") || ids.size != 2 || auth.userId !in ids) null
            else ids.firstOrNull { it != auth.userId }
        }.filter { it > 0 && !isDesktopAgent(it) }
        val conversations = (listOf(ANDROID_HERMES_ID, ANDROID_PI_ID) + peers).distinct()
            .map { knownConversation(auth.userId, it) }
        _ui.update { if (scope == activeScope) it.copy(conversations = (conversations + it.conversations).distinctBy { c -> c.sessionId }) else it }
    }

    private fun connectSocket(auth: AuthSession) {
        reconnectJob?.cancel()
        client.connect(auth.token, deviceId, auth.userId, this)
    }

    private fun scheduleReconnect() {
        if (reconnectJob?.isActive == true) return
        reconnectJob = viewModelScope.launch {
            delay(1800)
            val auth = _ui.value.auth ?: return@launch
            client.connect(auth.token, deviceId, auth.userId, this@SparkViewModel)
        }
    }

    private fun loadSessions(auth: AuthSession? = null) {
        val session = auth ?: _ui.value.auth ?: return
        val scope = activeScope
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
                    if (scope != activeScope || current.auth?.userId != session.userId) current
                    else current.copy(conversations = conversations, notice = if (current.connection == ConnectionState.CONNECTED) null else current.notice)
                }
            } catch (e: Exception) {
                _ui.update { current ->
                    if (scope != activeScope || current.auth?.userId != session.userId) current
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
        val scope = activeScope
        viewModelScope.launch(eventDispatcher) {
            if (scope == activeScope && scope.isNotBlank()) {
                val local = messageStore.timeline(scope, conversation.sessionId).mapNotNull { stored ->
                    parseMessage(stored.wire, conversation.sessionId)?.copy(state = stored.state)
                }
                applyHistory(conversation, true, local)
                if (_ui.value.connection == ConnectionState.CONNECTED) {
                    client.sync(conversation.sessionId, messageStore.cursor(scope, conversation.sessionId))
                }
            }
        }
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
        val scope = activeScope
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
                withContext(eventDispatcher) {
                    if (scope != activeScope || scope.isBlank()) return@withContext
                    val wires = buildList {
                        for (i in 0 until array.length()) {
                            val original = array.optJSONObject(i) ?: continue
                            durableWire(original, conversation.sessionId)?.let { add(it) }
                        }
                    }
                    messageStore.save(scope, wires, auth.userId)
                    flushReceiveReceipts(scope)
                    applyHistory(conversation, initial, parsed)
                }
            } catch (e: SparkHttpException) {
                if (initial && (e.statusCode == 404 || e.bodyCode == 404)) {
                    withContext(eventDispatcher) { if (scope == activeScope) applyHistory(conversation, true, emptyList()) }
                } else {
                    withContext(eventDispatcher) {
                        _ui.update { current ->
                            if (current.selected?.sessionId != conversation.sessionId) current
                            else current.copy(loading = false, loadingMore = false, error = e.message ?: "历史消息读取失败")
                        }
                    }
                }
            } catch (e: Exception) {
                withContext(eventDispatcher) {
                    _ui.update { current ->
                        if (current.selected?.sessionId != conversation.sessionId) current
                        else current.copy(loading = false, loadingMore = false, error = e.message ?: "历史消息读取失败")
                    }
                }
            }
        }
    }

    private fun applyHistory(conversation: Conversation, initial: Boolean, parsed: List<ChatMessage>) {
        val historyBytes = parsed.sumOf { utf8Bytes(it.text) }
        val historyChars = parsed.sumOf { unicodeChars(it.text) }
        Log.i(
            CLIENT_TIMING_TAG,
            "length_audit stage=android_history_display session=${conversation.sessionId} " +
                "message_count=${parsed.size} bytes=${historyBytes} chars=${historyChars}"
        )
        val now = nowNanos()
        for (message in parsed) {
            val requestId = message.clientMsgId.removePrefix("hermes:").takeIf {
                message.clientMsgId.startsWith("hermes:") && it.isNotBlank()
            } ?: continue
            timingsByRequestId[requestId]?.let { timing ->
                if (timing.historyReadableAtNanos == null) timing.historyReadableAtNanos = now
            }
        }
        _ui.update { current ->
            if (current.selected?.sessionId != conversation.sessionId) current
            else {
                val kept = current.messages.filter { old -> parsed.none { sameMessage(old, it) } }
                val monotonic = parsed.map { message ->
                    if (current.messages.any { old -> sameMessage(old, message) && old.state == DeliveryState.DELIVERED }) message.copy(state = DeliveryState.DELIVERED) else message
                }
                val merged = (monotonic + kept).distinctBy { messageKey(it) }
                    .sortedWith(compareBy<ChatMessage> { it.seq == 0L }.thenBy { it.seq }.thenBy { it.timestampMs })
                current.copy(
                    messages = merged,
                    hasHistory = merged.any { it.seq > 0L },
                    loading = false,
                    loadingMore = false,
                    notice = if (parsed.isEmpty() && initial) "暂无历史消息" else current.notice
                )
            }
        }
    }

    fun sendDraft() {
        val snapshot = _ui.value
        if (snapshot.draft.trim().isNotEmpty() || snapshot.pendingImages.isNotEmpty()) {
            sendMessage(snapshot.draft)
        }
    }

    fun addPendingImage(name: String, mime: String, bytes: ByteArray) {
        if (bytes.isEmpty() || bytes.size > 4 * 1024 * 1024) {
            _ui.update { it.copy(error = "图片不能超过 4 MiB") }
            return
        }
        if (_ui.value.pendingImages.size >= 4) {
            _ui.update { it.copy(error = "一条消息最多 4 张图片") }
            return
        }
        val encoded = android.util.Base64.encodeToString(bytes, android.util.Base64.NO_WRAP)
        _ui.update { it.copy(pendingImages = it.pendingImages + PendingChatImage(name, mime, encoded), error = null) }
    }

    fun removePendingImage(index: Int) {
        _ui.update {
            if (index !in it.pendingImages.indices) it
            else it.copy(pendingImages = it.pendingImages.toMutableList().also { list -> list.removeAt(index) })
        }
    }

    fun ensureImagePreview(id: String) {
        if (id.isBlank() || _ui.value.imagePreviews.containsKey(id)) return
        val auth = _ui.value.auth ?: return
        viewModelScope.launch {
            try {
                val response = client.post("/api/attachment/get", JSONObject().put("id", id), auth.token)
                val payload = response.optJSONObject("data") ?: return@launch
                val encoded = payload.optString("data")
                if (encoded.isBlank()) return@launch
                val bytes = android.util.Base64.decode(encoded, android.util.Base64.DEFAULT)
                val bitmap = android.graphics.BitmapFactory.decodeByteArray(bytes, 0, bytes.size) ?: return@launch
                _ui.update { it.copy(imagePreviews = it.imagePreviews + (id to bitmap)) }
            } catch (_: Exception) {
            }
        }
    }

    fun sendMessage(text: String, action: AgentAction? = null): Boolean {
        val current = _ui.value
        val auth = current.auth ?: return false
        val selected = current.selected ?: run {
            _ui.update { it.copy(error = "请先选择会话") }
            return false
        }
        val pending = current.pendingImages
        if (text.isBlank() && pending.isEmpty()) return false
        val scope = activeScope
        if (scope.isBlank()) return false
        viewModelScope.launch(eventDispatcher) {
            try {
                if (scope != activeScope) return@launch
                val bodyText = text.trim().ifBlank { if (pending.isNotEmpty()) "请查看这张图片。" else "" }
                val clientId = UUID.randomUUID().toString()
                val content = JSONObject().put("text", bodyText)
                if (action != null) {
                    val actionJson = JSONObject().put("kind", action.id)
                    action.value?.let { actionJson.put("value", it) }
                    action.provider?.let { actionJson.put("provider", it) }
                    action.modelId?.let { actionJson.put("modelId", it) }
                    content.put("agent_action", actionJson)
                }
                val payload = JSONObject().put("type", "single_chat").put("to_user_id", selected.peerId).put("client_msg_id", clientId).put("content", content)
                if (pending.isNotEmpty()) payload.put("_local_images", JSONArray().apply {
                    pending.forEach { put(JSONObject().put("name", it.name).put("mime", it.mime).put("data", it.data)) }
                })
                val optimistic = ChatMessage(
                    clientMsgId = clientId, sessionId = selected.sessionId, senderId = auth.userId,
                    timestampMs = System.currentTimeMillis(), text = bodyText, state = DeliveryState.SENDING
                )
                // Persist the stable ID, complete frame and local attachment bytes before any network work.
                messageStore.enqueue(scope, payload, canonicalWire(JSONObject().put("content", content), optimistic))
                _ui.update { state ->
                    if (activeScope != scope) state else state.copy(
                        messages = if (state.selected?.sessionId == selected.sessionId) state.messages + optimistic else state.messages,
                        draft = if (state.selected?.sessionId == selected.sessionId && state.draft.trim() == text.trim()) "" else state.draft,
                        pendingImages = state.pendingImages.filter { it !in pending }, error = null,
                        notice = if (state.connection != ConnectionState.CONNECTED) "消息已保存，将在连接恢复后发送" else null
                    )
                }
                registerTiming(clientId, selected.sessionId, selected.agent, nowNanos())
            } catch (exc: Exception) {
                _ui.update { it.copy(error = exc.message ?: "图片发送失败") }
            }
        }
        return true
    }

    private fun flushReceiveReceipts(scope: String, force: Boolean = false) {
        if (scope != activeScope || _ui.value.connection != ConnectionState.CONNECTED) return
        val now = SystemClock.elapsedRealtime()
        if (!force && lastReceiptScope == scope && now - lastReceiptSendAtMs < 1000) return
        lastReceiptScope = scope
        lastReceiptSendAtMs = now
        for ((session, sequences) in messageStore.receipts(scope)) {
            if (!client.received(session, messageStore.cursor(scope, session), sequences, scope)) break
        }
    }

    private suspend fun flushOutbox(scope: String) {
        val auth = _ui.value.auth ?: return
        for (entry in messageStore.due(scope, System.currentTimeMillis())) {
            if (scope != activeScope || _ui.value.connection != ConnectionState.CONNECTED) return
            // An ACK timeout follows the same bounded retry path as a failed socket send.
            messageStore.recordAttempt(scope, entry, System.currentTimeMillis())
            try {
                val images = entry.frame.optJSONArray("_local_images")
                if (images != null) {
                    val attachments = entry.frame.getJSONObject("content").optJSONArray("attachments") ?: JSONArray()
                    for (i in 0 until images.length()) {
                        val image = images.getJSONObject(i)
                        if (image.optBoolean("uploaded")) continue
                        val response = client.post("/api/attachment/upload", JSONObject()
                            .put("session_id", entry.sessionId).put("name", image.getString("name"))
                            .put("data", image.getString("data")), auth.token)
                        val code = response.optInt("code", -1)
                        if (code != 0) throw SparkHttpException(200, code, response.optString("message", "图片上传失败"))
                        if (scope != activeScope) return
                        val data = response.getJSONObject("data")
                        check(data.optString("id").isNotBlank()) { "图片上传响应缺少 ID" }
                        attachments.put(JSONObject().put("id", data.getString("id")).put("name", data.optString("name"))
                            .put("mime", data.optString("mime")).put("bytes", data.optInt("bytes")))
                        image.put("uploaded", true).remove("data")
                        entry.frame.getJSONObject("content").put("attachments", attachments)
                        // Persist each upload result so a later upload failure does not repeat earlier uploads.
                        messageStore.replaceFrame(scope, entry.clientId, entry.frame)
                    }
                    entry.frame.remove("_local_images")
                    messageStore.replaceFrame(scope, entry.clientId, entry.frame)
                    updateMessage(entry.clientId) { it.copy(attachments = parseAttachments(entry.frame.getJSONObject("content").optJSONArray("attachments"))) }
                }
                if (scope == activeScope) client.send(entry.frame, scope)
            } catch (e: SparkHttpException) {
                if (scope != activeScope) return
                if (!e.networkFailure && e.statusCode < 500 && e.bodyCode < 500 &&
                    e.statusCode !in listOf(408, 429) && e.bodyCode !in listOf(408, 429)) {
                    messageStore.setState(scope, entry.clientId, DeliveryState.FAILED)
                    updateMessage(entry.clientId) { it.copy(state = DeliveryState.FAILED) }
                    completeTimingForClient(entry.clientId, "upload_rejected")
                }
            }
        }
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
        val scopeAtCallback = activeScope
        viewModelScope.launch(eventDispatcher) {
            if (scopeAtCallback.isBlank() || scopeAtCallback != activeScope) return@launch
            if (state == ConnectionState.RECONNECTING || state == ConnectionState.DISCONNECTED) {
                clearStreams(null, "disconnect", showFailure = true)
            }
            _ui.update { it.copy(connection = state, notice = if (state == ConnectionState.CONNECTED) null else it.notice) }
            if (state == ConnectionState.CONNECTED) {
                realtimeBacklog.set(false)
                val scope = activeScope
                if (scope.isBlank()) return@launch
                // Repeat both sparse receipts and explicit sync after process/socket restart.
                flushReceiveReceipts(scope, force = true)
                val selected = _ui.value.selected
                if (selected != null) client.sync(selected.sessionId, messageStore.cursor(scope, selected.sessionId))
                _ui.value.auth?.let { loadSessions(it) }
            } else if (state == ConnectionState.RECONNECTING && _ui.value.auth != null) scheduleReconnect()
        }
    }

    override fun onEvent(raw: String) {
        if (realtimeBacklog.get() && looksLikeStreamDelta(raw)) return
        val accepted = eventQueue.trySend(activeScope to raw)
        if (accepted.isSuccess) return
        if (realtimeBacklog.compareAndSet(false, true)) {
            viewModelScope.launch(eventDispatcher) {
                stopStreamPreviews("实时通道积压，已停止临时预览，等待最终消息或历史同步", "event_queue_overflow")
                // Reconnect replays unacknowledged durable frames, including other conversations.
                client.close()
                _ui.update { it.copy(connection = ConnectionState.RECONNECTING) }
                scheduleReconnect()
            }
        }
    }

    private fun stopStreamPreviews(message: String, reason: String) {
        for (accumulator in streamAccumulators.values.toList()) {
            discardStream(accumulator, message, reason, showFailure = false)
        }
        _ui.update { it.copy(notice = message) }
    }

    override fun onFailure(message: String) {
        val scopeAtCallback = activeScope
        viewModelScope.launch(eventDispatcher) {
            if (scopeAtCallback.isBlank() || scopeAtCallback != activeScope) return@launch
            clearStreams(null, "failure", showFailure = true)
            _ui.update { it.copy(notice = message.take(180)) }
        }
    }

    private fun handleEvent(event: JSONObject, scope: String) {
        if (scope != activeScope) return
        when (event.optString("type")) {
            "accepted_ack" -> {
                val clientId = event.optString("client_msg_id")
                if (clientId.isNotBlank()) {
                    messageStore.accepted(scope, event)
                    flushReceiveReceipts(scope)
                    recordAccepted(event)
                    updateMessage(clientId) { it.copy(msgId = event.optString("msg_id"), seq = event.optLong("msg_seq", it.seq), state = if (it.state == DeliveryState.DELIVERED) DeliveryState.DELIVERED else DeliveryState.ACCEPTED) }
                }
            }
            "delivered_ack" -> {
                val clientId = event.optString("client_msg_id")
                if (clientId.isNotBlank()) {
                    messageStore.setState(scope, clientId, DeliveryState.DELIVERED)
                    timingsByClientId[clientId]?.let { if (!it.agent) completeTiming(it, "delivered") }
                    updateMessage(clientId) { it.copy(state = DeliveryState.DELIVERED) }
                }
            }
            "hermes_delta", "ai_delta" -> handleDelta(event)
            "received_ack_ok" -> {
                val sequences = event.optJSONArray("received_seqs") ?: JSONArray()
                messageStore.confirmReceipts(scope, event.optString("session_id"), event.optLong("msg_seq"),
                    (0 until sequences.length()).map { sequences.optLong(it) }.filter { it > 0 })
            }
            "error" -> {
                val clientId = event.optString("client_msg_id")
                val requestId = event.optString("request_id")
                if (requestId.isNotBlank()) discardStreamById(requestId, "Hermes 请求失败，请重试", "error")
                if (clientId.isNotBlank()) {
                    val code = event.optInt("code", 0)
                    if (code != 429 && code != 408 && code < 500 && code != 0) {
                        messageStore.setState(scope, clientId, DeliveryState.FAILED)
                        completeTimingForClient(clientId, "error")
                        updateMessage(clientId) { it.copy(state = DeliveryState.FAILED) }
                    }
                }
                _ui.update { it.copy(error = event.optString("message", "服务端发送失败")) }
            }
            "sync_end", "ack" -> Unit
            else -> {
                // Invisible control records still occupy a durable sequence and must be receipted.
                val durable = durableWire(event, event.optString("session_id").ifBlank { null })
                if (durable != null) {
                    messageStore.save(scope, listOf(durable), scope.substringAfterLast('|').toLongOrNull())
                    flushReceiveReceipts(scope)
                }
                if (scope != activeScope) return
                parseMessage(event, event.optString("session_id").ifBlank { null })?.let { incoming ->
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
    }

    private fun handleDelta(event: JSONObject) {
        val selected = _ui.value.selected ?: return
        val sessionId = event.optString("session_id")
        if (sessionId.isNotBlank() && sessionId != selected.sessionId) return
        val requestId = event.optString("request_id").trim()
        if (terminalStreamRequests.contains(requestId)) return
        val delta = event.optString("delta")
        if (requestId.isBlank() || delta.isEmpty()) return
        val isProgress = event.optBoolean("progress", false)
        val envelope = parseDeltaEnvelope(event)
        val decision = AgentDeltaValidator.decide(
            requestId = requestId,
            sessionId = selected.sessionId,
            delta = delta,
            progress = isProgress,
            deltaIndex = event.optInt("delta_index", -1),
            event = envelope
        )
        if (decision is DeltaDecision.Drop) return
        val now = nowNanos()
        val timing = timingForRequest(requestId, selected.sessionId)
        timing.stream = true
        if (timing.agentStartAtNanos == null) timing.agentStartAtNanos = now
        if (!isProgress && timing.firstDeltaAtNanos == null) timing.firstDeltaAtNanos = now
        val existing = streamAccumulators[requestId]
        val isNew = existing == null
        if (isNew && streamAccumulators.size >= MAX_STREAM_ACCUMULATORS) {
            val oldest = streamAccumulators.entries.firstOrNull()?.value
            if (oldest != null) discardStream(oldest, "流式预览数量超限，已改为等待最终消息", "stream_cap", showFailure = false)
        }
        val active = existing ?: StreamAccumulator(
            requestId = requestId,
            sessionId = selected.sessionId,
            senderId = event.optLong("from_user_id", selected.peerId).takeIf { it > 0L } ?: selected.peerId
        ).also {
            it.lastActivityAtNanos = now
            streamAccumulators[requestId] = it
        }
        if (envelope?.eventId != null && !active.seenEventIds.addIfNew(envelope.eventId)) return
        active.lastActivityAtNanos = now
        val accepted = acceptDelta(active, decision, StreamDelta(delta, isProgress))
        if (accepted) {
            timing.chunkCount += 1
            timing.deltaBytes += utf8Bytes(delta)
            timing.deltaChars += unicodeChars(delta)
        }
        ensureStreamFlushLoop()
        if (isNew && streamAccumulators.containsKey(requestId)) flushAccumulator(active, now, force = true)
    }

    private fun parseDeltaEnvelope(event: JSONObject): AgentDeltaEnvelope? {
        val raw = event.optJSONObject("agent_event") ?: return null
        val envelope = raw.optJSONObject("envelope")
        val data = raw.optJSONObject("data")
        fun optionalString(obj: JSONObject?, key: String): String? {
            if (obj == null || !obj.has(key) || obj.isNull(key)) return null
            return obj.optString(key).takeIf { it.isNotBlank() }
        }
        return AgentDeltaEnvelope(
            schema = optionalString(raw, "schema"),
            type = optionalString(raw, "type"),
            dataText = optionalString(data, "text"),
            envelopeSchema = optionalString(envelope, "schema"),
            requestId = optionalString(envelope, "request_id"),
            conversationId = optionalString(envelope, "conversation_id"),
            sequence = if (envelope != null && envelope.has("sequence")) envelope.optInt("sequence") else null,
            terminal = if (envelope != null && envelope.has("terminal")) envelope.optBoolean("terminal") else null,
            eventId = optionalString(envelope, "event_id")
        )
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
            val idx = state.messages.indexOfFirst { sameMessage(it, message) || (streamKey.isNotEmpty() && it.clientMsgId == streamKey) }
            val delivered = message.copy(state = if (idx >= 0 && state.messages[idx].state == DeliveryState.DELIVERED) DeliveryState.DELIVERED else message.state, streaming = false)
            if (idx >= 0) {
                state.copy(messages = state.messages.toMutableList().also { it[idx] = delivered }, hasHistory = state.hasHistory || message.seq > 0L)
            } else {
                state.copy(messages = state.messages + delivered, hasHistory = state.hasHistory || message.seq > 0L)
            }
        }
        if (requestId != null) {
            markTerminalStream(requestId)
            streamAccumulators.remove(requestId)
            timingsByRequestId[requestId]?.let { timing ->
                if (message.seq > 0 && timing.historyReadableAtNanos == null) {
                    timing.historyReadableAtNanos = now
                }
            }
            completeTimingForRequest(requestId, "completed")
        }
        realtimeBacklog.set(false)
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
        val attachments = parseAttachments(content?.optJSONArray("attachments") ?: outer?.optJSONArray("attachments"))
        if (displayText.isBlank() && card == null && citations.isEmpty() && attachments.isEmpty()) return null
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
            state = if (sender == _ui.value.auth?.userId) DeliveryState.ACCEPTED else DeliveryState.DELIVERED,
            agentCard = card,
            citations = citations,
            attachments = attachments
        )
    }

    private fun parseAttachments(array: JSONArray?): List<ImageAttachment> {
        if (array == null) return emptyList()
        return buildList {
            for (i in 0 until minOf(array.length(), 4)) {
                val item = array.optJSONObject(i) ?: continue
                val id = item.optString("id")
                if (id.isBlank()) continue
                add(ImageAttachment(id, item.optString("name"), item.optString("mime"), item.optInt("bytes")))
            }
        }
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

    private fun sameMessage(a: ChatMessage, b: ChatMessage): Boolean = a.sessionId == b.sessionId && (
        (a.msgId.isNotBlank() && a.msgId == b.msgId) ||
            (a.clientMsgId.isNotBlank() && a.clientMsgId == b.clientMsgId) ||
            (a.seq > 0 && a.seq == b.seq)
        )

    private fun canonicalWire(original: JSONObject, message: ChatMessage): JSONObject = JSONObject(original.toString())
        .put("session_id", message.sessionId).put("msg_id", message.msgId)
        .put("client_msg_id", message.clientMsgId).put("msg_seq", message.seq)
        .put("sender_id", message.senderId).put("timestamp_ms", message.timestampMs)

    private fun durableWire(original: JSONObject, sessionOverride: String?): JSONObject? {
        if (original.optString("type") in listOf("ai_delta", "hermes_delta")) return null
        parseMessage(original, sessionOverride)?.let { if (it.seq > 0) return canonicalWire(original, it) }
        val raw = original.opt("content")
        val outer = when (raw) {
            is JSONObject -> raw
            is String -> try { JSONObject(raw) } catch (_: Exception) { null }
            else -> null
        }
        val seq = original.optLong("msg_seq").takeIf { it > 0 } ?: outer?.optLong("msg_seq") ?: 0L
        val sid = sessionOverride?.takeIf { it.isNotBlank() } ?: original.optString("session_id")
        if (seq <= 0 || sid.isBlank()) return null
        return JSONObject(original.toString()).put("session_id", sid).put("msg_seq", seq)
            .put("msg_id", original.optString("msg_id").ifBlank { outer?.optString("msg_id").orEmpty() })
            .put("client_msg_id", original.optString("client_msg_id").ifBlank { outer?.optString("client_msg_id").orEmpty() })
            .put("sender_id", original.optLong("sender_id", original.optLong("from_user_id"))
                .takeIf { it > 0 } ?: outer?.optLong("from_user_id") ?: 0L)
    }

    fun logout() { adminLogout(); clearStoredAuth() }
    fun reconnect() { _ui.value.auth?.let { connectSocket(it) } }

    override fun onCleared() {
        val cleanupJobs = viewModelScope.coroutineContext[Job]?.children?.toList().orEmpty()
        eventQueue.close()
        eventJob?.cancel()
        streamFlushJob?.cancel()
        outboxJob?.cancel()
        streamAccumulators.clear()
        timingsByClientId.clear()
        timingsByRequestId.clear()
        terminalStreamRequests.clear()
        client.close()
        // Join canceled workers before closing SQLite; HTTP cancellation may still be unwinding.
        CoroutineScope(Dispatchers.IO).launch {
            cleanupJobs.forEach { it.join() }
            messageStore.close()
        }
        super.onCleared()
    }
}
