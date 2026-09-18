package com.peco.sparkim

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.net.URLEncoder
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private const val MAX_WS_TEXT_BYTES = 16 * 1024 * 1024

class SparkClient {
    interface Listener {
        fun onState(state: ConnectionState)
        fun onEvent(raw: String)
        fun onFailure(message: String)
    }

    private val http = OkHttpClient.Builder()
        .connectTimeout(8, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .pingInterval(25, TimeUnit.SECONDS)
        .build()
    private var endpoint: Endpoint? = null
    private var socket: WebSocket? = null
    private val connectionGeneration = AtomicLong(0L)
    private val jsonType = "application/json; charset=utf-8".toMediaType()

    fun configure(value: String): Boolean {
        val parsed = Endpoint.parse(value) ?: return false
        endpoint = parsed
        return true
    }

    fun currentEndpoint(): Endpoint? = endpoint

    suspend fun post(path: String, body: JSONObject, token: String? = null): JSONObject = withContext(Dispatchers.IO) {
        val base = endpoint ?: throw IllegalStateException("服务器地址未设置")
        val requestBuilder = Request.Builder()
            .url(base.logicBase + path)
            .post(body.toString().toRequestBody(jsonType))
            .header("Accept", "application/json")
        if (!token.isNullOrBlank()) requestBuilder.header("Authorization", "Bearer $token")
        http.newCall(requestBuilder.build()).execute().use { response ->
            val raw = response.body.string()
            if (raw.isBlank()) throw IllegalStateException("服务端返回空响应（HTTP ${response.code}）")
            try { JSONObject(raw) } catch (e: Exception) {
                throw IllegalStateException("服务端返回格式错误（HTTP ${response.code}）")
            }
        }
    }

    fun connect(token: String, listener: Listener) {
        val base = endpoint ?: run {
            listener.onFailure("服务器地址未设置")
            return
        }
        // OkHttp callbacks can arrive after close() and after a replacement
        // socket has already opened. Give every socket a generation and ignore
        // stale callbacks so a reconnect cannot roll the UI back to RECONNECTING.
        val generation = connectionGeneration.incrementAndGet()
        socket?.cancel()
        socket = null
        listener.onState(ConnectionState.CONNECTING)
        val encoded = URLEncoder.encode(token, Charsets.UTF_8.name())
        val request = Request.Builder().url(base.wsUrl + "?token=" + encoded).build()
        socket = http.newWebSocket(request, object : WebSocketListener() {
            private fun isCurrent(webSocket: WebSocket): Boolean =
                connectionGeneration.get() == generation && socket === webSocket

            override fun onOpen(webSocket: WebSocket, response: Response) {
                if (isCurrent(webSocket)) listener.onState(ConnectionState.CONNECTED)
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                if (!isCurrent(webSocket)) return
                if (text.toByteArray(Charsets.UTF_8).size > MAX_WS_TEXT_BYTES) {
                    listener.onFailure("实时消息超过客户端安全大小限制")
                    webSocket.cancel()
                    return
                }
                listener.onEvent(text)
            }

            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                if (isCurrent(webSocket)) listener.onState(ConnectionState.RECONNECTING)
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                if (isCurrent(webSocket)) listener.onState(ConnectionState.RECONNECTING)
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                if (!isCurrent(webSocket)) return
                listener.onState(ConnectionState.RECONNECTING)
                listener.onFailure(t.message ?: "WebSocket 连接失败")
            }
        })
    }

    fun send(payload: JSONObject): Boolean = socket?.send(payload.toString()) == true

    fun sync(sessionId: String, afterSeq: Long) {
        send(JSONObject().put("type", "sync").put("session_id", sessionId).put("after_seq", afterSeq).put("limit", 100))
    }

    fun close() {
        connectionGeneration.incrementAndGet()
        socket?.cancel()
        socket = null
    }
}
