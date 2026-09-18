package com.peco.sparkim

import androidx.compose.runtime.Immutable

import androidx.compose.ui.graphics.Color

@Immutable
data class Endpoint(val input: String, val httpBase: String, val logicBase: String, val wsUrl: String) {
    companion object {
        fun parse(raw: String): Endpoint? {
            var value = raw.trim()
            if (value.isEmpty()) return null
            if (!value.contains("://")) value = "http://$value"
            return try {
                val uri = java.net.URI(value)
                val host = uri.host ?: return null
                val secure = uri.scheme.equals("https", true)
                val scheme = if (secure) "https" else "http"
                val wsScheme = if (secure) "wss" else "ws"
                Endpoint(
                    input = "$scheme://$host",
                    httpBase = "$scheme://$host",
                    logicBase = "$scheme://$host:9101",
                    wsUrl = "$wsScheme://$host:9000/ws"
                )
            } catch (_: Exception) { null }
        }
    }
}

@Immutable
data class AuthSession(val userId: Long, val token: String, val name: String)

enum class ConnectionState { DISCONNECTED, CONNECTING, CONNECTED, RECONNECTING }

enum class Screen { SETUP, AUTH, HOME, CHAT, KNOWLEDGE, ADMIN }

enum class AdminSection { LOGIN, OVERVIEW, USERS, AUDIT, OPERATIONS }

@Immutable
data class Conversation(
    val sessionId: String,
    val peerId: Long,
    val title: String,
    val subtitle: String,
    val accent: Color,
    val agent: Boolean = false
)

enum class DeliveryState { SENDING, ACCEPTED, DELIVERED, FAILED }

@Immutable
data class AgentAction(
    val id: String,
    val label: String,
    val value: String? = null,
    val provider: String? = null,
    val modelId: String? = null
)

@Immutable
data class AgentModel(
    val provider: String,
    val id: String,
    val name: String,
    val reasoning: Boolean = false
)

@Immutable
data class AgentProvider(
    val id: String,
    val name: String,
    val modelCount: Int,
    val current: Boolean = false
)

@Immutable
data class AgentCard(
    val title: String,
    val kind: String,
    val summary: String = "",
    val actions: List<AgentAction> = emptyList(),
    val artifactName: String? = null,
    val currentModel: AgentModel? = null,
    val models: List<AgentModel> = emptyList(),
    val providers: List<AgentProvider> = emptyList(),
    val selectedProvider: String? = null,
    val truncated: Boolean = false
)

@Immutable
data class Citation(
    val citationId: String,
    val docId: String,
    val chunkId: String,
    val title: String,
    val authority: String = "",
    val locator: String = ""
)

@Immutable
data class ChatMessage(
    val msgId: String = "",
    val clientMsgId: String = "",
    val sessionId: String,
    val seq: Long = 0,
    val senderId: Long,
    val timestampMs: Long = 0,
    val text: String,
    val markdown: Boolean = false,
    val state: DeliveryState? = null,
    val streaming: Boolean = false,
    val progress: String? = null,
    val agentCard: AgentCard? = null,
    val citations: List<Citation> = emptyList()
)

@Immutable
data class KnowledgeDocument(
    val docId: String,
    val title: String,
    val sourceId: String = "",
    val authority: String = "",
    val revision: String = "",
    val generation: String = "",
    val locator: String = "",
    val content: String = "",
    val chunkId: String = ""
)

@Immutable
data class AdminUser(
    val userId: Long,
    val account: String,
    val name: String,
    val status: String,
    val createdAt: String = "",
    val deletedAt: String = ""
)

@Immutable
data class AdminRoom(val roomId: Long, val name: String, val ownerId: Long)

@Immutable
data class AuditLog(
    val id: Long,
    val action: String,
    val actorId: Long,
    val targetId: Long,
    val reason: String,
    val metadata: String,
    val createdAt: String
)

@Immutable
data class SparkUiState(
    val screen: Screen = Screen.SETUP,
    val serverInput: String = "http://100.89.19.125",
    val account: String = "",
    val password: String = "",
    val nickname: String = "",
    val registerMode: Boolean = false,
    val auth: AuthSession? = null,
    val connection: ConnectionState = ConnectionState.DISCONNECTED,
    val conversations: List<Conversation> = emptyList(),
    val selected: Conversation? = null,
    val messages: List<ChatMessage> = emptyList(),
    // Maintained by the ViewModel so ChatScreen does not scan the full
    // timeline on every streaming frame.
    val hasHistory: Boolean = false,
    val draft: String = "",
    val loading: Boolean = false,
    val loadingMore: Boolean = false,
    val error: String? = null,
    val notice: String? = null,
    val showNewConversation: Boolean = false,
    val newPeerInput: String = "",
    val unreadCount: Long = 0,
    val knowledgeDocId: String = "",
    val knowledgeChunkId: String = "",
    val knowledge: KnowledgeDocument? = null,
    val knowledgeLoading: Boolean = false,
    val knowledgeError: String? = null,
    val adminToken: String? = null,
    val adminUserId: Long = 0,
    val adminSection: AdminSection = AdminSection.LOGIN,
    val adminAccount: String = "",
    val adminPassword: String = "",
    val adminUsers: List<AdminUser> = emptyList(),
    val adminRooms: List<AdminRoom> = emptyList(),
    val adminAudit: List<AuditLog> = emptyList(),
    val adminSearch: String = "",
    val adminRoomName: String = "",
    val adminRoomOwner: String = "",
    val adminBroadcastScope: String = "all",
    val adminBroadcastRoom: String = "",
    val adminBroadcastText: String = "",
    val adminBusy: Boolean = false,
    val adminError: String? = null,
    val adminNotice: String? = null,
    val restoreRetryAvailable: Boolean = false
)

// Android uses dedicated Agent contacts. The PC/WebDemo/Telegram channel
// keeps the legacy IDs below, so its transcript and model state remain
// independent from this app.
const val DESKTOP_PI_ID = 900000000001L
const val DESKTOP_HERMES_ID = 900000000101L
const val ANDROID_PI_ID = 900000000201L
const val ANDROID_HERMES_ID = 900000000211L

fun isDesktopAgent(peerId: Long): Boolean = peerId == DESKTOP_PI_ID || peerId == DESKTOP_HERMES_ID

fun singleSessionId(a: Long, b: Long): String = "s_${minOf(a, b)}_${maxOf(a, b)}"

fun knownConversation(userId: Long, peerId: Long): Conversation = when (peerId) {
    ANDROID_PI_ID -> Conversation(singleSessionId(userId, peerId), peerId, "Pi Agent · Android", "CANN / 通用 Agent · 独立会话", Color(0xFF159A9C), true)
    ANDROID_HERMES_ID -> Conversation(singleSessionId(userId, peerId), peerId, "Hermes · Android", "technical · 独立模型与记忆", Color(0xFF8064A2), true)
    else -> Conversation(singleSessionId(userId, peerId), peerId, "用户 $peerId", "Spark Push 单聊", Color(0xFF718096))
}
