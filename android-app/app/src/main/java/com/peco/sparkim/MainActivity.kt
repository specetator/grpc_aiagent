package com.peco.sparkim

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.BackHandler
import androidx.activity.compose.setContent
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.imePadding
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyListState
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.runtime.snapshotFlow
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import kotlinx.coroutines.flow.distinctUntilChanged

private val SparkCanvas = Color(0xFFF7F6F2)
private val SparkInk = Color(0xFF29262B)
private val SparkMuted = Color(0xFF77727A)
private val SparkLine = Color(0xFFE6E1E4)
private val SparkLavender = Color(0xFFECE6F4)
private val SparkPurple = Color(0xFF6D5A91)
private val SparkTeal = Color(0xFF159A9C)

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent { SparkRoot() }
    }
}

@Composable
private fun SparkRoot(vm: SparkViewModel = viewModel()) {
    val state by vm.ui.collectAsStateWithLifecycle()
    val colors = remember {
        lightColorScheme(
            background = SparkCanvas,
            surface = Color.White,
            surfaceVariant = Color(0xFFF0ECEF),
            primary = SparkPurple,
            onPrimary = Color.White,
            onBackground = SparkInk,
            onSurface = SparkInk,
            onSurfaceVariant = SparkMuted
        )
    }
    MaterialTheme(colorScheme = colors) {
        Box(Modifier.fillMaxSize().background(SparkCanvas)) {
            // Android 15 target 默认启用 edge-to-edge；把内容显式放到状态栏下方，
            // 避免标题和返回按钮被系统栏覆盖。
            Surface(Modifier.fillMaxSize().statusBarsPadding(), color = SparkCanvas) {
                when (state.screen) {
                    Screen.SETUP -> SetupScreen(state, vm)
                    Screen.AUTH -> AuthScreen(state, vm)
                    Screen.HOME -> HomeScreen(state, vm)
                    Screen.CHAT -> ChatScreen(state, vm)
                    Screen.KNOWLEDGE -> KnowledgeScreen(state, vm)
                    Screen.ADMIN -> AdminScreen(state, vm)
                }
            }
        }
    }
    BackHandler(enabled = state.screen == Screen.CHAT || state.screen == Screen.KNOWLEDGE || state.screen == Screen.ADMIN) {
        vm.goHome()
    }
}

@Composable
private fun SetupScreen(state: SparkUiState, vm: SparkViewModel) {
    Column(Modifier.fillMaxSize().padding(28.dp), verticalArrangement = Arrangement.Center) {
        Text("Spark IM", fontSize = 34.sp, fontWeight = FontWeight.SemiBold, color = Color(0xFF30283B))
        Text(
            "Hermes technical · CANN Pi Agent · Android 独立通道",
            color = Color(0xFF777477),
            modifier = Modifier.padding(top = 8.dp, bottom = 28.dp)
        )
        Card(colors = CardDefaults.cardColors(containerColor = Color.White), shape = RoundedCornerShape(24.dp)) {
            Column(Modifier.padding(22.dp)) {
                Text("连接本机 Spark Push", fontSize = 19.sp, fontWeight = FontWeight.Medium)
                Text(
                    "客户端会自动使用 9101 API 和 9000 WebSocket。",
                    fontSize = 13.sp,
                    color = Color(0xFF777477),
                    modifier = Modifier.padding(top = 8.dp, bottom = 16.dp)
                )
                OutlinedTextField(
                    value = state.serverInput,
                    onValueChange = vm::setServer,
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                    label = { Text("服务器地址") },
                    placeholder = { Text("http://100.89.19.125") }
                )
                Spacer(Modifier.height(16.dp))
                Button(onClick = vm::continueToAuth, modifier = Modifier.fillMaxWidth(), shape = RoundedCornerShape(14.dp)) {
                    Text("继续")
                }
            }
        }
        ErrorText(state.error)
    }
}

@Composable
private fun AuthScreen(state: SparkUiState, vm: SparkViewModel) {
    Column(Modifier.fillMaxSize().padding(28.dp), verticalArrangement = Arrangement.Center) {
        Text("欢迎回来", fontSize = 32.sp, fontWeight = FontWeight.SemiBold, color = Color(0xFF30283B))
        Text(
            state.serverInput,
            fontSize = 12.sp,
            color = Color(0xFF159A9C),
            modifier = Modifier.padding(top = 6.dp, bottom = 22.dp),
            maxLines = 1,
            overflow = TextOverflow.Ellipsis
        )
        Card(colors = CardDefaults.cardColors(containerColor = Color.White), shape = RoundedCornerShape(24.dp)) {
            Column(Modifier.padding(22.dp)) {
                Text(if (state.registerMode) "创建 Spark 账号" else "登录 Spark IM", fontSize = 19.sp, fontWeight = FontWeight.Medium)
                OutlinedTextField(
                    value = state.account,
                    onValueChange = vm::setAccount,
                    modifier = Modifier.fillMaxWidth().padding(top = 16.dp),
                    singleLine = true,
                    label = { Text("账号") }
                )
                OutlinedTextField(
                    value = state.password,
                    onValueChange = vm::setPassword,
                    modifier = Modifier.fillMaxWidth().padding(top = 10.dp),
                    singleLine = true,
                    label = { Text("密码") },
                    visualTransformation = PasswordVisualTransformation()
                )
                if (state.registerMode) {
                    OutlinedTextField(
                        value = state.nickname,
                        onValueChange = vm::setNickname,
                        modifier = Modifier.fillMaxWidth().padding(top = 10.dp),
                        singleLine = true,
                        label = { Text("昵称（可选）") }
                    )
                }
                Spacer(Modifier.height(16.dp))
                Button(
                    onClick = vm::authenticate,
                    enabled = !state.loading,
                    modifier = Modifier.fillMaxWidth(),
                    shape = RoundedCornerShape(14.dp)
                ) {
                    if (state.loading) CircularProgressIndicator(Modifier.size(18.dp), strokeWidth = 2.dp)
                    else Text(if (state.registerMode) "注册并登录" else "登录")
                }
                TextButton(onClick = vm::toggleRegister, modifier = Modifier.align(Alignment.CenterHorizontally)) {
                    Text(if (state.registerMode) "已有账号，返回登录" else "没有账号？注册")
                }
            }
        }
        ErrorText(state.error)
        state.notice?.let { Text(it, color = Color(0xFF777477), fontSize = 12.sp, modifier = Modifier.padding(top = 8.dp)) }
    }
}

@Composable
private fun HomeScreen(state: SparkUiState, vm: SparkViewModel) {
    Column(Modifier.fillMaxSize()) {
        SparkBrandBar(state, vm)
        HorizontalDivider(color = SparkLine)
        LazyColumn(Modifier.fillMaxSize(), contentPadding = PaddingValues(horizontal = 18.dp, vertical = 18.dp)) {
            item { SectionLabel("工作台", "Android 专用 Agent 会话；PC / WebDemo 状态彼此隔离") }
            item { HomeActionRow("＋", "新建 UserID 会话", "输入一个数字 UserID，打开 Spark Push 单聊") { vm.showNewConversation() } }
            item {
                Row(Modifier.fillMaxWidth().padding(top = 10.dp), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    ToolAction("知识原文", "CANN", Modifier.weight(1f)) { vm.openKnowledge() }
                    ToolAction("管理控制台", "Admin", Modifier.weight(1f)) { vm.openAdmin() }
                }
            }
            item { SectionLabel("助手与会话", "最近使用的目标") }
            items(state.conversations, key = { it.sessionId }) { conversation ->
                ConversationRow(conversation) { vm.openConversation(conversation) }
            }
            if (!state.notice.isNullOrBlank()) item { NoticeStrip(state.notice.orEmpty()) }
            item {
                Row(Modifier.fillMaxWidth().padding(top = 18.dp), verticalAlignment = Alignment.CenterVertically) {
                    Column(Modifier.weight(1f)) {
                        Text(state.auth?.name ?: "未登录", fontSize = 13.sp, fontWeight = FontWeight.Medium)
                        Text("UserID ${state.auth?.userId ?: "-"}", fontSize = 11.sp, color = SparkMuted)
                    }
                    Text("退出登录", fontSize = 12.sp, color = SparkMuted, modifier = Modifier.clickable { vm.logout() }.padding(8.dp))
                }
            }
        }
    }
    if (state.showNewConversation) NewConversationDialog(state, vm)
}

@Composable
private fun ConversationRow(conversation: Conversation, onClick: () -> Unit) {
    Column(Modifier.fillMaxWidth().clickable(onClick = onClick)) {
        Row(Modifier.padding(vertical = 12.dp), verticalAlignment = Alignment.CenterVertically) {
            Box(Modifier.size(38.dp).background(conversation.accent, RoundedCornerShape(12.dp)), contentAlignment = Alignment.Center) {
                Text(if (conversation.agent) "✦" else conversation.peerId.toString().takeLast(3), color = Color.White, fontSize = 13.sp, fontWeight = FontWeight.SemiBold)
            }
            Column(Modifier.padding(start = 12.dp).weight(1f)) {
                Text(conversation.title, fontSize = 15.sp, fontWeight = FontWeight.Medium, color = SparkInk)
                Text(conversation.subtitle, fontSize = 12.sp, color = SparkMuted, maxLines = 1, overflow = TextOverflow.Ellipsis, modifier = Modifier.padding(top = 2.dp))
            }
            Text("›", fontSize = 24.sp, color = Color(0xFFAAA4AC), modifier = Modifier.padding(start = 8.dp))
        }
        HorizontalDivider(color = SparkLine)
    }
}

@Composable
private fun ChatScreen(state: SparkUiState, vm: SparkViewModel) {
    val selected = state.selected ?: return
    val listState = rememberLazyListState()
    val messages = state.messages
    val hasHistory = state.hasHistory
    var followTail by remember(selected.sessionId) { mutableStateOf(true) }
    val lastMessage = messages.lastOrNull()

    // Keep the user's reading position. A stream only follows the tail when
    // the last item is currently visible or very close to the viewport bottom.
    LaunchedEffect(selected.sessionId) {
        snapshotFlow { isNearBottom(listState) }
            .distinctUntilChanged()
            .collect { followTail = it }
    }
    // The key contains only cheap tail metadata. It avoids hashing/copying the
    // complete answer while still following appended stream text at 25 FPS.
    LaunchedEffect(
        selected.sessionId,
        messages.size,
        hasHistory,
        lastMessage?.clientMsgId,
        lastMessage?.msgId,
        lastMessage?.text?.length,
        lastMessage?.progress?.length,
        lastMessage?.streaming
    ) {
        if (followTail && messages.isNotEmpty()) {
            val tailIndex = messages.lastIndex + if (hasHistory) 1 else 0
            listState.scrollToItem(tailIndex)
        }
    }
    var draft by remember(selected.sessionId) { mutableStateOf("") }
    Column(Modifier.fillMaxSize()) {
        ChatTopBar(selected, state.connection, state.unreadCount, vm)
        HorizontalDivider(color = SparkLine)
        Box(Modifier.weight(1f).fillMaxWidth()) {
            LazyColumn(
                state = listState,
                modifier = Modifier.fillMaxSize(),
                contentPadding = PaddingValues(start = 18.dp, top = 16.dp, end = 18.dp, bottom = 180.dp)
            ) {
                if (hasHistory) item(key = "history-loader", contentType = "history") {
                    CompactTextAction("加载更早消息", vm::loadMoreHistory, Modifier.fillMaxWidth().padding(bottom = 8.dp))
                }
                if (messages.isEmpty() && !state.loading) item(key = "empty-chat", contentType = "empty") { EmptyChatState(selected.title) }
                items(
                    items = messages,
                    key = { message -> messageKeyForUi(message) },
                    contentType = { message ->
                        when {
                            message.streaming -> "streaming"
                            message.agentCard != null -> "agent-card"
                            else -> "message"
                        }
                    }
                ) { message ->
                    MessageRow(message, state.auth?.userId == message.senderId, selected.accent, vm)
                }
            }
            if (state.loading) CircularProgressIndicator(Modifier.size(24.dp).align(Alignment.Center), strokeWidth = 2.dp, color = SparkPurple)
        }
        QuickCommands(selected, vm)
        Row(Modifier.fillMaxWidth().imePadding().navigationBarsPadding().padding(horizontal = 14.dp, vertical = 10.dp), verticalAlignment = Alignment.Bottom) {
            OutlinedTextField(
                value = draft,
                onValueChange = { draft = it },
                modifier = Modifier.weight(1f).heightIn(min = 54.dp, max = 118.dp),
                placeholder = { Text(if (selected.agent) "输入问题，或使用 /cann、/kb…" else "输入消息…") },
                shape = RoundedCornerShape(18.dp),
                minLines = 1,
                maxLines = 4
            )
            Spacer(Modifier.width(8.dp))
            SparkSendButton(
                enabled = draft.isNotBlank(),
                onClick = {
                    val outgoing = draft.trim()
                    if (outgoing.isNotEmpty() && vm.sendMessage(outgoing)) draft = ""
                }
            )
        }
        state.error?.let { ErrorText(it) }
    }
}

@Composable
private fun QuickCommands(conversation: Conversation, vm: SparkViewModel) {
    if (!conversation.agent) return
    val commands = listOf("/cann", "/kb", "/status", "/agent", "/model", "/reasoning", "/help")
    LazyRow(Modifier.fillMaxWidth().padding(top = 6.dp), horizontalArrangement = Arrangement.spacedBy(7.dp), contentPadding = PaddingValues(horizontal = 16.dp)) {
        items(commands, key = { it }) { command ->
            Surface(
                color = SparkCanvas,
                shape = RoundedCornerShape(12.dp),
                border = BorderStroke(1.dp, Color(0xFFD9D2DE)),
                modifier = Modifier.height(34.dp).clickable { vm.sendMessage(command) }
            ) {
                Box(Modifier.padding(horizontal = 12.dp), contentAlignment = Alignment.Center) {
                    Text(command, fontSize = 12.sp, color = Color(0xFF5F5368))
                }
            }
        }
    }
}

@Composable
private fun MessageRow(message: ChatMessage, mine: Boolean, accent: Color, vm: SparkViewModel) {
    Row(Modifier.fillMaxWidth().padding(vertical = 5.dp), horizontalArrangement = if (mine) Arrangement.End else Arrangement.Start) {
        if (mine) {
            Surface(color = SparkLavender, shape = RoundedCornerShape(17.dp), modifier = Modifier.widthIn(max = 380.dp)) {
                MessageBody(message, accent, vm, Modifier.padding(horizontal = 15.dp, vertical = 12.dp))
            }
        } else {
            Column(Modifier.widthIn(max = 390.dp)) {
                Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(bottom = 5.dp)) {
                    Box(Modifier.size(24.dp).background(accent, RoundedCornerShape(8.dp)), contentAlignment = Alignment.Center) {
                        Text("✦", color = Color.White, fontSize = 12.sp)
                    }
                    Text("Spark", fontSize = 12.sp, fontWeight = FontWeight.SemiBold, color = Color(0xFF625B66), modifier = Modifier.padding(start = 7.dp))
                }
                MessageBody(message, accent, vm, Modifier.padding(start = 31.dp))
            }
        }
    }
}

@Composable
private fun AgentCardView(card: AgentCard, vm: SparkViewModel) {
    if (card.kind == "model_picker") {
        ModelPickerCard(card, vm)
        return
    }
    Surface(color = Color(0xFFF1EDF5), shape = RoundedCornerShape(15.dp), border = BorderStroke(1.dp, Color(0xFFE0D8E8)), modifier = Modifier.fillMaxWidth().padding(top = 10.dp)) {
        Column(Modifier.padding(13.dp)) {
            Text(card.title, fontWeight = FontWeight.SemiBold, color = Color(0xFF5B477D), fontSize = 14.sp)
            if (card.summary.isNotBlank()) Text(card.summary, fontSize = 12.sp, color = SparkMuted, modifier = Modifier.padding(top = 4.dp), lineHeight = 18.sp)
            if (card.actions.isNotEmpty()) {
                LazyRow(Modifier.padding(top = 9.dp), horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                    items(card.actions.take(16), key = { it.id + ":" + (it.value ?: "") }) { action ->
                        Surface(color = Color.White, shape = RoundedCornerShape(9.dp), border = BorderStroke(1.dp, Color(0xFFD8CEDF)), modifier = Modifier.height(31.dp).clickable { vm.activateAgentAction(action) }) {
                            Box(Modifier.padding(horizontal = 10.dp), contentAlignment = Alignment.Center) { Text(action.label, fontSize = 11.sp, color = Color(0xFF5B477D)) }
                        }
                    }
                }
            }
            card.artifactName?.let { Text("附件：$it", fontSize = 11.sp, color = SparkMuted, modifier = Modifier.padding(top = 6.dp)) }
        }
    }
}

@Composable
private fun ModelPickerCard(card: AgentCard, vm: SparkViewModel) {
    var query by remember { mutableStateOf("") }
    var selectedProvider by remember(card.selectedProvider) { mutableStateOf(card.selectedProvider) }
    var page by remember { mutableStateOf(0) }
    var pending by remember { mutableStateOf<String?>(null) }

    // The server sends a small provider index first. A provider's models are
    // loaded only after the user enters that provider.
    val current = card.currentModel
    val providerEntries = remember(card.providers, card.models, current?.provider) {
        if (card.providers.isNotEmpty()) {
            card.providers.sortedBy { if (it.current || it.id == current?.provider) "" else it.name.lowercase() }
        } else {
            card.models.groupBy { it.provider }.map { (id, models) ->
                AgentProvider(id, id, models.size, id == current?.provider)
            }.sortedBy { if (it.current) "" else it.name.lowercase() }
        }
    }
    val currentProvider = current?.let { model ->
        providerEntries.firstOrNull { it.id == model.provider }
    }
    val currentKey = current?.let { it.provider + ":" + it.id }
    val selectedProviderInfo = selectedProvider?.let { id ->
        providerEntries.firstOrNull { it.id == id }
    }
    val selectedModels = selectedProvider?.let { id ->
        card.models.filter { it.provider == id }
    }.orEmpty()
    val normalizedQuery = query.trim().lowercase()
    val visibleProviders = if (selectedProvider == null) {
        providerEntries.filter {
            normalizedQuery.isBlank() ||
                it.name.lowercase().contains(normalizedQuery) ||
                it.id.lowercase().contains(normalizedQuery)
        }
    } else emptyList()
    val filteredModels = if (selectedProvider == null) {
        emptyList()
    } else {
        selectedModels.filter { model ->
            normalizedQuery.isBlank() ||
                model.name.lowercase().contains(normalizedQuery) ||
                model.id.lowercase().contains(normalizedQuery)
        }
    }
    val pageSize = 12
    val pageCount = maxOf(1, (filteredModels.size + pageSize - 1) / pageSize)
    val safePage = page.coerceIn(0, pageCount - 1)
    val pageModels = filteredModels.drop(safePage * pageSize).take(pageSize)

    LaunchedEffect(card.selectedProvider, card.models, card.providers) {
        selectedProvider = card.selectedProvider
        query = ""
        page = 0
        pending = null
    }

    Surface(
        color = Color(0xFFF1EDF5),
        shape = RoundedCornerShape(15.dp),
        border = BorderStroke(1.dp, Color(0xFFE0D8E8)),
        modifier = Modifier.fillMaxWidth().padding(top = 10.dp)
    ) {
        Column(Modifier.padding(13.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(card.title.ifBlank { "选择模型" }, fontWeight = FontWeight.SemiBold, color = Color(0xFF5B477D), fontSize = 14.sp)
                Spacer(Modifier.width(8.dp))
                Text(
                    providerEntries.size.toString() + " 个 provider · " +
                        providerEntries.sumOf { it.modelCount }.toString() + " 个模型",
                    fontSize = 10.sp,
                    color = SparkMuted,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
            }

            if (current != null) {
                Surface(
                    color = Color.White,
                    shape = RoundedCornerShape(11.dp),
                    border = BorderStroke(1.dp, Color(0xFFDCD2E4)),
                    modifier = Modifier.fillMaxWidth().padding(top = 9.dp)
                ) {
                    Column(Modifier.padding(horizontal = 11.dp, vertical = 9.dp)) {
                        Text("当前配置", fontSize = 10.sp, color = SparkMuted)
                        Text(current.name, fontSize = 13.sp, fontWeight = FontWeight.Medium, color = SparkInk, maxLines = 1, overflow = TextOverflow.Ellipsis, modifier = Modifier.padding(top = 2.dp))
                        Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(top = 2.dp)) {
                            Text(currentProvider?.name ?: current.provider, fontSize = 10.sp, color = SparkPurple, fontWeight = FontWeight.Medium)
                            Text(" · " + current.id, fontSize = 10.sp, color = SparkMuted, maxLines = 1, overflow = TextOverflow.Ellipsis)
                            if (current.reasoning) Text("  推理", fontSize = 10.sp, color = SparkTeal)
                        }
                    }
                }
            }

            if (selectedProvider == null) {
                Text("先选择 provider", fontSize = 11.sp, color = SparkMuted, modifier = Modifier.padding(top = 11.dp))
                OutlinedTextField(
                    value = query,
                    onValueChange = { query = it },
                    singleLine = true,
                    placeholder = { Text("搜索 provider", fontSize = 12.sp) },
                    modifier = Modifier.fillMaxWidth().padding(top = 6.dp)
                )
                if (visibleProviders.isEmpty()) {
                    Text("没有匹配的 provider", fontSize = 12.sp, color = SparkMuted, modifier = Modifier.padding(top = 12.dp, bottom = 4.dp))
                } else {
                    LazyColumn(
                        modifier = Modifier.fillMaxWidth().heightIn(max = 340.dp).padding(top = 6.dp),
                        verticalArrangement = Arrangement.spacedBy(6.dp),
                        contentPadding = PaddingValues(bottom = 4.dp)
                    ) {
                        items(visibleProviders, key = { it.id }) { provider ->
                            ProviderCard(
                                provider = provider,
                                enabled = pending == null,
                                onClick = {
                                    selectedProvider = provider.id
                                    query = ""
                                    page = 0
                                    pending = provider.id
                                    vm.activateAgentAction(
                                        AgentAction("list_provider_models", provider.name, provider = provider.id)
                                    )
                                }
                            )
                        }
                    }
                }
            } else {
                val provider = selectedProviderInfo
                val providerName = provider?.name ?: selectedProvider.orEmpty()
                Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(top = 8.dp)) {
                    TextButton(
                        onClick = {
                            selectedProvider = null
                            query = ""
                            page = 0
                            pending = null
                        },
                        enabled = pending == null,
                        contentPadding = PaddingValues(horizontal = 0.dp, vertical = 0.dp)
                    ) {
                        Text("‹ provider", fontSize = 11.sp)
                    }
                    Text(providerName, fontSize = 13.sp, fontWeight = FontWeight.SemiBold, color = SparkInk, maxLines = 1, overflow = TextOverflow.Ellipsis, modifier = Modifier.padding(start = 8.dp))
                    Spacer(Modifier.weight(1f))
                    Text(selectedModels.size.toString() + " 个模型", fontSize = 10.sp, color = SparkMuted)
                }
                OutlinedTextField(
                    value = query,
                    onValueChange = { query = it },
                    singleLine = true,
                    placeholder = { Text("搜索此 provider 的模型", fontSize = 12.sp) },
                    modifier = Modifier.fillMaxWidth().padding(top = 5.dp)
                )
                if (selectedModels.isEmpty() && pending != null) {
                    Text("正在加载此 provider 的模型…", fontSize = 12.sp, color = SparkTeal, modifier = Modifier.padding(top = 12.dp, bottom = 4.dp))
                } else if (filteredModels.isEmpty()) {
                    Text("没有匹配的模型", fontSize = 12.sp, color = SparkMuted, modifier = Modifier.padding(top = 12.dp, bottom = 4.dp))
                } else {
                    LazyColumn(
                        modifier = Modifier.fillMaxWidth().heightIn(max = 420.dp).padding(top = 6.dp),
                        verticalArrangement = Arrangement.spacedBy(2.dp),
                        contentPadding = PaddingValues(bottom = 4.dp)
                    ) {
                        items(pageModels, key = { it.provider + ":" + it.id }) { model ->
                            val modelKey = model.provider + ":" + model.id
                            val selected = modelKey == currentKey
                            ModelOptionRow(
                                model = model,
                                selected = selected,
                                enabled = !selected && pending == null,
                                onClick = {
                                    pending = modelKey
                                    vm.activateAgentAction(AgentAction("select_model", model.name, provider = model.provider, modelId = model.id))
                                }
                            )
                        }
                    }
                }

                if (filteredModels.size > pageSize) {
                    Row(Modifier.fillMaxWidth().padding(top = 5.dp), verticalAlignment = Alignment.CenterVertically) {
                        TextButton(onClick = { page = (safePage - 1).coerceAtLeast(0) }, enabled = safePage > 0) { Text("‹ 上一页", fontSize = 11.sp) }
                        Text((safePage + 1).toString() + " / " + pageCount.toString(), fontSize = 10.sp, color = SparkMuted, modifier = Modifier.weight(1f), textAlign = TextAlign.Center)
                        TextButton(onClick = { page = (safePage + 1).coerceAtMost(pageCount - 1) }, enabled = safePage < pageCount - 1) { Text("下一页 ›", fontSize = 11.sp) }
                    }
                }
            }

            Row(Modifier.fillMaxWidth().padding(top = 7.dp), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                ModelPickerAction("恢复默认", enabled = pending == null) {
                    pending = "reset"
                    vm.activateAgentAction(AgentAction("reset_model", "恢复默认"))
                }
                ModelPickerAction("刷新列表", enabled = pending == null) {
                    pending = "refresh"
                    vm.activateAgentAction(AgentAction("refresh_models", "刷新列表"))
                }
            }
            if (card.truncated) {
                Text(
                    if (card.providers.isNotEmpty()) "provider 目录已完整加载；模型会在进入 provider 后按需加载。"
                    else "服务端列表已截断；先按 provider 浏览，未显示的模型仍可直接输入 /model provider:model。",
                    fontSize = 10.sp,
                    color = SparkMuted,
                    lineHeight = 14.sp,
                    modifier = Modifier.padding(top = 7.dp)
                )
            }
            if (pending != null) {
                Text("已发送请求，等待 Hermes 确认…", fontSize = 10.sp, color = SparkTeal, modifier = Modifier.padding(top = 6.dp))
            }
        }
    }
}

@Composable
private fun ProviderCard(
    provider: AgentProvider,
    enabled: Boolean,
    onClick: () -> Unit
) {
    val subtitle = provider.modelCount.toString() + " 个模型" +
        if (provider.name != provider.id) " · " + provider.id else ""
    Surface(
        color = if (provider.current) Color(0xFFE8DFF0) else Color.White,
        shape = RoundedCornerShape(11.dp),
        border = BorderStroke(1.dp, if (provider.current) Color(0xFFBFA8D2) else Color(0xFFE3DCE6)),
        modifier = Modifier.fillMaxWidth().clickable(enabled = enabled, onClick = onClick)
    ) {
        Row(Modifier.padding(horizontal = 11.dp, vertical = 10.dp), verticalAlignment = Alignment.CenterVertically) {
            Box(
                Modifier.size(32.dp).background(if (provider.current) SparkPurple else Color(0xFFEDE8F1), RoundedCornerShape(9.dp)),
                contentAlignment = Alignment.Center
            ) {
                Text(provider.name.take(1).uppercase(), color = if (provider.current) Color.White else SparkPurple, fontSize = 12.sp, fontWeight = FontWeight.SemiBold)
            }
            Column(Modifier.padding(start = 10.dp).weight(1f)) {
                Text(provider.name, fontSize = 13.sp, fontWeight = FontWeight.Medium, color = SparkInk, maxLines = 1, overflow = TextOverflow.Ellipsis)
                Text(subtitle, fontSize = 10.sp, color = SparkMuted, modifier = Modifier.padding(top = 2.dp), maxLines = 1, overflow = TextOverflow.Ellipsis)
            }
            if (provider.current) Text("当前", fontSize = 10.sp, color = SparkPurple, modifier = Modifier.padding(end = 8.dp))
            Text("›", fontSize = 22.sp, color = Color(0xFFA69FA8))
        }
    }
}

@Composable
private fun ModelOptionRow(model: AgentModel, selected: Boolean, enabled: Boolean, onClick: () -> Unit) {
    Surface(
        color = if (selected) Color(0xFFE8DFF0) else Color.White,
        shape = RoundedCornerShape(10.dp),
        border = BorderStroke(1.dp, if (selected) Color(0xFFBFA8D2) else Color(0xFFE3DCE6)),
        modifier = Modifier.fillMaxWidth().padding(top = 5.dp).clickable(enabled = enabled, onClick = onClick)
    ) {
        Row(Modifier.padding(horizontal = 10.dp, vertical = 8.dp), verticalAlignment = Alignment.CenterVertically) {
            Column(Modifier.weight(1f)) {
                Text(model.name, fontSize = 12.sp, fontWeight = if (selected) FontWeight.SemiBold else FontWeight.Medium, color = SparkInk, maxLines = 1, overflow = TextOverflow.Ellipsis)
                Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(top = 2.dp)) {
                    Text(model.id, fontSize = 10.sp, color = SparkMuted, maxLines = 1, overflow = TextOverflow.Ellipsis)
                    if (model.reasoning) Text("  推理", fontSize = 10.sp, color = SparkTeal)
                }
            }
            Text(if (selected) "✓ 当前" else "选择", fontSize = 10.sp, color = if (selected) SparkPurple else SparkMuted, modifier = Modifier.padding(start = 8.dp))
        }
    }
}

@Composable
private fun ModelPickerAction(label: String, enabled: Boolean, onClick: () -> Unit) {
    Surface(
        color = if (enabled) Color.White else Color(0xFFE8E3E9),
        shape = RoundedCornerShape(9.dp),
        border = BorderStroke(1.dp, Color(0xFFD8CEDF)),
        modifier = Modifier.height(31.dp).clickable(enabled = enabled, onClick = onClick)
    ) {
        Box(Modifier.padding(horizontal = 11.dp), contentAlignment = Alignment.Center) {
            Text(label, fontSize = 10.sp, color = if (enabled) Color(0xFF5B477D) else SparkMuted)
        }
    }
}

@Composable
private fun SparkBrandBar(state: SparkUiState, vm: SparkViewModel) {
    Row(Modifier.fillMaxWidth().padding(horizontal = 18.dp, vertical = 14.dp), verticalAlignment = Alignment.CenterVertically) {
        SparkMark()
        Column(Modifier.weight(1f).padding(start = 11.dp)) {
            Text("Spark", fontSize = 20.sp, fontWeight = FontWeight.SemiBold, color = SparkInk)
            Text("本机 Agent 工作台", fontSize = 11.sp, color = SparkMuted)
        }
        ConnectionPill(state.connection)
        Text("＋", fontSize = 24.sp, color = SparkPurple, textAlign = TextAlign.Center, modifier = Modifier.clickable { vm.showNewConversation() }.padding(start = 13.dp, top = 5.dp, bottom = 5.dp))
    }
}

@Composable
private fun SparkMark(modifier: Modifier = Modifier) {
    Box(modifier.size(36.dp).background(Color(0xFF39343B), RoundedCornerShape(11.dp)), contentAlignment = Alignment.Center) {
        Text("S", color = Color.White, fontSize = 17.sp, fontWeight = FontWeight.SemiBold)
    }
}

@Composable
private fun SectionLabel(title: String, subtitle: String) {
    Column(Modifier.padding(top = 14.dp, bottom = 9.dp)) {
        Text(title, fontSize = 12.sp, fontWeight = FontWeight.SemiBold, color = Color(0xFF5C565F))
        Text(subtitle, fontSize = 11.sp, color = SparkMuted, modifier = Modifier.padding(top = 2.dp))
    }
}

@Composable
private fun HomeActionRow(symbol: String, title: String, subtitle: String, onClick: () -> Unit) {
    Surface(color = Color.White, shape = RoundedCornerShape(15.dp), border = BorderStroke(1.dp, SparkLine), modifier = Modifier.fillMaxWidth().clickable(onClick = onClick)) {
        Row(Modifier.padding(horizontal = 14.dp, vertical = 13.dp), verticalAlignment = Alignment.CenterVertically) {
            Box(Modifier.size(34.dp).background(Color(0xFFF0EBF4), RoundedCornerShape(11.dp)), contentAlignment = Alignment.Center) {
                Text(symbol, color = SparkPurple, fontSize = 20.sp)
            }
            Column(Modifier.weight(1f).padding(start = 11.dp)) {
                Text(title, fontSize = 14.sp, fontWeight = FontWeight.Medium, color = SparkInk)
                Text(subtitle, fontSize = 11.sp, color = SparkMuted, maxLines = 1, overflow = TextOverflow.Ellipsis, modifier = Modifier.padding(top = 2.dp))
            }
            Text("›", color = Color(0xFFA69FA8), fontSize = 22.sp)
        }
    }
}

@Composable
private fun ToolAction(title: String, badge: String, modifier: Modifier = Modifier, onClick: () -> Unit) {
    Surface(color = Color(0xFFF0EDEF), shape = RoundedCornerShape(13.dp), modifier = modifier.clickable(onClick = onClick)) {
        Row(Modifier.padding(horizontal = 12.dp, vertical = 11.dp), verticalAlignment = Alignment.CenterVertically) {
            Text(title, fontSize = 12.sp, color = SparkInk, modifier = Modifier.weight(1f))
            Text(badge, fontSize = 10.sp, color = SparkMuted)
        }
    }
}

@Composable
private fun NoticeStrip(text: String) {
    Surface(color = Color(0xFFFFF4D8), shape = RoundedCornerShape(11.dp), modifier = Modifier.fillMaxWidth().padding(top = 14.dp)) {
        Text(text, color = Color(0xFF7B5E1A), fontSize = 11.sp, lineHeight = 16.sp, modifier = Modifier.padding(horizontal = 12.dp, vertical = 9.dp))
    }
}

@Composable
private fun ChatTopBar(selected: Conversation, connection: ConnectionState, unreadCount: Long, vm: SparkViewModel) {
    Row(Modifier.fillMaxWidth().padding(horizontal = 10.dp, vertical = 8.dp), verticalAlignment = Alignment.CenterVertically) {
        Box(Modifier.size(38.dp).clickable { vm.goHome() }, contentAlignment = Alignment.Center) {
            Text("‹", fontSize = 30.sp, color = SparkPurple)
        }
        Box(Modifier.size(34.dp).background(selected.accent, RoundedCornerShape(11.dp)), contentAlignment = Alignment.Center) {
            Text("✦", color = Color.White, fontSize = 14.sp)
        }
        Column(Modifier.weight(1f).padding(start = 10.dp)) {
            Text(selected.title, fontSize = 16.sp, fontWeight = FontWeight.SemiBold, color = SparkInk, maxLines = 1, overflow = TextOverflow.Ellipsis)
            Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(top = 1.dp)) {
                Box(Modifier.size(6.dp).background(if (connection == ConnectionState.CONNECTED) SparkTeal else Color(0xFFB48A2A), RoundedCornerShape(3.dp)))
                Text(connectionLabel(connection), fontSize = 10.sp, color = SparkMuted, modifier = Modifier.padding(start = 5.dp))
            }
        }
        Surface(color = Color.Transparent, shape = RoundedCornerShape(10.dp), modifier = Modifier.height(36.dp).clickable { vm.refreshUnread() }) {
            Box(Modifier.padding(horizontal = 8.dp), contentAlignment = Alignment.Center) { Text("未读 $unreadCount", fontSize = 10.sp, color = SparkPurple) }
        }
        Surface(color = Color(0xFFF0ECEF), shape = RoundedCornerShape(10.dp), modifier = Modifier.height(36.dp).clickable { vm.markSelectedRead() }) {
            Box(Modifier.padding(horizontal = 10.dp), contentAlignment = Alignment.Center) {
                Text("✓ 已读", fontSize = 10.sp, color = Color(0xFF5F5862))
            }
        }
    }
}

@Composable
private fun EmptyChatState(title: String) {
    Column(Modifier.fillMaxWidth().height(260.dp), horizontalAlignment = Alignment.CenterHorizontally, verticalArrangement = Arrangement.Center) {
        SparkMark(Modifier.size(42.dp))
        Text("开始新对话", fontSize = 18.sp, fontWeight = FontWeight.SemiBold, color = SparkInk, modifier = Modifier.padding(top = 13.dp))
        Text("向 $title 发送消息，历史记录会通过 Spark Push 同步。", fontSize = 12.sp, color = SparkMuted, textAlign = TextAlign.Center, modifier = Modifier.padding(horizontal = 26.dp, vertical = 6.dp))
    }
}

@Composable
private fun CompactTextAction(label: String, onClick: () -> Unit, modifier: Modifier = Modifier) {
    Text(label, fontSize = 11.sp, color = SparkPurple, textAlign = TextAlign.Center, modifier = modifier.clickable(onClick = onClick).padding(vertical = 6.dp))
}

@Composable
private fun SparkSendButton(enabled: Boolean, onClick: () -> Unit) {
    val background = if (enabled) SparkPurple else Color(0xFFD5CFD8)
    Surface(color = background, shape = RoundedCornerShape(16.dp), modifier = Modifier.width(70.dp).height(54.dp).clickable(enabled = enabled, onClick = onClick)) {
        Box(contentAlignment = Alignment.Center) { Text("发送", fontSize = 14.sp, fontWeight = FontWeight.Medium, color = Color.White) }
    }
}

@Composable
private fun MessageBody(message: ChatMessage, accent: Color, vm: SparkViewModel, modifier: Modifier = Modifier) {
    Column(modifier) {
        if (message.streaming && !message.progress.isNullOrBlank()) {
            Text(message.progress.orEmpty(), color = accent, fontSize = 11.sp, modifier = Modifier.padding(bottom = 6.dp))
        }
        if (message.text.isNotBlank()) Text(message.text, color = SparkInk, fontSize = 15.sp, lineHeight = 22.sp)
        message.agentCard?.let { AgentCardView(it, vm) }
        if (message.citations.isNotEmpty()) {
            Text("CANN 依据", fontWeight = FontWeight.SemiBold, color = Color(0xFF5B477D), fontSize = 11.sp, modifier = Modifier.padding(top = 10.dp, bottom = 3.dp))
            message.citations.forEachIndexed { index, citation ->
                Text(
                    "[${index + 1}] ${citation.title}",
                    fontSize = 11.sp,
                    color = SparkPurple,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                    modifier = Modifier.fillMaxWidth().clickable { vm.openKnowledge(citation.docId, citation.chunkId) }.padding(vertical = 4.dp)
                )
            }
        }
        val status = when (message.state) {
            DeliveryState.SENDING -> "发送中"
            DeliveryState.ACCEPTED -> "已接收"
            DeliveryState.DELIVERED -> "已送达"
            DeliveryState.FAILED -> "发送失败"
            null -> if (message.streaming) "生成中" else ""
        }
        if (status.isNotBlank()) Text(status, color = if (message.state == DeliveryState.FAILED) Color(0xFFB3261E) else Color(0xFF918B93), fontSize = 9.sp, modifier = Modifier.padding(top = 7.dp))
    }
}

private fun hasVisibleContent(message: ChatMessage): Boolean =
    message.text.isNotBlank() || message.agentCard != null || message.citations.isNotEmpty() ||
        (message.streaming && !message.progress.isNullOrBlank())

@Composable
private fun NewConversationDialog(state: SparkUiState, vm: SparkViewModel) {
    AlertDialog(
        onDismissRequest = vm::hideNewConversation,
        title = { Text("新建会话") },
        text = { OutlinedTextField(value = state.newPeerInput, onValueChange = vm::setNewPeer, singleLine = true, label = { Text("对方数字 UserID") }, placeholder = { Text("例如 10001") }) },
        confirmButton = { Button(onClick = vm::createNewConversation) { Text("打开") } },
        dismissButton = { TextButton(onClick = vm::hideNewConversation) { Text("取消") } }
    )
}

@Composable
private fun KnowledgeScreen(state: SparkUiState, vm: SparkViewModel) {
    Column(Modifier.fillMaxSize()) {
        FeatureTopBar("CANN 知识原文", "受保护的文档与片段查看", vm::goHome)
        Column(Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(18.dp)) {
            OutlinedTextField(value = state.knowledgeDocId, onValueChange = vm::setKnowledgeDocId, modifier = Modifier.fillMaxWidth(), singleLine = true, label = { Text("doc_id") }, placeholder = { Text("doc_…") })
            OutlinedTextField(value = state.knowledgeChunkId, onValueChange = vm::setKnowledgeChunkId, modifier = Modifier.fillMaxWidth().padding(top = 8.dp), singleLine = true, label = { Text("chunk_id（可选）") })
            Button(onClick = vm::loadKnowledge, enabled = !state.knowledgeLoading, modifier = Modifier.padding(top = 12.dp), shape = RoundedCornerShape(12.dp)) {
                if (state.knowledgeLoading) CircularProgressIndicator(Modifier.size(17.dp), strokeWidth = 2.dp) else Text("读取原文")
            }
            state.knowledgeError?.let { ErrorText(it) }
            state.knowledge?.let { doc ->
                Card(colors = CardDefaults.cardColors(containerColor = Color.White), modifier = Modifier.fillMaxWidth().padding(top = 18.dp), shape = RoundedCornerShape(18.dp)) {
                    Column(Modifier.padding(18.dp)) {
                        Text(doc.title, fontSize = 21.sp, fontWeight = FontWeight.SemiBold)
                        val meta = listOf("来源" to doc.sourceId, "权威级别" to doc.authority, "版本" to doc.revision, "Generation" to doc.generation, "定位" to doc.locator)
                        meta.filter { it.second.isNotBlank() }.forEach { (label, value) -> Text("$label：$value", fontSize = 12.sp, color = Color(0xFF6D686D), modifier = Modifier.padding(top = 5.dp)) }
                        HorizontalDivider(modifier = Modifier.padding(vertical = 14.dp), color = Color(0xFFE5E2E5))
                        Text(doc.content.ifBlank { "文档没有可显示的正文。" }, fontSize = 14.sp, lineHeight = 21.sp)
                    }
                }
            }
        }
    }
}

@Composable
private fun AdminScreen(state: SparkUiState, vm: SparkViewModel) {
    if (state.adminToken.isNullOrBlank()) AdminLogin(state, vm) else AdminConsole(state, vm)
}

@Composable
private fun AdminLogin(state: SparkUiState, vm: SparkViewModel) {
    Column(Modifier.fillMaxSize().padding(28.dp), verticalArrangement = Arrangement.Center) {
        FeatureTopBar("管理员控制台", "用户、Token、审计、房间和广播", vm::goHome)
        Card(colors = CardDefaults.cardColors(containerColor = Color.White), shape = RoundedCornerShape(22.dp), modifier = Modifier.padding(top = 24.dp)) {
            Column(Modifier.padding(22.dp)) {
                Text("管理员登录", fontSize = 20.sp, fontWeight = FontWeight.SemiBold)
                Text("管理员凭证由服务端环境配置验证。", fontSize = 12.sp, color = Color(0xFF777477), modifier = Modifier.padding(top = 6.dp, bottom = 14.dp))
                OutlinedTextField(value = state.adminAccount, onValueChange = vm::setAdminAccount, modifier = Modifier.fillMaxWidth(), singleLine = true, label = { Text("管理员账号") })
                OutlinedTextField(value = state.adminPassword, onValueChange = vm::setAdminPassword, modifier = Modifier.fillMaxWidth().padding(top = 10.dp), singleLine = true, label = { Text("管理员密码") }, visualTransformation = PasswordVisualTransformation())
                Button(onClick = vm::adminLogin, enabled = !state.adminBusy, modifier = Modifier.fillMaxWidth().padding(top = 16.dp), shape = RoundedCornerShape(14.dp)) {
                    if (state.adminBusy) CircularProgressIndicator(Modifier.size(18.dp), strokeWidth = 2.dp) else Text("登录控制台")
                }
            }
        }
        ErrorText(state.adminError)
    }
}

@Composable
private fun AdminConsole(state: SparkUiState, vm: SparkViewModel) {
    Column(Modifier.fillMaxSize()) {
        Row(Modifier.fillMaxWidth().padding(horizontal = 18.dp, vertical = 12.dp), verticalAlignment = Alignment.CenterVertically) {
            Text("‹", fontSize = 32.sp, color = Color(0xFF6D5A91), modifier = Modifier.clickable { vm.goHome() })
            Column(Modifier.weight(1f).padding(start = 10.dp)) {
                Text("管理员控制台", fontSize = 20.sp, fontWeight = FontWeight.SemiBold)
                Text("管理员 ID ${state.adminUserId}", fontSize = 12.sp, color = Color(0xFF777477))
            }
            TextButton(onClick = vm::adminLogout) { Text("退出") }
        }
        HorizontalDivider(color = Color(0xFFE5E2E5))
        AdminTabs(state.adminSection, vm)
        state.adminError?.let { ErrorText(it) }
        state.adminNotice?.let { Text(it, color = Color(0xFF22734F), fontSize = 12.sp, modifier = Modifier.padding(horizontal = 18.dp, vertical = 5.dp)) }
        when (state.adminSection) {
            AdminSection.LOGIN -> Unit
            AdminSection.OVERVIEW -> AdminOverview(state, vm)
            AdminSection.USERS -> AdminUsers(state, vm)
            AdminSection.AUDIT -> AdminAudit(state, vm)
            AdminSection.OPERATIONS -> AdminOperations(state, vm)
        }
    }
}

@Composable
private fun AdminTabs(section: AdminSection, vm: SparkViewModel) {
    val tabs = listOf(AdminSection.OVERVIEW to "概览", AdminSection.USERS to "用户", AdminSection.AUDIT to "审计", AdminSection.OPERATIONS to "系统广播")
    Row(Modifier.fillMaxWidth().horizontalScroll(rememberScrollState()).padding(horizontal = 10.dp), horizontalArrangement = Arrangement.spacedBy(4.dp)) {
        tabs.forEach { (value, label) ->
            if (value == section) Button(onClick = { vm.setAdminSection(value) }, contentPadding = PaddingValues(horizontal = 13.dp, vertical = 2.dp), shape = RoundedCornerShape(12.dp)) { Text(label, fontSize = 12.sp) }
            else OutlinedButton(onClick = { vm.setAdminSection(value) }, contentPadding = PaddingValues(horizontal = 13.dp, vertical = 2.dp), shape = RoundedCornerShape(12.dp)) { Text(label, fontSize = 12.sp) }
        }
    }
}

@Composable
private fun AdminOverview(state: SparkUiState, vm: SparkViewModel) {
    Column(Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(18.dp)) {
        Text("系统概览", fontSize = 22.sp, fontWeight = FontWeight.SemiBold)
        Row(Modifier.fillMaxWidth().padding(top = 14.dp), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            StatCard("用户", state.adminUsers.size.toString(), Modifier.weight(1f))
            StatCard("审计", state.adminAudit.size.toString(), Modifier.weight(1f))
            StatCard("协议", "Spark", Modifier.weight(1f))
        }
        Text("管理员端点与 PC 控制台共用同一套 /api/admin 协议。", fontSize = 13.sp, color = Color(0xFF777477), modifier = Modifier.padding(top = 18.dp))
        OutlinedButton(onClick = vm::loadAdminAll, modifier = Modifier.padding(top = 12.dp)) { Text("刷新全部数据") }
    }
}

@Composable
private fun StatCard(label: String, value: String, modifier: Modifier = Modifier) {
    Surface(color = Color.White, shape = RoundedCornerShape(15.dp), modifier = modifier) {
        Column(Modifier.padding(13.dp)) { Text(label, fontSize = 12.sp, color = Color(0xFF777477)); Text(value, fontSize = 24.sp, fontWeight = FontWeight.SemiBold, modifier = Modifier.padding(top = 3.dp)) }
    }
}

@Composable
private fun AdminUsers(state: SparkUiState, vm: SparkViewModel) {
    var editing by remember { mutableStateOf<AdminUser?>(null) }
    Column(Modifier.fillMaxSize()) {
        Row(Modifier.fillMaxWidth().padding(horizontal = 18.dp, vertical = 6.dp), verticalAlignment = Alignment.CenterVertically) {
            OutlinedTextField(value = state.adminSearch, onValueChange = vm::setAdminSearch, modifier = Modifier.weight(1f), singleLine = true, label = { Text("搜索账号 / ID / 昵称") })
            TextButton(onClick = vm::loadAdminUsers) { Text("刷新") }
        }
        LazyColumn(Modifier.fillMaxSize().padding(horizontal = 14.dp)) {
            val keyword = state.adminSearch.trim().lowercase()
            val users = state.adminUsers.filter { keyword.isBlank() || "${it.userId} ${it.account} ${it.name}".lowercase().contains(keyword) }
            items(users, key = { it.userId }) { user ->
                Card(colors = CardDefaults.cardColors(containerColor = Color.White), modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp), shape = RoundedCornerShape(15.dp)) {
                    Column(Modifier.padding(13.dp)) {
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Column(Modifier.weight(1f)) { Text("#${user.userId} · ${user.name.ifBlank { "未命名" }}", fontWeight = FontWeight.Medium); Text(user.account, fontSize = 12.sp, color = Color(0xFF777477)) }
                            Text(user.status, fontSize = 12.sp, color = if (user.status == "active") Color(0xFF22734F) else Color(0xFFB3261E))
                        }
                        Row(Modifier.padding(top = 7.dp), horizontalArrangement = Arrangement.spacedBy(5.dp)) {
                            if (user.status == "active") OutlinedButton(onClick = { vm.adminChangeStatus(user.userId, "disable") }, contentPadding = PaddingValues(horizontal = 8.dp, vertical = 1.dp)) { Text("禁用", fontSize = 11.sp) }
                            else OutlinedButton(onClick = { vm.adminChangeStatus(user.userId, "restore") }, contentPadding = PaddingValues(horizontal = 8.dp, vertical = 1.dp)) { Text("恢复", fontSize = 11.sp) }
                            OutlinedButton(onClick = { vm.adminRevokeTokens(user.userId) }, contentPadding = PaddingValues(horizontal = 8.dp, vertical = 1.dp)) { Text("撤销 Token", fontSize = 11.sp) }
                            TextButton(onClick = { editing = user }, contentPadding = PaddingValues(horizontal = 7.dp, vertical = 1.dp)) { Text("改昵称", fontSize = 11.sp) }
                            TextButton(onClick = { vm.loadAdminAudit(user.userId); vm.setAdminSection(AdminSection.AUDIT) }, contentPadding = PaddingValues(horizontal = 7.dp, vertical = 1.dp)) { Text("审计", fontSize = 11.sp) }
                        }
                    }
                }
            }
        }
    }
    editing?.let { user -> EditNameDialog(user, vm) { editing = null } }
}

@Composable
private fun EditNameDialog(user: AdminUser, vm: SparkViewModel, close: () -> Unit) {
    var value by remember(user.userId) { mutableStateOf(user.name) }
    AlertDialog(onDismissRequest = close, title = { Text("修改昵称") }, text = { OutlinedTextField(value = value, onValueChange = { value = it }, singleLine = true, label = { Text("昵称") }) }, confirmButton = { Button(onClick = { vm.adminUpdateName(user.userId, value); close() }) { Text("保存") } }, dismissButton = { TextButton(onClick = close) { Text("取消") } })
}

@Composable
private fun AdminAudit(state: SparkUiState, vm: SparkViewModel) {
    Column(Modifier.fillMaxSize()) {
        Row(Modifier.fillMaxWidth().padding(horizontal = 18.dp, vertical = 7.dp), verticalAlignment = Alignment.CenterVertically) {
            Text("操作记录", fontSize = 19.sp, fontWeight = FontWeight.SemiBold, modifier = Modifier.weight(1f))
            TextButton(onClick = { vm.loadAdminAudit() }) { Text("刷新") }
        }
        LazyColumn(Modifier.fillMaxSize().padding(horizontal = 14.dp)) {
            items(state.adminAudit, key = { if (it.id > 0) it.id else "${it.createdAt}:${it.action}:${it.targetId}" }) { log ->
                Surface(color = Color.White, shape = RoundedCornerShape(13.dp), modifier = Modifier.fillMaxWidth().padding(vertical = 3.dp)) {
                    Column(Modifier.padding(12.dp)) {
                        Text(log.action, fontWeight = FontWeight.Medium)
                        Text("${log.createdAt} · 操作者 #${log.actorId} · 目标 #${log.targetId}", fontSize = 11.sp, color = Color(0xFF777477))
                        if (log.reason.isNotBlank()) Text("原因：${log.reason}", fontSize = 12.sp, modifier = Modifier.padding(top = 4.dp))
                        if (log.metadata.isNotBlank()) Text(log.metadata, fontSize = 11.sp, color = Color(0xFF777477), maxLines = 3, overflow = TextOverflow.Ellipsis, modifier = Modifier.padding(top = 3.dp))
                    }
                }
            }
        }
    }
}

@Composable
private fun AdminOperations(state: SparkUiState, vm: SparkViewModel) {
    Column(Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(18.dp)) {
        Text("系统广播", fontSize = 21.sp, fontWeight = FontWeight.SemiBold)
        Text("保留 PC 端的全站通知能力；聊天室和视频弹幕不进入 Android 客户端。", fontSize = 12.sp, color = SparkMuted, modifier = Modifier.padding(top = 5.dp))
        Card(colors = CardDefaults.cardColors(containerColor = Color.White), modifier = Modifier.fillMaxWidth().padding(top = 12.dp), shape = RoundedCornerShape(16.dp)) {
            Column(Modifier.padding(15.dp)) {
                Text("向所有 Spark Push 客户端发送通知", fontWeight = FontWeight.Medium)
                OutlinedTextField(value = state.adminBroadcastText, onValueChange = vm::setAdminBroadcastText, modifier = Modifier.fillMaxWidth().padding(top = 8.dp), minLines = 3, label = { Text("广播内容") })
                Button(onClick = vm::adminBroadcast, modifier = Modifier.padding(top = 10.dp)) { Text("发送广播") }
            }
        }
    }
}

@Composable
private fun FeatureTopBar(title: String, subtitle: String, onBack: () -> Unit) {
    Row(Modifier.fillMaxWidth().padding(horizontal = 18.dp, vertical = 12.dp), verticalAlignment = Alignment.CenterVertically) {
        Text("‹", fontSize = 32.sp, color = Color(0xFF6D5A91), modifier = Modifier.clickable(onClick = onBack))
        Column(Modifier.padding(start = 10.dp)) { Text(title, fontSize = 20.sp, fontWeight = FontWeight.SemiBold); Text(subtitle, fontSize = 12.sp, color = Color(0xFF777477)) }
    }
}

@Composable
private fun ConnectionPill(state: ConnectionState) {
    Surface(color = if (state == ConnectionState.CONNECTED) Color(0xFFDDF3EA) else Color(0xFFFFF0CC), shape = RoundedCornerShape(30.dp)) {
        Text(connectionLabel(state), color = if (state == ConnectionState.CONNECTED) Color(0xFF22734F) else Color(0xFF86631A), fontSize = 11.sp, modifier = Modifier.padding(horizontal = 10.dp, vertical = 6.dp))
    }
}

private fun connectionLabel(state: ConnectionState): String = when (state) {
    ConnectionState.CONNECTED -> "已连接"
    ConnectionState.CONNECTING -> "连接中"
    ConnectionState.RECONNECTING -> "重连中"
    ConnectionState.DISCONNECTED -> "未连接"
}

private fun isNearBottom(listState: LazyListState): Boolean {
    val layout = listState.layoutInfo
    if (layout.totalItemsCount == 0) return true
    val last = layout.visibleItemsInfo.lastOrNull() ?: return false
    if (last.index < layout.totalItemsCount - 1) return false
    return layout.viewportEndOffset - (last.offset + last.size) <= 240
}

private fun messageKeyForUi(message: ChatMessage): String = if (message.msgId.isNotBlank()) "m:${message.msgId}" else if (message.clientMsgId.isNotBlank()) "c:${message.clientMsgId}" else "${message.sessionId}:${message.seq}:${message.timestampMs}:${message.senderId}"

@Composable
private fun ErrorText(error: String?) {
    if (!error.isNullOrBlank()) Text(error, color = Color(0xFFB3261E), fontSize = 12.sp, modifier = Modifier.padding(top = 12.dp))
}
