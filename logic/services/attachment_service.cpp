#include "services/attachment_service.h"

namespace sparkpush {
bool AttachmentService::Available() const {
    return repository_ && repository_->Available();
}
ServiceStatus AttachmentService::Upload(int64_t user, const std::string& session,
                                        const std::string& name,
                                        const std::string& data,
                                        StoredAttachment* attachment) {
    if (!Available()) return {503, "image attachments are not enabled"};
    if (session.empty() || data.empty())
        return {400, "session_id and data are required"};
    if (!conversations_) return {500, "conversation store not initialized"};
    Session value;
    // create_single=true：上传是全工程唯一允许惰性建会话的路径。
    // 必须先鉴权再解码：无权访问的会话不应该让服务端白白消耗
    // 一轮 base64 解码的 CPU，也不应该为它多分配一份字节副本。
    auto status = conversations_->CheckAccess(user, session, &value, true);
    if (!status.ok()) {
        // 成员关系查询失败与普通 403 区分开：前者是服务端故障，
        // 拼上详细文本方便排查，也避开把它误报成「你无权访问」。
        if (status.reason == ServiceReason::kMembershipLookupFailed)
            status.message = "check session membership failed: " + status.message;
        return status;
    }
    std::string bytes;
    if (!DecodeBase64(data, &bytes)) return {400, "invalid base64 image data"};
    std::string error;
    // 尺寸上限、魔数探测、MIME 白名单全在 repository_->Create 里面，
    // 本层不重复校验，避开两边阈值不一致。
    // 注意失败统一返回 400：对客户端而言「图太大/格式不对」都是请求错。
    if (!attachment ||
        !repository_->Create(user, session, name, bytes, attachment, &error))
        return {400, error};
    return {};
}
ServiceStatus AttachmentService::Read(int64_t user, const std::string& id,
                                      AttachmentContents* result) {
    if (!Available()) return {503, "image attachments are not enabled"};
    if (!result) return {500, "attachment output is null"};
    *result = {};
    std::string error;
    if (!repository_->Find(id, &result->metadata, &error)) return {404, error};
    const auto& stored = result->metadata;
    // 可见性：上传者本人 或 所属会话的成员。
    // 不是本人时才去查会话权限，避开了「自己读自己的图还要多一次成员查询」。
    // 三个失败条件归为同一个 403：session_id 为空、conversations_ 未初始化、
    // CheckAccess 不通过。对外统一措辞是故意的：不泄露「附件存在但你无权」
    // 与「附件不存在」的差异，避开用附件 id 做存在性探测。
    if (stored.owner_user_id != user) {
        Session session;
        if (stored.session_id.empty() || !conversations_ ||
            !conversations_->CheckAccess(user, stored.session_id, &session).ok())
            return {403, "attachment is not visible"};
    }
    // 鉴权通过之后才读磁盘：避开无权请求也触发一次文件 IO。
    if (!repository_->Read(stored, &result->bytes, &error)) return {404, error};
    return {};
}
// 发消息时对 content_json 里每个图片引用做校验。
// 返回的全是 400 而不是 403：对发送者而言这些都是「你这条消息不合法」，
// 而不是「你无权访问某个资源」。
ServiceStatus AttachmentService::AuthorizeMessageImages(int64_t sender,
                                                        const std::string& session,
                                                        const std::string& content) {
    // 先提取引用，空则直接返回成功。这个提前出口很关键：
    // 绝大多数消息是纯文本，不应该为它们多跑一次 Available() 与循环。
    const auto refs = ExtractImageRefsFromContentJson(content);
    if (refs.empty()) return {};
    if (!Available()) return {400, "image attachments are not enabled"};
    for (const auto& ref : refs) {
        StoredAttachment stored;
        std::string error;
        if (!repository_->Find(ref.id, &stored, &error)) return {400, error};
        // 防 IDOR：只能引用自己上传的图。不做这一步的话，
        // 任何人只要拿到一个 attachment_id 就能把它当成自己的内容发出去。
        if (stored.owner_user_id != sender)
            return {400, "attachment does not belong to the sender"};
        if (stored.status != "ready") return {400, "attachment is not ready"};
        // 已绑定的附件必须属于当前会话：防止把 A 会话的图泄到 B 会话。
        if (!stored.session_id.empty() && stored.session_id != session)
            return {400, "attachment belongs to another conversation"};
        // 未绑定的当场绑定到当前会话。这步有副作用且不可逆：
        // 一旦绑定，上面的校验就会拒绝其他会话再引用它。
        if (stored.session_id.empty() && !repository_->Bind(ref.id, session, &error))
            return {400, error};
    }
    return {};
}
}  // namespace sparkpush
