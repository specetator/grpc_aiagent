#pragma once

#include "image_attachment.h"
#include "services/conversation_service.h"

namespace sparkpush {
// 一次读取的结果：元信息 + 字节一起返回，避开适配层再发一次请求。
struct AttachmentContents {
    StoredAttachment metadata;
    std::string bytes;
};

// 图片附件的业务层。本类只负责**鉴权与编排**，
// 字节读写、魔数探测、尺寸限制全在 AttachmentRepository / AttachmentDao 里。
//
// 三个方法对应附件生命周期的三个阶段：
//   Upload                    存图（可惰性建会话）
//   AuthorizeMessageImages    发消息时校验引用的图合法（关键门禁）
//   Read                      下载/展示
//
// 中间那个是最容易被忽略但最关键的一步：若不在发消息时校验，
// 客户端就可以在 content_json 里写任意 attachment_id，
// 把别人上传的图当成自己的发出去（典型 IDOR）。
class AttachmentService {
   public:
    AttachmentService(AttachmentRepository* repository,
                      ConversationService* conversations)
        : repository_(repository), conversations_(conversations) {}
    bool Available() const;

    // 上传图片。顺序是：先鉴权 -> 再解码 -> 最后落盘入库。
    // 鉴权放最前且传 create_single=true：发图时单聊会话可能还没建，
    // 这是全工程**唯一**允许惰性创建会话的路径。
    // 先鉴权再解码也很重要：对无权访问的会话先解码等于白耗一轮 CPU，
    // 还会多分配一份字节副本（base64 文本本身就是原始体积的 4/3）。
    // 入参是 base64 字符串而不是原始字节，因为 HTTP JSON 体里无法直接携二进制。
    ServiceStatus Upload(int64_t user, const std::string& session,
                         const std::string& name, const std::string& base64_data,
                         StoredAttachment* attachment);

    // 读取附件。可见性规则是「二者之一」：
    //   上传者本人，或该附件所属会话的成员
    // 因此群里其他人发的图能被同会话成员看到，但跨会话不行。
    // session_id 为空的附件（上传后从未被任何消息引用）只有本人能读。
    ServiceStatus Read(int64_t user, const std::string& id, AttachmentContents* result);

    // Shared by all message transports. Checks ownership/readiness/session
    // binding before accepting any attachment reference into the message.
    //
    // 译：所有消息传输通道共用。在把任何附件引用接受进消息之前，
    // 先校验归属、就绪状态与会话绑定。
    //
    // 四道校验，少一道就是一个漏洞：
    //   存在         Find 不到 -> 400（避开消息里悬一个永远 404 的引用）
    //   归属         owner != sender -> 400（防 IDOR：不能引用别人的图）
    //   就绪         status != "ready" -> 400（避开引用一个未完成/已作废的附件）
    //   会话绑定     已绑定则必须等于当前会话；未绑定则当场绑定到当前会话
    // 最后一条的绑定动作有副作用：它把「上传时还不知道会话」的附件
    // 固定到首次引用它的会话上，之后就不能再被别的会话引用了。
    // 没有图片引用时直接返回成功，因此普通文本消息不会因本检查变慢。
    ServiceStatus AuthorizeMessageImages(int64_t sender, const std::string& session,
                                         const std::string& content_json);

   private:
    AttachmentRepository* repository_;
    ConversationService* conversations_;
};
}  // namespace sparkpush
