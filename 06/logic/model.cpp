#include "model.h"

#include <algorithm>
#include <chrono>

namespace sparkpush {

User InMemoryModel::RegisterOrGetUser(const std::string& account,
                                      const std::string& password) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& kv : users_) {
    if (kv.second.account == account) {
      return kv.second;
    }
  }
  User u;
  u.id = static_cast<int64_t>(users_.size() + 1);
  u.account = account;
  u.password_hash = password;  // demo：明文保存，真实实现要做 hash
  users_[u.id] = u;
  return u;
}

User InMemoryModel::GetUserById(int64_t user_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = users_.find(user_id);
  if (it != users_.end()) return it->second;
  return User{};
}

Session InMemoryModel::GetOrCreateSingleSession(int64_t user1, int64_t user2) {
  if (user1 > user2) std::swap(user1, user2);
  std::string sid = "single:" + std::to_string(user1) + ":" + std::to_string(user2);
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = sessions_.find(sid);
  if (it != sessions_.end()) return it->second;
  Session s;
  s.id = sid;
  s.type = SessionType::kSingle;
  s.user1_id = user1;
  s.user2_id = user2;
  s.last_msg_seq = 0;
  sessions_[sid] = s;
  return s;
}

Session InMemoryModel::GetOrCreateRoomSession(int64_t room_id) {
  std::string sid = "room:" + std::to_string(room_id);
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = sessions_.find(sid);
  if (it != sessions_.end()) return it->second;
  Session s;
  s.id = sid;
  s.type = SessionType::kChatroom;
  s.group_id = room_id;
  s.last_msg_seq = 0;
  sessions_[sid] = s;
  return s;
}

Message InMemoryModel::AppendMessage(const Session& session,
                                     int64_t sender_id,
                                     const std::string& msg_type,
                                     const std::string& content_json,
                                     int64_t now_ms,
                                     const std::string& client_msg_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& s = sessions_[session.id];
  s.last_msg_seq += 1;
  Message m;
  m.msg_id = session.id + "-" + std::to_string(s.last_msg_seq);
  m.session_id = session.id;
  m.msg_seq = s.last_msg_seq;
  m.sender_id = sender_id;
  m.timestamp_ms = now_ms;
  m.msg_type = msg_type;
  m.content_json = content_json;
  m.client_msg_id = client_msg_id;
  messages_[session.id].push_back(m);
  return m;
}

std::vector<Message> InMemoryModel::GetHistory(const std::string& session_id,
                                               int64_t anchor_seq,
                                               int limit) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<Message> result;
  auto it = messages_.find(session_id);
  if (it == messages_.end()) return result;
  const auto& vec = it->second;
  for (auto rit = vec.rbegin(); rit != vec.rend() && (int)result.size() < limit;
       ++rit) {
    if (anchor_seq == 0 || rit->msg_seq < anchor_seq) {
      result.push_back(*rit);
    }
  }
  std::reverse(result.begin(), result.end());
  return result;
}

std::vector<Session> InMemoryModel::ListUserSingleSessions(int64_t user_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<Session> result;
  for (const auto& kv : sessions_) {
    const Session& s = kv.second;
    if (s.type != SessionType::kSingle) continue;
    if (s.user1_id == user_id || s.user2_id == user_id) {
      result.push_back(s);
    }
  }
  return result;
}

std::vector<Session> InMemoryModel::ListUserRoomSessions(int64_t user_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<Session> result;
  // 从房间成员表中找出当前用户所在的所有 room_id，然后根据约定的 session_id 反查会话
  for (const auto& kv : room_members_) {
    int64_t room_id = kv.first;
    const auto& members = kv.second;
    if (std::find(members.begin(), members.end(), user_id) == members.end()) {
      continue;
    }
    std::string sid = "room:" + std::to_string(room_id);
    auto it = sessions_.find(sid);
    if (it != sessions_.end()) {
      result.push_back(it->second);
    }
  }
  return result;
}

void InMemoryModel::JoinRoom(int64_t room_id, int64_t user_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& vec = room_members_[room_id];
  if (std::find(vec.begin(), vec.end(), user_id) == vec.end()) {
    vec.push_back(user_id);
  }
}

void InMemoryModel::LeaveRoom(int64_t room_id, int64_t user_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = room_members_.find(room_id);
  if (it == room_members_.end()) return;
  auto& vec = it->second;
  vec.erase(std::remove(vec.begin(), vec.end(), user_id), vec.end());
}

std::vector<int64_t> InMemoryModel::GetRoomMembers(int64_t room_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<int64_t> result;
  auto it = room_members_.find(room_id);
  if (it == room_members_.end()) return result;
  result = it->second;
  return result;
}

std::string InMemoryModel::makeUserSessionKey(int64_t user_id,
                                              const std::string& session_id) const {
  return std::to_string(user_id) + ":" + session_id;
}

void InMemoryModel::MarkRead(int64_t user_id,
                             const std::string& session_id,
                             int64_t read_seq) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string key = makeUserSessionKey(user_id, session_id);
  auto& st = user_session_state_[key];
  if (read_seq > st.read_seq) {
    st.read_seq = read_seq;
  }
}

int64_t InMemoryModel::GetUnread(int64_t user_id,
                                 const std::string& session_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string key = makeUserSessionKey(user_id, session_id);
  int64_t read_seq = 0;
  auto it = user_session_state_.find(key);
  if (it != user_session_state_.end()) {
    read_seq = it->second.read_seq;
  }
  auto sit = sessions_.find(session_id);
  if (sit == sessions_.end()) return 0;
  int64_t last_seq = sit->second.last_msg_seq;
  if (last_seq <= read_seq) return 0;
  return last_seq - read_seq;
}

}  // namespace sparkpush


