#include "repositories/media_blob_store.h"
#include <openssl/evp.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/syscall.h>
#include <linux/fs.h>
#include <unistd.h>
#include <dirent.h>
#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <chrono>
#include <thread>

namespace sparkpush {
namespace {
class Fd {
 public:
    explicit Fd(int fd) : fd_(fd) { if (fd < 0) throw std::runtime_error("media storage unavailable"); }
    ~Fd() { close(fd_); }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    operator int() const { return fd_; }
    int Release() { const auto fd=fd_; fd_=-1; return fd; }
 private:
    int fd_;
};
void Check(bool value) { if (!value) throw std::runtime_error("media integrity or storage check failed"); }
void Id(const std::string& id) { Check(IsValidAttachmentId(id)); }
std::string ChunkName(const std::string& id, uint64_t index) {
    Id(id); Check(index < kMediaMaxBytes / kMediaChunkBytes);
    return id + "." + std::to_string(index);
}
int OpenRoot(const std::string& root) {
    std::filesystem::create_directories(root);
    return open(root.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
}
int ChunkDirectory(int root) {
    if (mkdirat(root, "media_chunks", 0700) != 0 && errno != EEXIST)
        throw std::runtime_error("media chunk directory unavailable");
    Check(fsync(root) == 0);
    return openat(root, "media_chunks", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
}
void FileSize(int fd, uint64_t expected) {
    struct stat info{};
    Check(fstat(fd, &info) == 0 && S_ISREG(info.st_mode) && info.st_nlink == 1 &&
          info.st_size >= 0 && static_cast<uint64_t>(info.st_size) == expected);
}
void WriteAll(int fd, const std::string& bytes) {
    size_t pos = 0;
    while (pos < bytes.size()) {
        auto n = write(fd, bytes.data() + pos, bytes.size() - pos);
        if (n < 0 && errno == EINTR) continue;
        Check(n > 0); pos += static_cast<size_t>(n);
    }
}
std::string ReadAt(int fd, uint64_t offset, uint64_t count) {
    Check(count <= kMediaChunkBytes);
    std::string bytes(count, '\0');
    size_t pos = 0;
    while (pos < bytes.size()) {
        auto n = pread(fd, bytes.data() + pos, bytes.size() - pos, offset + pos);
        if (n < 0 && errno == EINTR) continue;
        Check(n > 0); pos += static_cast<size_t>(n);
    }
    return bytes;
}
class Digest {
 public:
    Digest() : ctx_(EVP_MD_CTX_new(), EVP_MD_CTX_free) {
        Check(ctx_ && EVP_DigestInit_ex(ctx_.get(), EVP_sha256(), nullptr) == 1);
    }
    void Add(const std::string& value) { Check(EVP_DigestUpdate(ctx_.get(), value.data(), value.size()) == 1); }
    std::string Finish() {
        unsigned char bytes[EVP_MAX_MD_SIZE]; unsigned int n = 0;
        Check(EVP_DigestFinal_ex(ctx_.get(), bytes, &n) == 1 && n == 32);
        const char* digits = "0123456789abcdef";
        std::string result;
        for (unsigned int i = 0; i < n; ++i) { result += digits[bytes[i] >> 4]; result += digits[bytes[i] & 15]; }
        return result;
    }
 private:
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx_;
};
void VerifyFile(int fd, const RoomMedia& media) {
    FileSize(fd, media.file.bytes);
    Digest digest;
    for (uint64_t offset = 0; offset < media.file.bytes; offset += kMediaChunkBytes)
        digest.Add(ReadAt(fd, offset, std::min(kMediaChunkBytes, media.file.bytes - offset)));
    Check(digest.Finish() == media.file.sha256);
}
bool HeaderMatches(const std::string& mime, const std::string& head) {
    if (mime.rfind("image/", 0) == 0) return DetectImageMime(head) == mime;
    if (mime == "application/pdf") return head.rfind("%PDF-", 0) == 0;
    if (mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        return head.rfind(std::string("PK\3\4", 4), 0) == 0;
    if (mime == "audio/wav") return head.size() >= 12 && head.substr(0,4) == "RIFF" && head.substr(8,4) == "WAVE";
    if (mime == "audio/flac") return head.rfind("fLaC", 0) == 0;
    if (mime == "audio/ogg" || mime == "video/ogg") return head.rfind("OggS", 0) == 0;
    if (mime == "audio/mpeg") return head.rfind("ID3", 0) == 0 ||
        (head.size() >= 2 && static_cast<unsigned char>(head[0]) == 255 &&
         (static_cast<unsigned char>(head[1]) & 0xe0) == 0xe0);
    if (mime == "audio/mp4" || mime == "video/mp4") return head.size() >= 12 && head.substr(4,4) == "ftyp";
    if (mime == "audio/webm" || mime == "video/webm") return head.rfind(std::string("\x1a\x45\xdf\xa3",4),0) == 0;
    if (mime == "text/plain" || mime == "text/markdown") return head.find('\0') == std::string::npos;
    return false;
}
class Temporary {
 public:
    Temporary(int parent, const std::string& owner) : parent_(parent) {
        Check(GenerateAttachmentId(&name_)); name_ = owner + ".partial." + name_;
    }
    ~Temporary() { unlinkat(parent_, name_.c_str(), 0); }
    const std::string& name() const { return name_; }
 private:
    int parent_;
    std::string name_;
};
void CleanPartials(int directory, const std::string& asset) {
    // A duplicate directory fd shares the seek offset. Open a new description
    // so repeated cleanup on the same parent sees every entry.
    int scan=openat(directory,".",O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW);
    Check(scan>=0);
    DIR* raw=fdopendir(scan);
    if(!raw) { close(scan); throw std::runtime_error("media cleanup unavailable"); }
    auto close_directory=[](DIR* value) { closedir(value); };
    std::unique_ptr<DIR,decltype(close_directory)> entries(raw,close_directory);
    errno=0;
    while(auto* entry=readdir(entries.get())) {
        const std::string name=entry->d_name;
        if(name.rfind(asset+".",0)==0 && name.find(".partial.")!=std::string::npos)
            Check(unlinkat(directory,name.c_str(),0)==0 || errno==ENOENT);
        errno=0;
    }
    Check(errno==0);
}
bool Publish(int directory, const std::string& temp, const std::string& name) {
    if (syscall(SYS_renameat2, directory, temp.c_str(), directory, name.c_str(), RENAME_NOREPLACE) == 0)
        return true;
    Check(errno == EEXIST);
    return false;
}
}  // namespace

MediaFileLock::~MediaFileLock() { close(fd_); }
std::unique_ptr<MediaFileLock> MediaBlobStore::Lock(const std::string& id) const {
    Id(id);
    Fd root(OpenRoot(root_));
    if(mkdirat(root,"media_locks",0700)!=0 && errno!=EEXIST)
        throw std::runtime_error("media lock directory unavailable");
    Fd directory(openat(root,"media_locks",O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW));
    Fd lock(openat(directory,id.c_str(),O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW|O_NONBLOCK,0600));
    FileSize(lock,0);
    Check(fsync(directory)==0 && fsync(root)==0);
    const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
    while(flock(lock,LOCK_EX|LOCK_NB)!=0) {
        if((errno!=EWOULDBLOCK && errno!=EINTR) || std::chrono::steady_clock::now()>=deadline)
            throw std::runtime_error("media operation still in progress; retry same identity");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    auto held=std::make_unique<MediaFileLock>(static_cast<int>(lock));
    lock.Release();
    return held;
}

void MediaBlobStore::WriteChunk(const std::string& id, uint64_t index, const std::string& bytes) const {
    Check(!bytes.empty() && bytes.size() <= kMediaChunkBytes);
    const auto name = ChunkName(id, index);
    Fd root(OpenRoot(root_)); Fd directory(ChunkDirectory(root));
    CleanPartials(directory,id);
    Temporary temp(directory,name);
    Fd output(openat(directory, temp.name().c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    WriteAll(output, bytes); Check(fsync(output) == 0);
    if (!Publish(directory, temp.name(), name)) {
        Fd existing(openat(directory, name.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
        FileSize(existing, bytes.size());
        Check(ReadAt(existing, 0, bytes.size()) == bytes);
    }
    Check(fsync(directory) == 0);
}

void MediaBlobStore::Assemble(const RoomMedia& media) const {
    Id(media.file.id);
    Check(media.file.bytes > 0 && media.file.bytes <= kMediaMaxBytes);
    const auto count = (media.file.bytes + kMediaChunkBytes - 1) / kMediaChunkBytes;
    Check(media.chunks.size() == count);
    Fd root(OpenRoot(root_)); Fd directory(ChunkDirectory(root));
    CleanPartials(root,media.file.id);
    Temporary temp(root,media.file.id);
    Fd output(openat(root, temp.name().c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    Digest digest;
    uint64_t total = 0;
    for (uint64_t i = 0; i < count; ++i) {
        const auto& chunk = media.chunks.at(i);
        Check(chunk.index == i && chunk.bytes == std::min(kMediaChunkBytes, media.file.bytes - total));
        Fd input(openat(directory, ChunkName(media.file.id, i).c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
        FileSize(input, chunk.bytes);
        const auto bytes = ReadAt(input, 0, chunk.bytes);
        Check(Sha256Hex(bytes) == chunk.sha256);
        if (i == 0) Check(HeaderMatches(media.file.mime, bytes.substr(0,4096)));
        digest.Add(bytes); WriteAll(output, bytes); total += bytes.size();
    }
    Check(total == media.file.bytes && digest.Finish() == media.file.sha256);
    Check(fsync(output) == 0);
    // Never overwrite an original. A file left after SQL rollback must match
    // exactly before the finalization can be retried.
    if (!Publish(root, temp.name(), media.file.id)) {
        Fd existing(openat(root, media.file.id.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
        VerifyFile(existing, media);
    }
    Check(fsync(root) == 0);
}

std::string MediaBlobStore::Read(const RoomMedia& media, uint64_t offset, uint64_t length) const {
    Id(media.file.id);
    Check(length > 0 && length <= kMediaChunkBytes && offset < media.file.bytes);
    Fd root(OpenRoot(root_));
    Fd input(openat(root, media.file.id.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
    FileSize(input, media.file.bytes);
    return ReadAt(input, offset, std::min(length, media.file.bytes - offset));
}

void MediaBlobStore::RemoveChunks(const RoomMedia& media) const {
    Id(media.file.id);
    Check(media.file.bytes>0 && media.file.bytes<=kMediaMaxBytes);
    Fd root(OpenRoot(root_)); Fd directory(ChunkDirectory(root));
    // Include chunks whose immutable file was written before its SQL row.
    const auto count=(media.file.bytes+kMediaChunkBytes-1)/kMediaChunkBytes;
    for (uint64_t index=0;index<count;++index)
        if (unlinkat(directory, ChunkName(media.file.id, index).c_str(), 0) != 0 && errno != ENOENT)
            throw std::runtime_error("media chunk cleanup incomplete");
    CleanPartials(directory,media.file.id);
    Check(fsync(directory) == 0);
}
void MediaBlobStore::RemoveStaging(const RoomMedia& media) const {
    Id(media.file.id);
    RemoveChunks(media);
    Fd root(OpenRoot(root_));
    CleanPartials(root,media.file.id);
    Check(fsync(root)==0);
}
void MediaBlobStore::DiscardUpload(const RoomMedia& media) const {
    RemoveStaging(media);
    Fd root(OpenRoot(root_));
    if(unlinkat(root,media.file.id.c_str(),0)!=0 && errno!=ENOENT)
        throw std::runtime_error("media original cleanup incomplete");
    Check(fsync(root)==0);
}
}  // namespace sparkpush
