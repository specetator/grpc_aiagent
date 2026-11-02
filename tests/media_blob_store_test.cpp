#include "repositories/media_blob_store.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <sys/stat.h>
using namespace sparkpush;
namespace fs=std::filesystem;
void Require(bool value,const char* why) { if(!value) throw std::runtime_error(why); }
template<class F> void Reject(F call) {
    bool rejected=false;
    try { call(); } catch(const std::exception&) { rejected=true; }
    Require(rejected,"invalid storage operation accepted");
}
int main() {
    char pattern[]="/tmp/spark-media-blob-XXXXXX";
    const char* created=mkdtemp(pattern); Require(created,"temporary directory");
    const fs::path root(created);
    try {
        MediaBlobStore blobs(root.string());
        RoomMedia media; Require(GenerateAttachmentId(&media.file.id),"id");
        const std::string first(kMediaChunkBytes,'a'), last="late meeting decision";
        media.file.mime="text/plain"; media.file.bytes=first.size()+last.size();
        media.file.sha256=Sha256Hex(first+last);
        media.chunks={{0,first.size(),Sha256Hex(first)},{1,last.size(),Sha256Hex(last)}};
        blobs.WriteChunk(media.file.id,1,last); blobs.WriteChunk(media.file.id,0,first);
        blobs.WriteChunk(media.file.id,0,first);
        Reject([&]{blobs.WriteChunk(media.file.id,0,std::string(first.size(),'b'));});
        Reject([&]{blobs.WriteChunk("../../escape",0,"x");});
        Reject([&]{blobs.WriteChunk(media.file.id,1024,"x");});
        auto wrong=media; wrong.file.sha256=std::string(64,'0');
        Reject([&]{blobs.Assemble(wrong);});
        Require(!fs::exists(root/media.file.id),"bad hash published original");
        blobs.Assemble(media);
        // Simulate SQL not committing after immutable publication, then restart
        // the repository and repeat finalization against the same files.
        MediaBlobStore restarted(root.string()); restarted.Assemble(media);
        Require(restarted.Read(media,kMediaChunkBytes-2,8)=="aa"+last.substr(0,6),"range spans chunks");
        Require(restarted.Read(media,kMediaChunkBytes,kMediaChunkBytes)==last,"bounded final range");
        Reject([&]{restarted.Read(media,media.file.bytes,1);});
        restarted.RemoveChunks(media);
        Require(restarted.Read(media,0,3)=="aaa","cleanup damaged original");
        const auto original=root/media.file.id;
        fs::rename(original,root/"saved"); fs::create_symlink(root/"saved",original);
        Reject([&]{restarted.Read(media,0,3);});
        fs::remove(original); fs::create_hard_link(root/"saved",original);
        Reject([&]{restarted.Read(media,0,3);});
        fs::remove(original); Require(mkfifo(original.c_str(),0600)==0,"FIFO fixture");
        Reject([&]{restarted.Read(media,0,3);});
        fs::remove(original); fs::rename(root/"saved",original);
        RoomMedia image; Require(GenerateAttachmentId(&image.file.id),"image id");
        image.file.mime="image/png"; image.file.bytes=4; image.file.sha256=Sha256Hex("text");
        image.chunks={{0,4,image.file.sha256}};
        blobs.WriteChunk(image.file.id,0,"text");
        Reject([&]{blobs.Assemble(image);});
        Require(!fs::exists(root/image.file.id),"false MIME published");
        std::cout << "media blob integrity, range, restart, traversal and link checks passed\n";
    } catch(...) { fs::remove_all(root); throw; }
    fs::remove_all(root);
}
