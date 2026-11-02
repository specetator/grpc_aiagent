#include "services/media_manifest.h"
#include "security.h"
#include "image_attachment.h"
#include <cmath>
#include <regex>
#include <set>

namespace sparkpush {
namespace {
using J=nlohmann::json;
void Check(bool v) { if(!v) throw std::invalid_argument("invalid media manifest"); }
int64_t N(const J& v,int64_t lo,int64_t hi) {
    Check(v.is_number_integer());
    if(v.is_number_unsigned()) Check(v.get<uint64_t>()<=static_cast<uint64_t>(INT64_MAX));
    auto n=v.get<int64_t>();Check(n>=lo && n<=hi);return n;
}
bool Fields(const J& v,const std::set<std::string>& keys) {
    if(!v.is_object() || v.size()!=keys.size()) return false;
    for(auto& k:keys) if(!v.contains(k)) return false;
    return true;
}
void Base(const J& a) {
    if(Fields(a,{"page"})) N(a.at("page"),1,200);
    else if(Fields(a,{"image"})) N(a.at("image"),1,1);
    else if(Fields(a,{"timestamp_ms"})) N(a.at("timestamp_ms"),0,3601000);
    else {
        Check(Fields(a,{"section","paragraph"})); N(a.at("paragraph"),1,100000);
        static const std::regex name("document|header[0-9]{1,6}|footer[0-9]{1,6}|footnotes|endnotes");
        Check(a.at("section").is_string() && std::regex_match(a.at("section").get<std::string>(),name));
    }
}
void Fragment(const J& f) {
    Check(Fields(f,{"kind","text","anchor"}) && f.at("kind").is_string() && f.at("text").is_string());
    const auto text=f.at("text").get<std::string>();
    Check(!text.empty() && text.size()<=16384 && text.find('\0')==std::string::npos);
    (void)f.dump(); // Reject invalid UTF-8, including callers outside HTTP JSON parsing.
    J a=f.at("anchor"); Check(a.is_object() && a.contains("part")); N(a.at("part"),0,4095);a.erase("part");
    const auto kind=f.at("kind").get<std::string>();
    if(kind=="transcript") {
        Check(Fields(a,{"start_ms","end_ms"}));const auto start=N(a.at("start_ms"),0,3601500);
        N(a.at("end_ms"),start,3601500);
    } else {
        Check(kind=="text" || kind=="ocr");
        if(kind=="ocr") {Check(a.contains("artifact"));N(a.at("artifact"),0,1023);a.erase("artifact");}
        Base(a);
    }
}
}  // namespace
bool ValidateMediaFragment(const J& f) {
    try { Fragment(f);return true; } catch(const std::exception&) {return false;}
}
bool ValidateMediaCoverage(const std::string& mime,const J& c,const J& fs,const J& as) {
    try {
        Check(c.is_object() && fs.is_array() && as.is_array() && fs.size()<=4096 && as.size()<=1024);
        for(const auto& a:as) Base(a.at("anchor"));
        for(const auto& f:fs) {
            Fragment(f);
            if(f.at("kind")=="ocr") {
                auto anchor=f.at("anchor");const auto index=N(anchor.at("artifact"),0,1023);
                Check(static_cast<size_t>(index)<as.size());anchor.erase("part");anchor.erase("artifact");
                Check(anchor==as.at(index).at("anchor"));
            }
        }
        const auto kind=c.at("kind").get<std::string>();
        if(kind=="image") {
            Check(mime.rfind("image/",0)==0 && c==J({{"kind","image"},{"images",1}}) && as.size()==1 && as[0].at("anchor")==J({{"image",1}}));
            for(const auto& f:fs) Check(f.at("kind")=="ocr");
        } else if(kind=="pdf") {
            Check(mime=="application/pdf" && Fields(c,{"kind","page_count","rendered_pages"}));
            const auto pages=N(c.at("page_count"),1,200);Check(c.at("rendered_pages")==pages && as.size()==static_cast<size_t>(pages));
            for(size_t i=0;i<as.size();++i) Check(as[i].at("anchor")==J({{"page",i+1}}));
            for(const auto& f:fs) {Check(f.at("kind")=="text" || f.at("kind")=="ocr");N(f.at("anchor").at("page"),1,pages);}
        } else if(kind=="docx") {
            Check(mime=="application/vnd.openxmlformats-officedocument.wordprocessingml.document" && Fields(c,{"kind","sections","paragraphs"}));
            const auto paragraphs=N(c.at("paragraphs"),0,100000);const auto& sections=c.at("sections");
            Check(sections.is_array() && !sections.empty() && sections.size()<=2000);std::set<std::string> names;
            for(const auto& s:sections) {Base({{"section",s},{"paragraph",1}});Check(names.insert(s.get<std::string>()).second);}
            Check(names.count("document"));
            auto check=[&](const J& a) {Check(names.count(a.at("section").get<std::string>()));N(a.at("paragraph"),1,paragraphs);};
            for(const auto& a:as) check(a.at("anchor"));
            for(const auto& f:fs) {Check(f.at("kind")=="text" || f.at("kind")=="ocr");check(f.at("anchor"));}
        } else {
            Check(kind=="audio" || kind=="video");Check(mime.rfind(kind+"/",0)==0);
            auto fields=std::set<std::string>{"kind","duration_ms","container_duration_ms","source_origin_ms","audio"};if(kind=="video") fields.insert("video");
            Check(Fields(c,fields));const auto duration=N(c.at("duration_ms"),1,3600000);
            N(c.at("container_duration_ms"),1,9000000000000LL);N(c.at("source_origin_ms"),-9000000000000LL,9000000000000LL);
            const auto& a=c.at("audio");Check(!a.is_null() || kind=="video");
            int64_t latest_end=0,first_sample=duration;
            if(!a.is_null()) {
                Check(Fields(a,{"decoded_samples","sample_rate","duration_ms","offset_ms","language","segments","speaker_labels","track_end_ms"}));
                const auto samples=N(a.at("decoded_samples"),1,57616000), span=N(a.at("duration_ms"),1,3601000);
                const auto offset=N(a.at("offset_ms"),0,duration), end=N(a.at("track_end_ms"),1,duration+1000);
                latest_end=end;first_sample=offset;
                Check(a.at("sample_rate")==16000 && std::abs(samples/16.0-span)<=1 && offset+span<=duration+1000 && std::abs(end-offset-span)<=1000);
                N(a.at("segments"),0,4096);Check(a.at("speaker_labels")==false);
                Check(a.at("language").is_string() && std::regex_match(a.at("language").get<std::string>(),std::regex("[a-z]{2,3}")));
                for(const auto& f:fs) if(f.at("kind")=="transcript") {
                    const auto start=N(f.at("anchor").at("start_ms"),offset,offset+span+500);
                    N(f.at("anchor").at("end_ms"),start,offset+span+500);
                }
            }
            for(const auto& f:fs) Check(f.at("kind")=="transcript" ? !a.is_null() : kind=="video" && f.at("kind")=="ocr");
            if(kind=="audio") Check(as.empty());
            else {
                const auto& v=c.at("video");Check(Fields(v,{"frames","sample_interval_ms","first_frame_ms","last_frame_ms","track_end_ms"}));
                Check(!as.empty() && v.at("frames")==as.size() && v.at("sample_interval_ms")==5000);
                int64_t previous=-1;
                for(const auto& image:as) {
                    const auto stamp=N(image.at("anchor").at("timestamp_ms"),0,duration+1000);
                    Check(previous<0 || (stamp>previous && stamp-previous<=6000));previous=stamp;
                }
                Check(v.at("first_frame_ms")==as[0].at("anchor").at("timestamp_ms") && v.at("last_frame_ms")==previous);
                const auto end=N(v.at("track_end_ms"),1,duration+1000);Check(previous>=end-6000 && previous<=end);
                latest_end=std::max(latest_end,end);
                first_sample=std::min(first_sample,N(v.at("first_frame_ms"),0,duration));
            }
            // The manifest's span ends at the last actual selected track. It
            // cannot claim a long clip while covering only a shorter prefix.
            Check(latest_end==duration && first_sample<=200);
        }
        return true;
    } catch(const std::exception&) {return false;}
}
std::string MediaManifestDigest(const J& source,const std::string& pipeline,const std::string& parser,
                               const J& coverage,const J& fragments,const J& artifacts) {
    return Sha256Hex(J({{"schema","room-media-publication-v1"},{"source",source},{"pipeline_sha256",pipeline},
        {"parser_sha256",parser},{"coverage",coverage},{"fragments",fragments},{"artifacts",artifacts}}).dump());
}
}  // namespace sparkpush
