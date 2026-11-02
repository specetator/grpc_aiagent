#include "services/media_manifest.h"
#include <iostream>
#include <stdexcept>
using namespace sparkpush;
using J=nlohmann::json;
void Require(bool v,const char* why) { if(!v) throw std::runtime_error(why); }
int main() {
    const auto empty=J::array();
    J picture=J::array({{{"anchor",{{"image",1}}}}});
    Require(ValidateMediaCoverage("image/png",{{"kind","image"},{"images",1}},empty,picture),"empty OCR image");
    Require(!ValidateMediaCoverage("image/png",{{"kind","image"},{"images",1}},empty,empty),"missing visual");
    J ocr={{"kind","ocr"},{"text","真实文字"},{"anchor",{{"image",1},{"artifact",0},{"part",0}}}};
    Require(ValidateMediaFragment(ocr),"OCR fragment");
    Require(ValidateMediaCoverage("image/png",{{"kind","image"},{"images",1}},J::array({ocr}),picture),"OCR reference");
    ocr["anchor"]["artifact"]=1;
    Require(!ValidateMediaCoverage("image/png",{{"kind","image"},{"images",1}},J::array({ocr}),picture),"cross artifact reference");
    J pdf={{"kind","pdf"},{"page_count",2},{"rendered_pages",2}};
    J pages=J::array({{{"anchor",{{"page",1}}}},{{"anchor",{{"page",2}}}}});
    Require(ValidateMediaCoverage("application/pdf",pdf,empty,pages),"PDF pages");pages[1]["anchor"]["page"]=1;
    Require(!ValidateMediaCoverage("application/pdf",pdf,empty,pages),"duplicate PDF page");
    J speech={{"kind","transcript"},{"text","late decision"},{"anchor",{{"start_ms",3000},{"end_ms",4000},{"part",0}}}};
    J audio={{"decoded_samples",80000},{"sample_rate",16000},{"duration_ms",5000},{"offset_ms",0},
        {"language","en"},{"segments",1},{"speaker_labels",false},{"track_end_ms",5000}};
    J coverage={{"kind","audio"},{"duration_ms",5000},{"container_duration_ms",5000},{"source_origin_ms",0},{"audio",audio}};
    Require(ValidateMediaCoverage("audio/wav",coverage,J::array({speech}),empty),"audio timing");
    auto bad=coverage;bad["duration_ms"]=20000;
    Require(!ValidateMediaCoverage("audio/wav",bad,J::array({speech}),empty),"short prefix declared full duration");
    bad=coverage;bad["audio"]["decoded_samples"]=1;
    Require(!ValidateMediaCoverage("audio/wav",bad,J::array({speech}),empty),"false decoded sample span");
    bad=coverage;bad["runtime_url"]="http://untrusted";
    Require(!ValidateMediaCoverage("audio/wav",bad,J::array({speech}),empty),"unexpected coverage field");
    speech["anchor"]["end_ms"]=9000;
    Require(!ValidateMediaCoverage("audio/wav",coverage,J::array({speech}),empty),"transcript outside audio");
    J video=coverage;video["kind"]="video";video["duration_ms"]=10000;video["container_duration_ms"]=10000;
    video["video"]={{"frames",2},{"sample_interval_ms",5000},{"first_frame_ms",2000},
        {"last_frame_ms",7000},{"track_end_ms",10000}};
    J frames=J::array({{{"anchor",{{"timestamp_ms",2000}}}},{{"anchor",{{"timestamp_ms",7000}}}}});
    Require(ValidateMediaCoverage("video/mp4",video,empty,frames),"audio first, delayed video remains valid");
    video["audio"]["offset_ms"]=1000;video["audio"]["track_end_ms"]=6000;
    Require(!ValidateMediaCoverage("video/mp4",video,empty,frames),"all selected tracks omit normalized start");
    video["audio"]=nullptr;video["video"]["first_frame_ms"]=0;frames[0]["anchor"]["timestamp_ms"]=0;
    video["video"]["last_frame_ms"]=5000;frames[1]["anchor"]["timestamp_ms"]=5000;
    Require(ValidateMediaCoverage("video/mp4",video,empty,frames),"silent video at normalized origin");
    std::cout<<"media coverage, references, empty text, source spans and field boundaries passed\n";
}
