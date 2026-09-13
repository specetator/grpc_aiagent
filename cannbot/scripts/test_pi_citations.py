#!/usr/bin/env python3
from pi_citations import TurnCitationState


def sample_citation(citation_id="ref_0123456789abcdef"):
    return {
        "schema_version": "cannkb.citation.v1",
        "citation_id": citation_id,
        "doc_id": "doc_" + "a" * 24,
        "chunk_id": "chk_" + "b" * 24,
        "title": "DataCopyPad",
        "uri": "cannkb://document/doc_" + "a" * 24 + "?chunk=chk_" + "b" * 24,
        "authority": "cannbot-curated",
        "source_revision": "abcdef1234567890",
        "content_sha256": "c" * 64,
        "locator": {"type": "heading", "value": "对齐约束"},
        "citation": None,
    }


def test_replaces_token_and_appends_sources():
    state = TurnCitationState()
    citation = sample_citation()
    citation["citation"] = dict(citation)
    state.observe_tool(
        "cann_knowledge_search",
        {
            "content": [
                {
                    "type": "text",
                    "text": '{"evidence":{"sufficient":true},"results":[{"citation":'
                    + __import__("json").dumps(citation)
                    + "}]}",
                }
            ]
        },
    )
    out = state.transform("搬运需对齐 [[CANN_REF:ref_0123456789abcdef]]。")
    assert "cannkb://document/" in out["text"]
    assert "### 资料来源" in out["text"]
    assert out["metadata"]["verified"] is True
    assert out["metadata"]["citations"][0]["citation_id"] == "ref_0123456789abcdef"


def test_invalid_token_and_insufficient_evidence():
    state = TurnCitationState()
    state.observe_tool(
        "cann_knowledge_search",
        {"content": [{"type": "text", "text": '{"evidence":{"sufficient":false},"results":[]}'}]},
    )
    out = state.transform("结论 [[CANN_REF:ref_ffffffffffffffff]]")
    assert "[引用无效]" in out["text"]
    assert "证据不足" in out["text"]
    assert out["metadata"]["verified"] is False


def test_no_tools_leaves_plain_text():
    state = TurnCitationState()
    out = state.transform("pong")
    assert out["text"] == "pong"
    assert out["metadata"]["citations"] == []


def test_status_only_skips_citation_requirement():
    state = TurnCitationState()
    state.observe_tool(
        "cann_knowledge_status",
        {"content": [{"type": "text", "text": '{"generation":"g1","sources":[]}'}]},
    )
    out = state.transform("当前 generation 为 g1。")
    assert "未包含可验证引用" not in out["text"]
    assert out["metadata"]["verified"] is True


def test_latest_user_text_and_session_name():
    from pi_gateway import latest_user_text, session_filename

    text = latest_user_text(
        [
            {"role": "user", "content": "旧问题"},
            {"role": "assistant", "content": "旧回答"},
            {"role": "user", "content": "列出当前目录"},
        ]
    )
    assert text == "列出当前目录"
    assert session_filename("s_1_900000000001", 12) == "s_1_900000000001__12.jsonl"
    assert " " not in session_filename("s 1/../x", 0)


def test_read_coverage_requires_all_ranges_of_cited_revision():
    ref = "ref_0123456789abcdef"
    citation = sample_citation(ref)
    state = TurnCitationState()
    def observe(start, end, digest="a" * 64):
        state.observe_tool("cann_knowledge_get", {"citation": citation,
            "coverage": {"unit": "unicode_codepoint", "source_hash": digest,
                         "start": start, "end": end, "total": 100, "complete": True},
            "evidence": {"sufficient": True}})
    def result():
        return state.transform("约束 [[CANN_REF:" + ref + "]]")
    observe(80, 100)  # Merely reaching the end (or saying complete) is not enough.
    assert not result()["metadata"]["verified"]
    assert "证据未读全" in result()["text"]
    observe(0, 40)
    observe(0, 40)  # Duplicate reads cannot fill gaps.
    assert not result()["metadata"]["verified"]
    observe(40, 80)
    assert result()["metadata"]["verified"]
    assert "证据未读全" not in result()["text"]
    observe(80, 100, "b" * 64)  # New version cannot reuse old prefix.
    assert not result()["metadata"]["verified"]
    observe(0, 80, "b" * 64)
    assert result()["metadata"]["verified"]
    # Unused search previews do not invalidate unrelated complete evidence.
    state.observe_tool("cann_knowledge_neighbors", {"results": [{
        "citation": sample_citation("ref_ffffffffffffffff"),
        "coverage": {"preview_only": True, "complete": False}}]})
    assert result()["metadata"]["verified"]
    unread = state.transform("预览 [[CANN_REF:ref_ffffffffffffffff]]")
    assert not unread["metadata"]["verified"]
    assert unread["metadata"]["unread_evidence"] == ["ref_ffffffffffffffff"]


if __name__ == "__main__":
    test_read_coverage_requires_all_ranges_of_cited_revision()
    test_replaces_token_and_appends_sources()
    test_invalid_token_and_insufficient_evidence()
    test_no_tools_leaves_plain_text()
    test_status_only_skips_citation_requirement()
    test_latest_user_text_and_session_name()
    print("pi citation tests passed")
