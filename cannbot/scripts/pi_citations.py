#!/usr/bin/env python3
"""Turn-scoped CANN citation transform shared by the Pi gateway.

This is the Spark-facing equivalent of the former Hermes cann-advisor
transform_llm_output hook. Tools still come from the Pi extension; the
gateway rewrites [[CANN_REF:...]] tokens, appends a source list, and
emits structured citations for Logic/WebDemo.
"""

from __future__ import annotations

import json
import re
from typing import Any

TOKEN = re.compile(r"\[\[CANN_REF:(ref_[0-9a-f]{16})\]\]")
KNOWLEDGE_TOOLS = {
    "cann_knowledge_search",
    "cann_knowledge_get",
    "cann_knowledge_neighbors",
    "cann_knowledge_status",
}


def _citations_from(value: Any) -> list[dict]:
    found: list[dict] = []
    if isinstance(value, dict):
        citation = value.get("citation")
        if isinstance(citation, dict) and citation.get("citation_id"):
            found.append(citation)
        for child in value.values():
            found.extend(_citations_from(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_citations_from(child))
    return found


def parse_tool_payload(result: Any) -> dict:
    if isinstance(result, dict):
        if "content" in result and isinstance(result["content"], list):
            texts: list[str] = []
            for part in result["content"]:
                if isinstance(part, str):
                    texts.append(part)
                elif isinstance(part, dict) and part.get("type") == "text":
                    texts.append(str(part.get("text") or ""))
            joined = "".join(texts).strip()
            if joined:
                try:
                    parsed = json.loads(joined)
                    if isinstance(parsed, dict):
                        return parsed
                except json.JSONDecodeError:
                    return {"raw": joined}
        return result
    if isinstance(result, str):
        try:
            parsed = json.loads(result)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {"raw": result}
    return {}


def source_line(number: int, citation: dict) -> str:
    locator = citation.get("locator") or {}
    where = f"{locator.get('type', 'section')} {locator.get('value', '')}".strip()
    revision = str(citation.get("source_revision") or "")[:12]
    suffix = f" · {citation.get('authority', 'unknown')}"
    if revision:
        suffix += f" · {revision}"
    title = citation.get("title", "未命名资料")
    uri = citation.get("uri", "")
    return f"[{number}] [{title} — {where}]({uri}){suffix}"


class TurnCitationState:
    def __init__(self) -> None:
        self.refs: dict[str, dict] = {}
        self.generation: Any = None
        self.evidence: dict | None = None
        self.tools: list[str] = []
        self.read_coverage: dict[str, dict] = {}

    def _observe_coverage(self, value: Any) -> None:
        if isinstance(value, list):
            for child in value:
                self._observe_coverage(child)
            return
        if not isinstance(value, dict):
            return
        citation, coverage = value.get("citation"), value.get("coverage")
        if isinstance(citation, dict) and isinstance(coverage, dict):
            ref = citation.get("citation_id")
            if isinstance(ref, str):
                previous = self.read_coverage.setdefault(ref, {"ranges": [], "total": None})
                if coverage.get("preview_only") is not True:
                    start, end, total = (coverage.get(k) for k in ("start", "end", "total"))
                    digest = coverage.get("source_hash")
                    valid = (coverage.get("unit") == "unicode_codepoint" and
                             all(type(n) is int for n in (start, end, total)) and
                             0 <= start <= end <= total and isinstance(digest, str) and
                             re.fullmatch(r"[0-9a-f]{64}", digest))
                    if valid:
                        if previous.get("hash") != digest or previous.get("total") != total:
                            previous = {"hash": digest, "total": total, "ranges": []}
                            self.read_coverage[ref] = previous
                        previous["ranges"].append((start, end))
        for child in value.values():
            self._observe_coverage(child)

    def _read_complete(self, ref: str) -> bool:
        coverage = self.read_coverage.get(ref)
        if coverage is None:
            return True  # Compatibility with pre-pagination tool contracts.
        if coverage["total"] is None:
            return False
        reached = 0
        for start, end in sorted(coverage["ranges"]):
            if start > reached:
                return False
            reached = max(reached, end)
        return reached >= coverage["total"]

    def observe_tool(self, tool_name: str, result: Any) -> None:
        if tool_name not in KNOWLEDGE_TOOLS:
            return
        payload = parse_tool_payload(result)
        self.tools.append(tool_name)
        self._observe_coverage(payload)
        if payload.get("generation"):
            self.generation = payload["generation"]
        if isinstance(payload.get("evidence"), dict):
            current = payload["evidence"]
            previous = self.evidence or {}
            if previous.get("sufficient") and not current.get("sufficient"):
                pass
            else:
                self.evidence = current
        for citation in _citations_from(payload):
            citation_id = str(citation.get("citation_id") or "")
            if citation_id:
                self.refs[citation_id] = citation

    def transform(self, response_text: str, *, strict_evidence: bool = True) -> dict:
        if not self.tools:
            return {
                "text": response_text,
                "metadata": {
                    "schema_version": "cann-advisor.response.v1",
                    "citations": [],
                    "knowledge_generation": self.generation,
                    "evidence": self.evidence or {},
                    "verified": False,
                    "tools": [],
                },
            }
        ordered: list[dict] = []
        numbers: dict[str, int] = {}

        def replace(match: re.Match[str]) -> str:
            citation_id = match.group(1)
            citation = self.refs.get(citation_id)
            if citation is None:
                return "[引用无效]"
            if citation_id not in numbers:
                numbers[citation_id] = len(ordered) + 1
                ordered.append(citation)
            number = numbers[citation_id]
            return f"[{number}]({citation['uri']})"

        text = TOKEN.sub(replace, response_text)
        evidence = self.evidence or {}
        sufficient = bool(evidence.get("sufficient"))
        status_only = bool(self.tools) and all(
            name == "cann_knowledge_status" for name in self.tools
        )
        if strict_evidence and not status_only and not sufficient and "证据不足" not in text:
            text = text.rstrip() + "\n\n证据不足：当前知识库未达到确定结论所需的证据门槛。"
        unread = [ref for ref in numbers if not self._read_complete(ref)]
        if strict_evidence and unread:
            text = text.rstrip() + "\n\n证据未读全：部分引用仅查看了摘要或不完整片段，参数、限制条件及版本适用性仍需补读核对。"
        if ordered:
            text = text.rstrip() + "\n\n### 资料来源\n\n" + "\n".join(
                source_line(i, citation) for i, citation in enumerate(ordered, 1)
            )
        elif strict_evidence and not status_only and "未包含可验证引用" not in text:
            text = text.rstrip() + "\n\n本回答未包含可验证引用，不能视为已验证结论。"
        return {
            "text": text,
            "metadata": {
                "schema_version": "cann-advisor.response.v1",
                "citations": ordered,
                "knowledge_generation": self.generation,
                "evidence": evidence,
                "verified": bool(status_only or (ordered and sufficient and not unread)),
                "unread_evidence": unread,
                "tools": list(self.tools),
            },
        }
