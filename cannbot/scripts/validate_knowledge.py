#!/usr/bin/env python3
"""Validate Spark Push's curated Hermes knowledge sources and local links."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path, PurePosixPath


LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
ALLOWED_AUTHORITIES = {"code", "canonical", "snapshot", "historical"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="also require every top-level docs/*.md file to be registered",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="print the registered sources after validation",
    )
    return parser.parse_args()


def is_safe_relative_path(value: str) -> bool:
    path = PurePosixPath(value)
    return bool(value) and not path.is_absolute() and ".." not in path.parts


def validate_manifest(repo_root: Path, manifest_path: Path) -> tuple[list[str], list[dict]]:
    errors: list[str] = []
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"cannot read {manifest_path}: {exc}"], []

    if manifest.get("schema_version") != 1:
        errors.append("knowledge-sources.json: schema_version must be 1")

    sources = manifest.get("sources")
    if not isinstance(sources, list) or not sources:
        return errors + ["knowledge-sources.json: sources must be a non-empty list"], []

    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    valid_sources: list[dict] = []
    for index, source in enumerate(sources):
        label = f"sources[{index}]"
        if not isinstance(source, dict):
            errors.append(f"{label}: must be an object")
            continue

        source_id = source.get("id")
        source_path = source.get("path")
        authority = source.get("authority")
        topics = source.get("topics")
        if not isinstance(source_id, str) or not source_id:
            errors.append(f"{label}: id must be a non-empty string")
        elif source_id in seen_ids:
            errors.append(f"{label}: duplicate id {source_id!r}")
        else:
            seen_ids.add(source_id)

        if not isinstance(source_path, str) or not is_safe_relative_path(source_path):
            errors.append(f"{label}: path must be a safe repository-relative path")
        elif source_path in seen_paths:
            errors.append(f"{label}: duplicate path {source_path!r}")
        else:
            seen_paths.add(source_path)
            resolved = repo_root / source_path
            if not resolved.is_file():
                errors.append(f"{label}: source does not exist: {source_path}")

        if authority not in ALLOWED_AUTHORITIES:
            errors.append(
                f"{label}: authority must be one of {sorted(ALLOWED_AUTHORITIES)}"
            )
        if (
            not isinstance(topics, list)
            or not topics
            or not all(isinstance(topic, str) and topic for topic in topics)
        ):
            errors.append(f"{label}: topics must be a non-empty string list")

        valid_sources.append(source)

    return errors, valid_sources


def validate_markdown_links(repo_root: Path) -> list[str]:
    errors: list[str] = []
    roots = [repo_root / "cannbot"]
    markdown_files = sorted(
        path for root in roots if root.exists() for path in root.rglob("*.md")
    )
    for markdown_path in markdown_files:
        text = markdown_path.read_text(encoding="utf-8")
        for target in LINK_RE.findall(text):
            target = target.strip().split("#", 1)[0]
            if not target or target.startswith(("http://", "https://", "mailto:")):
                continue
            resolved = (markdown_path.parent / target).resolve()
            try:
                resolved.relative_to(repo_root.resolve())
            except ValueError:
                errors.append(
                    f"{markdown_path.relative_to(repo_root)}: link escapes repository: {target}"
                )
                continue
            if not resolved.exists():
                errors.append(
                    f"{markdown_path.relative_to(repo_root)}: broken local link: {target}"
                )
    return errors


def validate_strict_docs(repo_root: Path, sources: list[dict]) -> list[str]:
    registered = {source.get("path") for source in sources}
    docs = {
        path.relative_to(repo_root).as_posix()
        for path in (repo_root / "docs").glob("*.md")
        if path.is_file()
    }
    missing = sorted(docs - registered)
    return [f"unregistered docs source: {path}" for path in missing]


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    manifest_path = repo_root / "cannbot" / "knowledge-sources.json"

    errors, sources = validate_manifest(repo_root, manifest_path)
    errors.extend(validate_markdown_links(repo_root))
    if args.strict:
        errors.extend(validate_strict_docs(repo_root, sources))

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        print(f"knowledge validation failed: {len(errors)} error(s)", file=sys.stderr)
        return 1

    print(f"knowledge validation passed: {len(sources)} source(s)")
    if args.list:
        for source in sources:
            topics = ",".join(source["topics"])
            print(
                f"{source['id']}\t{source['authority']}\t{source['path']}\t{topics}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
