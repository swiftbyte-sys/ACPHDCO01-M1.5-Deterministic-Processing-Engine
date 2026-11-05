"""Deterministic qualitative evidence and thematic-analysis engine.

The engine stores source evidence, human-authored code assignments, themes,
and code-to-theme links in an append-only JSONL event log. Every accepted
mutation is fsync'd before it is published in memory. This is a single-process
educational implementation, not a multi-host distributed database.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any
import hashlib
import json
import os


class QualitativeProcessingError(ValueError):
    """Invalid evidence, coding, theme, or event-log operation."""


@dataclass(frozen=True)
class EvidenceRecord:
    evidence_id: str
    source_id: str
    source_type: str
    excerpt: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class CodeAssignment:
    assignment_id: str
    evidence_id: str
    code: str
    coder_id: str
    stance: str
    memo: str = ""


@dataclass(frozen=True)
class Theme:
    theme_id: str
    name: str
    description: str = ""


class QualitativeResearchEngine:
    """Thread-safe, event-sourced qualitative research workspace.

    Supported event types:
      * evidence_added
      * code_assigned
      * theme_created
      * code_linked_to_theme

    Code assignment remains human-authored. The engine organizes and audits
    interpretation; it does not claim to automate valid qualitative judgment.
    """

    VALID_STANCES = {"support", "contradict", "neutral"}

    def __init__(self, wal_path: str | Path, *, fsync: bool = True):
        self.wal_path = Path(wal_path)
        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync
        self._lock = RLock()
        self._evidence: dict[str, EvidenceRecord] = {}
        self._assignments: dict[str, CodeAssignment] = {}
        self._themes: dict[str, Theme] = {}
        self._theme_codes: dict[str, set[str]] = {}
        self._event_fingerprints: dict[str, str] = {}
        self._sequence = 0
        self._recover()

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    @staticmethod
    def _required_text(value: Any, field: str, limit: int = 512) -> str:
        if not isinstance(value, str) or not value.strip() or len(value) > limit:
            raise QualitativeProcessingError(
                f"{field} must be non-empty text of at most {limit} characters"
            )
        return value.strip()

    @staticmethod
    def _canonical(value: Any) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)

    def _apply(self, event: dict[str, Any]) -> None:
        kind, payload = event["type"], event["payload"]
        if kind == "evidence_added":
            item = EvidenceRecord(**payload)
            self._evidence[item.evidence_id] = item
        elif kind == "code_assigned":
            item = CodeAssignment(**payload)
            self._assignments[item.assignment_id] = item
        elif kind == "theme_created":
            item = Theme(**payload)
            self._themes[item.theme_id] = item
            self._theme_codes[item.theme_id] = set()
        elif kind == "code_linked_to_theme":
            self._theme_codes[payload["theme_id"]].add(payload["code"])
        else:
            raise QualitativeProcessingError(f"unknown event type: {kind}")

    def _recover(self) -> None:
        if not self.wal_path.exists():
            self.wal_path.touch()
            return
        valid_bytes = 0
        with self.wal_path.open("rb") as stream:
            lines = stream.readlines()
        for index, line in enumerate(lines):
            if not line.endswith(b"\n"):
                if index == len(lines) - 1:
                    # Remove a torn final record before future appends; otherwise
                    # later valid JSON would be concatenated to corrupt bytes.
                    with self.wal_path.open("r+b") as repair:
                        repair.truncate(valid_bytes)
                        repair.flush()
                        if self.fsync:
                            os.fsync(repair.fileno())
                    break
                raise QualitativeProcessingError(f"incomplete WAL record at line {index + 1}")
            try:
                event = json.loads(line)
                sequence = int(event["sequence"])
                if sequence != self._sequence + 1:
                    raise QualitativeProcessingError("non-contiguous event sequence")
                event_id = event["event_id"]
                fingerprint = hashlib.sha256(
                    self._canonical({"type": event["type"], "payload": event["payload"]}).encode("utf-8")
                ).hexdigest()
                if event_id in self._event_fingerprints:
                    raise QualitativeProcessingError("duplicate event ID in WAL")
                self._apply(event)
                self._event_fingerprints[event_id] = fingerprint
                self._sequence = sequence
                valid_bytes += len(line)
            except Exception as exc:
                raise QualitativeProcessingError(
                    f"WAL recovery failed at line {index + 1}: {exc}"
                ) from exc

    def _commit(self, event_id: str, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        event_id = self._required_text(event_id, "event_id", 128)
        fingerprint = hashlib.sha256(
            self._canonical({"type": kind, "payload": payload}).encode("utf-8")
        ).hexdigest()
        with self._lock:
            previous = self._event_fingerprints.get(event_id)
            if previous is not None:
                if previous != fingerprint:
                    raise QualitativeProcessingError(
                        "event_id was already used for a different operation"
                    )
                return {"event_id": event_id, "sequence": None, "duplicate": True}

            event = {
                "sequence": self._sequence + 1,
                "event_id": event_id,
                "timestamp": self._utc_now(),
                "type": kind,
                "payload": payload,
            }
            encoded = (self._canonical(event) + "\n").encode("utf-8")
            with self.wal_path.open("ab", buffering=0) as stream:
                view = memoryview(encoded)
                while view:
                    written = stream.write(view)
                    if not written:
                        raise OSError("short write while appending qualitative WAL")
                    view = view[written:]
                if self.fsync:
                    os.fsync(stream.fileno())

            self._apply(event)
            self._sequence += 1
            self._event_fingerprints[event_id] = fingerprint
            return {"event_id": event_id, "sequence": self._sequence, "duplicate": False}

    def add_evidence(
        self, *, event_id: str, evidence_id: str, source_id: str,
        source_type: str, excerpt: str, metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        evidence_id = self._required_text(evidence_id, "evidence_id", 128)
        source_id = self._required_text(source_id, "source_id", 128)
        source_type = self._required_text(source_type, "source_type", 64)
        excerpt = self._required_text(excerpt, "excerpt", 20000)
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict):
            raise QualitativeProcessingError("metadata must be a JSON object")
        try:
            metadata = json.loads(self._canonical(metadata))  # detach caller-owned nested objects
        except (TypeError, ValueError) as exc:
            raise QualitativeProcessingError("metadata must be JSON serializable") from exc
        with self._lock:
            existing = self._evidence.get(evidence_id)
            candidate = EvidenceRecord(evidence_id, source_id, source_type, excerpt, metadata)
            if existing is not None and existing != candidate:
                raise QualitativeProcessingError("evidence_id already exists with different content")
            if existing is not None:
                return {"evidence_id": evidence_id, "duplicate": True, "sequence": None}
            result = self._commit(event_id, "evidence_added", asdict(candidate))
            return {"evidence_id": evidence_id, **result}

    def assign_code(
        self, *, event_id: str, assignment_id: str, evidence_id: str,
        code: str, coder_id: str, stance: str = "neutral", memo: str = "",
    ) -> dict[str, Any]:
        assignment_id = self._required_text(assignment_id, "assignment_id", 128)
        evidence_id = self._required_text(evidence_id, "evidence_id", 128)
        code = self._required_text(code, "code", 256)
        coder_id = self._required_text(coder_id, "coder_id", 128)
        if stance not in self.VALID_STANCES:
            raise QualitativeProcessingError(f"stance must be one of {sorted(self.VALID_STANCES)}")
        if not isinstance(memo, str) or len(memo) > 4000:
            raise QualitativeProcessingError("memo must be text of at most 4000 characters")
        with self._lock:
            if evidence_id not in self._evidence:
                raise QualitativeProcessingError("cannot code unknown evidence_id")
            candidate = CodeAssignment(
                assignment_id, evidence_id, code, coder_id, stance, memo
            )
            existing = self._assignments.get(assignment_id)
            if existing is not None:
                if existing != candidate:
                    raise QualitativeProcessingError("assignment_id already exists with different content")
                return {"assignment_id": assignment_id, "duplicate": True, "sequence": None}
            result = self._commit(event_id, "code_assigned", asdict(candidate))
            return {"assignment_id": assignment_id, **result}

    def create_theme(
        self, *, event_id: str, theme_id: str, name: str, description: str = "",
    ) -> dict[str, Any]:
        theme_id = self._required_text(theme_id, "theme_id", 128)
        name = self._required_text(name, "name", 256)
        if not isinstance(description, str) or len(description) > 4000:
            raise QualitativeProcessingError("description must be text of at most 4000 characters")
        with self._lock:
            candidate = Theme(theme_id, name, description)
            existing = self._themes.get(theme_id)
            if existing is not None:
                if existing != candidate:
                    raise QualitativeProcessingError("theme_id already exists with different content")
                return {"theme_id": theme_id, "duplicate": True, "sequence": None}
            result = self._commit(event_id, "theme_created", asdict(candidate))
            return {"theme_id": theme_id, **result}

    def link_code_to_theme(
        self, *, event_id: str, theme_id: str, code: str,
    ) -> dict[str, Any]:
        theme_id = self._required_text(theme_id, "theme_id", 128)
        code = self._required_text(code, "code", 256)
        with self._lock:
            if theme_id not in self._themes:
                raise QualitativeProcessingError("unknown theme_id")
            if not any(item.code == code for item in self._assignments.values()):
                raise QualitativeProcessingError("cannot link a code with no assignments")
            if code in self._theme_codes[theme_id]:
                return {"theme_id": theme_id, "code": code, "duplicate": True, "sequence": None}
            result = self._commit(
                event_id, "code_linked_to_theme", {"theme_id": theme_id, "code": code}
            )
            return {"theme_id": theme_id, "code": code, **result}

    def triangulate(self, theme_id: str) -> dict[str, Any]:
        """Compare linked coded evidence across independent source IDs.

        This is an evidence-coverage summary, not an automatic truth judgment.
        Multiple excerpts from one source count as one source for corroboration.
        """
        with self._lock:
            if theme_id not in self._themes:
                raise QualitativeProcessingError("unknown theme_id")
            linked_codes = self._theme_codes[theme_id]
            by_source: dict[str, dict[str, Any]] = {}
            for assignment in self._assignments.values():
                if assignment.code not in linked_codes:
                    continue
                evidence = self._evidence[assignment.evidence_id]
                source = by_source.setdefault(evidence.source_id, {
                    "source_type": evidence.source_type,
                    "stances": set(),
                    "evidence_ids": set(),
                    "codes": set(),
                })
                source["stances"].add(assignment.stance)
                source["evidence_ids"].add(evidence.evidence_id)
                source["codes"].add(assignment.code)

            support_sources = sorted(
                source_id for source_id, item in by_source.items()
                if "support" in item["stances"]
            )
            contradict_sources = sorted(
                source_id for source_id, item in by_source.items()
                if "contradict" in item["stances"]
            )
            all_sources = sorted(by_source)
            if len(all_sources) < 2:
                status = "insufficient_sources"
            elif support_sources and contradict_sources:
                status = "mixed_evidence"
            elif len(support_sources) >= 2:
                status = "cross_source_support"
            else:
                status = "inconclusive"

            source_details = {
                source_id: {
                    "source_type": item["source_type"],
                    "stances": sorted(item["stances"]),
                    "evidence_ids": sorted(item["evidence_ids"]),
                    "codes": sorted(item["codes"]),
                }
                for source_id, item in sorted(by_source.items())
            }
            return {
                "theme_id": theme_id,
                "theme": asdict(self._themes[theme_id]),
                "status": status,
                "source_count": len(all_sources),
                "supporting_sources": support_sources,
                "contradicting_sources": contradict_sources,
                "sources": source_details,
                "interpretation_limit": (
                    "This report summarizes coded evidence coverage. Researchers must "
                    "review source quality, context, coding consistency, and alternative explanations."
                ),
            }

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "sequence": self._sequence,
                "evidence_records": len(self._evidence),
                "code_assignments": len(self._assignments),
                "themes": len(self._themes),
                "theme_code_links": sum(len(codes) for codes in self._theme_codes.values()),
            }

    def list_evidence(self) -> list[dict[str, Any]]:
        with self._lock:
            return [asdict(item) for _, item in sorted(self._evidence.items())]

    def list_assignments(self) -> list[dict[str, Any]]:
        with self._lock:
            return [asdict(item) for _, item in sorted(self._assignments.items())]
