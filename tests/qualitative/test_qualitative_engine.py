"""Functional, recovery, idempotency, triangulation, and concurrency tests."""
import threading

import pytest

from acp_engine.qualitative import (
    QualitativeProcessingError,
    QualitativeResearchEngine,
)


def seed(engine):
    engine.add_evidence(
        event_id="ev1", evidence_id="e1", source_id="interview-1",
        source_type="interview", excerpt="Retries amplified the incident.",
    )
    engine.add_evidence(
        event_id="ev2", evidence_id="e2", source_id="incident-report",
        source_type="report", excerpt="The retry policy increased request volume.",
    )
    engine.add_evidence(
        event_id="ev3", evidence_id="e3", source_id="interview-2",
        source_type="interview", excerpt="Backoff was missing in the client.",
    )
    engine.assign_code(
        event_id="c1", assignment_id="a1", evidence_id="e1",
        code="retry-amplification", coder_id="researcher-1", stance="support",
    )
    engine.assign_code(
        event_id="c2", assignment_id="a2", evidence_id="e2",
        code="retry-amplification", coder_id="researcher-1", stance="support",
    )
    engine.assign_code(
        event_id="c3", assignment_id="a3", evidence_id="e3",
        code="retry-amplification", coder_id="researcher-2", stance="contradict",
    )
    engine.create_theme(
        event_id="t1", theme_id="theme-1", name="Retry behavior",
        description="How client retries contributed to service degradation.",
    )
    engine.link_code_to_theme(
        event_id="l1", theme_id="theme-1", code="retry-amplification",
    )


def test_evidence_coding_and_triangulation(tmp_path):
    engine = QualitativeResearchEngine(tmp_path / "qualitative.wal")
    seed(engine)
    result = engine.triangulate("theme-1")
    assert result["source_count"] == 3
    assert result["supporting_sources"] == ["incident-report", "interview-1"]
    assert result["contradicting_sources"] == ["interview-2"]
    assert result["status"] == "mixed_evidence"
    assert engine.snapshot() == {
        "sequence": 8, "evidence_records": 3, "code_assignments": 3,
        "themes": 1, "theme_code_links": 1,
    }


def test_idempotency_and_conflicting_event_id(tmp_path):
    engine = QualitativeResearchEngine(tmp_path / "idempotent.wal")
    first = engine.add_evidence(
        event_id="same", evidence_id="e1", source_id="s1",
        source_type="interview", excerpt="First statement",
    )
    second = engine.add_evidence(
        event_id="same", evidence_id="e1", source_id="s1",
        source_type="interview", excerpt="First statement",
    )
    assert first["duplicate"] is False
    assert second["duplicate"] is True
    with pytest.raises(QualitativeProcessingError):
        engine.add_evidence(
            event_id="same", evidence_id="e2", source_id="s2",
            source_type="interview", excerpt="Different statement",
        )


def test_unknown_evidence_and_invalid_stance_rejected(tmp_path):
    engine = QualitativeResearchEngine(tmp_path / "validation.wal")
    with pytest.raises(QualitativeProcessingError):
        engine.assign_code(
            event_id="c1", assignment_id="a1", evidence_id="missing",
            code="x", coder_id="r1",
        )
    engine.add_evidence(
        event_id="e1", evidence_id="e1", source_id="s1",
        source_type="interview", excerpt="Evidence",
    )
    with pytest.raises(QualitativeProcessingError):
        engine.assign_code(
            event_id="c2", assignment_id="a2", evidence_id="e1",
            code="x", coder_id="r1", stance="maybe",
        )


def test_wal_recovery_reconstructs_workspace(tmp_path):
    path = tmp_path / "recovery.wal"
    engine = QualitativeResearchEngine(path)
    seed(engine)
    recovered = QualitativeResearchEngine(path)
    assert recovered.snapshot() == engine.snapshot()
    assert recovered.triangulate("theme-1") == engine.triangulate("theme-1")


def test_incomplete_final_wal_record_is_ignored(tmp_path):
    path = tmp_path / "partial.wal"
    engine = QualitativeResearchEngine(path)
    engine.add_evidence(
        event_id="e1", evidence_id="e1", source_id="s1",
        source_type="interview", excerpt="Committed evidence",
    )
    with path.open("ab") as stream:
        stream.write(b'{"sequence":')
    recovered = QualitativeResearchEngine(path)
    assert recovered.snapshot()["evidence_records"] == 1


def test_concurrent_evidence_ingestion_is_thread_safe(tmp_path):
    engine = QualitativeResearchEngine(tmp_path / "concurrent.wal")
    errors = []

    def add(index):
        try:
            engine.add_evidence(
                event_id=f"event-{index}", evidence_id=f"evidence-{index}",
                source_id=f"source-{index % 5}", source_type="interview",
                excerpt=f"Evidence excerpt {index}",
            )
        except Exception as exc:  # surfaced after joining all workers
            errors.append(exc)

    workers = [threading.Thread(target=add, args=(i,)) for i in range(200)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    assert not errors
    assert engine.snapshot()["evidence_records"] == 200
    assert engine.snapshot()["sequence"] == 200


def test_triangulation_requires_multiple_sources(tmp_path):
    engine = QualitativeResearchEngine(tmp_path / "single-source.wal")
    engine.add_evidence(
        event_id="e1", evidence_id="e1", source_id="only-source",
        source_type="interview", excerpt="One observation",
    )
    engine.assign_code(
        event_id="c1", assignment_id="a1", evidence_id="e1",
        code="theme-code", coder_id="r1", stance="support",
    )
    engine.create_theme(event_id="t1", theme_id="t1", name="A theme")
    engine.link_code_to_theme(event_id="l1", theme_id="t1", code="theme-code")
    assert engine.triangulate("t1")["status"] == "insufficient_sources"
