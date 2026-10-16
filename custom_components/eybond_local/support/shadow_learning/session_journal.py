"""Atomic recovery records and historical finalization diagnostics.

A finalization receipt records a local lifecycle decision, never cloud telemetry
availability or settlement of queued commands. It cannot authorize mutations.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
import tempfile


FINALIZATION_OUTCOMES = frozenset({
    "endpoint_verified", "restore_not_required", "start_not_mutated",
    "start_rollback_verified", "explicit_discard",
})


def sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_record(path: Path, payload: dict) -> None:
    """Publish a complete record; failed pre-replace writes keep the old one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def finalization_path(root: Path, *, entry_id: str, collector_pn: str) -> Path:
    key = hashlib.sha256(f"{entry_id}\0{collector_pn}".encode()).hexdigest()
    return root / f"finalization_{key}.json"


def inspect_finalization(root: Path, *, entry_id: str, collector_pn: str) -> dict[str, object]:
    """Export only bounded, allowlisted historical facts for this exact owner."""
    if not entry_id or not collector_pn:
        return {}
    path = finalization_path(root, entry_id=entry_id, collector_pn=collector_pn)
    try:
        with path.open("rb") as stream:
            raw = stream.read(4097)
        if len(raw) > 4096:
            return {}
        record = json.loads(raw)
    except (OSError, ValueError, RecursionError):
        return {}
    if (
        type(record) is not dict
        or record.get("entry_id") != entry_id
        or record.get("collector_pn") != collector_pn
        or record.get("schema_version") != 1
        or type(record.get("outcome")) is not str
        or record["outcome"] not in FINALIZATION_OUTCOMES
    ):
        return {}
    prefix = "shadow_learning_recovery_last_finalization_"
    result: dict[str, object] = {prefix + "outcome": record["outcome"]}
    # Do not export arbitrary contents from a malformed on-disk timestamp.
    stamp = record.get("completed_at")
    if type(stamp) is str and len(stamp) <= 40:
        try:
            parsed = datetime.fromisoformat(stamp)
        except ValueError:
            return result
        if parsed.tzinfo is not None:
            result[prefix + "at"] = parsed.isoformat()
    return result
