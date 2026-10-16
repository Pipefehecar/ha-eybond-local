#!/usr/bin/env python3
"""Maintainer-only reviewed recovery for an OLD, proven-unsent first request.

This records an audit receipt, not a forced endpoint change or cloud-job expiry.
Do not use for a lost response, partial send, earlier command, or unknown build.
New builds obtain the same proof from the owned HTTP resource automatically.
"""

import argparse
import hashlib
from pathlib import Path
import sys


def record_review(*, config_dir: Path, collector_pn: str, study_id: str,
                  evidence_file: Path, confirmed: bool) -> str:
    from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
    from custom_components.eybond_local.support.shadow_learning.session import load_shadow_learning_session_state

    if confirmed is not True:
        raise ValueError("explicit_no_request_review_required")
    store = CloudLearningProtectionStore(config_dir)
    hold = store.load(collector_pn)
    state = load_shadow_learning_session_state(config_dir)
    if (hold is None or state is None or hold.study_id != study_id
            or hold.source not in {"smartess", "dessmonitor"}
            or state.protection_version != 1 or state.status != "restore_failed"
            or state.entry_id != hold.entry_id or state.collector_pn != hold.collector_pn
            or state.cloud_source != hold.source
            or state.route_owner_id != "shadow_learning:" + hold.study_id
            or not state.cloud_commands_possible):
        raise ValueError("reviewed_learning_scope_mismatch")
    with evidence_file.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if evidence_file.stat().st_size == 0:
        raise ValueError("review_evidence_empty")
    store.record_no_request(hold, evidence_sha256=digest)
    return digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", type=Path, required=True)
    parser.add_argument("--collector-pn", required=True)
    parser.add_argument("--study-id", required=True)
    parser.add_argument("--evidence-file", type=Path, required=True)
    parser.add_argument("--confirm-no-http-request-sent", action="store_true")
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    sys.path.insert(0, str(args.config_dir))
    digest = record_review(config_dir=args.config_dir, collector_pn=args.collector_pn,
                           study_id=args.study_id, evidence_file=args.evidence_file,
                           confirmed=args.confirm_no_http_request_sent)
    print("No-request review receipt recorded; evidence SHA256=" + digest)
    print("Use normal HA connection recovery/restart. No endpoint was changed by this tool.")


if __name__ == "__main__":
    main()
