"""Stricter draft qualification after a previous command could have been sent.

Separate traces/cursors prevent reuse of old records, not late wire delivery.
This check rejects known ambiguity; it is NOT proof of a drained cloud queue.
"""

from collections import defaultdict
from copy import deepcopy
import json


def qualify_retry_evidence(result: dict) -> dict:
    result = dict(result)
    correlation = deepcopy(result.get("correlation") or {})
    groups = defaultdict(list)
    for item in correlation.get("matched", []):
        groups[item.get("field_id", "")].append(item)
    ambiguous = {item.get("field_id") for item in result.get("results", [])
                 if item.get("observation_count", 0) > 1}
    extra_writes = bool(correlation.get("unmatched_writes"))
    qualified, rejected = [], []
    for field, items in groups.items():
        targets, requested, observed = set(), set(), set()
        for item in items:
            wire = item.get("observation", {})
            targets.add(tuple(wire.get(k) for k in
                ("protocol", "command", "function_code", "devcode", "devaddr", "unit", "register")))
            requested.add(str(item.get("requested_value", "")))
            observed.add(json.dumps(wire.get("values") or wire.get("value", ""), sort_keys=True))
        valid = (bool(field) and not extra_writes and field not in ambiguous
                 and len(targets) == 1 and len(requested) >= 2 and len(observed) >= 2)
        if valid:
            qualified.extend(items)
        else:
            rejected.extend({**item, "reason": "protected_retry_correlation_unconfirmed"} for item in items)
    correlation["matched"] = qualified
    correlation["matched_count"] = len(qualified)
    correlation["retry_quarantined"] = rejected
    correlation["retry_quarantined_count"] = len(rejected)
    result["correlation"] = correlation
    result["protected_retry"] = True
    return result
