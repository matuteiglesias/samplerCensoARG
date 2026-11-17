"""Scientific-use gates for partially observed Census donor frames.

The frame's physical validity is orthogonal to its population coverage.
Explicitly partial frames can be used for bounded commissioning experiments
but must not silently produce an ordinary target-year sample release.
"""
from __future__ import annotations

from typing import Any


class PartialFrameCoverageError(ValueError):
    """A frame's declared source coverage cannot support ordinary sampling."""


def partial_source_coverage(frame_manifest: dict[str, Any]) -> dict[str, Any] | None:
    """Return the declared partial-coverage evidence, or None for legacy frames.

    Do not infer *full* coverage when an older frame lacks this metadata.
    This helper only gates frames explicitly marked incomplete.
    """
    coverage = frame_manifest.get("source_coverage")
    if coverage is None:
        return None
    if not isinstance(coverage, dict):
        raise PartialFrameCoverageError("frame_source_coverage_must_be_object")
    partial = coverage.get("coverage_is_partial")
    if not isinstance(partial, bool):
        raise PartialFrameCoverageError("frame_source_coverage_flag_invalid")
    if not partial:
        return None
    if coverage.get("contract") != "rpo11-frame-coverage/v1":
        raise PartialFrameCoverageError("unknown_partial_frame_coverage_contract")
    attempted = coverage.get("attempted_radios")
    committed = coverage.get("committed_radios")
    included = coverage.get("included_radios")
    missing = coverage.get("needs_local_radios")
    lo, hi = coverage.get("rank_min"), coverage.get("rank_max")
    if (type(attempted) is not int or type(committed) is not int
            or type(lo) is not int or type(hi) is not int
            or lo < 1 or hi - lo + 1 != attempted
            or not 0 < committed <= attempted or not isinstance(included, list)
            or not isinstance(missing, list) or len(included) != committed
            or len(missing) != attempted - committed
            or len(set(included)) != len(included)
            or len(set(missing)) != len(missing)
            or set(included) & set(missing)
            or any(not isinstance(code, str) or len(code) != 9 or not code.isdigit()
                   for code in included + missing)):
        raise PartialFrameCoverageError("inconsistent_partial_frame_coverage")
    return coverage


def require_commissioning_for_partial(frame_manifest: dict[str, Any],
                                      *, commissioning_only: bool) -> dict[str, Any] | None:
    coverage = partial_source_coverage(frame_manifest)
    if coverage is not None and not commissioning_only:
        raise PartialFrameCoverageError("partial_frame_requires_commissioning_only")
    return coverage
