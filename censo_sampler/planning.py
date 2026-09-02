"""Dry-run planning for vintage-neutral Census samples."""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from .frame_contract import CensusFrameError, validate_frame
from .target_adapter import (
    TargetPopulationAdapterError,
    department_alignment,
    load_target_population,
)


class SamplePlanningError(ValueError):
    """Raised when a sample plan cannot be evaluated safely."""


def _load_donor_mass(frame_root: Path) -> dict[str, int]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise SamplePlanningError("pyarrow_required_for_sample_plan") from exc
    path = frame_root / "donor_person_mass.parquet"
    table = pq.read_table(path, columns=["department_id", "donor_person_mass"])
    masses: dict[str, int] = {}
    for row in table.to_pylist():
        department = str(row["department_id"])
        mass = int(row["donor_person_mass"])
        if not department or department in masses or mass <= 0:
            raise SamplePlanningError("invalid_frame_donor_person_mass")
        masses[department] = mass
    return masses


def plan_sample(
    frame_root: Path,
    *,
    target_population: Path,
    target_year: int,
    fraction: float,
) -> dict[str, Any]:
    """Inspect alignment/probabilities without selecting or writing Census rows."""
    if not (0 < fraction <= 1):
        raise SamplePlanningError("fraction_must_be_in_(0,1]")
    frame_root = Path(frame_root).expanduser().resolve()
    try:
        frame = validate_frame(frame_root, verify_hashes=True, deep=False)
        target_mass, target_meta = load_target_population(target_population, target_year)
    except (CensusFrameError, TargetPopulationAdapterError) as exc:
        raise SamplePlanningError(str(exc)) from exc
    donor_mass = _load_donor_mass(frame_root)
    alignment = department_alignment(set(donor_mass), set(target_mass))

    rows: list[dict[str, Any]] = []
    overflow: list[str] = []
    for department in sorted(set(donor_mass) | set(target_mass)):
        donor = donor_mass.get(department)
        target = target_mass.get(department)
        probability = None
        expected_selected_person_mass = None
        status = "aligned"
        if donor is None:
            status = "target-only"
        elif target is None:
            status = "frame-only"
        else:
            probability = fraction * target / donor
            expected_selected_person_mass = fraction * target
            if not math.isfinite(probability) or probability <= 0:
                status = "invalid-probability"
            elif probability > 1:
                status = "probability-overflow"
                overflow.append(department)
        rows.append(
            {
                "department_id": department,
                "donor_person_mass": donor,
                "target_person_mass": target,
                "selection_probability": probability,
                "expected_selected_person_mass": expected_selected_person_mass,
                "status": status,
            }
        )

    aligned = not alignment["frame_only"] and not alignment["target_only"]
    valid = aligned and not overflow and all(row["status"] == "aligned" for row in rows)
    probabilities = [
        float(row["selection_probability"])
        for row in rows
        if row["selection_probability"] is not None
    ]
    return {
        "contract": "research.census-sample-plan/v1",
        "status": "ready" if valid else "blocked",
        "frame_release_id": frame["frame_release_id"],
        "census_vintage": frame["census_vintage"],
        "target_year": target_year,
        "fraction": fraction,
        "department_alignment_policy": "assume-code-identity/v1",
        "department_alignment": alignment,
        "probability_overflow_departments": overflow,
        "probability_range": (
            [min(probabilities), max(probabilities)] if probabilities else [None, None]
        ),
        "total_donor_person_mass": sum(donor_mass.values()),
        "total_target_person_mass": sum(target_mass.values()),
        "expected_selected_person_mass": fraction * sum(target_mass.values()),
        "target_population": target_meta,
        "departments": rows,
    }
