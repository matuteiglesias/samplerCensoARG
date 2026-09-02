"""Vintage-neutral deterministic household-selection kernel."""
from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Mapping
from typing import Any

SELECTION_ALGORITHM = "sha256-household-department/v1"


class SelectionError(ValueError):
    """Raised when a Census-frame selection cannot be formed safely."""


def household_score(seed: int, frame_household_id: str, department_id: str) -> float:
    """Return the legacy-compatible deterministic household score.

    The score intentionally preserves the current CPV-2010 algorithm so the
    vintage-neutral migration can prove exact scientific parity. Frame identity
    namespaces *sample IDs* and release identity, not the pseudo-random score.
    Target year is deliberately absent, preserving common random numbers across
    2024/2025 for one donor frame.
    """
    payload = f"{seed}\x1f{frame_household_id}\x1f{department_id}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest(), "big") / 2**256


def selection_probabilities(
    donor_person_mass: Mapping[str, int],
    target_person_mass: Mapping[str, int],
    fraction: float,
) -> dict[str, float]:
    if not (0 < fraction <= 1):
        raise SelectionError("fraction_must_be_in_(0,1]")
    donor_departments = set(map(str, donor_person_mass))
    target_departments = set(map(str, target_person_mass))
    if donor_departments != target_departments:
        raise SelectionError(
            "department_alignment_mismatch:"
            f"frame_only={sorted(donor_departments-target_departments)}:"
            f"target_only={sorted(target_departments-donor_departments)}"
        )
    probabilities: dict[str, float] = {}
    for department in sorted(donor_departments):
        donor = int(donor_person_mass[department])
        target = int(target_person_mass[department])
        if donor <= 0 or target <= 0:
            raise SelectionError(f"nonpositive_mass:{department}")
        probability = fraction * target / donor
        if not math.isfinite(probability) or probability <= 0:
            raise SelectionError(f"invalid_selection_probability:{department}")
        if probability > 1:
            raise SelectionError(
                f"selection_probability_overflow:{department}:{probability:.12g}"
            )
        probabilities[department] = probability
    return probabilities


def select_households(
    households: Iterable[Mapping[str, Any]],
    *,
    donor_person_mass: Mapping[str, int],
    target_person_mass: Mapping[str, int],
    fraction: float,
    seed: int,
) -> list[dict[str, Any]]:
    probabilities = selection_probabilities(
        donor_person_mass, target_person_mass, fraction
    )
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in households:
        household_id = str(row.get("frame_household_id") or "")
        department = str(row.get("department_id") or "")
        if not household_id or household_id in seen:
            raise SelectionError(
                f"duplicate_or_empty_frame_household_id:{household_id}"
            )
        seen.add(household_id)
        if department not in probabilities:
            raise SelectionError(f"household_department_not_in_target:{department}")
        score = household_score(seed, household_id, department)
        probability = probabilities[department]
        if score < probability:
            selected.append(
                {
                    "frame_household_id": household_id,
                    "frame_dwelling_id": str(row.get("frame_dwelling_id") or ""),
                    "department_id": department,
                    "radio_id": str(row.get("radio_id") or ""),
                    "household_person_count": int(row.get("household_person_count") or 0),
                    "selection_probability": probability,
                    "design_inverse_probability_weight": 1.0 / probability,
                    "selection_score": score,
                }
            )
    if not selected:
        raise SelectionError("deterministic_selection_empty")
    selected.sort(key=lambda row: row["frame_household_id"])
    return selected
