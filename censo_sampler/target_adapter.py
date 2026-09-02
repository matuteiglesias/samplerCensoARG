"""Neutralize governed target-population parents for Census-frame sampling."""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any


class TargetPopulationAdapterError(ValueError):
    """Raised when target-population data cannot be consumed safely."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_manifest_payload(
    source_manifest: dict[str, Any], path: Path, payload_sha256: str
) -> None:
    """Verify a governed parent's declared payload hash when one is present."""
    artifacts = source_manifest.get("artifacts")
    if artifacts is None:
        return
    if not isinstance(artifacts, dict):
        raise TargetPopulationAdapterError("target_population_manifest_artifacts_invalid")
    record = artifacts.get(path.name)
    if record is None:
        raise TargetPopulationAdapterError(
            f"target_population_manifest_artifact_missing:{path.name}"
        )
    if not isinstance(record, dict) or not isinstance(record.get("sha256"), str):
        raise TargetPopulationAdapterError(
            f"target_population_manifest_hash_missing:{path.name}"
        )
    if record["sha256"] != payload_sha256:
        raise TargetPopulationAdapterError("target_population_payload_hash_mismatch")


def load_target_population(
    parent: Path, target_year: int
) -> tuple[dict[str, int], dict[str, Any]]:
    """Return neutral ``department_id -> person mass`` for one target year.

    Existing governed parents with ``department_2010_id`` are adapted without
    mutating their payload. A future parent may provide ``department_id``
    directly. When a parent manifest declares artifact hashes, custody is
    verified before any sampling probabilities are computed.
    """
    parent = Path(parent).expanduser().resolve()
    if parent.is_dir():
        path = parent / "target_population.csv"
        manifest_path = parent / "manifest.json"
    else:
        path = parent
        manifest_path = None
    if not path.is_file():
        raise TargetPopulationAdapterError("target_population_payload_missing")

    payload_sha256 = _sha256(path)
    source_manifest: dict[str, Any] | None = None
    manifest_sha256: str | None = None
    if manifest_path is not None and manifest_path.is_file():
        try:
            source_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise TargetPopulationAdapterError(
                "target_population_manifest_invalid"
            ) from exc
        if not isinstance(source_manifest, dict):
            raise TargetPopulationAdapterError("target_population_manifest_invalid")
        _verify_manifest_payload(source_manifest, path, payload_sha256)
        manifest_sha256 = _sha256(manifest_path)

    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        fields = set(reader.fieldnames or [])
        department_field = (
            "department_id" if "department_id" in fields else "department_2010_id"
        )
        required = {department_field, "target_year", "target_person_mass"}
        missing = sorted(required - fields)
        if missing:
            raise TargetPopulationAdapterError(
                "target_population_missing_columns:" + ",".join(missing)
            )
        masses: dict[str, int] = {}
        for row in reader:
            try:
                year = int((row.get("target_year") or "").strip())
            except ValueError as exc:
                raise TargetPopulationAdapterError(
                    "target_population_invalid_year"
                ) from exc
            if year != target_year:
                continue
            department = (row.get(department_field) or "").strip()
            if not department or department in masses:
                raise TargetPopulationAdapterError(
                    f"target_population_duplicate_or_empty_department:{department}"
                )
            try:
                mass = int((row.get("target_person_mass") or "").strip())
            except ValueError as exc:
                raise TargetPopulationAdapterError(
                    f"target_population_invalid_mass:{department}"
                ) from exc
            if mass <= 0:
                raise TargetPopulationAdapterError(
                    f"target_population_nonpositive_mass:{department}"
                )
            masses[department] = mass
    if not masses:
        raise TargetPopulationAdapterError(
            f"target_population_no_rows_for_year:{target_year}"
        )

    return masses, {
        "source_path": path.name,
        "sha256": payload_sha256,
        "size_bytes": path.stat().st_size,
        "source_department_field": department_field,
        "department_alignment_policy": "assume-code-identity/v1",
        "source_manifest_sha256": manifest_sha256,
        "source_release_id": (
            source_manifest.get("release_id") if source_manifest else None
        ),
    }


def department_alignment(
    frame_departments: set[str] | list[str], target_departments: set[str] | list[str]
) -> dict[str, list[str]]:
    frame = set(map(str, frame_departments))
    target = set(map(str, target_departments))
    return {
        "matched": sorted(frame & target),
        "frame_only": sorted(frame - target),
        "target_only": sorted(target - frame),
    }


def require_department_alignment(
    frame_departments: set[str] | list[str], target_departments: set[str] | list[str]
) -> dict[str, list[str]]:
    result = department_alignment(frame_departments, target_departments)
    if result["frame_only"] or result["target_only"]:
        raise TargetPopulationAdapterError(
            "department_alignment_mismatch:"
            f"frame_only={result['frame_only']}:target_only={result['target_only']}"
        )
    return result
