"""Hydrate an existing governed selection-only v2 release from its exact frame.

This module deliberately does not resample. It verifies custody of the exact
frame declared by the selection release, copies the fixed selection/membership
artifacts, and materializes complete relational payload tables by key filters.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

from .frame_contract import CensusFrameError, canonical_json, sha256_file, validate_frame
from .release_v2 import (
    SAMPLE_CONTRACT_V2,
    SampleReleaseV2Error,
    _filter_parquet,
    _iter_parquet,
    _pa,
    validate_sample_release_v2,
)

MATERIALIZATION_OPERATION = "existing-selection-full-payload/v1"


def _load_json(path: Path, error: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SampleReleaseV2Error(error) from exc
    if not isinstance(value, dict):
        raise SampleReleaseV2Error(error)
    return value


def _schema_record(path: Path) -> dict[str, Any]:
    _, _, pq = _pa()
    schema = pq.read_schema(path)
    fields = [
        {
            "name": field.name,
            "type": str(field.type),
            "nullable": bool(field.nullable),
        }
        for field in schema
    ]
    encoded = json.dumps(fields, sort_keys=True, separators=(",", ":")).encode()
    return {
        "fields": fields,
        "fingerprint_sha256": hashlib.sha256(encoded).hexdigest(),
    }


def _require_same_schema(source: Path, output: Path, name: str) -> dict[str, Any]:
    source_record = _schema_record(source)
    output_record = _schema_record(output)
    if source_record != output_record:
        raise SampleReleaseV2Error(f"materialized_payload_schema_mismatch:{name}")
    return {
        "frame_payload": source_record,
        "materialized_payload": output_record,
    }


def materialize_existing_selection_v2(
    frame_root: Path,
    selection_release_root: Path,
    output_root: Path,
) -> Path:
    """Create a governed full-payload child from one fixed selection-only release."""
    frame_root = Path(frame_root).expanduser().resolve()
    selection_release_root = Path(selection_release_root).expanduser().resolve()
    output_root = Path(output_root).expanduser().resolve()

    try:
        frame_info = validate_frame(frame_root, verify_hashes=True, deep=False)
    except CensusFrameError as exc:
        raise SampleReleaseV2Error(str(exc)) from exc
    validate_sample_release_v2(selection_release_root)

    parent_manifest = _load_json(
        selection_release_root / "manifest.json",
        "selection_parent_manifest_invalid",
    )
    parent_qa = _load_json(
        selection_release_root / "qa.json",
        "selection_parent_qa_invalid",
    )
    if parent_manifest.get("contract") != SAMPLE_CONTRACT_V2:
        raise SampleReleaseV2Error("selection_parent_contract_mismatch")
    if parent_manifest.get("materialization") != "selection-only":
        raise SampleReleaseV2Error("selection_parent_must_be_selection_only")

    parent_frame = parent_manifest.get("frame") or {}
    exact_frame_id = str(frame_info["frame_release_id"])
    exact_frame_manifest_sha = sha256_file(frame_root / "manifest.json")
    if parent_frame.get("frame_release_id") != exact_frame_id:
        raise SampleReleaseV2Error("selection_parent_frame_release_id_mismatch")
    if parent_frame.get("manifest_sha256") != exact_frame_manifest_sha:
        raise SampleReleaseV2Error("selection_parent_frame_manifest_sha256_mismatch")

    selection_manifest_sha = sha256_file(selection_release_root / "manifest.json")
    parent_release_id = str(parent_manifest.get("release_id") or "")
    if not parent_release_id:
        raise SampleReleaseV2Error("selection_parent_release_id_missing")
    target_year = int((parent_manifest.get("target_population_parent") or {}).get("target_year"))

    selected_households: set[str] = set()
    selected_dwellings: set[str] = set()
    for row in _iter_parquet(
        selection_release_root / "selection.parquet",
        ["frame_household_id", "frame_dwelling_id"],
    ):
        selected_households.add(str(row["frame_household_id"]))
        selected_dwellings.add(str(row["frame_dwelling_id"]))
    if not selected_households or not selected_dwellings:
        raise SampleReleaseV2Error("selection_parent_empty")

    expected_persons = sum(
        1
        for _ in _iter_parquet(
            selection_release_root / "person_membership.parquet",
            ["frame_person_id"],
        )
    )

    release_identity = {
        "contract": SAMPLE_CONTRACT_V2,
        "operation": MATERIALIZATION_OPERATION,
        "selection_parent_release_id": parent_release_id,
        "selection_parent_manifest_sha256": selection_manifest_sha,
        "frame_release_id": exact_frame_id,
        "frame_manifest_sha256": exact_frame_manifest_sha,
    }
    release_hash = hashlib.sha256(
        json.dumps(release_identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    release_id = f"census-sample-{target_year}-{release_hash[:16]}"
    destination = output_root / release_id
    if destination.exists():
        raise SampleReleaseV2Error(f"immutable_release_exists:{destination}")
    if output_root == frame_root or frame_root in output_root.parents:
        raise SampleReleaseV2Error("unsafe_output_path_inside_frame")
    if output_root == selection_release_root or selection_release_root in output_root.parents:
        raise SampleReleaseV2Error("unsafe_output_path_inside_selection_parent")

    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{release_id}.", dir=output_root))
    try:
        for name in ("selection.parquet", "person_membership.parquet"):
            shutil.copyfile(selection_release_root / name, staging / name)

        counts = {
            "viviendas": _filter_parquet(
                frame_root / "payload/vivienda.parquet",
                staging / "vivienda.parquet",
                key_field="frame_dwelling_id",
                selected_keys=selected_dwellings,
            ),
            "households": _filter_parquet(
                frame_root / "payload/hogar.parquet",
                staging / "hogar.parquet",
                key_field="frame_household_id",
                selected_keys=selected_households,
            ),
            "persons": _filter_parquet(
                frame_root / "payload/persona.parquet",
                staging / "persona.parquet",
                key_field="frame_household_id",
                selected_keys=selected_households,
            ),
        }
        if counts["households"] != len(selected_households):
            raise SampleReleaseV2Error("materialized_household_count_mismatch")
        if counts["persons"] != expected_persons:
            raise SampleReleaseV2Error("materialized_person_count_mismatch")

        schema_custody = {
            "vivienda.parquet": _require_same_schema(
                frame_root / "payload/vivienda.parquet",
                staging / "vivienda.parquet",
                "vivienda.parquet",
            ),
            "hogar.parquet": _require_same_schema(
                frame_root / "payload/hogar.parquet",
                staging / "hogar.parquet",
                "hogar.parquet",
            ),
            "persona.parquet": _require_same_schema(
                frame_root / "payload/persona.parquet",
                staging / "persona.parquet",
                "persona.parquet",
            ),
        }

        qa = deepcopy(parent_qa)
        qa["materialization"] = "full-payload"
        qa["materialized_counts"] = counts
        selected_counts = dict(qa.get("selected_counts") or {})
        selected_counts["dwellings"] = len(selected_dwellings)
        qa["selected_counts"] = selected_counts
        qa["materialization_lineage"] = {
            "operation": MATERIALIZATION_OPERATION,
            "selection_parent_release_id": parent_release_id,
            "selection_parent_manifest_sha256": selection_manifest_sha,
            "selection_reused_without_resampling": True,
            "schema_custody_status": "pass",
        }
        (staging / "qa.json").write_text(canonical_json(qa), encoding="utf-8")

        artifact_names = [
            "selection.parquet",
            "person_membership.parquet",
            "vivienda.parquet",
            "hogar.parquet",
            "persona.parquet",
            "qa.json",
        ]
        artifacts = {
            name: {
                "sha256": sha256_file(staging / name),
                "size_bytes": (staging / name).stat().st_size,
            }
            for name in artifact_names
        }

        manifest = deepcopy(parent_manifest)
        manifest["release_id"] = release_id
        manifest["materialization"] = "full-payload"
        manifest["artifacts"] = artifacts
        manifest["qa"] = qa
        manifest["materialization_lineage"] = {
            "operation": MATERIALIZATION_OPERATION,
            "selection_parent_release_id": parent_release_id,
            "selection_parent_manifest_sha256": selection_manifest_sha,
            "selection_artifacts_reused": {
                name: {
                    "parent_sha256": sha256_file(selection_release_root / name),
                    "child_sha256": sha256_file(staging / name),
                    "byte_identical": sha256_file(selection_release_root / name)
                    == sha256_file(staging / name),
                }
                for name in ("selection.parquet", "person_membership.parquet")
            },
            "frame_manifest_sha256": exact_frame_manifest_sha,
            "frame_hashes_verified_before_materialization": True,
        }
        manifest["payload_schema_custody"] = schema_custody
        (staging / "manifest.json").write_text(canonical_json(manifest), encoding="utf-8")

        validate_sample_release_v2(staging)
        staging.replace(destination)
        return destination
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
