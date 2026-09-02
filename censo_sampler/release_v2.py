"""Vintage-neutral Census-frame sample release v2."""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from .frame_contract import CensusFrameError, canonical_json, sha256_file, validate_frame
from .selection import SELECTION_ALGORITHM, SelectionError, select_households
from .target_adapter import (
    TargetPopulationAdapterError,
    load_target_population,
    require_department_alignment,
)

SAMPLE_CONTRACT_V2 = "research.census-target-year-sample/v2"
MATERIALIZATION_MODES = {"selection-only", "full-payload"}


class SampleReleaseV2Error(ValueError):
    """Raised when a vintage-neutral sample release cannot be built or checked."""


def _pa():
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise SampleReleaseV2Error("pyarrow_required_for_sample_v2") from exc
    return pa, pc, pq


def _frame_namespace(frame_release_id: str) -> str:
    digest = hashlib.sha256(frame_release_id.encode()).hexdigest()[:16]
    return digest


def sample_id(frame_release_id: str, unit: str, frame_id: str) -> str:
    return f"census:{_frame_namespace(frame_release_id)}:{unit}:{frame_id}"


def _iter_parquet(path: Path, columns: list[str], batch_size: int = 65536) -> Iterator[dict[str, Any]]:
    _, _, pq = _pa()
    parquet = pq.ParquetFile(path)
    missing = sorted(set(columns) - set(parquet.schema_arrow.names))
    if missing:
        raise SampleReleaseV2Error(
            f"{path.name}:missing_required_columns:{','.join(missing)}"
        )
    for batch in parquet.iter_batches(columns=columns, batch_size=batch_size):
        values = batch.to_pydict()
        for idx in range(batch.num_rows):
            yield {name: values[name][idx] for name in columns}


def _write_pylist(path: Path, rows: list[dict[str, Any]]) -> None:
    pa, _, pq = _pa()
    if not rows:
        raise SampleReleaseV2Error(f"cannot_write_empty_artifact:{path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression="zstd")


def _filter_parquet(
    source: Path,
    destination: Path,
    *,
    key_field: str,
    selected_keys: set[str],
    batch_size: int = 65536,
) -> int:
    pa, pc, pq = _pa()
    parquet = pq.ParquetFile(source)
    if key_field not in parquet.schema_arrow.names:
        raise SampleReleaseV2Error(f"{source.name}:missing_key:{key_field}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    count = 0
    value_set = pa.array(sorted(selected_keys), type=pa.string())
    try:
        for batch in parquet.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            mask = pc.is_in(table[key_field], value_set=value_set)
            filtered = table.filter(mask)
            if filtered.num_rows == 0:
                continue
            if writer is None:
                writer = pq.ParquetWriter(
                    destination, filtered.schema, compression="zstd"
                )
            writer.write_table(filtered)
            count += filtered.num_rows
        if writer is None:
            raise SampleReleaseV2Error(
                f"materialized_payload_empty:{destination.name}"
            )
    finally:
        if writer is not None:
            writer.close()
    return count


def _load_donor_mass(frame_root: Path) -> dict[str, int]:
    masses: dict[str, int] = {}
    for row in _iter_parquet(
        frame_root / "donor_person_mass.parquet",
        ["department_id", "donor_person_mass"],
    ):
        department = str(row["department_id"])
        masses[department] = int(row["donor_person_mass"])
    return masses


def _frame_households(frame_root: Path):
    yield from _iter_parquet(
        frame_root / "frame_households.parquet",
        [
            "frame_household_id",
            "frame_dwelling_id",
            "department_id",
            "radio_id",
            "household_person_count",
        ],
    )


def build_sample_release_v2(
    frame_root: Path,
    output_root: Path,
    *,
    target_population: Path,
    target_year: int,
    fraction: float = 0.01,
    seed: int = 20260831,
    materialization: str = "full-payload",
) -> Path:
    """Sample a prepared Census frame using one vintage-neutral algorithm."""
    if materialization not in MATERIALIZATION_MODES:
        raise SampleReleaseV2Error(
            f"unsupported_materialization:{materialization}"
        )
    frame_root = Path(frame_root).expanduser().resolve()
    output_root = Path(output_root).expanduser().resolve()
    try:
        frame_info = validate_frame(frame_root, verify_hashes=True, deep=False)
        target_mass, target_meta = load_target_population(target_population, target_year)
        donor_mass = _load_donor_mass(frame_root)
        alignment = require_department_alignment(set(donor_mass), set(target_mass))
        selected = select_households(
            _frame_households(frame_root),
            donor_person_mass=donor_mass,
            target_person_mass=target_mass,
            fraction=fraction,
            seed=seed,
        )
    except (CensusFrameError, TargetPopulationAdapterError, SelectionError) as exc:
        raise SampleReleaseV2Error(str(exc)) from exc

    frame_release_id = str(frame_info["frame_release_id"])
    frame_manifest_hash = sha256_file(frame_root / "manifest.json")
    release_identity = {
        "contract": SAMPLE_CONTRACT_V2,
        "frame_release_id": frame_release_id,
        "frame_manifest_sha256": frame_manifest_hash,
        "target_population_sha256": target_meta["sha256"],
        "target_year": target_year,
        "fraction": fraction,
        "seed": seed,
        "selection_algorithm": SELECTION_ALGORITHM,
        "materialization": materialization,
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

    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{release_id}.", dir=output_root))
    try:
        selection_rows: list[dict[str, Any]] = []
        selected_households: set[str] = set()
        selected_dwellings: set[str] = set()
        expected_persons = 0
        for row in selected:
            hh = row["frame_household_id"]
            dwelling = row["frame_dwelling_id"]
            selected_households.add(hh)
            selected_dwellings.add(dwelling)
            expected_persons += int(row["household_person_count"])
            selection_rows.append(
                {
                    "sample_household_id": sample_id(
                        frame_release_id, "household", hh
                    ),
                    **row,
                }
            )
        _write_pylist(staging / "selection.parquet", selection_rows)

        person_membership: list[dict[str, Any]] = []
        seen_people: set[str] = set()
        member_counts: Counter[str] = Counter()
        for row in _iter_parquet(
            frame_root / "payload/persona.parquet",
            ["frame_person_id", "frame_household_id"],
        ):
            hh = str(row["frame_household_id"])
            if hh not in selected_households:
                continue
            person = str(row["frame_person_id"])
            if person in seen_people:
                raise SampleReleaseV2Error(
                    f"duplicate_selected_frame_person_id:{person}"
                )
            seen_people.add(person)
            member_counts[hh] += 1
            person_membership.append(
                {
                    "sample_person_id": sample_id(frame_release_id, "person", person),
                    "frame_person_id": person,
                    "sample_household_id": sample_id(
                        frame_release_id, "household", hh
                    ),
                    "frame_household_id": hh,
                }
            )
        person_membership.sort(key=lambda row: row["frame_person_id"])
        if len(person_membership) != expected_persons:
            raise SampleReleaseV2Error(
                f"selected_person_membership_count_mismatch:{len(person_membership)}!={expected_persons}"
            )
        expected_by_hh = {
            row["frame_household_id"]: int(row["household_person_count"])
            for row in selected
        }
        if dict(member_counts) != expected_by_hh:
            raise SampleReleaseV2Error("incomplete_selected_household_membership")
        _write_pylist(staging / "person_membership.parquet", person_membership)

        materialized_counts: dict[str, int] | None = None
        if materialization == "full-payload":
            materialized_counts = {
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
            if materialized_counts["households"] != len(selected_households):
                raise SampleReleaseV2Error("materialized_household_count_mismatch")
            if materialized_counts["persons"] != len(person_membership):
                raise SampleReleaseV2Error("materialized_person_count_mismatch")

        selected_person_by_department: Counter[str] = Counter()
        selected_household_by_department: Counter[str] = Counter()
        for row in selected:
            department = row["department_id"]
            selected_household_by_department[department] += 1
            selected_person_by_department[department] += int(row["household_person_count"])
        total_target = sum(target_mass.values())
        total_selected_persons = len(person_membership)
        department_qa: dict[str, Any] = {}
        for department in sorted(donor_mass):
            probability = fraction * target_mass[department] / donor_mass[department]
            target_share = target_mass[department] / total_target
            realized_share = (
                selected_person_by_department[department] / total_selected_persons
            )
            department_qa[department] = {
                "donor_person_mass": donor_mass[department],
                "target_person_mass": target_mass[department],
                "selection_probability": probability,
                "expected_selected_person_mass": fraction * target_mass[department],
                "selected_households": selected_household_by_department[department],
                "selected_persons": selected_person_by_department[department],
                "target_person_share": target_share,
                "realized_person_share": realized_share,
                "realized_minus_target_person_share": realized_share - target_share,
            }

        qa = {
            "frame_vintage": frame_info["census_vintage"],
            "sampling_target_year": target_year,
            "selection_unit": "household",
            "target_mass_unit": "person",
            "complete_household_membership": True,
            "selected_counts": {
                "households": len(selection_rows),
                "persons": len(person_membership),
                **({"dwellings": len(selected_dwellings)} if materialization == "full-payload" else {}),
            },
            "department_alignment": alignment,
            "departments": department_qa,
            "materialization": materialization,
            "materialized_counts": materialized_counts,
        }
        (staging / "qa.json").write_text(canonical_json(qa), encoding="utf-8")

        artifact_names = ["selection.parquet", "person_membership.parquet", "qa.json"]
        if materialization == "full-payload":
            artifact_names.extend(["vivienda.parquet", "hogar.parquet", "persona.parquet"])
        artifacts = {
            name: {
                "sha256": sha256_file(staging / name),
                "size_bytes": (staging / name).stat().st_size,
            }
            for name in artifact_names
        }
        manifest = {
            "contract": SAMPLE_CONTRACT_V2,
            "release_id": release_id,
            "frame": {
                "frame_release_id": frame_release_id,
                "census_vintage": frame_info["census_vintage"],
                "source_release_id": frame_info["manifest"].get("source_release_id"),
                "manifest_sha256": frame_manifest_hash,
            },
            "target_population_parent": {
                "target_year": target_year,
                **target_meta,
            },
            "selection": {
                "algorithm": SELECTION_ALGORITHM,
                "unit": "household",
                "target_mass_unit": "person",
                "fraction": fraction,
                "seed": seed,
                "formula": "p[d,y] = fraction * target_person_mass[d,y] / donor_person_mass[d]",
                "common_score_across_target_years": True,
                "probability_bound_policy": "fail_on_overflow",
                "score_frame_namespace": False,
                "score_migration_note": "Legacy CPV-2010 score bytes are preserved for exact migration parity; frame identity namespaces release/sample IDs instead.",
            },
            "weight_semantics": {
                "selection_probability": "household inclusion probability under target-year sampling design",
                "design_inverse_probability_weight": "1 / selection_probability; donor-frame design diagnostic only",
                "analysis_weight": None,
                "generic_sample_weight": None,
            },
            "department_alignment_policy": "assume-code-identity/v1",
            "materialization": materialization,
            "artifacts": artifacts,
            "qa": qa,
            "scientific_assumptions": [
                f"Selected records remain Census-{frame_info['census_vintage']} donor households and persons.",
                "Only relative department person mass is intentionally updated by the target-year parent.",
                "Within-department joint distributions remain inherited from the donor Census frame.",
                "Feature selection and EPH semantic alignment are downstream concerns.",
            ],
        }
        (staging / "manifest.json").write_text(canonical_json(manifest), encoding="utf-8")
        staging.replace(destination)
        return destination
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def validate_sample_release_v2(root: Path) -> dict[str, Any]:
    root = Path(root).expanduser().resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        qa = json.loads((root / "qa.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SampleReleaseV2Error("sample_v2_manifest_or_qa_invalid") from exc
    if manifest.get("contract") != SAMPLE_CONTRACT_V2:
        raise SampleReleaseV2Error("unexpected_sample_v2_contract")
    artifacts = manifest.get("artifacts") or {}
    for name, record in artifacts.items():
        path = root / name
        if not path.is_file():
            raise SampleReleaseV2Error(f"sample_v2_artifact_missing:{name}")
        if record.get("sha256") != sha256_file(path):
            raise SampleReleaseV2Error(f"sample_v2_artifact_hash_mismatch:{name}")

    selections = list(
        _iter_parquet(
            root / "selection.parquet",
            [
                "sample_household_id",
                "frame_household_id",
                "frame_dwelling_id",
                "department_id",
                "selection_probability",
                "design_inverse_probability_weight",
                "household_person_count",
            ],
        )
    )
    household_ids: set[str] = set()
    frame_households: set[str] = set()
    expected_members = 0
    for row in selections:
        sample_hh = str(row["sample_household_id"])
        frame_hh = str(row["frame_household_id"])
        if not sample_hh or sample_hh in household_ids or not frame_hh or frame_hh in frame_households:
            raise SampleReleaseV2Error("sample_v2_duplicate_household_identity")
        household_ids.add(sample_hh)
        frame_households.add(frame_hh)
        p = float(row["selection_probability"])
        w = float(row["design_inverse_probability_weight"])
        if not (0 < p <= 1) or not math.isclose(w, 1 / p, rel_tol=1e-12, abs_tol=1e-12):
            raise SampleReleaseV2Error("sample_v2_invalid_probability_semantics")
        expected_members += int(row["household_person_count"])

    persons: set[str] = set()
    member_counts: Counter[str] = Counter()
    for row in _iter_parquet(
        root / "person_membership.parquet",
        ["sample_person_id", "frame_person_id", "sample_household_id", "frame_household_id"],
    ):
        sample_person = str(row["sample_person_id"])
        if not sample_person or sample_person in persons:
            raise SampleReleaseV2Error("sample_v2_duplicate_person_identity")
        if str(row["sample_household_id"]) not in household_ids:
            raise SampleReleaseV2Error("sample_v2_orphan_person_household")
        if str(row["frame_household_id"]) not in frame_households:
            raise SampleReleaseV2Error("sample_v2_orphan_frame_household")
        persons.add(sample_person)
        member_counts[str(row["frame_household_id"])] += 1
    if len(persons) != expected_members:
        raise SampleReleaseV2Error("sample_v2_incomplete_household_membership")

    counts = qa.get("selected_counts") or {}
    if counts.get("households") != len(selections) or counts.get("persons") != len(persons):
        raise SampleReleaseV2Error("sample_v2_qa_count_mismatch")
    semantics = manifest.get("weight_semantics") or {}
    if semantics.get("analysis_weight") is not None or semantics.get("generic_sample_weight") is not None:
        raise SampleReleaseV2Error("sample_v2_forbidden_analysis_weight")
    return {
        "contract": SAMPLE_CONTRACT_V2,
        "status": "valid",
        "release_id": manifest.get("release_id"),
        "frame_vintage": (manifest.get("frame") or {}).get("census_vintage"),
        "households": len(selections),
        "persons": len(persons),
        "materialization": manifest.get("materialization"),
    }
