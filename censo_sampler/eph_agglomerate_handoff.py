"""Attach official EPH agglomerate identity to an already-fixed Census sample.

This is a geography handoff only. It does not alter household selection,
selection probabilities, weights, substantive Census payloads, or model inputs.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .frame_contract import canonical_json, sha256_file
from .release_v2 import validate_sample_release_v2

HANDOFF_CONTRACT = "research.census-eph-agglomerate-handoff/v1"
A7_DATASET_ID = "arggeo.indec.eph.census2010.radio_frame"
G1_DATASET_ID = "arggeo.indec.eph.census2010.agglomerate-footprint"


class EphAgglomerateHandoffError(ValueError):
    pass


def _require(frame: pd.DataFrame, fields: set[str], label: str) -> None:
    missing = sorted(fields - set(frame.columns))
    if missing:
        raise EphAgglomerateHandoffError(
            f"{label}:missing_required_columns:{','.join(missing)}"
        )


def _load_a7_frame(root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    root = Path(root).expanduser().resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EphAgglomerateHandoffError("a7_manifest_missing_or_invalid") from exc
    dataset = manifest.get("dataset") or {}
    if dataset.get("dataset_id") != A7_DATASET_ID:
        raise EphAgglomerateHandoffError("unexpected_a7_dataset")
    artifacts = manifest.get("artifacts") or {}
    frame_name = artifacts.get("frame")
    if not isinstance(frame_name, str):
        raise EphAgglomerateHandoffError("a7_frame_artifact_missing")
    frame_path = root / frame_name
    expected_hash = (manifest.get("content_sha256") or {}).get("frame")
    if not isinstance(expected_hash, str) or sha256_file(frame_path) != expected_hash:
        raise EphAgglomerateHandoffError("a7_frame_hash_mismatch")
    frame = pd.read_parquet(frame_path)
    return frame, manifest


def _load_g1_relation(root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    root = Path(root).expanduser().resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EphAgglomerateHandoffError("g1_manifest_missing_or_invalid") from exc
    dataset = manifest.get("dataset") or {}
    if dataset.get("dataset_id") != G1_DATASET_ID:
        raise EphAgglomerateHandoffError("unexpected_g1_dataset")
    membership = manifest.get("membership") or {}
    relation_name = membership.get("artifact")
    expected_hash = membership.get("content_sha256")
    if not isinstance(relation_name, str) or not isinstance(expected_hash, str):
        raise EphAgglomerateHandoffError("g1_membership_artifact_missing")
    relation_path = root / relation_name
    if not relation_path.is_file() or sha256_file(relation_path) != expected_hash:
        raise EphAgglomerateHandoffError("g1_membership_hash_mismatch")
    frame = pd.read_parquet(relation_path)
    required = {
        "radio_2010_id",
        "department_2010_id",
        "province_2010_id",
        "eph_agglomerate_id",
    }
    _require(frame, required, "g1_relation")
    if frame["radio_2010_id"].duplicated().any():
        raise EphAgglomerateHandoffError("g1_radio_identity_not_unique")
    if membership.get("spatial_inference") is not False:
        raise EphAgglomerateHandoffError("g1_membership_must_be_direct")
    return frame, manifest


def _load_geography_parent(
    *,
    a7_release: Path | None = None,
    g1_release: Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    if (a7_release is None) == (g1_release is None):
        raise EphAgglomerateHandoffError(
            "exactly_one_of_a7_release_or_g1_release_is_required"
        )
    if g1_release is not None:
        frame, manifest = _load_g1_relation(g1_release)
        membership = manifest.get("membership") or {}
        provenance = {
            "dataset_id": G1_DATASET_ID,
            "release_version": (manifest.get("dataset") or {}).get("version"),
            "manifest_sha256": sha256_file(g1_release / "manifest.json"),
            "direct_mapping_sha256": membership.get("relation_sha256"),
            "relation_artifact_sha256": membership.get("content_sha256"),
            "parent_kind": "G1_first_class_agglomerate",
        }
        return frame, manifest, provenance
    frame, manifest = _load_a7_frame(a7_release)
    provenance = {
        "dataset_id": A7_DATASET_ID,
        "release_version": (manifest.get("dataset") or {}).get("version"),
        "manifest_sha256": sha256_file(a7_release / "manifest.json"),
        "direct_mapping_sha256": (
            (manifest.get("run") or {}).get("parameters") or {}
        ).get("radio_to_agglomerate_relation_sha256"),
        "parent_kind": "A7_legacy_radio_frame",
    }
    return frame, manifest, provenance


def derive_household_agglomerates(
    selection: pd.DataFrame,
    a7_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    _require(
        selection,
        {
            "sample_household_id",
            "frame_household_id",
            "department_id",
            "radio_id",
            "household_person_count",
            "selection_probability",
            "design_inverse_probability_weight",
        },
        "selection",
    )
    _require(
        a7_frame,
        {
            "radio_2010_id",
            "department_2010_id",
            "province_2010_id",
            "eph_agglomerate_id",
        },
        "a7_frame",
    )
    if selection["sample_household_id"].duplicated().any():
        raise EphAgglomerateHandoffError("selection_household_identity_not_unique")
    if a7_frame["radio_2010_id"].duplicated().any():
        raise EphAgglomerateHandoffError("a7_radio_identity_not_unique")

    sample = selection.copy()
    for field in ("sample_household_id", "frame_household_id", "department_id", "radio_id"):
        sample[field] = sample[field].astype(str)
    if not sample["radio_id"].str.fullmatch(r"[0-9]{9}").all():
        raise EphAgglomerateHandoffError("selection_radio_id_must_be_9_digits")
    if not sample["department_id"].str.fullmatch(r"[0-9]{5}").all():
        raise EphAgglomerateHandoffError("selection_department_id_must_be_5_digits")
    if not sample["radio_id"].str[:5].eq(sample["department_id"]).all():
        raise EphAgglomerateHandoffError("selection_radio_department_identity_mismatch")

    a7 = a7_frame[
        [
            "radio_2010_id",
            "department_2010_id",
            "province_2010_id",
            "eph_agglomerate_id",
        ]
    ].copy()
    for field in a7.columns:
        a7[field] = a7[field].astype(str)
    if not a7["radio_2010_id"].str.fullmatch(r"[0-9]{9}").all():
        raise EphAgglomerateHandoffError("a7_radio_id_must_be_9_digits")
    if not a7["eph_agglomerate_id"].str.fullmatch(r"[0-9]{2}").all():
        raise EphAgglomerateHandoffError("a7_agglomerate_id_must_be_2_digits")

    joined = sample.merge(
        a7,
        left_on="radio_id",
        right_on="radio_2010_id",
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    if len(joined) != len(sample):
        raise EphAgglomerateHandoffError("geography_join_changed_household_count")

    mapped = joined["_merge"].eq("both")
    if not joined.loc[mapped, "department_id"].eq(
        joined.loc[mapped, "department_2010_id"]
    ).all():
        raise EphAgglomerateHandoffError("mapped_radio_department_identity_mismatch")

    joined["mapped_to_eph_frame"] = mapped
    joined["outside_eph_frame"] = ~mapped
    joined["eph_agglomerate_id"] = joined["eph_agglomerate_id"].where(mapped, None)
    joined["eph_frame_province_2010_id"] = joined["province_2010_id"].where(mapped, None)
    joined["radio_2010_id"] = joined["radio_id"]
    joined["selected_person_mass"] = pd.to_numeric(
        joined["household_person_count"], errors="raise"
    )
    joined["design_inverse_probability_weight"] = pd.to_numeric(
        joined["design_inverse_probability_weight"], errors="raise"
    )
    joined["selection_probability"] = pd.to_numeric(
        joined["selection_probability"], errors="raise"
    )
    if (joined["selected_person_mass"] <= 0).any():
        raise EphAgglomerateHandoffError("household_person_count_must_be_positive")
    if (
        (joined["selection_probability"] <= 0)
        | (joined["selection_probability"] > 1)
        | ~joined["selection_probability"].map(math.isfinite)
    ).any():
        raise EphAgglomerateHandoffError("selection_probability_invalid")
    joined["design_person_mass"] = (
        joined["selected_person_mass"] * joined["design_inverse_probability_weight"]
    )

    represented = sorted(
        joined.loc[mapped, "eph_agglomerate_id"].dropna().astype(str).unique().tolist()
    )
    per_agglomerate: dict[str, Any] = {}
    for aglo, group in joined.loc[mapped].groupby("eph_agglomerate_id", sort=True):
        per_agglomerate[str(aglo)] = {
            "selected_households": int(len(group)),
            "selected_persons": int(group["selected_person_mass"].sum()),
            "design_person_mass": float(group["design_person_mass"].sum()),
        }

    outside = joined.loc[~mapped]
    qa = {
        "input_households": int(len(sample)),
        "output_households": int(len(joined)),
        "zero_households_dropped": len(joined) == len(sample),
        "mapped_households": int(mapped.sum()),
        "outside_eph_frame_households": int((~mapped).sum()),
        "mapped_selected_persons": int(joined.loc[mapped, "selected_person_mass"].sum()),
        "outside_eph_frame_selected_persons": int(
            outside["selected_person_mass"].sum()
        ),
        "mapped_design_person_mass": float(joined.loc[mapped, "design_person_mass"].sum()),
        "outside_eph_frame_design_person_mass": float(
            outside["design_person_mass"].sum()
        ),
        "represented_eph_agglomerate_ids": represented,
        "represented_eph_agglomerate_count": len(represented),
        "redistribution_applied": False,
        "imputation_applied": False,
        "selection_changed": False,
        "weight_changed": False,
        "design_mass_semantics": (
            "household_person_count * design_inverse_probability_weight; "
            "donor-frame design diagnostic only, not target-year agglomerate calibration"
        ),
        "per_agglomerate": per_agglomerate,
    }

    output_fields = [
        "sample_household_id",
        "frame_household_id",
        "radio_2010_id",
        "department_id",
        "eph_frame_province_2010_id",
        "eph_agglomerate_id",
        "mapped_to_eph_frame",
        "outside_eph_frame",
        "household_person_count",
        "selection_probability",
        "design_inverse_probability_weight",
        "selected_person_mass",
        "design_person_mass",
    ]
    return joined[output_fields].sort_values("sample_household_id", ignore_index=True), qa


def materialize(
    sample_release: Path,
    output: Path,
    *,
    a7_release: Path | None = None,
    g1_release: Path | None = None,
) -> Path:
    sample_release = Path(sample_release).expanduser().resolve()
    a7_release = (
        Path(a7_release).expanduser().resolve() if a7_release is not None else None
    )
    g1_release = (
        Path(g1_release).expanduser().resolve() if g1_release is not None else None
    )
    output = Path(output).expanduser().resolve()
    checked = validate_sample_release_v2(sample_release)
    if checked["frame_vintage"] != 2010:
        raise EphAgglomerateHandoffError(
            "A7 Census-2010 EPH geography can only attach to a Census-2010 donor frame"
        )
    if output.exists() and any(output.iterdir()):
        raise EphAgglomerateHandoffError("output_directory_must_be_empty")
    output.mkdir(parents=True, exist_ok=True)

    selection = pd.read_parquet(sample_release / "selection.parquet")
    geography_frame, geography_manifest, geography_parent = _load_geography_parent(
        a7_release=a7_release,
        g1_release=g1_release,
    )
    households, qa = derive_household_agglomerates(selection, geography_frame)

    household_path = output / "household_geography.parquet"
    pq.write_table(pa.Table.from_pandas(households, preserve_index=False), household_path)

    patch = [
        {
            "household_id": row.sample_household_id,
            "eph_agglomerate_id": (
                None if pd.isna(row.eph_agglomerate_id) else str(row.eph_agglomerate_id)
            ),
            "mapped_to_eph_frame": bool(row.mapped_to_eph_frame),
        }
        for row in households.itertuples()
    ]
    (output / "population_frame_geography_patch.json").write_text(
        canonical_json(
            {
                "schema_version": "research.population-frame-geography-patch/v1",
                "household_key": "household_id",
                "geography_field": "eph_agglomerate_id",
                "rows": patch,
            }
        ),
        encoding="utf-8",
    )
    (output / "qa.json").write_text(canonical_json(qa), encoding="utf-8")

    sample_manifest_sha = sha256_file(sample_release / "manifest.json")
    geography_manifest_sha = geography_parent["manifest_sha256"]
    artifacts = {}
    for name in (
        "household_geography.parquet",
        "population_frame_geography_patch.json",
        "qa.json",
    ):
        path = output / name
        artifacts[name] = {
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        }

    manifest = {
        "contract": HANDOFF_CONTRACT,
        "status": "geography_only",
        "sample_parent": {
            "release_id": checked["release_id"],
            "manifest_sha256": sample_manifest_sha,
            "frame_vintage": checked["frame_vintage"],
        },
        "geography_parent": geography_parent,
        "join": {
            "left_field": "selection.radio_id",
            "right_field": "governed_geography_relation.radio_2010_id",
            "mode": "left_many_to_one",
            "membership_semantics": "direct_official_A7_relation_exposed_by_G1_or_A7",
            "unmatched_policy": "preserve_row_and_set_eph_agglomerate_id_null",
        },
        "scientific_scope": {
            "sampling_changed": False,
            "weights_changed": False,
            "model_changed": False,
            "poverty_region_added": False,
            "agglomerate_population_calibration": False,
        },
        "qa": qa,
        "artifacts": artifacts,
    }
    (output / "manifest.json").write_text(canonical_json(manifest), encoding="utf-8")
    checksum_names = [
        "household_geography.parquet",
        "population_frame_geography_patch.json",
        "qa.json",
        "manifest.json",
    ]
    (output / "checksums.sha256").write_text(
        "".join(f"{sha256_file(output / name)}  {name}\n" for name in checksum_names),
        encoding="utf-8",
    )
    verify_handoff(output)
    return output


def verify_handoff(root: Path) -> dict[str, Any]:
    root = Path(root).expanduser().resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        qa = json.loads((root / "qa.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EphAgglomerateHandoffError("handoff_manifest_or_qa_invalid") from exc
    if manifest.get("contract") != HANDOFF_CONTRACT:
        raise EphAgglomerateHandoffError("unexpected_handoff_contract")
    for name, record in (manifest.get("artifacts") or {}).items():
        path = root / name
        if not path.is_file() or sha256_file(path) != record.get("sha256"):
            raise EphAgglomerateHandoffError(f"handoff_artifact_invalid:{name}")
    frame = pd.read_parquet(root / "household_geography.parquet")
    if frame["sample_household_id"].duplicated().any():
        raise EphAgglomerateHandoffError("handoff_duplicate_household")
    if len(frame) != qa.get("input_households") or len(frame) != qa.get("output_households"):
        raise EphAgglomerateHandoffError("handoff_household_count_mismatch")
    if qa.get("zero_households_dropped") is not True:
        raise EphAgglomerateHandoffError("handoff_must_not_drop_households")
    if qa.get("redistribution_applied") is not False or qa.get("imputation_applied") is not False:
        raise EphAgglomerateHandoffError("handoff_cannot_redistribute_or_impute")
    mapped = frame["mapped_to_eph_frame"].astype(bool)
    if frame.loc[mapped, "eph_agglomerate_id"].isna().any():
        raise EphAgglomerateHandoffError("mapped_household_missing_agglomerate")
    if frame.loc[~mapped, "eph_agglomerate_id"].notna().any():
        raise EphAgglomerateHandoffError("outside_household_has_agglomerate")
    values = frame.loc[mapped, "eph_agglomerate_id"].astype(str)
    if not values.str.fullmatch(r"[0-9]{2}").all():
        raise EphAgglomerateHandoffError("handoff_agglomerate_id_format_invalid")
    return {
        "contract": HANDOFF_CONTRACT,
        "status": "valid",
        "households": len(frame),
        "mapped_households": int(mapped.sum()),
        "outside_eph_frame_households": int((~mapped).sum()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("materialize")
    build.add_argument("--sample-release", type=Path, required=True)
    parent_group = build.add_mutually_exclusive_group(required=True)
    parent_group.add_argument("--a7-release", type=Path)
    parent_group.add_argument("--g1-release", type=Path)
    build.add_argument("--output", type=Path, required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--release", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "materialize":
        print(
            materialize(
                args.sample_release,
                args.output,
                a7_release=args.a7_release,
                g1_release=args.g1_release,
            )
        )
    else:
        print(json.dumps(verify_handoff(args.release), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
