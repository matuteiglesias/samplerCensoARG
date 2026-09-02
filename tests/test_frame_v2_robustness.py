import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from censo_sampler.frame_2010 import build_cpv2010_frame
from censo_sampler.frame_contract import CensusFrameError, validate_frame
from censo_sampler.release_v2 import (
    SampleReleaseV2Error,
    build_sample_release_v2,
    sample_id,
    validate_sample_release_v2,
)
from censo_sampler.selection import select_households

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


def _target(path: Path) -> Path:
    path.write_text(
        "department_id,target_year,target_person_mass\n"
        "02001,2024,4\n50007,2024,2\n90084,2024,3\n94008,2024,1\n",
        encoding="utf-8",
    )
    return path


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_selection_is_independent_of_household_row_order() -> None:
    households = [
        {
            "frame_household_id": "h01",
            "frame_dwelling_id": "v01",
            "department_id": "02001",
            "radio_id": "r1",
            "household_person_count": 2,
        },
        {
            "frame_household_id": "h03",
            "frame_dwelling_id": "v03",
            "department_id": "50007",
            "radio_id": "r2",
            "household_person_count": 1,
        },
        {
            "frame_household_id": "h04",
            "frame_dwelling_id": "v04",
            "department_id": "90084",
            "radio_id": "r3",
            "household_person_count": 2,
        },
        {
            "frame_household_id": "h06",
            "frame_dwelling_id": "v06",
            "department_id": "94008",
            "radio_id": "r4",
            "household_person_count": 1,
        },
    ]
    donor = {"02001": 4, "50007": 2, "90084": 3, "94008": 1}
    target = dict(donor)
    forward = select_households(
        households,
        donor_person_mass=donor,
        target_person_mass=target,
        fraction=0.5,
        seed=20260831,
    )
    reverse = select_households(
        reversed(households),
        donor_person_mass=donor,
        target_person_mass=target,
        fraction=0.5,
        seed=20260831,
    )
    assert forward == reverse


def test_sample_ids_are_stable_across_target_years_but_namespaced_by_frame() -> None:
    assert sample_id("frame-a", "household", "h1") == sample_id(
        "frame-a", "household", "h1"
    )
    assert sample_id("frame-a", "household", "h1") != sample_id(
        "frame-b", "household", "h1"
    )
    assert sample_id("frame-a", "household", "h1") != sample_id(
        "frame-a", "person", "h1"
    )


def test_frame_hash_tampering_fails_closed(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    path = frame / "donor_person_mass.parquet"
    path.write_bytes(path.read_bytes() + b"tamper")
    with pytest.raises(CensusFrameError, match="frame_artifact_hash_mismatch"):
        validate_frame(frame, deep=False)


def test_sample_hash_tampering_fails_closed(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    release = build_sample_release_v2(
        frame,
        tmp_path / "samples",
        target_population=_target(tmp_path / "target.csv"),
        target_year=2024,
        fraction=0.5,
        materialization="selection-only",
    )
    membership = release / "person_membership.parquet"
    membership.write_bytes(membership.read_bytes() + b"tamper")
    with pytest.raises(SampleReleaseV2Error, match="sample_v2_artifact_hash_mismatch"):
        validate_sample_release_v2(release)


def test_governed_target_parent_hash_mismatch_fails_before_sampling(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    parent = tmp_path / "target-parent"
    parent.mkdir()
    payload = _target(parent / "target_population.csv")
    manifest = {
        "release_id": "target-fixture-v1",
        "artifacts": {
            "target_population.csv": {
                "sha256": _sha256(payload),
                "size_bytes": payload.stat().st_size,
            }
        },
    }
    (parent / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    payload.write_text(payload.read_text(encoding="utf-8") + "06001,2024,1\n", encoding="utf-8")

    with pytest.raises(SampleReleaseV2Error, match="target_population_payload_hash_mismatch"):
        build_sample_release_v2(
            frame,
            tmp_path / "samples",
            target_population=parent,
            target_year=2024,
            fraction=0.5,
        )


def test_frame_deep_check_rejects_manifest_count_drift(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    manifest_path = frame / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["counts"]["persons"] += 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(CensusFrameError, match="frame_manifest_count_mismatch:persons"):
        validate_frame(frame, verify_hashes=True, deep=True)


def test_full_payload_contains_exactly_selected_person_keys(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    release = build_sample_release_v2(
        frame,
        tmp_path / "samples",
        target_population=_target(tmp_path / "target.csv"),
        target_year=2024,
        fraction=0.5,
        materialization="full-payload",
    )
    membership_people = {
        row["frame_person_id"]
        for row in pq.read_table(release / "person_membership.parquet").to_pylist()
    }
    payload_people = {
        row["frame_person_id"]
        for row in pq.read_table(release / "persona.parquet").to_pylist()
    }
    assert payload_people == membership_people
