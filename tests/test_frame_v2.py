import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from censo_sampler.frame_2010 import build_cpv2010_frame
from censo_sampler.frame_contract import (
    FRAME_CONTRACT,
    canonical_json,
    sha256_file,
    validate_frame,
)
from censo_sampler.release_v2 import (
    build_sample_release_v2,
    validate_sample_release_v2,
)
from censo_sampler.selection import household_score, selection_probabilities
from censo_sampler.target_adapter import (
    TargetPopulationAdapterError,
    load_target_population,
    require_department_alignment,
)

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


def _target(path: Path) -> Path:
    path.write_text(
        "department_2010_id,target_year,target_person_mass\n"
        "02001,2024,4\n50007,2024,2\n90084,2024,3\n94008,2024,1\n"
        "02001,2025,6\n50007,2025,1\n90084,2025,4\n94008,2025,1\n",
        encoding="utf-8",
    )
    return path


def _parquet_rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


def test_selection_kernel_preserves_legacy_fixture_scores() -> None:
    assert household_score(20260831, "h03", "50007") == pytest.approx(
        0.23899021496428527
    )
    assert household_score(20260831, "h04", "90084") == pytest.approx(
        0.3853227435722824
    )
    probabilities = selection_probabilities(
        {"02001": 4, "50007": 2, "90084": 3, "94008": 1},
        {"02001": 4, "50007": 2, "90084": 3, "94008": 1},
        0.5,
    )
    assert set(probabilities.values()) == {0.5}


def test_build_2010_frame_preserves_full_payload_and_validates(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(
        FIXTURE,
        tmp_path / "frames",
        geography_path=FIXTURE / "GEOGRAPHY.csv",
    )
    checked = validate_frame(frame)
    assert checked["status"] == "valid"
    assert checked["census_vintage"] == 2010
    assert checked["counts"] == {
        "dwellings": 6,
        "households": 6,
        "persons": 10,
        "departments": 4,
    }

    persona = pq.read_table(frame / "payload/persona.parquet")
    assert {"PERSONA_REF_ID", "HOGAR_REF_ID", "P02", "P03"} <= set(
        persona.column_names
    )
    assert {"frame_person_id", "frame_household_id"} <= set(persona.column_names)
    manifest = json.loads((frame / "manifest.json").read_text())
    assert manifest["feature_projection"] is None
    assert manifest["payload_policy"].startswith("full-source-columns")


def test_frame_based_2010_selection_matches_legacy_scientific_choice(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    target = _target(tmp_path / "target.csv")

    release_2024 = build_sample_release_v2(
        frame,
        tmp_path / "samples-24",
        target_population=target,
        target_year=2024,
        fraction=0.5,
        seed=20260831,
    )
    release_2025 = build_sample_release_v2(
        frame,
        tmp_path / "samples-25",
        target_population=target,
        target_year=2025,
        fraction=0.5,
        seed=20260831,
    )

    selected_24 = {
        row["frame_household_id"]
        for row in _parquet_rows(release_2024 / "selection.parquet")
    }
    selected_25 = {
        row["frame_household_id"]
        for row in _parquet_rows(release_2025 / "selection.parquet")
    }
    assert selected_24 == {"h03", "h04", "h06"}
    assert selected_25 == {"h02", "h03", "h04", "h06"}
    assert selected_24 <= selected_25

    probs24 = {
        row["frame_household_id"]: row["selection_probability"]
        for row in _parquet_rows(release_2024 / "selection.parquet")
    }
    assert all(value == pytest.approx(0.5) for value in probs24.values())


def test_v2_materializes_full_relational_payload_after_key_selection(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    release = build_sample_release_v2(
        frame,
        tmp_path / "samples",
        target_population=_target(tmp_path / "target.csv"),
        target_year=2024,
        fraction=0.5,
        seed=20260831,
        materialization="full-payload",
    )
    checked = validate_sample_release_v2(release)
    assert checked["status"] == "valid"
    assert checked["frame_vintage"] == 2010
    assert checked["households"] == 3
    assert checked["persons"] == 4

    hogar = _parquet_rows(release / "hogar.parquet")
    persona = _parquet_rows(release / "persona.parquet")
    vivienda = _parquet_rows(release / "vivienda.parquet")
    assert {row["frame_household_id"] for row in hogar} == {"h03", "h04", "h06"}
    assert {row["frame_household_id"] for row in persona} == {"h03", "h04", "h06"}
    assert {row["frame_dwelling_id"] for row in vivienda} == {"v03", "v04", "v06"}
    assert "P02" in persona[0] and "P03" in persona[0]


def test_selection_only_release_does_not_copy_substantive_payload(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    release = build_sample_release_v2(
        frame,
        tmp_path / "samples",
        target_population=_target(tmp_path / "target.csv"),
        target_year=2024,
        fraction=0.5,
        materialization="selection-only",
    )
    assert (release / "selection.parquet").is_file()
    assert (release / "person_membership.parquet").is_file()
    assert not (release / "persona.parquet").exists()
    assert validate_sample_release_v2(release)["materialization"] == "selection-only"


def _build_synthetic_2022_frame(root: Path) -> Path:
    root.mkdir(parents=True)
    (root / "payload").mkdir()
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "frame_household_id": "061471101:1",
                    "frame_dwelling_id": "061471101:1",
                    "department_id": "06147",
                    "radio_id": "061471101",
                    "household_person_count": 2,
                },
                {
                    "frame_household_id": "061471101:2",
                    "frame_dwelling_id": "061471101:2",
                    "department_id": "06147",
                    "radio_id": "061471101",
                    "household_person_count": 1,
                },
            ]
        ),
        root / "frame_households.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([{"department_id": "06147", "donor_person_mass": 3}]),
        root / "donor_person_mass.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"frame_dwelling_id": "061471101:1", "V01": "1", "NEW2022": "x"},
                {"frame_dwelling_id": "061471101:2", "V01": "2", "NEW2022": "y"},
            ]
        ),
        root / "payload/vivienda.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "frame_household_id": "061471101:1",
                    "frame_dwelling_id": "061471101:1",
                    "H10": "1",
                },
                {
                    "frame_household_id": "061471101:2",
                    "frame_dwelling_id": "061471101:2",
                    "H10": "2",
                },
            ]
        ),
        root / "payload/hogar.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "frame_person_id": "061471101:1",
                    "frame_household_id": "061471101:1",
                    "P02": "1",
                    "EDAD": "40",
                    "ONLY_2022": "a",
                },
                {
                    "frame_person_id": "061471101:2",
                    "frame_household_id": "061471101:1",
                    "P02": "2",
                    "EDAD": "39",
                    "ONLY_2022": "b",
                },
                {
                    "frame_person_id": "061471101:3",
                    "frame_household_id": "061471101:2",
                    "P02": "1",
                    "EDAD": "10",
                    "ONLY_2022": "c",
                },
            ]
        ),
        root / "payload/persona.parquet",
    )
    artifacts = {}
    for name in (
        "frame_households.parquet",
        "donor_person_mass.parquet",
        "payload/vivienda.parquet",
        "payload/hogar.parquet",
        "payload/persona.parquet",
    ):
        path = root / name
        artifacts[name] = {"sha256": sha256_file(path), "size_bytes": path.stat().st_size}
    manifest = {
        "contract": FRAME_CONTRACT,
        "frame_release_id": "fixture-cpv2022-radio-061471101",
        "country": "ARG",
        "census_vintage": 2022,
        "source_release_id": "fixture-rxdb",
        "department_alignment_policy": "assume-code-identity/v1",
        "counts": {"dwellings": 2, "households": 2, "persons": 3, "departments": 1},
        "artifacts": artifacts,
        "feature_projection": None,
    }
    (root / "manifest.json").write_text(canonical_json(manifest), encoding="utf-8")
    return root


def test_same_sampler_accepts_2022_frame_without_feature_assumptions(tmp_path: Path) -> None:
    frame = _build_synthetic_2022_frame(tmp_path / "frame2022")
    validate_frame(frame)
    target = tmp_path / "target2022.csv"
    target.write_text(
        "department_id,target_year,target_person_mass\n06147,2024,3\n",
        encoding="utf-8",
    )
    release = build_sample_release_v2(
        frame,
        tmp_path / "samples",
        target_population=target,
        target_year=2024,
        fraction=1.0,
    )
    checked = validate_sample_release_v2(release)
    assert checked["frame_vintage"] == 2022
    persona = _parquet_rows(release / "persona.parquet")
    assert len(persona) == 3
    assert {"P02", "EDAD", "ONLY_2022"} <= set(persona[0])


def test_department_mismatch_is_visible_not_silently_dropped(tmp_path: Path) -> None:
    target = tmp_path / "target.csv"
    target.write_text(
        "department_2010_id,target_year,target_person_mass\n02001,2024,10\n",
        encoding="utf-8",
    )
    masses, _ = load_target_population(target, 2024)
    assert masses == {"02001": 10}
    with pytest.raises(TargetPopulationAdapterError, match="department_alignment_mismatch"):
        require_department_alignment({"02001", "50007"}, set(masses))
