import json
from pathlib import Path

import pyarrow.parquet as pq

from censo_sampler.frame_2010 import build_cpv2010_frame
from censo_sampler.frame_contract import sha256_file
from censo_sampler.frontdoor import main
from censo_sampler.materialize_selection import materialize_existing_selection_v2
from censo_sampler.release_v2 import (
    SampleReleaseV2Error,
    build_sample_release_v2,
    validate_sample_release_v2,
)

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


def _target(path: Path) -> Path:
    path.write_text(
        "department_2010_id,target_year,target_person_mass\n"
        "02001,2024,4\n50007,2024,2\n90084,2024,3\n94008,2024,1\n",
        encoding="utf-8",
    )
    return path


def _selection_only(tmp_path: Path) -> tuple[Path, Path]:
    frame = build_cpv2010_frame(
        FIXTURE,
        tmp_path / "frames",
        geography_path=FIXTURE / "GEOGRAPHY.csv",
    )
    selection = build_sample_release_v2(
        frame,
        tmp_path / "selection",
        target_population=_target(tmp_path / "target.csv"),
        target_year=2024,
        fraction=0.5,
        seed=20260831,
        materialization="selection-only",
    )
    return frame, selection


def test_hydrate_existing_selection_preserves_exact_selection_and_full_schema(
    tmp_path: Path,
) -> None:
    frame, selection = _selection_only(tmp_path)
    child = materialize_existing_selection_v2(
        frame,
        selection,
        tmp_path / "hydrated",
    )

    checked = validate_sample_release_v2(child)
    assert checked["status"] == "valid"
    assert checked["materialization"] == "full-payload"
    assert sha256_file(child / "selection.parquet") == sha256_file(
        selection / "selection.parquet"
    )
    assert sha256_file(child / "person_membership.parquet") == sha256_file(
        selection / "person_membership.parquet"
    )

    frame_person_schema = pq.read_schema(frame / "payload/persona.parquet")
    child_person_schema = pq.read_schema(child / "persona.parquet")
    assert frame_person_schema.equals(child_person_schema, check_metadata=False)
    assert {"P02", "P03", "frame_person_id", "frame_household_id"} <= set(
        child_person_schema.names
    )

    manifest = json.loads((child / "manifest.json").read_text())
    lineage = manifest["materialization_lineage"]
    assert lineage["selection_parent_release_id"] == json.loads(
        (selection / "manifest.json").read_text()
    )["release_id"]
    assert lineage["frame_hashes_verified_before_materialization"] is True
    assert all(
        item["byte_identical"]
        for item in lineage["selection_artifacts_reused"].values()
    )
    assert (
        manifest["payload_schema_custody"]["persona.parquet"]["frame_payload"]
        == manifest["payload_schema_custody"]["persona.parquet"][
            "materialized_payload"
        ]
    )


def test_hydration_fails_if_frame_payload_bytes_drift_from_governed_manifest(
    tmp_path: Path,
) -> None:
    frame, selection = _selection_only(tmp_path)
    persona_path = frame / "payload/persona.parquet"
    table = pq.read_table(persona_path)
    pq.write_table(table.drop(["P03"]), persona_path)

    try:
        materialize_existing_selection_v2(
            frame,
            selection,
            tmp_path / "hydrated",
        )
    except SampleReleaseV2Error as exc:
        assert "frame_artifact_hash_mismatch:payload/persona.parquet" in str(exc)
    else:
        raise AssertionError("frame payload custody drift must fail closed")


def test_cli_materialize_selection_uses_existing_fixed_selection(
    tmp_path: Path, capsys
) -> None:
    frame, selection = _selection_only(tmp_path)
    assert (
        main(
            [
                "materialize-selection",
                "--frame",
                str(frame),
                "--selection-release",
                str(selection),
                "--output-root",
                str(tmp_path / "hydrated"),
            ]
        )
        == 0
    )
    child = Path(capsys.readouterr().out.strip())
    assert child.is_dir()
    assert validate_sample_release_v2(child)["materialization"] == "full-payload"
    assert "P03" in pq.read_schema(child / "persona.parquet").names
