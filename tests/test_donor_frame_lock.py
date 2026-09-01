import csv
import json
import shutil
from pathlib import Path

import pytest

from censo_sampler.donor_frame import (
    DonorFrameError,
    build_donor_frame_lock,
    validate_donor_frame_lock,
)

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


def _masses(path: Path) -> dict[str, int]:
    with path.open(encoding="utf-8", newline="") as stream:
        return {
            row["department_2010_id"]: int(row["donor_person_mass"])
            for row in csv.DictReader(stream)
        }


def test_fixture_builds_metadata_only_relational_lock(tmp_path: Path) -> None:
    release = build_donor_frame_lock(
        FIXTURE,
        tmp_path / "locks",
        geography_path=FIXTURE / "GEOGRAPHY.csv",
    )
    manifest = json.loads((release / "manifest.json").read_text(encoding="utf-8"))
    qa = json.loads((release / "qa.json").read_text(encoding="utf-8"))

    assert manifest["contract"] == "research.cpv2010-donor-frame-lock/v1"
    assert manifest["status"] == "local_authorized_metadata_lock"
    assert manifest["custody"]["microdata_payload_republished"] is False
    assert manifest["custody"]["local_absolute_paths_recorded"] is False
    assert qa == {
        "departments": 4,
        "donor_person_mass_total": 10,
        "dwellings": 6,
        "frame_vintage": 2010,
        "geography_rows": 4,
        "household_identity_unique": True,
        "household_to_vivienda": "validated",
        "households": 6,
        "person_identity_unique": True,
        "person_to_household": "validated",
        "persons": 10,
        "radio_identity_unique": True,
        "vivienda_identity_unique": True,
        "vivienda_to_radio_geography": "validated",
    }
    assert _masses(release / "donor_person_mass.csv") == {
        "02001": 4,
        "50007": 2,
        "90084": 3,
        "94008": 1,
    }
    assert sorted(path.name for path in release.iterdir()) == [
        "donor_person_mass.csv",
        "manifest.json",
        "qa.json",
    ]


def test_existing_lock_revalidates_exact_local_source_bytes(tmp_path: Path) -> None:
    local = tmp_path / "cpv"
    shutil.copytree(FIXTURE, local)
    release = build_donor_frame_lock(
        local,
        tmp_path / "locks",
        geography_path=local / "GEOGRAPHY.csv",
    )
    manifest = validate_donor_frame_lock(
        release,
        local,
        geography_path=local / "GEOGRAPHY.csv",
    )
    assert manifest["release_id"] == release.name

    with (local / "PERSONA.csv").open("a", encoding="utf-8") as stream:
        stream.write("\n")
    with pytest.raises(DonorFrameError, match="donor_lock_source_hash_mismatch:PERSONA.csv"):
        validate_donor_frame_lock(
            release,
            local,
            geography_path=local / "GEOGRAPHY.csv",
        )


def test_duplicate_person_identity_fails_closed(tmp_path: Path) -> None:
    local = tmp_path / "cpv"
    shutil.copytree(FIXTURE, local)
    with (local / "PERSONA.csv").open("a", encoding="utf-8") as stream:
        stream.write("p01;h01;1;40\n")

    with pytest.raises(DonorFrameError, match="PERSONA:duplicate_PERSONA_REF_ID"):
        build_donor_frame_lock(
            local,
            tmp_path / "locks",
            geography_path=local / "GEOGRAPHY.csv",
        )


def test_orphan_person_household_fails_closed(tmp_path: Path) -> None:
    local = tmp_path / "cpv"
    shutil.copytree(FIXTURE, local)
    with (local / "PERSONA.csv").open("a", encoding="utf-8") as stream:
        stream.write("p11;missing-household;1;20\n")

    with pytest.raises(DonorFrameError, match="orphan_person_households=1"):
        build_donor_frame_lock(
            local,
            tmp_path / "locks",
            geography_path=local / "GEOGRAPHY.csv",
        )
