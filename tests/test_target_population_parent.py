import csv
import hashlib
import json
from pathlib import Path

import pytest

from censo_sampler.governed_target_year import build_target_year_release_from_parent
from censo_sampler.target_population import (
    CONTRACT,
    SOURCE_GIT_BLOB_SHA1,
    TargetPopulationError,
    build_indec_2010_2025_target_parent,
    validate_target_population_parent,
)

ROOT = Path(__file__).parents[1]
SOURCE = ROOT / "data" / "info" / "proy_pop200125.csv"
DONOR_FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def test_exact_committed_indec_snapshot_builds_two_year_parent(tmp_path: Path) -> None:
    release = build_indec_2010_2025_target_parent(SOURCE, tmp_path / "parents")
    manifest = json.loads((release / "manifest.json").read_text(encoding="utf-8"))
    qa = json.loads((release / "qa.json").read_text(encoding="utf-8"))
    rows = _rows(release / "target_population.csv")

    assert manifest["contract"] == CONTRACT
    assert manifest["status"] == "source_backed_snapshot"
    assert manifest["source_snapshot"]["git_blob_sha1"] == SOURCE_GIT_BLOB_SHA1
    assert manifest["coverage"]["target_years"] == [2024, 2025]
    assert manifest["coverage"]["mass_unit"] == "person"
    assert {row["target_year"] for row in rows} == {"2024", "2025"}
    assert len(rows) == 2 * qa["department_count"]
    assert qa["unique_department_year"] is True

    values = {
        (row["department_2010_id"], row["target_year"]): int(row["target_person_mass"])
        for row in rows
    }
    assert values[("02001", "2024")] == 258922
    assert values[("02001", "2025")] == 259205
    assert values[("06028", "2024")] == 612438
    assert values[("06028", "2025")] == 616000
    assert values[("06035", "2024")] == 360583
    assert values[("06035", "2025")] == 361532
    assert "proy_pop20012225.csv" in " ".join(manifest["limitations"])


def test_exact_source_bytes_are_pinned_by_git_blob_identity(tmp_path: Path) -> None:
    tampered = tmp_path / "population.csv"
    tampered.write_bytes(SOURCE.read_bytes() + b"\n")

    with pytest.raises(TargetPopulationError, match="source_population_snapshot_drift"):
        build_indec_2010_2025_target_parent(tampered, tmp_path / "parents")


def _governed_fixture_parent(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-parent"
    root.mkdir()
    population = root / "target_population.csv"
    population.write_text(
        "department_2010_id,department_name,target_year,target_person_mass\n"
        "02001,A,2024,4\n50007,B,2024,2\n90084,C,2024,3\n94008,D,2024,1\n"
        "02001,A,2025,6\n50007,B,2025,1\n90084,C,2025,4\n94008,D,2025,1\n",
        encoding="utf-8",
    )
    digest = hashlib.sha256(population.read_bytes()).hexdigest()
    manifest = {
        "contract": CONTRACT,
        "release_id": "fixture-target-population-v1",
        "coverage": {"target_years": [2024, 2025]},
        "artifacts": {"target_population.csv": {"sha256": digest}},
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def test_sampler_binds_source_id_to_validated_parent_manifest(tmp_path: Path) -> None:
    parent = _governed_fixture_parent(tmp_path)
    release = build_target_year_release_from_parent(
        DONOR_FIXTURE,
        tmp_path / "samples",
        target_population_parent=parent,
        target_year=2024,
        fraction=0.5,
        seed=20260831,
        geography_path=DONOR_FIXTURE / "GEOGRAPHY.csv",
        max_households=20,
    )
    manifest = json.loads((release / "manifest.json").read_text(encoding="utf-8"))
    manifest_sha = hashlib.sha256((parent / "manifest.json").read_bytes()).hexdigest()

    assert manifest["target_population_parent"]["source_id"] == (
        f"fixture-target-population-v1@manifest-sha256:{manifest_sha}"
    )
    assert manifest["target_population_parent"]["sha256"] == hashlib.sha256(
        (parent / "target_population.csv").read_bytes()
    ).hexdigest()


def test_governed_parent_payload_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    parent = _governed_fixture_parent(tmp_path)
    (parent / "target_population.csv").write_text("corrupted\n", encoding="utf-8")

    with pytest.raises(TargetPopulationError, match="target_population_payload_hash_mismatch"):
        validate_target_population_parent(parent)
