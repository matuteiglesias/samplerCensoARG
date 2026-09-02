import csv
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from censo_sampler.frame_2010 import build_cpv2010_frame
from censo_sampler.release_v2 import build_sample_release_v2
from censo_sampler.streaming_target_year import build_target_year_release_streaming

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"
SEED = 20260831
FRACTION = 0.5


def _target(path: Path) -> Path:
    path.write_text(
        "department_2010_id,target_year,target_person_mass\n"
        "02001,2024,4\n50007,2024,2\n90084,2024,3\n94008,2024,1\n"
        "02001,2025,6\n50007,2025,1\n90084,2025,4\n94008,2025,1\n",
        encoding="utf-8",
    )
    return path


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def _source_id(namespaced: str) -> str:
    return namespaced.rsplit(":", 1)[-1]


@pytest.mark.parametrize("year", [2024, 2025])
def test_frame_sampler_is_scientifically_identical_to_streaming_2010_oracle(
    tmp_path: Path, year: int
) -> None:
    target = _target(tmp_path / "target.csv")
    old = build_target_year_release_streaming(
        FIXTURE,
        tmp_path / f"legacy-{year}",
        target_population_path=target,
        target_source_id="fixture.indec-cpv2010-department-series/v1",
        target_year=year,
        fraction=FRACTION,
        seed=SEED,
        geography_path=FIXTURE / "GEOGRAPHY.csv",
        max_households=20,
    )
    frame = build_cpv2010_frame(
        FIXTURE,
        tmp_path / "frames",
        geography_path=FIXTURE / "GEOGRAPHY.csv",
    )
    new = build_sample_release_v2(
        frame,
        tmp_path / f"new-{year}",
        target_population=target,
        target_year=year,
        fraction=FRACTION,
        seed=SEED,
        materialization="selection-only",
    )

    old_households = _csv_rows(old / "households.csv")
    old_people = _csv_rows(old / "persons.csv")
    new_selection = pq.read_table(new / "selection.parquet").to_pylist()
    new_membership = pq.read_table(new / "person_membership.parquet").to_pylist()

    old_household_ids = {
        _source_id(row["sample_household_id"]) for row in old_households
    }
    new_household_ids = {row["frame_household_id"] for row in new_selection}
    assert new_household_ids == old_household_ids

    old_person_ids = {_source_id(row["sample_person_id"]) for row in old_people}
    new_person_ids = {row["frame_person_id"] for row in new_membership}
    assert new_person_ids == old_person_ids

    old_probability = {
        _source_id(row["sample_household_id"]): float(row["selection_probability"])
        for row in old_households
    }
    new_probability = {
        row["frame_household_id"]: float(row["selection_probability"])
        for row in new_selection
    }
    assert set(new_probability) == set(old_probability)
    for household in old_probability:
        assert new_probability[household] == pytest.approx(
            old_probability[household], rel=0, abs=1e-15
        )

    old_person_households = {
        _source_id(row["sample_person_id"]): _source_id(row["sample_household_id"])
        for row in old_people
    }
    new_person_households = {
        row["frame_person_id"]: row["frame_household_id"] for row in new_membership
    }
    assert new_person_households == old_person_households
