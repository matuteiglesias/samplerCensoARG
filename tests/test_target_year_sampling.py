import csv
import json
from pathlib import Path

import pytest

from censo_sampler.target_year import (
    TargetYearSamplingError,
    build_target_year_release,
)

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


def _target_parent(path: Path) -> Path:
    path.write_text(
        "department_2010_id,target_year,target_person_mass\n"
        "02001,2024,4\n"
        "50007,2024,2\n"
        "90084,2024,3\n"
        "94008,2024,1\n"
        "02001,2025,6\n"
        "50007,2025,1\n"
        "90084,2025,4\n"
        "94008,2025,1\n",
        encoding="utf-8",
    )
    return path


def _build(tmp_path: Path, year: int, *, fraction: float = 0.5, output_name: str = "out") -> Path:
    target = _target_parent(tmp_path / "target_population.csv")
    return build_target_year_release(
        FIXTURE,
        tmp_path / output_name,
        target_population_path=target,
        target_source_id="fixture.indec-cpv2010-department-series/v1",
        target_year=year,
        fraction=fraction,
        seed=20260831,
        geography_path=FIXTURE / "GEOGRAPHY.csv",
        max_households=20,
    )


def _rows(release: Path, filename: str) -> list[dict[str, str]]:
    with (release / filename).open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def test_equal_target_and_donor_mass_yields_base_probability(tmp_path: Path) -> None:
    release = _build(tmp_path, 2024)
    manifest = json.loads((release / "manifest.json").read_text())

    probabilities = {
        dept: facts["selection_probability"]
        for dept, facts in manifest["qa"]["departments"].items()
    }
    assert probabilities == {
        "02001": pytest.approx(0.5),
        "50007": pytest.approx(0.5),
        "90084": pytest.approx(0.5),
        "94008": pytest.approx(0.5),
    }
    assert manifest["frame"]["vintage"] == 2010
    assert manifest["target_population_parent"]["target_year"] == 2024
    assert manifest["selection"]["unit"] == "household"
    assert manifest["selection"]["target_mass_unit"] == "person"


def test_2024_and_2025_share_common_household_score_and_change_probabilities(tmp_path: Path) -> None:
    release_2024 = _build(tmp_path, 2024, output_name="y2024")
    release_2025 = _build(tmp_path, 2025, output_name="y2025")
    m24 = json.loads((release_2024 / "manifest.json").read_text())
    m25 = json.loads((release_2025 / "manifest.json").read_text())

    assert m24["selection"]["common_score_across_target_years"] is True
    assert m25["selection"]["common_score_across_target_years"] is True
    assert m25["qa"]["departments"]["02001"]["selection_probability"] > m24["qa"]["departments"]["02001"]["selection_probability"]
    assert m25["qa"]["departments"]["50007"]["selection_probability"] < m24["qa"]["departments"]["50007"]["selection_probability"]

    selected_2024 = {row["sample_household_id"] for row in _rows(release_2024, "households.csv")}
    selected_2025 = {row["sample_household_id"] for row in _rows(release_2025, "households.csv")}
    # On this fixture all 2025 thresholds remain above the selected 2024 scores;
    # the assertion therefore detects accidental inclusion of target year in the score.
    assert selected_2024 <= selected_2025


def test_selected_households_keep_all_members_and_weights_are_not_overloaded(tmp_path: Path) -> None:
    release = _build(tmp_path, 2024)
    households = _rows(release, "households.csv")
    persons = _rows(release, "persons.csv")
    manifest = json.loads((release / "manifest.json").read_text())

    selected_ids = {row["sample_household_id"] for row in households}
    assert {row["sample_household_id"] for row in persons} <= selected_ids
    assert manifest["qa"]["complete_household_membership"] is True
    assert "sample_weight" not in households[0]
    assert "sample_weight" not in persons[0]
    assert "selection_probability" in households[0]
    assert "design_inverse_probability_weight" in households[0]
    assert manifest["weight_semantics"]["analysis_weight"] is None
    assert manifest["weight_semantics"]["generic_sample_weight"] is None


def test_qa_reports_target_expected_and_realized_person_shares(tmp_path: Path) -> None:
    release = _build(tmp_path, 2024)
    qa = json.loads((release / "qa.json").read_text())

    for facts in qa["departments"].values():
        assert "target_person_share" in facts
        assert "expected_selected_person_mass" in facts
        assert "realized_person_share" in facts
        assert "realized_minus_target_person_share" in facts
        assert "selected_households" in facts
        assert "selected_persons" in facts


def test_probability_overflow_fails_instead_of_clipping(tmp_path: Path) -> None:
    target = tmp_path / "overflow.csv"
    target.write_text(
        "department_2010_id,target_year,target_person_mass\n"
        "02001,2024,8\n50007,2024,2\n90084,2024,3\n94008,2024,1\n",
        encoding="utf-8",
    )
    with pytest.raises(TargetYearSamplingError, match="selection_probability_overflow:02001"):
        build_target_year_release(
            FIXTURE,
            tmp_path / "out",
            target_population_path=target,
            target_source_id="fixture-overflow",
            target_year=2024,
            fraction=1.0,
            geography_path=FIXTURE / "GEOGRAPHY.csv",
            max_households=20,
        )


def test_only_2024_and_2025_are_accepted(tmp_path: Path) -> None:
    target = _target_parent(tmp_path / "target_population.csv")
    with pytest.raises(TargetYearSamplingError, match="target_year_must_be_one_of"):
        build_target_year_release(
            FIXTURE,
            tmp_path / "out",
            target_population_path=target,
            target_source_id="fixture",
            target_year=2023,
            geography_path=FIXTURE / "GEOGRAPHY.csv",
        )


def test_releases_are_byte_stable_across_output_roots(tmp_path: Path) -> None:
    first = _build(tmp_path, 2024, output_name="one")
    second = _build(tmp_path, 2024, output_name="two")
    assert first.name == second.name
    for filename in ("households.csv", "persons.csv", "qa.json", "manifest.json"):
        assert (first / filename).read_bytes() == (second / filename).read_bytes()
