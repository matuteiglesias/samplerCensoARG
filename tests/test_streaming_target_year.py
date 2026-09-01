from pathlib import Path

import pytest

from censo_sampler.streaming_target_year import build_target_year_release_streaming
from censo_sampler.target_year import TargetYearSamplingError, build_target_year_release

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


def _args(tmp_path: Path, year: int) -> dict[str, object]:
    return {
        "target_population_path": _target_parent(tmp_path / "target_population.csv"),
        "target_source_id": "fixture.indec-cpv2010-department-series/v1",
        "target_year": year,
        "fraction": 0.5,
        "seed": 20260831,
        "geography_path": FIXTURE / "GEOGRAPHY.csv",
        "max_households": 20,
    }


@pytest.mark.parametrize("year", [2024, 2025])
def test_streaming_backend_is_byte_equivalent_to_reference_fixture(
    tmp_path: Path, year: int
) -> None:
    args = _args(tmp_path, year)
    reference = build_target_year_release(
        FIXTURE,
        tmp_path / f"reference-{year}",
        **args,
    )
    streaming = build_target_year_release_streaming(
        FIXTURE,
        tmp_path / f"streaming-{year}",
        **args,
    )

    assert streaming.name == reference.name
    for filename in ("households.csv", "persons.csv", "qa.json", "manifest.json"):
        assert (streaming / filename).read_bytes() == (reference / filename).read_bytes()


def test_streaming_backend_keeps_probability_overflow_fail_closed(
    tmp_path: Path,
) -> None:
    target = tmp_path / "overflow.csv"
    target.write_text(
        "department_2010_id,target_year,target_person_mass\n"
        "02001,2024,8\n50007,2024,2\n90084,2024,3\n94008,2024,1\n",
        encoding="utf-8",
    )
    with pytest.raises(TargetYearSamplingError, match="selection_probability_overflow:02001"):
        build_target_year_release_streaming(
            FIXTURE,
            tmp_path / "out",
            target_population_path=target,
            target_source_id="fixture-overflow",
            target_year=2024,
            fraction=1.0,
            geography_path=FIXTURE / "GEOGRAPHY.csv",
            max_households=20,
        )


def test_streaming_backend_can_remove_fixture_scale_household_cap(tmp_path: Path) -> None:
    args = _args(tmp_path, 2024)
    args["max_households"] = None
    release = build_target_year_release_streaming(
        FIXTURE,
        tmp_path / "uncapped",
        **args,
    )
    assert release.is_dir()
