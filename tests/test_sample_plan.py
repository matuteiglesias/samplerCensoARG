import json
from pathlib import Path

from censo_sampler.frame_2010 import build_cpv2010_frame
from censo_sampler.plan_cli import main as plan_main
from censo_sampler.planning import plan_sample

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


def _target(path: Path, *, overflow: bool = False) -> Path:
    first = 10 if overflow else 4
    path.write_text(
        "department_id,target_year,target_person_mass\n"
        f"02001,2024,{first}\n"
        "50007,2024,2\n"
        "90084,2024,3\n"
        "94008,2024,1\n",
        encoding="utf-8",
    )
    return path


def test_sample_plan_is_ready_without_materializing_rows(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    plan = plan_sample(
        frame,
        target_population=_target(tmp_path / "target.csv"),
        target_year=2024,
        fraction=0.5,
    )
    assert plan["status"] == "ready"
    assert plan["census_vintage"] == 2010
    assert plan["probability_range"] == [0.5, 0.5]
    assert plan["expected_selected_person_mass"] == 5.0
    assert plan["probability_overflow_departments"] == []


def test_sample_plan_reports_probability_overflow_without_selecting(tmp_path: Path) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    plan = plan_sample(
        frame,
        target_population=_target(tmp_path / "target.csv", overflow=True),
        target_year=2024,
        fraction=0.5,
    )
    assert plan["status"] == "blocked"
    assert plan["probability_overflow_departments"] == ["02001"]
    row = next(row for row in plan["departments"] if row["department_id"] == "02001")
    assert row["status"] == "probability-overflow"
    assert row["selection_probability"] == 1.25


def test_plan_cli_returns_nonzero_for_blocked_plan(tmp_path: Path, capsys) -> None:
    frame = build_cpv2010_frame(FIXTURE, tmp_path / "frames")
    target = _target(tmp_path / "target.csv", overflow=True)
    status = plan_main(
        [
            "--frame",
            str(frame),
            "--target-population",
            str(target),
            "--target-year",
            "2024",
            "--fraction",
            "0.5",
        ]
    )
    assert status == 2
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "blocked"
    assert "departments" not in result
