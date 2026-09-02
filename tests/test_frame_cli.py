import json
from pathlib import Path

from censo_sampler.frontdoor import main

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


def _target(path: Path) -> Path:
    path.write_text(
        "department_2010_id,target_year,target_person_mass\n"
        "02001,2024,4\n50007,2024,2\n90084,2024,3\n94008,2024,1\n",
        encoding="utf-8",
    )
    return path


def test_frame_cli_build_check_sample_and_release_check(
    tmp_path: Path, capsys
) -> None:
    assert (
        main(
            [
                "frame",
                "build-2010",
                "--databasepath",
                str(FIXTURE),
                "--geography",
                str(FIXTURE / "GEOGRAPHY.csv"),
                "--output-root",
                str(tmp_path / "frames"),
            ]
        )
        == 0
    )
    frame = Path(capsys.readouterr().out.strip())
    assert frame.is_dir()

    assert main(["frame", "check", str(frame)]) == 0
    checked = json.loads(capsys.readouterr().out)
    assert checked["status"] == "valid"
    assert checked["census_vintage"] == 2010

    target = _target(tmp_path / "target.csv")
    assert (
        main(
            [
                "sample",
                "--frame",
                str(frame),
                "--target-population",
                str(target),
                "--target-year",
                "2024",
                "--fraction",
                "0.5",
                "--seed",
                "20260831",
                "--materialize",
                "selection-only",
                "--output-root",
                str(tmp_path / "samples"),
            ]
        )
        == 0
    )
    release = Path(capsys.readouterr().out.strip())
    assert release.is_dir()

    assert main(["check-release-v2", str(release)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "valid"
    assert result["frame_vintage"] == 2010


def test_frame_cli_fails_closed_on_department_mismatch(tmp_path: Path, capsys) -> None:
    assert (
        main(
            [
                "frame",
                "build-2010",
                "--databasepath",
                str(FIXTURE),
                "--output-root",
                str(tmp_path / "frames"),
            ]
        )
        == 0
    )
    frame = Path(capsys.readouterr().out.strip())
    target = tmp_path / "bad-target.csv"
    target.write_text(
        "department_id,target_year,target_person_mass\n02001,2024,4\n",
        encoding="utf-8",
    )
    assert (
        main(
            [
                "sample",
                "--frame",
                str(frame),
                "--target-population",
                str(target),
                "--target-year",
                "2024",
                "--fraction",
                "0.5",
                "--output-root",
                str(tmp_path / "samples"),
            ]
        )
        == 2
    )
    assert "department_alignment_mismatch" in capsys.readouterr().err
