import csv
import json
from pathlib import Path

import pytest

from censo_sampler.canonical_population import (
    CanonicalPopulationError,
    build_canonical_department_population,
)


def _write_legacy(path: Path, rows: list[dict[str, object]], years: range) -> None:
    fields = ["DPTO", "NOMDPTO", *(str(y) for y in years)]
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_updated(path: Path, rows: list[dict[str, object]]) -> None:
    fields = [
        "Jurisdicción",
        "Nombre jurisdicción",
        "Código departamento",
        "Nombre departamento",
        "Sexo",
        "Población",
        "Fecha",
    ]
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def _fixture_sources(tmp_path: Path) -> tuple[Path, Path, Path]:
    history = tmp_path / "history.csv"
    projection = tmp_path / "projection.csv"
    updated = tmp_path / "updated.csv"

    history_rows = []
    projection_rows = []
    for code, name, base in [
        ("2001", "Comuna 01", 1000),
        ("94007", "Río Grande", 2000),
    ]:
        h = {"DPTO": code, "NOMDPTO": name}
        for year in range(2001, 2011):
            h[str(year)] = base + 10 * (year - 2001)
        history_rows.append(h)

        p = {"DPTO": code, "NOMDPTO": name}
        p["2010"] = h["2010"]
        for year in range(2011, 2023):
            p[str(year)] = int(h["2010"]) + 20 * (year - 2010)
        projection_rows.append(p)

    _write_legacy(history, history_rows, range(2001, 2011))
    _write_legacy(projection, projection_rows, range(2010, 2023))

    updated_rows = []
    # Both IDs deliberately change; name matching is the governed bridge.
    for code, name, p22 in [
        ("02007", "Comuna 1", 1500),
        ("94008", "Río Grande", 3000),
        ("94011", "Tolhuin", 100),
    ]:
        for year in range(2022, 2025):
            updated_rows.append(
                {
                    "Jurisdicción": code[:2],
                    "Nombre jurisdicción": "X",
                    "Código departamento": code,
                    "Nombre departamento": name,
                    "Sexo": 0,
                    "Población": p22 + 25 * (year - 2022),
                    "Fecha": f"{year}-07-01",
                }
            )
            # Sex-specific rows must be ignored.
            updated_rows.append(
                {
                    "Jurisdicción": code[:2],
                    "Nombre jurisdicción": "X",
                    "Código departamento": code,
                    "Nombre departamento": name,
                    "Sexo": 1,
                    "Población": 1,
                    "Fecha": f"{year}-07-01",
                }
            )
    _write_updated(updated, updated_rows)
    return history, projection, updated


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def test_builds_period_native_canonical_surface_with_exact_anchors(tmp_path: Path) -> None:
    history, projection, updated = _fixture_sources(tmp_path)
    release = build_canonical_department_population(
        history, projection, updated, tmp_path / "out"
    )

    rows = _rows(release / "target_population.csv")
    by_key = {(r["department_id"], int(r["target_year"])): r for r in rows}

    # 2010 is preserved exactly on legacy geography.
    assert by_key[("02001", 2010)]["target_person_mass"] == "1090"
    assert by_key[("02001", 2010)]["bridge_scale"] == "1"
    assert by_key[("02001", 2010)]["value_status"] == "legacy_official_copy"

    # Updated geography is direct from 2022 onward.
    assert by_key[("02007", 2022)]["target_person_mass"] == "1500"
    assert by_key[("02007", 2022)]["value_status"] == (
        "direct_indec_2022_based_estimate"
    )
    assert ("02001", 2022) not in by_key

    # Updated-only units are not fabricated historically.
    assert ("94011", 2021) not in by_key
    assert by_key[("94011", 2022)]["target_person_mass"] == "100"

    manifest = json.loads((release / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["coverage"]["start_year"] == 2001
    assert manifest["coverage"]["end_year"] == 2024
    assert manifest["method_id"].endswith("vintage-bridge-linear-v1")


def test_bridge_scale_is_linear_and_hits_updated_over_legacy_ratio(tmp_path: Path) -> None:
    history, projection, updated = _fixture_sources(tmp_path)
    release = build_canonical_department_population(
        history, projection, updated, tmp_path / "out"
    )
    rows = _rows(release / "target_population.csv")
    by_key = {(r["department_id"], int(r["target_year"])): r for r in rows}

    # Comuna 01 legacy 2022 is 1330; updated 2022 is 1500.
    ratio = 1500 / 1330
    # 2016 is halfway from 2010 to 2022.
    expected_scale = 1 + 0.5 * (ratio - 1)
    expected_mass = round((1090 + 20 * 6) * expected_scale)

    row = by_key[("02001", 2016)]
    assert float(row["bridge_alpha"]) == pytest.approx(0.5)
    assert float(row["bridge_scale"]) == pytest.approx(expected_scale)
    assert int(row["target_person_mass"]) == expected_mass

    # Algebraic convention: terminal multiplier is updated/legacy, not inverse.
    assert float(row["terminal_2022_ratio"]) == pytest.approx(ratio)


def test_changed_codes_align_by_normalized_name_and_new_units_are_reported(tmp_path: Path) -> None:
    history, projection, updated = _fixture_sources(tmp_path)
    release = build_canonical_department_population(
        history, projection, updated, tmp_path / "out"
    )

    alignment = _rows(release / "geography_alignment.csv")
    by_legacy = {r["legacy_department_id"]: r for r in alignment}
    assert by_legacy["02001"]["updated_department_id"] == "02007"
    assert by_legacy["02001"]["match_method"] == "same_normalized_name"
    assert by_legacy["94007"]["updated_department_id"] == "94008"

    updated_only = _rows(release / "updated_only_departments.csv")
    assert {(r["updated_department_id"], r["updated_department_name"]) for r in updated_only} == {
        ("94011", "Tolhuin")
    }


def test_2010_anchor_mismatch_fails_closed(tmp_path: Path) -> None:
    history, projection, updated = _fixture_sources(tmp_path)
    text = projection.read_text(encoding="utf-8")
    projection.write_text(text.replace("1090,1110", "1091,1110", 1), encoding="utf-8")

    with pytest.raises(CanonicalPopulationError, match="legacy_2010_anchor_mismatch"):
        build_canonical_department_population(
            history, projection, updated, tmp_path / "out"
        )


def test_unresolved_legacy_department_requires_explicit_geography_resolution(
    tmp_path: Path,
) -> None:
    history, projection, updated = _fixture_sources(tmp_path)
    text = updated.read_text(encoding="utf-8")
    text = text.replace("Río Grande", "Different Name")
    updated.write_text(text, encoding="utf-8")

    with pytest.raises(CanonicalPopulationError, match="unresolved_geography_alignment"):
        build_canonical_department_population(
            history, projection, updated, tmp_path / "out"
        )
