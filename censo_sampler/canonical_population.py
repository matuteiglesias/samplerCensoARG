"""Canonical department population target surface across demographic vintages.

This module builds a research target-mass product for samplerCensoARG.  It does
not claim to be an INDEC publication.  It combines:

* a trusted legacy 2001+ copy for 2001-2010 levels;
* the documented INDEC 2010-2025 projection family as the legacy trajectory;
* the INDEC Census-2022-based 2022-2035 department estimates from 2022 onward.

For 2011-2021 the legacy trajectory is multiplicatively bridged toward the
updated 2022 level.  The correction factor is 1 at 2010 and reaches
updated_2022 / legacy_2022 at 2022.

Administrative identities are period-native.  Historical rows retain legacy
IDs; 2022+ rows retain the updated source IDs.  A separate alignment table
records which updated department anchors each historical bridge.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import shutil
import tempfile
import unicodedata
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

CONTRACT = "publicdata.argentina-department-population-target/v1"
METHOD_ID = "research.argentina-department-population-target/vintage-bridge-linear-v1"

HISTORY_START = 2001
ANCHOR_YEAR = 2010
UPDATED_START = 2022

LEGACY_HISTORY_REPO_PATH = "data/info/proy_pop20012225.csv"
LEGACY_PROJECTION_REPO_PATH = "data/info/proy_pop200125.csv"

UPDATED_SOURCE_URL = (
    "https://www.indec.gob.ar/ftp/cuadros/poblacion/"
    "base_estimaciones_pob_deptos_2022_2035.csv"
)
UPDATED_METADATA_URL = (
    "https://www.indec.gob.ar/ftp/cuadros/poblacion/"
    "metadatos_estimaciones_deptos_2022_2035.pdf"
)
UPDATED_PUBLICATION_URL = (
    "https://www.indec.gob.ar/ftp/cuadros/publicaciones/"
    "estimaciones_departamentos_2022_2035.pdf"
)

OUTPUT_FIELDS = [
    "department_id",
    "department_name",
    "target_year",
    "target_person_mass",
    "value_status",
    "geography_vintage",
    "source_family",
    "legacy_department_id",
    "updated_department_id",
    "bridge_alpha",
    "bridge_scale",
    "terminal_2022_ratio",
]


class CanonicalPopulationError(ValueError):
    """Raised when the canonical population surface cannot be built safely."""


@dataclass(frozen=True)
class DepartmentSeries:
    department_id: str
    department_name: str
    values: dict[int, int]


def _canonical_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _normalize_id(value: str) -> str:
    value = (value or "").strip()
    if not value.isdigit():
        raise CanonicalPopulationError(f"invalid_department_id:{value}")
    return value.zfill(5)


def _normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", (value or "").strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    text = re.sub(r"^comuna\s+0+(\d+)$", r"comuna \1", text)
    return text


def _parse_positive_int(value: str, *, context: str) -> int:
    raw = (value or "").strip()
    if not raw or raw.upper() in {"NA", "N/A", "ND", "-", "..."}:
        raise CanonicalPopulationError(f"missing_population:{context}")
    compact = raw.replace("\u00a0", "").replace(" ", "")
    if re.fullmatch(r"-?\d{1,3}(?:\.\d{3})+", compact):
        compact = compact.replace(".", "")
    elif "," in compact and "." not in compact:
        compact = compact.replace(",", ".")
    try:
        number = Decimal(compact)
    except Exception as exc:
        raise CanonicalPopulationError(f"invalid_population:{context}:{raw}") from exc
    if number <= 0:
        raise CanonicalPopulationError(f"nonpositive_population:{context}:{raw}")
    integral = number.to_integral_value(rounding=ROUND_HALF_UP)
    return int(integral)


def _read_legacy(path: Path, required_years: range) -> dict[str, DepartmentSeries]:
    path = Path(path).expanduser().resolve()
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        fields = set(reader.fieldnames or [])
        required = {"DPTO", "NOMDPTO", *(str(y) for y in required_years)}
        missing = sorted(required - fields)
        if missing:
            raise CanonicalPopulationError(
                "legacy_source_missing_columns:" + ",".join(missing)
            )
        out: dict[str, DepartmentSeries] = {}
        for row in reader:
            department_id = _normalize_id(row["DPTO"])
            if department_id in out:
                raise CanonicalPopulationError(
                    f"duplicate_legacy_department:{department_id}"
                )
            name = (row["NOMDPTO"] or "").strip()
            if not name:
                raise CanonicalPopulationError(
                    f"empty_legacy_department_name:{department_id}"
                )
            values = {
                y: _parse_positive_int(
                    row[str(y)], context=f"legacy:{department_id}:{y}"
                )
                for y in required_years
            }
            out[department_id] = DepartmentSeries(department_id, name, values)
    if not out:
        raise CanonicalPopulationError("legacy_source_empty")
    return out


def _header_map(fieldnames: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for field in fieldnames:
        key = _normalize_name(field)
        result[key] = field
    return result


def _extract_year(value: str) -> int:
    raw = (value or "").strip()
    match = re.search(r"(20\d{2})", raw)
    if not match:
        raise CanonicalPopulationError(f"invalid_updated_date:{raw}")
    return int(match.group(1))


def _is_total_sex(value: str) -> bool:
    raw = _normalize_name(value)
    return raw in {"0", "na", "ambos sexos", "total", "total ambos sexos"}


def _read_updated(path: Path) -> dict[str, DepartmentSeries]:
    """Read the official INDEC 2022-2035 CSV and retain both-sex totals only."""
    path = Path(path).expanduser().resolve()
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, delimiter=";")
        fieldnames = list(reader.fieldnames or [])
        mapped = _header_map(fieldnames)
        wanted = {
            "codigo departamento": None,
            "nombre departamento": None,
            "sexo": None,
            "poblacion": None,
            "fecha": None,
        }
        for key in list(wanted):
            if key not in mapped:
                raise CanonicalPopulationError(f"updated_source_missing_column:{key}")
            wanted[key] = mapped[key]

        names: dict[str, str] = {}
        values: dict[str, dict[int, int]] = {}
        for row in reader:
            sex = row[wanted["sexo"]]
            if not _is_total_sex(sex):
                continue
            department_id = _normalize_id(row[wanted["codigo departamento"]])
            name = (row[wanted["nombre departamento"]] or "").strip()
            if not name:
                raise CanonicalPopulationError(
                    f"empty_updated_department_name:{department_id}"
                )
            year = _extract_year(row[wanted["fecha"]])
            raw_population = (row[wanted["poblacion"]] or "").strip()
            if not raw_population or raw_population.upper() in {"NA", "N/A", "ND", "-", "..."}:
                # INDEC explicitly publishes no estimate for Islas del Atlántico Sur.
                continue
            population = _parse_positive_int(
                raw_population, context=f"updated:{department_id}:{year}"
            )
            if year in values.setdefault(department_id, {}):
                raise CanonicalPopulationError(
                    f"duplicate_updated_department_year:{department_id}:{year}"
                )
            values[department_id][year] = population
            names[department_id] = name

    if not values:
        raise CanonicalPopulationError("updated_source_no_total_rows")
    out = {
        department_id: DepartmentSeries(department_id, names[department_id], years)
        for department_id, years in values.items()
    }
    all_years = sorted({year for series in out.values() for year in series.values})
    if not all_years or all_years[0] != UPDATED_START:
        raise CanonicalPopulationError(
            f"updated_source_unexpected_first_year:{all_years[:1]}"
        )
    expected = list(range(all_years[0], all_years[-1] + 1))
    if all_years != expected:
        raise CanonicalPopulationError(
            f"updated_source_noncontinuous_years:{all_years}"
        )
    for department_id, series in out.items():
        missing = sorted(set(expected) - set(series.values))
        if missing:
            raise CanonicalPopulationError(
                f"updated_source_missing_years:{department_id}:{missing}"
            )
    return out


def _read_overrides(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    path = Path(path).expanduser().resolve()
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"legacy_department_id", "updated_department_id"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise CanonicalPopulationError("override_missing_columns")
        out: dict[str, str] = {}
        for row in reader:
            legacy = _normalize_id(row["legacy_department_id"])
            updated = _normalize_id(row["updated_department_id"])
            if legacy in out:
                raise CanonicalPopulationError(f"duplicate_override:{legacy}")
            out[legacy] = updated
    return out


def _alignment(
    legacy: dict[str, DepartmentSeries],
    updated: dict[str, DepartmentSeries],
    overrides: dict[str, str],
) -> tuple[dict[str, str], list[dict[str, str]], list[str]]:
    by_name: dict[tuple[str, str], list[str]] = {}
    for updated_id, series in updated.items():
        province = updated_id[:2]
        by_name.setdefault((province, _normalize_name(series.department_name)), []).append(
            updated_id
        )

    mapping: dict[str, str] = {}
    rows: list[dict[str, str]] = []
    unresolved: list[str] = []
    for legacy_id, series in sorted(legacy.items()):
        method = ""
        updated_id = ""
        if legacy_id in overrides:
            updated_id = overrides[legacy_id]
            method = "explicit_override"
            if updated_id not in updated:
                raise CanonicalPopulationError(
                    f"override_updated_department_missing:{legacy_id}:{updated_id}"
                )
        elif legacy_id in updated:
            updated_id = legacy_id
            method = "same_code"
        else:
            candidates = by_name.get(
                (legacy_id[:2], _normalize_name(series.department_name)), []
            )
            if len(candidates) == 1:
                updated_id = candidates[0]
                method = "same_normalized_name"
            elif len(candidates) > 1:
                raise CanonicalPopulationError(
                    f"ambiguous_name_alignment:{legacy_id}:{candidates}"
                )
        if not updated_id:
            unresolved.append(legacy_id)
            rows.append(
                {
                    "legacy_department_id": legacy_id,
                    "legacy_department_name": series.department_name,
                    "updated_department_id": "",
                    "updated_department_name": "",
                    "match_method": "unresolved",
                    "bridge_eligible": "false",
                }
            )
            continue
        mapping[legacy_id] = updated_id
        rows.append(
            {
                "legacy_department_id": legacy_id,
                "legacy_department_name": series.department_name,
                "updated_department_id": updated_id,
                "updated_department_name": updated[updated_id].department_name,
                "match_method": method,
                "bridge_eligible": "true",
            }
        )
    return mapping, rows, unresolved


def _assert_2010_compatibility(
    history: dict[str, DepartmentSeries],
    projection: dict[str, DepartmentSeries],
) -> None:
    if set(history) != set(projection):
        raise CanonicalPopulationError("legacy_source_department_universe_mismatch")
    mismatches = [
        department_id
        for department_id in history
        if history[department_id].values[ANCHOR_YEAR]
        != projection[department_id].values[ANCHOR_YEAR]
    ]
    if mismatches:
        raise CanonicalPopulationError(
            "legacy_2010_anchor_mismatch:" + ",".join(mismatches[:20])
        )


def _round_mass(value: Decimal) -> int:
    mass = int(value.to_integral_value(rounding=ROUND_HALF_UP))
    if mass <= 0:
        raise CanonicalPopulationError(f"derived_nonpositive_population:{mass}")
    return mass


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def build_canonical_department_population(
    legacy_history_path: Path,
    legacy_projection_path: Path,
    updated_source_path: Path,
    output_root: Path,
    *,
    geography_overrides_path: Path | None = None,
) -> Path:
    history = _read_legacy(
        legacy_history_path, range(HISTORY_START, ANCHOR_YEAR + 1)
    )
    projection = _read_legacy(
        legacy_projection_path, range(ANCHOR_YEAR, UPDATED_START + 1)
    )
    _assert_2010_compatibility(history, projection)

    updated = _read_updated(updated_source_path)
    updated_years = sorted({year for s in updated.values() for year in s.values})
    max_year = updated_years[-1]
    overrides = _read_overrides(geography_overrides_path)
    mapping, alignment_rows, unresolved = _alignment(projection, updated, overrides)
    if unresolved:
        raise CanonicalPopulationError(
            "unresolved_geography_alignment:" + ",".join(unresolved[:50])
        )

    rows: list[dict[str, object]] = []
    terminal_ratios: dict[str, Decimal] = {}
    for legacy_id, legacy_series in sorted(projection.items()):
        updated_id = mapping[legacy_id]
        legacy_2022 = legacy_series.values[UPDATED_START]
        updated_2022 = updated[updated_id].values[UPDATED_START]
        terminal_ratios[legacy_id] = Decimal(updated_2022) / Decimal(legacy_2022)

    # Trusted historical levels, period-native legacy geography.
    for legacy_id, series in sorted(history.items()):
        updated_id = mapping[legacy_id]
        terminal_ratio = terminal_ratios[legacy_id]
        for year in range(HISTORY_START, ANCHOR_YEAR + 1):
            rows.append(
                {
                    "department_id": legacy_id,
                    "department_name": series.department_name,
                    "target_year": year,
                    "target_person_mass": series.values[year],
                    "value_status": "legacy_official_copy",
                    "geography_vintage": "legacy_pre_2022",
                    "source_family": "trusted_legacy_2001_plus_copy",
                    "legacy_department_id": legacy_id,
                    "updated_department_id": updated_id,
                    "bridge_alpha": "0",
                    "bridge_scale": "1",
                    "terminal_2022_ratio": str(terminal_ratio),
                }
            )

    # Bridge the documented legacy trajectory toward the revised 2022 level.
    span = Decimal(UPDATED_START - ANCHOR_YEAR)
    for legacy_id, series in sorted(projection.items()):
        updated_id = mapping[legacy_id]
        ratio = terminal_ratios[legacy_id]
        for year in range(ANCHOR_YEAR + 1, UPDATED_START):
            alpha = Decimal(year - ANCHOR_YEAR) / span
            scale = Decimal(1) + alpha * (ratio - Decimal(1))
            unrounded = Decimal(series.values[year]) * scale
            rows.append(
                {
                    "department_id": legacy_id,
                    "department_name": series.department_name,
                    "target_year": year,
                    "target_person_mass": _round_mass(unrounded),
                    "value_status": "derived_linear_vintage_bridge",
                    "geography_vintage": "legacy_pre_2022",
                    "source_family": "indec_2010_2025_projection_bridged_to_cpv2022",
                    "legacy_department_id": legacy_id,
                    "updated_department_id": updated_id,
                    "bridge_alpha": str(alpha),
                    "bridge_scale": str(scale),
                    "terminal_2022_ratio": str(ratio),
                }
            )

    # Updated official estimates, period-native updated geography.
    for updated_id, series in sorted(updated.items()):
        for year in updated_years:
            rows.append(
                {
                    "department_id": updated_id,
                    "department_name": series.department_name,
                    "target_year": year,
                    "target_person_mass": series.values[year],
                    "value_status": "direct_indec_2022_based_estimate",
                    "geography_vintage": "cpv2022_department",
                    "source_family": "indec_estimaciones_departamentos_2022_2035",
                    "legacy_department_id": "",
                    "updated_department_id": updated_id,
                    "bridge_alpha": "1",
                    "bridge_scale": "1",
                    "terminal_2022_ratio": "1",
                }
            )

    rows.sort(key=lambda r: (int(r["target_year"]), str(r["department_id"])))

    legacy_history = Path(legacy_history_path).expanduser().resolve()
    legacy_projection = Path(legacy_projection_path).expanduser().resolve()
    updated_source = Path(updated_source_path).expanduser().resolve()
    source_hashes = {
        "legacy_history_sha256": _sha256(legacy_history),
        "legacy_projection_sha256": _sha256(legacy_projection),
        "updated_source_sha256": _sha256(updated_source),
    }
    identity = {
        "contract": CONTRACT,
        "method_id": METHOD_ID,
        **source_hashes,
        "coverage": [HISTORY_START, max_year],
        "anchor_year": ANCHOR_YEAR,
        "updated_start": UPDATED_START,
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    release_id = f"arg-department-pop-canonical-{digest[:16]}"
    output_root = Path(output_root).expanduser().resolve()
    destination = output_root / release_id
    if destination.exists():
        manifest_path = destination / "manifest.json"
        if manifest_path.is_file():
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
            if existing.get("release_id") == release_id:
                return destination
        raise CanonicalPopulationError(f"immutable_release_exists:{destination}")

    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{release_id}.", dir=output_root))
    try:
        target_path = staging / "target_population.csv"
        _write_csv(target_path, OUTPUT_FIELDS, rows)
        alignment_path = staging / "geography_alignment.csv"
        _write_csv(
            alignment_path,
            [
                "legacy_department_id",
                "legacy_department_name",
                "updated_department_id",
                "updated_department_name",
                "match_method",
                "bridge_eligible",
            ],
            alignment_rows,
        )
        updated_only = sorted(set(updated) - set(mapping.values()))
        updated_only_rows = [
            {
                "updated_department_id": department_id,
                "updated_department_name": updated[department_id].department_name,
                "first_year": UPDATED_START,
                "status": "updated_geography_only_no_historical_bridge",
            }
            for department_id in updated_only
        ]
        updated_only_path = staging / "updated_only_departments.csv"
        _write_csv(
            updated_only_path,
            ["updated_department_id", "updated_department_name", "first_year", "status"],
            updated_only_rows,
        )

        year_counts: dict[str, int] = {}
        year_totals: dict[str, int] = {}
        for row in rows:
            year = str(row["target_year"])
            year_counts[year] = year_counts.get(year, 0) + 1
            year_totals[year] = year_totals.get(year, 0) + int(row["target_person_mass"])

        ratio_values = [float(x) for x in terminal_ratios.values()]
        qa = {
            "status": "pass",
            "coverage_start": HISTORY_START,
            "coverage_end": max_year,
            "anchor_year": ANCHOR_YEAR,
            "updated_start": UPDATED_START,
            "history_department_count": len(history),
            "projection_department_count": len(projection),
            "updated_department_count": len(updated),
            "updated_only_department_count": len(updated_only),
            "unresolved_alignment_count": 0,
            "year_row_counts": year_counts,
            "year_population_totals": year_totals,
            "bridge_terminal_ratio_min": min(ratio_values),
            "bridge_terminal_ratio_max": max(ratio_values),
            "bridge_terminal_ratio_mean": sum(ratio_values) / len(ratio_values),
            "bridge_formula": (
                "legacy[y] * (1 + ((y-2010)/12) * "
                "((updated[2022]/legacy[2022]) - 1))"
            ),
            "anchor_checks": {
                "2010_scale_is_one": True,
                "2022_updated_source_is_direct": True,
            },
        }
        qa_path = staging / "qa.json"
        qa_path.write_text(_canonical_json(qa), encoding="utf-8")

        manifest = {
            "contract": CONTRACT,
            "release_id": release_id,
            "status": "research_canonical_target_mass",
            "method_id": METHOD_ID,
            "coverage": {
                "target_years": list(range(HISTORY_START, max_year + 1)),
                "start_year": HISTORY_START,
                "end_year": max_year,
                "mass_unit": "person",
                "reference_date": "July 1",
            },
            "sources": {
                "legacy_history": {
                    "path": LEGACY_HISTORY_REPO_PATH,
                    "sha256": source_hashes["legacy_history_sha256"],
                    "role": "trusted official-copy levels 2001-2010",
                },
                "legacy_projection": {
                    "path": LEGACY_PROJECTION_REPO_PATH,
                    "sha256": source_hashes["legacy_projection_sha256"],
                    "role": "documented INDEC 2010-2025 trajectory for bridge years",
                },
                "updated_2022_2035": {
                    "url": UPDATED_SOURCE_URL,
                    "metadata_url": UPDATED_METADATA_URL,
                    "publication_url": UPDATED_PUBLICATION_URL,
                    "sha256": source_hashes["updated_source_sha256"],
                    "role": "direct Census-2022-based INDEC estimates from 2022 onward",
                },
            },
            "method": {
                "2001_2010": "trusted legacy levels unchanged",
                "2011_2021": (
                    "legacy projection levels times a department-specific linear "
                    "multiplicative bridge from scale 1 in 2010 to "
                    "updated_2022/legacy_2022 in 2022"
                ),
                "2022_onward": "direct updated INDEC estimates",
                "rounding": "nearest integer, ROUND_HALF_UP",
                "geography": (
                    "period-native IDs; explicit alignment is used only to compute "
                    "the 2022 terminal ratio for historical bridge rows"
                ),
            },
            "artifacts": {
                "target_population.csv": {
                    "sha256": _sha256(target_path),
                    "size_bytes": target_path.stat().st_size,
                },
                "geography_alignment.csv": {
                    "sha256": _sha256(alignment_path),
                    "size_bytes": alignment_path.stat().st_size,
                },
                "updated_only_departments.csv": {
                    "sha256": _sha256(updated_only_path),
                    "size_bytes": updated_only_path.stat().st_size,
                },
                "qa.json": {
                    "sha256": _sha256(qa_path),
                    "size_bytes": qa_path.stat().st_size,
                },
            },
            "limitations": [
                "2011-2021 values are a research bridge, not official INDEC estimates.",
                "The bridge adjusts levels only and does not reproduce INDEC demographic estimation methodology.",
                "Administrative geography is not forced to be constant across the 2022 seam.",
                "Updated-only departments have no fabricated pre-2022 history.",
                "The product is a sampling target-mass input, not an official synthetic population.",
            ],
        }
        manifest_path = staging / "manifest.json"
        manifest_path.write_text(_canonical_json(manifest), encoding="utf-8")
        staging.replace(destination)
        return destination
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def fetch_updated_source(destination: Path) -> Path:
    """Fetch the exact official INDEC 2022-2035 CSV to a caller-owned path."""
    destination = Path(destination).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        UPDATED_SOURCE_URL,
        headers={"User-Agent": "samplerCensoARG/1 canonical-population-research"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = response.read()
    if not payload:
        raise CanonicalPopulationError("updated_source_download_empty")
    destination.write_bytes(payload)
    return destination


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m censo_sampler.canonical_population")
    sub = parser.add_subparsers(dest="command", required=True)

    fetch = sub.add_parser("fetch-updated")
    fetch.add_argument("--output", required=True)

    build = sub.add_parser("build")
    build.add_argument("--legacy-history", default=LEGACY_HISTORY_REPO_PATH)
    build.add_argument("--legacy-projection", default=LEGACY_PROJECTION_REPO_PATH)
    build.add_argument("--updated-source", required=True)
    build.add_argument("--geography-overrides")
    build.add_argument("--output-root", required=True)

    args = parser.parse_args(argv)
    if args.command == "fetch-updated":
        print(fetch_updated_source(Path(args.output)))
        return 0
    if args.command == "build":
        release = build_canonical_department_population(
            Path(args.legacy_history),
            Path(args.legacy_projection),
            Path(args.updated_source),
            Path(args.output_root),
            geography_overrides_path=(
                Path(args.geography_overrides) if args.geography_overrides else None
            ),
        )
        print(release)
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(_main())
