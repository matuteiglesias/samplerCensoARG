"""Govern the legacy-pinned INDEC 2010-2025 department population snapshot.

The repository already contains ``data/info/proy_pop200125.csv``. Historical
notebook evidence identifies that exact file family with INDEC's *Estimaciones
de población por sexo, departamento y año calendario 2010-2025* (Análisis
Demográfico 38). This module does not scrape or reinterpret the PDF. It turns
the exact committed bytes into a small, immutable 2024/2025 consumer parent
and fails if those source bytes drift.
"""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
import tempfile
from pathlib import Path

CONTRACT = "publicdata.argentina-department-population-target/v1"
SOURCE_GIT_BLOB_SHA1 = "2bfa46dc782d70399b5b749e1d71e38887dec5ca"
SOURCE_REPO_PATH = "data/info/proy_pop200125.csv"
TARGET_YEARS = (2024, 2025)
CANONICAL_FIELDS = [
    "department_2010_id",
    "department_name",
    "target_year",
    "target_person_mass",
]
OFFICIAL_PUBLICATION = (
    "INDEC, Estimaciones de población por sexo, departamento y año calendario "
    "2010-2025, Análisis Demográfico 38 (2015)"
)
OFFICIAL_URL = (
    "https://www.indec.gob.ar/ftp/cuadros/poblacion/"
    "proyeccion_departamentos_10_25.pdf"
)


class TargetPopulationError(ValueError):
    """Raised when the governed target-population parent cannot be built safely."""


def _canonical_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _git_blob_sha1(path: Path) -> str:
    payload = path.read_bytes()
    header = f"blob {len(payload)}\0".encode()
    return hashlib.sha1(header + payload).hexdigest()


def _read_source(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        rows = [
            {key: (value or "").strip() for key, value in row.items()}
            for row in csv.DictReader(stream)
        ]
    if not rows:
        raise TargetPopulationError("source_population_table_empty")
    required = {"DPTO", "NOMDPTO", *(str(year) for year in TARGET_YEARS)}
    missing = sorted(required - set(rows[0]))
    if missing:
        raise TargetPopulationError(
            "source_population_table_missing_columns:" + ",".join(missing)
        )
    return rows


def _canonical_rows(source_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    departments: set[str] = set()
    output: list[dict[str, str]] = []
    for row in source_rows:
        raw_department = row["DPTO"]
        if not raw_department.isdigit():
            raise TargetPopulationError(f"invalid_department_code:{raw_department}")
        department = raw_department.zfill(5)
        if department in departments:
            raise TargetPopulationError(f"duplicate_department_code:{department}")
        departments.add(department)
        name = row["NOMDPTO"].strip()
        if not name:
            raise TargetPopulationError(f"empty_department_name:{department}")
        for year in TARGET_YEARS:
            raw_mass = row[str(year)]
            try:
                mass = int(raw_mass)
            except ValueError as exc:
                raise TargetPopulationError(
                    f"invalid_target_person_mass:{department}:{year}"
                ) from exc
            if mass <= 0:
                raise TargetPopulationError(
                    f"nonpositive_target_person_mass:{department}:{year}"
                )
            output.append(
                {
                    "department_2010_id": department,
                    "department_name": name,
                    "target_year": str(year),
                    "target_person_mass": str(mass),
                }
            )
    output.sort(key=lambda row: (row["department_2010_id"], row["target_year"]))
    return output


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=CANONICAL_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def build_indec_2010_2025_target_parent(source_path: Path, output_root: Path) -> Path:
    """Materialize an immutable 2024/2025 parent from the exact committed snapshot."""
    source = Path(source_path).expanduser().resolve()
    output_root = Path(output_root).expanduser().resolve()
    if not source.is_file():
        raise TargetPopulationError("source_population_table_missing")
    source_blob_sha1 = _git_blob_sha1(source)
    if source_blob_sha1 != SOURCE_GIT_BLOB_SHA1:
        raise TargetPopulationError(
            f"source_population_snapshot_drift:{source_blob_sha1}"
        )

    source_rows = _read_source(source)
    canonical_rows = _canonical_rows(source_rows)
    source_sha256 = _sha256(source)
    identity = {
        "contract": CONTRACT,
        "source_git_blob_sha1": SOURCE_GIT_BLOB_SHA1,
        "source_sha256": source_sha256,
        "target_years": list(TARGET_YEARS),
        "geography_basis": "CPV-2010 department/partido/comuna identities",
        "mass_unit": "person",
        "reference_date": "July 1 of target year",
    }
    release_hash = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    release_id = f"arg-department-pop-target-2010-2025-{release_hash[:16]}"
    destination = output_root / release_id
    if destination.exists():
        existing = destination / "manifest.json"
        if existing.is_file():
            manifest = json.loads(existing.read_text(encoding="utf-8"))
            if manifest.get("release_id") == release_id:
                return destination
        raise TargetPopulationError(f"immutable_release_exists:{destination}")

    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{release_id}.", dir=output_root))
    try:
        population_path = staging / "target_population.csv"
        _write_csv(population_path, canonical_rows)
        qa = {
            "source_rows": len(source_rows),
            "canonical_rows": len(canonical_rows),
            "department_count": len(source_rows),
            "target_years": list(TARGET_YEARS),
            "unique_department_year": len(canonical_rows)
            == len({(row["department_2010_id"], row["target_year"]) for row in canonical_rows}),
            "sentinels": {
                "06028": {"2024": 612438, "2025": 616000},
                "06035": {"2024": 360583, "2025": 361532},
            },
        }
        values = {
            (row["department_2010_id"], row["target_year"]): int(row["target_person_mass"])
            for row in canonical_rows
        }
        for department, years in qa["sentinels"].items():
            for year, expected in years.items():
                if values.get((department, year)) != expected:
                    raise TargetPopulationError(
                        f"official_sentinel_mismatch:{department}:{year}"
                    )
        (staging / "qa.json").write_text(_canonical_json(qa), encoding="utf-8")
        manifest = {
            "contract": CONTRACT,
            "release_id": release_id,
            "status": "source_backed_snapshot",
            "source_snapshot": {
                "repository": "matuteiglesias/samplerCensoARG",
                "path": SOURCE_REPO_PATH,
                "git_blob_sha1": SOURCE_GIT_BLOB_SHA1,
                "sha256": source_sha256,
                "size_bytes": source.stat().st_size,
            },
            "provenance": {
                "publisher": "Instituto Nacional de Estadística y Censos (INDEC)",
                "publication": OFFICIAL_PUBLICATION,
                "official_url": OFFICIAL_URL,
                "publication_year": 2015,
                "table_semantics": (
                    "Population estimated at July 1 of each calendar year, by department; "
                    "partido for Buenos Aires and comuna for CABA."
                ),
                "evidence": [
                    "Historical repository notebook explicitly identifies the committed table with this INDEC publication.",
                    "2024/2025 Buenos Aires sentinel values were checked against the official publication before governing this snapshot.",
                ],
            },
            "coverage": {
                "target_years": list(TARGET_YEARS),
                "mass_unit": "person",
                "reference_date": "July 1",
                "geography_basis": "2010-era department/partido/comuna geography used by the publication",
            },
            "artifacts": {
                "target_population.csv": {
                    "sha256": _sha256(population_path),
                    "size_bytes": population_path.stat().st_size,
                },
                "qa.json": {
                    "sha256": _sha256(staging / "qa.json"),
                    "size_bytes": (staging / "qa.json").stat().st_size,
                },
            },
            "qa": qa,
            "limitations": [
                "This parent governs the exact legacy-pinned repository snapshot; it does not claim byte identity with the PDF publication.",
                "The estimates inherit INDEC's small-area projection methodology and its published limitations.",
                "This baseline is intentionally tied to 2010-era geography; Census-2022-based estimates require a separate explicit geography relation.",
                "data/info/proy_pop20012225.csv is not used because its repository provenance is unresolved.",
            ],
        }
        (staging / "manifest.json").write_text(
            _canonical_json(manifest), encoding="utf-8"
        )
        staging.replace(destination)
        return destination
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def validate_target_population_parent(root: Path) -> dict[str, object]:
    """Validate a governed target-population parent and its canonical payload."""
    root = Path(root).expanduser().resolve()
    manifest_path = root / "manifest.json"
    population_path = root / "target_population.csv"
    if not manifest_path.is_file() or not population_path.is_file():
        raise TargetPopulationError("target_population_parent_payload_missing")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise TargetPopulationError("target_population_parent_manifest_invalid") from exc
    if manifest.get("contract") != CONTRACT:
        raise TargetPopulationError("unexpected_target_population_contract")
    artifact = (manifest.get("artifacts") or {}).get("target_population.csv") or {}
    if artifact.get("sha256") != _sha256(population_path):
        raise TargetPopulationError("target_population_payload_hash_mismatch")
    coverage = manifest.get("coverage") or {}
    if coverage.get("target_years") != list(TARGET_YEARS):
        raise TargetPopulationError("target_population_year_coverage_mismatch")
    return manifest
