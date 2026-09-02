"""Build a vintage-neutral Census frame from the legacy CPV-2010 CSV export."""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Iterator

from .frame_contract import FRAME_CONTRACT, CensusFrameError, canonical_json, sha256_file

SOURCE_FILES = ("VIVIENDA.csv", "HOGAR.csv", "PERSONA.csv")
BUILDER_VERSION = "cpv2010-csv-parquet/v1"


def _dialect(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        sample = stream.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        return csv.excel


def _iter_rows(path: Path) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, dialect=_dialect(path))
        if not reader.fieldnames:
            raise CensusFrameError(f"empty_csv_schema:{path.name}")
        for row in reader:
            yield {key: (value or "").strip() for key, value in row.items()}


def _require(fields: set[str], required: set[str], table: str) -> None:
    missing = sorted(required - fields)
    if missing:
        raise CensusFrameError(f"{table}:missing_required_columns:{','.join(missing)}")


def _csv_fields(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, dialect=_dialect(path))
        return list(reader.fieldnames or [])


def _write_parquet_rows(
    path: Path, rows: Iterator[dict[str, object]], *, batch_size: int = 50000
) -> int:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise CensusFrameError("pyarrow_required_for_frame_build") from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    batch: list[dict[str, object]] = []
    count = 0
    try:
        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                table = pa.Table.from_pylist(batch)
                if writer is None:
                    writer = pq.ParquetWriter(path, table.schema, compression="zstd")
                writer.write_table(table)
                count += len(batch)
                batch.clear()
        if batch:
            table = pa.Table.from_pylist(batch)
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema, compression="zstd")
            writer.write_table(table)
            count += len(batch)
        if writer is None:
            raise CensusFrameError(f"cannot_write_empty_parquet:{path.name}")
    finally:
        if writer is not None:
            writer.close()
    return count


def _payload_rows(path: Path, table: str) -> Iterator[dict[str, object]]:
    for row in _iter_rows(path):
        out: dict[str, object] = dict(row)
        if table == "VIVIENDA":
            out["frame_dwelling_id"] = row["VIVIENDA_REF_ID"]
        elif table == "HOGAR":
            out["frame_household_id"] = row["HOGAR_REF_ID"]
            out["frame_dwelling_id"] = row["VIVIENDA_REF_ID"]
        elif table == "PERSONA":
            out["frame_person_id"] = row["PERSONA_REF_ID"]
            out["frame_household_id"] = row["HOGAR_REF_ID"]
        else:  # pragma: no cover
            raise AssertionError(table)
        yield out


def build_cpv2010_frame(
    databasepath: Path,
    output_root: Path,
    *,
    geography_path: Path | None = None,
) -> Path:
    """Create an immutable full-payload ``research.census-frame/v1`` release."""
    source = Path(databasepath).expanduser().resolve()
    output_root = Path(output_root).expanduser().resolve()
    missing = [name for name in SOURCE_FILES if not (source / name).is_file()]
    if missing:
        raise CensusFrameError("missing_cpv2010_source_files:" + ",".join(missing))
    geography = (
        Path(geography_path).expanduser().resolve()
        if geography_path is not None
        else source / "GEOGRAPHY.csv"
    )
    if not geography.is_file():
        raise CensusFrameError("missing_geography_crosswalk")
    if output_root == source or source in output_root.parents:
        raise CensusFrameError("unsafe_output_path_inside_source")

    fields = {name: set(_csv_fields(source / name)) for name in SOURCE_FILES}
    _require(fields["VIVIENDA.csv"], {"VIVIENDA_REF_ID", "RADIO_REF_ID"}, "VIVIENDA")
    _require(fields["HOGAR.csv"], {"HOGAR_REF_ID", "VIVIENDA_REF_ID"}, "HOGAR")
    _require(fields["PERSONA.csv"], {"PERSONA_REF_ID", "HOGAR_REF_ID"}, "PERSONA")
    _require(
        set(_csv_fields(geography)),
        {"RADIO_REF_ID", "radio_2010_id", "department_2010_id"},
        "GEOGRAPHY",
    )

    source_meta = {
        name: {
            "sha256": sha256_file(source / name),
            "size_bytes": (source / name).stat().st_size,
        }
        for name in SOURCE_FILES
    }
    geography_meta = {
        "sha256": sha256_file(geography),
        "size_bytes": geography.stat().st_size,
    }
    identity = {
        "contract": FRAME_CONTRACT,
        "builder": BUILDER_VERSION,
        "country": "ARG",
        "census_vintage": 2010,
        "sources": {name: meta["sha256"] for name, meta in source_meta.items()},
        "geography": geography_meta["sha256"],
        "department_alignment_policy": "assume-code-identity/v1",
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    release_id = f"arg-cpv2010-frame-{digest[:16]}"
    destination = output_root / release_id
    if destination.exists():
        manifest_path = destination / "manifest.json"
        if manifest_path.is_file():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                manifest = {}
            if manifest.get("frame_release_id") == release_id:
                return destination
        raise CensusFrameError(f"immutable_frame_exists:{destination}")

    output_root.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=f".{release_id}.", dir=output_root))
    db_path = work / ".frame-index.sqlite"
    try:
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA temp_store=FILE")
            conn.execute(
                "CREATE TABLE geo (radio TEXT PRIMARY KEY, canonical_radio TEXT NOT NULL, dept TEXT NOT NULL)"
            )
            conn.execute("CREATE TABLE vivienda (viv TEXT PRIMARY KEY, radio TEXT NOT NULL)")
            conn.execute("CREATE TABLE hogar (hh TEXT PRIMARY KEY, viv TEXT NOT NULL)")
            conn.execute("CREATE TABLE persona (person TEXT PRIMARY KEY, hh TEXT NOT NULL)")
            try:
                conn.executemany(
                    "INSERT INTO geo(radio,canonical_radio,dept) VALUES (?,?,?)",
                    (
                        (row["RADIO_REF_ID"], row["radio_2010_id"], row["department_2010_id"])
                        for row in _iter_rows(geography)
                    ),
                )
                conn.executemany(
                    "INSERT INTO vivienda(viv,radio) VALUES (?,?)",
                    (
                        (row["VIVIENDA_REF_ID"], row["RADIO_REF_ID"])
                        for row in _iter_rows(source / "VIVIENDA.csv")
                    ),
                )
                conn.executemany(
                    "INSERT INTO hogar(hh,viv) VALUES (?,?)",
                    (
                        (row["HOGAR_REF_ID"], row["VIVIENDA_REF_ID"])
                        for row in _iter_rows(source / "HOGAR.csv")
                    ),
                )
                conn.executemany(
                    "INSERT INTO persona(person,hh) VALUES (?,?)",
                    (
                        (row["PERSONA_REF_ID"], row["HOGAR_REF_ID"])
                        for row in _iter_rows(source / "PERSONA.csv")
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise CensusFrameError("duplicate_or_empty_source_identity") from exc
            conn.commit()

            missing_geo = conn.execute(
                "SELECT COUNT(*) FROM vivienda v LEFT JOIN geo g ON g.radio=v.radio WHERE g.radio IS NULL"
            ).fetchone()[0]
            missing_viv = conn.execute(
                "SELECT COUNT(*) FROM hogar h LEFT JOIN vivienda v ON v.viv=h.viv WHERE v.viv IS NULL"
            ).fetchone()[0]
            orphan_people = conn.execute(
                "SELECT COUNT(*) FROM persona p LEFT JOIN hogar h ON h.hh=p.hh WHERE h.hh IS NULL"
            ).fetchone()[0]
            if missing_geo or missing_viv or orphan_people:
                raise CensusFrameError(
                    "source_relational_failure:"
                    f"missing_geography={missing_geo}:missing_viviendas={missing_viv}:"
                    f"orphan_people={orphan_people}"
                )
            empty_households = conn.execute(
                "SELECT COUNT(*) FROM hogar h LEFT JOIN persona p ON p.hh=h.hh WHERE p.hh IS NULL"
            ).fetchone()[0]
            if empty_households:
                raise CensusFrameError(f"households_without_persons:{empty_households}")

            conn.execute(
                "CREATE TABLE hh_index AS "
                "SELECT h.hh AS hh,h.viv AS viv,g.dept AS dept,g.canonical_radio AS radio,COUNT(p.person) AS n "
                "FROM hogar h JOIN vivienda v ON v.viv=h.viv "
                "JOIN geo g ON g.radio=v.radio JOIN persona p ON p.hh=h.hh "
                "GROUP BY h.hh,h.viv,g.dept,g.canonical_radio"
            )
            conn.execute("CREATE UNIQUE INDEX hh_index_pk ON hh_index(hh)")
            conn.commit()

            frame_household_rows = (
                {
                    "frame_household_id": str(hh),
                    "frame_dwelling_id": str(viv),
                    "department_id": str(dept),
                    "radio_id": str(radio),
                    "household_person_count": int(n),
                }
                for hh, viv, dept, radio, n in conn.execute(
                    "SELECT hh,viv,dept,radio,n FROM hh_index ORDER BY hh"
                )
            )
            household_count = _write_parquet_rows(
                work / "frame_households.parquet", frame_household_rows
            )
            donor_rows = [
                {"department_id": str(dept), "donor_person_mass": int(mass)}
                for dept, mass in conn.execute(
                    "SELECT dept,SUM(n) FROM hh_index GROUP BY dept ORDER BY dept"
                )
            ]
            _write_parquet_rows(work / "donor_person_mass.parquet", iter(donor_rows))
            person_count = conn.execute("SELECT COUNT(*) FROM persona").fetchone()[0]
            vivienda_count = conn.execute("SELECT COUNT(*) FROM vivienda").fetchone()[0]
            if sum(int(row["donor_person_mass"]) for row in donor_rows) != person_count:
                raise CensusFrameError("donor_person_mass_does_not_sum_to_person_count")
        finally:
            conn.close()

        payload = work / "payload"
        payload.mkdir()
        written_viv = _write_parquet_rows(
            payload / "vivienda.parquet",
            _payload_rows(source / "VIVIENDA.csv", "VIVIENDA"),
        )
        written_hh = _write_parquet_rows(
            payload / "hogar.parquet",
            _payload_rows(source / "HOGAR.csv", "HOGAR"),
        )
        written_person = _write_parquet_rows(
            payload / "persona.parquet",
            _payload_rows(source / "PERSONA.csv", "PERSONA"),
        )
        if (written_viv, written_hh, written_person) != (
            vivienda_count,
            household_count,
            person_count,
        ):
            raise CensusFrameError("payload_row_count_mismatch")

        artifact_names = (
            "frame_households.parquet",
            "donor_person_mass.parquet",
            "payload/vivienda.parquet",
            "payload/hogar.parquet",
            "payload/persona.parquet",
        )
        artifacts = {
            name: {
                "sha256": sha256_file(work / name),
                "size_bytes": (work / name).stat().st_size,
            }
            for name in artifact_names
        }
        manifest = {
            "contract": FRAME_CONTRACT,
            "frame_release_id": release_id,
            "country": "ARG",
            "census_vintage": 2010,
            "builder": BUILDER_VERSION,
            "source_release_id": "Argentina CPV-2010 local authorized copy",
            "source": {
                "tables": source_meta,
                "geography_crosswalk": geography_meta,
                "microdata_republished_outside_local_frame": False,
            },
            "department_alignment_policy": "assume-code-identity/v1",
            "identity": {
                "frame_dwelling_id": "VIVIENDA_REF_ID",
                "frame_household_id": "HOGAR_REF_ID",
                "frame_person_id": "PERSONA_REF_ID",
            },
            "counts": {
                "dwellings": written_viv,
                "households": written_hh,
                "persons": written_person,
                "departments": len(donor_rows),
            },
            "artifacts": artifacts,
            "payload_policy": "full-source-columns-plus-neutral-frame-ids/v1",
            "feature_projection": None,
            "limitations": [
                "Department code identity is provisionally assumed across Census vintages and target-population parents; mismatches must be surfaced by the sampler.",
                "The frame preserves source Census variables but does not define EPH semantic mappings.",
            ],
        }
        (work / "manifest.json").write_text(canonical_json(manifest), encoding="utf-8")
        db_path.unlink(missing_ok=True)
        work.replace(destination)
        return destination
    except Exception:
        shutil.rmtree(work, ignore_errors=True)
        raise
