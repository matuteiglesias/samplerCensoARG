"""Normalize legacy CPV-2010 geography sources into the frame contract.

The user's historical converted Census corpus already contains a ``GEO.parquet``
whose stable source fields are ``RADIO_REF_ID``, ``IDRADIO`` and ``IDDPTO``.
Earlier sampler retrofit work temporarily expected a hand-normalized CSV with
``radio_2010_id`` / ``department_2010_id``.  This module accepts both forms and
keeps the normalization explicit at the frame-builder boundary.
"""
from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Iterator

from .frame_contract import CensusFrameError

RADIO_WIDTH = 9
DEPARTMENT_WIDTH = 5


def _csv_dialect(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        sample = stream.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        return csv.excel


def _csv_fields(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream, dialect=_csv_dialect(path)).fieldnames or [])


def _parquet_fields(path: Path) -> list[str]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise CensusFrameError("pyarrow_required_for_geography_parquet") from exc
    return list(pq.read_schema(path).names)


def _source_fields(path: Path) -> list[str]:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return _parquet_fields(path)
    if suffix in {".csv", ".txt"}:
        return _csv_fields(path)
    raise CensusFrameError(f"unsupported_geography_format:{suffix or '<none>'}")


def _pick(fields: set[str], canonical: str, aliases: tuple[str, ...]) -> str:
    if canonical in fields:
        return canonical
    for alias in aliases:
        if alias in fields:
            return alias
    expected = ",".join((canonical, *aliases))
    raise CensusFrameError(f"GEOGRAPHY:missing_required_semantic:{expected}")


def inspect_geography_source(path: Path) -> dict[str, object]:
    """Resolve source columns without modifying the geography file."""
    path = Path(path).expanduser().resolve()
    if not path.is_file():
        raise CensusFrameError("missing_geography_crosswalk")
    fields = set(_source_fields(path))
    if "RADIO_REF_ID" not in fields:
        raise CensusFrameError("GEOGRAPHY:missing_required_semantic:RADIO_REF_ID")
    radio_field = _pick(fields, "radio_2010_id", ("IDRADIO",))
    department_field = _pick(fields, "department_2010_id", ("IDDPTO",))
    return {
        "format": "parquet" if path.suffix.lower() == ".parquet" else "csv",
        "join_field": "RADIO_REF_ID",
        "radio_field": radio_field,
        "department_field": department_field,
        "normalization": {
            "radio_id": f"decimal-code-zero-pad-{RADIO_WIDTH}/v1",
            "department_id": f"decimal-code-zero-pad-{DEPARTMENT_WIDTH}/v1",
        },
    }


def _decimal_code(value: object, *, width: int, field: str) -> str:
    if value is None or isinstance(value, bool):
        raise CensusFrameError(f"GEOGRAPHY:invalid_{field}:{value!r}")
    if isinstance(value, int):
        number = value
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise CensusFrameError(f"GEOGRAPHY:invalid_{field}:{value!r}")
        number = int(value)
    else:
        text = str(value).strip()
        if not text or not text.isdigit():
            raise CensusFrameError(f"GEOGRAPHY:invalid_{field}:{value!r}")
        number = int(text)
    if number < 0 or number >= 10**width:
        raise CensusFrameError(f"GEOGRAPHY:invalid_{field}:{value!r}")
    return f"{number:0{width}d}"


def _radio_ref(value: object) -> str:
    if value is None or isinstance(value, bool):
        raise CensusFrameError(f"GEOGRAPHY:invalid_RADIO_REF_ID:{value!r}")
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise CensusFrameError(f"GEOGRAPHY:invalid_RADIO_REF_ID:{value!r}")
        value = int(value)
    text = str(value).strip()
    if not text:
        raise CensusFrameError("GEOGRAPHY:invalid_RADIO_REF_ID:''")
    return text


def _iter_raw(path: Path, columns: tuple[str, ...]) -> Iterator[dict[str, object]]:
    if path.suffix.lower() == ".parquet":
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:  # pragma: no cover
            raise CensusFrameError("pyarrow_required_for_geography_parquet") from exc
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=65536, columns=list(columns)):
            values = batch.to_pydict()
            for i in range(batch.num_rows):
                yield {name: values[name][i] for name in columns}
        return

    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, dialect=_csv_dialect(path))
        for row in reader:
            yield {name: row.get(name) for name in columns}


def iter_normalized_geography(path: Path) -> Iterator[dict[str, str]]:
    """Yield the neutral geography semantics required by the 2010 frame builder."""
    path = Path(path).expanduser().resolve()
    profile = inspect_geography_source(path)
    radio_field = str(profile["radio_field"])
    department_field = str(profile["department_field"])
    for row in _iter_raw(path, ("RADIO_REF_ID", radio_field, department_field)):
        yield {
            "RADIO_REF_ID": _radio_ref(row["RADIO_REF_ID"]),
            "radio_2010_id": _decimal_code(
                row[radio_field], width=RADIO_WIDTH, field=radio_field
            ),
            "department_2010_id": _decimal_code(
                row[department_field], width=DEPARTMENT_WIDTH, field=department_field
            ),
        }
