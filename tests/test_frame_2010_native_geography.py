from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from censo_sampler.frame_2010 import build_cpv2010_frame
from censo_sampler.frame_contract import validate_frame
from censo_sampler.geography_2010 import inspect_geography_source

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


def _native_geo(path: Path) -> Path:
    source = pacsv.read_csv(FIXTURE / "GEOGRAPHY.csv")
    rows = source.to_pylist()
    native = []
    for row in rows:
        native.append(
            {
                "PROV_REF_ID": 1,
                "CPV2010_REF_ID": 1,
                "IDPROV": int(str(row["department_2010_id"])[:2]),
                "DPTO_REF_ID": 1,
                "IDDPTO": int(row["department_2010_id"]),
                "FRAC_REF_ID": 1,
                "RADIO_REF_ID": int(row["RADIO_REF_ID"]),
                "IDRADIO": int(row["radio_2010_id"]),
            }
        )
    pq.write_table(pa.Table.from_pylist(native), path)
    return path


def test_native_geo_parquet_maps_iddpto_and_idradio(tmp_path: Path) -> None:
    geography = _native_geo(tmp_path / "GEO.parquet")
    profile = inspect_geography_source(geography)
    assert profile["radio_field"] == "IDRADIO"
    assert profile["department_field"] == "IDDPTO"

    frame = build_cpv2010_frame(
        FIXTURE,
        tmp_path / "frames",
        geography_path=geography,
    )
    checked = validate_frame(frame)
    assert checked["status"] == "valid"
    assert checked["counts"] == {
        "dwellings": 6,
        "households": 6,
        "persons": 10,
        "departments": 4,
    }

    hh = pq.read_table(frame / "frame_households.parquet").to_pylist()
    assert {row["department_id"] for row in hh} == {
        "02001",
        "50007",
        "90084",
        "94008",
    }
    assert all(len(row["radio_id"]) == 9 for row in hh)
