# coding: utf-8
"""
Export utilities — CSV kept as in original. Parquet + manifest will be added next.
"""

from pathlib import Path


def ensure_out_dir(out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)


def write_year_csv(df, *, frac: float, year: str, name: str, out_dir: Path):
    filename = out_dir / f"table_f{frac}_{year}_{name}.csv"
    df.to_csv(filename, index=False, single_file=True)
    print(f"[write] Data saved to {filename}")
