#!/usr/bin/env python
# coding: utf-8
"""
CLI entrypoint — preserves original argparse interface and behavior.
We only orchestrate calls into io/sample/export modules.
"""

import argparse
import sys
from pathlib import Path

from . import io as io_mod
from . import sample as sample_mod
from . import export as export_mod
from . import validate as validate_mod


def parse_args(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('-dbp', '--databasepath', required=True, help='Path to the database')
    parser.add_argument('-f', '--frac', type=float, default=0.01, help='Fraction of the sample')
    parser.add_argument('-n', '--nombre', default='ARG', help='Name of the sample')
    parser.add_argument('-y', '--years', nargs=2, type=int, default=[2021, 2022],
                        help='Years to sample (start, end) — NOTE: half-open [start, end) kept for now')

    distritos = parser.add_mutually_exclusive_group()
    distritos.add_argument('-d', '--departamentos', nargs='+', type=int, help='Departments to sample')
    distritos.add_argument('-p', '--provincias', nargs='+', type=int, help='Provinces to sample')

    # Output directory (kept default as in original path layout)
    parser.add_argument('--out-dir', default=str(Path(__file__).resolve().parents[1] / 'data' / 'censo_samples'),
                        help='Output directory (defaults to ../data/censo_samples relative to repo)')

    args = parser.parse_args(argv)
    return args


import textwrap
import datetime
import socket

def print_run_banner(args):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    host = socket.gethostname()

    header = f"""
    ==========================================
      Census Sampler — Run Started
    ==========================================
      Timestamp    : {now}
      Host         : {host}
      Database Path: {args.databasepath}
      Fraction     : {args.frac}
      Sample Name  : {args.nombre}
      Years        : {args.years[0]} → {args.years[1]}  (half-open interval)
      Departamentos: {args.departamentos or "-"}
      Provincias   : {args.provincias or "-"}
      Output Dir   : {args.out_dir}
    ==========================================
    """
    print(textwrap.dedent(header))


def main(argv=None):
    args = parse_args(argv)

    # Pretty header instead of weak echo
    print_run_banner(args)

    censo_DB_path = args.databasepath
    frac = args.frac
    name = args.nombre
    startyr, endyr = args.years
    departamentos = args.departamentos
    provincias = args.provincias
    out_dir = Path(args.out_dir)

    # # Echo provided args (parity with original “show the results” loop)
    # for k, v in vars(args).items():
    #     if v is not None:
    #         print(f"{k} = {v}")

    # Load reference data + main tables
    proy_pop, ratios, radio_ref = io_mod.load_reference_data()
    VIVIENDA, HOGAR, PERSONA = io_mod.load_main_tables(
        censo_DB_path,
        radio_ref=radio_ref,
        departamentos=departamentos,
        provincias=provincias
    )

    # Basic validations (lightweight, non-fatal)
    validate_mod.validate_columns(VIVIENDA, ['VIVIENDA_REF_ID', 'RADIO_REF_ID'])
    validate_mod.validate_columns(HOGAR, ['HOGAR_REF_ID', 'VIVIENDA_REF_ID', 'RADIO_REF_ID', 'DPTO'])
    validate_mod.validate_columns(PERSONA, ['PERSONA_REF_ID', 'HOGAR_REF_ID', 'RADIO_REF_ID'])

    # Ensure output directory
    export_mod.ensure_out_dir(out_dir)

    # Run the original sampling loop (half-open [start, end))
    sample_mod.run_groupby_sampler_over_years(
        VIVIENDA=VIVIENDA,
        HOGAR=HOGAR,
        PERSONA=PERSONA,
        ratios=ratios,
        frac=frac,
        startyr=startyr,
        endyr=endyr,  # kept as half-open for now
        name=name,
        out_dir=out_dir
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
