#!/usr/bin/env python
# coding: utf-8
"""
CLI entrypoint — preserves original argparse interface and behavior.
We only orchestrate calls into io/sample/export modules.
"""

import argparse
import json
import sys
from pathlib import Path

from .release import ReleaseError, build_release, check_release
from .target_year import TargetYearSamplingError, build_target_year_release


def _release_parser():
    parser = argparse.ArgumentParser(prog="censo-sampler release")
    parser.add_argument("--databasepath", required=True)
    parser.add_argument("--fraction", type=float, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--analysis-period", required=True)
    parser.add_argument("--name", default="ARG")
    parser.add_argument("--weight-policy", default="cpv2010_frame_inverse_probability",
                        choices=["cpv2010_frame_inverse_probability", "legacy_department_projection_candidate"])
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--departments", nargs="*")
    parser.add_argument("--geography")
    parser.add_argument("--handoff-dir")
    parser.add_argument("--max-households", type=int, default=100000)
    return parser


def _release_main(argv):
    args = _release_parser().parse_args(argv)
    path = build_release(args.databasepath, args.output_root, fraction=args.fraction, seed=args.seed,
                         analysis_period=args.analysis_period, name=args.name, weight_policy=args.weight_policy,
                         departments=args.departments, geography_path=args.geography,
                         handoff_dir=args.handoff_dir, max_households=args.max_households)
    print(path)
    return 0


def _target_year_release_parser():
    parser = argparse.ArgumentParser(prog="censo-sampler target-year-release")
    parser.add_argument("--databasepath", required=True)
    parser.add_argument("--target-population", required=True)
    parser.add_argument("--target-source-id", required=True)
    parser.add_argument("--target-year", type=int, required=True, choices=[2024, 2025])
    parser.add_argument("--fraction", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=20260831)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--geography")
    parser.add_argument("--max-households", type=int, default=100000)
    return parser


def _target_year_release_main(argv):
    args = _target_year_release_parser().parse_args(argv)
    path = build_target_year_release(
        args.databasepath,
        args.output_root,
        target_population_path=args.target_population,
        target_source_id=args.target_source_id,
        target_year=args.target_year,
        fraction=args.fraction,
        seed=args.seed,
        geography_path=args.geography,
        max_households=args.max_households,
    )
    print(path)
    return 0


def _check_main(argv):
    parser = argparse.ArgumentParser(prog="censo-sampler check-release")
    parser.add_argument("release_dir")
    manifest = check_release(parser.parse_args(argv).release_dir)
    print(json.dumps({"release_id": manifest["release_id"], "status": "valid"}, sort_keys=True))
    return 0


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
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        if argv and argv[0] == "release":
            return _release_main(argv[1:])
        if argv and argv[0] == "target-year-release":
            return _target_year_release_main(argv[1:])
        if argv and argv[0] == "check-release":
            return _check_main(argv[1:])
    except (ReleaseError, TargetYearSamplingError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    # The historical sampler has optional pandas/Dask dependencies. Keep these
    # imports out of the stdlib-only release/check command path.
    from . import export as export_mod
    from . import io as io_mod
    from . import sample as sample_mod
    from . import validate as validate_mod
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
