"""Modern frame-based CLI front door with legacy-command fallback."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .frame_2010 import build_cpv2010_frame
from .frame_contract import CensusFrameError, validate_frame
from .release_v2 import (
    MATERIALIZATION_MODES,
    SampleReleaseV2Error,
    build_sample_release_v2,
    validate_sample_release_v2,
)


def _frame_build_2010(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="censo-sampler frame build-2010")
    parser.add_argument("--databasepath", required=True)
    parser.add_argument("--geography")
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args(argv)
    path = build_cpv2010_frame(
        Path(args.databasepath),
        Path(args.output_root),
        geography_path=Path(args.geography) if args.geography else None,
    )
    print(path)
    return 0


def _frame_check(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="censo-sampler frame check")
    parser.add_argument("frame")
    parser.add_argument(
        "--shallow",
        action="store_true",
        help="verify custody/hashes without rescanning all frame relationships",
    )
    args = parser.parse_args(argv)
    result = validate_frame(Path(args.frame), deep=not args.shallow)
    print(
        json.dumps(
            {
                "contract": result["contract"],
                "frame_release_id": result["frame_release_id"],
                "census_vintage": result["census_vintage"],
                "status": result["status"],
                "counts": result["counts"],
            },
            sort_keys=True,
        )
    )
    return 0


def _sample(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="censo-sampler sample")
    parser.add_argument("--frame", required=True)
    parser.add_argument("--target-population", required=True)
    parser.add_argument("--target-year", type=int, required=True)
    parser.add_argument("--fraction", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=20260831)
    parser.add_argument(
        "--materialize",
        default="full-payload",
        choices=sorted(MATERIALIZATION_MODES),
    )
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args(argv)
    path = build_sample_release_v2(
        Path(args.frame),
        Path(args.output_root),
        target_population=Path(args.target_population),
        target_year=args.target_year,
        fraction=args.fraction,
        seed=args.seed,
        materialization=args.materialize,
    )
    print(path)
    return 0


def _check_release_v2(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="censo-sampler check-release-v2")
    parser.add_argument("release")
    args = parser.parse_args(argv)
    result = validate_sample_release_v2(Path(args.release))
    print(json.dumps(result, sort_keys=True))
    return 0


def _modern_dispatch(argv: list[str]) -> int | None:
    if argv[:2] == ["frame", "build-2010"]:
        return _frame_build_2010(argv[2:])
    if argv[:2] == ["frame", "check"]:
        return _frame_check(argv[2:])
    if argv and argv[0] == "sample":
        return _sample(argv[1:])
    if argv and argv[0] == "check-release-v2":
        return _check_release_v2(argv[1:])
    return None


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        result = _modern_dispatch(argv)
        if result is not None:
            return result
    except (CensusFrameError, SampleReleaseV2Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    # Keep the historical and v1 governed interfaces intact. The legacy module
    # is imported only when one of those commands is actually requested.
    from .cli import main as legacy_main

    return legacy_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
