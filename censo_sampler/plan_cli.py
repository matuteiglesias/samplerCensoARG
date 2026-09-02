"""CLI for non-materializing Census sample plans."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .planning import SamplePlanningError, plan_sample


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="censo-sample-plan")
    parser.add_argument("--frame", required=True)
    parser.add_argument("--target-population", required=True)
    parser.add_argument("--target-year", type=int, required=True)
    parser.add_argument("--fraction", type=float, default=0.01)
    parser.add_argument(
        "--details",
        action="store_true",
        help="include per-department diagnostics",
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    try:
        result = plan_sample(
            Path(args.frame),
            target_population=Path(args.target_population),
            target_year=args.target_year,
            fraction=args.fraction,
        )
    except SamplePlanningError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if not args.details:
        result = {key: value for key, value in result.items() if key != "departments"}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
