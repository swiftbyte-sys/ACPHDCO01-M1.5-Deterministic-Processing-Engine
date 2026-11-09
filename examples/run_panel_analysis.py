#!/usr/bin/env python3
"""Run a reproducible synthetic panel analysis and save JSON output."""
import argparse
import json
from pathlib import Path

from acp_engine.analytics import analyze_panel, generate_synthetic_panel


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--teams", type=int, default=24)
    parser.add_argument("--periods", type=int, default=12)
    parser.add_argument("--bootstrap", type=int, default=200)
    parser.add_argument("--seed", type=int, default=20261109)
    parser.add_argument("--output", default="panel-analysis-results.json")
    parser.add_argument("--csv", default="synthetic-panel.csv")
    args = parser.parse_args()
    data = generate_synthetic_panel(args.teams, args.periods, args.seed)
    result = analyze_panel(
        data, bootstrap_replicates=args.bootstrap, seed=args.seed, data_kind="synthetic"
    )
    data.to_csv(args.csv, index=False)
    payload = result.to_dict()
    Path(args.output).write_text(json.dumps(payload, indent=2, allow_nan=False))
    print(json.dumps(payload, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
