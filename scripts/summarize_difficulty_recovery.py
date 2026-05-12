#!/usr/bin/env python3
"""Print recovery summaries for Zebra difficulty simulation CSV files."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from difficulty_csv import default_csv_paths, first_recovery_row, read_difficulty_csv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize when each difficulty simulation recovers to target spacing."
    )
    parser.add_argument(
        "csv",
        nargs="*",
        type=Path,
        help="CSV file(s) to summarize. Defaults to target/hash-rate-shock-sim/*.csv.",
    )
    parser.add_argument(
        "--tolerance-percent",
        type=float,
        default=10.0,
        help="Recovery tolerance around target spacing. Default: 10.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        help="Optional path to write the summary as CSV.",
    )
    return parser.parse_args()


def summarize_csv(csv_path: Path, tolerance_percent: float) -> dict[str, object]:
    df = read_difficulty_csv(csv_path)
    recovery_row = first_recovery_row(df, tolerance_percent)

    summary: dict[str, object] = {
        "scenario": df.attrs["scenario"],
        "file": str(csv_path),
        "tolerance_percent": tolerance_percent,
        "target_spacing_seconds": df["target_spacing_seconds"].iloc[0],
        "hash_rate_percent": df["hash_rate_percent"].iloc[0]
        if "hash_rate_percent" in df.columns
        else None,
        "pow_averaging_window": df["pow_averaging_window"].iloc[0]
        if "pow_averaging_window" in df.columns
        else None,
        "pow_median_block_span": df["pow_median_block_span"].iloc[0]
        if "pow_median_block_span" in df.columns
        else None,
    }

    if recovery_row is None:
        summary.update(
            {
                "recovered": False,
                "recovery_height": None,
                "blocks_after_event": None,
                "elapsed_minutes": None,
                "expected_spacing_seconds": None,
                "spacing_error_percent": None,
                "difficulty_ratio": None,
            }
        )
    else:
        summary.update(
            {
                "recovered": True,
                "recovery_height": int(recovery_row["height"]),
                "blocks_after_event": int(recovery_row["blocks_after_event"])
                if "blocks_after_event" in recovery_row
                else None,
                "elapsed_minutes": recovery_row["elapsed_minutes"],
                "expected_spacing_seconds": recovery_row["expected_spacing_seconds"],
                "spacing_error_percent": recovery_row["spacing_error_percent"],
                "difficulty_ratio": recovery_row["difficulty_ratio"]
                if "difficulty_ratio" in recovery_row
                else None,
            }
        )

    return summary


def main() -> None:
    args = parse_args()
    csv_paths = args.csv or default_csv_paths()

    if not csv_paths:
        raise SystemExit("No CSV files found. Pass CSV paths or generate target/hash-rate-shock-sim/*.csv.")

    summaries = [summarize_csv(csv_path, args.tolerance_percent) for csv_path in csv_paths]
    summary_df = pd.DataFrame(summaries)

    if args.output_csv:
        args.output_csv.parent.mkdir(parents=True, exist_ok=True)
        summary_df.to_csv(args.output_csv, index=False)

    display_columns = [
        "scenario",
        "recovered",
        "recovery_height",
        "blocks_after_event",
        "elapsed_minutes",
        "expected_spacing_seconds",
        "spacing_error_percent",
    ]
    print(summary_df[display_columns].to_string(index=False))


if __name__ == "__main__":
    main()
