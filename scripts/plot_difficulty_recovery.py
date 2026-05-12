#!/usr/bin/env python3
"""Plot Zebra difficulty simulation CSV files."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt

from difficulty_csv import default_csv_paths, first_recovery_row, read_difficulty_csv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create side-by-side plots showing expected block spacing and "
            "difficulty by height and by elapsed time."
        )
    )
    parser.add_argument(
        "csv",
        nargs="*",
        type=Path,
        help="CSV file(s) to plot. Defaults to target/hash-rate-shock-sim/*.csv.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Directory for PNG output. Defaults to a plots/ directory next to "
            "each input CSV."
        ),
    )
    parser.add_argument(
        "--tolerance-percent",
        type=float,
        default=10.0,
        help="Recovery tolerance around target spacing. Default: 10.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=160,
        help="PNG output DPI. Default: 160.",
    )
    return parser.parse_args()


def add_recovery_line(axis, recovery_row, x_column: str, tolerance_percent: float) -> None:
    if recovery_row is None:
        return

    axis.axvline(
        recovery_row[x_column],
        color="0.25",
        linestyle="--",
        linewidth=1.2,
        label=f"First within {tolerance_percent:g}%",
    )


def add_panel(
    axis,
    df,
    x_column: str,
    x_label: str,
    recovery_row,
    tolerance_percent: float,
) -> None:
    spacing_color = "#1f77b4"
    difficulty_color = "#d95f02"
    target_color = "#2ca02c"

    axis.plot(
        df[x_column],
        df["expected_spacing_seconds"],
        color=spacing_color,
        linewidth=1.8,
        label="Expected spacing",
    )

    lower_bound = df["target_spacing_seconds"] * (1.0 - tolerance_percent / 100.0)
    upper_bound = df["target_spacing_seconds"] * (1.0 + tolerance_percent / 100.0)
    axis.fill_between(
        df[x_column],
        lower_bound,
        upper_bound,
        color=target_color,
        alpha=0.12,
        label=f"+/- {tolerance_percent:g}% target band",
    )
    axis.plot(
        df[x_column],
        df["target_spacing_seconds"],
        color=target_color,
        linestyle=":",
        linewidth=1.4,
        label="Target spacing",
    )

    add_recovery_line(axis, recovery_row, x_column, tolerance_percent)

    axis.set_xlabel(x_label)
    axis.set_ylabel("Expected spacing seconds", color=spacing_color)
    axis.tick_params(axis="y", labelcolor=spacing_color)
    axis.grid(True, axis="both", color="0.9", linewidth=0.8)

    difficulty_axis = axis.twinx()
    difficulty_axis.plot(
        df[x_column],
        df["difficulty_plot_value"],
        color=difficulty_color,
        linewidth=1.6,
        label=df.attrs["difficulty_label"],
    )
    difficulty_axis.set_ylabel(df.attrs["difficulty_label"], color=difficulty_color)
    difficulty_axis.tick_params(axis="y", labelcolor=difficulty_color)

    handles, labels = axis.get_legend_handles_labels()
    difficulty_handles, difficulty_labels = difficulty_axis.get_legend_handles_labels()
    axis.legend(
        handles + difficulty_handles,
        labels + difficulty_labels,
        loc="best",
        fontsize="small",
        frameon=True,
    )


def output_path_for(csv_path: Path, output_dir: Path | None) -> Path:
    if output_dir is None:
        output_dir = csv_path.parent / "plots"

    return output_dir / f"{csv_path.stem}_difficulty_recovery.png"


def plot_csv(csv_path: Path, output_dir: Path | None, tolerance_percent: float, dpi: int) -> Path:
    df = read_difficulty_csv(csv_path)
    recovery_row = first_recovery_row(df, tolerance_percent)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5), constrained_layout=True)
    fig.suptitle(
        f"{df.attrs['scenario']} difficulty recovery",
        fontsize=14,
        fontweight="bold",
    )

    add_panel(
        axes[0],
        df,
        "height",
        "Height",
        recovery_row,
        tolerance_percent,
    )
    add_panel(
        axes[1],
        df,
        "elapsed_minutes",
        df.attrs["elapsed_label"],
        recovery_row,
        tolerance_percent,
    )

    if recovery_row is None:
        recovery_text = f"No recovery within {tolerance_percent:g}%"
    else:
        recovery_text = (
            f"First within {tolerance_percent:g}%: height "
            f"{int(recovery_row['height'])}, "
            f"{recovery_row['elapsed_minutes']:.2f} minutes, "
            f"{recovery_row['expected_spacing_seconds']:.3f}s spacing"
        )

    fig.text(0.5, 0.01, recovery_text, ha="center", fontsize=10)

    plot_path = output_path_for(csv_path, output_dir)
    plot_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(plot_path, dpi=dpi)
    plt.close(fig)

    return plot_path


def main() -> None:
    args = parse_args()
    csv_paths = args.csv or default_csv_paths()

    if not csv_paths:
        raise SystemExit("No CSV files found. Pass CSV paths or generate target/hash-rate-shock-sim/*.csv.")

    for csv_path in csv_paths:
        plot_path = plot_csv(
            csv_path,
            args.output_dir,
            args.tolerance_percent,
            args.dpi,
        )
        print(plot_path)


if __name__ == "__main__":
    main()
