"""Shared helpers for analyzing Zebra difficulty simulation CSV files."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV_GLOB = REPO_ROOT / "target" / "hash-rate-shock-sim" / "*.csv"


REQUIRED_COLUMNS = {
    "height",
    "target_spacing_seconds",
    "expected_spacing_seconds",
}


def default_csv_paths() -> list[Path]:
    """Return the CSV files produced by the hash-rate shock simulations."""

    return sorted(DEFAULT_CSV_GLOB.parent.glob(DEFAULT_CSV_GLOB.name))


def read_difficulty_csv(csv_path: Path) -> pd.DataFrame:
    """Load a simulation CSV and add normalized plotting columns."""

    df = pd.read_csv(csv_path)
    missing_columns = REQUIRED_COLUMNS.difference(df.columns)

    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"{csv_path} is missing required column(s): {missing}")

    df = df.copy()
    df["height"] = pd.to_numeric(df["height"])
    df["target_spacing_seconds"] = pd.to_numeric(df["target_spacing_seconds"])
    df["expected_spacing_seconds"] = pd.to_numeric(df["expected_spacing_seconds"])

    if "spacing_error_percent" not in df.columns:
        df["spacing_error_percent"] = (
            (df["expected_spacing_seconds"] - df["target_spacing_seconds"]).abs()
            / df["target_spacing_seconds"]
        ) * 100.0
    else:
        df["spacing_error_percent"] = pd.to_numeric(df["spacing_error_percent"])

    if "elapsed_since_shock_minutes" in df.columns:
        df["elapsed_minutes"] = pd.to_numeric(df["elapsed_since_shock_minutes"])
        df.attrs["elapsed_label"] = "Elapsed minutes since shock"
    elif "elapsed_since_shock_seconds" in df.columns:
        df["elapsed_minutes"] = pd.to_numeric(df["elapsed_since_shock_seconds"]) / 60.0
        df.attrs["elapsed_label"] = "Elapsed minutes since shock"
    else:
        df["elapsed_minutes"] = df["expected_spacing_seconds"].cumsum() / 60.0
        df.attrs["elapsed_label"] = "Elapsed minutes since first row"

    if "difficulty_ratio" in df.columns:
        df["difficulty_plot_value"] = pd.to_numeric(df["difficulty_ratio"])
        df.attrs["difficulty_label"] = "Difficulty ratio"
    elif "relative_difficulty" in df.columns:
        df["difficulty_plot_value"] = pd.to_numeric(df["relative_difficulty"])
        df.attrs["difficulty_label"] = "Relative difficulty"
    elif "difficulty_work_bits" in df.columns:
        df["difficulty_plot_value"] = pd.to_numeric(df["difficulty_work_bits"])
        df.attrs["difficulty_label"] = "Difficulty work bits"
    else:
        raise ValueError(
            f"{csv_path} needs one of difficulty_ratio, relative_difficulty, "
            "or difficulty_work_bits"
        )

    if "scenario" in df.columns and not df["scenario"].dropna().empty:
        df.attrs["scenario"] = str(df["scenario"].dropna().iloc[0])
    else:
        df.attrs["scenario"] = csv_path.stem

    if "blocks_after_shock" in df.columns:
        df["blocks_after_event"] = pd.to_numeric(df["blocks_after_shock"])
        df.attrs["event_label"] = "Blocks after shock"
    elif "blocks_after_activation" in df.columns:
        df["blocks_after_event"] = pd.to_numeric(df["blocks_after_activation"])
        df.attrs["event_label"] = "Blocks after activation"

    return df.replace([np.inf, -np.inf], np.nan).dropna(
        subset=[
            "height",
            "target_spacing_seconds",
            "expected_spacing_seconds",
            "spacing_error_percent",
            "elapsed_minutes",
            "difficulty_plot_value",
        ]
    )


def first_recovery_row(df: pd.DataFrame, tolerance_percent: float) -> pd.Series | None:
    """Return the first row within the target spacing tolerance."""

    recovered_rows = df[df["spacing_error_percent"] <= tolerance_percent]

    if recovered_rows.empty:
        return None

    return recovered_rows.iloc[0]
