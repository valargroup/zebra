#!/usr/bin/env python3
"""Plot Zebra replay-bench commit + verify timing traces by height."""

from __future__ import annotations

import argparse
import html
import json
import math
from pathlib import Path


def load_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.open() if line.strip()]


def number(row: dict[str, object], key: str) -> float:
    try:
        return float(row.get(key, 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def height(row: dict[str, object]) -> int:
    return int(row["height"])


def percentile(values: list[float], percent: float) -> float:
    values = sorted(value for value in values if math.isfinite(value))
    if not values:
        return 1.0

    index = (len(values) - 1) * percent / 100
    low = math.floor(index)
    high = math.ceil(index)
    if low == high:
        return values[low]

    return values[low] * (high - index) + values[high] * (index - low)


def nice_max(values: list[float]) -> float:
    maximum = max([value for value in values if math.isfinite(value)] or [1.0])
    if maximum <= 0:
        return 1.0

    exponent = math.floor(math.log10(maximum))
    base = 10**exponent
    for multiplier in (1, 2, 5, 10):
        if maximum <= multiplier * base:
            return multiplier * base

    return 10 * base


def rolling_blocks_per_second(
    rows: list[dict[str, object]],
    window: int,
) -> tuple[list[int], list[float]]:
    xs: list[int] = []
    ys: list[float] = []

    for index in range(0, len(rows) - window):
        start = rows[index]
        end = rows[index + window]
        height_delta = height(end) - height(start)
        time_delta = (number(end, "ts_us") - number(start, "ts_us")) / 1_000_000

        if height_delta > 0 and time_delta > 0:
            xs.append(height(end))
            ys.append(height_delta / time_delta)

    return xs, ys


def polyline(
    xs: list[int],
    ys: list[float],
    *,
    min_height: int,
    max_height: int,
    max_value: float,
    left: int,
    width: int,
    right: int,
    panel_top: int,
    panel_height: int,
) -> str:
    plot_height = panel_height - 45
    points = []

    for x_value, y_value in zip(xs, ys):
        clamped = min(max(y_value, 0.0), max_value)
        x = left + (x_value - min_height) / (max_height - min_height) * (width - left - right)
        y = panel_top + 25 + plot_height - (clamped / max_value) * plot_height
        points.append(f"{x:.1f},{y:.1f}")

    return " ".join(points)


def write_svg(
    commit_rows: list[dict[str, object]],
    verify_rows: list[dict[str, object]],
    out_path: Path,
    *,
    title: str,
    window: int,
) -> None:
    commit_heights = [height(row) for row in commit_rows]
    verify_heights = [height(row) for row in verify_rows]
    min_height = min(commit_heights + verify_heights)
    max_height = max(commit_heights + verify_heights)
    if min_height == max_height:
        max_height += 1

    commit_phase_heights = commit_heights
    verify_phase_heights = verify_heights
    commit_stream_heights, commit_bps = rolling_blocks_per_second(commit_rows, window)
    verify_stream_heights, verify_bps = rolling_blocks_per_second(verify_rows, window)

    panels = [
        (
            "Commit phases (ms, y capped at p99.5)",
            99.5,
            [
                ("spent_utxo_reads", commit_phase_heights, [number(row, "reads_ms") for row in commit_rows], "#1f77b4"),
                ("address_reads", commit_phase_heights, [number(row, "address_reads_ms") for row in commit_rows], "#ff7f0e"),
                ("batch_assembly", commit_phase_heights, [number(row, "batch_assembly_ms") for row in commit_rows], "#2ca02c"),
                ("batch_commit", commit_phase_heights, [number(row, "batch_commit_ms") for row in commit_rows], "#d62728"),
            ],
        ),
        (
            "Commit total (ms, y capped at p99.5)",
            99.5,
            [
                ("commit_total", commit_phase_heights, [number(row, "commit_total_ms") for row in commit_rows], "#111111"),
            ],
        ),
        (
            "Verify phases (ms, y capped at p99.5)",
            99.5,
            [
                ("pow", verify_phase_heights, [number(row, "pow_ms") for row in verify_rows], "#9467bd"),
                ("precompute", verify_phase_heights, [number(row, "precompute_ms") for row in verify_rows], "#8c564b"),
                ("merkle", verify_phase_heights, [number(row, "merkle_ms") for row in verify_rows], "#17becf"),
            ],
        ),
        (
            "Rolling throughput from timing timestamps (blk/s)",
            100.0,
            [
                ("commit stream", commit_stream_heights, commit_bps, "#111111"),
                ("verify stream", verify_stream_heights, verify_bps, "#2ca02c"),
            ],
        ),
    ]

    width = 1500
    panel_height = 230
    left = 80
    right = 30
    top = 58
    gap = 52
    bottom = 45
    svg_height = top + len(panels) * panel_height + (len(panels) - 1) * gap + bottom

    svg: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{svg_height}" viewBox="0 0 {width} {svg_height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<style>text{font-family:Arial,sans-serif;font-size:13px;fill:#222}.title{font-size:18px;font-weight:bold}.panel{font-size:15px;font-weight:bold}.axis{stroke:#888;stroke-width:1}.grid{stroke:#ddd;stroke-width:1}.line{fill:none;stroke-width:1.5}</style>',
        f'<text x="{left}" y="30" class="title">{html.escape(title)}</text>',
    ]

    for panel_index, (panel_title, cap_percentile, series) in enumerate(panels):
        panel_top = top + panel_index * (panel_height + gap)
        values = [value for _, _, ys, _ in series for value in ys]
        max_value = nice_max([percentile(values, cap_percentile)])
        plot_height = panel_height - 45

        svg.append(
            f'<text x="{left}" y="{panel_top + 15}" class="panel">'
            f"{html.escape(panel_title)}; y_max={max_value:g}</text>"
        )

        for fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
            y = panel_top + 25 + plot_height - fraction * plot_height
            label = fraction * max_value
            svg.append(
                f'<line x1="{left}" x2="{width - right}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/>'
            )
            svg.append(f'<text x="8" y="{y + 4:.1f}">{label:.3g}</text>')

        svg.append(
            f'<line x1="{left}" x2="{width - right}" y1="{panel_top + 25 + plot_height}" '
            f'y2="{panel_top + 25 + plot_height}" class="axis"/>'
        )
        svg.append(
            f'<line x1="{left}" x2="{left}" y1="{panel_top + 25}" '
            f'y2="{panel_top + 25 + plot_height}" class="axis"/>'
        )

        legend_x = width - right - 260
        legend_y = panel_top + 15
        for series_index, (name, xs, ys, color) in enumerate(series):
            if xs and ys:
                points = polyline(
                    xs,
                    ys,
                    min_height=min_height,
                    max_height=max_height,
                    max_value=max_value,
                    left=left,
                    width=width,
                    right=right,
                    panel_top=panel_top,
                    panel_height=panel_height,
                )
                svg.append(f'<polyline points="{points}" class="line" stroke="{color}"/>')

            y = legend_y + series_index * 18
            svg.append(f'<line x1="{legend_x}" x2="{legend_x + 22}" y1="{y}" y2="{y}" stroke="{color}" stroke-width="2"/>')
            svg.append(f'<text x="{legend_x + 28}" y="{y + 4}">{html.escape(name)}</text>')

    for fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
        x = left + fraction * (width - left - right)
        value = int(min_height + fraction * (max_height - min_height))
        svg.append(f'<text x="{x - 28:.1f}" y="{svg_height - 18}">{value}</text>')

    svg.append(f'<text x="{width / 2 - 25:.1f}" y="{svg_height - 4}">height</text>')
    svg.append("</svg>")

    out_path.write_text("\n".join(svg))


def write_summary(
    commit_rows: list[dict[str, object]],
    verify_rows: list[dict[str, object]],
    out_path: Path,
    plot_path: Path,
) -> None:
    out_path.write_text(
        f"commit_samples: {len(commit_rows)}\n"
        f"verify_samples: {len(verify_rows)}\n"
        f"commit_height_range: {height(commit_rows[0])}-{height(commit_rows[-1])}\n"
        f"verify_height_range: {height(verify_rows[0])}-{height(verify_rows[-1])}\n"
        f"plot: {plot_path}\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("commit_timing", type=Path, help="commit timing JSONL path")
    parser.add_argument("verify_timing", type=Path, help="verify timing JSONL path")
    parser.add_argument("--out-dir", type=Path, default=Path("perf-artifacts"))
    parser.add_argument("--label", help="output file label; defaults to commit timing stem")
    parser.add_argument("--window", type=int, default=50, help="sample window for rolling throughput")
    args = parser.parse_args()

    commit_rows = load_jsonl(args.commit_timing)
    verify_rows = load_jsonl(args.verify_timing)
    if not commit_rows:
        raise SystemExit(f"no rows in {args.commit_timing}")
    if not verify_rows:
        raise SystemExit(f"no rows in {args.verify_timing}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    label = args.label or args.commit_timing.stem.removesuffix("-commit-timing")
    plot_path = args.out_dir / f"{label}-commit-verify-by-height.svg"
    summary_path = args.out_dir / f"{label}-commit-verify-summary.txt"

    write_svg(
        commit_rows,
        verify_rows,
        plot_path,
        title=f"{label} commit/verify timing by height",
        window=max(args.window, 1),
    )
    write_summary(commit_rows, verify_rows, summary_path, plot_path)

    print(plot_path)
    print(summary_path)


if __name__ == "__main__":
    main()
