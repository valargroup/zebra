#!/usr/bin/env python3
"""Overlay two stage-timing runs: throughput and transparent keys/block by height.

For A/B comparisons where each run has a stage-timing.jsonl (per-block keys_* and
ts_us). Throughput comes from the stage timestamps, so it is NOT skewed by any
commit-timing trace overhead — both runs are measured the same, lightweight way.

Usage:
  python3 plot_ab_throughput_keys.py LABEL_A=stageA.jsonl LABEL_B=stageB.jsonl \
      --out-dir perf-artifacts --name skip-ab [--bins 90]
"""
import argparse
import json
import math
from pathlib import Path

COLORS = ["#d62728", "#2ca02c", "#1f77b4", "#ff7f0e"]


def load(path):
    ts, ktrans = {}, {}
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        s, h = r.get("stage", ""), r.get("height")
        if h is None:
            continue
        if s.startswith("keys_"):
            if s == "keys_transparent":
                ktrans[h] = r.get("val", 0)
            ts[h] = max(ts.get(h, 0), r.get("ts_us", 0))
    return ts, ktrans


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("runs", nargs="+", help="LABEL=stage.jsonl")
    ap.add_argument("--out-dir", default="perf-artifacts")
    ap.add_argument("--name", default="ab")
    ap.add_argument("--bins", type=int, default=90)
    args = ap.parse_args()

    runs = []
    for spec in args.runs:
        label, path = spec.split("=", 1)
        ts, kt = load(path)
        runs.append((label, ts, kt))

    h0 = min(min(ts) for _, ts, _ in runs)
    h1 = max(max(ts) for _, ts, _ in runs)
    width = max(1, math.ceil((h1 - h0 + 1) / args.bins))

    def throughput_bins(ts):
        buckets = {}
        for h, t in ts.items():
            buckets.setdefault((h - h0) // width, []).append((h, t))
        out = {}
        for k, pts in buckets.items():
            pts.sort()
            if len(pts) >= 2 and pts[-1][1] > pts[0][1]:
                out[k] = (pts[-1][0] - pts[0][0]) / ((pts[-1][1] - pts[0][1]) / 1e6)
        return out

    def key_bins(kt):
        buckets = {}
        for h, v in kt.items():
            buckets.setdefault((h - h0) // width, []).append(v)
        return {k: sum(v) / len(v) for k, v in buckets.items()}

    series = []
    for i, (label, ts, kt) in enumerate(runs):
        series.append((label, COLORS[i % len(COLORS)], throughput_bins(ts), key_bins(kt),
                       (h1 - h0) / ((max(ts.values()) - min(ts.values())) / 1e6) if len(ts) > 1 else 0))

    W, left, right = 1240, 96, 36
    plot_w = W - left - right
    panel_h, gap = 290, 92
    top1, top2 = 104, 104 + 290 + 92
    H = top2 + panel_h + 60

    bps_max = max(max(s[2].values(), default=1) for s in series) * 1.05
    key_max = max(max(s[3].values(), default=1) for s in series) * 1.05

    def xm(k):
        h = h0 + k * width + width // 2
        return left + (h - h0) / (h1 - h0) * plot_w

    def ymap(v, vmax, top):
        return top + panel_h - min(max(v, 0), vmax) / vmax * panel_h

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'font-family="-apple-system,Segoe UI,Roboto,sans-serif" font-size="13">',
        f'<rect width="{W}" height="{H}" fill="white"/>',
        f'<text x="{left}" y="30" font-size="17" font-weight="600">A/B by height — {args.name}</text>',
    ]

    # sandblast region band + per-run time-weighted rates for annotation
    SB_LO, SB_HI = 1862900, 1872000

    def region_rate(ts, lo, hi):
        sel = sorted((h, t) for h, t in ts.items() if lo <= h < hi)
        return (sel[-1][0] - sel[0][0]) / ((sel[-1][1] - sel[0][1]) / 1e6) if len(sel) > 1 and sel[-1][1] > sel[0][1] else 0
    sb_rates = [(label, color, region_rate(ts, SB_LO, SB_HI)) for (label, color, _, _, _), (_, ts, _) in zip(series, runs)]
    sb_x0 = left + (SB_LO - h0) / (h1 - h0) * plot_w
    sb_x1 = left + (SB_HI - h0) / (h1 - h0) * plot_w

    def panel(top, title, get, vmax, fmt, dashed=None, annotate_rates=False):
        svg.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{panel_h}" fill="#fafafa" stroke="#ddd"/>')
        # shade the sandblast region
        svg.append(f'<rect x="{sb_x0:.1f}" y="{top}" width="{sb_x1-sb_x0:.1f}" height="{panel_h}" fill="#000" opacity="0.05"/>')
        svg.append(f'<text x="{(sb_x0+sb_x1)/2:.0f}" y="{top+16}" text-anchor="middle" fill="#999" font-size="12">sandblast</text>')
        # title + inline legend
        x = left
        svg.append(f'<text x="{x}" y="{top-12}" font-weight="600">{title}:  </text>')
        x += int(8.6 * (len(title) + 3))
        for label, color, *_ in series:
            svg.append(f'<text x="{x}" y="{top-12}" fill="{color}" font-weight="700">{label}</text>')
            x += int(8.6 * len(label)) + 14
        for frac in (0, 0.25, 0.5, 0.75, 1.0):
            v = vmax * frac
            y = top + panel_h - frac * panel_h
            svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" stroke="#eee"/>')
            svg.append(f'<text x="{left-8}" y="{y+4:.1f}" text-anchor="end" fill="#888">{fmt(v)}</text>')
        for label, color, tb, kb, overall in series:
            data = get(tb, kb)
            pts = " ".join(f"{xm(k):.1f},{ymap(v, vmax, top):.1f}" for k, v in sorted(data.items()))
            svg.append(f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{pts}"/>')
            if dashed:
                y = ymap(overall, vmax, top)
                svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" stroke="{color}" stroke-width="1.3" stroke-dasharray="6,4" opacity="0.7"/>')
                svg.append(f'<text x="{left+plot_w-6}" y="{y-4:.1f}" text-anchor="end" fill="{color}" font-size="12">overall {overall:.0f}</text>')
        # annotate the per-run sandblast time-weighted rate inside the band
        if annotate_rates:
            for j, (label, color, r) in enumerate(sb_rates):
                ry = ymap(r, vmax, top)
                svg.append(f'<circle cx="{sb_x1+6:.1f}" cy="{ry:.1f}" r="3" fill="{color}"/>')
                svg.append(f'<text x="{sb_x1+12:.1f}" y="{ry+4:.1f}" fill="{color}" font-size="12" font-weight="700">{label} {r:.0f} blk/s</text>')

    panel(top1, "throughput (blk/s) — solid=per-bin, dashed=overall", lambda tb, kb: tb, bps_max, lambda v: f"{v:.0f}", dashed=True, annotate_rates=True)
    panel(top2, "transparent keys/block (UTXO set + address index)", lambda tb, kb: kb, key_max, lambda v: f"{v:.0f}")

    for i in range(6):
        h = h0 + (h1 - h0) * i / 5
        x = left + (h - h0) / (h1 - h0) * plot_w
        for top in (top1, top2):
            svg.append(f'<line x1="{x:.1f}" y1="{top+panel_h}" x2="{x:.1f}" y2="{top+panel_h+5}" stroke="#888"/>')
        svg.append(f'<text x="{x:.1f}" y="{top2+panel_h+22}" text-anchor="middle" fill="#444">{int(h):,}</text>')
    svg.append(f'<text x="{left+plot_w/2:.0f}" y="{H-8}" text-anchor="middle" fill="#444">block height</text>')
    svg.append("</svg>")

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    p = out / f"{args.name}-throughput-keys-ab.svg"
    p.write_text("\n".join(svg))
    print(f"wrote {p}")
    # time-weighted regional throughput
    def rate(ts, lo, hi):
        sel = sorted((h, t) for h, t in ts.items() if lo <= h < hi)
        return (sel[-1][0] - sel[0][0]) / ((sel[-1][1] - sel[0][1]) / 1e6) if len(sel) > 1 and sel[-1][1] > sel[0][1] else None
    print(f"  {'region':>22} " + " ".join(f"{lbl:>10}" for lbl, *_ in series))
    for name, lo, hi in [("light <1862900", h0, 1862900), ("sandblast 1862.9-1872k", 1862900, 1872000), ("recovery 1872k+", 1872000, h1 + 1)]:
        vals = [rate(ts, lo, hi) for _, ts, _ in runs]
        if all(v is not None for v in vals):
            print(f"  {name:>22} " + " ".join(f"{v:>10.0f}" for v in vals))


if __name__ == "__main__":
    main()
