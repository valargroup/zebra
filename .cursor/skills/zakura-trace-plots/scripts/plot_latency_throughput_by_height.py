#!/usr/bin/env python3
"""Plot per-block metric percentiles, throughput, and key composition by height.

Companion to plot_commit_verify_timing.py: a percentile-by-height view from a
commit-timing.jsonl stream, optionally with a stage-timing.jsonl stream for the
per-category key breakdown.

Panels (top -> bottom), all sharing the block-height x-axis:
  1. p50/p90 of a chosen metric (default block size, KB)
  2. throughput (blk/s) derived from ts_us deltas
  3. (if --stage-file given) commit batch keys/block, stacked by category
     (shielded nullifiers vs transparent UTXO+index vs other)

Usage:
  python3 plot_latency_throughput_by_height.py COMMIT_TIMING.jsonl \
      --stage-file STAGE.jsonl --out-dir perf-artifacts [--bins 150]
"""
import argparse
import json
import math
from pathlib import Path

BLUE, RED, GREEN = "#1f77b4", "#d62728", "#2ca02c"
# stacked key categories: (label, color, stage names summed into it)
KEY_CATS = [
    ("shielded (nullifiers)", "#9467bd", ["keys_shielded"]),
    ("transparent (UTXO+index)", "#ff7f0e", ["keys_transparent"]),
    ("other (header/trees/pool)", "#9aa0a6",
     ["keys_header_tx", "keys_trees", "keys_valuepool", "keys_vct_evict"]),
]


def load(path):
    rows = []
    for line in open(path):
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def percentile(values, pct):
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * pct / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return s[int(k)] if lo == hi else s[lo] * (hi - k) + s[hi] * (k - lo)


def nice_max(v):
    if v <= 0:
        return 1.0
    mag = 10 ** math.floor(math.log10(v))
    for m in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if m * mag >= v:
            return m * mag
    return 10 * mag


def x_map(h, h0, h1, left, w):
    return left + (h - h0) / (h1 - h0) * w


def lin_y(v, vmax, top, h):
    return top + h - min(max(v, 0.0), vmax) / vmax * h


def polyline(pts, color, wid=2.0):
    if not pts:
        return ""
    d = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    return f'<polyline fill="none" stroke="{color}" stroke-width="{wid}" points="{d}"/>'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("commit_timing")
    ap.add_argument("--stage-file", default=None)
    ap.add_argument("--out-dir", default="perf-artifacts")
    ap.add_argument("--bins", type=int, default=150)
    ap.add_argument("--metric", default="block_bytes")
    ap.add_argument("--scale", default="kb", choices=["kb", "mb", "raw", "ms"])
    ap.add_argument("--name", default=None)
    args = ap.parse_args()

    sf = {"kb": 1 / 1024, "mb": 1 / 1048576, "raw": 1.0, "ms": 1.0}[args.scale]
    unit = {"kb": "KB", "mb": "MB", "raw": "", "ms": "ms"}[args.scale]

    rows = sorted(load(args.commit_timing), key=lambda r: r["height"])
    if not rows:
        raise SystemExit("no rows")
    h0, h1 = rows[0]["height"], rows[-1]["height"]
    width = max(1, math.ceil((h1 - h0 + 1) / args.bins))

    def bin_by(items, key):
        b = {}
        for it in items:
            b.setdefault((key(it) - h0) // width, []).append(it)
        return b

    # commit-timing bins: metric p50/p90
    cb = bin_by(rows, lambda r: r["height"])
    mbins = []
    for k in sorted(cb):
        rs = cb[k]
        vals = [float(r.get(args.metric, 0.0)) * sf for r in rs]
        mbins.append({"h": h0 + k * width + width // 2,
                      "p50": percentile(vals, 50), "p90": percentile(vals, 90)})

    # stage-timing: per-category mean keys/block + per-height commit timestamps.
    # Throughput is taken from the stage stream when available (every block, so far
    # finer than the sampled commit-timing, and unskewed by the commit-trace cost).
    kbins = None
    ts_pairs = [(r["height"], r["ts_us"]) for r in rows]  # fallback: sampled commit-timing
    if args.stage_file and Path(args.stage_file).exists():
        per_height = {}
        per_height_ts = {}
        for r in load(args.stage_file):
            s = r.get("stage", "")
            h = r["height"]
            if s.startswith("keys_"):
                per_height.setdefault(h, {})[s] = r.get("val", 0)
            ts = r.get("ts_us")
            if ts is not None:
                per_height_ts[h] = max(per_height_ts.get(h, 0), ts)
        if per_height_ts:
            ts_pairs = sorted(per_height_ts.items())
        items = [{"h": h, **v} for h, v in per_height.items()]
        sb = bin_by(items, lambda it: it["h"])
        kbins = []
        for k in sorted(sb):
            rs = sb[k]
            row = {"h": h0 + k * width + width // 2}
            for label, _c, stages in KEY_CATS:
                row[label] = sum(sum(it.get(s, 0) for s in stages) for it in rs) / len(rs)
            kbins.append(row)

    # throughput bins from the chosen timestamp stream
    tb = bin_by([{"h": h, "ts": ts} for h, ts in ts_pairs], lambda it: it["h"])
    tbins = []
    for k in sorted(tb):
        rs = sorted(tb[k], key=lambda it: it["h"])
        bps = None
        if len(rs) >= 2:
            dt = (rs[-1]["ts"] - rs[0]["ts"]) / 1e6
            dh = rs[-1]["h"] - rs[0]["h"]
            if dt > 0 and dh > 0:
                bps = dh / dt
        tbins.append({"h": h0 + k * width + width // 2, "bps": bps})

    # ---- layout ----
    W, left, right = 1240, 96, 36
    plot_w = W - left - right
    panel_h, gap = 280, 86
    n_panels = 3 if kbins else 2
    tops = [104 + i * (panel_h + gap) for i in range(n_panels)]
    H = tops[-1] + panel_h + 64

    m_max = nice_max(max([b["p90"] for b in mbins] + [1.0]))
    bps_max = nice_max(max([b["bps"] for b in tbins if b["bps"] is not None] + [1.0]) * 1.05)
    keys_max = nice_max(max([sum(b[l] for l, _c, _s in KEY_CATS) for b in kbins] + [1.0])) if kbins else 1.0

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'font-family="-apple-system,Segoe UI,Roboto,sans-serif" font-size="13">',
        f'<rect width="{W}" height="{H}" fill="white"/>',
    ]
    title = args.name or Path(args.commit_timing).parent.name
    svg.append(f'<text x="{left}" y="30" font-size="17" font-weight="600">'
               f"Commit profile by height — {title}</text>")
    svg.append(f'<text x="{left}" y="52" fill="#666">{len(rows):,} sampled blocks, '
               f"heights {h0:,}–{h1:,}, {len(mbins)} bins</text>")

    def grid(top, vmax, fmt):
        for frac in (0, 0.25, 0.5, 0.75, 1.0):
            v = vmax * frac
            y = lin_y(v, vmax, top, panel_h)
            svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" stroke="#eee"/>')
            svg.append(f'<text x="{left-8}" y="{y+4:.1f}" text-anchor="end" fill="#888">{fmt(v)}</text>')

    def frame(top, segs):
        svg.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{panel_h}" fill="#fafafa" stroke="#ddd"/>')
        x = left
        for text, color, weight in segs:
            svg.append(f'<text x="{x}" y="{top-12}" fill="{color}" font-weight="{weight}">{text}</text>')
            x += int(8.6 * len(text))

    # Panel 1: metric p50/p90
    mlabel = args.metric.replace("_", " ")
    frame(tops[0], [(f"{mlabel} ({unit}) — ", "#222", "600"), ("p50", BLUE, "700"),
                    ("  /  ", "#888", "400"), ("p90", RED, "700")])
    grid(tops[0], m_max, lambda v: f"{v:.0f} {unit}")
    svg.append(polyline([(x_map(b["h"], h0, h1, left, plot_w), lin_y(b["p90"], m_max, tops[0], panel_h)) for b in mbins], RED))
    svg.append(polyline([(x_map(b["h"], h0, h1, left, plot_w), lin_y(b["p50"], m_max, tops[0], panel_h)) for b in mbins], BLUE))

    # Panel 2: throughput (instantaneous per-bin) + overall sustained reference line
    overall_bps = None
    if len(ts_pairs) >= 2 and ts_pairs[-1][1] > ts_pairs[0][1]:
        overall_bps = (ts_pairs[-1][0] - ts_pairs[0][0]) / ((ts_pairs[-1][1] - ts_pairs[0][1]) / 1e6)
    frame(tops[1], [("throughput (blk/s) — ", "#222", "600"),
                    ("per-bin instantaneous", GREEN, "700"),
                    (f"   overall sustained = {overall_bps:.0f}" if overall_bps else "", "#666", "400")])
    grid(tops[1], bps_max, lambda v: f"{v:.0f}")
    if overall_bps:
        y = lin_y(overall_bps, bps_max, tops[1], panel_h)
        svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" '
                   f'stroke="#666" stroke-width="1.5" stroke-dasharray="6,4"/>')
    svg.append(polyline([(x_map(b["h"], h0, h1, left, plot_w), lin_y(b["bps"], bps_max, tops[1], panel_h))
                         for b in tbins if b["bps"] is not None], GREEN))

    # Panel 3: stacked keys by category
    if kbins:
        segs = [("commit batch keys/block, stacked: ", "#222", "600")]
        for label, color, _s in KEY_CATS:
            segs.append((label, color, "700"))
            segs.append(("  ", "#888", "400"))
        frame(tops[2], segs)
        grid(tops[2], keys_max, lambda v: f"{v:.0f}")
        cum = [0.0] * len(kbins)
        for label, color, _s in KEY_CATS:
            top_edge, bot_edge = [], []
            for i, b in enumerate(kbins):
                x = x_map(b["h"], h0, h1, left, plot_w)
                lo = cum[i]
                hi = cum[i] + b[label]
                cum[i] = hi
                top_edge.append((x, lin_y(hi, keys_max, tops[2], panel_h)))
                bot_edge.append((x, lin_y(lo, keys_max, tops[2], panel_h)))
            pts = top_edge + bot_edge[::-1]
            d = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
            svg.append(f'<polygon fill="{color}" fill-opacity="0.78" stroke="none" points="{d}"/>')

    # shared x ticks
    for i in range(6):
        h = h0 + (h1 - h0) * i / 5
        x = x_map(h, h0, h1, left, plot_w)
        for top in tops:
            svg.append(f'<line x1="{x:.1f}" y1="{top+panel_h}" x2="{x:.1f}" y2="{top+panel_h+5}" stroke="#888"/>')
        svg.append(f'<text x="{x:.1f}" y="{tops[-1]+panel_h+22}" text-anchor="middle" fill="#444">{int(h):,}</text>')
    svg.append(f'<text x="{left+plot_w/2:.0f}" y="{H-10}" text-anchor="middle" fill="#444">block height</text>')
    svg.append("</svg>")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{title}-commit-profile-by-height.svg"
    out.write_text("\n".join(svg))
    print(f"wrote {out}")
    # time-weighted regional throughput (blocks / wall-time), NOT mean of bin rates
    def region_rate(lo, hi):
        sel = [(h, t) for h, t in ts_pairs if lo <= h < hi]
        if len(sel) < 2 or sel[-1][1] <= sel[0][1]:
            return None
        return (sel[-1][0] - sel[0][0]) / ((sel[-1][1] - sel[0][1]) / 1e6)
    print("  throughput (time-weighted, blk/s):")
    for name, lo, hi in [("overall", h0, h1 + 1), ("light <1862900", h0, 1862900),
                         ("sandblast 1862.9-1872k", 1862900, 1872000),
                         ("recovery 1872k-end", 1872000, h1 + 1)]:
        r = region_rate(lo, hi)
        print(f"    {name:26} {r:7.1f}" if r else f"    {name:26}   n/a")
    if kbins:
        def avg(rs, l): return sum(b[l] for b in rs) / len(rs) if rs else 0
        light = [b for b in kbins if b["h"] < 1862900]
        sand = [b for b in kbins if 1862900 <= b["h"] <= 1872000]
        print("  keys/block (mean):")
        for l, _c, _s in KEY_CATS:
            print(f"    {l:28} light~{avg(light,l):7.0f}  sandblast~{avg(sand,l):7.0f}")


if __name__ == "__main__":
    main()
