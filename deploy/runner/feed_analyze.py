#!/usr/bin/env python3
"""Steady-state bottleneck attribution for feed_run.sh CSVs.

Per-block timings from the monotonic histogram sum/count columns
(1000*dsum/dcount = ms/block), committer utilization, download/verify latency,
the obtain-tips cadence, and a burst-vs-gap decomposition of committer-idle time
(the step-1 question: is the idle bandwidth-limited or obtain-tips scheduling?).

Usage: feed_analyze.py CSV [h_lo h_hi]
  No window -> steady-state middle 60% of rows (skips warm-up + final flush).
"""
import csv, sys

def load(path):
    with open(path) as f:
        return [r for r in csv.DictReader(f)]

def fnum(r, k):
    try: return float(r.get(k, 0) or 0)
    except (ValueError, TypeError): return 0.0

def window_indices(rows, h_lo, h_hi):
    hs = [fnum(r, "height") for r in rows]
    if h_lo is None:
        return int(len(rows)*0.2), int(len(rows)*0.8)
    lo = next((i for i,h in enumerate(hs) if h >= h_lo), 0)
    hi = next((i for i,h in enumerate(hs) if h >= h_hi), len(rows)-1)
    return lo, hi

def per_block(a, b, key):
    ds = fnum(b, key+"_sum") - fnum(a, key+"_sum")
    dc = fnum(b, key+"_cnt") - fnum(a, key+"_cnt")
    return (1000.0*ds/dc) if dc > 0 else 0.0

def avg_event(a, b, key):  # seconds per event (download/verify latency)
    ds = fnum(b, key+"_sum") - fnum(a, key+"_sum")
    dc = fnum(b, key+"_cnt") - fnum(a, key+"_cnt")
    return (ds/dc) if dc > 0 else 0.0, dc

def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(1)
    rows = load(sys.argv[1])
    if len(rows) < 3:
        print("not enough samples yet"); sys.exit(0)
    h_lo = float(sys.argv[2]) if len(sys.argv) > 2 else None
    h_hi = float(sys.argv[3]) if len(sys.argv) > 3 else None
    lo, hi = window_indices(rows, h_lo, h_hi)
    a, b = rows[lo], rows[hi]
    win = rows[lo:hi+1]

    dh = fnum(b,"height") - fnum(a,"height")
    dt = fnum(b,"elapsed") - fnum(a,"elapsed")
    blk_s = dh/dt if dt > 0 else 0
    ms_per_block = 1000.0/blk_s if blk_s > 0 else 0

    commit_busy = per_block(a, b, "commit")
    util = commit_busy/ms_per_block if ms_per_block > 0 else 0

    verifier  = per_block(a,b,"eq") + per_block(a,b,"mk")
    cpu_tree  = per_block(a,b,"ut")
    cpu_check = per_block(a,b,"cc")
    cpu_bprep = per_block(a,b,"bp")
    db_reads  = per_block(a,b,"pr")
    db_write  = per_block(a,b,"dc")
    wblock    = per_block(a,b,"wbt")

    dl = fnum(b,"downloaded") - fnum(a,"downloaded")
    dl_s = dl/dt if dt > 0 else 0
    in_flight = sum(fnum(r,"in_flight") for r in win)/len(win)
    dld_lat, dld_n = avg_event(a,b,"dld")     # s per block download
    vfy_lat, vfy_n = avg_event(a,b,"vfy")     # s per verify completion
    obt_rounds = fnum(b,"obt_cnt") - fnum(a,"obt_cnt")
    ext_rounds = fnum(b,"ext_cnt") - fnum(a,"ext_cnt")
    net_in = (fnum(b,"net_in_bytes")-fnum(a,"net_in_bytes"))/dt/1e6 if dt>0 else 0
    peers = sum(fnum(r,"peers") for r in win)/len(win)
    qd = sum(fnum(r,"qdepth") for r in win)/len(win)

    # burst-vs-gap: 5s samples with near-zero blk_s are obtain-tips/stall gaps.
    bps = [fnum(r,"blk_s") for r in win]
    gaps = [x for x in bps if x < 5]
    active = [x for x in bps if x >= 5]
    gap_frac = len(gaps)/len(bps) if bps else 0
    burst = sum(active)/len(active) if active else 0

    print(f"window: height {fnum(a,'height'):.0f} -> {fnum(b,'height'):.0f}  "
          f"({dh:.0f} blocks, {dt:.0f}s, {len(win)} samples)")
    print(f"throughput: {blk_s:.1f} blk/s  ({ms_per_block:.2f} ms/block wall)\n")

    # commit sub-phases that make up committer-busy beyond note_tree + write_block
    ckc = per_block(a,b,"ckc")   # checkpoint_compute = compute scope + history_push
    ttr = per_block(a,b,"ttr")   # parent treestate read + clone (subset of prep)
    hpu = per_block(a,b,"hpu")   # history-MMR push (+ root computation), subset of ckc
    rtc = per_block(a,b,"rtc")   # result note-trees clone
    prp = per_block(a,b,"prp")   # all setup before the compute scope (umbrella; incl. ttr)
    wbi = per_block(a,b,"wbi")   # write_block install wrapper (incl. write_block_total)
    pst = per_block(a,b,"pst")   # post-write bookkeeping
    # Non-overlapping accounting: prep + checkpoint_compute + write_block_install + post.
    # (ttr is inside prep; hpu inside ckc; write_block_total inside wbi.)
    install_overhead = wbi - wblock
    scope_overhead = ckc - cpu_tree - cpu_check - hpu
    accounted = prp + ckc + wbi + pst
    other = commit_busy - accounted
    print(f"COMMITTER  busy={commit_busy:.2f} ms/blk  util={util*100:.0f}%  "
          f"queue_depth~{qd:.0f}")
    print(f"  prep={prp:.2f} (tip_trees_read={ttr:.2f})  "
          f"checkpoint_compute={ckc:.2f} (note_tree={cpu_tree:.2f} history_push={hpu:.2f} scope_overhead={scope_overhead:.2f})")
    print(f"  write_block_install={wbi:.2f} (write_block={wblock:.2f} install_overhead={install_overhead:.2f})  "
          f"post={pst:.2f}  result_trees_clone={rtc:.2f}")
    print(f"  accounted={accounted:.2f}  unattributed={other:.2f} ms/blk")
    print(f"  >> total per-block rayon install overhead ~= scope_overhead + install_overhead "
          f"= {scope_overhead+install_overhead:.2f} ms (the double-install cost)\n")

    print("per-block cost by category (ms/block):")
    print(f"  NETWORK    in={net_in:.1f} MB/s  peers~{peers:.0f}")
    print(f"  DOWNLOAD   {dl_s:.1f} blk/s delivered  per-block latency={dld_lat*1000:.0f} ms  in_flight~{in_flight:.0f}")
    print(f"  VERIFIER   cpu {verifier:.2f} ms/blk (eq+mk)   verify-call latency={vfy_lat:.1f} s (batch/queue, n={vfy_n:.0f})")
    print(f"  COMMIT_CPU {cpu_tree+cpu_check+cpu_bprep:.2f}   "
          f"(note_tree={cpu_tree:.2f} commit_check={cpu_check:.2f} batch_prep={cpu_bprep:.2f})")
    print(f"  COMMIT_DB  {db_reads+db_write:.2f}   "
          f"(prep_reads={db_reads:.2f} rocksdb_write={db_write:.2f})")

    print(f"\nCADENCE    obtain rounds={obt_rounds:.0f}  extend rounds={ext_rounds:.0f}  "
          f"(~500 hashes/round)")
    print(f"           burst rate={burst:.0f} blk/s during active samples; "
          f"gap (blk_s<5) fraction={gap_frac*100:.0f}% of wall")

    # verdict
    if util >= 0.85:
        stages = {"VERIFIER":verifier,"COMMIT_CPU_note_tree":cpu_tree,
                  "COMMIT_CPU_batch_prep":cpu_bprep,"COMMIT_DB_reads":db_reads,
                  "COMMIT_DB_write":db_write}
        top = max(stages, key=stages.get)
        v = f"COMMIT-BOUND, dominant phase: {top} ({stages[top]:.2f} ms/blk)"
    elif gap_frac > 0.30 and dld_lat < 1.0:
        v = (f"SCHEDULING-BOUND: committer starved ({util*100:.0f}% util), but per-block "
             f"download is fast ({dld_lat*1000:.0f} ms) and {gap_frac*100:.0f}% of wall is "
             f"obtain-tips gaps. The cadence (tip discovery / batch handoff), not bandwidth, gates it.")
    else:
        v = (f"BANDWIDTH/LATENCY-BOUND: committer starved ({util*100:.0f}% util), download "
             f"delivers {dl_s:.0f} blk/s at {dld_lat*1000:.0f} ms/block, in_flight~{in_flight:.0f}.")
    print(f"\nVERDICT: {v}")

if __name__ == "__main__":
    main()
