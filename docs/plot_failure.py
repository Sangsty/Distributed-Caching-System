"""Plot a cmd/loadgen run (hit ratio, latency, traffic per cache node, outcomes).

Usage:  python docs/plot_failure.py docs/results/failure [--warmup 10] [--title "..."]
Needs:  pip install matplotlib

Reads timeseries.csv, nodes.csv, events.csv (and the first line of summary.md for
the subtitle) from the given directory and writes failure.png next to them.
"""
import argparse
import csv
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display needed
import matplotlib.pyplot as plt
import matplotlib.ticker

# Light-mode tokens (validated reference palette). Text is always ink, never a series colour.
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
INK_3 = "#8a8984"
GRID = "#e6e5e1"
AXIS = "#cfceca"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]  # categorical slots 1-3: fixed order, one per node
CRITICAL = "#d03b3b"
GOOD = "#0ca30c"


def read(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def num(v):
    return float(v) if v not in ("", None) else math.nan


def event_colour(label):
    l = label.lower()
    if any(w in l for w in ("crash", "kill", "stop", "down", "fail")):
        return CRITICAL
    if any(w in l for w in ("restart", "start", "up", "join", "recover")):
        return GOOD
    return INK_3


def style(ax, title, ylabel):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=11, color=INK, fontweight="bold", pad=8)
    ax.set_ylabel(ylabel, fontsize=9, color=INK_2)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
    ax.tick_params(colors=INK_2, labelsize=9, length=3, color=AXIS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir", nargs="?", default="docs/results/failure")
    ap.add_argument("--warmup", type=float, default=10, help="seconds shaded as cold-cache warm-up")
    ap.add_argument("--title", default="A cache node crashes under a hot-key workload")
    ap.add_argument("--out", default="failure.png")
    args = ap.parse_args()
    d = Path(args.dir)

    ts = read(d / "timeseries.csv")
    nodes_rows = read(d / "nodes.csv")
    events = read(d / "events.csv") if (d / "events.csv").exists() else []

    t = [int(r["t"]) for r in ts]
    hit = [num(r["hit_ratio"]) * 100 for r in ts]
    p50, p95, p99 = ([num(r[k]) for r in ts] for k in ("p50_ms", "p95_ms", "p99_ms"))
    ok = [int(r["ok"]) for r in ts]
    err = [int(r["errors"]) + int(r["dropped"]) for r in ts]
    node_names = [c for c in nodes_rows[0].keys() if c != "t"] if nodes_rows else []
    node_series = [[int(r[n]) for r in nodes_rows] for n in node_names]

    subtitle = ""
    sm = d / "summary.md"
    if sm.exists():
        first = sm.read_text(encoding="utf-8").splitlines()[0]
        subtitle = first.replace("Load: ", "")

    fig, axes = plt.subplots(4, 1, figsize=(11, 11.5), sharex=True, facecolor=SURFACE,
                             gridspec_kw={"height_ratios": [1.15, 1.15, 1.15, 0.8], "hspace": 0.42})
    ax_hit, ax_lat, ax_node, ax_out = axes

    # 1. hit ratio
    style(ax_hit, "Cache hit ratio", "% of reads served from cache")
    ax_hit.plot(t, hit, color=INK, linewidth=1.8)
    ax_hit.set_ylim(0, 102)

    # 2. latency (ordered quantiles: one neutral ramp, lighter = lower quantile)
    style(ax_lat, "Request latency (log scale)", "ms")
    ax_lat.plot(t, p50, color=INK_3, linewidth=1.4, label="p50")
    ax_lat.plot(t, p95, color=INK_2, linewidth=1.6, label="p95")
    ax_lat.plot(t, p99, color=INK, linewidth=1.8, label="p99")
    ax_lat.set_yscale("log")
    vals = [v for v in p50 + p95 + p99 if not math.isnan(v) and v > 0] or [1.0]
    lo, hi = min(vals) * 0.8, max(vals) * 1.25
    ax_lat.set_ylim(lo, hi)
    ticks = [v for v in (0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000) if lo <= v <= hi]
    ax_lat.yaxis.set_major_locator(matplotlib.ticker.FixedLocator(ticks))
    ax_lat.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax_lat.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
    leg = ax_lat.legend(loc="lower right", bbox_to_anchor=(1.0, 1.0), ncol=3, frameon=False, fontsize=9, handlelength=1.6)
    for txt in leg.get_texts():
        txt.set_color(INK_2)

    # 3. traffic by owning cache node (stacked; 2 px surface gap between fills)
    style(ax_node, "Requests by the cache node that owns the key", "requests / s")
    if node_series:
        cols = [SERIES[i % len(SERIES)] for i in range(len(node_names))]
        ax_node.stackplot(t[: len(node_series[0])], *node_series, colors=cols, labels=node_names,
                          edgecolor=SURFACE, linewidth=1.5)
        leg = ax_node.legend(loc="lower right", bbox_to_anchor=(1.0, 1.0), ncol=len(node_names), frameon=False,
                             fontsize=9, handlelength=1.2)
        for txt in leg.get_texts():
            txt.set_color(INK_2)
    ax_node.set_ylim(0, max([sum(v) for v in zip(*node_series)] or [1]) * 1.05)

    # 4. outcomes
    style(ax_out, "Outcomes", "requests / s")
    ax_out.plot(t, ok, color=INK, linewidth=1.6, label="successful")
    ax_out.plot(t, err, color=CRITICAL, linewidth=1.8, label="errors")
    ax_out.set_ylim(0, max(ok + [1]) * 1.25)
    leg = ax_out.legend(loc="lower right", bbox_to_anchor=(1.0, 1.0), ncol=2, frameon=False, fontsize=9, handlelength=1.6)
    for txt in leg.get_texts():
        txt.set_color(INK_2)
    total_err = sum(err)
    ax_out.text(0.99, 0.12, f"{total_err} failed request{'s' if total_err != 1 else ''} in the whole run",
                transform=ax_out.transAxes, fontsize=9, color=INK_2, ha="right")
    ax_out.set_xlabel("seconds since the start of the run", fontsize=9, color=INK_2)

    # warm-up shading and event markers on every panel
    for ax in axes:
        ax.set_xlim(0, max(t))
        if args.warmup > 0:
            ax.axvspan(0, args.warmup, color=GRID, alpha=0.55, linewidth=0)
        for e in events:
            ax.axvline(float(e["t_seconds"]), color=event_colour(e["label"]), linewidth=1.2, alpha=0.9)
    if args.warmup > 0:
        ax_hit.text(args.warmup / 2, 4, "cold-cache\nwarm-up", ha="center", va="bottom", fontsize=8, color=INK_2)
    for e in events:
        ax_hit.annotate(e["label"], xy=(float(e["t_seconds"]), 102), xytext=(3, 6), textcoords="offset points",
                        fontsize=9, color=INK, annotation_clip=False, ha="left", va="bottom")

    fig.suptitle(args.title, x=0.075, y=0.985, ha="left", fontsize=14, fontweight="bold", color=INK)
    if subtitle:
        fig.text(0.075, 0.953, subtitle, ha="left", fontsize=9, color=INK_2)
    fig.subplots_adjust(left=0.075, right=0.975, top=0.915, bottom=0.05)

    out = d / args.out
    fig.savefig(out, dpi=140, facecolor=SURFACE)
    print("wrote", out)


if __name__ == "__main__":
    main()
