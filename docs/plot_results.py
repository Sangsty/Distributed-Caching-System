"""Turn the experiment CSVs into graphs for the report.

Usage:  python docs/plot_results.py docs/results
Needs:  pip install matplotlib
"""
import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display needed
import matplotlib.pyplot as plt


def read(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def main():
    d = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/results")

    rows = read(d / "remap.csv")
    x = [int(r["nodes_before"]) for r in rows]
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.plot(x, [float(r["modulo_moved_pct"]) for r in rows], "o-", label="hash(key) % N")
    ax.plot(x, [float(r["consistent_moved_pct"]) for r in rows], "s-", label="consistent hashing (150 vnodes)")
    ax.plot(x, [float(r["ideal_pct"]) for r in rows], "--", color="gray", label="ideal 1/(N+1)")
    ax.set_xlabel("nodes before adding one")
    ax.set_ylabel("keys that change owner (%)")
    ax.set_title("Keys remapped when a node is added")
    ax.set_ylim(0, 100)
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(d / "remap.png", dpi=160)

    rows = read(d / "balance.csv")
    x = [int(r["vnodes"]) for r in rows]
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.plot(x, [float(r["cv_pct"]) for r in rows], "o-")
    ax.set_xscale("log")
    ax.set_xlabel("virtual nodes per node (log scale)")
    ax.set_ylabel("load imbalance: std dev / mean (%)")
    ax.set_title("Load imbalance vs virtual nodes (10 nodes)")
    ax.grid(alpha=0.3, which="both")
    fig.tight_layout()
    fig.savefig(d / "balance.png", dpi=160)
    print("wrote", d / "remap.png", "and", d / "balance.png")


if __name__ == "__main__":
    main()
