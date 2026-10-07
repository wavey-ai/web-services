#!/usr/bin/env python3
"""Summarize runs.jsonl from run.sh into summary.md and summary.csv.

Usage: summarize.py <results-dir>

For each scenario and tree, the summary gives the median and the range (min..max)
of each metric over the repeats. With two or more trees, it also compares each tree
with the tree listed before it, and with three or more trees the last tree with the first.
"""
import csv
import json
import statistics
import sys
from collections import OrderedDict, defaultdict
from pathlib import Path


def get(d, path):
    for key in path.split("."):
        if not isinstance(d, dict) or key not in d:
            return None
        d = d[key]
    return d


# (column, json path, scale, format, better)  better: "hi", "lo" or None
CLOSED = [
    ("streams/s", "streams_per_s", 1, "{:.0f}", "hi"),
    ("GiB/s", "gib_per_s", 1, "{:.2f}", "hi"),
    ("TTFB p50 ms", "ttfb.p50_ms", 1, "{:.2f}", "lo"),
    ("TTFB p99 ms", "ttfb.p99_ms", 1, "{:.2f}", "lo"),
    ("TTFB p99.9 ms", "ttfb.p999_ms", 1, "{:.2f}", "lo"),
    ("TTLB p50 ms", "ttlb.p50_ms", 1, "{:.2f}", "lo"),
    ("TTLB p99 ms", "ttlb.p99_ms", 1, "{:.2f}", "lo"),
    ("TTLB p99.9 ms", "ttlb.p999_ms", 1, "{:.2f}", "lo"),
    ("srv CPU us/stream", "server.cpu_us_per_stream", 1, "{:.1f}", "lo"),
    ("srv cores busy", "server.cpu_util_cores", 1, "{:.1f}", None),
    ("srv ctxsw/stream", "server.ctx_switches_per_stream", 1, "{:.2f}", "lo"),
    ("srv writes/stream", "server.write_syscalls_per_stream", 1, "{:.2f}", "lo"),
    ("TCP segs/stream", "server.tcp_out_segs_per_stream_machine", 1, "{:.2f}", "lo"),
    ("srv peak RSS MiB", "server.hwm_window_kb", 1 / 1024, "{:.0f}", "lo"),
    ("errors", "errors.total_in_window", 1, "{:.0f}", "lo"),
    ("timeouts", "errors.timeout", 1, "{:.0f}", "lo"),
    ("client CPU %", "client.cpu_util_frac", 100, "{:.0f}", None),
]
STALL = [
    ("plateau KiB/stream", "server.plateau_kb_per_stream", 1, "{:.0f}", "lo"),
    ("peak KiB/stream", "server.peak_kb_per_stream", 1, "{:.0f}", "lo"),
    ("srv RSS baseline MiB", "server.rss_baseline_kb", 1 / 1024, "{:.0f}", None),
    ("srv RSS plateau MiB", "server.rss_plateau_kb", 1 / 1024, "{:.0f}", "lo"),
    ("srv peak RSS MiB", "server.hwm_window_kb", 1 / 1024, "{:.0f}", "lo"),
    ("TTFB p50 ms", "ttfb.p50_ms", 1, "{:.1f}", "lo"),
    ("TTFB p99 ms", "ttfb.p99_ms", 1, "{:.1f}", "lo"),
    ("drain s", "drain_s", 1, "{:.2f}", "lo"),
    ("srv CPU us/stream", "server.cpu_us_per_stream", 1, "{:.0f}", "lo"),
    ("errors", "errors.total_in_window", 1, "{:.0f}", "lo"),
]
# Metrics used for the repeat-spread (noise) table.
NOISE_CLOSED = ["streams/s", "TTLB p50 ms", "TTLB p99 ms", "srv CPU us/stream", "srv peak RSS MiB"]
NOISE_STALL = ["plateau KiB/stream", "peak KiB/stream", "drain s"]


def fmt_cell(values, fmt):
    if not values:
        return "n/a"
    med = statistics.median(values)
    if len(values) == 1:
        return fmt.format(med)
    return f"{fmt.format(med)} [{fmt.format(min(values))}..{fmt.format(max(values))}]"


def spread(values):
    if len(values) < 2:
        return None
    med = statistics.median(values)
    if med == 0:
        return None
    return 100.0 * (max(values) - min(values)) / abs(med)


def main():
    out = Path(sys.argv[1])
    runs = [json.loads(l) for l in (out / "runs.jsonl").read_text().splitlines() if l.strip()]
    scen_order = OrderedDict()
    trees = []
    data = defaultdict(list)
    fatal = []
    for r in runs:
        scen_order.setdefault(r["scenario"], None)
        if r["tree"] not in trees:
            trees.append(r["tree"])
        if "fatal" in r:
            fatal.append(r)
            continue
        data[(r["scenario"], r["tree"])].append(r)

    md = []
    md.append(f"# Streaming benchmark summary: {out.name}\n")
    env = (out / "environment.txt").read_text().splitlines() if (out / "environment.txt").exists() else []
    md.append("## Setup\n")
    for line in env:
        if line.startswith("---"):
            break
        md.append(f"- {line}")
    for t in trees:
        p = out / "builds" / f"{t}.txt"
        if p.exists():
            info = dict(
                l.split(": ", 1) for l in p.read_text().splitlines() if ": " in l and not l.startswith(" ")
            )
            md.append(f"- tree `{t}`: {info.get('source')} @ {info.get('git_head')} "
                      f"(server sha256 {info.get('server_binary_sha256', '')[:12]})")
    md.append("")
    md.append("Each cell: median [min..max] over the repeats. TTFB is the time from the request "
              "to the first response body byte. TTLB is the time from the request to the last "
              "response body byte. Server CPU and RSS come from /proc of the server process "
              "inside the measurement window.\n")

    csv_rows = []
    for kind, metrics, title in (("closed", CLOSED, "Closed-loop scenarios"),
                                 ("stall", STALL, "Slow-reader (stall) scenarios")):
        scen = [s for s in scen_order if any(
            get(r, "config.mode") == kind for t in trees for r in data.get((s, t), []))]
        if not scen:
            continue
        md.append(f"## {title}\n")
        header = ["scenario", "tree", "n"] + [m[0] for m in metrics]
        md.append("| " + " | ".join(header) + " |")
        md.append("|" + "---|" * len(header))
        for s in scen:
            for t in trees:
                rs = data.get((s, t), [])
                if not rs:
                    continue
                cells = [s, t, str(len(rs))]
                for col, path, scale, fmt, _ in metrics:
                    vals = [get(r, path) * scale for r in rs if get(r, path) is not None]
                    cells.append(fmt_cell(vals, fmt))
                    if vals:
                        csv_rows.append([s, t, col, len(vals), statistics.median(vals), min(vals), max(vals)])
                md.append("| " + " | ".join(cells) + " |")
        md.append("")

        pairs = [(ti - 1, ti) for ti in range(1, len(trees))]
        if len(trees) >= 3:
            pairs.append((0, len(trees) - 1))
        for bi, ti in pairs:
            base = trees[bi]
            md.append(f"### {title}: `{trees[ti]}` against `{base}`\n")
            md.append("Delta = median(tree) / median(base) - 1. The mark `*` shows that the "
                      "ranges of the two trees do not overlap.\n")
            key = [m for m in metrics if m[4] is not None]
            header = ["scenario", "tree"] + [m[0] for m in key]
            md.append("| " + " | ".join(header) + " |")
            md.append("|" + "---|" * len(header))
            for s in scen:
                for t in [trees[ti]]:
                    a, b = data.get((s, base), []), data.get((s, t), [])
                    if not a or not b:
                        continue
                    cells = [s, t]
                    for col, path, scale, fmt, better in key:
                        va = [get(r, path) * scale for r in a if get(r, path) is not None]
                        vb = [get(r, path) * scale for r in b if get(r, path) is not None]
                        if not va or not vb:
                            cells.append("n/a")
                            continue
                        ma, mb = statistics.median(va), statistics.median(vb)
                        if ma == 0:
                            cells.append(f"{fmt.format(ma)} -> {fmt.format(mb)}")
                            continue
                        d = 100 * (mb / ma - 1)
                        sep = max(va) < min(vb) or max(vb) < min(va)
                        cells.append(f"{d:+.1f}%{'*' if sep else ''}")
                    md.append("| " + " | ".join(cells) + " |")
            md.append("")

    md.append("## Repeat spread\n")
    md.append("Spread = (max - min) / median, in percent, over the repeats of one scenario "
              "and tree.\n")
    allsp = defaultdict(list)
    for kind, metrics, names in (("closed", CLOSED, NOISE_CLOSED), ("stall", STALL, NOISE_STALL)):
        byname = {m[0]: m for m in metrics}
        rows = []
        for s in scen_order:
            for t in trees:
                rs = data.get((s, t), [])
                if not rs or get(rs[0], "config.mode") != kind:
                    continue
                cells = [s, t]
                for n in names:
                    _, path, scale, _, _ = byname[n]
                    vals = [get(r, path) * scale for r in rs if get(r, path) is not None]
                    sp = spread(vals)
                    if sp is not None:
                        allsp[n].append(sp)
                    cells.append("n/a" if sp is None else f"{sp:.1f}%")
                rows.append(cells)
        if not rows:
            continue
        header = ["scenario", "tree"] + names
        md.append("| " + " | ".join(header) + " |")
        md.append("|" + "---|" * len(header))
        md.extend("| " + " | ".join(c) + " |" for c in rows)
        md.append("")
    md.append("Median and maximum spread across all scenarios:\n")
    for n, v in allsp.items():
        md.append(f"- {n}: median {statistics.median(v):.1f}%, max {max(v):.1f}%")
    md.append("")

    if fatal:
        md.append("## Failed runs\n")
        for r in fatal:
            md.append(f"- {r['scenario']} {r['tree']} r{r['repeat']}: {r['fatal']}")
        md.append("")
    errs = [r for k, rs in data.items() for r in rs if (get(r, "errors.total_in_window") or 0) > 0]
    if errs:
        md.append("## Runs with errors\n")
        for r in errs:
            md.append(f"- {r['scenario']} {r['tree']} r{r['repeat']}: "
                      f"{json.dumps(r['errors'])}")
        md.append("")

    (out / "summary.md").write_text("\n".join(md) + "\n")
    with open(out / "summary.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["scenario", "tree", "metric", "n", "median", "min", "max"])
        w.writerows(csv_rows)


if __name__ == "__main__":
    main()
