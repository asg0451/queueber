#!/usr/bin/env python3
import argparse
import json
import os
from typing import Dict, List, Tuple


def load_summary(path: str) -> Dict[str, float]:
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    result: Dict[str, float] = {}
    if isinstance(data, list):
        for entry in data:
            try:
                name = str(entry.get("name"))
                value = float(entry.get("value"))
            except (TypeError, ValueError):
                continue
            if name:
                result[name] = value
    return result


def compute_delta(
    baseline: Dict[str, float], current: Dict[str, float]
) -> List[Tuple[str, float, float, float, float]]:
    names = set(baseline.keys()) | set(current.keys())
    rows: List[Tuple[str, float, float, float, float]] = []
    for name in sorted(names):
        b = baseline.get(name)
        c = current.get(name)
        delta = None
        pct = None
        if b is not None and c is not None:
            delta = c - b
            pct = (delta / b * 100.0) if b != 0 else None
        rows.append((name, c if c is not None else float("nan"), b if b is not None else float("nan"), delta if delta is not None else float("nan"), pct if pct is not None else float("nan")))
    return rows


def fmt_num(v: float) -> str:
    if v != v:  # NaN
        return "—"
    return f"{v:.3f}"


def fmt_pct(v: float) -> str:
    if v != v:  # NaN
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1f}%"


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute Criterion delta between current and baseline summaries.")
    parser.add_argument("--current", default="criterion-summary.json")
    parser.add_argument("--baseline", default="master-artifacts/criterion-summary.json")
    parser.add_argument("--out", default="bench_delta.md")
    args = parser.parse_args()

    cur = load_summary(args.current)
    base = load_summary(args.baseline)

    with open(args.out, "w", encoding="utf-8") as out:
        if not base:
            out.write("No baseline benchmark artifact found on master. Showing current results only.\n\n")
            out.write("Benchmarks (ms, lower is better)\n\n")
            out.write("| Benchmark | PR (ms) |\n")
            out.write("|---|---:|\n")
            for name in sorted(cur.keys()):
                out.write(f"| {name} | {fmt_num(cur[name])} |\n")
            return 0

        rows = compute_delta(base, cur)
        out.write("Criterion benchmarks delta (ms; lower is better)\n\n")
        out.write("| Benchmark | PR (ms) | Master (ms) | Δ (ms) | Δ (%) |\n")
        out.write("|---|---:|---:|---:|---:|\n")
        for name, c, b, d, p in rows:
            out.write(f"| {name} | {fmt_num(c)} | {fmt_num(b)} | {fmt_num(d)} | {fmt_pct(p)} |\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

