"""
Runs the MongoDB pipelines of allMDBQueries.py and reports results and timings.

Usage:
    python run_queries.py                          # run all queries once
    python run_queries.py --only Q1 Q2             # selected queries
    python run_queries.py --out results            # rows printed / output folder (CSV)

Requirements: pip install pymongo
"""

# native imports
import argparse
import csv
import os
import statistics
import time
from pathlib import Path

# other libraries
from pymongo import MongoClient

# queries kept in other file for better structure
import allMDBQueries as Q

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "pudl_cems")

QUERIES = {
    "Q1_COMMON": ("cems_hourly_emissions", Q.Q1_COMMON),
    "Q1": ("cems_hourly_emissions", Q.Q1),
    "Q1_AVG": ("cems_hourly_emissions", Q.Q1_AVG),
    "Q2": ("cems_hourly_emissions", Q.Q2),
    "Q3_PARETO_FACILITY": ("cems_hourly_emissions", Q.Q3_PARETO_FACILITY),
    "Q3_PARETO_COUNTY": ("cems_hourly_emissions", Q.Q3_PARETO_COUNTY),
    "Q3_HOTSPOTS": ("cems_hourly_emissions", Q.Q3_HOTSPOTS),
    "Q4": ("generators", Q.Q4),
    "Q5": ("cems_hourly_emissions", Q.Q5)
}


# Execution
def runOnce(db, coll, pipeline):
    start = time.perf_counter()
    rows = list(db[coll].aggregate(pipeline, allowDiskUse=True))
    return rows, time.perf_counter() - start

def runQ(db, name, runs):
    coll, pipeline = QUERIES[name]
    times, rows = [], []
    for i in range(runs):
        rows, secs = runOnce(db, coll, pipeline)
        times.append(secs)
        label = "cold" if i == 0 else "warm"
        print(f"  run {i + 1}/{runs} ({label}): {secs:8.2f} s  -> {len(rows):,} rows")
    return rows, times


# Output
def fmt(value):
    if isinstance(value, float):
        return f"{value:,.4f}"
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value:,}"
    return "" if value is None else str(value)

def printTable(rows, limit):
    if not rows:
        print("  (no rows)")
        return
    cols = list(rows[0].keys())
    shown = rows[:limit]
    widths = [max(len(c), *(len(fmt(r.get(c))) for r in shown)) for c in cols]
    print("  " + "  ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  " + "  ".join("-" * w for w in widths))
    for r in shown:
        print("  " + "  ".join(fmt(r.get(c)).ljust(w) for c, w in zip(cols, widths)))
    if len(rows) > limit:
        print(f"  ... {len(rows) - limit:,} more rows (all of them are in the CSV)")

def writeCSV(rows, path):
    if not rows:
        return
    cols = []
    for r in rows:
        for k in r:
            if k not in cols:
                cols.append(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)

def printParetoVerdict(rows):
    print("\n  80/20 test (top 20% of the rows):")
    for r in rows:
        for pollutant in ("co2", "so2", "nox"):
            share = r.get(f"{pollutant}_top20_share")
            if share is None:
                continue
            verdict = "PASS" if share >= 0.80 else "below 80%"
            print(f"    {r['level']:<9}{pollutant.upper():<5}{share * 100:6.1f}%  {verdict}")



def main():
    parser = argparse.ArgumentParser(description="Run the Q2 / Q3 MongoDB pipelines")
    parser.add_argument("--only", nargs="+", choices=list(QUERIES), help="queries to run")
    parser.add_argument("--out", default="results", help="output folder for CSV / explain files")
    parser.add_argument("--runs", type=int, default=3, help="times to run each query (first = cold)")
    parser.add_argument("--limit", type=int, default=20, help="rows to print per query")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    names = args.only or list(QUERIES)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    summary, results = {}, {}

    for name in names:
        print(f"\n=== {name} ===")
        rows, times = runQ(db, name, args.runs)
        results[name] = rows
        summary[name] = times
        writeCSV(rows, out_dir / f"{name}.csv")
        if not name.startswith("Q3_PARETO"):
            printTable(rows, args.limit)

    # the two Pareto levels are one table in the SQL (UNION ALL)
    pareto = results.get("Q3_PARETO_FACILITY", []) + results.get("Q3_PARETO_COUNTY", [])
    if pareto:
        print("\n=== Q3 Pareto test ===")
        printTable(pareto, args.limit)
        printParetoVerdict(pareto)

    print("\nTimings (seconds):")
    print(f"  {'query':<24}{'first':>10}{'min':>10}{'median':>10}")
    for name, times in summary.items():
        print(f"  {name:<24}{times[0]:>10.2f}{min(times):>10.2f}{statistics.median(times):>10.2f}")

    client.close()


if __name__ == "__main__":
    main()
