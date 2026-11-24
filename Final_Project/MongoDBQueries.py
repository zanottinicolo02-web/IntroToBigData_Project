"""
Runs the MongoDB pipelines of allMDBQueries.py and reports results and timings.

Usage:
    python MongoDBQueries.py                          # run all queries twice before indexes and twice after
    python MongoDBQueries.py --out results            # rows printed / output folder (CSV)

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
from pymongo.errors import OperationFailure

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

INDEXES = [
    ("plants", [("location", "2dsphere")]),
    ("plants", [("state", 1), ("county_id_fips", 1)]),

    ("generators", [("plant_id_eia", 1), ("generator_id", 1)]),
    ("generators", [("state", 1)]),
    ("generators", [("yearly.report_date", 1), ("yearly.technology_description", 1)]),
    ("generators", [("yearly.operational_status_code", 1)]),

    ("units", [("plant_id_epa", 1), ("emissions_unit_id_epa", 1)]),
    ("units", [("plant_id_eia", 1)]),
    ("units", [("state", 1), ("county_id_fips", 1)]),
    ("units", [("location", "2dsphere")]),
    ("units", [("generators.generator_id", 1)]),

    ("monthly_generation_fuel", [("plant_id_eia", 1), ("report_date", 1)]),
    ("monthly_generation_fuel", [("state", 1), ("report_date", 1)]),
    ("monthly_generation_fuel", [("report_date", 1)]),

    ("cems_hourly_emissions", [("unit.plant_id_eia", 1), ("operating_datetime_utc", 1)]),
    ("cems_hourly_emissions", [("unit.state", 1), ("operating_datetime_utc", 1)]),
    ("cems_hourly_emissions", [("unit.plant_id_epa", 1), ("unit.emissions_unit_id_epa", 1), ("operating_datetime_utc", 1)]),

    ("unit_monthly", [("plant_id_eia", 1), ("month", 1)]),
    ("unit_monthly", [("state", 1), ("month", 1)]),
    ("plant_year_emissions", [("state", 1), ("county_id_fips", 1), ("year", 1)]),
    ("plant_year_emissions", [("location", "2dsphere")]),
]


# Index functionality
def drop_indexes(db):
    dropped = 0
    for col, keys in INDEXES:
        try:
            db[col].drop_index(keys)
            dropped += 1
        except OperationFailure:
            pass
    print(f"dropped {dropped} of {len(INDEXES)} indexes")

def create_indexes(db):
    total = 0.0
    for col, keys in INDEXES:
        start = time.perf_counter()
        db[col].create_index(keys)
        secs = time.perf_counter() - start
        total += secs
        print(f"Created index ({col}: {keys}) in {secs:.2f}")
    print(f"Total index build time: {total:.2f}")
    return total

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

def run_all(db, names, runs, out_dir, limit, mode):
    out_dir.mkdir(parents=True, exist_ok=True)
    summary, results = {}, {}
    for name in names:
        print(f"\n=== {name} ({mode}) ===")
        rows, times = runQ(db, name, runs)
        results[name] = rows
        summary[name] = times
        writeCSV(rows, out_dir / f"{name}_{mode}.csv")
        if not name.startswith("Q3_PARETO"):
            printTable(rows, limit)
    pareto = results.get("Q3_PARETO_FACILITY", []) + results.get("Q3_PARETO_COUNTY", [])
    if pareto:
        print(f"\n=== Q3 Pareto Test ({mode}) ===")
        printTable(pareto, limit)
        printParetoVerdict(pareto)
    return summary, results


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
    parser = argparse.ArgumentParser(description="Run all MongoDB pipelines")
    parser.add_argument("--only", nargs="+", choices=list(QUERIES), help="queries to run")
    parser.add_argument("--out", default="results", help="output folder for CSV / explain files")
    parser.add_argument("--runs", type=int, default=2, help="times to run each query (first = cold)")
    parser.add_argument("--limit", type=int, default=20, help="rows to print per query")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    names = args.only or list(QUERIES)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    drop_indexes(db)
    no_index_summary, no_index_results = run_all(db, names, args.runs, out_dir, args.limit, "no_index")
    create_indexes(db)
    indexed_summary, indexed_results = run_all(db, names, args.runs, out_dir, args.limit, "indexed")

    print("\nTimings (seconds):")
    print(f"  {'query':<22}{'pass':<10}{'first':>10}{'min':>10}{'median':>10}")
    for query in names:
        for label, summary in (("no_index", no_index_summary), ("indexed", indexed_summary)):
            times = summary[query]
            print(f"  {query:<22}{label:<10}{times[0]:>10.2f}{min(times):>10.2f}{statistics.median(times):>10.2f}")

    print("\nComparison (median, seconds):")
    print(f"  {'query':<22}{'no index':>10}{'indexed':>10}{'speed-up':>10}")
    for query in names:
        before = statistics.median(no_index_summary[query])
        after = statistics.median(indexed_summary[query])
        print(f"  {query:<22}{before:>10.2f}{after:>10.2f}{before / after:>9.2f}x")

    client.close()


if __name__ == "__main__":
    main()
