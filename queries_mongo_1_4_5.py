"""
Phase III queries (Q1, Q1_AVG, Q4, Q5) as MongoDB aggregation pipelines,
with the same benchmark scheme used in Phase II:

    drop indexes (clean baseline) -> explain + timing without index
    -> create indexes -> explain + timing with index -> drop indexes again

    python queries_mongo_1_4_5.py            # all queries
    python queries_mongo_1_4_5.py q1 q5      # only the selected ones

At the end a comparison table MongoDB vs MySQL is printed: paste the MySQL times of
Phase II in SQL_TIMES (and the row counts in SQL_ROWS to check that the results match).

Requires MongoDB 5.0+ ($setWindowFields, $lookup with localField + pipeline).
NULL handling: the loader stores missing CEMS measurements as ABSENT fields (not null).
On a raw CEMS field, {"$ne": [field, None]} is TRUE for an absent field, so "has a value" is
tested with {"$gt": [field, None]}. On fields created here with an explicit None the two are equivalent.
There is NO "year" field in cems_hourly_emissions: it is derived with $year.
"""
import sys
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure

DB_NAME = "pudl_cems"
FUELS = ["coal", "gas", "oil"]

# "queryPlanner" does not execute the pipeline. "executionStats" also shows the index usage
# inside $lookup, but it runs the whole pipeline once more (slow on CEMS).
EXPLAIN_VERBOSITY = "queryPlanner"

# ============================================================
# MySQL results of Phase II (seconds, (1st run, 2nd run)) and number of rows returned.
# To fill them in by hand. None = not available.
# ============================================================
SQL_TIMES = {
    "q1":    {"no_index": None, "index": None},
    "q1avg": {"no_index": None, "index": None},
    "q4":    {"no_index": None, "index": None},
    "q5":    {"no_index": None, "index": None},
}
SQL_ROWS = {"q1": None, "q1avg": None, "q4": None, "q5": None}


# ============================================================
# Q1 / Q1_AVG: emissions per MWh by plant and year.
# ============================================================

#db.cems_hourly_emissions.aggregate(Q1, allowDiskUse=True)

# Common part: one row per plant-year with total emissions, net generation
# (all fuels) and dominant fuel.
Q1_COMMON = [
    # 1. CEMS emissions per plant and year.
    {"$match": {"unit.plant_id_eia": {"$ne": None}}},
    {"$group": {
        "_id": {
            "plant_id_eia": "$unit.plant_id_eia",
            "year": {"$year": "$operating_datetime_utc"},
        },
        "_co2_sum": {"$sum": "$co2_mass_tons"}, "_co2_min": {"$min": "$co2_mass_tons"},
        "_so2_sum": {"$sum": "$so2_mass_lbs"},  "_so2_min": {"$min": "$so2_mass_lbs"},
        "_nox_sum": {"$sum": "$nox_mass_lbs"},  "_nox_min": {"$min": "$nox_mass_lbs"},
    }},
    {"$project": {
        "total_co2_tons": {"$cond": [{"$gt": ["$_co2_min", None]}, "$_co2_sum", None]},
        "total_so2_lbs":  {"$cond": [{"$gt": ["$_so2_min", None]}, "$_so2_sum", None]},
        "total_nox_lbs":  {"$cond": [{"$gt": ["$_nox_min", None]}, "$_nox_sum", None]},
    }},

    # 2. Yearly net generation over all fuels and dominant fuel (the one with the most
    # fuel_mmbtu). Only plants with generation > 0 and a dominant fuel in FUELS are kept;
    # the others find no match and are dropped.
    {"$lookup": {
        "from": "monthly_generation_fuel",
        "localField": "_id.plant_id_eia",
        "foreignField": "plant_id_eia",
        "let": {"yr": "$_id.year"},
        "pipeline": [
            {"$match": {"$expr": {"$eq": [{"$year": "$report_date"}, "$$yr"]}}},
            {"$unwind": "$fuels"},
            {"$match": {"fuels.fuel_type_code_pudl": {"$ne": None}}},
            {"$group": {
                "_id": "$fuels.fuel_type_code_pudl",
                "fuel_mmbtu": {"$sum": "$fuels.fuel_consumed_mmbtu"},
                "net_generation_mwh": {"$sum": "$fuels.net_generation_mwh"},
            }},
            {"$sort": {"fuel_mmbtu": -1, "_id": 1}},
            {"$group": {
                "_id": None,
                "fuel_type": {"$first": "$_id"},
                "net_generation_mwh": {"$sum": "$net_generation_mwh"},
            }},
            {"$match": {"net_generation_mwh": {"$gt": 0}, "fuel_type": {"$in": FUELS}}},
        ],
        "as": "generation",
    }},
    {"$unwind": "$generation"},
]

Q1 = Q1_COMMON + [
    # Join WITH "plants" (_id = plant_id_eia). If emission is None, $divide returns None.
    {"$lookup": {
        "from": "plants",
        "localField": "_id.plant_id_eia",
        "foreignField": "_id",
        "as": "plant",
    }},
    {"$unwind": "$plant"},
    {"$project": {
        "_id": 0,
        "year": "$_id.year",
        "state": "$plant.state",
        "plant_name_eia": "$plant.plant_name_eia",
        "fuel_type": "$generation.fuel_type",
        "co2_tons_per_mwh": {"$round": [{"$divide": ["$total_co2_tons", "$generation.net_generation_mwh"]}, 4]},
        "so2_lbs_per_mwh":  {"$round": [{"$divide": ["$total_so2_lbs",  "$generation.net_generation_mwh"]}, 4]},
        "nox_lbs_per_mwh":  {"$round": [{"$divide": ["$total_nox_lbs",  "$generation.net_generation_mwh"]}, 4]},
    }},
    {"$sort": {"year": 1, "co2_tons_per_mwh": -1}},
]

Q1_AVG = Q1_COMMON + [
    # 1. Per year and dominant fuel: total emissions and MWh of only the plant-years
    # that have that value
    {"$group": {
        "_id": {"year": "$_id.year", "fuel_type": "$generation.fuel_type"},
        "number_of_plants": {"$sum": 1},
        "co2": {"$sum": "$total_co2_tons"},
        "mwh_co2": {"$sum": {"$cond": [{"$gt": ["$total_co2_tons", None]}, "$generation.net_generation_mwh", 0]}},
        "so2": {"$sum": "$total_so2_lbs"},
        "mwh_so2": {"$sum": {"$cond": [{"$gt": ["$total_so2_lbs", None]}, "$generation.net_generation_mwh", 0]}},
        "nox": {"$sum": "$total_nox_lbs"},
        "mwh_nox": {"$sum": {"$cond": [{"$gt": ["$total_nox_lbs", None]}, "$generation.net_generation_mwh", 0]}},
    }},
    # 2. Weighted average intensities; mwh = 0 means no data -> None (also avoids dividing by 0)
    {"$project": {
        "_id": 0,
        "year": "$_id.year",
        "fuel_type": "$_id.fuel_type",
        "number_of_plants": 1,
        "avg_co2_tons_per_mwh": {"$cond": [{"$gt": ["$mwh_co2", 0]}, {"$round": [{"$divide": ["$co2", "$mwh_co2"]}, 4]}, None]},
        "avg_so2_lbs_per_mwh":  {"$cond": [{"$gt": ["$mwh_so2", 0]}, {"$round": [{"$divide": ["$so2", "$mwh_so2"]}, 4]}, None]},
        "avg_nox_lbs_per_mwh":  {"$cond": [{"$gt": ["$mwh_nox", 0]}, {"$round": [{"$divide": ["$nox", "$mwh_nox"]}, 4]}, None]},
    }},
    # 3. ORDER BY year, fuel_type
    {"$sort": {"year": 1, "fuel_type": 1}},
]

# ============================================================
# Q4: seasonal capacity margin 
# Array manipulation: $filter keeps only the yearly sub-documents that satisfy the
# conditions BEFORE $unwind, so fewer documents go through the $group.
# ============================================================

#   db.generators.aggregate(Q4)

Q4 = [
    # 1. Generators with at least one operational year (OP).
    {"$match": {"yearly.operational_status_code": "OP"}},

    # 2. From the embedded "yearly" array, keep only valid entries (operational, thermal,
    # with summer and winter capacity present: $gt null is false for null and missing fields).
    {"$project": {
        "state": 1,
        "yearly": {"$filter": {
            "input": "$yearly",
            "as": "y",
            "cond": {"$and": [
                {"$eq": ["$$y.operational_status_code", "OP"]},
                {"$in": ["$$y.fuel_type_code_pudl", FUELS]},
                {"$gt": ["$$y.summer_capacity_mw", None]},
                {"$gt": ["$$y.winter_capacity_mw", None]},
            ]},
        }},
    }},

    # 3. Unwind the filtered array (generators left with no valid entries disappear)
    {"$unwind": "$yearly"},

    # 4. GROUP BY state, technology_description, YEAR(report_date)
    {"$group": {
        "_id": {
            "state": "$state",
            "technology_description": "$yearly.technology_description",
            "report_year": {"$year": "$yearly.report_date"},
        },
        "number_of_generators": {"$sum": 1},
        "total_nominal_capacity_mw": {"$sum": "$yearly.capacity_mw"},
        "total_summer_capacity_mw": {"$sum": "$yearly.summer_capacity_mw"},
        "total_winter_capacity_mw": {"$sum": "$yearly.winter_capacity_mw"},
    }},

    # 5. HAVING SUM(winter_capacity_mw) > 0 (also avoids division by 0)
    {"$match": {"total_winter_capacity_mw": {"$gt": 0}}},

    # 6. Output: rounding and summer_derate_pct = (winter - summer) / winter * 100
    {"$project": {
        "_id": 0,
        "state": "$_id.state",
        "technology_description": "$_id.technology_description",
        "report_year": "$_id.report_year",
        "number_of_generators": 1,
        "total_nominal_capacity_mw": {"$round": ["$total_nominal_capacity_mw", 1]},
        "total_summer_capacity_mw": {"$round": ["$total_summer_capacity_mw", 1]},
        "total_winter_capacity_mw": {"$round": ["$total_winter_capacity_mw", 1]},
        "summer_derate_pct": {"$round": [
            {"$multiply": [
                {"$divide": [
                    {"$subtract": ["$total_winter_capacity_mw", "$total_summer_capacity_mw"]},
                    "$total_winter_capacity_mw",
                ]},
                100,
            ]},
            2,
        ]},
    }},

    # 7. ORDER BY report_year, state, summer_derate_pct DESC
    {"$sort": {"report_year": 1, "state": 1, "summer_derate_pct": -1}},
]

# ============================================================
# Q5: emissions per MWh by load tier
# The SQL self-join (max load per unit and year) becomes a $setWindowFields.
# ============================================================

# Q5 - Real Capacity Factor & Environmental Penalties of Sub-Optimal Operation (MongoDB)

#   db.cems_hourly_emissions.aggregate(Q5, allowDiskUse=True)   
# we may need to use allowDiskUse=True since i tryed to use setWindowFields to compute the max_load (an aggregate function) 
# without modify the dimension of the documents. It will compute the conditions using the RAM

Q5 = [
    # 1. Only hours with load > 0, it reduces the documents entering the window stage.
    {"$match": {"gross_load_mw": {"$gt": 0}}},

    # 2. Maximum load of each unit in each year.
    # Replaces the self-join with the MAX(gross_load_mw) subquery from SQL.
    {"$setWindowFields": {
        "partitionBy": {
            "plant": "$unit.plant_id_epa",
            "unit": "$unit.emissions_unit_id_epa",
            "year": {"$year": "$operating_datetime_utc"},
        },
        "output": {"max_load_mw": {"$max": "$gross_load_mw"}},
    }},

    # 3. Only actual operating hours
    {"$match": {"operating_time_hours": {"$gt": 0}}},

    # 4. Load tier (relative to the unit's maximum) and MWh for the hour
    {"$addFields": {
        "load_tier": {"$switch": {
            "branches": [
                {"case": {"$lt": ["$gross_load_mw", {"$multiply": [0.20, "$max_load_mw"]}]},
                 "then": "1-Peaking (<20%)"},
                {"case": {"$lt": ["$gross_load_mw", {"$multiply": [0.70, "$max_load_mw"]}]},
                 "then": "2-Mid-merit (20-70%)"},
            ],
            "default": "3-Baseload (>=70%)",
        }},
        "_mwh": {"$multiply": ["$gross_load_mw", "$operating_time_hours"]},
    }},

    # 5. GROUP BY state, year, load_tier. For each pollutant: total emissions and MWh only
    # for hours where it was measured ($gt null is false for null and missing fields).
    {"$group": {
        "_id": {
            "state": "$unit.state",
            "year": {"$year": "$operating_datetime_utc"},
            "load_tier": "$load_tier",
        },
        "operating_hours": {"$sum": 1},
        "nox": {"$sum": "$nox_mass_lbs"},
        "mwh_nox": {"$sum": {"$cond": [{"$gt": ["$nox_mass_lbs", None]}, "$_mwh", 0]}},
        "co2": {"$sum": "$co2_mass_tons"},
        "mwh_co2": {"$sum": {"$cond": [{"$gt": ["$co2_mass_tons", None]}, "$_mwh", 0]}},
    }},

    # 6. Intensity per MWh; mwh = 0 means no measurement -> None (avoids division by 0)
    {"$project": {
        "_id": 0,
        "state": "$_id.state",
        "year": "$_id.year",
        "load_tier": "$_id.load_tier",
        "operating_hours": 1,
        "nox_lbs_per_mwh": {"$cond": [{"$gt": ["$mwh_nox", 0]}, {"$round": [{"$divide": ["$nox", "$mwh_nox"]}, 4]}, None]},
        "co2_tons_per_mwh": {"$cond": [{"$gt": ["$mwh_co2", 0]}, {"$round": [{"$divide": ["$co2", "$mwh_co2"]}, 4]}, None]},
    }},

    # 7. ORDER BY state, year, load_tier
    {"$sort": {"state": 1, "year": 1, "load_tier": 1}},
]


# ============================================================
# CONFIG: each query with its proposed indexes
# ============================================================
IDX_MGF = dict(coll="monthly_generation_fuel", name="idx_mgf_plant_date",
               keys=[("plant_id_eia", 1), ("report_date", 1)])
IDX_CEMS_PLANT = dict(coll="cems_hourly_emissions", name="idx_cems_plant_time",
                      keys=[("unit.plant_id_eia", 1), ("operating_datetime_utc", 1)])
IDX_CEMS_UNIT = dict(coll="cems_hourly_emissions", name="idx_cems_unit_time",
                     keys=[("unit.plant_id_epa", 1), ("unit.emissions_unit_id_epa", 1),
                           ("operating_datetime_utc", 1)])
IDX_GEN_STATUS = dict(coll="generators", name="idx_gen_status",
                      keys=[("yearly.operational_status_code", 1)])

BENCHMARKS = {
    "q1": dict(
        title="Q1 - CO2, SO2, NOx intensity per MWh",
        coll="cems_hourly_emissions", pipeline=Q1,
        # IDX_MGF serves the $lookup (one lookup per plant-year); IDX_CEMS_PLANT is expected
        # to help little, since the $group reads the whole collection.
        indexes=[IDX_MGF, IDX_CEMS_PLANT],
    ),
    "q1avg": dict(
        title="Q1 AVG - average intensity by year and fuel",
        coll="cems_hourly_emissions", pipeline=Q1_AVG,
        indexes=[IDX_MGF, IDX_CEMS_PLANT],
    ),
    "q4": dict(
        title="Q4 - Seasonal capacity margins",
        coll="generators", pipeline=Q4,
        # small collection and non-selective filter, little gain expected.
        indexes=[IDX_GEN_STATUS],
    ),
    "q5": dict(
        title="Q5 - Load tiers and environmental penalty of peaking",
        coll="cems_hourly_emissions", pipeline=Q5,
        # The window stage partitions on the expression $year, so the index is unlikely to be used.
        indexes=[IDX_CEMS_UNIT],
    ),
}


# ============================================================
# FUNCTIONS
# ============================================================
def drop_indexes(db, b):
    for idx in b["indexes"]:
        try:
            db[idx["coll"]].drop_index(idx["name"])
            print(f"Existing index dropped: {idx['name']}")
        except OperationFailure:
            pass  # the index does not exist: the baseline is clean


def create_indexes(db, b):
    start = time.perf_counter()
    for idx in b["indexes"]:
        db[idx["coll"]].create_index(idx["keys"], name=idx["name"])
    return time.perf_counter() - start


def collect_plan(node, stages, indexes):
    """Walk the explain output and collect plan stages and index names."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "stage" and isinstance(v, str):
                stages.add(v)
            elif k == "indexName" and isinstance(v, str):
                indexes.add(v)
            else:
                collect_plan(v, stages, indexes)
    elif isinstance(node, list):
        for x in node:
            collect_plan(x, stages, indexes)


def show_plan(db, b):
    out = db.command({"explain": {"aggregate": b["coll"], "pipeline": b["pipeline"],
                                  "cursor": {}, "allowDiskUse": True}, "verbosity": EXPLAIN_VERBOSITY})
    stages, indexes = set(), set()
    collect_plan(out, stages, indexes)
    print(f"  {b['coll']:<24} stages={sorted(stages)}  indexes={sorted(indexes) or '-'}")


def time_pipeline(coll, pipeline, runs=2):
    times, rows = [], []
    for _ in range(runs):
        start = time.perf_counter()
        rows = list(coll.aggregate(pipeline, allowDiskUse=True))
        times.append(time.perf_counter() - start)
    return times, rows


def print_summary(title, before, after, build_time):
    print(f"\n{title}")
    print(f"{'':<14}{'before':>10}{'after':>10}{'speed-up':>10}")
    for label, b, a in [("1st run (s)", before[0], after[0]),
                        ("2nd run (s)", before[1], after[1])]:
        print(f"{label:<14}{b:>10.3f}{a:>10.3f}{b / a:>9.3f}x")
    print(f"index build time: {build_time:.3f}s")


def run_benchmark(db, name, b):
    print(f"\n{'=' * 60}\n{b['title']}\n{'=' * 60}")
    coll = db[b["coll"]]

    drop_indexes(db, b)
    print("EXPLAIN without index:")
    show_plan(db, b)
    print("Running without index...")
    before, rows = time_pipeline(coll, b["pipeline"])
    n_rows = len(rows)
    print(f"rows returned: {n_rows:,}")
    for r in rows[:3]:
        print("  ", r)

    print("Creating indexes...")
    build_time = create_indexes(db, b)

    print("EXPLAIN with index:")
    show_plan(db, b)
    print("Running with index...")
    after, _ = time_pipeline(coll, b["pipeline"])

    print_summary(b["title"], before, after, build_time)
    if SQL_ROWS.get(name) is not None and SQL_ROWS[name] != n_rows:
        print(f"WARNING: MySQL returned {SQL_ROWS[name]:,} rows, MongoDB {n_rows:,}")

    drop_indexes(db, b)
    return dict(before=before, after=after, rows=n_rows)


def fmt(times):
    return "n/a" if not times else f"{times[0]:.1f}/{times[1]:.1f}"


def print_comparison(results):
    print(f"\n{'=' * 78}\nMySQL vs MongoDB (seconds, 1st run / 2nd run)\n{'=' * 78}")
    print(f"{'query':<8}{'MySQL no idx':>16}{'MySQL idx':>16}{'Mongo no idx':>16}{'Mongo idx':>16}")
    for name, r in results.items():
        s = SQL_TIMES.get(name, {})
        print(f"{name:<8}{fmt(s.get('no_index')):>16}{fmt(s.get('index')):>16}"
              f"{fmt(r['before']):>16}{fmt(r['after']):>16}")


def main():
    names = sys.argv[1:] or list(BENCHMARKS)
    db = MongoClient("mongodb://localhost:27017")[DB_NAME]
    results = {}
    for name in names:
        results[name] = run_benchmark(db, name, BENCHMARKS[name])
    print_comparison(results)


if __name__ == "__main__":
    main()
