"""
Loader: PUDL + EPA CEMS (US Northeast, sample years) -> MongoDB

Companion of docSchema.js (same folder):
- Reads the Parquet files directly from the public PUDL S3 bucket through DuckDB
- Nested documents (generators.yearly[], etc) are built in DuckDB with 
  list()/struct_pack(), then sent to MongoDB in batches
- The `units` collection (EPA -> EIA bridge) is built in Python from the crosswalk and
  the generators that were just loaded
- cems_hourly_emissions is loaded one year at a time (ordered by unit and timestamp)
- validators + indexes come from docSchema.js, runs first through mongosh (no foreign 
keys in MongoDB)
- optional to build the derived collections (unit_monthly, plant_year_emissions)

THE COLLECTIONS ARE DROPPED AND RECREATED EVERY RUN
"""

# native imports
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

# other libraries
import duckdb
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# CONFIG PARAMS
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = "pudl_cems"
SCHEMA_JS = Path(__file__).with_name("docSchema.js")
BATCH_SIZE = 20_000             # rows per read / insert
DOC_BATCH_SIZE = 2_000          # docs per insert w/ nested arrays
BUILD_ROLLUPS = True            # unit_monthly and plant_year_emissions last

BASE = "s3://pudl.catalyst.coop/stable/"
SRC = {
    "cems": BASE + "core_epacems__hourly_emissions.parquet",
    "plants": BASE + "core_eia__entity_plants.parquet",
    "generators": BASE + "core_eia860__scd_generators.parquet",
    "gen_fuel": BASE + "core_eia923__monthly_generation_fuel.parquet",
    "crosswalk": BASE + "core_epa__assn_eia_epacamd.parquet",
}

STATE_LIST = ["CT", "ME", "MA", "NH", "RI", "VT", "NJ", "NY", "PA"]
YEAR_LIST = [2000, 2005, 2010, 2015, 2018, 2020, 2023, 2025]
STATES = ", ".join(f"'{s}'" for s in STATE_LIST)
YEARS = ", ".join(str(y) for y in YEAR_LIST)

# Assuming it's fine to use SQL to help filter for specific data we need
NE_PLANTS = f"SELECT plant_id_eia FROM '{SRC['plants']}' WHERE state IN ({STATES})"

WHERE_GEN = (f"plant_id_eia IN ({NE_PLANTS}) AND generator_id IS NOT NULL "
             f"AND EXTRACT(year FROM report_date) IN ({YEARS})")
WHERE_GENFUEL = f"plant_id_eia IN ({NE_PLANTS}) AND EXTRACT(year FROM report_date) IN ({YEARS})"
WHERE_XWALK = (f"plant_id_eia IN ({NE_PLANTS}) AND report_year IN ({YEARS}) "
               f"AND plant_id_epa IS NOT NULL AND emissions_unit_id_epa IS NOT NULL")

# COLS (field, kind): kind = "dbl" (DOUBLE, NaN -> null), "date" (DATE -> datetime), "raw"
GEN_YEARLY_FIELDS = [
    ("report_date", "date"), ("utility_id_eia", "raw"), ("operational_status_code", "raw"),
    ("capacity_mw", "dbl"), ("summer_capacity_mw", "dbl"), ("winter_capacity_mw", "dbl"),
    ("prime_mover_code", "raw"), ("energy_source_code_1", "raw"),
    ("fuel_type_code_pudl", "raw"), ("technology_description", "raw"),
    ("generator_retirement_date", "date"),
]
GEN_FUEL_FIELDS = [
    ("energy_source_code", "raw"), ("prime_mover_code", "raw"),
    ("fuel_type_code_pudl", "raw"), ("fuel_type_code_agg", "raw"),
    ("fuel_consumed_units", "dbl"), ("fuel_consumed_for_electricity_units", "dbl"),
    ("fuel_mmbtu_per_unit", "dbl"), ("fuel_consumed_mmbtu", "dbl"),
    ("fuel_consumed_for_electricity_mmbtu", "dbl"), ("net_generation_mwh", "dbl"),
]
PLANT_COLS = ["plant_id_eia", "plant_name_eia", "city", "state", "county",
              "county_id_fips", "latitude", "longitude", "timezone"]
XWALK_COLS = ["report_year", "plant_id_epa", "emissions_unit_id_epa", "generator_id_epa",
              "plant_id_eia", "boiler_id", "generator_id"]
CEMS_KEYS = ["plant_id_epa", "emissions_unit_id_epa", "operating_datetime_utc",
             "plant_id_eia", "state", "year"]
CEMS_MEASURES = [
    "operating_time_hours", "gross_load_mw", "heat_content_mmbtu", "steam_load_lbs",
    "so2_mass_lbs", "so2_mass_measurement_code", "nox_mass_lbs", "nox_mass_measurement_code",
    "co2_mass_tons", "co2_mass_measurement_code",
]
CEMS_TEXT_MEASURES = {"so2_mass_measurement_code", "nox_mass_measurement_code",
                      "co2_mass_measurement_code"}

REQUIRED = {
    "plants": PLANT_COLS,
    "generators": ["plant_id_eia", "generator_id"] + [f for f, _ in GEN_YEARLY_FIELDS],
    "gen_fuel": ["plant_id_eia", "report_date"] + [f for f, _ in GEN_FUEL_FIELDS],
    "crosswalk": XWALK_COLS,
    "cems": CEMS_KEYS + CEMS_MEASURES,
}

COLLECTIONS = ["plants", "generators", "units", "monthly_generation_fuel",
               "cems_hourly_emissions", "unit_monthly", "plant_year_emissions"]

plant_info = {}   # plant_id_eia -> {"state", "county_id_fips", "location"}
rejected = {}     # any docs not validated/refused


# SQL / VALUE HELPERS
def dbl(col):
    return f"CASE WHEN isnan(CAST({col} AS DOUBLE)) THEN NULL ELSE CAST({col} AS DOUBLE) END"


def struct_expr(fields):
    parts = []
    for name, kind in fields:
        if kind == "date":
            expr = f"CAST({name} AS TIMESTAMP)"
        elif kind == "dbl":
            expr = dbl(name)
        else:
            expr = name
        parts.append(f"{name} := {expr}")
    return "struct_pack(" + ", ".join(parts) + ")"


def norm_fips(value):
    if value is None:
        return None
    s = str(value).strip()
    return s.zfill(5) if s.isdigit() and len(s) <= 5 else None


def geo_point(lon, lat):
    if lon is None or lat is None:
        return None
    return {"type": "Point", "coordinates": [lon, lat]}


# INSERT HELPERS
def report_errors(name, errors, docs):
    rejected[name] = rejected.get(name, 0) + len(errors)
    for err in errors[:3]:
        doc = docs[err["index"]]
        key = doc.get("_id", doc.get("unit"))
        print(f"\n  [rejected] {name}: {err.get('errmsg', '')[:80]} | {str(key)[:80]}")


def insert_docs(coll, docs, raw=False):
    if not docs:
        return 0
    if raw:
        # Raw `insert` command: the driver does not add an _id, which time series
        # collections do not need (insert_many would add an ObjectId to every row)
        res = coll.database.command("insert", coll.name, documents=docs, ordered=False)
        errors = res.get("writeErrors", [])
        if errors:
            report_errors(coll.name, errors, docs)
        return res.get("n", 0)
    try:
        return len(coll.insert_many(docs, ordered=False).inserted_ids)
    except BulkWriteError as exc:
        errors = exc.details.get("writeErrors", [])
        report_errors(coll.name, errors, docs)
        return exc.details.get("nInserted", 0)


def stream(result, make_doc, coll, label, raw=False, batch=BATCH_SIZE):
    total = 0
    while True:
        rows = result.fetchmany(batch)
        if not rows:
            break
        docs = [d for d in map(make_doc, rows) if d is not None]
        total += insert_docs(coll, docs, raw=raw)
        print(f"\r  {label}: {total:,} docs", end="", flush=True)
    print()
    return total


# STEPS
def check_columns(duck):
    for src, needed in REQUIRED.items():
        rows = duck.execute(f"DESCRIBE SELECT * FROM '{SRC[src]}'").fetchall()
        found = {r[0] for r in rows}
        missing = [c for c in needed if c not in found]
        if missing:
            raise SystemExit(f"{src}: columns not found in the Parquet file: {missing}")


def apply_schema():
    if shutil.which("mongosh") is None:
        raise SystemExit("mongosh not found on the PATH. Install it, or run schema.js yourself "
                         "with DROP_EXISTING=true and comment out apply_schema() in main().")
    script = f"var DROP_EXISTING = true; load({json.dumps(SCHEMA_JS.as_posix())})"
    subprocess.run(["mongosh", MONGO_URI, "--quiet", "--eval", script], check=True)


def load_plants(duck, db):
    query = (f"SELECT plant_id_eia, plant_name_eia, city, state, county, county_id_fips, "
             f"{dbl('latitude')}, {dbl('longitude')}, timezone "
             f"FROM '{SRC['plants']}' WHERE state IN ({STATES})")

    def make(row):
        pid, name, city, state, county, fips, lat, lon, tz = row
        fips = norm_fips(fips)
        loc = geo_point(lon, lat)
        plant_info[pid] = {"state": state, "county_id_fips": fips, "location": loc}
        doc = {"_id": pid, "plant_name_eia": name, "city": city, "state": state,
               "county": county, "county_id_fips": fips, "timezone": tz}
        if loc:
            doc["location"] = loc
        return doc

    return stream(duck.execute(query), make, db.plants, "plants")


def load_generators(duck, db):
    query = f"""
        WITH keys AS (
            SELECT DISTINCT plant_id_eia, generator_id
            FROM '{SRC['generators']}' WHERE {WHERE_GEN}
            UNION
            SELECT DISTINCT plant_id_eia, generator_id
            FROM '{SRC['crosswalk']}'
            WHERE {WHERE_XWALK} AND plant_id_eia IS NOT NULL AND generator_id IS NOT NULL
        ),
        yearly AS (
            SELECT plant_id_eia, generator_id,
                   list({struct_expr(GEN_YEARLY_FIELDS)} ORDER BY report_date) AS yearly
            FROM '{SRC['generators']}' WHERE {WHERE_GEN}
            GROUP BY plant_id_eia, generator_id
        )
        SELECT k.plant_id_eia, k.generator_id, y.yearly
        FROM keys k LEFT JOIN yearly y
          ON y.plant_id_eia = k.plant_id_eia AND y.generator_id = k.generator_id
    """

    def make(row):
        pid, gid, yearly = row
        info = plant_info.get(pid, {})
        return {
            "_id": {"plant_id_eia": pid, "generator_id": gid},
            "plant_id_eia": pid, "generator_id": gid,
            "state": info.get("state"),
            "county_id_fips": info.get("county_id_fips"),
            "yearly": yearly or [],
        }

    return stream(duck.execute(query), make, db.generators, "generators", batch=DOC_BATCH_SIZE)


def build_units(duck, db):
    # generator lookup from what was just loaded
    gen_lookup = {}
    for g in db.generators.find({}, {"yearly": 1}):
        gen_lookup[(g["_id"]["plant_id_eia"], g["_id"]["generator_id"])] = g["yearly"]

    query = (f"SELECT {', '.join(XWALK_COLS)} FROM '{SRC['crosswalk']}' WHERE {WHERE_XWALK}")
    units = {}
    for (year, pe, uid, gid_epa, peia, boiler, gid) in duck.execute(query).fetchall():
        unit = units.setdefault((pe, uid), {"plant_id_eia": peia, "gens": {}})
        if unit["plant_id_eia"] is None:
            unit["plant_id_eia"] = peia
        if gid is None:
            continue
        g = unit["gens"].setdefault(gid, {"generator_id_epa": gid_epa, "boilers": set(),
                                          "years": set()})
        if boiler is not None:
            g["boilers"].add(boiler)
        g["years"].add(year)
        if g["generator_id_epa"] is None:
            g["generator_id_epa"] = gid_epa

    docs = []
    for (pe, uid), unit in units.items():
        peia = unit["plant_id_eia"]
        info = plant_info.get(peia, {})
        generators = []
        for gid, g in sorted(unit["gens"].items()):
            yearly = gen_lookup.get((peia, gid), [])
            latest = yearly[-1] if yearly else {}
            retirement = next((y["generator_retirement_date"] for y in reversed(yearly)
                               if y.get("generator_retirement_date") is not None), None)
            generators.append({
                "generator_id": gid,
                "generator_id_epa": g["generator_id_epa"],
                # a generator can map to several boilers: the smallest id is kept
                "boiler_id": min(g["boilers"]) if g["boilers"] else None,
                "crosswalk_years": sorted(g["years"]),
                "technology_description": latest.get("technology_description"),
                "fuel_type_code_pudl": latest.get("fuel_type_code_pudl"),
                "prime_mover_code": latest.get("prime_mover_code"),
                "retirement_date": retirement,     # latest non-null value across the years
                "capacity_by_year": {str(y["report_date"].year): y["capacity_mw"]
                                     for y in yearly if y.get("capacity_mw") is not None},
            })
        docs.append({
            "_id": {"plant_id_epa": pe, "emissions_unit_id_epa": uid},
            "plant_id_epa": pe, "emissions_unit_id_epa": uid,
            "plant_id_eia": peia,
            "state": info.get("state"),
            "county_id_fips": info.get("county_id_fips"),
            "location": info.get("location"),
            "generators": generators,
        })
    total = 0
    for i in range(0, len(docs), DOC_BATCH_SIZE):
        total += insert_docs(db.units, docs[i:i + DOC_BATCH_SIZE])
    print(f"  units: {total:,} docs")
    return {key: u["plant_id_eia"] for key, u in units.items()}


#One document per (plant, month): energy source / prime mover rows embedded as fuels[]
def load_monthly_generation_fuel(duck, db):
    query = f"""
        SELECT plant_id_eia, CAST(report_date AS TIMESTAMP) AS report_date,
               list({struct_expr(GEN_FUEL_FIELDS)}) AS fuels
        FROM '{SRC['gen_fuel']}' WHERE {WHERE_GENFUEL}
        GROUP BY plant_id_eia, report_date
    """

    def make(row):
        pid, report_date, fuels = row
        return {
            "_id": {"plant_id_eia": pid, "report_date": report_date},
            "plant_id_eia": pid, "report_date": report_date,
            "state": plant_info.get(pid, {}).get("state"),
            "fuels": fuels,
        }

    return stream(duck.execute(query), make, db.monthly_generation_fuel,
                  "monthly_generation_fuel", batch=DOC_BATCH_SIZE)


def load_cems(duck, db, unit_to_plant):
    meta_cache, seen = {}, {}
    measure_sql = ", ".join(c if c in CEMS_TEXT_MEASURES else dbl(c) for c in CEMS_MEASURES)

    def make(row):
        pe, uid, ts, peia, state = row[:5]
        key = (pe, uid, peia, state)
        meta = meta_cache.get(key)
        if meta is None:
            resolved = peia if peia is not None else unit_to_plant.get((pe, uid))
            info = plant_info.get(resolved, {})
            meta = {"plant_id_epa": pe, "emissions_unit_id_epa": uid,
                    "plant_id_eia": resolved, "state": state,
                    "county_id_fips": info.get("county_id_fips")}
            meta_cache[key] = meta
            seen.setdefault((pe, uid), (resolved, state))
        doc = {"operating_datetime_utc": ts, "unit": meta}
        for name, value in zip(CEMS_MEASURES, row[5:]):
            if value is not None:                  # absent field instead of null: smaller buckets
                doc[name] = value
        return doc

    total = 0
    for year in YEAR_LIST:                         # one year at a time, sorted by unit and time
        query = (f"SELECT plant_id_epa, emissions_unit_id_epa, "
                 f"CAST(operating_datetime_utc AS TIMESTAMP), plant_id_eia, state, {measure_sql} "
                 f"FROM '{SRC['cems']}' WHERE state IN ({STATES}) AND year = {year} "
                 f"ORDER BY plant_id_epa, emissions_unit_id_epa, operating_datetime_utc")
        total += stream(duck.execute(query), make, db.cems_hourly_emissions,
                        f"cems {year}", raw=True)
    print(f"  cems_hourly_emissions total: {total:,} docs")
    return seen


def add_cems_only_units(db, seen, known_units):
    docs = []
    for (pe, uid), (peia, state) in seen.items():
        if (pe, uid) in known_units:
            continue
        info = plant_info.get(peia, {})
        docs.append({
            "_id": {"plant_id_epa": pe, "emissions_unit_id_epa": uid},
            "plant_id_epa": pe, "emissions_unit_id_epa": uid,
            "plant_id_eia": peia, "state": state or info.get("state"),
            "county_id_fips": info.get("county_id_fips"),
            "location": info.get("location"), "generators": [],
        })
    total = insert_docs(db.units, docs)
    print(f"  units added from CEMS only (not in the crosswalk): {total:,}")


# DERIVED COLLECTIONS (optional, layer 2 in docSchema)
def build_rollups(db):
    unit_monthly = [
        {"$group": {
            "_id": {"plant_id_epa": "$unit.plant_id_epa",
                    "emissions_unit_id_epa": "$unit.emissions_unit_id_epa",
                    "month": {"$dateTrunc": {"date": "$operating_datetime_utc", "unit": "month"}}},
            "plant_id_eia": {"$first": "$unit.plant_id_eia"},
            "state": {"$first": "$unit.state"},
            "county_id_fips": {"$first": "$unit.county_id_fips"},
            "hours_operated": {"$sum": "$operating_time_hours"},
            "gross_mwh": {"$sum": "$gross_load_mw"},
            "heat_input_mmbtu": {"$sum": "$heat_content_mmbtu"},
            "co2_tons": {"$sum": "$co2_mass_tons"},
            "so2_lbs": {"$sum": "$so2_mass_lbs"},
            "nox_lbs": {"$sum": "$nox_mass_lbs"}}},
        {"$set": {
            "plant_id_epa": "$_id.plant_id_epa",
            "emissions_unit_id_epa": "$_id.emissions_unit_id_epa",
            "month": "$_id.month",
            "unit_key": {"plant_id_epa": "$_id.plant_id_epa",
                         "emissions_unit_id_epa": "$_id.emissions_unit_id_epa"},
            "heat_rate": {"$cond": [{"$gt": ["$gross_mwh", 0]},
                                    {"$divide": ["$heat_input_mmbtu", "$gross_mwh"]}, None]}}},
        {"$lookup": {"from": "units", "localField": "unit_key", "foreignField": "_id", "as": "u"}},
        {"$unwind": {"path": "$u", "preserveNullAndEmptyArrays": True}},
        {"$set": {"ret": {"$max": "$u.generators.retirement_date"}}},
        {"$set": {"months_to_retirement": {"$cond": [
            {"$eq": [{"$type": "$ret"}, "date"]},
            {"$dateDiff": {"startDate": "$month", "endDate": "$ret", "unit": "month"}}, None]}}},
        {"$unset": ["u", "unit_key", "ret"]},
        {"$merge": {"into": "unit_monthly", "on": "_id",
                    "whenMatched": "replace", "whenNotMatched": "insert"}},
    ]
    plant_year = [
        {"$match": {"plant_id_eia": {"$ne": None}}},
        {"$group": {
            "_id": {"plant_id_eia": "$plant_id_eia", "year": {"$year": "$month"}},
            "co2_tons": {"$sum": "$co2_tons"},
            "so2_lbs": {"$sum": "$so2_lbs"},
            "nox_lbs": {"$sum": "$nox_lbs"}}},
        {"$set": {"plant_id_eia": "$_id.plant_id_eia", "year": "$_id.year"}},
        {"$lookup": {"from": "plants", "localField": "plant_id_eia",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": {"path": "$p", "preserveNullAndEmptyArrays": True}},
        {"$set": {"state": "$p.state", "county_id_fips": "$p.county_id_fips",
                  "location": "$p.location"}},
        {"$unset": "p"},
        {"$merge": {"into": "plant_year_emissions", "on": "_id",
                    "whenMatched": "replace", "whenNotMatched": "insert"}},
    ]
    db.cems_hourly_emissions.aggregate(unit_monthly, allowDiskUse=True)
    db.unit_monthly.aggregate(plant_year, allowDiskUse=True)


# VALIDATION
def validate(db):
    print("\nDocument counts:")
    for name in COLLECTIONS:
        print(f"  {name:<28}{db[name].count_documents({}):>14,}")
    # logic ref CEMS -> plants
    ids = list(plant_info.keys())
    orphans = db.cems_hourly_emissions.count_documents(
        {"unit.plant_id_eia": {"$ne": None, "$nin": ids}})
    unresolved = db.cems_hourly_emissions.count_documents({"unit.plant_id_eia": None})
    print(f"\nCEMS rows whose plant_id_eia is not in plants: {orphans:,}")
    print(f"CEMS rows with no plant_id_eia at all:          {unresolved:,}")
    if rejected:
        print("\nDocuments rejected by the validators:")
        for name, n in rejected.items():
            print(f"  {name:<28}{n:>14,}")
    else:
        print("\nNo documents were rejected by the validators.")


# MAIN
def main():
    duck = duckdb.connect()
    duck.execute("INSTALL httpfs")
    duck.execute("LOAD httpfs")
    duck.execute("CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-west-2', URL_STYLE 'path')")
    duck.execute("SET TimeZone = 'UTC'")
    check_columns(duck)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    timings = {}

    print("\n0) Applying schema.js (collections, validators, indexes)")
    start = time.perf_counter()
    apply_schema()
    timings["schema"] = time.perf_counter() - start

    print("\n1) Loading dimensions")
    start = time.perf_counter()
    load_plants(duck, db)
    timings["plants"] = time.perf_counter() - start

    start = time.perf_counter()
    load_generators(duck, db)
    timings["generators"] = time.perf_counter() - start

    print("\n2) Building units (EPA -> EIA bridge)")
    start = time.perf_counter()
    unit_to_plant = build_units(duck, db)
    timings["units"] = time.perf_counter() - start

    print("\n3) Loading facts")
    start = time.perf_counter()
    load_monthly_generation_fuel(duck, db)
    timings["monthly_generation_fuel"] = time.perf_counter() - start

    start = time.perf_counter()
    seen = load_cems(duck, db, unit_to_plant)
    add_cems_only_units(db, seen, unit_to_plant)
    timings["cems_hourly_emissions"] = time.perf_counter() - start

    if BUILD_ROLLUPS:
        print("\n4) Building derived collections")
        start = time.perf_counter()
        build_rollups(db)
        timings["rollups"] = time.perf_counter() - start

    validate(db)
    print("\nTimings (seconds):")
    for name, secs in timings.items():
        print(f"  {name:<28}{secs:>10.1f}")

    client.close()


if __name__ == "__main__":
    main()