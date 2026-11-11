"""
Phase II queries (1, 4, 5), run without and with the proposed index.

Can run just the selected query 
    python queries_1_4_5.py          # all queries
    python queries_1_4_5.py q1       # only Q1
    python queries_1_4_5.py q5       # only Q5
    python queries_1_4_5.py q4       # only Q4

Each benchmark: drop the index (clean baseline) -> EXPLAIN + timing without index -> create the index 
    -> EXPLAIN + timing with index -> drop the index again

"""
import getpass
import sys
import time

import mysql.connector

# ============================================================
# QUERIES
# ============================================================
# Q1: CO2, SO2 and NOx emitted per MWh generated, for each plant and year.
# Only plants burning a single fuel (coal, gas or oil) in that year are kept.
# Output: year, state, plant name, fuel type and the three intensities.
# Ordered by year and CO2 intensity (highest first).
Q1 = """
SELECT emission.year,
       plants.state,
       plants.plant_name_eia,
       generation.fuel_type,
       ROUND(emission.total_co2_tons / generation.total_net_generation_mwh, 4) AS co2_tons_per_mwh,
       ROUND(emission.total_so2_lbs  / generation.total_net_generation_mwh, 4) AS so2_lbs_per_mwh,
       ROUND(emission.total_nox_lbs  / generation.total_net_generation_mwh, 4) AS nox_lbs_per_mwh
FROM (
  SELECT plant_id_eia, year,
         SUM(co2_mass_tons) AS total_co2_tons,
         SUM(so2_mass_lbs)  AS total_so2_lbs,
         SUM(nox_mass_lbs)  AS total_nox_lbs
  FROM cems_hourly_emissions
  WHERE plant_id_eia IS NOT NULL
  GROUP BY plant_id_eia, year
) AS emission
JOIN (
  SELECT plant_id_eia, YEAR(report_date) AS year,
         MIN(fuel_type_code_pudl) AS fuel_type,
         SUM(net_generation_mwh)  AS total_net_generation_mwh
  FROM monthly_generation_fuel
  WHERE fuel_type_code_pudl IN ('coal', 'gas', 'oil')
  GROUP BY plant_id_eia, YEAR(report_date)
  HAVING COUNT(DISTINCT fuel_type_code_pudl) = 1 AND SUM(net_generation_mwh) > 0
) AS generation
  ON generation.plant_id_eia = emission.plant_id_eia
 AND generation.year         = emission.year
JOIN plants
  ON plants.plant_id_eia = emission.plant_id_eia
ORDER BY emission.year, co2_tons_per_mwh DESC
"""
# Q1 AVG: the same intensities over all the plants of each year and fuel type
# (total emissions / total generation), to see how they change through the years.
# Output: year, fuel type, number of plants, average CO2, SO2 and NOx per MWh.
# Ordered by year and fuel type.
Q1_AVG = """
SELECT generation.year,
       generation.fuel_type,
       COUNT(*) AS number_of_plants,
       ROUND(SUM(emission.total_co2_tons) / SUM(generation.total_net_generation_mwh), 4) AS avg_co2_tons_per_mwh,
       ROUND(SUM(emission.total_so2_lbs)  / SUM(generation.total_net_generation_mwh), 4) AS avg_so2_lbs_per_mwh,
       ROUND(SUM(emission.total_nox_lbs)  / SUM(generation.total_net_generation_mwh), 4) AS avg_nox_lbs_per_mwh
FROM (
  SELECT plant_id_eia, year,
         SUM(co2_mass_tons) AS total_co2_tons,
         SUM(so2_mass_lbs)  AS total_so2_lbs,
         SUM(nox_mass_lbs)  AS total_nox_lbs
  FROM cems_hourly_emissions
  WHERE plant_id_eia IS NOT NULL
  GROUP BY plant_id_eia, year
) AS emission
JOIN (
  SELECT plant_id_eia, YEAR(report_date) AS year,
         MIN(fuel_type_code_pudl) AS fuel_type,
         SUM(net_generation_mwh)  AS total_net_generation_mwh
  FROM monthly_generation_fuel
  WHERE fuel_type_code_pudl IN ('coal', 'gas', 'oil')
  GROUP BY plant_id_eia, YEAR(report_date)
  HAVING COUNT(DISTINCT fuel_type_code_pudl) = 1 AND SUM(net_generation_mwh) > 0
) AS generation
  ON generation.plant_id_eia = emission.plant_id_eia
 AND generation.year         = emission.year
GROUP BY generation.year, generation.fuel_type
ORDER BY generation.year, generation.fuel_type
"""

# Q4: seasonal capacity margin. Summer vs winter capacity of the operating thermal
# generators (coal, gas, oil), by state, technology and year. The summer derating
# reflects the loss of thermal efficiency at high ambient temperatures.
# Output: state, technology, year, number of generators, nominal/summer/winter capacity
# and summer derating (%). Ordered by year, state and derating (highest first).
Q4 = """
SELECT plants.state,
       generator_yearly.technology_description,
       YEAR(generator_yearly.report_date) AS report_year,
       COUNT(*) AS number_of_generators,
       ROUND(SUM(generator_yearly.capacity_mw), 1)        AS total_nominal_capacity_mw,
       ROUND(SUM(generator_yearly.summer_capacity_mw), 1) AS total_summer_capacity_mw,
       ROUND(SUM(generator_yearly.winter_capacity_mw), 1) AS total_winter_capacity_mw,
       ROUND((SUM(generator_yearly.winter_capacity_mw) - SUM(generator_yearly.summer_capacity_mw))
             / SUM(generator_yearly.winter_capacity_mw) * 100, 2) AS summer_derate_pct
FROM generator_yearly
JOIN plants ON plants.plant_id_eia = generator_yearly.plant_id_eia
WHERE generator_yearly.operational_status_code = 'OP'
  AND generator_yearly.fuel_type_code_pudl IN ('coal', 'gas', 'oil')
  AND generator_yearly.summer_capacity_mw IS NOT NULL
  AND generator_yearly.winter_capacity_mw IS NOT NULL
GROUP BY plants.state, generator_yearly.technology_description, YEAR(generator_yearly.report_date)
HAVING SUM(generator_yearly.winter_capacity_mw) > 0
ORDER BY report_year, plants.state, summer_derate_pct DESC
"""

# Q5: emissions per MWh by load tier. Each operating hour is classified by the unit's
# gross load relative to the maximum load of that unit in that year
# (peaking < 20%, mid-merit 20-70%, baseload >= 70%), to check whether the plants
# emit more per MWh at low load.
# Output: state, year, load tier, operating hours, NOx and CO2 per MWh.
# Ordered by state, year and load tier.
Q5 = """
SELECT hourly.state,
       hourly.year,
       CASE WHEN hourly.gross_load_mw < 0.20 * unit_max.max_load_mw THEN '1-Peaking (<20%)'
            WHEN hourly.gross_load_mw < 0.70 * unit_max.max_load_mw THEN '2-Mid-merit (20-70%)'
            ELSE '3-Baseload (>=70%)' END AS load_tier,
       COUNT(*) AS operating_hours,
       ROUND(SUM(hourly.nox_mass_lbs)  / SUM(hourly.gross_load_mw * hourly.operating_time_hours), 4) AS nox_lbs_per_mwh,
       ROUND(SUM(hourly.co2_mass_tons) / SUM(hourly.gross_load_mw * hourly.operating_time_hours), 4) AS co2_tons_per_mwh
FROM cems_hourly_emissions AS hourly
JOIN (
  SELECT plant_id_epa, emissions_unit_id_epa, year,
         MAX(gross_load_mw) AS max_load_mw
  FROM cems_hourly_emissions
  GROUP BY plant_id_epa, emissions_unit_id_epa, year
) AS unit_max
  ON unit_max.plant_id_epa          = hourly.plant_id_epa
 AND unit_max.emissions_unit_id_epa = hourly.emissions_unit_id_epa
 AND unit_max.year                  = hourly.year
WHERE hourly.gross_load_mw > 0 AND hourly.operating_time_hours > 0
GROUP BY hourly.state, hourly.year, load_tier
ORDER BY hourly.state, hourly.year, load_tier
"""

# ============================================================
# CONFIG: each query with its proposed index
# ============================================================
BENCHMARKS = {
    "q1": dict(
        title="Q1 - CO2, SO2, NOx intensity per MWh",
        sql=Q1,
        table="cems_hourly_emissions",
        index_name="idx_cems_plant_year",
        # Covering index, already ordered by the GROUP BY columns: read this narrow
        # index instead of the full table (moderate speed-up expected).
        index_columns="plant_id_eia, year, co2_mass_tons, so2_mass_lbs, nox_mass_lbs",
        extra_sql=Q1_AVG,
        extra_title="Average emissions per MWh (by year and fuel)",
    ),
    "q4": dict(
        title="Q4 - Seasonal capacity margins",
        sql=Q4,
        table="generator_yearly",
        index_name="idx_gy_status_cover",
        # The index will probably not help: small table, non-selective filter
        # and the GROUP BY cannot use the index order.
        index_columns=("operational_status_code, plant_id_eia, report_date, "
                       "technology_description, fuel_type_code_pudl, "
                       "capacity_mw, summer_capacity_mw, winter_capacity_mw"),
    ),
    "q5": dict(
        title="Q5 - Load tiers and environmental penalty of peaking",
        sql=Q5,
        table="cems_hourly_emissions",
        index_name="idx_cems_unit_year",
        # Covering index: avoids the table lookups and orders the unit_max subquery, but the
        # outer GROUP BY on load_tier still needs a temporary table (moderate speed-up expected).
        index_columns=("plant_id_epa, emissions_unit_id_epa, year, gross_load_mw, "
                       "operating_time_hours, state, nox_mass_lbs, co2_mass_tons"),
    ),
}


# ============================================================
# FUNCTIONS
# ============================================================
def connect():
    return mysql.connector.connect(
        host="localhost", user="root",
        password=getpass.getpass("MySQL password: "),
        database="pudl_northeast", autocommit=True,
    )


def drop_index(cur, b):
    try:
        cur.execute(f"DROP INDEX {b['index_name']} ON {b['table']}")
        print("Existing index dropped.")
    except mysql.connector.Error:
        pass  # the index does not exist: the baseline is clean


def create_index(cur, b):
    start = time.perf_counter()
    cur.execute(f"CREATE INDEX {b['index_name']} ON {b['table']} ({b['index_columns']})")
    return time.perf_counter() - start


def show_plan(db, sql):
    cur = db.cursor(dictionary=True)
    cur.execute("EXPLAIN " + sql)
    for r in cur.fetchall():
        print(f"  {str(r['table']):<24} key={r['key']}  rows={r['rows']}  {r['Extra'] or ''}")


def time_query(cur, sql, runs=2):
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        cur.execute(sql)
        cur.fetchall()
        times.append(time.perf_counter() - start)
    return times


def print_summary(title, before, after, build_time):
    print(f"\n{title}")
    print(f"{'':<14}{'before':>10}{'after':>10}{'speed-up':>10}")
    for label, b, a in [("1st run (s)", before[0], after[0]),
                        ("2nd run (s)", before[1], after[1])]:
        print(f"{label:<14}{b:>10.3f}{a:>10.3f}{b / a:>9.3f}x")
    print(f"index build time: {build_time:.3f}s")


def print_table(cur, sql, title):
    print(f"\n{title}")
    cur.execute(sql)
    print("  ".join(f"{c:<16}" for c in cur.column_names))
    for row in cur.fetchall():
        print("  ".join(f"{str(v):<16}" for v in row))


def run_benchmark(db, b):
    cur = db.cursor()
    print(f"\n{'=' * 60}\n{b['title']}\n{'=' * 60}")

    drop_index(cur, b)
    print("EXPLAIN without index:")
    show_plan(db, b["sql"])
    print("Running without index...")
    before = time_query(cur, b["sql"])

    print("Creating index...")
    build_time = create_index(cur, b)

    print("EXPLAIN with index:")
    show_plan(db, b["sql"])
    print("Running with index...")
    after = time_query(cur, b["sql"])

    print_summary(b["title"], before, after, build_time)
    if "extra_sql" in b:
        print_table(cur, b["extra_sql"], b["extra_title"])

    drop_index(cur, b)


def main():
    names = sys.argv[1:] or list(BENCHMARKS)
    db = connect()
    for name in names:
        run_benchmark(db, BENCHMARKS[name])
    db.close()


if __name__ == "__main__":
    main()
