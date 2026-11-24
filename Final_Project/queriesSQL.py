"""
Phase II queries (1, 4, 5), run without and with the proposed index.

Can run just the selected query 
    python queriesSQL.py          # all queries
    python queriesSQL.py q1       # only Q1
    python queriesSQL.py q2       # only Q2
    python queriesSQL.py q3       # only Q3
    python queriesSQL.py q4       # only Q4
    python queriesSQL.py q5       # only Q5
    

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

Q1 = """
WITH emission AS (
  SELECT plant_id_eia,
         year,
         SUM(co2_mass_tons) AS total_co2_tons,
         SUM(so2_mass_lbs)  AS total_so2_lbs,
         SUM(nox_mass_lbs)  AS total_nox_lbs
  FROM cems_hourly_emissions
  WHERE plant_id_eia IS NOT NULL
  GROUP BY plant_id_eia, year
),
fuel_totals AS (
  SELECT plant_id_eia,
         YEAR(report_date) AS year,
         fuel_type_code_pudl AS fuel_type,
         COALESCE(SUM(fuel_consumed_mmbtu), 0) AS fuel_mmbtu,
         SUM(net_generation_mwh)               AS fuel_net_generation_mwh
  FROM monthly_generation_fuel
  WHERE fuel_type_code_pudl IS NOT NULL
  GROUP BY plant_id_eia, YEAR(report_date), fuel_type_code_pudl
),
plant_year_totals AS (
  SELECT plant_id_eia,
         year,
         SUM(fuel_net_generation_mwh) AS total_net_generation_mwh,
         MAX(fuel_mmbtu)              AS max_fuel_mmbtu
  FROM fuel_totals
  GROUP BY plant_id_eia, year
),
generation AS (
  SELECT pyt.plant_id_eia,
         pyt.year,
         pyt.total_net_generation_mwh,
         MIN(ft.fuel_type) AS fuel_type
  FROM plant_year_totals AS pyt
  JOIN fuel_totals AS ft
    ON ft.plant_id_eia = pyt.plant_id_eia
   AND ft.year         = pyt.year
   AND ft.fuel_mmbtu   = pyt.max_fuel_mmbtu
  WHERE pyt.total_net_generation_mwh > 0
  GROUP BY pyt.plant_id_eia, pyt.year, pyt.total_net_generation_mwh
  HAVING MIN(ft.fuel_type) IN ('coal', 'gas', 'oil')
)
SELECT emission.year,
       plants.state,
       plants.plant_name_eia,
       generation.fuel_type,
       ROUND(emission.total_co2_tons / generation.total_net_generation_mwh, 4) AS co2_tons_per_mwh,
       ROUND(emission.total_so2_lbs  / generation.total_net_generation_mwh, 4) AS so2_lbs_per_mwh,
       ROUND(emission.total_nox_lbs  / generation.total_net_generation_mwh, 4) AS nox_lbs_per_mwh
FROM emission
JOIN generation
  ON generation.plant_id_eia = emission.plant_id_eia
 AND generation.year         = emission.year
JOIN plants
  ON plants.plant_id_eia = emission.plant_id_eia
ORDER BY emission.year, co2_tons_per_mwh DESC;
"""

# Q1 AVG: same intensities across all plants for each year and dominant fuel type
# (total emissions / total net generation across all fuels), using non-NULL emission
# denominators so missing pollutant totals do not dilute averages.
Q1_AVG = """
WITH emission AS (
  SELECT plant_id_eia,
         year,
         SUM(co2_mass_tons) AS total_co2_tons,
         SUM(so2_mass_lbs)  AS total_so2_lbs,
         SUM(nox_mass_lbs)  AS total_nox_lbs
  FROM cems_hourly_emissions
  WHERE plant_id_eia IS NOT NULL
  GROUP BY plant_id_eia, year
),
fuel_totals AS (
  SELECT plant_id_eia,
         YEAR(report_date) AS year,
         fuel_type_code_pudl AS fuel_type,
         COALESCE(SUM(fuel_consumed_mmbtu), 0) AS fuel_mmbtu,
         SUM(net_generation_mwh)               AS fuel_net_generation_mwh
  FROM monthly_generation_fuel
  WHERE fuel_type_code_pudl IS NOT NULL
  GROUP BY plant_id_eia, YEAR(report_date), fuel_type_code_pudl
),
plant_year_totals AS (
  SELECT plant_id_eia,
         year,
         SUM(fuel_net_generation_mwh) AS total_net_generation_mwh,
         MAX(fuel_mmbtu)              AS max_fuel_mmbtu
  FROM fuel_totals
  GROUP BY plant_id_eia, year
),
generation AS (
  SELECT pyt.plant_id_eia,
         pyt.year,
         pyt.total_net_generation_mwh,
         MIN(ft.fuel_type) AS fuel_type
  FROM plant_year_totals AS pyt
  JOIN fuel_totals AS ft
    ON ft.plant_id_eia = pyt.plant_id_eia
   AND ft.year         = pyt.year
   AND ft.fuel_mmbtu   = pyt.max_fuel_mmbtu
  WHERE pyt.total_net_generation_mwh > 0
  GROUP BY pyt.plant_id_eia, pyt.year, pyt.total_net_generation_mwh
  HAVING MIN(ft.fuel_type) IN ('coal', 'gas', 'oil')
)
SELECT generation.year,
       generation.fuel_type,
       COUNT(*) AS number_of_plants,
       ROUND(SUM(emission.total_co2_tons) /
             SUM(CASE WHEN emission.total_co2_tons IS NOT NULL THEN generation.total_net_generation_mwh END), 4) AS avg_co2_tons_per_mwh,
       ROUND(SUM(emission.total_so2_lbs) /
             SUM(CASE WHEN emission.total_so2_lbs IS NOT NULL THEN generation.total_net_generation_mwh END), 4) AS avg_so2_lbs_per_mwh,
       ROUND(SUM(emission.total_nox_lbs) /
             SUM(CASE WHEN emission.total_nox_lbs IS NOT NULL THEN generation.total_net_generation_mwh END), 4) AS avg_nox_lbs_per_mwh
FROM emission
JOIN generation
  ON generation.plant_id_eia = emission.plant_id_eia
 AND generation.year         = emission.year
GROUP BY generation.year, generation.fuel_type
ORDER BY generation.year, generation.fuel_type;
"""

# Q2: Heat Rate Degradation vs. Retirement Date

Q2 = """
WITH gen_lifetime AS (
    SELECT plant_id_eia, generator_id,
           MIN(generator_retirement_date) AS lifetime_retirement_date,
           MIN(technology_description)    AS lifetime_technology
    FROM generator_yearly
    GROUP BY plant_id_eia, generator_id
)
SELECT u.technology,
       TIMESTAMPDIFF(YEAR, c.operating_datetime_utc, u.retirement_date) AS years_to_retirement,
       SUM(c.heat_content_mmbtu) / SUM(c.gross_load_mw * c.operating_time_hours) AS heat_rate_mmbtu_per_mwh,
       COUNT(*) AS n_hours,
       COUNT(DISTINCT c.plant_id_epa, c.emissions_unit_id_epa) AS n_units
FROM cems_hourly_emissions c
JOIN (
    SELECT x.plant_id_epa,
           x.emissions_unit_id_epa,
           YEAR(g.report_date) AS report_year,
           MIN(r.lifetime_technology)      AS technology,
           MIN(r.lifetime_retirement_date) AS retirement_date
    FROM epa_eia_crosswalk x
    JOIN generator_yearly g
      ON x.plant_id_eia = g.plant_id_eia
     AND x.generator_id = g.generator_id
    JOIN gen_lifetime r
      ON r.plant_id_eia = g.plant_id_eia
     AND r.generator_id = g.generator_id
    GROUP BY x.plant_id_epa, x.emissions_unit_id_epa, YEAR(g.report_date)
    HAVING COUNT(DISTINCT r.lifetime_technology) = 1
) u
  ON c.plant_id_epa = u.plant_id_epa
 AND c.emissions_unit_id_epa = u.emissions_unit_id_epa
 AND c.year = u.report_year
WHERE c.heat_content_mmbtu > 0
  AND c.gross_load_mw > 0
  AND c.operating_time_hours > 0
  AND u.retirement_date IS NOT NULL
  AND u.retirement_date >= c.operating_datetime_utc
GROUP BY u.technology, years_to_retirement
ORDER BY u.technology, years_to_retirement DESC;
"""


# Q3: Spatial Hotspots
Q3 = """
-- Pareto test
WITH facility_totals AS (
    SELECT plant_id_epa, state,
           SUM(co2_mass_tons) AS total_co2_tons,
           SUM(so2_mass_lbs)  AS total_so2_lbs,
           SUM(nox_mass_lbs)  AS total_nox_lbs
    FROM cems_hourly_emissions
    GROUP BY plant_id_epa, state
),
facility_ranked AS (
    SELECT total_co2_tons, ROW_NUMBER() OVER (ORDER BY total_co2_tons DESC) AS co2_rank,
           total_so2_lbs,  ROW_NUMBER() OVER (ORDER BY total_so2_lbs DESC)  AS so2_rank,
           total_nox_lbs,  ROW_NUMBER() OVER (ORDER BY total_nox_lbs DESC)  AS nox_rank,
           COUNT(*) OVER () AS n_units
    FROM facility_totals
),
plant_totals AS (
    SELECT plant_id_eia,
           SUM(co2_mass_tons) AS total_co2_mass_tons,
           SUM(so2_mass_lbs)  AS total_so2_mass_lbs,
           SUM(nox_mass_lbs)  AS total_nox_mass_lbs
    FROM cems_hourly_emissions
    WHERE plant_id_eia IS NOT NULL
    GROUP BY plant_id_eia
),
county_totals AS (
    SELECT p.county_id_fips,
           p.county,
           p.state,
           SUM(pt.total_co2_mass_tons) AS total_co2_tons,
           SUM(pt.total_so2_mass_lbs)  AS total_so2_lbs,
           SUM(pt.total_nox_mass_lbs)  AS total_nox_lbs
    FROM plant_totals pt
    JOIN plants p ON pt.plant_id_eia = p.plant_id_eia
    GROUP BY p.county_id_fips, p.county, p.state
),
county_ranked AS (
    SELECT total_co2_tons, ROW_NUMBER() OVER (ORDER BY total_co2_tons DESC) AS co2_rank,
           total_so2_lbs,  ROW_NUMBER() OVER (ORDER BY total_so2_lbs DESC)  AS so2_rank,
           total_nox_lbs,  ROW_NUMBER() OVER (ORDER BY total_nox_lbs DESC)  AS nox_rank,
           COUNT(*) OVER () AS n_units
    FROM county_totals
)
SELECT 'facility' AS level,
       MAX(n_units) AS n_units,
       CAST(SUM(CASE WHEN co2_rank <= n_units * 0.20 THEN total_co2_tons END) / SUM(total_co2_tons) AS DECIMAL(5,4)) AS co2_top20_share,
       CAST(SUM(CASE WHEN so2_rank <= n_units * 0.20 THEN total_so2_lbs  END) / SUM(total_so2_lbs)  AS DECIMAL(5,4)) AS so2_top20_share,
       CAST(SUM(CASE WHEN nox_rank <= n_units * 0.20 THEN total_nox_lbs  END) / SUM(total_nox_lbs)  AS DECIMAL(5,4)) AS nox_top20_share
FROM facility_ranked
UNION ALL
SELECT 'county',
       MAX(n_units),
       CAST(SUM(CASE WHEN co2_rank <= n_units * 0.20 THEN total_co2_tons END) / SUM(total_co2_tons) AS DECIMAL(5,4)),
       CAST(SUM(CASE WHEN so2_rank <= n_units * 0.20 THEN total_so2_lbs  END) / SUM(total_so2_lbs)  AS DECIMAL(5,4)),
       CAST(SUM(CASE WHEN nox_rank <= n_units * 0.20 THEN total_nox_lbs  END) / SUM(total_nox_lbs)  AS DECIMAL(5,4))
FROM county_ranked;

-- Top 10 hotspots
WITH plant_totals AS (
    SELECT plant_id_eia,
           SUM(co2_mass_tons) AS total_co2_mass_tons,
           SUM(so2_mass_lbs)  AS total_so2_mass_lbs,
           SUM(nox_mass_lbs)  AS total_nox_mass_lbs
    FROM cems_hourly_emissions
    WHERE plant_id_eia IS NOT NULL
    GROUP BY plant_id_eia
),
county_totals AS (
    SELECT p.county_id_fips,
           p.county,
           p.state,
           SUM(pt.total_co2_mass_tons) AS total_co2_tons,
           SUM(pt.total_so2_mass_lbs)  AS total_so2_lbs,
           SUM(pt.total_nox_mass_lbs)  AS total_nox_lbs
    FROM plant_totals pt
    JOIN plants p ON pt.plant_id_eia = p.plant_id_eia
    GROUP BY p.county_id_fips, p.county, p.state
),
ranked AS (
    SELECT county_id_fips, county, state,
           total_co2_tons, ROW_NUMBER() OVER (ORDER BY total_co2_tons DESC) AS co2_rank,
           total_so2_lbs,  ROW_NUMBER() OVER (ORDER BY total_so2_lbs DESC)  AS so2_rank,
           total_nox_lbs,  ROW_NUMBER() OVER (ORDER BY total_nox_lbs DESC)  AS nox_rank
    FROM county_totals
)
SELECT 'CO2' AS pollutant, co2_rank AS hotspot_rank, county, state,
       total_co2_tons AS total_emissions, 'tons' AS units
FROM ranked WHERE co2_rank <= 10
UNION ALL
SELECT 'SO2', so2_rank, county, state, total_so2_lbs, 'lbs'
FROM ranked WHERE so2_rank <= 10
UNION ALL
SELECT 'NOx', nox_rank, county, state, total_nox_lbs, 'lbs'
FROM ranked WHERE nox_rank <= 10
ORDER BY pollutant, hotspot_rank;
"""

Q3_PARETO, Q3_HOTSPOTS = [
    part.replace("-- Pareto test", "").strip()
    for part in Q3.split("-- Top 10 hotspots")
]


# Q4: seasonal capacity margin. Summer vs winter capacity of the operating thermal
# generators (coal, gas, oil), by state, technology and year. The summer derating
# reflects the loss of thermal efficiency at high ambient temperatures.
# Output: state, technology, year, number of generators, nominal/summer/winter capacity
# and summer derating (%). Ordered by year, state and derating (highest first).
Q4 = """
WITH gen_lifetime AS (
    SELECT plant_id_eia, generator_id,
          MIN(technology_description) AS lifetime_technology
    FROM generator_yearly
   GROUP BY plant_id_eia, generator_id
)
SELECT plants.state,
       r.lifetime_technology AS technology_description,
       YEAR(generator_yearly.report_date) AS report_year,
       COUNT(*) AS number_of_generators,
       ROUND(SUM(generator_yearly.capacity_mw), 1)       AS
total_nominal_capacity_mw,
       ROUND(SUM(generator_yearly.summer_capacity_mw), 1) AS
total_summer_capacity_mw,
       ROUND(SUM(generator_yearly.winter_capacity_mw), 1) AS
total_winter_capacity_mw,
       ROUND((SUM(generator_yearly.winter_capacity_mw) -
SUM(generator_yearly.summer_capacity_mw))/ SUM(generator_yearly.winter_capacity_mw) * 100, 2) AS
summer_derate_pct

FROM generator_yearly
JOIN plants ON plants.plant_id_eia = generator_yearly.plant_id_eia
JOIN gen_lifetime r
 ON r.plant_id_eia = generator_yearly.plant_id_eia
 AND r.generator_id = generator_yearly.generator_id
WHERE generator_yearly.operational_status_code = 'OP'
  AND generator_yearly.fuel_type_code_pudl IN ('coal', 'gas', 'oil')
  AND generator_yearly.summer_capacity_mw IS NOT NULL
  AND generator_yearly.winter_capacity_mw IS NOT NULL
GROUP BY plants.state, r.lifetime_technology, YEAR(generator_yearly.report_date)
HAVING SUM(generator_yearly.winter_capacity_mw) > 0
ORDER BY report_year, plants.state, summer_derate_pct DESC;
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
       ROUND(SUM(hourly.nox_mass_lbs) /
             SUM(CASE WHEN hourly.nox_mass_lbs IS NOT NULL
                      THEN hourly.gross_load_mw * hourly.operating_time_hours END), 4) AS nox_lbs_per_mwh,
       ROUND(SUM(hourly.co2_mass_tons) /
             SUM(CASE WHEN hourly.co2_mass_tons IS NOT NULL
                      THEN hourly.gross_load_mw * hourly.operating_time_hours END), 4) AS co2_tons_per_mwh
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
ORDER BY hourly.state, hourly.year, load_tier;
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
        index_columns=("plant_id_eia, year, co2_mass_tons, so2_mass_lbs, nox_mass_lbs"),
        extra_sql=Q1_AVG,
        extra_title="Average emissions per MWh (by year and fuel)",
    ),
    "q2": dict(
    title="Q2 - Heat Rate Degradation vs. Retirement Date",
    sql=Q2,
    table="cems_hourly_emissions",
    index_name="idx_cems_heat_rate",
    index_columns=("plant_id_epa, emissions_unit_id_epa, year, heat_content_mmbtu, gross_load_mw, operating_time_hours"),
    # Without better statistics the optimizer ignores the index (wrong row estimates).
    # Executed right after the index is created, before the "with index" EXPLAIN/timing.
    setup_sql=[
        "ALTER TABLE cems_hourly_emissions STATS_SAMPLE_PAGES = 1000",
        "ANALYZE TABLE cems_hourly_emissions",
    ],
),
    "q3": dict(
    title="Q3 - Spatial Hotspots",
    queries=[("Pareto test", Q3_PARETO), ("Top 10 hotspots", Q3_HOTSPOTS)],
    indexes=[
        dict(table="cems_hourly_emissions",
             index_name="idx_cems_facility_emissions",
             index_columns="plant_id_epa, state, co2_mass_tons, so2_mass_lbs, nox_mass_lbs"),
        dict(table="cems_hourly_emissions",
             index_name="idx_cems_plant",
             index_columns="plant_id_eia, co2_mass_tons, so2_mass_lbs, nox_mass_lbs"),
    ],
),
    "q4": dict(
        title="Q4 - Seasonal capacity margins",
        sql=Q4,
        table="generator_yearly",
        index_name="idx_gy_status_cover",
        # The index will probably not help: small table, non-selective filter
        # and the GROUP BY cannot use the index order.
        index_columns=("operational_status_code, plant_id_eia, report_date, "
                       "fuel_type_code_pudl, "
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


def show_plan(db, sql):
    cur = db.cursor(dictionary=True)
    cur.execute("EXPLAIN FORMAT=TRADITIONAL " + sql)
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


def get_indexes(b):
    """Always returns a list of indexes (supports both config formats)."""
    if "indexes" in b:
        return b["indexes"]
    return [dict(table=b["table"], index_name=b["index_name"],
                 index_columns=b["index_columns"])]


def get_queries(b):
    """Always returns a list of (label, sql)."""
    if "queries" in b:
        return b["queries"]
    return [(b["title"], b["sql"])]


def drop_indexes(cur, b):
    for idx in get_indexes(b):
        try:
            cur.execute(f"DROP INDEX {idx['index_name']} ON {idx['table']}")
            print(f"Existing index dropped: {idx['index_name']}")
        except mysql.connector.Error:
            pass  # the index does not exist: the baseline is clean


def create_indexes(cur, b):
    start = time.perf_counter()
    for idx in get_indexes(b):
        cur.execute(f"CREATE INDEX {idx['index_name']} ON {idx['table']} ({idx['index_columns']})")
    return time.perf_counter() - start  # total build time of all the indexes

def run_setup(cur, b):
    """Optional statements to run after the index is created (e.g. ANALYZE TABLE)."""
    for stmt in b.get("setup_sql", []):
        print(f"  setup: {stmt}")
        cur.execute(stmt)
        if cur.with_rows:
            cur.fetchall()  # ANALYZE TABLE returns a result set: consume it

def run_benchmark(db, b):
    cur = db.cursor()
    queries = get_queries(b)
    print(f"\n{'=' * 60}\n{b['title']}\n{'=' * 60}")

    drop_indexes(cur, b)
    before = {}
    for label, sql in queries:
        print(f"EXPLAIN without index [{label}]:")
        show_plan(db, sql)
        print(f"Running without index [{label}]...")
        before[label] = time_query(cur, sql)

    print("Creating indexes...")
    build_time = create_indexes(cur, b)
    run_setup(cur, b)

    after = {}
    for label, sql in queries:
        print(f"EXPLAIN with index [{label}]:")
        show_plan(db, sql)
        print(f"Running with index [{label}]...")
        after[label] = time_query(cur, sql)

    for label, _ in queries:
        print_summary(f"{b['title']} - {label}", before[label], after[label], build_time)
    if "extra_sql" in b:
        print_table(cur, b["extra_sql"], b["extra_title"])

    drop_indexes(cur, b)

def main():
    names = sys.argv[1:] or list(BENCHMARKS)
    db = connect()
    cur = db.cursor()
    for b in BENCHMARKS.values():
        drop_indexes(cur, b)
    for name in names:
        run_benchmark(db, BENCHMARKS[name])
    db.close()


if __name__ == "__main__":
    main()
