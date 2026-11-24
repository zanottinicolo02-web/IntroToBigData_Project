"""
Loader: PUDL + EPA CEMS (US Northeast, sample years) -> MySQL 8.0

- Reads the Parquet files directly from the public PUDL S3 bucket through DuckDB
  (filters are pushed down to the files, nothing is stored on the local disk).
- Writes to MySQL with mysql-connector-python, using batched INSERTs.
- Tables are loaded WITHOUT keys; primary and foreign keys are added at the end
  (much faster for the big tables, and ALTER validates all the data).
- No secondary indexes are created in this phase.

Requirements:  pip install duckdb mysql-connector-python
WARNING: the tables of the schema are DROPPED and recreated at every run.
"""
import getpass
import os
import time

import duckdb
import mysql.connector

# ============================================================
# CONFIG
# ============================================================
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_DB = "pudl_northeast"
BATCH_SIZE = 20_000  # rows per INSERT / commit

BASE = "s3://pudl.catalyst.coop/stable/"
SRC = {
    "cems": BASE + "core_epacems__hourly_emissions.parquet",
    "plants": BASE + "core_eia__entity_plants.parquet",
    "generators": BASE + "core_eia860__scd_generators.parquet",
    "gen_fuel": BASE + "core_eia923__monthly_generation_fuel.parquet",
    "crosswalk": BASE + "core_epa__assn_eia_epacamd.parquet",
}

STATES = ", ".join(f"'{s}'" for s in ["CT", "ME", "MA", "NH", "RI", "VT", "NJ", "NY", "PA"])
YEARS = ", ".join(str(y) for y in [2000, 2005, 2010, 2015, 2018, 2020, 2023, 2025])

# Plants of the Northeast: used to filter all the EIA tables and the crosswalk
NE_PLANTS = f"SELECT plant_id_eia FROM '{SRC['plants']}' WHERE state IN ({STATES})"

# ============================================================
# SCHEMA (from relational_model_NICO_draft.drawio)
# Each table: MySQL name, source file, filter, columns (name, MySQL type).
# Column names are the same in Parquet and in MySQL.
# ============================================================
TABLES = [
    dict(
        name="plants", src="plants",
        where=f"state IN ({STATES})",
        cols=[
            ("plant_id_eia", "INT UNSIGNED"), ("plant_name_eia", "VARCHAR(255)"),
            ("city", "VARCHAR(100)"), ("state", "CHAR(2)"), ("county", "VARCHAR(100)"),
            ("county_id_fips", "CHAR(5)"), ("latitude", "DECIMAL(9,6)"),
            ("longitude", "DECIMAL(9,6)"), ("timezone", "VARCHAR(40)"),
        ],
    ),
    dict(
        name="generator_yearly", src="generators",
        where=f"plant_id_eia IN ({NE_PLANTS}) AND EXTRACT(year FROM report_date) IN ({YEARS})",
        cols=[
            ("plant_id_eia", "INT UNSIGNED"), ("generator_id", "VARCHAR(20)"),
            ("report_date", "DATE"), ("utility_id_eia", "INT UNSIGNED"),
            ("operational_status_code", "VARCHAR(10)"), ("capacity_mw", "DOUBLE"),
            ("summer_capacity_mw", "DOUBLE"), ("winter_capacity_mw", "DOUBLE"),
            ("prime_mover_code", "VARCHAR(10)"), ("energy_source_code_1", "VARCHAR(10)"),
            ("fuel_type_code_pudl", "VARCHAR(30)"), ("technology_description", "VARCHAR(100)"),
            ("generator_retirement_date", "DATE"),
        ],
    ),
    dict(
        name="monthly_generation_fuel", src="gen_fuel",
        where=f"plant_id_eia IN ({NE_PLANTS}) AND EXTRACT(year FROM report_date) IN ({YEARS})",
        cols=[
            ("plant_id_eia", "INT UNSIGNED"), ("report_date", "DATE"),
            ("energy_source_code", "VARCHAR(10)"), ("prime_mover_code", "VARCHAR(10)"),
            ("fuel_type_code_pudl", "VARCHAR(30)"), ("fuel_type_code_agg", "VARCHAR(10)"),
            ("fuel_consumed_units", "DOUBLE"), ("fuel_consumed_for_electricity_units", "DOUBLE"),
            ("fuel_mmbtu_per_unit", "DOUBLE"), ("fuel_consumed_mmbtu", "DOUBLE"),
            ("fuel_consumed_for_electricity_mmbtu", "DOUBLE"), ("net_generation_mwh", "DOUBLE"),
        ],
    ),
    dict(
        name="epa_eia_crosswalk", src="crosswalk", auto_id=True,  # surrogate PK crosswalk_id
        where=f"plant_id_eia IN ({NE_PLANTS}) AND report_year IN ({YEARS})",
        cols=[
            ("report_year", "SMALLINT UNSIGNED"), ("plant_id_epa", "INT UNSIGNED"),
            ("emissions_unit_id_epa", "VARCHAR(20)"), ("generator_id_epa", "VARCHAR(20)"),
            ("plant_id_eia", "INT UNSIGNED"), ("boiler_id", "VARCHAR(20)"),
            ("generator_id", "VARCHAR(20)"),
        ],
    ),
    dict(
        name="cems_hourly_emissions", src="cems",
        where=f"state IN ({STATES}) AND year IN ({YEARS})",
        cols=[
            ("plant_id_epa", "INT UNSIGNED"), ("emissions_unit_id_epa", "VARCHAR(20)"),
            ("operating_datetime_utc", "DATETIME"), ("plant_id_eia", "INT UNSIGNED"),
            ("state", "CHAR(2)"), ("year", "SMALLINT UNSIGNED"),
            ("operating_time_hours", "DOUBLE"), ("gross_load_mw", "DOUBLE"),
            ("heat_content_mmbtu", "DOUBLE"), ("steam_load_lbs", "DOUBLE"),
            ("so2_mass_lbs", "DOUBLE"), ("so2_mass_measurement_code", "VARCHAR(40)"),
            ("nox_mass_lbs", "DOUBLE"), ("nox_mass_measurement_code", "VARCHAR(40)"),
            ("co2_mass_tons", "DOUBLE"), ("co2_mass_measurement_code", "VARCHAR(40)"),
        ],
    ),
]

# The "generators" table (plant_id_eia, generator_id) is not a Parquet table:
# it is derived from the data already loaded (see build_generators).
GENERATORS_DDL = """
    CREATE TABLE generators (
        plant_id_eia INT UNSIGNED NOT NULL,
        generator_id VARCHAR(20) NOT NULL,
        PRIMARY KEY (plant_id_eia, generator_id)
    ) ENGINE=InnoDB
"""

ALL_TABLE_NAMES = ["plants", "generators", "generator_yearly", "monthly_generation_fuel",
                   "epa_eia_crosswalk", "cems_hourly_emissions"]

# Keys added after the load. The CEMS primary key is last because it is the slowest.
# (CEMS.plant_id_eia -> plants is only a logical join, as in the diagram: no FK.)
KEYS = [
    "ALTER TABLE plants ADD PRIMARY KEY (plant_id_eia)",
    "ALTER TABLE generator_yearly ADD PRIMARY KEY (plant_id_eia, generator_id, report_date)",
    "ALTER TABLE monthly_generation_fuel ADD PRIMARY KEY "
    "(plant_id_eia, report_date, energy_source_code, prime_mover_code)",
    "ALTER TABLE generators ADD CONSTRAINT fk_generators_plants "
    "FOREIGN KEY (plant_id_eia) REFERENCES plants (plant_id_eia)",
    "ALTER TABLE generator_yearly ADD CONSTRAINT fk_genyearly_generators "
    "FOREIGN KEY (plant_id_eia, generator_id) REFERENCES generators (plant_id_eia, generator_id)",
    "ALTER TABLE monthly_generation_fuel ADD CONSTRAINT fk_genfuel_plants "
    "FOREIGN KEY (plant_id_eia) REFERENCES plants (plant_id_eia)",
    "ALTER TABLE epa_eia_crosswalk ADD CONSTRAINT fk_crosswalk_generators "
    "FOREIGN KEY (plant_id_eia, generator_id) REFERENCES generators (plant_id_eia, generator_id)",
    "ALTER TABLE cems_hourly_emissions ADD PRIMARY KEY "
    "(plant_id_epa, emissions_unit_id_epa, operating_datetime_utc)",
]


# ============================================================
# FUNCTIONS
# ============================================================
def check_columns(duck):
    """Stop early if a column of the diagram is missing in the Parquet files."""
    for t in TABLES:
        rows = duck.execute(f"DESCRIBE SELECT * FROM '{SRC[t['src']]}'").fetchall()
        found = {r[0] for r in rows}
        missing = [c for c, _ in t["cols"] if c not in found]
        if missing:
            raise SystemExit(f"{t['name']}: columns not found in the Parquet file: {missing}")


def reset_schema(cur):
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    for name in ALL_TABLE_NAMES:
        cur.execute(f"DROP TABLE IF EXISTS {name}")
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")


def create_table(cur, t):
    cols = [f"{name} {typ}" for name, typ in t["cols"]]
    if t.get("auto_id"):
        cols.insert(0, "crosswalk_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY")
    cur.execute(f"CREATE TABLE {t['name']} ({', '.join(cols)})")


def load_table(duck, db, cur, t):
    """Stream the filtered Parquet data from DuckDB into MySQL, in batches."""
    names = [name for name, _ in t["cols"]]
    # DATETIME columns are cast to a naive UTC timestamp (the session is in UTC)
    select = ", ".join(
        f"CAST({n} AS TIMESTAMP) AS {n}" if typ == "DATETIME" else n for n, typ in t["cols"]
    )
    query = f"SELECT {select} FROM '{SRC[t['src']]}' WHERE {t['where']}"
    insert = (f"INSERT INTO {t['name']} ({', '.join(names)}) "
              f"VALUES ({', '.join(['%s'] * len(names))})")

    total = 0
    result = duck.execute(query)
    while True:
        rows = result.fetchmany(BATCH_SIZE)  # list of tuples, SQL NULL -> None
        if not rows:
            break
        cur.executemany(insert, rows)
        db.commit()
        total += len(rows)
        print(f"\r  {t['name']}: {total:,} rows", end="", flush=True)
    print()
    return total


def build_generators(db, cur):
    """generators = distinct (plant, generator) of the yearly table + those of the crosswalk."""
    cur.execute(GENERATORS_DDL)
    cur.execute("INSERT INTO generators SELECT DISTINCT plant_id_eia, generator_id "
                "FROM generator_yearly")
    cur.execute("INSERT IGNORE INTO generators SELECT DISTINCT plant_id_eia, generator_id "
                "FROM epa_eia_crosswalk "
                "WHERE plant_id_eia IS NOT NULL AND generator_id IS NOT NULL")
    db.commit()


def add_keys(cur):
    for stmt in KEYS:
        start = time.perf_counter()
        print(f"  {stmt[:90]}...")
        cur.execute(stmt)
        print(f"    done in {time.perf_counter() - start:.1f}s")


def validate(cur):
    print("\nRow counts:")
    for name in ALL_TABLE_NAMES:
        cur.execute(f"SELECT COUNT(*) FROM {name}")
        print(f"  {name:<28}{cur.fetchone()[0]:>14,}")
    # Logical join CEMS -> plants (no FK): CEMS ids missing in EIA are expected, just report them
    cur.execute("""
        SELECT COUNT(*) FROM cems_hourly_emissions c
        LEFT JOIN plants p ON p.plant_id_eia = c.plant_id_eia
        WHERE c.plant_id_eia IS NOT NULL AND p.plant_id_eia IS NULL
    """)
    print(f"\nCEMS rows whose plant_id_eia is not in plants: {cur.fetchone()[0]:,}")


def main():
    duck = duckdb.connect()
    duck.execute("INSTALL httpfs")
    duck.execute("LOAD httpfs")
    duck.execute("CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-west-2', URL_STYLE 'path')")
    duck.execute("SET TimeZone = 'UTC'")
    check_columns(duck)

    password = os.getenv("MYSQL_PASSWORD") or getpass.getpass("MySQL password: ")
    db = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, password=password,
                                 autocommit=False)
    cur = db.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")
    cur.execute(f"USE {MYSQL_DB}")

    reset_schema(cur)
    timings = {}

    print("\n1) Loading tables (no keys yet)")
    for t in TABLES:
        start = time.perf_counter()
        create_table(cur, t)
        load_table(duck, db, cur, t)
        timings[t["name"]] = time.perf_counter() - start

    print("\n2) Building generators")
    build_generators(db, cur)

    print("\n3) Adding primary and foreign keys")
    start = time.perf_counter()
    add_keys(cur)
    timings["keys"] = time.perf_counter() - start

    validate(cur)
    print("\nTimings (seconds):")
    for name, secs in timings.items():
        print(f"  {name:<28}{secs:>10.1f}")

    cur.close()
    db.close()


if __name__ == "__main__":
    main()
