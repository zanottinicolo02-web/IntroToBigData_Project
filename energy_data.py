import duckdb
import pandas as pd
import os

# Connect to in-memory DuckDB
con = duckdb.connect()

print("Setting up connection to PUDL public S3 bucket...")
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-west-2', URL_STYLE 'path');")

# S3 Parquet file paths from Catalyst Cooperative
cems_path = "s3://pudl.catalyst.coop/stable/core_epacems__hourly_emissions.parquet"
plants_path = "s3://pudl.catalyst.coop/stable/core_eia__entity_plants.parquet"
gens_path = "s3://pudl.catalyst.coop/stable/core_eia860__scd_generators.parquet"
gen_fuel_path = "s3://pudl.catalyst.coop/stable/core_eia923__monthly_generation_fuel.parquet"

datasets = [
    ("CEMS Hourly Emissions", cems_path),
    ("EIA Plants", plants_path),
    ("EIA-860 Generators (SCD)", gens_path),
    ("EIA-923 Monthly Generation & Fuel", gen_fuel_path)
]

# ==========================================
# 0. TOTAL ROW COUNTS
# ==========================================
print("\n" + "="*50)
print("0. TOTAL ROW COUNTS")
print("="*50)

xwalk_path = "s3://pudl.catalyst.coop/stable/core_epa__assn_eia_epacamd.parquet"

# Full-table counts (instant: they come from Parquet metadata)
for name, path in datasets + [("EPA-EIA Crosswalk", xwalk_path)]:
    count = con.sql(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
    print(f"-> {name} (whole table): {count:,} rows")

# Project scope
ne = ['CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA']
sample_years = [2000, 2005, 2010, 2015, 2018, 2020, 2023, 2025]
ne_sql = ", ".join(f"'{s}'" for s in ne)
years_sql = ", ".join(str(y) for y in sample_years)

csv_path = "cems_counts_state_year.csv"
if not os.path.exists(csv_path):
    print("CSV not found: scanning CEMS on S3 (about 7 minutes)...")
    con.sql("PRAGMA enable_progress_bar;")
    con.sql(f"""
        SELECT state, year, COUNT(*) AS n
        FROM '{cems_path}'
        GROUP BY state, year
    """).df().to_csv(csv_path, index=False)
counts = pd.read_csv(csv_path)
sel = counts[counts["state"].isin(ne) & counts["year"].isin(sample_years)]

print("\n--- CEMS, Northeast, sample years ---")
per_year = sel.groupby("year")["n"].sum()
print(per_year.to_string())
cems_total = int(per_year.sum())
print(f"CEMS total: {cems_total:,}")

print("\n--- CEMS rows by state ---")
print(sel.groupby("state")["n"].sum().sort_values(ascending=False).to_string())

# EIA tables restricted to the same scope
ne_plants = f"SELECT plant_id_eia FROM '{plants_path}' WHERE state IN ({ne_sql})"

plants_n = con.sql(f"SELECT COUNT(*) FROM '{plants_path}' WHERE state IN ({ne_sql})").fetchone()[0]
gens_n = con.sql(f"""
    SELECT COUNT(*) FROM '{gens_path}'
    WHERE plant_id_eia IN ({ne_plants})
      AND EXTRACT(year FROM report_date) IN ({years_sql})
""").fetchone()[0]
genfuel_n = con.sql(f"""
    SELECT COUNT(*) FROM '{gen_fuel_path}'
    WHERE plant_id_eia IN ({ne_plants})
      AND EXTRACT(year FROM report_date) IN ({years_sql})
""").fetchone()[0]

# Crosswalk: detect the real column names first
xwalk_cols = con.sql(f"DESCRIBE SELECT * FROM '{xwalk_path}'").df()["column_name"].tolist()
print("\n--- Crosswalk columns ---")
print(xwalk_cols)

year_col = next((c for c in ("report_year", "year") if c in xwalk_cols), None)
if "plant_id_eia" not in xwalk_cols:
    print("WARNING: plant_id_eia not found in the crosswalk, adjust the filter below")
    xwalk_n = 0
else:
    year_filter = f"AND {year_col} IN ({years_sql})" if year_col else ""
    if year_col:
        print("Crosswalk years available:",
              con.sql(f"SELECT MIN({year_col}), MAX({year_col}) FROM '{xwalk_path}'").fetchone())
    else:
        print("WARNING: no year column found, counting without the year filter")
    xwalk_n = con.sql(f"""
        SELECT COUNT(*) FROM '{xwalk_path}'
        WHERE plant_id_eia IN ({ne_plants}) {year_filter}
    """).fetchone()[0]

print("\n--- EIA tables and crosswalk, Northeast, sample years ---")
print(f"Plants (all years, static): {plants_n:,}")
print(f"Generators SCD: {gens_n:,}")
print(f"Monthly generation & fuel: {genfuel_n:,}")
print(f"Crosswalk: {xwalk_n:,}")
print(f"\nTOTAL TUPLES: {cems_total + plants_n + gens_n + genfuel_n + xwalk_n:,}")

# ==========================================
# 1. CEMS TABLE (Hourly Emissions)
# ==========================================
print("\n" + "="*50)
print("1. core_epacems__hourly_emissions (SCHEMA & SAMPLE)")
print("="*50)
print(con.sql(f"DESCRIBE SELECT * FROM '{cems_path}'").df().to_string())
print("\n--- SAMPLE ROWS ---")
print(con.sql(f"SELECT * FROM '{cems_path}' LIMIT 3").df().to_string())


# ==========================================
# 2. EIA PLANTS TABLE (Entities)
# ==========================================
print("\n" + "="*50)
print("2. core_eia__entity_plants (SCHEMA & SAMPLE)")
print("="*50)
print(con.sql(f"DESCRIBE SELECT * FROM '{plants_path}'").df().to_string())
print("\n--- SAMPLE ROWS ---")
print(con.sql(f"SELECT * FROM '{plants_path}' LIMIT 3").df().to_string())


# ==========================================
# 3. EIA-860 GENERATORS TABLE (Infrastructure)
# ==========================================
print("\n" + "="*50)
print("3. core_eia860__scd_generators (SCHEMA & SAMPLE)")
print("="*50)
print(con.sql(f"DESCRIBE SELECT * FROM '{gens_path}'").df().to_string())
print("\n--- SAMPLE ROWS ---")
print(con.sql(f"SELECT * FROM '{gens_path}' LIMIT 3").df().to_string())


# ==========================================
# 4. EIA-923 MONTHLY GENERATION & FUEL TABLE
# ==========================================
print("\n" + "="*50)
print("4. core_eia923__monthly_generation_fuel (SCHEMA & SAMPLE)")
print("="*50)
print(con.sql(f"DESCRIBE SELECT * FROM '{gen_fuel_path}'").df().to_string())
print("\n--- SAMPLE ROWS ---")
print(con.sql(f"SELECT * FROM '{gen_fuel_path}' LIMIT 3").df().to_string())