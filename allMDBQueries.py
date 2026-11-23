"""
Contains all the queries based on the relational model, converted for MongoDB.
To execute, run the MongoDBQueries.py file

Collections used (see docSchema.js):
  cems_hourly_emissions  time series
  Metadata `unit` = {plant_id_epa, emissions_unit_id_epa, plant_id_eia, state, county_id_fips}.
  units _id = {plant_id_epa, emissions_unit_id_epa}; generators[] = mapped generators
  generators _id = {plant_id_eia, generator_id}; yearly[] = generator_yearly rows
  plants _id = plant_id_eia
"""


# Q1 / Q1_AVG: Measure real environmental efficiency by calculating specific greenhouse gas 
# and pollutant emissions (CO2, SO2, NOx) per MWh of net electricity actually generated. 
# This allows the identification of plants that exhibit hidden operational inefficiencies 
# or abnormal emission rates. Each plant-year is labeled by its dominant fuel and its 
# emissions are divided by total net generation across all fuels instead of only keeping 
# single-fuel plants. 

# db.cems_hourly_emissions.aggregate(Q1, allowDiskUse=True)
# Common part: one row per plant-year with total emissions, net generation
# (all fuels) and dominant fuel.
FUELS = ["coal", "gas", "oil"]

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



# Q2: Heat Rate Degradation vs. Retirement Date
# Objective: Empirically evaluate whether aging power plants, measured by their temporal
# proximity to their actual retirement date (generator_retirement_date), experience
# progressive thermal efficiency degradation (i.e., an increase in heat input required,
# heat_content_mmbtu, per unit of gross electrical output, gross_load_mw). Only generators
# that have already retired have a retirement date, so generators that are still operating
# are excluded.
# Results (MySQL): coal showed no progressive degradation, and the other technologies
# didn't have enough units to support a conclusion.
#
# Differences with the SQL version (the result must be the same):
#  - The hours are collapsed BEFORE the join with the unit mapping, so the big time series
#    is scanned once with plain $match / $group stages (they can use the bucket-level
#    pushdown of time series collections). Only ~one document per unit-year reaches the
#    $lookup stages.
#  - TIMESTAMPDIFF(YEAR, ...) counts WHOLE years. $dateDiff counts calendar boundaries, so
#    the whole-year rule is written by hand. The retirement date is a DATE (midnight), so
#    it is enough to know the calendar day (MMDD) of each hour and whether it is hour 0.
#  - "gen_lifetime" (MIN over the generator's years) and the HAVING COUNT(DISTINCT
#    technology) = 1 filter are computed from generators.yearly[] and units.generators[].

Q2 = [
    # equiv. to WHERE in the SQL query
    {"$match": {
        "heat_content_mmbtu": {"$gt": 0},
        "gross_load_mw": {"$gt": 0},
        "operating_time_hours": {"$gt": 0},
    }},

    {"$group": {
        "_id": {
            "plant_id_epa": "$unit.plant_id_epa",
            "emissions_unit_id_epa": "$unit.emissions_unit_id_epa",
            "year": {"$year": "$operating_datetime_utc"},
            "md": {"$dateToString": {"date": "$operating_datetime_utc", "format": "%m%d"}},
            "h0": {"$eq": [{"$hour": "$operating_datetime_utc"}, 0]},
        },
        "heat_mmbtu": {"$sum": "$heat_content_mmbtu"},
        "mwh": {"$sum": {"$multiply": ["$gross_load_mw", "$operating_time_hours"]}},
        "n_hours": {"$sum": 1},
    }},

    # the c.year = u.report_year in the SQL portion
    {"$group": {
        "_id": {
            "plant_id_epa": "$_id.plant_id_epa",
            "emissions_unit_id_epa": "$_id.emissions_unit_id_epa",
            "year": "$_id.year",
        },
        "days": {"$push": {
            "md": "$_id.md", "h0": "$_id.h0",
            "heat_mmbtu": "$heat_mmbtu", "mwh": "$mwh", "n_hours": "$n_hours",
        }},
    }},

    # unit -> mapped generators -> generators (crosswalk + generator_yearly + gen_lifetime)
    {"$set": {"unit_key": {
        "plant_id_epa": "$_id.plant_id_epa",
        "emissions_unit_id_epa": "$_id.emissions_unit_id_epa",
    }}},
    {"$lookup": {"from": "units", "localField": "unit_key", "foreignField": "_id", "as": "unit"}},
    {"$unwind": "$unit"},
    {"$unwind": "$unit.generators"},
    {"$set": {"gen_key": {
        "plant_id_eia": "$unit.plant_id_eia",
        "generator_id": "$unit.generators.generator_id",
    }}},
    {"$lookup": {"from": "generators", "localField": "gen_key", "foreignField": "_id", "as": "gen"}},
    {"$unwind": "$gen"},
    {"$set": {
        "in_year": {"$in": ["$_id.year", {"$map": {
            "input": "$gen.yearly", "as": "y", "in": {"$year": "$$y.report_date"}}}]},
        "lifetime_technology": {"$min": "$gen.yearly.technology_description"},
        "lifetime_retirement": {"$min": "$gen.yearly.generator_retirement_date"},
    }},
    {"$match": {"in_year": True}},

    # (HAVING COUNT(DISTINCT lifetime_technology) = 1) and retirement date
    {"$group": {
        "_id": "$_id",
        "days": {"$first": "$days"},
        "technologies": {"$addToSet": "$lifetime_technology"},
        "retirement_date": {"$min": "$lifetime_retirement"},
    }},
    {"$set": {"technologies": {"$filter": {
        "input": "$technologies", "cond": {"$ne": ["$$this", None]}}}}},
    {"$match": {"technologies": {"$size": 1}, "retirement_date": {"$ne": None}}},

    {"$unwind": "$days"},
    {"$set": {
        "dt_day": {"$concat": [{"$toString": "$_id.year"}, "$days.md"]},
        "ret_day": {"$dateToString": {"date": "$retirement_date", "format": "%Y%m%d"}},
        "ret_md": {"$dateToString": {"date": "$retirement_date", "format": "%m%d"}},
    }},
    {"$match": {"$expr": {"$or": [
        {"$lt": ["$dt_day", "$ret_day"]},
        {"$and": [{"$eq": ["$dt_day", "$ret_day"]}, "$days.h0"]},
    ]}}},

    # TIMESTAMPDIFF(YEAR, datetime, retirement_date)
    {"$set": {"years_to_retirement": {"$subtract": [
        {"$subtract": [{"$year": "$retirement_date"}, "$_id.year"]},
        {"$cond": [{"$or": [
            {"$lt": ["$ret_md", "$days.md"]},
            {"$and": [{"$eq": ["$ret_md", "$days.md"]}, {"$not": ["$days.h0"]}]},
        ]}, 1, 0]},
    ]}}},

    # GROUP BY technology, years_to_retirement
    {"$group": {
        "_id": {
            "technology": {"$arrayElemAt": ["$technologies", 0]},
            "years_to_retirement": "$years_to_retirement",
        },
        "heat_mmbtu": {"$sum": "$days.heat_mmbtu"},
        "mwh": {"$sum": "$days.mwh"},
        "n_hours": {"$sum": "$days.n_hours"},
        "units": {"$addToSet": {
            "plant_id_epa": "$_id.plant_id_epa",
            "emissions_unit_id_epa": "$_id.emissions_unit_id_epa",
        }},
    }},
    {"$project": {
        "_id": 0,
        "technology": "$_id.technology",
        "years_to_retirement": "$_id.years_to_retirement",
        "heat_rate_mmbtu_per_mwh": {"$divide": ["$heat_mmbtu", "$mwh"]},
        "n_hours": 1,
        "n_units": {"$size": "$units"},
    }},
    # ORDER BY technology, years_to_retirement DESC
    {"$sort": {"technology": 1, "years_to_retirement": -1}},
]


# Q3: Spatial Hotspots
# Objective: Identify critical geographical pollution clusters across the Northeast region
# and test the Pareto Principle on Big Data: verifying whether a small minority of
# facilities or counties (e.g., top 20%) accounts for the vast majority (e.g., 80%) of
# total cumulative CO2, SO2, and NOx emissions.
# Results (MySQL): SO2 (99.2%) and NOx (87.1%) pass the 80/20 test at the facility
# level and CO2 just misses (78.6%). Hotspots are western PA coal counties and the NYC area.

def _ranks():
    """ROW_NUMBER() OVER (ORDER BY total DESC), one independent ranking per pollutant."""
    return [
        {"$setWindowFields": {"sortBy": {"total_co2_tons": -1},
                              "output": {"co2_rank": {"$documentNumber": {}}}}},
        {"$setWindowFields": {"sortBy": {"total_so2_lbs": -1},
                              "output": {"so2_rank": {"$documentNumber": {}}}}},
        {"$setWindowFields": {"sortBy": {"total_nox_lbs": -1},
                              "output": {"nox_rank": {"$documentNumber": {}}}}},
    ]


def _share(top, total):
    """top / total rounded to 4 decimals; None when there is nothing to divide (total = 0)."""
    return {"$cond": [{"$gt": [f"${total}", 0]},
                      {"$round": [{"$divide": [f"${top}", f"${total}"]}, 4]}, None]}


def _pareto_tail(level):
    def top_sum(rank, total):
        return {"$sum": {"$cond": [
            {"$lte": [f"${rank}", {"$multiply": ["$n_units", 0.20]}]}, f"${total}", 0]}}

    return _ranks() + [
        # COUNT(*) OVER ()
        {"$setWindowFields": {"output": {"n_units": {"$count": {}}}}},
        {"$group": {
            "_id": None,
            "n_units": {"$max": "$n_units"},
            "co2_top": top_sum("co2_rank", "total_co2_tons"), "co2_all": {"$sum": "$total_co2_tons"},
            "so2_top": top_sum("so2_rank", "total_so2_lbs"),  "so2_all": {"$sum": "$total_so2_lbs"},
            "nox_top": top_sum("nox_rank", "total_nox_lbs"),  "nox_all": {"$sum": "$total_nox_lbs"},
        }},
        {"$project": {
            "_id": 0,
            "level": {"$literal": level},
            "n_units": 1,
            "co2_top20_share": _share("co2_top", "co2_all"),
            "so2_top20_share": _share("so2_top", "so2_all"),
            "nox_top20_share": _share("nox_top", "nox_all"),
        }},
    ]


# Common part, facility level: emissions per facility (plant_id_epa, state).
Q3_FACILITY_TOTALS = [
    {"$group": {
        "_id": {"plant_id_epa": "$unit.plant_id_epa", "state": "$unit.state"},
        "total_co2_tons": {"$sum": "$co2_mass_tons"},
        "total_so2_lbs": {"$sum": "$so2_mass_lbs"},
        "total_nox_lbs": {"$sum": "$nox_mass_lbs"},
    }},
]

# Common part, county level: plant totals (only plants with plant_id_eia), joined with
# "plants" (_id = plant_id_eia) to get the county, then county totals.
Q3_COUNTY_TOTALS = [
    {"$match": {"unit.plant_id_eia": {"$ne": None}}},
    {"$group": {
        "_id": "$unit.plant_id_eia",
        "total_co2_tons": {"$sum": "$co2_mass_tons"},
        "total_so2_lbs": {"$sum": "$so2_mass_lbs"},
        "total_nox_lbs": {"$sum": "$nox_mass_lbs"},
    }},
    {"$lookup": {"from": "plants", "localField": "_id", "foreignField": "_id", "as": "plant"}},
    {"$unwind": "$plant"},
    {"$group": {
        "_id": {"county_id_fips": "$plant.county_id_fips",
                "county": "$plant.county",
                "state": "$plant.state"},
        "total_co2_tons": {"$sum": "$total_co2_tons"},
        "total_so2_lbs": {"$sum": "$total_so2_lbs"},
        "total_nox_lbs": {"$sum": "$total_nox_lbs"},
    }},
]

# Pareto test, one row per level: share of the emissions of the top 20% of the facilities.
Q3_PARETO_FACILITY = Q3_FACILITY_TOTALS + _pareto_tail("facility")

# Pareto test, county level.
Q3_PARETO_COUNTY = Q3_COUNTY_TOTALS + _pareto_tail("county")

# Top 10 hotspots (counties) per pollutant.
Q3_HOTSPOTS = Q3_COUNTY_TOTALS + _ranks() + [
    # One entry per pollutant (the three UNION ALL branches of the SQL).
    {"$set": {"entries": [
        {"pollutant": "CO2", "hotspot_rank": "$co2_rank",
         "total_emissions": "$total_co2_tons", "units": "tons"},
        {"pollutant": "SO2", "hotspot_rank": "$so2_rank",
         "total_emissions": "$total_so2_lbs", "units": "lbs"},
        {"pollutant": "NOx", "hotspot_rank": "$nox_rank",
         "total_emissions": "$total_nox_lbs", "units": "lbs"},
    ]}},
    {"$unwind": "$entries"},
    {"$match": {"entries.hotspot_rank": {"$lte": 10}}},
    {"$project": {
        "_id": 0,
        "pollutant": "$entries.pollutant",
        "hotspot_rank": "$entries.hotspot_rank",
        "county": "$_id.county",
        "state": "$_id.state",
        "total_emissions": "$entries.total_emissions",
        "units": "$entries.units",
    }},
    # ORDER BY pollutant, hotspot_rank
    {"$sort": {"pollutant": 1, "hotspot_rank": 1}},
]



# Q4: Seasonal Capacity Margins
# Objective Analyze the gap between the summer and winter rated capacities (summer_capacity_mw 
# vs. winter_capacity_mw) of operating thermal generators, broken down by state, 
# technology and year. Only generators with operational status "OP", fuel type coal, gas 
# or oil, and non-null summer and winter capacity are considered. For each group the query 
# reports the number of generators, the total nominal, summer and winter capacity, and the 
# summer derating, defined as (winter − summer) / winter × 100. Because thermal efficiency 
# drops under high ambient summer temperatures, the derating highlights which technologies 
# and states lose the most capacity in summer, which can be read as a potential reliability 
# risk during peak summer demand. Note that these are declared capacity ratings (EIA-860), 
# not actual output, and that the derating can be negative when the summer rating exceeds 
# the winter one.

# Array manipulation: $filter keeps only the yearly sub-documents that satisfy the
# conditions BEFORE $unwind, so fewer documents go through the $group.


#   db.generators.aggregate(Q4)

Q4 = [
    # 1. Pre-filter: generators with at least one operational year (OP).
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

# Q5: Load Tiers & Environmental Penalties of Sub-Optimal Operation
# Objective: Evaluate the real operating regimes of the emission units by classifying each operating 
# hour according to the unit's gross load relative to the maximum load observed for that unit in that 
# year (gross_load_mw / max_load_mw): peaking (< 20%), mid-merit (20–70%) and baseload (≥ 70%). The 
# analysis is done on EPA emission units (not EIA generators) and uses only hours with positive load 
# and positive operating time. For each state, year and load tier, the query computes the number of 
# operating hours and the NOx and CO2 emitted per MWh (gross load × operating hours; only hours where 
# the pollutant was measured count in both numerator and denominator). The goal is to check whether 
# low-load operation is associated with higher specific emission rates compared to operation near 
# full capacity. This is an association, not proof of causality, since peaking units are often a 
# different technology from baseload ones. The query excludes hours with zero load or zero operating time. The MWh are 
# gross generation, so the intensities are not directly comparable with the net-generation intensities 
# of Q1.

Q5 = [
    # 1. Only hours with load > 0. Does not change the per-unit maximum (discards rows that
    # would be discarded anyway), but reduces the documents entering the window stage.
    {"$match": {"gross_load_mw": {"$gt": 0}}},

    # 2. Maximum load of each unit in each year (no "window" = entire partition).
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
