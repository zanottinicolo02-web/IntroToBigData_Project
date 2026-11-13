// schema.js: MongoDB schema for PUDL + EPA CEMS (Northeast only)
//
// Run with:   mongosh "mongodb://localhost:27017" schema.js
// Re-create:  mongosh "mongodb://localhost:27017" --eval "var DROP_EXISTING=true" schema.js
//
// Layer 1 (core, query-agnostic):
//   plants, generators, units, monthly_generation_fuel, cems_hourly_emissions
// Layer 2 (derived, optional, disposable; rebuilt with $merge):
//   unit_monthly, plant_year_emissions
//
// Notes
//  - bsonType "number" used for numeric fields (for int, long, double, decimal),
//    so pymongo treats int32/int64/double similar
//  - id fields that are INT UNSIGNED in MySQL are ["int", "long"]
//  - MongoDB does not enforce foreign keys!!!
//  - Requires MongoDB 5.0+ (for time series)

const DB_NAME = "pudl_cems";
const DROP = (typeof DROP_EXISTING !== "undefined") && DROP_EXISTING === true;

db = db.getSiblingDB(DB_NAME);

// Reusable types
const INT_ID      = { bsonType: ["int", "long"] };
const INT_ID_NULL = { bsonType: ["int", "long", "null"] };
const NUM         = { bsonType: "number" };
const NUM_NULL    = { bsonType: ["number", "null"] };
const STR         = { bsonType: "string" };
const STR_NULL    = { bsonType: ["string", "null"] };
const DATE        = { bsonType: "date" };
const DATE_NULL   = { bsonType: ["date", "null"] };
const STATE       = { bsonType: "string", minLength: 2, maxLength: 2 };
const FIPS        = { bsonType: "string", pattern: "^[0-9]{5}$" };

// GeoJSON Point: coordinates are [longitude, latitude]
const GEO_POINT = {
  bsonType: "object",
  required: ["type", "coordinates"],
  properties: {
    type: { enum: ["Point"] },
    coordinates: { bsonType: "array", minItems: 2, maxItems: 2, items: NUM }
  }
};

// Create collection/update validator if exists
function ensureCollection(name, jsonSchema, extraOptions) {
  const options = Object.assign({
    validator: { $jsonSchema: jsonSchema },
    validationLevel: "strict",
    validationAction: "error"
  }, extraOptions || {});

  let exists = db.getCollectionNames().includes(name);
  if (exists && DROP) {
    db.getCollection(name).drop();
    exists = false;
  }
  if (!exists) {
    db.createCollection(name, options);
    print("created   " + name);
  } else {
    db.runCommand({
      collMod: name,
      validator: options.validator,
      validationLevel: options.validationLevel,
      validationAction: options.validationAction
    });
    print("updated   " + name + " (validator)");
  }
}

// LAYER 1: MAIN MODEL

// plants: dimension, one doc per EIA plant
ensureCollection("plants", {
  bsonType: "object",
  required: ["_id", "plant_name_eia", "state"],
  properties: {
    _id: INT_ID,
    plant_name_eia: STR,
    city: STR_NULL,
    state: STATE,
    county: STR_NULL,
    county_id_fips: { bsonType: ["string", "null"], pattern: "^[0-9]{5}$" },
    timezone: STR_NULL,
    location: GEO_POINT                  // from latitude / longitude
  }
});

// generators: generators + generator_yearly (embedded)
ensureCollection("generators", {
  bsonType: "object",
  required: ["_id", "plant_id_eia", "generator_id", "state", "yearly"],
  properties: {
    _id: {
      bsonType: "object",
      required: ["plant_id_eia", "generator_id"],
      properties: { plant_id_eia: INT_ID, generator_id: STR }
    },
    plant_id_eia: INT_ID,
    generator_id: STR,
    state: STATE,
    county_id_fips: { bsonType: ["string", "null"], pattern: "^[0-9]{5}$" },
    yearly: {
      bsonType: "array",
      items: {
        bsonType: "object",
        required: ["report_date"],
        properties: {
          report_date: DATE,
          utility_id_eia: INT_ID_NULL,
          operational_status_code: STR_NULL,
          capacity_mw: NUM_NULL,
          summer_capacity_mw: NUM_NULL,
          winter_capacity_mw: NUM_NULL,
          prime_mover_code: STR_NULL,
          energy_source_code_1: STR_NULL,
          fuel_type_code_pudl: STR_NULL,
          technology_description: STR_NULL,
          generator_retirement_date: DATE_NULL
        }
      }
    }
  }
});

// units: EPA emissions unit dimension and the EPA -> EIA bridge.
ensureCollection("units", {
  bsonType: "object",
  required: ["_id", "plant_id_epa", "emissions_unit_id_epa"],
  properties: {
    _id: {
      bsonType: "object",
      required: ["plant_id_epa", "emissions_unit_id_epa"],
      properties: { plant_id_epa: INT_ID, emissions_unit_id_epa: STR }
    },
    plant_id_epa: INT_ID,
    emissions_unit_id_epa: STR,
    plant_id_eia: INT_ID_NULL,
    state: { bsonType: ["string", "null"], minLength: 2, maxLength: 2 },
    county_id_fips: { bsonType: ["string", "null"], pattern: "^[0-9]{5}$" },
    location: { oneOf: [GEO_POINT, { bsonType: "null" }] },
    generators: {
      bsonType: "array",
      items: {
        bsonType: "object",
        required: ["generator_id"],
        properties: {
          generator_id: STR,
          generator_id_epa: STR_NULL,
          boiler_id: STR_NULL,
          crosswalk_years: { bsonType: "array", items: { bsonType: ["int", "long"] } },
          technology_description: STR_NULL,
          fuel_type_code_pudl: STR_NULL,
          prime_mover_code: STR_NULL,
          retirement_date: DATE_NULL,
          // { "2022": 153.1, "2023": 153.1 }
          capacity_by_year: { bsonType: "object" }
        }
      }
    }
  }
});

// monthly_generation_fuel: one document per plant per month.
ensureCollection("monthly_generation_fuel", {
  bsonType: "object",
  required: ["_id", "plant_id_eia", "report_date", "fuels"],
  properties: {
    _id: {
      bsonType: "object",
      required: ["plant_id_eia", "report_date"],
      properties: { plant_id_eia: INT_ID, report_date: DATE }
    },
    plant_id_eia: INT_ID,
    report_date: DATE,
    state: { bsonType: ["string", "null"], minLength: 2, maxLength: 2 },
    fuels: {
      bsonType: "array",
      items: {
        bsonType: "object",
        required: ["energy_source_code", "prime_mover_code"],
        properties: {
          energy_source_code: STR,
          prime_mover_code: STR,
          fuel_type_code_pudl: STR_NULL,
          fuel_type_code_agg: STR_NULL,
          fuel_consumed_units: NUM_NULL,
          fuel_consumed_for_electricity_units: NUM_NULL,
          fuel_mmbtu_per_unit: NUM_NULL,
          fuel_consumed_mmbtu: NUM_NULL,
          fuel_consumed_for_electricity_mmbtu: NUM_NULL,
          net_generation_mwh: NUM_NULL
        }
      }
    }
  }
});

// cems_hourly_emissions: TIME SERIES collection.
const cemsSchema = {
  bsonType: "object",
  required: ["operating_datetime_utc", "unit"],
  properties: {
    operating_datetime_utc: DATE,
    unit: {
      bsonType: "object",
      required: ["plant_id_epa", "emissions_unit_id_epa"],
      properties: {
        plant_id_epa: INT_ID,
        emissions_unit_id_epa: STR,
        plant_id_eia: INT_ID_NULL,       // null/absent if no match at load time
        state: { bsonType: ["string", "null"], minLength: 2, maxLength: 2 },
        county_id_fips: { bsonType: ["string", "null"], pattern: "^[0-9]{5}$" }
      }
    },
    operating_time_hours: NUM_NULL,
    gross_load_mw: NUM_NULL,
    heat_content_mmbtu: NUM_NULL,
    steam_load_lbs: NUM_NULL,
    so2_mass_lbs: NUM_NULL,
    so2_mass_measurement_code: STR_NULL,
    nox_mass_lbs: NUM_NULL,
    nox_mass_measurement_code: STR_NULL,
    co2_mass_tons: NUM_NULL,
    co2_mass_measurement_code: STR_NULL
  }
};

(function createCems() {
  const name = "cems_hourly_emissions";
  let exists = db.getCollectionNames().includes(name);
  if (exists && DROP) {
    db.getCollection(name).drop();
    exists = false;
  }
  if (exists) {
    print("exists    " + name + " (time series options cannot be changed; use DROP_EXISTING=true)");
    return;
  }
  const tsOptions = {
    timeseries: {
      timeField: "operating_datetime_utc",
      metaField: "unit",
      granularity: "hours"
    }
  };
  try {
    // time series validation depends on version
    db.createCollection(name, Object.assign({
      validator: { $jsonSchema: cemsSchema },
      validationLevel: "strict",
      validationAction: "error"
    }, tsOptions));
    print("created   " + name + " (time series + validator)");
  } catch (e) {
    print("note      validator not accepted on time series for this server version: " + e.message);
    db.createCollection(name, tsOptions);
    print("created   " + name + " (time series, NO validator: validate in the loader)");
  }
})();

// LAYER 2: DERIVED COLLECTIONS (optional)
// Build with $merge from the core collections, treated like indexes

// unit_monthly: per-unit, per-month aggregates of cems_hourly_emissions
ensureCollection("unit_monthly", {
  bsonType: "object",
  required: ["_id", "plant_id_epa", "emissions_unit_id_epa", "month"],
  properties: {
    _id: {
      bsonType: "object",
      required: ["plant_id_epa", "emissions_unit_id_epa", "month"],
      properties: { plant_id_epa: INT_ID, emissions_unit_id_epa: STR, month: DATE }
    },
    plant_id_epa: INT_ID,
    emissions_unit_id_epa: STR,
    month: DATE,
    plant_id_eia: INT_ID_NULL,
    state: { bsonType: ["string", "null"] },
    county_id_fips: { bsonType: ["string", "null"] },
    hours_operated: NUM_NULL,
    gross_mwh: NUM_NULL,
    heat_input_mmbtu: NUM_NULL,
    heat_rate: NUM_NULL,
    co2_tons: NUM_NULL,
    so2_lbs: NUM_NULL,
    nox_lbs: NUM_NULL,
    months_to_retirement: NUM_NULL
  }
});

// plant_year_emissions: per-plant, per-year emission totals with geography
ensureCollection("plant_year_emissions", {
  bsonType: "object",
  required: ["_id", "plant_id_eia", "year"],
  properties: {
    _id: {
      bsonType: "object",
      required: ["plant_id_eia", "year"],
      properties: { plant_id_eia: INT_ID, year: { bsonType: ["int", "long"] } }
    },
    plant_id_eia: INT_ID,
    year: { bsonType: ["int", "long"] },
    state: { bsonType: ["string", "null"] },
    county_id_fips: { bsonType: ["string", "null"] },
    location: { oneOf: [GEO_POINT, { bsonType: "null" }] },
    co2_tons: NUM_NULL,
    so2_lbs: NUM_NULL,
    nox_lbs: NUM_NULL
  }
});

// INDEXES - used for comparison
// (_id is always indexed. Compound _id lookups are covered by it.)
/* 
db.plants.createIndex({ location: "2dsphere" });
db.plants.createIndex({ state: 1, county_id_fips: 1 });

db.generators.createIndex({ plant_id_eia: 1, generator_id: 1 });  // top-level fields; _id (object) does not serve field-level lookups
db.generators.createIndex({ state: 1 });
db.generators.createIndex({ "yearly.report_date": 1, "yearly.technology_description": 1 });

db.units.createIndex({ plant_id_epa: 1, emissions_unit_id_epa: 1 });  // CEMS unit -> units lookup
db.units.createIndex({ plant_id_eia: 1 });
db.units.createIndex({ state: 1, county_id_fips: 1 });
db.units.createIndex({ location: "2dsphere" });
db.units.createIndex({ "generators.generator_id": 1 });

db.monthly_generation_fuel.createIndex({ plant_id_eia: 1, report_date: 1 });
db.monthly_generation_fuel.createIndex({ state: 1, report_date: 1 });
db.monthly_generation_fuel.createIndex({ report_date: 1 });

db.cems_hourly_emissions.createIndex({ "unit.plant_id_eia": 1, operating_datetime_utc: 1 });
db.cems_hourly_emissions.createIndex({ "unit.state": 1, operating_datetime_utc: 1 });

db.unit_monthly.createIndex({ plant_id_eia: 1, month: 1 });
db.unit_monthly.createIndex({ state: 1, month: 1 });
db.plant_year_emissions.createIndex({ state: 1, county_id_fips: 1, year: 1 });
db.plant_year_emissions.createIndex({ location: "2dsphere" }); */

print("\nSchema ready in database '" + DB_NAME + "'.");
printjson(db.getCollectionNames());