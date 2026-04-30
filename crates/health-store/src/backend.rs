use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, TimeZone, Utc};
use csv::Writer;
use health_core::ProgressReporter;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{
    params, params_from_iter, Connection, OptionalExtension, Row, Statement, Transaction,
};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::NamedTempFile;

const INGEST_PROGRESS_INTERVAL: usize = 10_000;
const PREPROCESS_PROGRESS_INTERVAL: usize = 100_000;
const MAX_IN_CLAUSE_VARIABLES: usize = 900;
const DISTANCE_STATISTIC_TYPE_PATTERN: &str = "HKQuantityTypeIdentifierDistance%";
const ACTIVE_ENERGY_STATISTIC_TYPE: &str = "HKQuantityTypeIdentifierActiveEnergyBurned";
const HEART_RATE_STATISTIC_TYPE: &str = "HKQuantityTypeIdentifierHeartRate";
const STEP_COUNT_STATISTIC_TYPE: &str = "HKQuantityTypeIdentifierStepCount";
const RUNNING_ACTIVITY_TYPE: &str = "HKWorkoutActivityTypeRunning";
const ELEVATION_ASCENDED_METADATA_KEY: &str = "HKElevationAscended";
const WEATHER_TEMPERATURE_METADATA_KEY: &str = "HKWeatherTemperature";
const WORKOUT_EFFORT_LEVELS: [&str; 5] = ["easy", "easy-moderate", "moderate", "hard", "very hard"];
const CSV_EXPORT_PROFILES: [&str; 2] = ["full", "llm"];
const LLM_WORKOUT_CSV_FIELDS: [&str; 17] = [
    "db_id",
    "date",
    "activity_type",
    "type",
    "location",
    "duration_minutes",
    "distance_miles",
    "elevation_gain_ft",
    "temperature_f",
    "pace_min_per_mile",
    "avg_heart_rate",
    "max_heart_rate",
    "heart_rate_sample_count",
    "avg_running_cadence_spm",
    "max_running_cadence_spm",
    "energy_kcal",
    "effort",
];
const DATA_TABLES: [&str; 11] = [
    "effective_workouts",
    "workout_records",
    "record_metadata",
    "records",
    "workout_route_metadata",
    "workout_routes",
    "workout_statistics",
    "workout_events",
    "workout_metadata",
    "workouts",
    "dataset_info",
];

const DISTANCE_TO_METERS: &[(&str, f64)] = &[
    ("cm", 0.01),
    ("m", 1.0),
    ("km", 1000.0),
    ("mi", 1609.344),
    ("yd", 0.9144),
    ("ft", 0.3048),
];

const ENERGY_TO_KILOCALORIES: &[(&str, f64)] = &[
    ("kcal", 1.0),
    ("cal", 1.0),
    ("kj", 0.239005736),
    ("j", 1.0 / 4184.0),
];

const DURATION_TO_SECONDS: &[(&str, f64)] = &[
    ("s", 1.0),
    ("sec", 1.0),
    ("second", 1.0),
    ("seconds", 1.0),
    ("min", 60.0),
    ("minute", 60.0),
    ("minutes", 60.0),
    ("h", 3600.0),
    ("hr", 3600.0),
    ("hour", 3600.0),
    ("hours", 3600.0),
];

#[derive(Clone, Copy)]
struct HealthMetricDefinition {
    key: &'static str,
    label: &'static str,
    record_type: &'static str,
    category: &'static str,
    summary_kind: &'static str,
    snapshot: bool,
    digits: u32,
}

const HEALTH_OVERVIEW_METRICS: &[HealthMetricDefinition] = &[
    HealthMetricDefinition {
        key: "resting_heart_rate",
        label: "Resting heart rate",
        record_type: "HKQuantityTypeIdentifierRestingHeartRate",
        category: "Cardio",
        summary_kind: "latest",
        snapshot: true,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "heart_rate_variability_sdnn",
        label: "HRV",
        record_type: "HKQuantityTypeIdentifierHeartRateVariabilitySDNN",
        category: "Recovery",
        summary_kind: "latest",
        snapshot: true,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "heart_rate_recovery_one_minute",
        label: "Cardio recovery",
        record_type: "HKQuantityTypeIdentifierHeartRateRecoveryOneMinute",
        category: "Recovery",
        summary_kind: "latest",
        snapshot: true,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "walking_heart_rate_average",
        label: "Walking heart rate",
        record_type: "HKQuantityTypeIdentifierWalkingHeartRateAverage",
        category: "Cardio",
        summary_kind: "latest",
        snapshot: true,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "vo2_max",
        label: "VO2 max",
        record_type: "HKQuantityTypeIdentifierVO2Max",
        category: "Cardio",
        summary_kind: "latest",
        snapshot: true,
        digits: 1,
    },
    HealthMetricDefinition {
        key: "oxygen_saturation",
        label: "Oxygen saturation",
        record_type: "HKQuantityTypeIdentifierOxygenSaturation",
        category: "Vitals",
        summary_kind: "latest",
        snapshot: true,
        digits: 1,
    },
    HealthMetricDefinition {
        key: "respiratory_rate",
        label: "Respiratory rate",
        record_type: "HKQuantityTypeIdentifierRespiratoryRate",
        category: "Vitals",
        summary_kind: "latest",
        snapshot: false,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "sleeping_wrist_temperature",
        label: "Wrist temperature",
        record_type: "HKQuantityTypeIdentifierAppleSleepingWristTemperature",
        category: "Vitals",
        summary_kind: "latest",
        snapshot: false,
        digits: 1,
    },
    HealthMetricDefinition {
        key: "step_count",
        label: "Steps",
        record_type: "HKQuantityTypeIdentifierStepCount",
        category: "Activity",
        summary_kind: "total",
        snapshot: false,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "active_energy_burned",
        label: "Active energy",
        record_type: "HKQuantityTypeIdentifierActiveEnergyBurned",
        category: "Activity",
        summary_kind: "total",
        snapshot: false,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "basal_energy_burned",
        label: "Basal energy",
        record_type: "HKQuantityTypeIdentifierBasalEnergyBurned",
        category: "Activity",
        summary_kind: "total",
        snapshot: false,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "exercise_time",
        label: "Exercise time",
        record_type: "HKQuantityTypeIdentifierAppleExerciseTime",
        category: "Activity",
        summary_kind: "total",
        snapshot: false,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "stand_time",
        label: "Stand time",
        record_type: "HKQuantityTypeIdentifierAppleStandTime",
        category: "Activity",
        summary_kind: "total",
        snapshot: false,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "time_in_daylight",
        label: "Time in daylight",
        record_type: "HKQuantityTypeIdentifierTimeInDaylight",
        category: "Activity",
        summary_kind: "total",
        snapshot: false,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "flights_climbed",
        label: "Flights climbed",
        record_type: "HKQuantityTypeIdentifierFlightsClimbed",
        category: "Activity",
        summary_kind: "total",
        snapshot: false,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "distance_walking_running",
        label: "Walking + running distance",
        record_type: "HKQuantityTypeIdentifierDistanceWalkingRunning",
        category: "Activity",
        summary_kind: "total",
        snapshot: false,
        digits: 1,
    },
    HealthMetricDefinition {
        key: "walking_speed",
        label: "Walking speed",
        record_type: "HKQuantityTypeIdentifierWalkingSpeed",
        category: "Mobility",
        summary_kind: "latest",
        snapshot: false,
        digits: 1,
    },
    HealthMetricDefinition {
        key: "walking_step_length",
        label: "Step length",
        record_type: "HKQuantityTypeIdentifierWalkingStepLength",
        category: "Mobility",
        summary_kind: "latest",
        snapshot: false,
        digits: 1,
    },
    HealthMetricDefinition {
        key: "walking_double_support_percentage",
        label: "Double support",
        record_type: "HKQuantityTypeIdentifierWalkingDoubleSupportPercentage",
        category: "Mobility",
        summary_kind: "latest",
        snapshot: false,
        digits: 1,
    },
    HealthMetricDefinition {
        key: "walking_asymmetry_percentage",
        label: "Walking asymmetry",
        record_type: "HKQuantityTypeIdentifierWalkingAsymmetryPercentage",
        category: "Mobility",
        summary_kind: "latest",
        snapshot: false,
        digits: 1,
    },
    HealthMetricDefinition {
        key: "apple_walking_steadiness",
        label: "Walking steadiness",
        record_type: "HKQuantityTypeIdentifierAppleWalkingSteadiness",
        category: "Mobility",
        summary_kind: "latest",
        snapshot: false,
        digits: 1,
    },
    HealthMetricDefinition {
        key: "six_minute_walk_test_distance",
        label: "6-minute walk distance",
        record_type: "HKQuantityTypeIdentifierSixMinuteWalkTestDistance",
        category: "Mobility",
        summary_kind: "latest",
        snapshot: false,
        digits: 0,
    },
    HealthMetricDefinition {
        key: "body_mass",
        label: "Body mass",
        record_type: "HKQuantityTypeIdentifierBodyMass",
        category: "Body",
        summary_kind: "latest",
        snapshot: false,
        digits: 1,
    },
    HealthMetricDefinition {
        key: "height",
        label: "Height",
        record_type: "HKQuantityTypeIdentifierHeight",
        category: "Body",
        summary_kind: "latest",
        snapshot: false,
        digits: 2,
    },
];

#[derive(Clone)]
pub struct WorkoutQueryOptions {
    pub start: Option<String>,
    pub end: Option<String>,
    pub activity_types: Vec<String>,
    pub source_query: Option<String>,
    pub min_duration_minutes: Option<f64>,
    pub max_duration_minutes: Option<f64>,
    pub location: Option<String>,
    pub min_distance_miles: Option<f64>,
    pub max_distance_miles: Option<f64>,
    pub min_energy_kcal: Option<f64>,
    pub max_energy_kcal: Option<f64>,
    pub min_avg_heart_rate: Option<f64>,
    pub max_avg_heart_rate: Option<f64>,
    pub min_max_heart_rate: Option<f64>,
    pub max_max_heart_rate: Option<f64>,
    pub efforts: Vec<String>,
    pub requires_route_data: bool,
    pub requires_heart_rate_samples: bool,
}

#[derive(Clone)]
pub struct HealthQueryOptions {
    pub start: Option<String>,
    pub end: Option<String>,
    pub categories: Vec<String>,
    pub metric_query: Option<String>,
    pub source_query: Option<String>,
    pub only_with_samples: bool,
}

#[derive(Clone)]
pub struct ExportOptions {
    pub workout: WorkoutQueryOptions,
    pub output_path: PathBuf,
    pub export_format: String,
    pub summary: bool,
    pub csv_profile: String,
}

pub struct IngestReport {
    pub counts: Map<String, Value>,
    pub metrics: Map<String, Value>,
}

#[derive(Debug, Clone)]
struct MetadataEntry {
    key: String,
    value: Option<String>,
}

#[derive(Debug, Clone)]
struct WorkoutEventData {
    event_type: String,
    event_date: Option<String>,
    duration_seconds: Option<f64>,
    duration_unit: Option<String>,
    raw_attributes: String,
}

#[derive(Debug, Clone)]
struct WorkoutStatisticData {
    statistic_type: String,
    start_date: Option<String>,
    end_date: Option<String>,
    unit: Option<String>,
    aggregations: Vec<(String, f64)>,
    raw_attributes: String,
}

#[derive(Debug, Clone)]
struct WorkoutRouteData {
    route_type: Option<String>,
    source_name: Option<String>,
    source_version: Option<String>,
    device: Option<String>,
    creation_date: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    metadata: Vec<MetadataEntry>,
    raw_attributes: String,
}

#[derive(Debug, Clone)]
struct ParsedRecord {
    uuid: Option<String>,
    record_type: String,
    source_name: Option<String>,
    source_version: Option<String>,
    unit: Option<String>,
    value_text: Option<String>,
    value_numeric: Option<f64>,
    device: Option<String>,
    creation_date: Option<String>,
    start_date: String,
    end_date: String,
    metadata: Vec<MetadataEntry>,
    raw_attributes: String,
}

#[derive(Debug, Clone)]
struct ParsedWorkout {
    uuid: Option<String>,
    activity_type: String,
    source_name: Option<String>,
    source_version: Option<String>,
    device: Option<String>,
    creation_date: Option<String>,
    start_date: String,
    end_date: String,
    duration_seconds: Option<f64>,
    total_distance: Option<f64>,
    total_distance_unit: Option<String>,
    total_distance_meters: Option<f64>,
    total_energy_burned: Option<f64>,
    total_energy_burned_unit: Option<String>,
    total_energy_burned_kilocalories: Option<f64>,
    metadata: Vec<MetadataEntry>,
    events: Vec<WorkoutEventData>,
    statistics: Vec<WorkoutStatisticData>,
    routes: Vec<WorkoutRouteData>,
    raw_attributes: String,
}

enum ParsedItem {
    Record(ParsedRecord),
    Workout(ParsedWorkout),
}

pub fn connect_database(path: &Path) -> Result<Connection, String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let connection = open_connection(path)?;
    connection
        .execute_batch(&schema_sql())
        .map_err(|error| error.to_string())?;
    Ok(connection)
}

pub fn open_existing_database(path: &Path) -> Result<Connection, String> {
    if !path.is_file() {
        return Err(format!("SQLite database not found: {}", path.display()));
    }
    open_connection(path)
}

pub fn configure_connection_for_ingest(connection: &Connection) -> Result<(), String> {
    connection
        .execute_batch(
            "
            PRAGMA journal_mode = MEMORY;
            PRAGMA synchronous = OFF;
            PRAGMA temp_store = MEMORY;
            PRAGMA cache_size = -20000;
            ",
        )
        .map_err(|error| error.to_string())
}

pub fn ingest_export_xml(
    connection: &mut Connection,
    xml_path: &Path,
    source_path: &Path,
    reporter: &mut dyn ProgressReporter,
) -> Result<IngestReport, String> {
    let mut counts = BTreeMap::from([
        ("workouts".to_string(), 0_i64),
        ("records".to_string(), 0_i64),
        ("workout_events".to_string(), 0_i64),
        ("workout_statistics".to_string(), 0_i64),
        ("workout_routes".to_string(), 0_i64),
        ("workout_route_metadata".to_string(), 0_i64),
        ("workout_metadata".to_string(), 0_i64),
        ("record_metadata".to_string(), 0_i64),
        ("workout_record_links".to_string(), 0_i64),
    ]);
    reporter.progress("Rebuilding SQLite dataset...")?;
    let sqlite_started_at = Instant::now();
    let schema_started_at = Instant::now();
    recreate_ingest_schema(connection, reporter)?;
    let schema_duration_seconds = round_to_places(schema_started_at.elapsed().as_secs_f64(), 3);
    reporter.progress("Importing workouts and records into SQLite...")?;

    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    let mut statements = IngestStatements::new(&transaction)?;
    let mut item_count = 0usize;
    let mut parser = HealthExportParser::new(xml_path)?;
    let import_started_at = Instant::now();
    while let Some(item) = parser.next_item()? {
        if reporter.is_cancelled() {
            return Err("Ingest cancelled.".into());
        }
        item_count += 1;
        match item {
            ParsedItem::Workout(workout) => {
                *counts.get_mut("workouts").unwrap() += 1;
                *counts.get_mut("workout_events").unwrap() +=
                    insert_workout(&transaction, &mut statements, &workout)? as i64;
                *counts.get_mut("workout_metadata").unwrap() += workout.metadata.len() as i64;
                *counts.get_mut("workout_statistics").unwrap() += workout
                    .statistics
                    .iter()
                    .map(|stat| stat.aggregations.len() as i64)
                    .sum::<i64>();
                *counts.get_mut("workout_routes").unwrap() += workout.routes.len() as i64;
                *counts.get_mut("workout_route_metadata").unwrap() += workout
                    .routes
                    .iter()
                    .map(|route| route.metadata.len() as i64)
                    .sum::<i64>();
            }
            ParsedItem::Record(record) => {
                *counts.get_mut("records").unwrap() += 1;
                *counts.get_mut("record_metadata").unwrap() +=
                    insert_record(&transaction, &mut statements, &record)? as i64;
            }
        }
        if item_count % INGEST_PROGRESS_INTERVAL == 0 {
            reporter.progress(&format!(
                "Imported {} items ({} records, {} workouts)...",
                format_count(item_count),
                counts["records"],
                counts["workouts"]
            ))?;
        }
    }
    let import_duration_seconds = round_to_places(import_started_at.elapsed().as_secs_f64(), 3);
    let index_started_at = Instant::now();
    create_post_import_indexes(&transaction, reporter)?;
    let post_import_index_duration_seconds =
        round_to_places(index_started_at.elapsed().as_secs_f64(), 3);

    reporter.verbose(&format!(
        "Imported {} items total ({} records, {} workouts)",
        format_count(item_count),
        counts["records"],
        counts["workouts"]
    ))?;
    reporter.progress("Linking workouts to overlapping records...")?;
    let workout_record_link_started_at = Instant::now();
    transaction
        .execute(
            "
            INSERT INTO workout_records (workout_id, record_id)
            SELECT workouts.id, records.id
            FROM workouts
            JOIN records
              ON records.end_date >= workouts.start_date
             AND records.start_date <= workouts.end_date
            ",
            [],
        )
        .map_err(|error| error.to_string())?;
    let workout_record_links: i64 = transaction
        .query_row("SELECT COUNT(*) AS count FROM workout_records", [], |row| {
            row.get(0)
        })
        .map_err(|error| error.to_string())?;
    *counts.get_mut("workout_record_links").unwrap() = workout_record_links;
    let workout_record_link_duration_seconds =
        round_to_places(workout_record_link_started_at.elapsed().as_secs_f64(), 3);
    let cache_started_at = Instant::now();
    refresh_effective_workouts(&transaction, reporter)?;
    create_post_link_indexes(&transaction, reporter)?;
    let effective_workout_cache_duration_seconds =
        round_to_places(cache_started_at.elapsed().as_secs_f64(), 3);
    upsert_dataset_info(
        &transaction,
        &[
            ("source_xml_path", path_to_string(source_path)),
            ("ingested_at", utc_now_timestamp()),
            ("workout_count", counts["workouts"].to_string()),
            ("record_count", counts["records"].to_string()),
            (
                "workout_record_link_count",
                counts["workout_record_links"].to_string(),
            ),
        ],
    )?;
    drop(statements);
    let commit_started_at = Instant::now();
    transaction.commit().map_err(|error| error.to_string())?;
    let commit_duration_seconds = round_to_places(commit_started_at.elapsed().as_secs_f64(), 3);
    reporter.verbose(&format!(
        "SQLite ingest completed in {} with {} workouts, {} records, and {} workout-record links",
        format_elapsed(sqlite_started_at.elapsed().as_secs_f64()),
        counts["workouts"],
        counts["records"],
        counts["workout_record_links"]
    ))?;
    reporter.progress("Finished rebuilding SQLite dataset.")?;

    let mut metrics = Map::new();
    metrics.insert(
        "schema_reset_duration_seconds".into(),
        json!(schema_duration_seconds),
    );
    metrics.insert(
        "import_duration_seconds".into(),
        json!(import_duration_seconds),
    );
    metrics.insert(
        "post_import_index_duration_seconds".into(),
        json!(post_import_index_duration_seconds),
    );
    metrics.insert(
        "workout_record_link_duration_seconds".into(),
        json!(workout_record_link_duration_seconds),
    );
    metrics.insert(
        "effective_workout_cache_duration_seconds".into(),
        json!(effective_workout_cache_duration_seconds),
    );
    metrics.insert(
        "transaction_commit_duration_seconds".into(),
        json!(commit_duration_seconds),
    );
    metrics.insert(
        "sqlite_ingest_duration_seconds".into(),
        json!(round_to_places(
            sqlite_started_at.elapsed().as_secs_f64(),
            3
        )),
    );
    metrics.insert("imported_item_count".into(), json!(item_count));

    Ok(IngestReport {
        counts: counts
            .into_iter()
            .map(|(key, value)| (key, json!(value)))
            .collect(),
        metrics,
    })
}

pub fn get_dataset_info(connection: &Connection) -> Result<Map<String, Value>, String> {
    let mut statement = connection
        .prepare("SELECT key, value FROM dataset_info ORDER BY key")
        .map_err(|error| error.to_string())?;
    let mut rows = statement.query([]).map_err(|error| error.to_string())?;
    let mut payload = Map::new();
    while let Some(row) = rows.next().map_err(|error| error.to_string())? {
        payload.insert(
            row.get::<_, String>(0).map_err(|error| error.to_string())?,
            Value::String(row.get::<_, String>(1).map_err(|error| error.to_string())?),
        );
    }
    Ok(payload)
}

pub fn upsert_dataset_info(
    connection: &Connection,
    values: &[(&str, String)],
) -> Result<(), String> {
    let mut statement = connection
        .prepare(
            "
            INSERT INTO dataset_info(key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ",
        )
        .map_err(|error| error.to_string())?;
    for (key, value) in values {
        statement
            .execute(params![key, value])
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub fn parse_activity_filters(activity_types: &[String]) -> Vec<String> {
    activity_types
        .iter()
        .filter_map(|activity_type| {
            let candidate = activity_type.trim();
            if candidate.is_empty() {
                return None;
            }
            if candidate.starts_with("HKWorkoutActivityType") {
                return Some(candidate.to_string());
            }
            let words: Vec<&str> = candidate
                .split(|character: char| !character.is_ascii_alphanumeric())
                .filter(|segment| !segment.is_empty())
                .collect();
            if words.is_empty() {
                return None;
            }
            Some(format!(
                "HKWorkoutActivityType{}",
                words
                    .into_iter()
                    .map(|word| {
                        let mut characters = word.chars();
                        match characters.next() {
                            Some(first) => {
                                format!("{}{}", first.to_ascii_uppercase(), characters.as_str())
                            }
                            None => String::new(),
                        }
                    })
                    .collect::<String>()
            ))
        })
        .collect()
}

pub fn parse_workout_window(
    start: Option<&str>,
    end: Option<&str>,
) -> Result<(Option<String>, Option<String>), String> {
    let parsed_start = start
        .map(|value| parse_filter_datetime(value, false))
        .transpose()?;
    let parsed_end = end
        .map(|value| parse_filter_datetime(value, true))
        .transpose()?;
    if let (Some(start), Some(end)) = (&parsed_start, &parsed_end) {
        if start > end {
            return Err("--start must be earlier than or equal to --end.".into());
        }
    }
    Ok((parsed_start, parsed_end))
}

pub fn inspect_workouts(
    connection: &Connection,
    options: &WorkoutQueryOptions,
) -> Result<Value, String> {
    let workout_rows = load_effective_workout_rows(connection, options)?;
    build_workout_inspection_bundle(connection, options, &workout_rows)
}

pub fn load_health_overview(
    connection: &Connection,
    options: &HealthQueryOptions,
) -> Result<Value, String> {
    let selected_metrics =
        select_health_overview_metrics(&options.categories, options.metric_query.as_deref());
    if selected_metrics.is_empty() {
        return Ok(json!({
            "dataset_info": get_dataset_info(connection)?,
            "filters": build_health_filter_payload(options),
            "record_count": 0,
            "available_metric_count": 0,
            "first_start": Value::Null,
            "last_end": Value::Null,
            "metrics": [],
        }));
    }

    let record_types: Vec<String> = selected_metrics
        .iter()
        .map(|metric| metric.record_type.to_string())
        .collect();
    let (where_clause, parameters) = build_health_record_filter_clause(
        options.start.as_deref(),
        options.end.as_deref(),
        &record_types,
        options.source_query.as_deref(),
    );
    let overview = query_optional_object(
        connection,
        &format!(
            "
            SELECT
                COUNT(*) AS record_count,
                COUNT(DISTINCT r.record_type) AS available_metric_count,
                MIN(r.start_date) AS first_start,
                MAX(r.end_date) AS last_end
            FROM records r
            LEFT JOIN workout_records wr ON wr.record_id = r.id
            {where_clause}
            "
        ),
        &parameters,
    )?
    .unwrap_or_default();
    let aggregate_rows = query_objects(
        connection,
        &format!(
            "
            SELECT
                r.record_type,
                MAX(r.unit) AS unit,
                COUNT(*) AS sample_count,
                AVG(r.value_numeric) AS average_value,
                SUM(r.value_numeric) AS total_value,
                MIN(r.value_numeric) AS minimum_value,
                MAX(r.value_numeric) AS maximum_value
            FROM records r
            LEFT JOIN workout_records wr ON wr.record_id = r.id
            {where_clause}
            GROUP BY r.record_type
            "
        ),
        &parameters,
    )?;
    let aggregate_rows: HashMap<String, Map<String, Value>> = aggregate_rows
        .into_iter()
        .filter_map(|row| {
            let key = object_get_str(&row, "record_type")?.to_string();
            Some((key, row))
        })
        .collect();

    let mut daily_rows_by_type: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for row in query_objects(
        connection,
        &format!(
            "
            SELECT
                r.record_type,
                substr(r.start_date, 1, 10) AS day,
                MAX(r.unit) AS unit,
                COUNT(*) AS sample_count,
                AVG(r.value_numeric) AS average_value,
                SUM(r.value_numeric) AS total_value
            FROM records r
            LEFT JOIN workout_records wr ON wr.record_id = r.id
            {where_clause}
            GROUP BY r.record_type, day
            ORDER BY day
            "
        ),
        &parameters,
    )? {
        if let Some(record_type) = object_get_str(&row, "record_type") {
            daily_rows_by_type
                .entry(record_type.to_string())
                .or_default()
                .push(row);
        }
    }

    let latest_rows = query_objects(
        connection,
        &format!(
            "
            SELECT
                r.id,
                r.record_type,
                r.unit,
                r.value_numeric,
                r.end_date
            FROM records r
            LEFT JOIN workout_records wr ON wr.record_id = r.id
            {where_clause}
            ORDER BY r.record_type, r.end_date DESC, r.id DESC
            "
        ),
        &parameters,
    )?;
    let mut latest_by_type: HashMap<String, Map<String, Value>> = HashMap::new();
    for row in latest_rows {
        if let Some(record_type) = object_get_str(&row, "record_type") {
            latest_by_type.entry(record_type.to_string()).or_insert(row);
        }
    }

    let metrics: Vec<Value> = selected_metrics
        .into_iter()
        .map(|metric| {
            build_health_metric_summary(
                metric,
                aggregate_rows.get(metric.record_type),
                latest_by_type.get(metric.record_type),
                daily_rows_by_type
                    .get(metric.record_type)
                    .map(|rows| rows.as_slice())
                    .unwrap_or(&[]),
            )
        })
        .filter(|summary| {
            !options.only_with_samples || object_get_i64(summary, "sample_count").unwrap_or(0) > 0
        })
        .map(Value::Object)
        .collect();

    Ok(json!({
        "dataset_info": get_dataset_info(connection)?,
        "filters": build_health_filter_payload(options),
        "record_count": object_get_i64(&overview, "record_count").unwrap_or(0),
        "available_metric_count": object_get_i64(&overview, "available_metric_count").unwrap_or(0),
        "first_start": overview.get("first_start").cloned().unwrap_or(Value::Null),
        "last_end": overview.get("last_end").cloned().unwrap_or(Value::Null),
        "metrics": metrics,
    }))
}

pub fn load_workout_summary_bundle(
    connection: &Connection,
    options: &WorkoutQueryOptions,
    reporter: &mut dyn ProgressReporter,
) -> Result<Value, String> {
    let started_at = Instant::now();
    reporter.progress("Loading workout summaries from SQLite...")?;
    let mut summary_rows = load_effective_workout_rows(connection, options)?;
    build_workout_summary_bundle_from_rows(
        connection,
        options,
        &mut summary_rows,
        reporter,
        started_at,
    )
}

pub fn load_workout_dashboard_bundle(
    connection: &Connection,
    options: &WorkoutQueryOptions,
    reporter: &mut dyn ProgressReporter,
) -> Result<(Value, Value), String> {
    let started_at = Instant::now();
    reporter.progress("Loading workout summaries from SQLite...")?;
    let mut summary_rows = load_effective_workout_rows(connection, options)?;
    let inspection = build_workout_inspection_bundle(connection, options, &summary_rows)?;
    let summary = build_workout_summary_bundle_from_rows(
        connection,
        options,
        &mut summary_rows,
        reporter,
        started_at,
    )?;
    Ok((inspection, summary))
}

fn load_effective_workout_rows(
    connection: &Connection,
    options: &WorkoutQueryOptions,
) -> Result<Vec<Map<String, Value>>, String> {
    ensure_effective_workouts_cache(connection)?;
    let (where_clause, parameters) = build_workout_filter_clause(options)?;
    let (effective_where_clause, effective_parameters) =
        build_effective_workout_filter_clause(options)?;
    query_objects(
        connection,
        &format!(
            "
            SELECT *
            FROM effective_workouts
            {}
            ORDER BY start_date, id
            ",
            combine_where_clauses(&where_clause, &effective_where_clause)
        ),
        &combine_parameters(parameters, effective_parameters),
    )
}

fn build_workout_inspection_bundle(
    connection: &Connection,
    options: &WorkoutQueryOptions,
    workout_rows: &[Map<String, Value>],
) -> Result<Value, String> {
    Ok(json!({
        "dataset_info": get_dataset_info(connection)?,
        "filters": build_filter_payload(options),
        "overall": Value::Object(build_inspection_overall(workout_rows)),
        "by_activity_type": objects_to_values(build_inspection_by_activity(workout_rows)),
    }))
}

fn build_workout_summary_bundle_from_rows(
    connection: &Connection,
    options: &WorkoutQueryOptions,
    summary_rows: &mut [Map<String, Value>],
    reporter: &mut dyn ProgressReporter,
    started_at: Instant,
) -> Result<Value, String> {
    enrich_summary_rows_with_workout_metadata(connection, summary_rows)?;
    reporter.verbose(&format!(
        "Matched {} workouts for summary export",
        summary_rows.len()
    ))?;
    let workouts: Vec<Value> = summary_rows
        .iter()
        .map(|row| Value::Object(build_summary_workout_card(row)))
        .collect();
    reporter.verbose(&format!(
        "Assembled workout summary bundle in {}",
        format_elapsed(started_at.elapsed().as_secs_f64())
    ))?;
    Ok(json!({
        "dataset_info": get_dataset_info(connection)?,
        "filters": build_filter_payload(options),
        "timeframe": build_summary_timeframe(options),
        "workout_count": workouts.len(),
        "overall": Value::Object(build_summary_overall(summary_rows)),
        "activity_breakdown": objects_to_values(build_activity_breakdown(summary_rows)),
        "highlights": build_summary_highlights(summary_rows),
        "workouts": workouts,
    }))
}

fn enrich_summary_rows_with_workout_metadata(
    connection: &Connection,
    summary_rows: &mut [Map<String, Value>],
) -> Result<(), String> {
    if summary_rows.is_empty() {
        return Ok(());
    }
    let workout_ids: Vec<i64> = summary_rows
        .iter()
        .filter_map(|row| object_get_i64(row, "id"))
        .collect();
    let metadata_by_workout = fetch_workout_metadata_by_ids(connection, &workout_ids)?;
    for row in summary_rows {
        let workout_id = object_get_i64(row, "id").unwrap_or_default();
        let metadata = metadata_by_workout
            .get(&workout_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if let Some(elevation_gain_ft) = extract_workout_elevation_gain_ft(metadata) {
            row.insert("elevation_gain_ft".into(), json!(elevation_gain_ft));
        }
        if let Some(temperature_f) = extract_workout_temperature_f(metadata) {
            row.insert("temperature_f".into(), json!(temperature_f));
        }
    }
    Ok(())
}

pub fn load_workout_detail(
    connection: &Connection,
    workout_id: i64,
) -> Result<Option<Value>, String> {
    let workout = query_optional_object(
        connection,
        "SELECT * FROM workouts WHERE id = ?",
        &[SqlValue::Integer(workout_id)],
    )?;
    let Some(workout) = workout else {
        return Ok(None);
    };
    let workout_ids = [workout_id];
    let metadata_by_workout = fetch_workout_metadata_by_ids(connection, &workout_ids)?;
    let statistics_by_workout = load_workout_statistics_by_ids(connection, &workout_ids)?;
    let analysis_records_by_workout =
        load_workout_analysis_records_by_ids(connection, &workout_ids)?;

    let metadata = metadata_by_workout
        .get(&workout_id)
        .cloned()
        .unwrap_or_default();
    let statistics = statistics_by_workout
        .get(&workout_id)
        .cloned()
        .unwrap_or_default();
    let analysis_records = analysis_records_by_workout
        .get(&workout_id)
        .cloned()
        .unwrap_or_default();

    Ok(Some(Value::Object(build_workout_detail_payload(
        connection,
        &workout,
        metadata,
        statistics,
        analysis_records,
    )?)))
}

pub fn load_workout_metric_series(
    connection: &Connection,
    workout_id: i64,
) -> Result<Option<Vec<Value>>, String> {
    let workout = query_optional_object(
        connection,
        "SELECT * FROM workouts WHERE id = ?",
        &[SqlValue::Integer(workout_id)],
    )?;
    let Some(workout) = workout else {
        return Ok(None);
    };
    let analysis_records = load_workout_analysis_records_by_ids(connection, &[workout_id])?
        .remove(&workout_id)
        .unwrap_or_default();
    Ok(Some(build_workout_metric_series(
        object_get_str(&workout, "start_date"),
        &analysis_records,
    )))
}

pub fn list_activity_types(connection: &Connection) -> Result<Vec<Value>, String> {
    let rows = query_objects(
        connection,
        "
        SELECT
            activity_type,
            COUNT(*) AS workout_count,
            MIN(start_date) AS first_start,
            MAX(end_date) AS last_end
        FROM workouts
        GROUP BY activity_type
        ORDER BY workout_count DESC, activity_type
        ",
        &[],
    )?;
    Ok(rows
        .into_iter()
        .map(|row| {
            json!({
                "activity_type": object_get_str(&row, "activity_type"),
                "label": humanize_identifier(object_get_str(&row, "activity_type")),
                "workout_count": object_get_i64(&row, "workout_count").unwrap_or(0),
                "first_start": row.get("first_start").cloned().unwrap_or(Value::Null),
                "last_end": row.get("last_end").cloned().unwrap_or(Value::Null),
            })
        })
        .collect())
}

pub fn export_workouts(
    connection: &Connection,
    options: &ExportOptions,
    reporter: &mut dyn ProgressReporter,
) -> Result<Value, String> {
    if options.summary && options.export_format != "json" {
        return Err("--summary is only available with JSON export.".into());
    }
    if !CSV_EXPORT_PROFILES.contains(&options.csv_profile.as_str()) {
        return Err(format!(
            "Unsupported CSV export profile: {}",
            options.csv_profile
        ));
    }
    if options.export_format != "csv" && options.csv_profile != "full" {
        return Err("--csv-profile is only available with CSV export.".into());
    }

    let bundle =
        if options.summary || (options.export_format == "csv" && options.csv_profile == "llm") {
            load_workout_summary_bundle(connection, &options.workout, reporter)?
        } else {
            load_workout_export_bundle(connection, &options.workout, reporter)?
        };

    if options.export_format == "json" {
        let destination = resolve_json_output_path(&options.output_path, options.summary, &bundle);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        if options.summary {
            reporter.progress("Writing summary JSON export...")?;
            reporter.verbose(&format!(
                "Writing summary JSON export to {}",
                destination.display()
            ))?;
        } else {
            reporter.progress("Writing JSON export...")?;
            reporter.verbose(&format!("Writing JSON export to {}", destination.display()))?;
        }
        let file = File::create(&destination).map_err(|error| error.to_string())?;
        serde_json::to_writer_pretty(file, &bundle).map_err(|error| error.to_string())?;
        if options.summary {
            reporter.progress("Finished writing summary JSON export.")?;
        } else {
            reporter.progress("Finished writing JSON export.")?;
        }
        return Ok(json!({
            "format": "json",
            "path": path_to_string(&destination),
            "workout_count": bundle.get("workout_count").and_then(Value::as_u64).unwrap_or(0),
        }));
    }

    let destination_dir = resolve_csv_output_directory(&options.output_path)?;
    fs::create_dir_all(&destination_dir).map_err(|error| error.to_string())?;
    reporter.progress("Writing CSV export files...")?;
    reporter.verbose(&format!(
        "Writing CSV export directory to {} using profile '{}'",
        destination_dir.display(),
        options.csv_profile
    ))?;
    write_csv_exports(&bundle, &destination_dir, &options.csv_profile, reporter)?;
    reporter.progress("Finished writing CSV export files.")?;
    Ok(json!({
        "format": "csv",
        "path": path_to_string(&destination_dir),
        "workout_count": bundle.get("workout_count").and_then(Value::as_u64).unwrap_or(0),
    }))
}

fn load_workout_export_bundle(
    connection: &Connection,
    options: &WorkoutQueryOptions,
    reporter: &mut dyn ProgressReporter,
) -> Result<Value, String> {
    let started_at = Instant::now();
    reporter.progress("Loading matching workouts from SQLite...")?;
    let workout_rows = load_effective_workout_rows(connection, options)?;
    if workout_rows.is_empty() {
        reporter.verbose("No workouts matched the export filters.")?;
        return Ok(json!({
            "dataset_info": get_dataset_info(connection)?,
            "filters": build_filter_payload(options),
            "workout_count": 0,
            "workouts": [],
        }));
    }
    reporter.progress(&format!(
        "Loading associated data for {} workouts...",
        workout_rows.len()
    ))?;
    reporter.verbose(&format!(
        "Matched {} workouts for export",
        workout_rows.len()
    ))?;
    let workouts = build_workout_export_payloads(connection, &workout_rows, reporter)?;
    reporter.verbose(&format!(
        "Assembled workout export bundle in {}",
        format_elapsed(started_at.elapsed().as_secs_f64())
    ))?;
    Ok(json!({
        "dataset_info": get_dataset_info(connection)?,
        "filters": build_filter_payload(options),
        "workout_count": workouts.len(),
        "workouts": objects_to_values(workouts),
    }))
}

fn build_workout_export_payloads(
    connection: &Connection,
    workout_dicts: &[Map<String, Value>],
    reporter: &mut dyn ProgressReporter,
) -> Result<Vec<Map<String, Value>>, String> {
    if workout_dicts.is_empty() {
        return Ok(Vec::new());
    }

    let workout_ids: Vec<i64> = workout_dicts
        .iter()
        .filter_map(|row| object_get_i64(row, "id"))
        .collect();
    let database_path = main_database_path(connection)?;
    let associated_data_started_at = Instant::now();
    let (
        metadata_by_workout,
        events_by_workout,
        statistics_by_workout,
        routes_bundle,
        records_bundle,
    ) = std::thread::scope(|scope| {
        let metadata_db_path = database_path.clone();
        let metadata_ids = workout_ids.clone();
        let metadata_handle = scope.spawn(move || {
            let connection = open_existing_database(&metadata_db_path)?;
            fetch_workout_metadata_by_ids(&connection, &metadata_ids)
        });

        let events_db_path = database_path.clone();
        let event_ids = workout_ids.clone();
        let events_handle = scope.spawn(move || {
            let connection = open_existing_database(&events_db_path)?;
            load_workout_events_by_ids(&connection, &event_ids)
        });

        let statistics_db_path = database_path.clone();
        let statistic_ids = workout_ids.clone();
        let statistics_handle = scope.spawn(move || {
            let connection = open_existing_database(&statistics_db_path)?;
            load_workout_statistics_by_ids(&connection, &statistic_ids)
        });

        let routes_db_path = database_path.clone();
        let route_ids = workout_ids.clone();
        let routes_handle = scope.spawn(move || {
            let connection = open_existing_database(&routes_db_path)?;
            load_workout_routes_by_ids(&connection, &route_ids)
        });

        let records_db_path = database_path.clone();
        let record_ids = workout_ids.clone();
        let records_handle = scope.spawn(move || {
            let connection = open_existing_database(&records_db_path)?;
            load_workout_records_by_ids(&connection, &record_ids)
        });

        Ok::<_, String>((
            join_export_worker("workout metadata", metadata_handle)?,
            join_export_worker("workout events", events_handle)?,
            join_export_worker("workout statistics", statistics_handle)?,
            join_export_worker("workout routes", routes_handle)?,
            join_export_worker("linked workout records", records_handle)?,
        ))
    })?;
    let ExportRoutesBundle {
        routes_by_workout,
        route_count,
        route_metadata_count,
    } = routes_bundle;
    let ExportRecordsBundle {
        records_by_workout,
        linked_record_count,
        record_metadata_count,
    } = records_bundle;

    reporter.verbose(&format!(
        "Loaded associated export data in {}: {} workout metadata rows, {} workout events, {} workout statistics, {} workout routes ({} route metadata rows), and {} linked records ({} record metadata rows)",
        format_elapsed(associated_data_started_at.elapsed().as_secs_f64()),
        metadata_by_workout.values().map(|values| values.len()).sum::<usize>(),
        events_by_workout.values().map(|values| values.len()).sum::<usize>(),
        statistics_by_workout.values().map(|values| values.len()).sum::<usize>(),
        route_count,
        route_metadata_count,
        linked_record_count,
        record_metadata_count
    ))?;

    let mut workouts = Vec::new();
    for row in workout_dicts {
        let id = object_get_i64(row, "id").unwrap_or_default();
        let records = records_by_workout.get(&id).cloned().unwrap_or_default();
        let statistics = statistics_by_workout.get(&id).cloned().unwrap_or_default();
        let mut payload = Map::new();
        payload.insert("db_id".into(), json!(id));
        for key in [
            "uuid",
            "activity_type",
            "source_name",
            "source_version",
            "device",
            "creation_date",
            "start_date",
            "end_date",
            "duration_seconds",
            "total_distance",
            "total_distance_unit",
            "total_distance_meters",
            "total_energy_burned",
            "total_energy_burned_unit",
            "total_energy_burned_kilocalories",
        ] {
            payload.insert(key.into(), row.get(key).cloned().unwrap_or(Value::Null));
        }
        payload.insert(
            "metadata".into(),
            Value::Array(metadata_by_workout.get(&id).cloned().unwrap_or_default()),
        );
        payload.insert(
            "events".into(),
            Value::Array(events_by_workout.get(&id).cloned().unwrap_or_default()),
        );
        payload.insert("statistics".into(), Value::Array(statistics.clone()));
        payload.insert(
            "routes".into(),
            Value::Array(routes_by_workout.get(&id).cloned().unwrap_or_default()),
        );
        payload.insert("records".into(), Value::Array(records.clone()));
        let metadata = metadata_by_workout
            .get(&id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        payload.insert(
            "derived_metrics".into(),
            Value::Object(derive_workout_metrics(row, metadata, &statistics, &records)),
        );
        payload.insert(
            "raw_attributes".into(),
            parse_json_field(row, "raw_attributes"),
        );
        workouts.push(payload);
    }
    Ok(workouts)
}

fn fetch_workout_metadata_by_ids(
    connection: &Connection,
    workout_ids: &[i64],
) -> Result<HashMap<i64, Vec<Value>>, String> {
    if workout_ids.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(group_rows(
        &fetch_rows_for_ids(
            connection,
            "
            SELECT workout_id, key, value FROM workout_metadata
            WHERE workout_id IN ({placeholders})
            ORDER BY workout_id, key, value
            ",
            workout_ids,
        )?,
        "workout_id",
        |row| {
            json!({
                "key": object_get_str(row, "key"),
                "value": row.get("value").cloned().unwrap_or(Value::Null),
            })
        },
    ))
}

fn load_workout_events_by_ids(
    connection: &Connection,
    workout_ids: &[i64],
) -> Result<HashMap<i64, Vec<Value>>, String> {
    Ok(group_rows(
        &fetch_rows_for_ids(
            connection,
            "
            SELECT workout_id, event_type, event_date, duration_seconds, duration_unit, raw_attributes
            FROM workout_events
            WHERE workout_id IN ({placeholders})
            ORDER BY workout_id, event_date, id
            ",
            workout_ids,
        )?,
        "workout_id",
        |row| {
            json!({
                "event_type": object_get_str(row, "event_type"),
                "event_date": row.get("event_date").cloned().unwrap_or(Value::Null),
                "duration_seconds": row.get("duration_seconds").cloned().unwrap_or(Value::Null),
                "duration_unit": row.get("duration_unit").cloned().unwrap_or(Value::Null),
                "raw_attributes": parse_json_field(row, "raw_attributes"),
            })
        },
    ))
}

fn load_workout_statistics_by_ids(
    connection: &Connection,
    workout_ids: &[i64],
) -> Result<HashMap<i64, Vec<Value>>, String> {
    Ok(group_rows(
        &fetch_rows_for_ids(
            connection,
            "
            SELECT workout_id, statistic_type, start_date, end_date, unit, aggregation, value, raw_attributes
            FROM workout_statistics
            WHERE workout_id IN ({placeholders})
            ORDER BY workout_id, statistic_type, aggregation, start_date
            ",
            workout_ids,
        )?,
        "workout_id",
        |row| {
            json!({
                "statistic_type": object_get_str(row, "statistic_type"),
                "start_date": row.get("start_date").cloned().unwrap_or(Value::Null),
                "end_date": row.get("end_date").cloned().unwrap_or(Value::Null),
                "unit": row.get("unit").cloned().unwrap_or(Value::Null),
                "aggregation": object_get_str(row, "aggregation"),
                "value": row.get("value").cloned().unwrap_or(Value::Null),
                "raw_attributes": parse_json_field(row, "raw_attributes"),
            })
        },
    ))
}

struct ExportRoutesBundle {
    routes_by_workout: HashMap<i64, Vec<Value>>,
    route_count: usize,
    route_metadata_count: usize,
}

fn load_workout_routes_by_ids(
    connection: &Connection,
    workout_ids: &[i64],
) -> Result<ExportRoutesBundle, String> {
    let route_rows = fetch_rows_for_ids(
        connection,
        "
        SELECT id, workout_id, route_type, source_name, source_version, device, creation_date, start_date, end_date, raw_attributes
        FROM workout_routes
        WHERE workout_id IN ({placeholders})
        ORDER BY workout_id, start_date, id
        ",
        workout_ids,
    )?;
    let route_ids: Vec<i64> = route_rows
        .iter()
        .filter_map(|row| object_get_i64(row, "id"))
        .collect();
    let route_metadata_by_route = group_rows(
        &fetch_rows_for_ids(
            connection,
            "
            SELECT route_id, key, value FROM workout_route_metadata
            WHERE route_id IN ({placeholders})
            ORDER BY route_id, key, value
            ",
            &route_ids,
        )?,
        "route_id",
        |row| {
            json!({
                "key": object_get_str(row, "key"),
                "value": row.get("value").cloned().unwrap_or(Value::Null),
            })
        },
    );
    let route_metadata_count = route_metadata_by_route
        .values()
        .map(|values| values.len())
        .sum::<usize>();
    let mut routes_by_workout: HashMap<i64, Vec<Value>> = HashMap::new();
    for row in &route_rows {
        if let Some(workout_id) = object_get_i64(row, "workout_id") {
            routes_by_workout
                .entry(workout_id)
                .or_default()
                .push(json!({
                    "route_type": row.get("route_type").cloned().unwrap_or(Value::Null),
                    "source_name": row.get("source_name").cloned().unwrap_or(Value::Null),
                    "source_version": row.get("source_version").cloned().unwrap_or(Value::Null),
                    "device": row.get("device").cloned().unwrap_or(Value::Null),
                    "creation_date": row.get("creation_date").cloned().unwrap_or(Value::Null),
                    "start_date": row.get("start_date").cloned().unwrap_or(Value::Null),
                    "end_date": row.get("end_date").cloned().unwrap_or(Value::Null),
                    "metadata": route_metadata_by_route
                        .get(&object_get_i64(row, "id").unwrap_or_default())
                        .cloned()
                        .unwrap_or_default(),
                    "raw_attributes": parse_json_field(row, "raw_attributes"),
                }));
        }
    }
    Ok(ExportRoutesBundle {
        routes_by_workout,
        route_count: route_rows.len(),
        route_metadata_count,
    })
}

struct ExportRecordsBundle {
    records_by_workout: HashMap<i64, Vec<Value>>,
    linked_record_count: usize,
    record_metadata_count: usize,
}

fn load_workout_records_by_ids(
    connection: &Connection,
    workout_ids: &[i64],
) -> Result<ExportRecordsBundle, String> {
    let linked_record_rows = fetch_rows_for_ids(
        connection,
        "
        SELECT workout_records.workout_id, records.*
        FROM workout_records
        JOIN records ON records.id = workout_records.record_id
        WHERE workout_records.workout_id IN ({placeholders})
        ORDER BY workout_records.workout_id, records.start_date, records.id
        ",
        workout_ids,
    )?;
    let record_ids: Vec<i64> = linked_record_rows
        .iter()
        .filter_map(|row| object_get_i64(row, "id"))
        .collect();
    let record_metadata_by_record = group_rows(
        &fetch_rows_for_ids(
            connection,
            "
            SELECT record_id, key, value FROM record_metadata
            WHERE record_id IN ({placeholders})
            ORDER BY record_id, key, value
            ",
            &record_ids,
        )?,
        "record_id",
        |row| {
            json!({
                "key": object_get_str(row, "key"),
                "value": row.get("value").cloned().unwrap_or(Value::Null),
            })
        },
    );
    let record_metadata_count = record_metadata_by_record
        .values()
        .map(|values| values.len())
        .sum::<usize>();
    let mut records_by_workout: HashMap<i64, Vec<Value>> = HashMap::new();
    for row in &linked_record_rows {
        if let Some(workout_id) = object_get_i64(row, "workout_id") {
            records_by_workout
                .entry(workout_id)
                .or_default()
                .push(json!({
                    "uuid": row.get("uuid").cloned().unwrap_or(Value::Null),
                    "record_type": object_get_str(row, "record_type"),
                    "source_name": row.get("source_name").cloned().unwrap_or(Value::Null),
                    "source_version": row.get("source_version").cloned().unwrap_or(Value::Null),
                    "unit": row.get("unit").cloned().unwrap_or(Value::Null),
                    "value_text": row.get("value_text").cloned().unwrap_or(Value::Null),
                    "value_numeric": row.get("value_numeric").cloned().unwrap_or(Value::Null),
                    "device": row.get("device").cloned().unwrap_or(Value::Null),
                    "creation_date": row.get("creation_date").cloned().unwrap_or(Value::Null),
                    "start_date": row.get("start_date").cloned().unwrap_or(Value::Null),
                    "end_date": row.get("end_date").cloned().unwrap_or(Value::Null),
                    "metadata": record_metadata_by_record
                        .get(&object_get_i64(row, "id").unwrap_or_default())
                        .cloned()
                        .unwrap_or_default(),
                    "raw_attributes": parse_json_field(row, "raw_attributes"),
                }));
        }
    }
    Ok(ExportRecordsBundle {
        records_by_workout,
        linked_record_count: linked_record_rows.len(),
        record_metadata_count,
    })
}

fn load_workout_analysis_records_by_ids(
    connection: &Connection,
    workout_ids: &[i64],
) -> Result<HashMap<i64, Vec<Value>>, String> {
    if workout_ids.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(group_rows(
        &fetch_rows_for_ids(
            connection,
            "
            SELECT workout_records.workout_id, records.record_type, records.unit, records.value_numeric, records.start_date, records.end_date
            FROM workout_records
            JOIN records ON records.id = workout_records.record_id
            WHERE workout_records.workout_id IN ({placeholders})
            ORDER BY workout_records.workout_id, records.start_date, records.id
            ",
            workout_ids,
        )?,
        "workout_id",
        |row| {
            json!({
                "record_type": object_get_str(row, "record_type"),
                "unit": row.get("unit").cloned().unwrap_or(Value::Null),
                "value_numeric": row.get("value_numeric").cloned().unwrap_or(Value::Null),
                "start_date": row.get("start_date").cloned().unwrap_or(Value::Null),
                "end_date": row.get("end_date").cloned().unwrap_or(Value::Null),
            })
        },
    ))
}

fn build_workout_detail_payload(
    connection: &Connection,
    workout: &Map<String, Value>,
    metadata: Vec<Value>,
    statistics: Vec<Value>,
    analysis_records: Vec<Value>,
) -> Result<Map<String, Value>, String> {
    let workout_id = object_get_i64(workout, "id").unwrap_or_default();
    let linked_data_counts =
        load_workout_linked_data_counts(connection, workout_id, metadata.len() as i64)?;
    let mut derived_metrics =
        derive_workout_metrics(workout, &metadata, &statistics, &analysis_records);
    derived_metrics.insert(
        "associated_record_count".into(),
        json!(object_get_i64(&linked_data_counts, "records").unwrap_or(0)),
    );

    let mut payload = Map::new();
    payload.insert("db_id".into(), json!(workout_id));
    for key in [
        "uuid",
        "activity_type",
        "source_name",
        "source_version",
        "device",
        "creation_date",
        "start_date",
        "end_date",
        "duration_seconds",
        "total_distance",
        "total_distance_unit",
        "total_distance_meters",
        "total_energy_burned",
        "total_energy_burned_unit",
        "total_energy_burned_kilocalories",
    ] {
        payload.insert(key.into(), workout.get(key).cloned().unwrap_or(Value::Null));
    }
    payload.insert("metadata".into(), Value::Array(metadata));
    payload.insert(
        "linked_data_counts".into(),
        Value::Object(linked_data_counts),
    );
    payload.insert("derived_metrics".into(), Value::Object(derived_metrics));
    Ok(payload)
}

fn load_workout_linked_data_counts(
    connection: &Connection,
    workout_id: i64,
    metadata_count: i64,
) -> Result<Map<String, Value>, String> {
    let counts = query_optional_object(
        connection,
        "
        SELECT
            (SELECT COUNT(*) FROM workout_records WHERE workout_id = ?) AS record_count,
            (SELECT COUNT(*) FROM workout_events WHERE workout_id = ?) AS event_count,
            (SELECT COUNT(*) FROM workout_routes WHERE workout_id = ?) AS route_count
        ",
        &[
            SqlValue::Integer(workout_id),
            SqlValue::Integer(workout_id),
            SqlValue::Integer(workout_id),
        ],
    )?
    .unwrap_or_default();

    Ok(map_from_pairs([
        (
            "records",
            json!(object_get_i64(&counts, "record_count").unwrap_or(0)),
        ),
        ("metadata", json!(metadata_count)),
        (
            "events",
            json!(object_get_i64(&counts, "event_count").unwrap_or(0)),
        ),
        (
            "routes",
            json!(object_get_i64(&counts, "route_count").unwrap_or(0)),
        ),
    ]))
}

fn build_workout_metric_series(workout_start: Option<&str>, records: &[Value]) -> Vec<Value> {
    let parsed_workout_start =
        workout_start.and_then(|value| DateTime::parse_from_rfc3339(value).ok());
    let mut grouped: HashMap<String, (Option<String>, Vec<(String, f64, f64)>)> = HashMap::new();

    for record in records {
        let Some(record) = record.as_object() else {
            continue;
        };
        let Some(record_type) = object_get_str(record, "record_type") else {
            continue;
        };
        let Some(value_numeric) = object_get_f64(record, "value_numeric") else {
            continue;
        };
        let Some(timestamp) =
            object_get_str(record, "start_date").or_else(|| object_get_str(record, "end_date"))
        else {
            continue;
        };

        let entry = grouped.entry(record_type.to_string()).or_insert_with(|| {
            (
                object_get_str(record, "unit").map(ToOwned::to_owned),
                Vec::new(),
            )
        });
        if entry.0.is_none() {
            entry.0 = object_get_str(record, "unit").map(ToOwned::to_owned);
        }

        let elapsed_minutes = match (
            parsed_workout_start.as_ref(),
            DateTime::parse_from_rfc3339(timestamp).ok(),
        ) {
            (Some(workout_start), Some(point_time)) => {
                ((point_time.timestamp_millis() - workout_start.timestamp_millis()).max(0) as f64)
                    / 60_000.0
            }
            _ => entry.1.len() as f64,
        };

        entry
            .1
            .push((timestamp.to_string(), elapsed_minutes, value_numeric));
    }

    let mut series = grouped
        .into_iter()
        .map(|(record_type, (unit, mut points))| {
            points.sort_by(|left, right| {
                left.1
                    .partial_cmp(&right.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let sample_count = points.len();
            let mut total = 0.0;
            let mut minimum = f64::INFINITY;
            let mut maximum = f64::NEG_INFINITY;
            for (_, _, value) in &points {
                total += value;
                minimum = minimum.min(*value);
                maximum = maximum.max(*value);
            }

            let latest_point = points.last();
            let mut payload = Map::new();
            payload.insert("key".into(), json!(record_type));
            payload.insert(
                "label".into(),
                json!(humanize_identifier(Some(record_type.as_str()))),
            );
            payload.insert("unit".into(), json!(unit));
            payload.insert("sampleCount".into(), json!(sample_count));
            payload.insert("average".into(), json!(total / sample_count as f64));
            payload.insert("minimum".into(), json!(minimum));
            payload.insert("maximum".into(), json!(maximum));
            payload.insert(
                "latestAt".into(),
                json!(latest_point.map(|(timestamp, _, _)| timestamp.clone())),
            );
            payload.insert(
                "latestValue".into(),
                json!(latest_point.map(|(_, _, value)| *value)),
            );
            payload.insert(
                "points".into(),
                Value::Array(
                    points
                        .into_iter()
                        .map(|(timestamp, elapsed_minutes, value)| {
                            json!({
                                "timestamp": timestamp,
                                "elapsedMinutes": elapsed_minutes,
                                "value": value,
                            })
                        })
                        .collect(),
                ),
            );
            payload
        })
        .collect::<Vec<_>>();

    series.sort_by(|left, right| {
        let left_key = object_get_str(left, "key").unwrap_or("");
        let right_key = object_get_str(right, "key").unwrap_or("");
        let left_priority = if left_key == HEART_RATE_STATISTIC_TYPE {
            0
        } else {
            1
        };
        let right_priority = if right_key == HEART_RATE_STATISTIC_TYPE {
            0
        } else {
            1
        };
        left_priority
            .cmp(&right_priority)
            .then_with(|| {
                right
                    .get("sampleCount")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    .cmp(&left.get("sampleCount").and_then(Value::as_u64).unwrap_or(0))
            })
            .then_with(|| {
                object_get_str(left, "label")
                    .unwrap_or("")
                    .cmp(object_get_str(right, "label").unwrap_or(""))
            })
    });

    series.into_iter().map(Value::Object).collect()
}

fn recreate_ingest_schema(
    connection: &Connection,
    reporter: &mut dyn ProgressReporter,
) -> Result<(), String> {
    for table in DATA_TABLES {
        connection
            .execute(&format!("DROP TABLE IF EXISTS {table}"), [])
            .map_err(|error| error.to_string())?;
        reporter.verbose(&format!("Dropped table {table}"))?;
    }
    connection
        .execute_batch(&schema_sql())
        .map_err(|error| error.to_string())?;
    reporter.verbose("Recreated SQLite schema for ingest.")?;
    Ok(())
}

fn insert_workout(
    transaction: &Transaction<'_>,
    statements: &mut IngestStatements<'_>,
    workout: &ParsedWorkout,
) -> Result<usize, String> {
    statements
        .insert_workout
        .execute(params![
            workout.uuid,
            workout.activity_type,
            workout.source_name,
            workout.source_version,
            workout.device,
            workout.creation_date,
            workout.start_date,
            workout.end_date,
            workout.duration_seconds,
            workout.total_distance,
            workout.total_distance_unit,
            workout.total_distance_meters,
            workout.total_energy_burned,
            workout.total_energy_burned_unit,
            workout.total_energy_burned_kilocalories,
            workout.raw_attributes,
        ])
        .map_err(|error| error.to_string())?;
    let workout_id = transaction.last_insert_rowid();
    for entry in &workout.metadata {
        statements
            .insert_workout_metadata
            .execute(params![workout_id, entry.key, entry.value])
            .map_err(|error| error.to_string())?;
    }
    let mut event_count = 0usize;
    for event in &workout.events {
        statements
            .insert_workout_event
            .execute(params![
                workout_id,
                event.event_type,
                event.event_date,
                event.duration_seconds,
                event.duration_unit,
                event.raw_attributes
            ])
            .map_err(|error| error.to_string())?;
        event_count += 1;
    }
    for statistic in &workout.statistics {
        for (aggregation, value) in &statistic.aggregations {
            statements
                .insert_workout_statistic
                .execute(params![
                    workout_id,
                    statistic.statistic_type,
                    statistic.start_date,
                    statistic.end_date,
                    statistic.unit,
                    aggregation,
                    value,
                    statistic.raw_attributes
                ])
                .map_err(|error| error.to_string())?;
        }
    }
    for route in &workout.routes {
        statements
            .insert_workout_route
            .execute(params![
                workout_id,
                route.route_type,
                route.source_name,
                route.source_version,
                route.device,
                route.creation_date,
                route.start_date,
                route.end_date,
                route.raw_attributes,
            ])
            .map_err(|error| error.to_string())?;
        let route_id = transaction.last_insert_rowid();
        for entry in &route.metadata {
            statements
                .insert_workout_route_metadata
                .execute(params![route_id, entry.key, entry.value])
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(event_count)
}

fn insert_record(
    transaction: &Transaction<'_>,
    statements: &mut IngestStatements<'_>,
    record: &ParsedRecord,
) -> Result<usize, String> {
    statements
        .insert_record
        .execute(params![
            record.uuid,
            record.record_type,
            record.source_name,
            record.source_version,
            record.unit,
            record.value_text,
            record.value_numeric,
            record.device,
            record.creation_date,
            record.start_date,
            record.end_date,
            record.raw_attributes,
        ])
        .map_err(|error| error.to_string())?;
    let record_id = transaction.last_insert_rowid();
    for entry in &record.metadata {
        statements
            .insert_record_metadata
            .execute(params![record_id, entry.key, entry.value])
            .map_err(|error| error.to_string())?;
    }
    Ok(record.metadata.len())
}

struct HealthExportParser {
    reader: Reader<BufReader<File>>,
    buffer: Vec<u8>,
}

impl HealthExportParser {
    fn new(path: &Path) -> Result<Self, String> {
        let file = File::open(path).map_err(|error| error.to_string())?;
        let mut reader = Reader::from_reader(BufReader::new(file));
        reader.config_mut().trim_text(true);
        Ok(Self {
            reader,
            buffer: Vec::new(),
        })
    }

    fn next_item(&mut self) -> Result<Option<ParsedItem>, String> {
        loop {
            let event = self
                .reader
                .read_event_into(&mut self.buffer)
                .map_err(|error| error.to_string())?
                .into_owned();
            match event {
                Event::Start(event) if event.name().as_ref() == b"Record" => {
                    let record = self.parse_record(event)?;
                    self.buffer.clear();
                    return Ok(Some(ParsedItem::Record(record)));
                }
                Event::Empty(event) if event.name().as_ref() == b"Record" => {
                    let record = self.parse_empty_record(event)?;
                    self.buffer.clear();
                    return Ok(Some(ParsedItem::Record(record)));
                }
                Event::Start(event) if event.name().as_ref() == b"Workout" => {
                    let workout = self.parse_workout(event)?;
                    self.buffer.clear();
                    return Ok(Some(ParsedItem::Workout(workout)));
                }
                Event::Empty(event) if event.name().as_ref() == b"Workout" => {
                    let workout = self.parse_empty_workout(event)?;
                    self.buffer.clear();
                    return Ok(Some(ParsedItem::Workout(workout)));
                }
                Event::Eof => return Ok(None),
                _ => self.buffer.clear(),
            }
        }
    }

    fn parse_empty_record(&mut self, event: BytesStart<'_>) -> Result<ParsedRecord, String> {
        self.record_from_attributes(attributes_map(&event)?)
    }

    fn parse_record(&mut self, event: BytesStart<'_>) -> Result<ParsedRecord, String> {
        let attributes = attributes_map(&event)?;
        let mut metadata = Vec::new();
        loop {
            self.buffer.clear();
            match self
                .reader
                .read_event_into(&mut self.buffer)
                .map_err(|error| error.to_string())?
            {
                Event::Empty(child) if child.name().as_ref() == b"MetadataEntry" => {
                    metadata.push(metadata_entry_from_attributes(attributes_map(&child)?)?);
                }
                Event::Start(child) if child.name().as_ref() == b"MetadataEntry" => {
                    metadata.push(metadata_entry_from_attributes(attributes_map(&child)?)?);
                    consume_to_end(&mut self.reader, b"MetadataEntry", &mut self.buffer)?;
                }
                Event::End(child) if child.name().as_ref() == b"Record" => break,
                Event::Eof => return Err("Unexpected end of XML while parsing Record.".into()),
                _ => {}
            }
        }
        let mut record = self.record_from_attributes(attributes)?;
        record.metadata = metadata;
        Ok(record)
    }

    fn parse_empty_workout(&mut self, event: BytesStart<'_>) -> Result<ParsedWorkout, String> {
        self.workout_from_parts(
            attributes_map(&event)?,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    fn parse_workout(&mut self, event: BytesStart<'_>) -> Result<ParsedWorkout, String> {
        let attributes = attributes_map(&event)?;
        let mut metadata = Vec::new();
        let mut events = Vec::new();
        let mut statistics = Vec::new();
        let mut routes = Vec::new();
        loop {
            self.buffer.clear();
            match self
                .reader
                .read_event_into(&mut self.buffer)
                .map_err(|error| error.to_string())?
            {
                Event::Empty(child) if child.name().as_ref() == b"MetadataEntry" => {
                    metadata.push(metadata_entry_from_attributes(attributes_map(&child)?)?);
                }
                Event::Start(child) if child.name().as_ref() == b"MetadataEntry" => {
                    metadata.push(metadata_entry_from_attributes(attributes_map(&child)?)?);
                    consume_to_end(&mut self.reader, b"MetadataEntry", &mut self.buffer)?;
                }
                Event::Empty(child) if child.name().as_ref() == b"WorkoutEvent" => {
                    events.push(parse_workout_event(attributes_map(&child)?)?);
                }
                Event::Start(child) if child.name().as_ref() == b"WorkoutEvent" => {
                    events.push(parse_workout_event(attributes_map(&child)?)?);
                    consume_to_end(&mut self.reader, b"WorkoutEvent", &mut self.buffer)?;
                }
                Event::Empty(child) if child.name().as_ref() == b"WorkoutStatistics" => {
                    statistics.push(parse_workout_statistics(attributes_map(&child)?)?);
                }
                Event::Start(child) if child.name().as_ref() == b"WorkoutStatistics" => {
                    statistics.push(parse_workout_statistics(attributes_map(&child)?)?);
                    consume_to_end(&mut self.reader, b"WorkoutStatistics", &mut self.buffer)?;
                }
                Event::Empty(child) if child.name().as_ref() == b"WorkoutRoute" => {
                    routes.push(parse_empty_workout_route(attributes_map(&child)?)?);
                }
                Event::Start(child) if child.name().as_ref() == b"WorkoutRoute" => {
                    routes.push(parse_workout_route(
                        &mut self.reader,
                        attributes_map(&child)?,
                        &mut self.buffer,
                    )?);
                }
                Event::End(child) if child.name().as_ref() == b"Workout" => break,
                Event::Eof => return Err("Unexpected end of XML while parsing Workout.".into()),
                _ => {}
            }
        }
        self.workout_from_parts(attributes, metadata, events, statistics, routes)
    }

    fn record_from_attributes(
        &self,
        attributes: BTreeMap<String, String>,
    ) -> Result<ParsedRecord, String> {
        let record_type = require_attribute(&attributes, "type", "Record")?;
        let start_date = parse_required_timestamp(&attributes, "startDate", "Record")?;
        let end_date = parse_required_timestamp(&attributes, "endDate", "Record")?;
        Ok(ParsedRecord {
            uuid: attributes.get("uuid").cloned(),
            record_type,
            source_name: attributes.get("sourceName").cloned(),
            source_version: attributes.get("sourceVersion").cloned(),
            unit: attributes.get("unit").cloned(),
            value_text: attributes.get("value").cloned(),
            value_numeric: parse_optional_float(attributes.get("value").map(String::as_str)),
            device: attributes.get("device").cloned(),
            creation_date: maybe_parse_health_datetime(
                attributes.get("creationDate").map(String::as_str),
            )?,
            start_date,
            end_date,
            metadata: Vec::new(),
            raw_attributes: serialize_attributes(&attributes)?,
        })
    }

    fn workout_from_parts(
        &self,
        attributes: BTreeMap<String, String>,
        metadata: Vec<MetadataEntry>,
        events: Vec<WorkoutEventData>,
        statistics: Vec<WorkoutStatisticData>,
        routes: Vec<WorkoutRouteData>,
    ) -> Result<ParsedWorkout, String> {
        let activity_type = require_attribute(&attributes, "workoutActivityType", "Workout")?;
        let start_date = parse_required_timestamp(&attributes, "startDate", "Workout")?;
        let end_date = parse_required_timestamp(&attributes, "endDate", "Workout")?;
        let total_distance =
            parse_optional_float(attributes.get("totalDistance").map(String::as_str));
        let total_distance_unit = attributes.get("totalDistanceUnit").cloned();
        let total_energy =
            parse_optional_float(attributes.get("totalEnergyBurned").map(String::as_str));
        let total_energy_unit = attributes.get("totalEnergyBurnedUnit").cloned();
        Ok(ParsedWorkout {
            uuid: attributes.get("uuid").cloned(),
            activity_type,
            source_name: attributes.get("sourceName").cloned(),
            source_version: attributes.get("sourceVersion").cloned(),
            device: attributes.get("device").cloned(),
            creation_date: maybe_parse_health_datetime(
                attributes.get("creationDate").map(String::as_str),
            )?,
            start_date,
            end_date,
            duration_seconds: convert_measurement(
                attributes.get("duration").map(String::as_str),
                attributes.get("durationUnit").map(String::as_str),
                DURATION_TO_SECONDS,
            ),
            total_distance,
            total_distance_unit: total_distance_unit.clone(),
            total_distance_meters: convert_measurement(
                attributes.get("totalDistance").map(String::as_str),
                total_distance_unit.as_deref(),
                DISTANCE_TO_METERS,
            ),
            total_energy_burned: total_energy,
            total_energy_burned_unit: total_energy_unit.clone(),
            total_energy_burned_kilocalories: convert_measurement(
                attributes.get("totalEnergyBurned").map(String::as_str),
                total_energy_unit.as_deref(),
                ENERGY_TO_KILOCALORIES,
            ),
            metadata,
            events,
            statistics,
            routes,
            raw_attributes: serialize_attributes(&attributes)?,
        })
    }
}

struct NoopReporter;

impl ProgressReporter for NoopReporter {
    fn progress(&mut self, _message: &str) -> Result<(), String> {
        Ok(())
    }

    fn verbose(&mut self, _message: &str) -> Result<(), String> {
        Ok(())
    }

    fn is_cancelled(&self) -> bool {
        false
    }
}

fn parse_workout_route(
    reader: &mut Reader<BufReader<File>>,
    attributes: BTreeMap<String, String>,
    buffer: &mut Vec<u8>,
) -> Result<WorkoutRouteData, String> {
    let mut metadata = Vec::new();
    loop {
        buffer.clear();
        match reader
            .read_event_into(buffer)
            .map_err(|error| error.to_string())?
        {
            Event::Empty(child) if child.name().as_ref() == b"MetadataEntry" => {
                metadata.push(metadata_entry_from_attributes(attributes_map(&child)?)?);
            }
            Event::Start(child) if child.name().as_ref() == b"MetadataEntry" => {
                metadata.push(metadata_entry_from_attributes(attributes_map(&child)?)?);
                consume_to_end(reader, b"MetadataEntry", buffer)?;
            }
            Event::End(child) if child.name().as_ref() == b"WorkoutRoute" => break,
            Event::Eof => return Err("Unexpected end of XML while parsing WorkoutRoute.".into()),
            _ => {}
        }
    }
    build_workout_route(attributes, metadata)
}

fn parse_empty_workout_route(
    attributes: BTreeMap<String, String>,
) -> Result<WorkoutRouteData, String> {
    build_workout_route(attributes, Vec::new())
}

fn build_workout_route(
    attributes: BTreeMap<String, String>,
    metadata: Vec<MetadataEntry>,
) -> Result<WorkoutRouteData, String> {
    Ok(WorkoutRouteData {
        route_type: attributes.get("type").cloned(),
        source_name: attributes.get("sourceName").cloned(),
        source_version: attributes.get("sourceVersion").cloned(),
        device: attributes.get("device").cloned(),
        creation_date: maybe_parse_health_datetime(
            attributes.get("creationDate").map(String::as_str),
        )?,
        start_date: maybe_parse_health_datetime(attributes.get("startDate").map(String::as_str))?,
        end_date: maybe_parse_health_datetime(attributes.get("endDate").map(String::as_str))?,
        metadata,
        raw_attributes: serialize_attributes(&attributes)?,
    })
}

fn parse_workout_event(attributes: BTreeMap<String, String>) -> Result<WorkoutEventData, String> {
    Ok(WorkoutEventData {
        event_type: require_attribute(&attributes, "type", "WorkoutEvent")?,
        event_date: maybe_parse_health_datetime(attributes.get("date").map(String::as_str))?,
        duration_seconds: convert_measurement(
            attributes.get("duration").map(String::as_str),
            attributes.get("durationUnit").map(String::as_str),
            DURATION_TO_SECONDS,
        ),
        duration_unit: attributes.get("durationUnit").cloned(),
        raw_attributes: serialize_attributes(&attributes)?,
    })
}

fn parse_workout_statistics(
    attributes: BTreeMap<String, String>,
) -> Result<WorkoutStatisticData, String> {
    let mut aggregations = Vec::new();
    for (aggregation, attribute_name) in [
        ("sum", "sum"),
        ("average", "average"),
        ("minimum", "minimum"),
        ("maximum", "maximum"),
    ] {
        if let Some(value) =
            parse_optional_float(attributes.get(attribute_name).map(String::as_str))
        {
            aggregations.push((aggregation.to_string(), value));
        }
    }
    Ok(WorkoutStatisticData {
        statistic_type: require_attribute(&attributes, "type", "WorkoutStatistics")?,
        start_date: maybe_parse_health_datetime(attributes.get("startDate").map(String::as_str))?,
        end_date: maybe_parse_health_datetime(attributes.get("endDate").map(String::as_str))?,
        unit: attributes.get("unit").cloned(),
        aggregations,
        raw_attributes: serialize_attributes(&attributes)?,
    })
}

fn metadata_entry_from_attributes(
    attributes: BTreeMap<String, String>,
) -> Result<MetadataEntry, String> {
    Ok(MetadataEntry {
        key: require_attribute(&attributes, "key", "MetadataEntry")?,
        value: attributes.get("value").cloned(),
    })
}

fn consume_to_end(
    reader: &mut Reader<BufReader<File>>,
    end_name: &[u8],
    buffer: &mut Vec<u8>,
) -> Result<(), String> {
    loop {
        buffer.clear();
        match reader
            .read_event_into(buffer)
            .map_err(|error| error.to_string())?
        {
            Event::End(event) if event.name().as_ref() == end_name => return Ok(()),
            Event::Eof => return Err("Unexpected end of XML.".into()),
            _ => {}
        }
    }
}

fn attributes_map(event: &BytesStart<'_>) -> Result<BTreeMap<String, String>, String> {
    let mut attributes = BTreeMap::new();
    for attribute in event.attributes() {
        let attribute = attribute.map_err(|error| error.to_string())?;
        attributes.insert(
            String::from_utf8_lossy(attribute.key.as_ref()).into_owned(),
            attribute
                .decode_and_unescape_value(event.decoder())
                .map_err(|error| error.to_string())?
                .into_owned(),
        );
    }
    Ok(attributes)
}

pub fn preprocess_export_xml(
    source_path: &Path,
    reporter: &mut dyn ProgressReporter,
) -> Result<NamedTempFile, String> {
    reporter.progress("Preprocessing Apple Health XML...")?;
    reporter.verbose(&format!(
        "Reading Apple Health export from {}",
        source_path.display()
    ))?;
    let started_at = Instant::now();
    let source = File::open(source_path).map_err(|error| error.to_string())?;
    let mut reader = BufReader::new(source);
    let mut destination = NamedTempFile::new().map_err(|error| error.to_string())?;
    let mut writer = BufWriter::new(destination.as_file_mut());
    let mut line = Vec::new();
    let mut line_count = 0usize;
    let mut skipping_doctype = false;
    let mut doctype_terminator: Option<&[u8]> = None;

    loop {
        if reporter.is_cancelled() {
            return Err("Ingest cancelled.".into());
        }
        line.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut line)
            .map_err(|error| error.to_string())?;
        if bytes_read == 0 {
            break;
        }
        if line.ends_with(b"\n") {
            line.pop();
            if line.ends_with(b"\r") {
                line.pop();
            }
        }
        line_count += 1;
        if line_count % PREPROCESS_PROGRESS_INTERVAL == 0 {
            reporter.progress(&format!(
                "Preprocessed {} XML lines...",
                format_count(line_count)
            ))?;
        }

        if !skipping_doctype {
            if let Some(index) = find_bytes(&line, b"<!DOCTYPE") {
                let (before, remainder) = line.split_at(index);
                if !before.is_empty() {
                    write_sanitized_xml_bytes(&mut writer, before)?;
                }
                let remainder = &remainder["<!DOCTYPE".len()..];
                let terminator = if remainder.contains(&b'[') {
                    b"]>".as_slice()
                } else {
                    b">".as_slice()
                };
                doctype_terminator = Some(terminator);
                if let Some(terminator_index) = find_bytes(remainder, terminator) {
                    let rest = &remainder[terminator_index + terminator.len()..];
                    write_sanitized_xml_bytes(&mut writer, rest)?;
                    writer.write_all(b"\n").map_err(|error| error.to_string())?;
                    doctype_terminator = None;
                } else {
                    skipping_doctype = true;
                }
                continue;
            }
        } else if let Some(terminator) = doctype_terminator {
            if let Some(index) = find_bytes(&line, terminator) {
                let rest = &line[index + terminator.len()..];
                write_sanitized_xml_bytes(&mut writer, rest)?;
                writer.write_all(b"\n").map_err(|error| error.to_string())?;
                skipping_doctype = false;
                doctype_terminator = None;
            }
            continue;
        }

        write_sanitized_xml_bytes(&mut writer, &line)?;
        writer.write_all(b"\n").map_err(|error| error.to_string())?;
    }
    writer.flush().map_err(|error| error.to_string())?;
    drop(writer);
    reporter.verbose(&format!(
        "Preprocessing completed in {} across {} lines",
        format_elapsed(started_at.elapsed().as_secs_f64()),
        format_count(line_count)
    ))?;
    reporter.verbose(&format!(
        "Temporary preprocessed XML written to {}",
        destination.path().display()
    ))?;
    reporter.progress("Finished preprocessing Apple Health XML.")?;
    Ok(destination)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn write_sanitized_xml_bytes<W: Write>(writer: &mut W, bytes: &[u8]) -> Result<(), String> {
    let mut start = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte >= 32 || matches!(*byte, 9 | 10 | 13) {
            continue;
        }
        if start < index {
            writer
                .write_all(&bytes[start..index])
                .map_err(|error| error.to_string())?;
        }
        start = index + 1;
    }
    if start < bytes.len() {
        writer
            .write_all(&bytes[start..])
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn build_workout_filter_clause(
    options: &WorkoutQueryOptions,
) -> Result<(String, Vec<SqlValue>), String> {
    let mut conditions = Vec::new();
    let mut parameters = Vec::new();
    validate_numeric_range(
        options.min_duration_minutes,
        options.max_duration_minutes,
        "Duration",
    )?;
    if let Some(start) = options.start.as_deref() {
        conditions.push("end_date >= ?".to_string());
        parameters.push(SqlValue::Text(start.to_string()));
    }
    if let Some(end) = options.end.as_deref() {
        conditions.push("start_date <= ?".to_string());
        parameters.push(SqlValue::Text(end.to_string()));
    }
    if !options.activity_types.is_empty() {
        conditions.push(format!(
            "activity_type IN ({})",
            vec!["?"; options.activity_types.len()].join(", ")
        ));
        parameters.extend(options.activity_types.iter().cloned().map(SqlValue::Text));
    }
    if let Some(source_query) = normalize_text_filter(options.source_query.as_deref()) {
        conditions.push("LOWER(COALESCE(source_name, '')) LIKE ?".to_string());
        parameters.push(SqlValue::Text(format!("%{}%", source_query.to_lowercase())));
    }
    if let Some(value) = options.min_duration_minutes {
        conditions.push("duration_seconds >= ?".to_string());
        parameters.push(SqlValue::Real(value * 60.0));
    }
    if let Some(value) = options.max_duration_minutes {
        conditions.push("duration_seconds <= ?".to_string());
        parameters.push(SqlValue::Real(value * 60.0));
    }
    if conditions.is_empty() {
        Ok((String::new(), parameters))
    } else {
        Ok((format!("WHERE {}", conditions.join(" AND ")), parameters))
    }
}

fn build_effective_workout_filter_clause(
    options: &WorkoutQueryOptions,
) -> Result<(String, Vec<SqlValue>), String> {
    let mut conditions = Vec::new();
    let mut parameters = Vec::new();
    let normalized_location =
        normalize_text_filter(options.location.as_deref()).map(str::to_string);
    let normalized_efforts: Vec<String> = options
        .efforts
        .iter()
        .filter_map(|value| normalize_text_filter(Some(value.as_str())).map(str::to_lowercase))
        .collect();
    validate_numeric_range(
        options.min_distance_miles,
        options.max_distance_miles,
        "Distance",
    )?;
    validate_numeric_range(options.min_energy_kcal, options.max_energy_kcal, "Energy")?;
    validate_numeric_range(
        options.min_avg_heart_rate,
        options.max_avg_heart_rate,
        "Average heart rate",
    )?;
    validate_numeric_range(
        options.min_max_heart_rate,
        options.max_max_heart_rate,
        "Max heart rate",
    )?;
    if let Some(location) = &normalized_location {
        if location != "indoor" && location != "outdoor" {
            return Err("Workout location filter must be one of: indoor, outdoor.".into());
        }
    }
    let invalid_efforts: Vec<String> = normalized_efforts
        .iter()
        .filter(|value| !WORKOUT_EFFORT_LEVELS.contains(&value.as_str()))
        .cloned()
        .collect();
    if !invalid_efforts.is_empty() {
        return Err(format!(
            "Unsupported workout effort filters: {}",
            invalid_efforts.join(", ")
        ));
    }
    match normalized_location.as_deref() {
        Some("indoor") => conditions.push("is_indoor = 1".to_string()),
        Some("outdoor") => conditions.push("is_indoor = 0".to_string()),
        _ => {}
    }
    if let Some(value) = options.min_distance_miles {
        conditions.push("effective_distance_meters >= ?".to_string());
        parameters.push(SqlValue::Real(value * 1609.344));
    }
    if let Some(value) = options.max_distance_miles {
        conditions.push("effective_distance_meters <= ?".to_string());
        parameters.push(SqlValue::Real(value * 1609.344));
    }
    if let Some(value) = options.min_energy_kcal {
        conditions.push("effective_energy_kilocalories >= ?".to_string());
        parameters.push(SqlValue::Real(value));
    }
    if let Some(value) = options.max_energy_kcal {
        conditions.push("effective_energy_kilocalories <= ?".to_string());
        parameters.push(SqlValue::Real(value));
    }
    if let Some(value) = options.min_avg_heart_rate {
        conditions.push("average_heart_rate >= ?".to_string());
        parameters.push(SqlValue::Real(value));
    }
    if let Some(value) = options.max_avg_heart_rate {
        conditions.push("average_heart_rate <= ?".to_string());
        parameters.push(SqlValue::Real(value));
    }
    if let Some(value) = options.min_max_heart_rate {
        conditions.push("maximum_heart_rate >= ?".to_string());
        parameters.push(SqlValue::Real(value));
    }
    if let Some(value) = options.max_max_heart_rate {
        conditions.push("maximum_heart_rate <= ?".to_string());
        parameters.push(SqlValue::Real(value));
    }
    if !normalized_efforts.is_empty() {
        conditions.push(format!(
            "effort IN ({})",
            vec!["?"; normalized_efforts.len()].join(", ")
        ));
        parameters.extend(normalized_efforts.into_iter().map(SqlValue::Text));
    }
    if options.requires_route_data {
        conditions.push("route_count > 0".to_string());
    }
    if options.requires_heart_rate_samples {
        conditions.push("heart_rate_sample_count > 0".to_string());
    }
    if conditions.is_empty() {
        Ok((String::new(), parameters))
    } else {
        Ok((format!("WHERE {}", conditions.join(" AND ")), parameters))
    }
}

fn build_health_record_filter_clause(
    start: Option<&str>,
    end: Option<&str>,
    record_types: &[String],
    source_query: Option<&str>,
) -> (String, Vec<SqlValue>) {
    let mut conditions = vec![
        "wr.record_id IS NULL".to_string(),
        "r.value_numeric IS NOT NULL".to_string(),
    ];
    let mut parameters = Vec::new();
    if let Some(start) = start {
        conditions.push("r.end_date >= ?".to_string());
        parameters.push(SqlValue::Text(start.to_string()));
    }
    if let Some(end) = end {
        conditions.push("r.start_date <= ?".to_string());
        parameters.push(SqlValue::Text(end.to_string()));
    }
    if !record_types.is_empty() {
        conditions.push(format!(
            "r.record_type IN ({})",
            vec!["?"; record_types.len()].join(", ")
        ));
        parameters.extend(record_types.iter().cloned().map(SqlValue::Text));
    }
    if let Some(source_query) = normalize_text_filter(source_query) {
        conditions.push("LOWER(COALESCE(r.source_name, '')) LIKE ?".to_string());
        parameters.push(SqlValue::Text(format!("%{}%", source_query.to_lowercase())));
    }
    (format!("WHERE {}", conditions.join(" AND ")), parameters)
}

fn build_filter_payload(options: &WorkoutQueryOptions) -> Value {
    json!({
        "start": options.start,
        "end": options.end,
        "activity_types": options.activity_types,
        "source_query": normalize_text_filter(options.source_query.as_deref()),
        "min_duration_minutes": options.min_duration_minutes,
        "max_duration_minutes": options.max_duration_minutes,
        "location": normalize_text_filter(options.location.as_deref()),
        "min_distance_miles": options.min_distance_miles,
        "max_distance_miles": options.max_distance_miles,
        "min_energy_kcal": options.min_energy_kcal,
        "max_energy_kcal": options.max_energy_kcal,
        "min_avg_heart_rate": options.min_avg_heart_rate,
        "max_avg_heart_rate": options.max_avg_heart_rate,
        "min_max_heart_rate": options.min_max_heart_rate,
        "max_max_heart_rate": options.max_max_heart_rate,
        "efforts": options
            .efforts
            .iter()
            .filter_map(|value| normalize_text_filter(Some(value.as_str())).map(str::to_string))
            .collect::<Vec<_>>(),
        "requires_route_data": options.requires_route_data,
        "requires_heart_rate_samples": options.requires_heart_rate_samples,
    })
}

fn build_health_filter_payload(options: &HealthQueryOptions) -> Value {
    json!({
        "start": options.start,
        "end": options.end,
        "categories": options.categories,
        "metric_query": normalize_text_filter(options.metric_query.as_deref()),
        "source_query": normalize_text_filter(options.source_query.as_deref()),
        "only_with_samples": options.only_with_samples,
    })
}

fn select_health_overview_metrics(
    categories: &[String],
    metric_query: Option<&str>,
) -> Vec<&'static HealthMetricDefinition> {
    let normalized_categories: Vec<String> = categories
        .iter()
        .filter_map(|value| normalize_text_filter(Some(value.as_str())).map(str::to_lowercase))
        .collect();
    let lowered_query = normalize_text_filter(metric_query).map(str::to_lowercase);
    HEALTH_OVERVIEW_METRICS
        .iter()
        .filter(|metric| {
            (normalized_categories.is_empty()
                || normalized_categories.contains(&metric.category.to_lowercase()))
                && lowered_query.as_ref().map_or(true, |query| {
                    format!(
                        "{} {} {} {}",
                        metric.key, metric.label, metric.record_type, metric.category
                    )
                    .to_lowercase()
                    .contains(query)
                })
        })
        .collect()
}

fn build_summary_timeframe(options: &WorkoutQueryOptions) -> Value {
    json!({
        "start": options.start,
        "end": options.end,
        "activity_types": options
            .activity_types
            .iter()
            .map(|value| humanize_identifier(Some(value.as_str())))
            .collect::<Vec<_>>(),
        "source_query": normalize_text_filter(options.source_query.as_deref()),
        "min_duration_minutes": options.min_duration_minutes,
        "max_duration_minutes": options.max_duration_minutes,
        "location": normalize_text_filter(options.location.as_deref()),
        "min_distance_miles": options.min_distance_miles,
        "max_distance_miles": options.max_distance_miles,
        "min_energy_kcal": options.min_energy_kcal,
        "max_energy_kcal": options.max_energy_kcal,
        "min_avg_heart_rate": options.min_avg_heart_rate,
        "max_avg_heart_rate": options.max_avg_heart_rate,
        "min_max_heart_rate": options.min_max_heart_rate,
        "max_max_heart_rate": options.max_max_heart_rate,
        "efforts": options
            .efforts
            .iter()
            .filter_map(|value| normalize_text_filter(Some(value.as_str())).map(str::to_string))
            .collect::<Vec<_>>(),
        "requires_route_data": options.requires_route_data,
        "requires_heart_rate_samples": options.requires_heart_rate_samples,
    })
}

fn build_summary_overall(workout_rows: &[Map<String, Value>]) -> Map<String, Value> {
    let duration_values = collect_f64(workout_rows, "duration_seconds");
    let distance_values = collect_f64(workout_rows, "effective_distance_meters");
    let energy_values = collect_f64(workout_rows, "effective_energy_kilocalories");
    let heart_rate_values = collect_f64(workout_rows, "average_heart_rate");
    let maximum_heart_rates = collect_f64(workout_rows, "maximum_heart_rate");
    let average_running_cadences = collect_f64(workout_rows, "average_running_cadence");
    let maximum_running_cadences = collect_f64(workout_rows, "maximum_running_cadence");
    map_from_pairs([
        ("workout_count", json!(workout_rows.len())),
        (
            "total_duration_hours",
            round_optional(
                if duration_values.is_empty() {
                    None
                } else {
                    Some(duration_values.iter().sum::<f64>() / 3600.0)
                },
                2,
            ),
        ),
        (
            "average_duration_minutes",
            round_optional(
                if duration_values.is_empty() {
                    None
                } else {
                    Some(duration_values.iter().sum::<f64>() / duration_values.len() as f64 / 60.0)
                },
                1,
            ),
        ),
        (
            "total_distance_miles",
            round_optional(
                if distance_values.is_empty() {
                    None
                } else {
                    Some(distance_values.iter().sum::<f64>() / 1609.344)
                },
                1,
            ),
        ),
        (
            "average_distance_miles",
            round_optional(
                if distance_values.is_empty() {
                    None
                } else {
                    Some(
                        distance_values.iter().sum::<f64>()
                            / distance_values.len() as f64
                            / 1609.344,
                    )
                },
                1,
            ),
        ),
        (
            "total_energy_kcal",
            round_optional(
                if energy_values.is_empty() {
                    None
                } else {
                    Some(energy_values.iter().sum::<f64>())
                },
                0,
            ),
        ),
        (
            "average_energy_kcal",
            round_optional(
                if energy_values.is_empty() {
                    None
                } else {
                    Some(energy_values.iter().sum::<f64>() / energy_values.len() as f64)
                },
                0,
            ),
        ),
        (
            "average_heart_rate",
            round_optional(
                if heart_rate_values.is_empty() {
                    None
                } else {
                    Some(heart_rate_values.iter().sum::<f64>() / heart_rate_values.len() as f64)
                },
                1,
            ),
        ),
        (
            "max_heart_rate",
            round_optional(max_optional(&maximum_heart_rates), 0),
        ),
        (
            "heart_rate_sample_count",
            json!(workout_rows
                .iter()
                .map(|row| object_get_i64(row, "heart_rate_sample_count").unwrap_or(0))
                .sum::<i64>()),
        ),
        (
            "average_running_cadence_spm",
            round_optional(
                if average_running_cadences.is_empty() {
                    None
                } else {
                    Some(
                        average_running_cadences.iter().sum::<f64>()
                            / average_running_cadences.len() as f64,
                    )
                },
                1,
            ),
        ),
        (
            "max_running_cadence_spm",
            round_optional(max_optional(&maximum_running_cadences), 0),
        ),
    ])
}

fn build_activity_breakdown(workout_rows: &[Map<String, Value>]) -> Vec<Map<String, Value>> {
    let mut grouped: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for row in workout_rows {
        if let Some(activity_type) = object_get_str(row, "activity_type") {
            grouped
                .entry(activity_type.to_string())
                .or_default()
                .push(row.clone());
        }
    }
    let mut breakdown = Vec::new();
    for (activity_type, rows) in grouped {
        let durations = collect_f64(&rows, "duration_seconds");
        let distances = collect_f64(&rows, "effective_distance_meters");
        let energies = collect_f64(&rows, "effective_energy_kilocalories");
        let heart_rates = collect_f64(&rows, "average_heart_rate");
        let maximum_heart_rates = collect_f64(&rows, "maximum_heart_rate");
        let average_running_cadences = collect_f64(&rows, "average_running_cadence");
        let maximum_running_cadences = collect_f64(&rows, "maximum_running_cadence");
        breakdown.push(map_from_pairs([
            ("activity_type", json!(activity_type)),
            ("type", json!(humanize_identifier(Some(&activity_type)))),
            ("count", json!(rows.len())),
            (
                "total_duration_hours",
                round_optional(
                    if durations.is_empty() {
                        None
                    } else {
                        Some(durations.iter().sum::<f64>() / 3600.0)
                    },
                    2,
                ),
            ),
            (
                "average_duration_minutes",
                round_optional(
                    if durations.is_empty() {
                        None
                    } else {
                        Some(durations.iter().sum::<f64>() / durations.len() as f64 / 60.0)
                    },
                    1,
                ),
            ),
            (
                "total_distance_miles",
                round_optional(
                    if distances.is_empty() {
                        None
                    } else {
                        Some(distances.iter().sum::<f64>() / 1609.344)
                    },
                    1,
                ),
            ),
            (
                "average_distance_miles",
                round_optional(
                    if distances.is_empty() {
                        None
                    } else {
                        Some(distances.iter().sum::<f64>() / distances.len() as f64 / 1609.344)
                    },
                    1,
                ),
            ),
            (
                "total_energy_kcal",
                round_optional(
                    if energies.is_empty() {
                        None
                    } else {
                        Some(energies.iter().sum::<f64>())
                    },
                    0,
                ),
            ),
            (
                "average_heart_rate",
                round_optional(
                    if heart_rates.is_empty() {
                        None
                    } else {
                        Some(heart_rates.iter().sum::<f64>() / heart_rates.len() as f64)
                    },
                    1,
                ),
            ),
            (
                "max_heart_rate",
                round_optional(max_optional(&maximum_heart_rates), 0),
            ),
            (
                "heart_rate_sample_count",
                json!(rows
                    .iter()
                    .map(|row| object_get_i64(row, "heart_rate_sample_count").unwrap_or(0))
                    .sum::<i64>()),
            ),
            (
                "average_running_cadence_spm",
                round_optional(
                    if average_running_cadences.is_empty() {
                        None
                    } else {
                        Some(
                            average_running_cadences.iter().sum::<f64>()
                                / average_running_cadences.len() as f64,
                        )
                    },
                    1,
                ),
            ),
            (
                "max_running_cadence_spm",
                round_optional(max_optional(&maximum_running_cadences), 0),
            ),
        ]));
    }
    breakdown.sort_by(|left, right| {
        let right_count = object_get_i64(right, "count").unwrap_or(0);
        let left_count = object_get_i64(left, "count").unwrap_or(0);
        right_count
            .cmp(&left_count)
            .then_with(|| object_get_str(left, "type").cmp(&object_get_str(right, "type")))
    });
    breakdown
}

fn build_inspection_overall(workout_rows: &[Map<String, Value>]) -> Map<String, Value> {
    map_from_pairs([
        ("workout_count", json!(workout_rows.len() as i64)),
        (
            "first_start",
            option_string_to_value(
                workout_rows
                    .iter()
                    .filter_map(|row| object_get_str(row, "start_date"))
                    .min()
                    .map(str::to_string),
            ),
        ),
        (
            "last_end",
            option_string_to_value(
                workout_rows
                    .iter()
                    .filter_map(|row| object_get_str(row, "end_date"))
                    .max()
                    .map(str::to_string),
            ),
        ),
        (
            "total_duration_seconds",
            optional_f64_to_value(sum_f64(workout_rows, "duration_seconds")),
        ),
        (
            "total_distance_meters",
            optional_f64_to_value(sum_f64(workout_rows, "effective_distance_meters")),
        ),
        (
            "total_energy_kilocalories",
            optional_f64_to_value(sum_f64(workout_rows, "effective_energy_kilocalories")),
        ),
    ])
}

fn build_inspection_by_activity(workout_rows: &[Map<String, Value>]) -> Vec<Map<String, Value>> {
    let mut grouped: HashMap<String, Vec<&Map<String, Value>>> = HashMap::new();
    for row in workout_rows {
        if let Some(activity_type) = object_get_str(row, "activity_type") {
            grouped
                .entry(activity_type.to_string())
                .or_default()
                .push(row);
        }
    }

    let mut breakdown = grouped
        .into_iter()
        .map(|(activity_type, rows)| {
            map_from_pairs([
                ("activity_type", json!(activity_type)),
                ("workout_count", json!(rows.len() as i64)),
                (
                    "first_start",
                    option_string_to_value(
                        rows.iter()
                            .filter_map(|row| object_get_str(row, "start_date"))
                            .min()
                            .map(str::to_string),
                    ),
                ),
                (
                    "last_end",
                    option_string_to_value(
                        rows.iter()
                            .filter_map(|row| object_get_str(row, "end_date"))
                            .max()
                            .map(str::to_string),
                    ),
                ),
                (
                    "total_duration_seconds",
                    optional_f64_to_value(sum_group_f64(&rows, "duration_seconds")),
                ),
                (
                    "total_distance_meters",
                    optional_f64_to_value(sum_group_f64(&rows, "effective_distance_meters")),
                ),
                (
                    "total_energy_kilocalories",
                    optional_f64_to_value(sum_group_f64(&rows, "effective_energy_kilocalories")),
                ),
            ])
        })
        .collect::<Vec<_>>();

    breakdown.sort_by(|left, right| {
        let right_count = object_get_i64(right, "workout_count").unwrap_or(0);
        let left_count = object_get_i64(left, "workout_count").unwrap_or(0);
        right_count.cmp(&left_count).then_with(|| {
            object_get_str(left, "activity_type").cmp(&object_get_str(right, "activity_type"))
        })
    });
    breakdown
}

fn build_summary_highlights(workout_rows: &[Map<String, Value>]) -> Vec<Value> {
    if workout_rows.is_empty() {
        return Vec::new();
    }
    let mut highlights = Vec::new();
    let mut counts: HashMap<String, usize> = HashMap::new();
    for row in workout_rows {
        if let Some(activity_type) = object_get_str(row, "activity_type") {
            *counts.entry(activity_type.to_string()).or_default() += 1;
        }
    }
    if let Some((most_common_type, most_common_count)) =
        counts.iter().max_by_key(|(_, count)| **count)
    {
        highlights.push(json!(format!(
            "Most frequent activity: {} ({} workouts)",
            humanize_identifier(Some(most_common_type)),
            most_common_count
        )));
    }
    if let Some(longest_workout) = workout_rows.iter().max_by(|left, right| {
        object_get_f64(left, "duration_seconds")
            .partial_cmp(&object_get_f64(right, "duration_seconds"))
            .unwrap_or(std::cmp::Ordering::Equal)
    }) {
        if let Some(duration_seconds) = object_get_f64(longest_workout, "duration_seconds") {
            highlights.push(json!(format!(
                "Longest workout: {}, {} min on {}",
                humanize_identifier(object_get_str(longest_workout, "activity_type")),
                (duration_seconds / 60.0).round() as i64,
                object_get_str(longest_workout, "start_date")
                    .unwrap_or("")
                    .get(..10)
                    .unwrap_or("")
            )));
        }
    }
    if let Some(longest_distance) = workout_rows.iter().max_by(|left, right| {
        object_get_f64(left, "effective_distance_meters")
            .partial_cmp(&object_get_f64(right, "effective_distance_meters"))
            .unwrap_or(std::cmp::Ordering::Equal)
    }) {
        if let Some(distance) = object_get_f64(longest_distance, "effective_distance_meters") {
            highlights.push(json!(format!(
                "Longest distance: {}, {} mi on {}",
                humanize_identifier(object_get_str(longest_distance, "activity_type")),
                round_to_places(distance / 1609.344, 1),
                object_get_str(longest_distance, "start_date")
                    .unwrap_or("")
                    .get(..10)
                    .unwrap_or("")
            )));
        }
    }
    if let Some(highest_energy) = workout_rows.iter().max_by(|left, right| {
        object_get_f64(left, "effective_energy_kilocalories")
            .partial_cmp(&object_get_f64(right, "effective_energy_kilocalories"))
            .unwrap_or(std::cmp::Ordering::Equal)
    }) {
        if let Some(energy) = object_get_f64(highest_energy, "effective_energy_kilocalories") {
            highlights.push(json!(format!(
                "Highest energy burn: {}, {} kcal on {}",
                humanize_identifier(object_get_str(highest_energy, "activity_type")),
                energy.round() as i64,
                object_get_str(highest_energy, "start_date")
                    .unwrap_or("")
                    .get(..10)
                    .unwrap_or("")
            )));
        }
    }
    highlights
}

fn build_summary_workout_card(workout_row: &Map<String, Value>) -> Map<String, Value> {
    let activity_type = object_get_str(workout_row, "activity_type").unwrap_or("Unknown");
    let activity_name = humanize_identifier(Some(activity_type));
    let duration_minutes = round_optional(
        object_get_f64(workout_row, "duration_seconds").map(|value| value / 60.0),
        1,
    );
    let distance_miles = round_optional(
        object_get_f64(workout_row, "effective_distance_meters").map(|value| value / 1609.344),
        1,
    );
    let energy_kcal = round_optional(
        object_get_f64(workout_row, "effective_energy_kilocalories"),
        0,
    );
    let average_heart_rate = round_optional(object_get_f64(workout_row, "average_heart_rate"), 1);
    let maximum_heart_rate = round_optional(object_get_f64(workout_row, "maximum_heart_rate"), 0);
    let average_running_cadence =
        round_optional(object_get_f64(workout_row, "average_running_cadence"), 1);
    let maximum_running_cadence =
        round_optional(object_get_f64(workout_row, "maximum_running_cadence"), 0);
    let elevation_gain_ft = round_optional(object_get_f64(workout_row, "elevation_gain_ft"), 0);
    let temperature_f = round_optional(object_get_f64(workout_row, "temperature_f"), 1);
    let speed_mph = calculate_speed_mph(
        object_get_f64(workout_row, "duration_seconds"),
        object_get_f64(workout_row, "effective_distance_meters"),
    );
    let pace_min_per_mile = calculate_pace_min_per_mile(
        activity_type,
        object_get_f64(workout_row, "duration_seconds"),
        object_get_f64(workout_row, "effective_distance_meters"),
    );
    let effort = classify_effort(
        object_get_f64(workout_row, "duration_seconds"),
        object_get_f64(workout_row, "average_heart_rate"),
        object_get_f64(workout_row, "effective_energy_kilocalories"),
    );
    let title = build_workout_title(
        &activity_name,
        object_get_i64(workout_row, "is_indoor").map(|value| value as i32),
    );
    let summary = build_workout_summary_sentence(
        &title,
        &duration_minutes,
        &distance_miles,
        &elevation_gain_ft,
        &temperature_f,
        &energy_kcal,
        &average_heart_rate,
        effort.as_deref(),
    );
    map_from_pairs([
        (
            "db_id",
            json!(object_get_i64(workout_row, "id").unwrap_or(0)),
        ),
        (
            "date",
            json!(object_get_str(workout_row, "start_date")
                .unwrap_or("")
                .get(..10)
                .unwrap_or("")),
        ),
        (
            "start",
            workout_row
                .get("start_date")
                .cloned()
                .unwrap_or(Value::Null),
        ),
        (
            "end",
            workout_row.get("end_date").cloned().unwrap_or(Value::Null),
        ),
        ("activity_type", json!(activity_type)),
        ("type", json!(activity_name)),
        ("title", json!(title.clone())),
        (
            "location",
            format_workout_location(
                object_get_i64(workout_row, "is_indoor").map(|value| value as i32),
            ),
        ),
        (
            "source",
            workout_row
                .get("source_name")
                .cloned()
                .unwrap_or(Value::Null),
        ),
        ("duration_minutes", duration_minutes.clone()),
        ("distance_miles", distance_miles.clone()),
        ("elevation_gain_ft", elevation_gain_ft),
        ("temperature_f", temperature_f),
        ("energy_kcal", energy_kcal.clone()),
        ("avg_heart_rate", average_heart_rate.clone()),
        ("max_heart_rate", maximum_heart_rate),
        (
            "heart_rate_sample_count",
            json!(object_get_i64(workout_row, "heart_rate_sample_count").unwrap_or(0)),
        ),
        ("avg_running_cadence_spm", average_running_cadence),
        ("max_running_cadence_spm", maximum_running_cadence),
        (
            "pace_min_per_mile",
            optional_f64_to_value(pace_min_per_mile),
        ),
        ("speed_mph", optional_f64_to_value(speed_mph)),
        ("effort", option_string_to_value(effort.clone())),
        ("summary", json!(summary)),
    ])
}

fn build_health_metric_summary(
    metric: &HealthMetricDefinition,
    aggregate_row: Option<&Map<String, Value>>,
    latest_row: Option<&Map<String, Value>>,
    daily_rows: &[Map<String, Value>],
) -> Map<String, Value> {
    let unit = latest_row
        .and_then(|row| object_get_str(row, "unit"))
        .or_else(|| aggregate_row.and_then(|row| object_get_str(row, "unit")));
    let normalized_unit = normalize_health_metric_unit(metric.record_type, unit);
    let latest_value = round_health_metric_value(
        metric.record_type,
        unit,
        latest_row.and_then(|row| object_get_f64(row, "value_numeric")),
        metric.digits,
    );
    let average_value = round_health_metric_value(
        metric.record_type,
        unit,
        aggregate_row.and_then(|row| object_get_f64(row, "average_value")),
        metric.digits,
    );
    let minimum_value = round_health_metric_value(
        metric.record_type,
        unit,
        aggregate_row.and_then(|row| object_get_f64(row, "minimum_value")),
        metric.digits,
    );
    let maximum_value = round_health_metric_value(
        metric.record_type,
        unit,
        aggregate_row.and_then(|row| object_get_f64(row, "maximum_value")),
        metric.digits,
    );
    let total_value = round_health_metric_value(
        metric.record_type,
        unit,
        aggregate_row.and_then(|row| object_get_f64(row, "total_value")),
        metric.digits,
    );
    let trend = build_health_metric_trend(metric, daily_rows);
    let best_day = trend.iter().max_by(|left, right| {
        object_get_f64(left, "value")
            .partial_cmp(&object_get_f64(right, "value"))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let daily_average_value = round_optional(
        if trend.is_empty() {
            None
        } else {
            Some(
                trend
                    .iter()
                    .filter_map(|point| object_get_f64(point, "value"))
                    .sum::<f64>()
                    / trend.len() as f64,
            )
        },
        metric.digits,
    );
    let (primary_label, primary_value, trend_aggregation) = if metric.summary_kind == "total" {
        ("Range total", total_value.clone(), "daily_total")
    } else {
        ("Latest", latest_value.clone(), "daily_average")
    };
    map_from_pairs([
        ("key", json!(metric.key)),
        ("label", json!(metric.label)),
        ("category", json!(metric.category)),
        ("record_type", json!(metric.record_type)),
        ("summary_kind", json!(metric.summary_kind)),
        ("snapshot", json!(metric.snapshot)),
        ("unit", option_string_to_value(normalized_unit)),
        (
            "sample_count",
            json!(aggregate_row
                .and_then(|row| object_get_i64(row, "sample_count"))
                .unwrap_or(0)),
        ),
        ("primary_label", json!(primary_label)),
        ("primary_value", primary_value),
        ("latest_value", latest_value),
        (
            "latest_at",
            latest_row
                .and_then(|row| row.get("end_date").cloned())
                .unwrap_or(Value::Null),
        ),
        ("average_value", average_value),
        ("minimum_value", minimum_value),
        ("maximum_value", maximum_value),
        ("total_value", total_value),
        ("daily_average_value", daily_average_value),
        (
            "best_day",
            option_string_to_value(
                best_day
                    .and_then(|point| object_get_str(point, "date"))
                    .map(str::to_string),
            ),
        ),
        (
            "best_day_value",
            if let Some(point) = best_day {
                round_optional(object_get_f64(point, "value"), metric.digits)
            } else {
                Value::Null
            },
        ),
        ("trend_aggregation", json!(trend_aggregation)),
        ("trend", Value::Array(objects_to_values(trend))),
    ])
}

fn build_health_metric_trend(
    metric: &HealthMetricDefinition,
    daily_rows: &[Map<String, Value>],
) -> Vec<Map<String, Value>> {
    let mut points = Vec::new();
    for row in daily_rows {
        let source_value = if metric.summary_kind == "total" {
            object_get_f64(row, "total_value")
        } else {
            object_get_f64(row, "average_value")
        };
        let value = round_health_metric_value(
            metric.record_type,
            object_get_str(row, "unit"),
            source_value,
            metric.digits,
        );
        if value.is_null() {
            continue;
        }
        points.push(map_from_pairs([
            ("date", json!(object_get_str(row, "day").unwrap_or(""))),
            ("value", value),
        ]));
    }
    points
}

fn derive_workout_metrics(
    workout_row: &Map<String, Value>,
    metadata: &[Value],
    statistics: &[Value],
    records: &[Value],
) -> Map<String, Value> {
    let mut metrics = Map::new();
    metrics.insert("associated_record_count".into(), json!(records.len()));
    let duration_seconds = object_get_f64(workout_row, "duration_seconds");
    let distance_meters = object_get_f64(workout_row, "total_distance_meters");
    if let (Some(duration_seconds), Some(distance_meters)) = (duration_seconds, distance_meters) {
        let hours = duration_seconds / 3600.0;
        let kilometers = distance_meters / 1000.0;
        if hours > 0.0 {
            metrics.insert("speed_kph".into(), json!(kilometers / hours));
        }
        if kilometers > 0.0 {
            metrics.insert(
                "pace_seconds_per_km".into(),
                json!(duration_seconds / kilometers),
            );
        }
    }

    let mut record_type_counts: HashMap<String, i64> = HashMap::new();
    let mut heart_rate_samples = Vec::new();
    for record in records {
        if let Some(record) = record.as_object() {
            if let Some(record_type) = object_get_str(record, "record_type") {
                *record_type_counts
                    .entry(record_type.to_string())
                    .or_default() += 1;
                if record_type == HEART_RATE_STATISTIC_TYPE {
                    if let Some(value) = object_get_f64(record, "value_numeric") {
                        heart_rate_samples.push(value);
                    }
                }
            }
        }
    }
    if !record_type_counts.is_empty() {
        let mut counts = Map::new();
        let mut keys: Vec<String> = record_type_counts.keys().cloned().collect();
        keys.sort();
        for key in keys {
            counts.insert(key.clone(), json!(record_type_counts[&key]));
        }
        metrics.insert("record_type_counts".into(), Value::Object(counts));
    }
    if !heart_rate_samples.is_empty() {
        metrics.insert(
            "heart_rate".into(),
            json!({
                "sample_count": heart_rate_samples.len(),
                "average": heart_rate_samples.iter().sum::<f64>() / heart_rate_samples.len() as f64,
                "minimum": heart_rate_samples.iter().copied().fold(f64::INFINITY, f64::min),
                "maximum": heart_rate_samples.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            }),
        );
    }
    if object_get_str(workout_row, "activity_type") == Some(RUNNING_ACTIVITY_TYPE) {
        let step_count_total =
            extract_workout_statistic_value(statistics, STEP_COUNT_STATISTIC_TYPE, "sum");
        let running_cadence_average =
            calculate_running_cadence_from_step_count(duration_seconds, step_count_total);
        let running_cadence_maximum = calculate_max_running_cadence_from_step_records(
            object_get_str(workout_row, "start_date").unwrap_or(""),
            object_get_str(workout_row, "end_date").unwrap_or(""),
            records,
        );
        if running_cadence_average.is_some() || running_cadence_maximum.is_some() {
            metrics.insert(
                "running_cadence".into(),
                json!({
                    "average": running_cadence_average,
                    "maximum": running_cadence_maximum,
                }),
            );
        }
    }
    if let Some(elevation_gain_ft) = extract_workout_elevation_gain_ft(metadata) {
        metrics.insert("elevation_gain_ft".into(), json!(elevation_gain_ft));
    }
    if let Some(temperature_f) = extract_workout_temperature_f(metadata) {
        metrics.insert("temperature_f".into(), json!(temperature_f));
    }
    metrics
}

fn extract_workout_elevation_gain_ft(metadata: &[Value]) -> Option<f64> {
    let (value, unit) =
        extract_workout_metadata_measurement(metadata, ELEVATION_ASCENDED_METADATA_KEY)?;
    let meters = value * lookup_measurement_multiplier(unit, DISTANCE_TO_METERS)?;
    Some(round_to_places(meters / 0.3048, 1))
}

fn extract_workout_temperature_f(metadata: &[Value]) -> Option<f64> {
    let (value, unit) =
        extract_workout_metadata_measurement(metadata, WEATHER_TEMPERATURE_METADATA_KEY)?;
    if unit.eq_ignore_ascii_case("degf") || unit.eq_ignore_ascii_case("f") {
        return Some(round_to_places(value, 1));
    }
    if unit.eq_ignore_ascii_case("degc") || unit.eq_ignore_ascii_case("c") {
        return Some(round_to_places((value * 9.0 / 5.0) + 32.0, 1));
    }
    None
}

fn extract_workout_metadata_measurement<'a>(
    metadata: &'a [Value],
    key: &str,
) -> Option<(f64, &'a str)> {
    let raw_value = metadata
        .iter()
        .filter_map(Value::as_object)
        .find_map(|entry| {
            if object_get_str(entry, "key") == Some(key) {
                entry.get("value").and_then(Value::as_str)
            } else {
                None
            }
        })?;
    parse_measurement_text(raw_value)
}

fn parse_measurement_text(value: &str) -> Option<(f64, &str)> {
    let trimmed = value.trim();
    let numeric_end = trimmed
        .find(|character: char| {
            !(character.is_ascii_digit() || matches!(character, '.' | '-' | '+'))
        })
        .unwrap_or(trimmed.len());
    if numeric_end == 0 {
        return None;
    }
    let numeric_value = trimmed.get(..numeric_end)?.parse::<f64>().ok()?;
    let unit = trimmed.get(numeric_end..)?.trim();
    if unit.is_empty() {
        None
    } else {
        Some((numeric_value, unit))
    }
}

fn calculate_running_cadence_from_step_count(
    duration_seconds: Option<f64>,
    step_count: Option<f64>,
) -> Option<f64> {
    match (duration_seconds, step_count) {
        (Some(duration_seconds), Some(step_count)) if duration_seconds > 0.0 => {
            Some((step_count * 60.0) / duration_seconds)
        }
        _ => None,
    }
}

fn calculate_max_running_cadence_from_step_records(
    workout_start: &str,
    workout_end: &str,
    records: &[Value],
) -> Option<f64> {
    records
        .iter()
        .filter_map(|record| record.as_object())
        .filter(|record| {
            object_get_str(record, "record_type") == Some(STEP_COUNT_STATISTIC_TYPE)
                && object_get_f64(record, "value_numeric").is_some()
                && object_get_str(record, "start_date").unwrap_or("") >= workout_start
                && object_get_str(record, "end_date").unwrap_or("") <= workout_end
        })
        .filter_map(calculate_running_cadence_from_step_record)
        .max_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
}

fn calculate_running_cadence_from_step_record(record: &Map<String, Value>) -> Option<f64> {
    let duration_seconds = calculate_record_duration_seconds(record)?;
    let value_numeric = object_get_f64(record, "value_numeric")?;
    Some((value_numeric * 60.0) / duration_seconds)
}

fn calculate_record_duration_seconds(record: &Map<String, Value>) -> Option<f64> {
    let start_date = object_get_str(record, "start_date")?;
    let end_date = object_get_str(record, "end_date")?;
    let start = DateTime::parse_from_rfc3339(start_date).ok()?;
    let end = DateTime::parse_from_rfc3339(end_date).ok()?;
    let duration = (end - start).num_seconds() as f64;
    if duration > 0.0 {
        Some(duration)
    } else {
        None
    }
}

fn extract_workout_statistic_value(
    statistics: &[Value],
    statistic_type: &str,
    aggregation: &str,
) -> Option<f64> {
    statistics.iter().find_map(|statistic| {
        let statistic = statistic.as_object()?;
        if object_get_str(statistic, "statistic_type") == Some(statistic_type)
            && object_get_str(statistic, "aggregation") == Some(aggregation)
        {
            object_get_f64(statistic, "value")
        } else {
            None
        }
    })
}

fn resolve_json_output_path(output_path: &Path, summary: bool, bundle: &Value) -> PathBuf {
    if output_path.exists() && output_path.is_dir() {
        return output_path.join(build_json_export_filename(summary, bundle));
    }
    if output_path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.eq_ignore_ascii_case("json"))
        .unwrap_or(false)
    {
        return output_path.to_path_buf();
    }
    if output_path.exists() && output_path.is_file() {
        return output_path.to_path_buf();
    }
    output_path.join(build_json_export_filename(summary, bundle))
}

fn resolve_csv_output_directory(output_path: &Path) -> Result<PathBuf, String> {
    if output_path.exists() && output_path.is_file() {
        return Err("CSV export output must be a directory path, not a file.".into());
    }
    Ok(output_path.to_path_buf())
}

fn write_csv_exports(
    bundle: &Value,
    destination_dir: &Path,
    csv_profile: &str,
    reporter: &mut dyn ProgressReporter,
) -> Result<(), String> {
    let workouts = bundle
        .get("workouts")
        .and_then(Value::as_array)
        .ok_or_else(|| "Export bundle is missing workouts.".to_string())?;
    let filenames = build_csv_export_filenames(csv_profile, bundle);
    if csv_profile == "llm" {
        write_csv_file(
            &destination_dir.join(&filenames["workouts_llm"]),
            &LLM_WORKOUT_CSV_FIELDS,
            workouts
                .iter()
                .filter_map(|workout| workout.as_object())
                .map(|workout| {
                    vec![
                        value_to_csv_string(workout.get("db_id")),
                        value_to_csv_string(workout.get("date")),
                        value_to_csv_string(workout.get("activity_type")),
                        value_to_csv_string(workout.get("type")),
                        value_to_csv_string(workout.get("location")),
                        value_to_csv_string(workout.get("duration_minutes")),
                        value_to_csv_string(workout.get("distance_miles")),
                        value_to_csv_string(workout.get("elevation_gain_ft")),
                        value_to_csv_string(workout.get("temperature_f")),
                        value_to_csv_string(workout.get("pace_min_per_mile")),
                        value_to_csv_string(workout.get("avg_heart_rate")),
                        value_to_csv_string(workout.get("max_heart_rate")),
                        value_to_csv_string(workout.get("heart_rate_sample_count")),
                        value_to_csv_string(workout.get("avg_running_cadence_spm")),
                        value_to_csv_string(workout.get("max_running_cadence_spm")),
                        value_to_csv_string(workout.get("energy_kcal")),
                        value_to_csv_string(workout.get("effort")),
                    ]
                }),
            reporter,
        )?;
        return Ok(());
    }
    if csv_profile != "full" {
        return Err(format!("Unsupported CSV export profile: {csv_profile}"));
    }
    write_csv_file(
        &destination_dir.join(&filenames["workouts"]),
        &[
            "db_id",
            "uuid",
            "activity_type",
            "source_name",
            "source_version",
            "device",
            "creation_date",
            "start_date",
            "end_date",
            "duration_seconds",
            "total_distance",
            "total_distance_unit",
            "total_distance_meters",
            "total_energy_burned",
            "total_energy_burned_unit",
            "total_energy_burned_kilocalories",
            "derived_metrics_json",
            "raw_attributes_json",
        ],
        workouts
            .iter()
            .filter_map(|workout| workout.as_object())
            .map(|workout| {
                vec![
                    value_to_csv_string(workout.get("db_id")),
                    value_to_csv_string(workout.get("uuid")),
                    value_to_csv_string(workout.get("activity_type")),
                    value_to_csv_string(workout.get("source_name")),
                    value_to_csv_string(workout.get("source_version")),
                    value_to_csv_string(workout.get("device")),
                    value_to_csv_string(workout.get("creation_date")),
                    value_to_csv_string(workout.get("start_date")),
                    value_to_csv_string(workout.get("end_date")),
                    value_to_csv_string(workout.get("duration_seconds")),
                    value_to_csv_string(workout.get("total_distance")),
                    value_to_csv_string(workout.get("total_distance_unit")),
                    value_to_csv_string(workout.get("total_distance_meters")),
                    value_to_csv_string(workout.get("total_energy_burned")),
                    value_to_csv_string(workout.get("total_energy_burned_unit")),
                    value_to_csv_string(workout.get("total_energy_burned_kilocalories")),
                    json_to_string(workout.get("derived_metrics").unwrap_or(&Value::Null)),
                    json_to_string(workout.get("raw_attributes").unwrap_or(&Value::Null)),
                ]
            }),
        reporter,
    )?;
    write_csv_file(
        &destination_dir.join(&filenames["workout_metadata"]),
        &["workout_db_id", "workout_uuid", "key", "value"],
        workouts
            .iter()
            .filter_map(|workout| workout.as_object())
            .flat_map(|workout| {
                workout
                    .get("metadata")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flat_map(move |entries| {
                        entries
                            .iter()
                            .filter_map(Value::as_object)
                            .map(move |entry| {
                                vec![
                                    value_to_csv_string(workout.get("db_id")),
                                    value_to_csv_string(workout.get("uuid")),
                                    value_to_csv_string(entry.get("key")),
                                    value_to_csv_string(entry.get("value")),
                                ]
                            })
                    })
            }),
        reporter,
    )?;
    write_csv_file(
        &destination_dir.join(&filenames["workout_events"]),
        &[
            "workout_db_id",
            "workout_uuid",
            "event_type",
            "event_date",
            "duration_seconds",
            "duration_unit",
            "raw_attributes_json",
        ],
        workouts
            .iter()
            .filter_map(|workout| workout.as_object())
            .flat_map(|workout| {
                workout
                    .get("events")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flat_map(move |events| {
                        events
                            .iter()
                            .filter_map(Value::as_object)
                            .map(move |event| {
                                vec![
                                    value_to_csv_string(workout.get("db_id")),
                                    value_to_csv_string(workout.get("uuid")),
                                    value_to_csv_string(event.get("event_type")),
                                    value_to_csv_string(event.get("event_date")),
                                    value_to_csv_string(event.get("duration_seconds")),
                                    value_to_csv_string(event.get("duration_unit")),
                                    json_to_string(
                                        event.get("raw_attributes").unwrap_or(&Value::Null),
                                    ),
                                ]
                            })
                    })
            }),
        reporter,
    )?;
    write_csv_file(
        &destination_dir.join(&filenames["workout_statistics"]),
        &[
            "workout_db_id",
            "workout_uuid",
            "statistic_type",
            "start_date",
            "end_date",
            "unit",
            "aggregation",
            "value",
            "raw_attributes_json",
        ],
        workouts
            .iter()
            .filter_map(|workout| workout.as_object())
            .flat_map(|workout| {
                workout
                    .get("statistics")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flat_map(move |statistics| {
                        statistics
                            .iter()
                            .filter_map(Value::as_object)
                            .map(move |statistic| {
                                vec![
                                    value_to_csv_string(workout.get("db_id")),
                                    value_to_csv_string(workout.get("uuid")),
                                    value_to_csv_string(statistic.get("statistic_type")),
                                    value_to_csv_string(statistic.get("start_date")),
                                    value_to_csv_string(statistic.get("end_date")),
                                    value_to_csv_string(statistic.get("unit")),
                                    value_to_csv_string(statistic.get("aggregation")),
                                    value_to_csv_string(statistic.get("value")),
                                    json_to_string(
                                        statistic.get("raw_attributes").unwrap_or(&Value::Null),
                                    ),
                                ]
                            })
                    })
            }),
        reporter,
    )?;
    write_csv_file(
        &destination_dir.join(&filenames["workout_routes"]),
        &[
            "workout_db_id",
            "workout_uuid",
            "route_type",
            "source_name",
            "source_version",
            "device",
            "creation_date",
            "start_date",
            "end_date",
            "metadata_json",
            "raw_attributes_json",
        ],
        workouts
            .iter()
            .filter_map(|workout| workout.as_object())
            .flat_map(|workout| {
                workout
                    .get("routes")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flat_map(move |routes| {
                        routes
                            .iter()
                            .filter_map(Value::as_object)
                            .map(move |route| {
                                vec![
                                    value_to_csv_string(workout.get("db_id")),
                                    value_to_csv_string(workout.get("uuid")),
                                    value_to_csv_string(route.get("route_type")),
                                    value_to_csv_string(route.get("source_name")),
                                    value_to_csv_string(route.get("source_version")),
                                    value_to_csv_string(route.get("device")),
                                    value_to_csv_string(route.get("creation_date")),
                                    value_to_csv_string(route.get("start_date")),
                                    value_to_csv_string(route.get("end_date")),
                                    json_to_string(route.get("metadata").unwrap_or(&Value::Null)),
                                    json_to_string(
                                        route.get("raw_attributes").unwrap_or(&Value::Null),
                                    ),
                                ]
                            })
                    })
            }),
        reporter,
    )?;
    write_csv_file(
        &destination_dir.join(&filenames["workout_records"]),
        &[
            "workout_db_id",
            "workout_uuid",
            "record_type",
            "record_uuid",
            "source_name",
            "source_version",
            "unit",
            "value_text",
            "value_numeric",
            "device",
            "creation_date",
            "start_date",
            "end_date",
            "metadata_json",
            "raw_attributes_json",
        ],
        workouts
            .iter()
            .filter_map(|workout| workout.as_object())
            .flat_map(|workout| {
                workout
                    .get("records")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flat_map(move |records| {
                        records
                            .iter()
                            .filter_map(Value::as_object)
                            .map(move |record| {
                                vec![
                                    value_to_csv_string(workout.get("db_id")),
                                    value_to_csv_string(workout.get("uuid")),
                                    value_to_csv_string(record.get("record_type")),
                                    value_to_csv_string(record.get("uuid")),
                                    value_to_csv_string(record.get("source_name")),
                                    value_to_csv_string(record.get("source_version")),
                                    value_to_csv_string(record.get("unit")),
                                    value_to_csv_string(record.get("value_text")),
                                    value_to_csv_string(record.get("value_numeric")),
                                    value_to_csv_string(record.get("device")),
                                    value_to_csv_string(record.get("creation_date")),
                                    value_to_csv_string(record.get("start_date")),
                                    value_to_csv_string(record.get("end_date")),
                                    json_to_string(record.get("metadata").unwrap_or(&Value::Null)),
                                    json_to_string(
                                        record.get("raw_attributes").unwrap_or(&Value::Null),
                                    ),
                                ]
                            })
                    })
            }),
        reporter,
    )?;
    Ok(())
}

fn write_csv_file<I>(
    path: &Path,
    headers: &[&str],
    rows: I,
    reporter: &mut dyn ProgressReporter,
) -> Result<(), String>
where
    I: IntoIterator<Item = Vec<String>>,
{
    let file = File::create(path).map_err(|error| error.to_string())?;
    let mut writer = Writer::from_writer(file);
    writer
        .write_record(headers)
        .map_err(|error| error.to_string())?;
    let mut row_count = 0usize;
    for row in rows {
        writer
            .write_record(row)
            .map_err(|error| error.to_string())?;
        row_count += 1;
    }
    writer.flush().map_err(|error| error.to_string())?;
    reporter.verbose(&format!("Wrote {row_count} rows to {}", path.display()))?;
    Ok(())
}

fn build_summary_stem(bundle: &Value) -> String {
    if let Some(breakdown) = bundle.get("activity_breakdown").and_then(Value::as_array) {
        if breakdown.len() == 1 {
            if let Some(workout_type) = breakdown[0].as_object().and_then(|obj| object_get_str(obj, "type")) {
                let safe_type = workout_type.replace(" ", "-").to_lowercase();
                return format!("workouts-summary-{safe_type}");
            }
        }
    }
    "workouts-summary".to_string()
}

fn build_csv_export_filenames(csv_profile: &str, bundle: &Value) -> HashMap<&'static str, String> {
    let suffix = build_export_date_range_label(bundle);
    if csv_profile == "llm" {
        let stem = build_summary_stem(bundle);
        return HashMap::from([("workouts_llm", format!("{stem}-{suffix}.csv"))]);
    }
    HashMap::from([
        ("workouts", format!("workouts-{suffix}.csv")),
        ("workout_metadata", format!("workout_metadata-{suffix}.csv")),
        ("workout_events", format!("workout_events-{suffix}.csv")),
        (
            "workout_statistics",
            format!("workout_statistics-{suffix}.csv"),
        ),
        ("workout_routes", format!("workout_routes-{suffix}.csv")),
        ("workout_records", format!("workout_records-{suffix}.csv")),
    ])
}

fn build_json_export_filename(summary: bool, bundle: &Value) -> String {
    let stem = if summary {
        build_summary_stem(bundle)
    } else {
        "workouts".to_string()
    };
    format!("{stem}-{}.json", build_export_date_range_label(bundle))
}

fn build_export_date_range_label(bundle: &Value) -> String {
    if let Some((start, end)) = extract_bundle_workout_date_range(bundle) {
        return format!("{start}_to_{end}");
    }
    if let Some(filters) = bundle.get("filters").and_then(Value::as_object) {
        let start = normalize_export_date_value(filters.get("start"));
        let end = normalize_export_date_value(filters.get("end"));
        return match (start, end) {
            (Some(start), Some(end)) => format!("{start}_to_{end}"),
            (Some(start), None) => format!("from_{start}"),
            (None, Some(end)) => format!("through_{end}"),
            _ => "all_dates".into(),
        };
    }
    "all_dates".into()
}

fn extract_bundle_workout_date_range(bundle: &Value) -> Option<(String, String)> {
    let workouts = bundle.get("workouts")?.as_array()?;
    if workouts.is_empty() {
        return None;
    }
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for workout in workouts {
        let workout = workout.as_object()?;
        if let Some(start) = normalize_export_date_value(workout.get("start_date"))
            .or_else(|| normalize_export_date_value(workout.get("start")))
            .or_else(|| normalize_export_date_value(workout.get("date")))
        {
            starts.push(start);
        }
        if let Some(end) = normalize_export_date_value(workout.get("end_date"))
            .or_else(|| normalize_export_date_value(workout.get("end")))
            .or_else(|| normalize_export_date_value(workout.get("date")))
        {
            ends.push(end);
        }
    }
    if starts.is_empty() || ends.is_empty() {
        None
    } else {
        starts.sort();
        ends.sort();
        Some((starts.first()?.clone(), ends.last()?.clone()))
    }
}

fn normalize_export_date_value(value: Option<&Value>) -> Option<String> {
    let candidate = value?.as_str()?.trim();
    if candidate.len() >= 10
        && candidate.as_bytes().get(4) == Some(&b'-')
        && candidate.as_bytes().get(7) == Some(&b'-')
    {
        Some(candidate[..10].to_string())
    } else {
        None
    }
}

fn combine_where_clauses(primary: &str, secondary: &str) -> String {
    match (primary.trim(), secondary.trim()) {
        ("", "") => String::new(),
        ("", secondary) => secondary.to_string(),
        (primary, "") => primary.to_string(),
        (primary, secondary) => format!("{primary} AND {}", secondary.trim_start_matches("WHERE ")),
    }
}

fn ensure_effective_workouts_cache(connection: &Connection) -> Result<(), String> {
    if !table_exists(connection, "effective_workouts")? {
        connection
            .execute_batch(&effective_workout_table_sql())
            .map_err(|error| error.to_string())?;
        refresh_effective_workouts(connection, &mut NoopReporter)?;
        connection
            .execute_batch(&post_link_index_sql())
            .map_err(|error| error.to_string())?;
        return Ok(());
    }

    let workout_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM workouts", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    let cached_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM effective_workouts", [], |row| {
            row.get(0)
        })
        .map_err(|error| error.to_string())?;
    if workout_count != cached_count {
        refresh_effective_workouts(connection, &mut NoopReporter)?;
    }
    Ok(())
}

fn table_exists(connection: &Connection, table_name: &str) -> Result<bool, String> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
            [table_name],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| value != 0)
        .map_err(|error| error.to_string())
}

fn create_post_import_indexes(
    connection: &Connection,
    reporter: &mut dyn ProgressReporter,
) -> Result<(), String> {
    reporter.progress("Finalizing SQLite indexes...")?;
    connection
        .execute_batch(&post_import_index_sql())
        .map_err(|error| error.to_string())?;
    reporter.verbose("Created post-import SQLite indexes.")?;
    Ok(())
}

fn create_post_link_indexes(
    connection: &Connection,
    reporter: &mut dyn ProgressReporter,
) -> Result<(), String> {
    connection
        .execute_batch(&post_link_index_sql())
        .map_err(|error| error.to_string())?;
    reporter.verbose("Created workout cache and relationship indexes.")?;
    Ok(())
}

fn refresh_effective_workouts(
    connection: &Connection,
    reporter: &mut dyn ProgressReporter,
) -> Result<(), String> {
    reporter.progress("Caching derived workout metrics...")?;
    connection
        .execute_batch(&effective_workouts_refresh_sql())
        .map_err(|error| error.to_string())?;
    reporter.verbose("Refreshed effective workout cache.")?;
    Ok(())
}

fn effective_workouts_refresh_sql() -> String {
    format!(
        "
        DELETE FROM effective_workouts;
        WITH workout_stat_fallbacks AS (
            SELECT
                ws.workout_id,
                SUM(
                    CASE
                        WHEN ws.aggregation = 'sum'
                         AND ws.statistic_type LIKE '{DISTANCE_STATISTIC_TYPE_PATTERN}'
                        THEN {distance_value_sql}
                    END
                ) AS fallback_distance_meters,
                SUM(
                    CASE
                        WHEN ws.aggregation = 'sum'
                         AND ws.statistic_type = '{ACTIVE_ENERGY_STATISTIC_TYPE}'
                        THEN {energy_value_sql}
                    END
                ) AS fallback_energy_kilocalories
            FROM workout_statistics ws
            GROUP BY ws.workout_id
        ),
        heart_rate_records AS (
            SELECT
                wr.workout_id,
                COUNT(*) AS sample_count,
                AVG(r.value_numeric) AS average_heart_rate,
                MIN(r.value_numeric) AS minimum_heart_rate,
                MAX(r.value_numeric) AS maximum_heart_rate
            FROM workout_records wr
            JOIN records r ON r.id = wr.record_id
            WHERE r.record_type = '{HEART_RATE_STATISTIC_TYPE}'
              AND r.value_numeric IS NOT NULL
            GROUP BY wr.workout_id
        ),
        heart_rate_stats AS (
            SELECT
                ws.workout_id,
                MAX(CASE WHEN ws.aggregation = 'average' THEN ws.value END) AS average_heart_rate,
                MAX(CASE WHEN ws.aggregation = 'maximum' THEN ws.value END) AS maximum_heart_rate
            FROM workout_statistics ws
            WHERE ws.statistic_type = '{HEART_RATE_STATISTIC_TYPE}'
            GROUP BY ws.workout_id
        ),
        step_count_stats AS (
            SELECT
                ws.workout_id,
                MAX(CASE WHEN ws.aggregation = 'sum' THEN ws.value END) AS total_step_count
            FROM workout_statistics ws
            WHERE ws.statistic_type = '{STEP_COUNT_STATISTIC_TYPE}'
            GROUP BY ws.workout_id
        ),
        step_count_record_cadence AS (
            SELECT
                wr.workout_id,
                MAX(
                    (r.value_numeric * 60.0) / ((julianday(r.end_date) - julianday(r.start_date)) * 86400.0)
                ) AS maximum_running_cadence
            FROM workout_records wr
            JOIN workouts w ON w.id = wr.workout_id
            JOIN records r ON r.id = wr.record_id
            WHERE w.activity_type = '{RUNNING_ACTIVITY_TYPE}'
              AND r.record_type = '{STEP_COUNT_STATISTIC_TYPE}'
              AND r.value_numeric IS NOT NULL
              AND r.start_date >= w.start_date
              AND r.end_date <= w.end_date
              AND ((julianday(r.end_date) - julianday(r.start_date)) * 86400.0) > 0
            GROUP BY wr.workout_id
        ),
        workout_location AS (
            SELECT
                wm.workout_id,
                MAX(
                    CASE
                        WHEN wm.key = 'HKIndoorWorkout' AND wm.value = '1' THEN 1
                        WHEN wm.key = 'HKIndoorWorkout' AND wm.value = '0' THEN 0
                    END
                ) AS is_indoor
            FROM workout_metadata wm
            GROUP BY wm.workout_id
        ),
        workout_route_counts AS (
            SELECT
                wr.workout_id,
                COUNT(*) AS route_count
            FROM workout_routes wr
            GROUP BY wr.workout_id
        ),
        enriched_workouts AS (
            SELECT
                w.*,
                COALESCE(w.total_distance_meters, workout_stat_fallbacks.fallback_distance_meters) AS effective_distance_meters,
                COALESCE(
                    w.total_energy_burned_kilocalories,
                    workout_stat_fallbacks.fallback_energy_kilocalories
                ) AS effective_energy_kilocalories,
                COALESCE(heart_rate_stats.average_heart_rate, heart_rate_records.average_heart_rate) AS average_heart_rate,
                heart_rate_records.minimum_heart_rate AS minimum_heart_rate,
                COALESCE(heart_rate_stats.maximum_heart_rate, heart_rate_records.maximum_heart_rate) AS maximum_heart_rate,
                COALESCE(heart_rate_records.sample_count, 0) AS heart_rate_sample_count,
                CASE
                    WHEN w.activity_type = '{RUNNING_ACTIVITY_TYPE}'
                     AND w.duration_seconds IS NOT NULL
                     AND w.duration_seconds > 0
                     AND step_count_stats.total_step_count IS NOT NULL
                    THEN (step_count_stats.total_step_count * 60.0) / w.duration_seconds
                END AS average_running_cadence,
                step_count_record_cadence.maximum_running_cadence AS maximum_running_cadence,
                workout_location.is_indoor AS is_indoor,
                COALESCE(workout_route_counts.route_count, 0) AS route_count
            FROM workouts w
            LEFT JOIN workout_stat_fallbacks ON workout_stat_fallbacks.workout_id = w.id
            LEFT JOIN heart_rate_records ON heart_rate_records.workout_id = w.id
            LEFT JOIN heart_rate_stats ON heart_rate_stats.workout_id = w.id
            LEFT JOIN step_count_stats ON step_count_stats.workout_id = w.id
            LEFT JOIN step_count_record_cadence ON step_count_record_cadence.workout_id = w.id
            LEFT JOIN workout_location ON workout_location.workout_id = w.id
            LEFT JOIN workout_route_counts ON workout_route_counts.workout_id = w.id
        )
        INSERT INTO effective_workouts (
            id,
            uuid,
            activity_type,
            source_name,
            source_version,
            device,
            creation_date,
            start_date,
            end_date,
            duration_seconds,
            total_distance,
            total_distance_unit,
            total_distance_meters,
            total_energy_burned,
            total_energy_burned_unit,
            total_energy_burned_kilocalories,
            raw_attributes,
            effective_distance_meters,
            effective_energy_kilocalories,
            average_heart_rate,
            minimum_heart_rate,
            maximum_heart_rate,
            heart_rate_sample_count,
            average_running_cadence,
            maximum_running_cadence,
            is_indoor,
            route_count,
            effort
        )
        SELECT
            ew.id,
            ew.uuid,
            ew.activity_type,
            ew.source_name,
            ew.source_version,
            ew.device,
            ew.creation_date,
            ew.start_date,
            ew.end_date,
            ew.duration_seconds,
            ew.total_distance,
            ew.total_distance_unit,
            ew.total_distance_meters,
            ew.total_energy_burned,
            ew.total_energy_burned_unit,
            ew.total_energy_burned_kilocalories,
            ew.raw_attributes,
            ew.effective_distance_meters,
            ew.effective_energy_kilocalories,
            ew.average_heart_rate,
            ew.minimum_heart_rate,
            ew.maximum_heart_rate,
            ew.heart_rate_sample_count,
            ew.average_running_cadence,
            ew.maximum_running_cadence,
            ew.is_indoor,
            ew.route_count,
            CASE
                WHEN ew.average_heart_rate IS NOT NULL THEN
                    CASE
                        WHEN ew.average_heart_rate >= 165 THEN 'very hard'
                        WHEN ew.average_heart_rate >= 150 THEN 'hard'
                        WHEN ew.average_heart_rate >= 135 THEN 'moderate'
                        WHEN ew.average_heart_rate >= 120 THEN 'easy-moderate'
                        ELSE 'easy'
                    END
                WHEN ew.effective_energy_kilocalories IS NULL
                     OR ew.duration_seconds IS NULL
                     OR ew.duration_seconds <= 0 THEN NULL
                WHEN (ew.effective_energy_kilocalories / (ew.duration_seconds / 3600.0)) >= 900 THEN 'very hard'
                WHEN (ew.effective_energy_kilocalories / (ew.duration_seconds / 3600.0)) >= 700 THEN 'hard'
                WHEN (ew.effective_energy_kilocalories / (ew.duration_seconds / 3600.0)) >= 450 THEN 'moderate'
                WHEN (ew.effective_energy_kilocalories / (ew.duration_seconds / 3600.0)) >= 250 THEN 'easy-moderate'
                ELSE 'easy'
            END AS effort
        FROM enriched_workouts ew;
        ",
        distance_value_sql = build_unit_conversion_case("ws.value", "ws.unit", DISTANCE_TO_METERS),
        energy_value_sql = build_unit_conversion_case("ws.value", "ws.unit", ENERGY_TO_KILOCALORIES),
    )
}

fn build_unit_conversion_case(
    value_expression: &str,
    unit_expression: &str,
    multipliers: &[(&str, f64)],
) -> String {
    let mut clauses = vec![format!("CASE LOWER(COALESCE({unit_expression}, ''))")];
    for (unit, multiplier) in multipliers {
        let converted_value = if (*multiplier - 1.0).abs() < f64::EPSILON {
            value_expression.to_string()
        } else {
            format!("({value_expression} * {multiplier})")
        };
        clauses.push(format!("WHEN '{unit}' THEN {converted_value}"));
    }
    clauses.push("ELSE NULL END".to_string());
    clauses.join("\n")
}

fn schema_sql() -> String {
    format!(
        "
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS dataset_info (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid TEXT UNIQUE,
            activity_type TEXT NOT NULL,
            source_name TEXT,
            source_version TEXT,
            device TEXT,
            creation_date TEXT,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            duration_seconds REAL,
            total_distance REAL,
            total_distance_unit TEXT,
            total_distance_meters REAL,
            total_energy_burned REAL,
            total_energy_burned_unit TEXT,
            total_energy_burned_kilocalories REAL,
            raw_attributes TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workout_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id INTEGER NOT NULL REFERENCES workouts(id) ON DELETE CASCADE,
            key TEXT NOT NULL,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS workout_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id INTEGER NOT NULL REFERENCES workouts(id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            event_date TEXT,
            duration_seconds REAL,
            duration_unit TEXT,
            raw_attributes TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workout_statistics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id INTEGER NOT NULL REFERENCES workouts(id) ON DELETE CASCADE,
            statistic_type TEXT NOT NULL,
            start_date TEXT,
            end_date TEXT,
            unit TEXT,
            aggregation TEXT NOT NULL,
            value REAL NOT NULL,
            raw_attributes TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workout_routes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id INTEGER NOT NULL REFERENCES workouts(id) ON DELETE CASCADE,
            route_type TEXT,
            source_name TEXT,
            source_version TEXT,
            device TEXT,
            creation_date TEXT,
            start_date TEXT,
            end_date TEXT,
            raw_attributes TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workout_route_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            route_id INTEGER NOT NULL REFERENCES workout_routes(id) ON DELETE CASCADE,
            key TEXT NOT NULL,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid TEXT UNIQUE,
            record_type TEXT NOT NULL,
            source_name TEXT,
            source_version TEXT,
            unit TEXT,
            value_text TEXT,
            value_numeric REAL,
            device TEXT,
            creation_date TEXT,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            raw_attributes TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS record_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
            key TEXT NOT NULL,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS workout_records (
            workout_id INTEGER NOT NULL REFERENCES workouts(id) ON DELETE CASCADE,
            record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
            PRIMARY KEY (workout_id, record_id)
        );

        {}
        ",
        effective_workout_table_sql()
    )
}

fn effective_workout_table_sql() -> String {
    "
    CREATE TABLE IF NOT EXISTS effective_workouts (
        id INTEGER PRIMARY KEY REFERENCES workouts(id) ON DELETE CASCADE,
        uuid TEXT UNIQUE,
        activity_type TEXT NOT NULL,
        source_name TEXT,
        source_version TEXT,
        device TEXT,
        creation_date TEXT,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        duration_seconds REAL,
        total_distance REAL,
        total_distance_unit TEXT,
        total_distance_meters REAL,
        total_energy_burned REAL,
        total_energy_burned_unit TEXT,
        total_energy_burned_kilocalories REAL,
        raw_attributes TEXT NOT NULL,
        effective_distance_meters REAL,
        effective_energy_kilocalories REAL,
        average_heart_rate REAL,
        minimum_heart_rate REAL,
        maximum_heart_rate REAL,
        heart_rate_sample_count INTEGER NOT NULL DEFAULT 0,
        average_running_cadence REAL,
        maximum_running_cadence REAL,
        is_indoor INTEGER,
        route_count INTEGER NOT NULL DEFAULT 0,
        effort TEXT
    );
    "
    .to_string()
}

fn post_import_index_sql() -> String {
    "
    CREATE INDEX IF NOT EXISTS idx_workout_metadata_workout_id ON workout_metadata(workout_id);
    CREATE INDEX IF NOT EXISTS idx_workout_events_workout_id ON workout_events(workout_id);
    CREATE INDEX IF NOT EXISTS idx_workout_statistics_workout_id ON workout_statistics(workout_id);
    CREATE INDEX IF NOT EXISTS idx_workout_routes_workout_id ON workout_routes(workout_id);
    CREATE INDEX IF NOT EXISTS idx_workout_route_metadata_route_id ON workout_route_metadata(route_id);
    CREATE INDEX IF NOT EXISTS idx_record_metadata_record_id ON record_metadata(record_id);
    CREATE INDEX IF NOT EXISTS idx_workouts_activity_type ON workouts(activity_type);
    CREATE INDEX IF NOT EXISTS idx_workouts_date_range ON workouts(start_date, end_date);
    CREATE INDEX IF NOT EXISTS idx_records_type ON records(record_type);
    CREATE INDEX IF NOT EXISTS idx_records_date_range ON records(start_date, end_date);
    "
    .to_string()
}

fn post_link_index_sql() -> String {
    format!(
        "
        CREATE INDEX IF NOT EXISTS idx_workout_records_record_id ON workout_records(record_id);
        CREATE INDEX IF NOT EXISTS idx_effective_workouts_activity_type ON effective_workouts(activity_type);
        CREATE INDEX IF NOT EXISTS idx_effective_workouts_date_range ON effective_workouts(start_date, end_date);
        CREATE INDEX IF NOT EXISTS idx_effective_workouts_duration_seconds ON effective_workouts(duration_seconds);
        CREATE INDEX IF NOT EXISTS idx_effective_workouts_distance ON effective_workouts(effective_distance_meters);
        CREATE INDEX IF NOT EXISTS idx_effective_workouts_energy ON effective_workouts(effective_energy_kilocalories);
        CREATE INDEX IF NOT EXISTS idx_effective_workouts_average_heart_rate ON effective_workouts(average_heart_rate);
        CREATE INDEX IF NOT EXISTS idx_effective_workouts_maximum_heart_rate ON effective_workouts(maximum_heart_rate);
        CREATE INDEX IF NOT EXISTS idx_effective_workouts_effort ON effective_workouts(effort);
        CREATE INDEX IF NOT EXISTS idx_effective_workouts_indoor ON effective_workouts(is_indoor);
        "
    )
}

fn fetch_rows_for_ids(
    connection: &Connection,
    template: &str,
    ids: &[i64],
) -> Result<Vec<Map<String, Value>>, String> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    for chunk in ids.chunks(MAX_IN_CLAUSE_VARIABLES) {
        let placeholders = vec!["?"; chunk.len()].join(", ");
        let statement = template.replace("{placeholders}", &placeholders);
        let parameters: Vec<SqlValue> = chunk.iter().copied().map(SqlValue::Integer).collect();
        rows.extend(query_objects(connection, &statement, &parameters)?);
    }
    Ok(rows)
}

fn main_database_path(connection: &Connection) -> Result<PathBuf, String> {
    let mut statement = connection
        .prepare("PRAGMA database_list")
        .map_err(|error| error.to_string())?;
    let mut rows = statement.query([]).map_err(|error| error.to_string())?;
    while let Some(row) = rows.next().map_err(|error| error.to_string())? {
        let name = row.get::<_, String>(1).map_err(|error| error.to_string())?;
        if name != "main" {
            continue;
        }
        let path = row.get::<_, String>(2).map_err(|error| error.to_string())?;
        if path.trim().is_empty() {
            return Err("Main SQLite database path is unavailable.".into());
        }
        return Ok(PathBuf::from(path));
    }
    Err("Failed to resolve main SQLite database path.".into())
}

fn group_rows<F>(
    rows: &[Map<String, Value>],
    key_name: &str,
    mut payload_builder: F,
) -> HashMap<i64, Vec<Value>>
where
    F: FnMut(&Map<String, Value>) -> Value,
{
    let mut grouped = HashMap::new();
    for row in rows {
        if let Some(id) = object_get_i64(row, key_name) {
            grouped
                .entry(id)
                .or_insert_with(Vec::new)
                .push(payload_builder(row));
        }
    }
    grouped
}

fn query_objects(
    connection: &Connection,
    statement: &str,
    parameters: &[SqlValue],
) -> Result<Vec<Map<String, Value>>, String> {
    let mut prepared = connection
        .prepare(statement)
        .map_err(|error| error.to_string())?;
    let rows = prepared
        .query_map(params_from_iter(parameters.iter()), row_to_object)
        .map_err(|error| error.to_string())?;
    let mut objects = Vec::new();
    for row in rows {
        objects.push(row.map_err(|error| error.to_string())?);
    }
    Ok(objects)
}

fn query_optional_object(
    connection: &Connection,
    statement: &str,
    parameters: &[SqlValue],
) -> Result<Option<Map<String, Value>>, String> {
    let mut prepared = connection
        .prepare(statement)
        .map_err(|error| error.to_string())?;
    prepared
        .query_row(params_from_iter(parameters.iter()), row_to_object)
        .optional()
        .map_err(|error| error.to_string())
}

fn row_to_object(row: &Row<'_>) -> rusqlite::Result<Map<String, Value>> {
    let mut object = Map::new();
    for index in 0..row.as_ref().column_count() {
        let name = row
            .as_ref()
            .column_name(index)
            .map(|value| value.to_string())
            .unwrap_or_else(|_| format!("column_{index}"));
        object.insert(name, sql_value_ref_to_json(row.get_ref(index)?));
    }
    Ok(object)
}

fn sql_value_ref_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => json!(value),
        ValueRef::Real(value) => json!(value),
        ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(_) => Value::Null,
    }
}

fn combine_parameters(first: Vec<SqlValue>, second: Vec<SqlValue>) -> Vec<SqlValue> {
    first.into_iter().chain(second).collect()
}

fn parse_json_field(row: &Map<String, Value>, field: &str) -> Value {
    row.get(field)
        .and_then(Value::as_str)
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or(Value::Null)
}

fn object_get_str<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key)?.as_str()
}

fn object_get_i64(object: &Map<String, Value>, key: &str) -> Option<i64> {
    object.get(key)?.as_i64()
}

fn object_get_f64(object: &Map<String, Value>, key: &str) -> Option<f64> {
    object.get(key)?.as_f64()
}

fn objects_to_values(objects: Vec<Map<String, Value>>) -> Vec<Value> {
    objects.into_iter().map(Value::Object).collect()
}

fn collect_f64(rows: &[Map<String, Value>], key: &str) -> Vec<f64> {
    rows.iter()
        .filter_map(|row| object_get_f64(row, key))
        .collect()
}

fn sum_f64(rows: &[Map<String, Value>], key: &str) -> Option<f64> {
    let values = collect_f64(rows, key);
    if values.is_empty() {
        None
    } else {
        Some(values.into_iter().sum())
    }
}

fn sum_group_f64(rows: &[&Map<String, Value>], key: &str) -> Option<f64> {
    let mut total = 0.0;
    let mut found = false;
    for row in rows {
        if let Some(value) = object_get_f64(row, key) {
            total += value;
            found = true;
        }
    }
    if found {
        Some(total)
    } else {
        None
    }
}

fn max_optional(values: &[f64]) -> Option<f64> {
    values
        .iter()
        .copied()
        .max_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
}

fn round_optional(value: Option<f64>, digits: u32) -> Value {
    match value {
        Some(value) => {
            let rounded = round_to_places(value, digits);
            if digits == 0 {
                json!(rounded as i64)
            } else {
                json!(rounded)
            }
        }
        None => Value::Null,
    }
}

fn round_to_places(value: f64, digits: u32) -> f64 {
    let factor = 10_f64.powi(digits as i32);
    (value * factor).round() / factor
}

fn join_export_worker<'scope, T>(
    worker_name: &str,
    handle: std::thread::ScopedJoinHandle<'scope, Result<T, String>>,
) -> Result<T, String> {
    handle
        .join()
        .map_err(|_| format!("{worker_name} worker panicked."))?
}

fn open_connection(path: &Path) -> Result<Connection, String> {
    let connection = Connection::open(path).map_err(|error| error.to_string())?;
    connection
        .execute_batch("PRAGMA foreign_keys = ON;")
        .map_err(|error| error.to_string())?;
    Ok(connection)
}

struct IngestStatements<'txn> {
    insert_workout: Statement<'txn>,
    insert_workout_metadata: Statement<'txn>,
    insert_workout_event: Statement<'txn>,
    insert_workout_statistic: Statement<'txn>,
    insert_workout_route: Statement<'txn>,
    insert_workout_route_metadata: Statement<'txn>,
    insert_record: Statement<'txn>,
    insert_record_metadata: Statement<'txn>,
}

impl<'txn> IngestStatements<'txn> {
    fn new(transaction: &'txn Transaction<'txn>) -> Result<Self, String> {
        Ok(Self {
            insert_workout: transaction
                .prepare(
                    "
                    INSERT INTO workouts (
                        uuid,
                        activity_type,
                        source_name,
                        source_version,
                        device,
                        creation_date,
                        start_date,
                        end_date,
                        duration_seconds,
                        total_distance,
                        total_distance_unit,
                        total_distance_meters,
                        total_energy_burned,
                        total_energy_burned_unit,
                        total_energy_burned_kilocalories,
                        raw_attributes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ",
                )
                .map_err(|error| error.to_string())?,
            insert_workout_metadata: transaction
                .prepare("INSERT INTO workout_metadata(workout_id, key, value) VALUES (?, ?, ?)")
                .map_err(|error| error.to_string())?,
            insert_workout_event: transaction
                .prepare(
                    "
                    INSERT INTO workout_events(
                        workout_id,
                        event_type,
                        event_date,
                        duration_seconds,
                        duration_unit,
                        raw_attributes
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ",
                )
                .map_err(|error| error.to_string())?,
            insert_workout_statistic: transaction
                .prepare(
                    "
                    INSERT INTO workout_statistics(
                        workout_id,
                        statistic_type,
                        start_date,
                        end_date,
                        unit,
                        aggregation,
                        value,
                        raw_attributes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ",
                )
                .map_err(|error| error.to_string())?,
            insert_workout_route: transaction
                .prepare(
                    "
                    INSERT INTO workout_routes(
                        workout_id,
                        route_type,
                        source_name,
                        source_version,
                        device,
                        creation_date,
                        start_date,
                        end_date,
                        raw_attributes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ",
                )
                .map_err(|error| error.to_string())?,
            insert_workout_route_metadata: transaction
                .prepare(
                    "INSERT INTO workout_route_metadata(route_id, key, value) VALUES (?, ?, ?)",
                )
                .map_err(|error| error.to_string())?,
            insert_record: transaction
                .prepare(
                    "
                    INSERT INTO records(
                        uuid,
                        record_type,
                        source_name,
                        source_version,
                        unit,
                        value_text,
                        value_numeric,
                        device,
                        creation_date,
                        start_date,
                        end_date,
                        raw_attributes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ",
                )
                .map_err(|error| error.to_string())?,
            insert_record_metadata: transaction
                .prepare("INSERT INTO record_metadata(record_id, key, value) VALUES (?, ?, ?)")
                .map_err(|error| error.to_string())?,
        })
    }
}

fn round_health_metric_value(
    record_type: &str,
    unit: Option<&str>,
    value: Option<f64>,
    digits: u32,
) -> Value {
    round_optional(
        normalize_health_metric_value(record_type, unit, value),
        digits,
    )
}

fn normalize_health_metric_unit(record_type: &str, unit: Option<&str>) -> Option<String> {
    match (record_type, unit) {
        ("HKQuantityTypeIdentifierActiveEnergyBurned", Some("cal" | "kcal"))
        | ("HKQuantityTypeIdentifierBasalEnergyBurned", Some("cal" | "kcal")) => {
            Some("kcal".into())
        }
        (_, other) => other.map(ToOwned::to_owned),
    }
}

fn normalize_health_metric_value(
    record_type: &str,
    unit: Option<&str>,
    value: Option<f64>,
) -> Option<f64> {
    match (record_type, unit, value) {
        ("HKQuantityTypeIdentifierOxygenSaturation", Some("%"), Some(value)) if value <= 1.5 => {
            Some(value * 100.0)
        }
        (_, _, value) => value,
    }
}

fn calculate_speed_mph(duration_seconds: Option<f64>, distance_meters: Option<f64>) -> Option<f64> {
    match (duration_seconds, distance_meters) {
        (Some(duration_seconds), Some(distance_meters)) if duration_seconds > 0.0 => Some(
            round_to_places(distance_meters / 1609.344 / (duration_seconds / 3600.0), 1),
        ),
        _ => None,
    }
}

fn calculate_pace_min_per_mile(
    activity_type: &str,
    duration_seconds: Option<f64>,
    distance_meters: Option<f64>,
) -> Option<f64> {
    match (duration_seconds, distance_meters) {
        (Some(duration_seconds), Some(distance_meters))
            if distance_meters > 0.0
                && ["Running", "Walking", "Hiking"]
                    .iter()
                    .any(|token| activity_type.contains(token)) =>
        {
            Some(round_to_places(
                (duration_seconds / 60.0) / (distance_meters / 1609.344),
                2,
            ))
        }
        _ => None,
    }
}

fn classify_effort(
    duration_seconds: Option<f64>,
    average_heart_rate: Option<f64>,
    energy_kcal: Option<f64>,
) -> Option<String> {
    if let Some(average_heart_rate) = average_heart_rate {
        return Some(
            if average_heart_rate >= 165.0 {
                "very hard"
            } else if average_heart_rate >= 150.0 {
                "hard"
            } else if average_heart_rate >= 135.0 {
                "moderate"
            } else if average_heart_rate >= 120.0 {
                "easy-moderate"
            } else {
                "easy"
            }
            .to_string(),
        );
    }
    match (duration_seconds, energy_kcal) {
        (Some(duration_seconds), Some(energy_kcal)) if duration_seconds > 0.0 => {
            let kcal_per_hour = energy_kcal / (duration_seconds / 3600.0);
            Some(
                if kcal_per_hour >= 900.0 {
                    "very hard"
                } else if kcal_per_hour >= 700.0 {
                    "hard"
                } else if kcal_per_hour >= 450.0 {
                    "moderate"
                } else if kcal_per_hour >= 250.0 {
                    "easy-moderate"
                } else {
                    "easy"
                }
                .to_string(),
            )
        }
        _ => None,
    }
}

fn build_workout_title(activity_name: &str, is_indoor: Option<i32>) -> String {
    match (activity_name, is_indoor) {
        ("Running", Some(1)) => "Indoor Run".into(),
        ("Running", Some(0)) => "Outdoor Run".into(),
        ("Running", _) => "Run".into(),
        ("Walking", Some(1)) => "Indoor Walk".into(),
        ("Walking", Some(0)) => "Outdoor Walk".into(),
        ("Walking", _) => "Walk".into(),
        ("Cycling", Some(1)) => "Indoor Cycle".into(),
        ("Cycling", Some(0)) => "Outdoor Ride".into(),
        ("Cycling", _) => "Ride".into(),
        _ => activity_name.to_string(),
    }
}

fn format_workout_location(is_indoor: Option<i32>) -> Value {
    match is_indoor {
        Some(1) => json!("indoor"),
        Some(0) => json!("outdoor"),
        _ => Value::Null,
    }
}

fn build_workout_summary_sentence(
    title: &str,
    duration_minutes: &Value,
    distance_miles: &Value,
    elevation_gain_ft: &Value,
    temperature_f: &Value,
    energy_kcal: &Value,
    average_heart_rate: &Value,
    effort: Option<&str>,
) -> String {
    let mut parts = vec![title.to_string()];
    if let Some(value) = value_to_display(duration_minutes) {
        parts.push(format!("{value} min"));
    }
    if let Some(value) = value_to_display(distance_miles) {
        parts.push(format!("{value} mi"));
    }
    if let Some(value) = value_to_display(elevation_gain_ft) {
        parts.push(format!("{value} ft gain"));
    }
    if let Some(value) = value_to_display(temperature_f) {
        parts.push(format!("{value} degF"));
    }
    if let Some(value) = value_to_display(energy_kcal) {
        parts.push(format!("{value} kcal"));
    }
    if let Some(value) = value_to_display(average_heart_rate) {
        parts.push(format!("avg HR {value}"));
    }
    if let Some(effort) = effort {
        parts.push(format!("{effort} effort"));
    }
    format!("{}.", parts.join(", "))
}

fn option_string_to_value(value: Option<String>) -> Value {
    value.map(Value::String).unwrap_or(Value::Null)
}

fn optional_f64_to_value(value: Option<f64>) -> Value {
    value.map_or(Value::Null, |value| json!(value))
}

fn value_to_display(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Number(number) => Some(number.to_string()),
        Value::String(value) => Some(value.clone()),
        _ => None,
    }
}

fn value_to_csv_string(value: Option<&Value>) -> String {
    match value.unwrap_or(&Value::Null) {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn json_to_string(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn map_from_pairs<const N: usize>(pairs: [(&str, Value); N]) -> Map<String, Value> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn humanize_identifier(value: Option<&str>) -> String {
    let Some(value) = value else {
        return "Unknown".into();
    };
    let prefixes = [
        "HKWorkoutActivityType",
        "HKQuantityTypeIdentifier",
        "HKCategoryTypeIdentifier",
        "HKWorkoutEventType",
        "HKSeriesType",
        "HKCorrelationTypeIdentifier",
    ];
    let mut cleaned = value.to_string();
    for prefix in prefixes {
        if let Some(stripped) = cleaned.strip_prefix(prefix) {
            cleaned = stripped.to_string();
            break;
        }
    }
    cleaned = cleaned.replace('_', " ");
    let mut humanized = String::new();
    for (index, character) in cleaned.chars().enumerate() {
        if index > 0 {
            let previous = cleaned.chars().nth(index - 1).unwrap_or_default();
            if previous.is_ascii_lowercase() && character.is_ascii_uppercase() {
                humanized.push(' ');
            }
        }
        humanized.push(character);
    }
    let trimmed = humanized.trim();
    if trimmed.is_empty() {
        "Unknown".into()
    } else {
        trimmed.to_string()
    }
}

fn require_attribute(
    attributes: &BTreeMap<String, String>,
    name: &str,
    element_name: &str,
) -> Result<String, String> {
    attributes
        .get(name)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| format!("{element_name} is missing required attribute {name:?}."))
}

fn parse_required_timestamp(
    attributes: &BTreeMap<String, String>,
    name: &str,
    element_name: &str,
) -> Result<String, String> {
    let value = require_attribute(attributes, name, element_name)?;
    parse_health_datetime(&value)
}

fn maybe_parse_health_datetime(value: Option<&str>) -> Result<Option<String>, String> {
    match value {
        Some(value) if !value.is_empty() => Ok(Some(parse_health_datetime(value)?)),
        _ => Ok(None),
    }
}

fn parse_health_datetime(value: &str) -> Result<String, String> {
    let value = value.trim();
    if let Ok(parsed) = DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S %z") {
        return Ok(parsed
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Secs, false));
    }
    if let Ok(parsed) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(Utc
            .from_utc_datetime(&parsed)
            .to_rfc3339_opts(SecondsFormat::Secs, false));
    }
    Err(format!("Unsupported Apple Health datetime: {value:?}"))
}

fn parse_filter_datetime(value: &str, end_of_day: bool) -> Result<String, String> {
    let text = value.trim();
    if text.is_empty() {
        return Err("Date filters cannot be empty.".into());
    }
    if !text.contains('T') && !text.contains(' ') {
        let parsed_date =
            NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|error| error.to_string())?;
        let parsed_time = if end_of_day {
            NaiveTime::from_hms_opt(23, 59, 59).unwrap()
        } else {
            NaiveTime::from_hms_opt(0, 0, 0).unwrap()
        };
        return Ok(Utc
            .from_utc_datetime(&NaiveDateTime::new(parsed_date, parsed_time))
            .to_rfc3339_opts(SecondsFormat::Secs, false));
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(text) {
        return Ok(parsed
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Secs, false));
    }
    if text.contains(' ') && !text.contains('T') {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(&text.replacen(' ', "T", 1)) {
            return Ok(parsed
                .with_timezone(&Utc)
                .to_rfc3339_opts(SecondsFormat::Secs, false));
        }
    }
    if let Ok(parsed) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
        return Ok(Utc
            .from_utc_datetime(&parsed)
            .to_rfc3339_opts(SecondsFormat::Secs, false));
    }
    Err(format!("Invalid ISO 8601 datetime: {value:?}"))
}

#[cfg(test)]
mod tests {
    use super::{
        load_workout_detail, load_workout_metric_series, preprocess_export_xml, schema_sql,
        write_sanitized_xml_bytes, NoopReporter,
    };
    use rusqlite::{params, Connection};
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn preprocess_export_xml_strips_multiline_doctype_and_control_bytes() {
        let temp_dir = tempdir().unwrap();
        let source_path = temp_dir.path().join("export.xml");
        fs::write(
            &source_path,
            b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE HealthData [\n<!ELEMENT HealthData ANY>\n]>\n<HealthData>\n<Record type=\"HKQuantityTypeIdentifierHeartRate\"\x0b value=\"145\"/>\n</HealthData>",
        )
        .unwrap();

        let mut reporter = NoopReporter;
        let processed = preprocess_export_xml(&source_path, &mut reporter).unwrap();
        let contents = fs::read(processed.path()).unwrap();

        assert_eq!(
            contents,
            b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n<HealthData>\n<Record type=\"HKQuantityTypeIdentifierHeartRate\" value=\"145\"/>\n</HealthData>\n"
        );
    }

    #[test]
    fn preprocess_export_xml_preserves_content_after_inline_doctype() {
        let temp_dir = tempdir().unwrap();
        let source_path = temp_dir.path().join("export.xml");
        fs::write(
            &source_path,
            b"<?xml version=\"1.0\"?>\n<!DOCTYPE HealthData [<!ELEMENT HealthData ANY>]><HealthData>\n<Record type=\"HKQuantityTypeIdentifierStepCount\" value=\"10\"/>\n</HealthData>",
        )
        .unwrap();

        let mut reporter = NoopReporter;
        let processed = preprocess_export_xml(&source_path, &mut reporter).unwrap();
        let contents = fs::read(processed.path()).unwrap();

        assert_eq!(
            contents,
            b"<?xml version=\"1.0\"?>\n<HealthData>\n<Record type=\"HKQuantityTypeIdentifierStepCount\" value=\"10\"/>\n</HealthData>\n"
        );
    }

    #[test]
    fn write_sanitized_xml_bytes_removes_only_invalid_control_bytes() {
        let mut output = Vec::new();
        write_sanitized_xml_bytes(&mut output, b"ok\tkeep\ntrim\x00\x0b\x1fnope\rend").unwrap();
        assert_eq!(output, b"ok\tkeep\ntrimnope\rend");
    }

    #[test]
    fn load_workout_detail_returns_trimmed_selected_workout_payload() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(&schema_sql()).unwrap();
        connection
            .execute(
                "
                INSERT INTO workouts (
                    id,
                    uuid,
                    activity_type,
                    start_date,
                    end_date,
                    duration_seconds,
                    total_distance_meters,
                    total_energy_burned_kilocalories,
                    raw_attributes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ",
                params![
                    1_i64,
                    "workout-1",
                    "HKWorkoutActivityTypeRunning",
                    "2026-04-21T07:00:00Z",
                    "2026-04-21T08:00:00Z",
                    3600.0_f64,
                    10_000.0_f64,
                    700.0_f64,
                    "{}",
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workout_metadata (workout_id, key, value) VALUES (?, ?, ?)",
                params![1_i64, "HKElevationAscended", "1200"],
            )
            .unwrap();
        connection
            .execute(
                "
                INSERT INTO workout_events (
                    workout_id,
                    event_type,
                    event_date,
                    raw_attributes
                ) VALUES (?, ?, ?, ?)
                ",
                params![
                    1_i64,
                    "HKWorkoutEventTypePause",
                    "2026-04-21T07:30:00Z",
                    "{}",
                ],
            )
            .unwrap();
        connection
            .execute(
                "
                INSERT INTO workout_statistics (
                    workout_id,
                    statistic_type,
                    start_date,
                    end_date,
                    unit,
                    aggregation,
                    value,
                    raw_attributes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ",
                params![
                    1_i64,
                    "HKQuantityTypeIdentifierStepCount",
                    "2026-04-21T07:00:00Z",
                    "2026-04-21T08:00:00Z",
                    "count",
                    "sum",
                    1800.0_f64,
                    "{}",
                ],
            )
            .unwrap();
        connection
            .execute(
                "
                INSERT INTO workout_routes (
                    workout_id,
                    start_date,
                    end_date,
                    raw_attributes
                ) VALUES (?, ?, ?, ?)
                ",
                params![1_i64, "2026-04-21T07:00:00Z", "2026-04-21T08:00:00Z", "{}",],
            )
            .unwrap();
        connection
            .execute(
                "
                INSERT INTO records (
                    id,
                    record_type,
                    unit,
                    value_numeric,
                    start_date,
                    end_date,
                    raw_attributes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ",
                params![
                    1_i64,
                    "HKQuantityTypeIdentifierHeartRate",
                    "count/min",
                    150.0_f64,
                    "2026-04-21T07:10:00Z",
                    "2026-04-21T07:10:05Z",
                    "{}",
                ],
            )
            .unwrap();
        connection
            .execute(
                "
                INSERT INTO records (
                    id,
                    record_type,
                    unit,
                    value_numeric,
                    start_date,
                    end_date,
                    raw_attributes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ",
                params![
                    2_i64,
                    "HKQuantityTypeIdentifierHeartRate",
                    "count/min",
                    160.0_f64,
                    "2026-04-21T07:20:00Z",
                    "2026-04-21T07:20:05Z",
                    "{}",
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workout_records (workout_id, record_id) VALUES (?, ?)",
                params![1_i64, 1_i64],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workout_records (workout_id, record_id) VALUES (?, ?)",
                params![1_i64, 2_i64],
            )
            .unwrap();

        let detail = load_workout_detail(&connection, 1_i64).unwrap().unwrap();
        let payload = detail.as_object().unwrap();

        assert_eq!(payload["linked_data_counts"]["records"].as_i64(), Some(2));
        assert_eq!(payload["linked_data_counts"]["metadata"].as_i64(), Some(1));
        assert_eq!(payload["linked_data_counts"]["events"].as_i64(), Some(1));
        assert_eq!(payload["linked_data_counts"]["routes"].as_i64(), Some(1));
        assert_eq!(
            payload["derived_metrics"]["associated_record_count"].as_i64(),
            Some(2)
        );
        assert!(payload.get("records").is_none());
        assert!(payload.get("events").is_none());
        assert!(payload.get("routes").is_none());
        assert!(payload.get("metric_series").is_none());
    }

    #[test]
    fn load_workout_metric_series_returns_lazy_drilldown_payload() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(&schema_sql()).unwrap();
        connection
            .execute(
                "
                INSERT INTO workouts (
                    id,
                    uuid,
                    activity_type,
                    start_date,
                    end_date,
                    duration_seconds,
                    raw_attributes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ",
                params![
                    1_i64,
                    "workout-1",
                    "HKWorkoutActivityTypeRunning",
                    "2026-04-21T07:00:00Z",
                    "2026-04-21T08:00:00Z",
                    3600.0_f64,
                    "{}",
                ],
            )
            .unwrap();
        connection
            .execute(
                "
                INSERT INTO records (
                    id,
                    record_type,
                    unit,
                    value_numeric,
                    start_date,
                    end_date,
                    raw_attributes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ",
                params![
                    1_i64,
                    "HKQuantityTypeIdentifierHeartRate",
                    "count/min",
                    150.0_f64,
                    "2026-04-21T07:10:00Z",
                    "2026-04-21T07:10:05Z",
                    "{}",
                ],
            )
            .unwrap();
        connection
            .execute(
                "
                INSERT INTO records (
                    id,
                    record_type,
                    unit,
                    value_numeric,
                    start_date,
                    end_date,
                    raw_attributes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ",
                params![
                    2_i64,
                    "HKQuantityTypeIdentifierHeartRate",
                    "count/min",
                    160.0_f64,
                    "2026-04-21T07:20:00Z",
                    "2026-04-21T07:20:05Z",
                    "{}",
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workout_records (workout_id, record_id) VALUES (?, ?)",
                params![1_i64, 1_i64],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workout_records (workout_id, record_id) VALUES (?, ?)",
                params![1_i64, 2_i64],
            )
            .unwrap();

        let metric_series = load_workout_metric_series(&connection, 1_i64)
            .unwrap()
            .unwrap();
        assert_eq!(metric_series.len(), 1);
        assert_eq!(
            metric_series[0]["key"].as_str(),
            Some("HKQuantityTypeIdentifierHeartRate")
        );
        assert_eq!(metric_series[0]["sampleCount"].as_u64(), Some(2));
        assert_eq!(metric_series[0]["points"].as_array().unwrap().len(), 2);
    }
}

fn utc_now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, false)
}

fn parse_optional_float(value: Option<&str>) -> Option<f64> {
    value.and_then(|value| value.parse::<f64>().ok())
}

fn convert_measurement(
    value: Option<&str>,
    unit: Option<&str>,
    multipliers: &[(&str, f64)],
) -> Option<f64> {
    let numeric_value = parse_optional_float(value)?;
    let multiplier = lookup_measurement_multiplier(unit?, multipliers)?;
    Some(numeric_value * multiplier)
}

fn lookup_measurement_multiplier(unit: &str, multipliers: &[(&str, f64)]) -> Option<f64> {
    multipliers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(unit))
        .map(|(_, multiplier)| *multiplier)
}

fn serialize_attributes(attributes: &BTreeMap<String, String>) -> Result<String, String> {
    serde_json::to_string(attributes).map_err(|error| error.to_string())
}

fn normalize_text_filter(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let candidate = value.trim();
        if candidate.is_empty() {
            None
        } else {
            Some(candidate)
        }
    })
}

fn validate_numeric_range(
    minimum: Option<f64>,
    maximum: Option<f64>,
    label: &str,
) -> Result<(), String> {
    if let (Some(minimum), Some(maximum)) = (minimum, maximum) {
        if minimum > maximum {
            return Err(format!(
                "{label} minimum must be less than or equal to maximum."
            ));
        }
    }
    Ok(())
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn format_elapsed(seconds: f64) -> String {
    if seconds < 1.0 {
        return format!("{seconds:.2}s");
    }
    if seconds < 60.0 {
        return format!("{seconds:.1}s");
    }
    let minutes = (seconds / 60.0).floor();
    let remainder = seconds - (minutes * 60.0);
    if minutes < 60.0 {
        return format!("{}m {:.1}s", minutes as i64, remainder);
    }
    let hours = (minutes / 60.0).floor();
    let minutes = minutes - (hours * 60.0);
    format!("{}h {}m {:.1}s", hours as i64, minutes as i64, remainder)
}

fn format_count(value: usize) -> String {
    let text = value.to_string();
    let mut formatted = String::new();
    for (index, character) in text.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(character);
    }
    formatted.chars().rev().collect()
}
