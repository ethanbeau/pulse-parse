use chrono::Utc;
use health_core::{
    DashboardRequest, ExportRequest, ProgressReporter, StoredDatasetMetadata, WorkoutDetailRequest,
    WorkoutMetricSeriesRequest,
};
use health_store::{
    configure_connection_for_ingest, connect_database, export_workouts, get_dataset_info,
    ingest_export_xml, list_activity_types, load_health_overview, load_workout_dashboard_bundle,
    load_workout_detail as store_load_workout_detail,
    load_workout_metric_series as store_load_workout_metric_series, open_existing_database,
    parse_activity_filters, parse_workout_window, preprocess_export_xml, read_dataset_metadata,
    upsert_dataset_info, write_dataset_metadata, ExportOptions, HealthQueryOptions,
    WorkoutQueryOptions,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub fn load_dashboard(db_path: &Path, request: &DashboardRequest) -> Result<Value, String> {
    let resolved_db_path = db_path.expand_home();
    let normalized = normalize_workout_options(request)?;
    let health_options = normalize_health_options(request)?;
    let dashboard_started_at = Instant::now();
    let workout_options = normalized.clone();
    let health_options_for_thread = health_options.clone();
    let (
        (available_activity_types, available_activity_types_duration_seconds),
        (health_overview, health_overview_duration_seconds),
        ((inspection, summary), workout_sections_duration_seconds),
    ) = std::thread::scope(|scope| {
        let activity_db_path = resolved_db_path.clone();
        let activity_handle =
            scope.spawn(move || timed_result(|| load_cached_activity_types(&activity_db_path)));

        let health_db_path = resolved_db_path.clone();
        let health_handle = scope.spawn(move || {
            timed_result(|| {
                let connection = open_existing_database(&health_db_path)?;
                load_health_overview(&connection, &health_options_for_thread)
            })
        });

        let workout_db_path = resolved_db_path.clone();
        let workout_handle = scope.spawn(move || {
            timed_result(|| {
                let connection = open_existing_database(&workout_db_path)?;
                let mut reporter = NoopReporter;
                load_workout_dashboard_bundle(&connection, &workout_options, &mut reporter)
            })
        });

        Ok::<_, String>((
            join_timed_worker("activity types", activity_handle)?,
            join_timed_worker("health overview", health_handle)?,
            join_timed_worker("workout dashboard sections", workout_handle)?,
        ))
    })?;

    let mut payload = json!({
        "db_path": path_to_string(&resolved_db_path),
        "available_activity_types": available_activity_types,
        "health_overview": health_overview,
        "inspection": inspection,
        "summary": summary,
        "performance": {
            "available_activity_types_duration_seconds": available_activity_types_duration_seconds,
            "health_overview_duration_seconds": health_overview_duration_seconds,
            "workout_sections_duration_seconds": workout_sections_duration_seconds,
            "total_duration_seconds": round_to_places(dashboard_started_at.elapsed().as_secs_f64(), 3),
        }
    });
    append_payload_size_bytes(&mut payload)?;
    Ok(payload)
}

pub fn load_workout_dashboard(db_path: &Path, request: &DashboardRequest) -> Result<Value, String> {
    let resolved_db_path = db_path.expand_home();
    let workout_options = normalize_workout_options(request)?;
    let ((inspection, summary), workout_sections_duration_seconds) = timed_result(|| {
        let connection = open_existing_database(&resolved_db_path)?;
        let mut reporter = NoopReporter;
        load_workout_dashboard_bundle(&connection, &workout_options, &mut reporter)
    })?;
    let mut payload = json!({
        "db_path": path_to_string(&resolved_db_path),
        "inspection": inspection,
        "summary": summary,
        "performance": {
            "workout_sections_duration_seconds": workout_sections_duration_seconds,
            "total_duration_seconds": workout_sections_duration_seconds,
        }
    });
    append_payload_size_bytes(&mut payload)?;
    Ok(payload)
}

pub fn load_health_dashboard(db_path: &Path, request: &DashboardRequest) -> Result<Value, String> {
    let resolved_db_path = db_path.expand_home();
    let health_options = normalize_health_options(request)?;
    let (health_overview, health_overview_duration_seconds) = timed_result(|| {
        let connection = open_existing_database(&resolved_db_path)?;
        load_health_overview(&connection, &health_options)
    })?;
    let mut payload = json!({
        "db_path": path_to_string(&resolved_db_path),
        "health_overview": health_overview,
        "performance": {
            "health_overview_duration_seconds": health_overview_duration_seconds,
            "total_duration_seconds": health_overview_duration_seconds,
        }
    });
    append_payload_size_bytes(&mut payload)?;
    Ok(payload)
}

pub fn load_workout_detail(
    db_path: &Path,
    request: &WorkoutDetailRequest,
) -> Result<Value, String> {
    if request.workout_id <= 0 {
        return Err("Workout id must be a positive integer.".into());
    }
    let resolved_db_path = db_path.expand_home();
    let (workout, total_duration_seconds) = timed_result(|| {
        let connection = open_existing_database(&resolved_db_path)?;
        let Some(workout) = store_load_workout_detail(&connection, request.workout_id)? else {
            return Err(format!("Workout not found: {}", request.workout_id));
        };
        Ok(workout)
    })?;
    let mut payload = json!({
        "db_path": path_to_string(&resolved_db_path),
        "workout": workout,
        "performance": {
            "total_duration_seconds": total_duration_seconds,
        }
    });
    append_payload_size_bytes(&mut payload)?;
    Ok(payload)
}

pub fn load_workout_metric_series(
    db_path: &Path,
    request: &WorkoutMetricSeriesRequest,
) -> Result<Value, String> {
    if request.workout_id <= 0 {
        return Err("Workout id must be a positive integer.".into());
    }
    let resolved_db_path = db_path.expand_home();
    let (metric_series, total_duration_seconds) = timed_result(|| {
        let connection = open_existing_database(&resolved_db_path)?;
        let Some(metric_series) =
            store_load_workout_metric_series(&connection, request.workout_id)?
        else {
            return Err(format!("Workout not found: {}", request.workout_id));
        };
        Ok(metric_series)
    })?;
    let point_count = metric_series
        .iter()
        .map(|metric| {
            metric
                .get("points")
                .and_then(Value::as_array)
                .map(|points| points.len())
                .unwrap_or(0)
        })
        .sum::<usize>();
    let metric_series_count = metric_series.len();
    let mut payload = json!({
        "db_path": path_to_string(&resolved_db_path),
        "workout_id": request.workout_id,
        "metric_series": metric_series,
        "performance": {
            "total_duration_seconds": total_duration_seconds,
            "metric_series_count": metric_series_count,
            "point_count": point_count,
        }
    });
    append_payload_size_bytes(&mut payload)?;
    Ok(payload)
}

pub fn run_export(db_path: &Path, request: &ExportRequest) -> Result<Value, String> {
    let resolved_db_path = db_path.expand_home();
    let workout = normalize_export_workout_options(request)?;
    let output_path = PathBuf::from(&request.output_path).expand_home();
    let (result, total_duration_seconds) = timed_result(|| {
        let connection = open_existing_database(&resolved_db_path)?;
        export_workouts(
            &connection,
            &ExportOptions {
                workout,
                output_path,
                export_format: request.export_format.clone(),
                summary: request.summary,
                csv_profile: request.csv_profile.clone().unwrap_or_else(|| "full".into()),
            },
            &mut NoopReporter,
        )
    })?;
    let mut payload = merge_db_path(result, &resolved_db_path)?;
    payload
        .as_object_mut()
        .ok_or_else(|| "Expected JSON object response.".to_string())?
        .insert(
            "performance".into(),
            json!({
                "total_duration_seconds": total_duration_seconds,
            }),
        );
    Ok(payload)
}

pub fn ingest_dataset<F, C>(
    xml_path: &Path,
    db_path: &Path,
    verbose: bool,
    progress_handler: F,
    is_cancelled: C,
) -> Result<Value, String>
where
    F: FnMut(&str, &str) -> Result<(), String>,
    C: Fn() -> bool,
{
    let resolved_xml_path = resolve_existing_file(xml_path, "Apple Health export")?;
    let resolved_db_path = db_path.expand_home();
    let source_xml_size_bytes = resolved_xml_path
        .metadata()
        .map_err(|error| error.to_string())?
        .len();
    let ingest_started_epoch_seconds = current_epoch_seconds();
    let ingest_started_at = utc_now_timestamp();
    let started_at = Instant::now();
    let mut reporter = DesktopReporter::new(verbose, progress_handler, is_cancelled);

    let preprocess_started_at = Instant::now();
    let prepared_xml = preprocess_export_xml(&resolved_xml_path, &mut reporter)?;
    let preprocess_duration_seconds =
        round_to_places(preprocess_started_at.elapsed().as_secs_f64(), 3);
    let database_setup_started_at = Instant::now();
    let mut connection = connect_database(&resolved_db_path)?;
    configure_connection_for_ingest(&connection)?;
    let database_setup_duration_seconds =
        round_to_places(database_setup_started_at.elapsed().as_secs_f64(), 3);
    let ingest_report = ingest_export_xml(
        &mut connection,
        prepared_xml.path(),
        &resolved_xml_path,
        &mut reporter,
    )?;
    let ingest_finished_epoch_seconds = current_epoch_seconds();
    let ingest_duration_seconds = round_to_places(started_at.elapsed().as_secs_f64(), 3);
    upsert_dataset_info(
        &connection,
        &[
            ("source_xml_size_bytes", source_xml_size_bytes.to_string()),
            ("ingest_started_at", ingest_started_at.clone()),
            ("ingest_finished_at", utc_now_timestamp()),
            (
                "ingest_started_epoch_seconds",
                ingest_started_epoch_seconds.to_string(),
            ),
            (
                "ingest_finished_epoch_seconds",
                ingest_finished_epoch_seconds.to_string(),
            ),
            (
                "ingest_duration_seconds",
                format!("{ingest_duration_seconds:.3}"),
            ),
        ],
    )?;
    let dataset_info = get_dataset_info(&connection)?;
    let mut ingest_metrics = ingest_report.metrics;
    ingest_metrics.insert("source_xml_size_bytes".into(), json!(source_xml_size_bytes));
    ingest_metrics.insert("ingest_started_at".into(), json!(ingest_started_at));
    ingest_metrics.insert(
        "ingest_finished_at".into(),
        dataset_info
            .get("ingest_finished_at")
            .cloned()
            .unwrap_or(Value::Null),
    );
    ingest_metrics.insert(
        "ingest_started_epoch_seconds".into(),
        json!(ingest_started_epoch_seconds),
    );
    ingest_metrics.insert(
        "ingest_finished_epoch_seconds".into(),
        json!(ingest_finished_epoch_seconds),
    );
    ingest_metrics.insert(
        "preprocess_duration_seconds".into(),
        json!(preprocess_duration_seconds),
    );
    ingest_metrics.insert(
        "database_setup_duration_seconds".into(),
        json!(database_setup_duration_seconds),
    );
    ingest_metrics.insert(
        "ingest_duration_seconds".into(),
        json!(ingest_duration_seconds),
    );

    Ok(json!({
        "db_path": path_to_string(&resolved_db_path),
        "source_xml_path": path_to_string(&resolved_xml_path),
        "dataset_info": dataset_info,
        "counts": ingest_report.counts,
        "ingest_metrics": ingest_metrics,
    }))
}

pub fn activate_ingested_dataset(
    active_db_path: &Path,
    metadata_path: &Path,
    staging_db_path: &Path,
    payload: &mut Value,
) -> Result<(), String> {
    replace_file(staging_db_path, active_db_path)?;
    let metadata = StoredDatasetMetadata {
        xml_path: lookup_value(payload, &["source_xml_path"])
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        workout_count: lookup_value(payload, &["counts", "workouts"])
            .and_then(Value::as_u64)
            .unwrap_or(0),
        record_count: lookup_value(payload, &["counts", "records"])
            .and_then(Value::as_u64)
            .unwrap_or(0),
        workout_record_link_count: lookup_value(payload, &["counts", "workout_record_links"])
            .and_then(Value::as_u64)
            .unwrap_or(0),
        source_xml_size_bytes: lookup_value(payload, &["ingest_metrics", "source_xml_size_bytes"])
            .and_then(Value::as_u64)
            .unwrap_or(0),
        ingest_duration_seconds: lookup_value(
            payload,
            &["ingest_metrics", "ingest_duration_seconds"],
        )
        .and_then(Value::as_f64)
        .unwrap_or(0.0),
        last_ingested_epoch_seconds: lookup_value(
            payload,
            &["ingest_metrics", "ingest_finished_epoch_seconds"],
        )
        .and_then(Value::as_u64)
        .unwrap_or(0),
        ingest_history: read_dataset_metadata(metadata_path)?
            .unwrap_or_default()
            .ingest_history,
    };
    write_dataset_metadata(metadata_path, &metadata)?;
    set_payload_db_path(payload, active_db_path)?;
    Ok(())
}

pub fn remove_file_if_exists(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    fs::remove_file(path).map_err(|error| error.to_string())
}

pub fn non_success_status_message(success: bool) -> String {
    if success {
        "Ingest completed without a result payload.".into()
    } else {
        "Ingest cancelled.".into()
    }
}

fn normalize_workout_options(request: &DashboardRequest) -> Result<WorkoutQueryOptions, String> {
    let (start, end) = parse_workout_window(request.start.as_deref(), request.end.as_deref())?;
    Ok(WorkoutQueryOptions {
        start,
        end,
        activity_types: parse_activity_filters(&request.activity_types),
        source_query: request.source_query.clone(),
        min_duration_minutes: request.min_duration_minutes,
        max_duration_minutes: request.max_duration_minutes,
        location: request.location.clone(),
        min_distance_miles: request.min_distance_miles,
        max_distance_miles: request.max_distance_miles,
        min_energy_kcal: request.min_energy_kcal,
        max_energy_kcal: request.max_energy_kcal,
        min_avg_heart_rate: request.min_avg_heart_rate,
        max_avg_heart_rate: request.max_avg_heart_rate,
        min_max_heart_rate: request.min_max_heart_rate,
        max_max_heart_rate: request.max_max_heart_rate,
        efforts: request.efforts.clone(),
        requires_route_data: request.requires_route_data.unwrap_or(false),
        requires_heart_rate_samples: request.requires_heart_rate_samples.unwrap_or(false),
    })
}

fn normalize_health_options(request: &DashboardRequest) -> Result<HealthQueryOptions, String> {
    let (start, end) = parse_workout_window(
        request.health_start.as_deref(),
        request.health_end.as_deref(),
    )?;
    Ok(HealthQueryOptions {
        start,
        end,
        categories: request.health_categories.clone(),
        metric_query: request.health_metric_query.clone(),
        source_query: request.health_source_query.clone(),
        only_with_samples: request.health_only_with_samples.unwrap_or(false),
    })
}

fn normalize_export_workout_options(
    request: &ExportRequest,
) -> Result<WorkoutQueryOptions, String> {
    let (start, end) = parse_workout_window(request.start.as_deref(), request.end.as_deref())?;
    Ok(WorkoutQueryOptions {
        start,
        end,
        activity_types: parse_activity_filters(&request.activity_types),
        source_query: request.source_query.clone(),
        min_duration_minutes: request.min_duration_minutes,
        max_duration_minutes: request.max_duration_minutes,
        location: request.location.clone(),
        min_distance_miles: request.min_distance_miles,
        max_distance_miles: request.max_distance_miles,
        min_energy_kcal: request.min_energy_kcal,
        max_energy_kcal: request.max_energy_kcal,
        min_avg_heart_rate: request.min_avg_heart_rate,
        max_avg_heart_rate: request.max_avg_heart_rate,
        min_max_heart_rate: request.min_max_heart_rate,
        max_max_heart_rate: request.max_max_heart_rate,
        efforts: request.efforts.clone(),
        requires_route_data: request.requires_route_data.unwrap_or(false),
        requires_heart_rate_samples: request.requires_heart_rate_samples.unwrap_or(false),
    })
}

struct DesktopReporter<F, C> {
    verbose_enabled: bool,
    event_handler: F,
    is_cancelled: C,
}

impl<F, C> DesktopReporter<F, C> {
    fn new(verbose_enabled: bool, event_handler: F, is_cancelled: C) -> Self {
        Self {
            verbose_enabled,
            event_handler,
            is_cancelled,
        }
    }
}

impl<F, C> ProgressReporter for DesktopReporter<F, C>
where
    F: FnMut(&str, &str) -> Result<(), String>,
    C: Fn() -> bool,
{
    fn progress(&mut self, message: &str) -> Result<(), String> {
        (self.event_handler)("Progress", message)
    }

    fn verbose(&mut self, message: &str) -> Result<(), String> {
        if self.verbose_enabled {
            (self.event_handler)("Verbose", message)?;
        }
        Ok(())
    }

    fn is_cancelled(&self) -> bool {
        (self.is_cancelled)()
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

trait ExpandHome {
    fn expand_home(&self) -> PathBuf;
}

impl ExpandHome for Path {
    fn expand_home(&self) -> PathBuf {
        expand_home_path(self, default_home_dir().as_deref())
    }
}

fn expand_home_path(path: &Path, home_dir: Option<&Path>) -> PathBuf {
    let Some(path) = path.to_str() else {
        return path.to_path_buf();
    };
    let Some(stripped) = path.strip_prefix("~/") else {
        return PathBuf::from(path);
    };
    let Some(home_dir) = home_dir else {
        return PathBuf::from(path);
    };
    home_dir.join(stripped)
}

fn default_home_dir() -> Option<PathBuf> {
    if let Some(home) = std::env::var_os("HOME").filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(home));
    }
    if let Some(home) = std::env::var_os("USERPROFILE").filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(home));
    }

    match (std::env::var_os("HOMEDRIVE"), std::env::var_os("HOMEPATH")) {
        (Some(drive), Some(path)) if !drive.is_empty() && !path.is_empty() => {
            let mut home_dir = PathBuf::from(drive);
            home_dir.push(path);
            Some(home_dir)
        }
        _ => None,
    }
}

fn resolve_existing_file(path: &Path, label: &str) -> Result<PathBuf, String> {
    let resolved_path = path.expand_home();
    if !resolved_path.exists() || !resolved_path.is_file() {
        return Err(format!("{label} not found: {}", resolved_path.display()));
    }
    Ok(resolved_path)
}

fn replace_file(source: &Path, destination: &Path) -> Result<(), String> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    remove_file_if_exists(destination)?;
    fs::rename(source, destination).map_err(|error| error.to_string())
}

fn merge_db_path(mut value: Value, db_path: &Path) -> Result<Value, String> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| "Expected JSON object response.".to_string())?;
    object.insert("db_path".into(), Value::String(path_to_string(db_path)));
    Ok(value)
}

fn append_payload_size_bytes(payload: &mut Value) -> Result<(), String> {
    let payload_size_bytes = serde_json::to_vec(payload)
        .map(|bytes| bytes.len() as u64)
        .map_err(|error| error.to_string())?;
    let performance = payload
        .get_mut("performance")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| "Expected JSON object performance payload.".to_string())?;
    performance.insert("payload_size_bytes".into(), json!(payload_size_bytes));
    Ok(())
}

fn set_payload_db_path(payload: &mut Value, db_path: &Path) -> Result<(), String> {
    let object = payload
        .as_object_mut()
        .ok_or_else(|| "Ingest result payload was not a JSON object.".to_string())?;
    object.insert("db_path".into(), Value::String(path_to_string(db_path)));
    Ok(())
}

fn lookup_value<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn current_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn utc_now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)
}

fn round_to_places(value: f64, digits: u32) -> f64 {
    let factor = 10_f64.powi(digits as i32);
    (value * factor).round() / factor
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct DatabaseCacheKey {
    db_path: PathBuf,
    length_bytes: u64,
    modified_epoch_nanos: u128,
}

static ACTIVITY_TYPES_CACHE: OnceLock<Mutex<HashMap<DatabaseCacheKey, Vec<Value>>>> =
    OnceLock::new();

fn load_cached_activity_types(db_path: &Path) -> Result<Vec<Value>, String> {
    let cache_key = DatabaseCacheKey::from_path(db_path)?;
    let cache = ACTIVITY_TYPES_CACHE.get_or_init(|| Mutex::new(HashMap::new()));

    {
        let guard = cache
            .lock()
            .map_err(|_| "Failed to lock activity type cache.".to_string())?;
        if let Some(cached) = guard.get(&cache_key) {
            return Ok(cached.clone());
        }
    }

    let connection = open_existing_database(db_path)?;
    let activity_types = list_activity_types(&connection)?;
    let mut guard = cache
        .lock()
        .map_err(|_| "Failed to lock activity type cache.".to_string())?;
    guard.retain(|existing_key, _| existing_key.db_path != cache_key.db_path);
    guard.insert(cache_key, activity_types.clone());
    Ok(activity_types)
}

fn timed_result<T, F>(operation: F) -> Result<(T, f64), String>
where
    F: FnOnce() -> Result<T, String>,
{
    let started_at = Instant::now();
    let value = operation()?;
    Ok((
        value,
        round_to_places(started_at.elapsed().as_secs_f64(), 3),
    ))
}

fn join_timed_worker<'scope, T>(
    worker_name: &str,
    handle: std::thread::ScopedJoinHandle<'scope, Result<(T, f64), String>>,
) -> Result<(T, f64), String> {
    handle
        .join()
        .map_err(|_| format!("{worker_name} worker panicked."))?
}

impl DatabaseCacheKey {
    fn from_path(db_path: &Path) -> Result<Self, String> {
        let metadata = fs::metadata(db_path).map_err(|error| error.to_string())?;
        let modified_epoch_nanos = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        Ok(Self {
            db_path: db_path.to_path_buf(),
            length_bytes: metadata.len(),
            modified_epoch_nanos,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        expand_home_path, ingest_dataset, load_dashboard, load_health_dashboard,
        load_workout_dashboard, load_workout_detail, run_export,
    };
    use health_core::{DashboardRequest, ExportRequest, WorkoutDetailRequest};
    use serde_json::Value;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    #[test]
    fn ingest_dashboard_and_workout_details_match_fixture() {
        let temp_dir = TempDir::new().unwrap();
        let database_path = temp_dir.path().join("health.sqlite");
        let mut progress_events = Vec::new();

        let ingest_result = ingest_dataset(
            fixture_export_path().as_path(),
            &database_path,
            false,
            |label, message| {
                progress_events.push((label.to_string(), message.to_string()));
                Ok(())
            },
            || false,
        )
        .unwrap();
        let dashboard = load_dashboard(
            &database_path,
            &DashboardRequest {
                start: None,
                end: None,
                activity_types: vec!["Running".into()],
                source_query: None,
                min_duration_minutes: None,
                max_duration_minutes: None,
                location: None,
                min_distance_miles: None,
                max_distance_miles: None,
                min_energy_kcal: None,
                max_energy_kcal: None,
                min_avg_heart_rate: None,
                max_avg_heart_rate: None,
                min_max_heart_rate: None,
                max_max_heart_rate: None,
                efforts: vec![],
                requires_route_data: Some(false),
                requires_heart_rate_samples: Some(false),
                health_start: None,
                health_end: None,
                health_categories: vec![],
                health_metric_query: None,
                health_source_query: None,
                health_only_with_samples: Some(false),
            },
        )
        .unwrap();
        let workout_id = dashboard["summary"]["workouts"][0]["db_id"]
            .as_i64()
            .unwrap();
        let workout_detail =
            load_workout_detail(&database_path, &WorkoutDetailRequest { workout_id }).unwrap();

        assert_eq!(ingest_result["counts"]["workouts"], Value::from(2));
        assert!(database_path.exists());
        assert!(progress_events.len() >= 3);
        assert!(
            ingest_result["ingest_metrics"]["source_xml_size_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            ingest_result["ingest_metrics"]["ingest_duration_seconds"]
                .as_f64()
                .unwrap()
                >= 0.0
        );
        assert_eq!(
            ingest_result["dataset_info"]["source_xml_size_bytes"],
            Value::from(
                ingest_result["ingest_metrics"]["source_xml_size_bytes"]
                    .as_u64()
                    .unwrap()
                    .to_string()
            )
        );
        assert!(ingest_result["dataset_info"]["ingest_finished_at"].is_string());
        assert_eq!(
            dashboard["inspection"]["overall"]["workout_count"],
            Value::from(1)
        );
        assert_eq!(dashboard["summary"]["workouts"][0]["title"], "Outdoor Run");
        assert_eq!(
            dashboard["summary"]["workouts"][0]["elevation_gain_ft"],
            Value::from(100)
        );
        assert_eq!(
            dashboard["summary"]["workouts"][0]["temperature_f"],
            Value::from(68.0)
        );
        assert!(dashboard["summary"]["workouts"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("100 ft gain"));
        assert!(dashboard["summary"]["workouts"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("degF"));
        assert_eq!(
            dashboard["health_overview"]["record_count"],
            Value::from(11)
        );
        let metrics = metrics_by_key(&dashboard["health_overview"]["metrics"]);
        assert_eq!(metrics["vo2_max"]["latest_value"], Value::from(42.4));
        assert_eq!(
            metrics["oxygen_saturation"]["latest_value"],
            Value::from(99.0)
        );
        assert_eq!(
            metrics["resting_heart_rate"]["average_value"],
            Value::from(59)
        );
        assert_eq!(
            metrics["heart_rate_variability_sdnn"]["latest_value"],
            Value::from(42)
        );
        assert_eq!(
            metrics["heart_rate_recovery_one_minute"]["latest_value"],
            Value::from(28)
        );
        assert_eq!(metrics["step_count"]["summary_kind"], "total");
        assert_eq!(metrics["step_count"]["primary_value"], Value::from(8400));
        assert_eq!(metrics["exercise_time"]["primary_value"], Value::from(45));
        assert_eq!(workout_detail["workout"]["db_id"], Value::from(workout_id));
        assert_eq!(
            workout_detail["workout"]["derived_metrics"]["elevation_gain_ft"],
            Value::from(100.0)
        );
        assert_eq!(
            workout_detail["workout"]["derived_metrics"]["temperature_f"],
            Value::from(68.0)
        );
        assert!(
            workout_detail["workout"]["records"]
                .as_array()
                .unwrap()
                .len()
                >= 3
        );
    }

    #[test]
    fn dashboard_applies_expanded_workout_and_health_filters() {
        let temp_dir = TempDir::new().unwrap();
        let database_path = temp_dir.path().join("health.sqlite");
        ingest_dataset(
            fixture_export_path().as_path(),
            &database_path,
            false,
            |_label, _message| Ok(()),
            || false,
        )
        .unwrap();

        let strava_dashboard = load_dashboard(
            &database_path,
            &DashboardRequest {
                start: None,
                end: None,
                activity_types: vec![],
                source_query: Some("Strava".into()),
                min_duration_minutes: None,
                max_duration_minutes: None,
                location: None,
                min_distance_miles: None,
                max_distance_miles: None,
                min_energy_kcal: None,
                max_energy_kcal: None,
                min_avg_heart_rate: None,
                max_avg_heart_rate: None,
                min_max_heart_rate: None,
                max_max_heart_rate: None,
                efforts: vec![],
                requires_route_data: Some(false),
                requires_heart_rate_samples: Some(false),
                health_start: None,
                health_end: None,
                health_categories: vec![],
                health_metric_query: None,
                health_source_query: None,
                health_only_with_samples: Some(false),
            },
        )
        .unwrap();
        assert_eq!(
            strava_dashboard["inspection"]["overall"]["workout_count"],
            Value::from(1)
        );
        assert_eq!(strava_dashboard["summary"]["workouts"][0]["title"], "Ride");

        let outdoor_dashboard = load_dashboard(
            &database_path,
            &DashboardRequest {
                start: None,
                end: None,
                activity_types: vec![],
                source_query: None,
                min_duration_minutes: None,
                max_duration_minutes: None,
                location: Some("outdoor".into()),
                min_distance_miles: None,
                max_distance_miles: None,
                min_energy_kcal: None,
                max_energy_kcal: None,
                min_avg_heart_rate: None,
                max_avg_heart_rate: None,
                min_max_heart_rate: None,
                max_max_heart_rate: None,
                efforts: vec!["hard".into()],
                requires_route_data: Some(true),
                requires_heart_rate_samples: Some(false),
                health_start: None,
                health_end: None,
                health_categories: vec![],
                health_metric_query: None,
                health_source_query: None,
                health_only_with_samples: Some(false),
            },
        )
        .unwrap();
        assert_eq!(
            outdoor_dashboard["inspection"]["overall"]["workout_count"],
            Value::from(1)
        );
        assert_eq!(
            outdoor_dashboard["summary"]["workouts"][0]["title"],
            "Outdoor Run"
        );

        let health_dashboard = load_dashboard(
            &database_path,
            &DashboardRequest {
                start: None,
                end: None,
                activity_types: vec![],
                source_query: None,
                min_duration_minutes: None,
                max_duration_minutes: None,
                location: None,
                min_distance_miles: None,
                max_distance_miles: None,
                min_energy_kcal: None,
                max_energy_kcal: None,
                min_avg_heart_rate: None,
                max_avg_heart_rate: None,
                min_max_heart_rate: None,
                max_max_heart_rate: None,
                efforts: vec![],
                requires_route_data: Some(false),
                requires_heart_rate_samples: Some(false),
                health_start: None,
                health_end: None,
                health_categories: vec!["Cardio".into()],
                health_metric_query: Some("vo2".into()),
                health_source_query: Some("Apple Watch".into()),
                health_only_with_samples: Some(true),
            },
        )
        .unwrap();
        assert_eq!(
            health_dashboard["health_overview"]["record_count"],
            Value::from(1)
        );
        let metric_keys: Vec<&str> = health_dashboard["health_overview"]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|metric| metric["key"].as_str().unwrap())
            .collect();
        assert_eq!(metric_keys, vec!["vo2_max"]);
    }

    #[test]
    fn partial_dashboard_loads_match_full_dashboard_sections() {
        let temp_dir = TempDir::new().unwrap();
        let database_path = temp_dir.path().join("health.sqlite");
        ingest_dataset(
            fixture_export_path().as_path(),
            &database_path,
            false,
            |_label, _message| Ok(()),
            || false,
        )
        .unwrap();

        let request = DashboardRequest {
            start: None,
            end: None,
            activity_types: vec!["Running".into()],
            source_query: None,
            min_duration_minutes: Some(20.0),
            max_duration_minutes: None,
            location: Some("outdoor".into()),
            min_distance_miles: None,
            max_distance_miles: None,
            min_energy_kcal: None,
            max_energy_kcal: None,
            min_avg_heart_rate: Some(120.0),
            max_avg_heart_rate: None,
            min_max_heart_rate: None,
            max_max_heart_rate: None,
            efforts: vec!["moderate".into()],
            requires_route_data: Some(true),
            requires_heart_rate_samples: Some(true),
            health_start: None,
            health_end: None,
            health_categories: vec!["Cardio".into()],
            health_metric_query: Some("vo2".into()),
            health_source_query: None,
            health_only_with_samples: Some(true),
        };

        let full_dashboard = load_dashboard(&database_path, &request).unwrap();
        let workout_dashboard = load_workout_dashboard(&database_path, &request).unwrap();
        let health_dashboard = load_health_dashboard(&database_path, &request).unwrap();

        assert_eq!(
            workout_dashboard["inspection"],
            full_dashboard["inspection"]
        );
        assert_eq!(workout_dashboard["summary"], full_dashboard["summary"]);
        assert_eq!(
            health_dashboard["health_overview"],
            full_dashboard["health_overview"]
        );
    }

    #[test]
    fn export_writes_summary_json_and_llm_csv() {
        let temp_dir = TempDir::new().unwrap();
        let database_path = temp_dir.path().join("health.sqlite");
        ingest_dataset(
            fixture_export_path().as_path(),
            &database_path,
            false,
            |_label, _message| Ok(()),
            || false,
        )
        .unwrap();

        let export_path = temp_dir.path().join("exports").join("summary.json");
        let result = run_export(
            &database_path,
            &ExportRequest {
                output_path: export_path.to_string_lossy().into_owned(),
                export_format: "json".into(),
                summary: true,
                csv_profile: None,
                start: None,
                end: None,
                activity_types: vec!["Running".into()],
                source_query: None,
                min_duration_minutes: None,
                max_duration_minutes: None,
                location: None,
                min_distance_miles: None,
                max_distance_miles: None,
                min_energy_kcal: None,
                max_energy_kcal: None,
                min_avg_heart_rate: None,
                max_avg_heart_rate: None,
                min_max_heart_rate: None,
                max_max_heart_rate: None,
                efforts: vec![],
                requires_route_data: Some(false),
                requires_heart_rate_samples: Some(false),
                verbose: Some(false),
            },
        )
        .unwrap();
        let exported: Value = serde_json::from_slice(&fs::read(&export_path).unwrap()).unwrap();
        assert_eq!(result["workout_count"], Value::from(1));
        assert_eq!(exported["workouts"][0]["title"], "Outdoor Run");
        assert_eq!(
            exported["workouts"][0]["elevation_gain_ft"],
            Value::from(100)
        );
        assert_eq!(exported["workouts"][0]["temperature_f"], Value::from(68.0));

        let csv_dir = temp_dir.path().join("llm-exports");
        let csv_result = run_export(
            &database_path,
            &ExportRequest {
                output_path: csv_dir.to_string_lossy().into_owned(),
                export_format: "csv".into(),
                summary: false,
                csv_profile: Some("llm".into()),
                start: None,
                end: None,
                activity_types: vec!["Running".into()],
                source_query: None,
                min_duration_minutes: None,
                max_duration_minutes: None,
                location: None,
                min_distance_miles: None,
                max_distance_miles: None,
                min_energy_kcal: None,
                max_energy_kcal: None,
                min_avg_heart_rate: None,
                max_avg_heart_rate: None,
                min_max_heart_rate: None,
                max_max_heart_rate: None,
                efforts: vec![],
                requires_route_data: Some(false),
                requires_heart_rate_samples: Some(false),
                verbose: Some(false),
            },
        )
        .unwrap();
        let csv_path = csv_dir.join("workouts-llm-2024-01-05_to_2024-01-05.csv");
        let mut reader = csv::Reader::from_path(&csv_path).unwrap();
        let headers = reader.headers().unwrap().clone();
        let rows: Vec<csv::StringRecord> = reader.records().collect::<Result<_, _>>().unwrap();
        assert_eq!(csv_result["workout_count"], Value::from(1));
        assert_eq!(
            headers.iter().collect::<Vec<_>>(),
            vec![
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
            ]
        );
        assert_eq!(rows[0].get(2), Some("HKWorkoutActivityTypeRunning"));
        assert_eq!(rows[0].get(4), Some("outdoor"));
        assert_eq!(rows[0].get(6), Some("3.1"));
        assert_eq!(rows[0].get(7), Some("100"));
        assert_eq!(rows[0].get(8), Some("68.0"));
        assert_eq!(rows[0].get(13), Some("176.0"));
        assert_eq!(rows[0].get(14), Some("180"));
        assert_eq!(rows[0].get(16), Some("hard"));
    }

    #[test]
    fn expand_home_path_replaces_tilde_prefix_when_home_is_available() {
        let expanded = expand_home_path(
            Path::new("~/exports/export.xml"),
            Some(Path::new("/Users/tester")),
        );

        assert_eq!(expanded, PathBuf::from("/Users/tester/exports/export.xml"));
    }

    #[test]
    fn expand_home_path_leaves_tilde_prefix_when_home_is_missing() {
        let expanded = expand_home_path(Path::new("~/exports/export.xml"), None);

        assert_eq!(expanded, PathBuf::from("~/exports/export.xml"));
    }

    #[test]
    fn expand_home_path_leaves_non_tilde_paths_unchanged() {
        let expanded = expand_home_path(
            Path::new("./exports/export.xml"),
            Some(Path::new("/Users/tester")),
        );

        assert_eq!(expanded, PathBuf::from("./exports/export.xml"));
    }

    fn metrics_by_key<'a>(metrics: &'a Value) -> std::collections::HashMap<&'a str, &'a Value> {
        metrics
            .as_array()
            .unwrap()
            .iter()
            .map(|metric| (metric["key"].as_str().unwrap(), metric))
            .collect()
    }

    fn fixture_export_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/export.xml")
    }
}
