use health_core::{
    CurrentDataset, DashboardRequest, ExportRequest, IngestFinishedEvent, IngestHistoryEntry,
    IngestHistoryStatus, IngestRequest, WorkoutDetailRequest, WorkoutMetricSeriesRequest,
    INGEST_FINISHED_EVENT_NAME, INGEST_PROGRESS_EVENT_NAME,
};
use health_service::{
    activate_ingested_dataset, ingest_dataset, load_dashboard as service_load_dashboard,
    load_health_dashboard as service_load_health_dashboard,
    load_workout_dashboard as service_load_workout_dashboard,
    load_workout_detail as service_load_workout_detail,
    load_workout_metric_series as service_load_workout_metric_series, remove_file_if_exists,
    run_export as service_run_export,
};
use health_store::{
    active_dataset_db_path_for_dir, append_ingest_history_entry, current_dataset_from_metadata,
    dataset_directory_path, dataset_state_path_for_dir, read_dataset_metadata, read_ingest_history,
    staging_dataset_db_path_for_dir,
};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use tauri::{AppHandle, Emitter, Manager, State};

#[derive(Clone, Default)]
struct DesktopState {
    ingest_cancellation: Arc<Mutex<Option<Arc<AtomicBool>>>>,
}

#[tauri::command]
fn load_current_dataset(app: AppHandle) -> Result<Option<CurrentDataset>, String> {
    read_current_dataset(&app)
}

#[tauri::command]
fn load_ingest_history(app: AppHandle) -> Result<Vec<IngestHistoryEntry>, String> {
    read_persisted_ingest_history(&app)
}

#[tauri::command]
fn load_dashboard(app: AppHandle, request: DashboardRequest) -> Result<Value, String> {
    let db_path = active_dataset_db_path(&app)?;
    service_load_dashboard(&db_path, &request)
}

#[tauri::command]
fn load_workout_dashboard(app: AppHandle, request: DashboardRequest) -> Result<Value, String> {
    let db_path = active_dataset_db_path(&app)?;
    service_load_workout_dashboard(&db_path, &request)
}

#[tauri::command]
fn load_health_dashboard(app: AppHandle, request: DashboardRequest) -> Result<Value, String> {
    let db_path = active_dataset_db_path(&app)?;
    service_load_health_dashboard(&db_path, &request)
}

#[tauri::command]
fn load_workout_detail(app: AppHandle, request: WorkoutDetailRequest) -> Result<Value, String> {
    let db_path = active_dataset_db_path(&app)?;
    service_load_workout_detail(&db_path, &request)
}

#[tauri::command]
fn load_workout_metric_series(
    app: AppHandle,
    request: WorkoutMetricSeriesRequest,
) -> Result<Value, String> {
    let db_path = active_dataset_db_path(&app)?;
    service_load_workout_metric_series(&db_path, &request)
}

#[tauri::command]
fn run_export(app: AppHandle, request: ExportRequest) -> Result<Value, String> {
    let db_path = active_dataset_db_path(&app)?;
    service_run_export(&db_path, &request)
}

#[tauri::command]
fn start_ingest(
    app: AppHandle,
    state: State<DesktopState>,
    request: IngestRequest,
) -> Result<(), String> {
    let staging_db_path = staging_dataset_db_path(&app)?;
    remove_file_if_exists(&staging_db_path)?;

    let cancellation = Arc::new(AtomicBool::new(false));
    {
        let mut slot = lock_ingest_state(&state)?;
        if slot.is_some() {
            return Err("An ingest job is already running.".into());
        }
        *slot = Some(cancellation.clone());
    }

    let app_handle = app.clone();
    let ingest_state = state.ingest_cancellation.clone();
    thread::spawn(move || {
        let result = ingest_dataset(
            PathBuf::from(&request.xml_path).as_path(),
            &staging_db_path,
            request.verbose.unwrap_or(false),
            |label, message| {
                app_handle
                    .emit(
                        INGEST_PROGRESS_EVENT_NAME,
                        serde_json::json!({
                            "label": label,
                            "message": message,
                        }),
                    )
                    .map_err(|error| error.to_string())
            },
            || cancellation.load(Ordering::SeqCst),
        );

        let mut finished_event = match result {
            Ok(mut payload) => {
                match managed_active_dataset_db_path(&app_handle)
                    .and_then(|active_db_path| {
                        current_dataset_state_path(&app_handle)
                            .map(|metadata_path| (active_db_path, metadata_path))
                    })
                    .and_then(|(active_db_path, metadata_path)| {
                        activate_ingested_dataset(
                            &active_db_path,
                            &metadata_path,
                            &staging_db_path,
                            &mut payload,
                        )
                    }) {
                    Ok(()) => IngestFinishedEvent {
                        success: true,
                        payload: Some(payload),
                        error: None,
                    },
                    Err(error) => IngestFinishedEvent {
                        success: false,
                        payload: None,
                        error: Some(error),
                    },
                }
            }
            Err(error) => IngestFinishedEvent {
                success: false,
                payload: None,
                error: Some(error),
            },
        };

        if let Ok(history_path) = current_dataset_state_path(&app_handle) {
            let entry = build_ingest_history_entry(&finished_event);
            if let Err(error) = append_ingest_history_entry(&history_path, entry, 25) {
                eprintln!("failed to persist ingest history: {error}");
                if finished_event.error.is_none() {
                    finished_event.error =
                        Some(format!("Failed to persist ingest history: {error}"));
                }
            }
        }

        if let Ok(mut slot) = ingest_state.lock() {
            *slot = None;
        }
        if !finished_event.success {
            let _ = remove_file_if_exists(&staging_db_path);
        }
        let _ = app_handle.emit(INGEST_FINISHED_EVENT_NAME, finished_event);
    });

    Ok(())
}

#[tauri::command]
fn cancel_ingest(state: State<DesktopState>) -> Result<bool, String> {
    let slot = lock_ingest_state(&state)?;
    let Some(cancellation) = slot.as_ref() else {
        return Ok(false);
    };
    cancellation.store(true, Ordering::SeqCst);
    Ok(true)
}

fn load_dataset_directory(app: &AppHandle) -> Result<PathBuf, String> {
    let app_data_dir = app
        .path()
        .app_data_dir()
        .map_err(|error| error.to_string())?;
    let dataset_dir = dataset_directory_path(&app_data_dir);
    if active_dataset_db_path_for_dir(&dataset_dir).is_file() {
        return Ok(dataset_dir);
    }
    fs::create_dir_all(&dataset_dir).map_err(|error| error.to_string())?;
    Ok(dataset_dir)
}

fn managed_active_dataset_db_path(app: &AppHandle) -> Result<PathBuf, String> {
    Ok(active_dataset_db_path_for_dir(&load_dataset_directory(
        app,
    )?))
}

fn staging_dataset_db_path(app: &AppHandle) -> Result<PathBuf, String> {
    Ok(staging_dataset_db_path_for_dir(&load_dataset_directory(
        app,
    )?))
}

fn current_dataset_state_path(app: &AppHandle) -> Result<PathBuf, String> {
    let config_dir = app
        .path()
        .app_config_dir()
        .map_err(|error| error.to_string())?;
    let state_path = dataset_state_path_for_dir(&config_dir);
    if state_path.is_file() {
        return Ok(state_path);
    }
    fs::create_dir_all(&config_dir).map_err(|error| error.to_string())?;
    Ok(state_path)
}

fn active_dataset_db_path(app: &AppHandle) -> Result<PathBuf, String> {
    let db_path = managed_active_dataset_db_path(app)?;
    if db_path.is_file() {
        return Ok(db_path);
    }
    Err("Load an Apple Health export.xml in Pulse Parse to create the desktop dataset.".into())
}

fn read_current_dataset(app: &AppHandle) -> Result<Option<CurrentDataset>, String> {
    let db_path = managed_active_dataset_db_path(app)?;
    let metadata = read_dataset_metadata(&current_dataset_state_path(app)?)?;
    Ok(current_dataset_from_metadata(&db_path, metadata))
}

fn read_persisted_ingest_history(app: &AppHandle) -> Result<Vec<IngestHistoryEntry>, String> {
    read_ingest_history(&current_dataset_state_path(app)?)
}

fn build_ingest_history_entry(event: &IngestFinishedEvent) -> IngestHistoryEntry {
    if event.success {
        if let Some(payload) = event.payload.as_ref() {
            let finished_at =
                lookup_payload_string(payload, &["ingest_metrics", "ingest_finished_at"])
                    .unwrap_or_else(utc_now_timestamp);
            let finished_epoch_seconds = lookup_payload_u64(
                payload,
                &["ingest_metrics", "ingest_finished_epoch_seconds"],
            )
            .unwrap_or(0);
            let db_path = lookup_payload_string(payload, &["db_path"]);
            let source_xml_path = lookup_payload_string(payload, &["source_xml_path"]);

            return IngestHistoryEntry {
                id: format!(
                    "success-{}-{}",
                    finished_epoch_seconds,
                    db_path.as_deref().unwrap_or("unknown")
                ),
                finished_at,
                status: IngestHistoryStatus::Success,
                source_xml_path,
                db_path,
                workout_count: lookup_payload_u64(payload, &["counts", "workouts"]),
                record_count: lookup_payload_u64(payload, &["counts", "records"]),
                workout_record_link_count: lookup_payload_u64(
                    payload,
                    &["counts", "workout_record_links"],
                ),
                ingest_duration_seconds: lookup_payload_f64(
                    payload,
                    &["ingest_metrics", "ingest_duration_seconds"],
                ),
                error: None,
            };
        }
    }

    IngestHistoryEntry {
        id: format!("failed-{}", utc_now_timestamp()),
        finished_at: utc_now_timestamp(),
        status: IngestHistoryStatus::Failed,
        source_xml_path: None,
        db_path: None,
        workout_count: None,
        record_count: None,
        workout_record_link_count: None,
        ingest_duration_seconds: None,
        error: event
            .error
            .clone()
            .or_else(|| Some("Dataset ingest failed.".into())),
    }
}

fn lookup_payload_string(payload: &Value, path: &[&str]) -> Option<String> {
    lookup_payload_value(payload, path)?
        .as_str()
        .map(ToOwned::to_owned)
}

fn lookup_payload_u64(payload: &Value, path: &[&str]) -> Option<u64> {
    lookup_payload_value(payload, path)?.as_u64()
}

fn lookup_payload_f64(payload: &Value, path: &[&str]) -> Option<f64> {
    lookup_payload_value(payload, path)?.as_f64()
}

fn lookup_payload_value<'a>(payload: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = payload;
    for segment in path {
        current = current.get(*segment)?;
    }
    Some(current)
}

fn utc_now_timestamp() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn lock_ingest_state<'a>(
    state: &'a State<'_, DesktopState>,
) -> Result<std::sync::MutexGuard<'a, Option<Arc<AtomicBool>>>, String> {
    state
        .ingest_cancellation
        .lock()
        .map_err(|_| "Failed to lock ingest state.".to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(DesktopState::default())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            load_current_dataset,
            load_ingest_history,
            load_dashboard,
            load_workout_dashboard,
            load_health_dashboard,
            load_workout_detail,
            load_workout_metric_series,
            run_export,
            start_ingest,
            cancel_ingest
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
