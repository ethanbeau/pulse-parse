mod backend;

pub use backend::{
    configure_connection_for_ingest, connect_database, export_workouts, get_dataset_info,
    ingest_export_xml, inspect_workouts, list_activity_types, load_health_overview,
    load_workout_dashboard_bundle, load_workout_detail, load_workout_metric_series,
    load_workout_summary_bundle, open_existing_database, parse_activity_filters,
    parse_workout_window, preprocess_export_xml, upsert_dataset_info, ExportOptions,
    HealthQueryOptions, IngestReport, WorkoutQueryOptions,
};

use health_core::{CurrentDataset, IngestHistoryEntry, StoredDatasetMetadata};
use std::fs;
use std::path::{Path, PathBuf};

pub const DATASET_DIRECTORY_NAME: &str = "datasets";
pub const ACTIVE_DATASET_NAME: &str = "current-dataset.sqlite";
pub const STAGING_DATASET_NAME: &str = "current-dataset.next.sqlite";
pub const DATASET_STATE_FILE_NAME: &str = "current-dataset.json";

pub fn dataset_directory_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join(DATASET_DIRECTORY_NAME)
}

pub fn active_dataset_db_path_for_dir(dataset_dir: &Path) -> PathBuf {
    dataset_dir.join(ACTIVE_DATASET_NAME)
}

pub fn staging_dataset_db_path_for_dir(dataset_dir: &Path) -> PathBuf {
    dataset_dir.join(STAGING_DATASET_NAME)
}

pub fn dataset_state_path_for_dir(config_dir: &Path) -> PathBuf {
    config_dir.join(DATASET_STATE_FILE_NAME)
}

pub fn current_dataset_from_metadata(
    db_path: &Path,
    metadata: Option<StoredDatasetMetadata>,
) -> Option<CurrentDataset> {
    if !db_path.is_file() {
        return None;
    }

    let metadata = metadata.unwrap_or_default();
    Some(CurrentDataset {
        db_path: path_to_string(db_path),
        xml_path: metadata.xml_path,
        workout_count: metadata.workout_count,
        record_count: metadata.record_count,
        workout_record_link_count: metadata.workout_record_link_count,
        source_xml_size_bytes: metadata.source_xml_size_bytes,
        ingest_duration_seconds: metadata.ingest_duration_seconds,
        last_ingested_epoch_seconds: metadata.last_ingested_epoch_seconds,
    })
}

pub fn read_dataset_metadata(path: &Path) -> Result<Option<StoredDatasetMetadata>, String> {
    if !path.exists() {
        return Ok(None);
    }

    let contents = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let metadata = serde_json::from_str(&contents).map_err(|error| error.to_string())?;
    Ok(Some(metadata))
}

pub fn write_dataset_metadata(path: &Path, metadata: &StoredDatasetMetadata) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let contents = serde_json::to_string_pretty(metadata).map_err(|error| error.to_string())?;
    fs::write(path, contents).map_err(|error| error.to_string())
}

pub fn read_ingest_history(path: &Path) -> Result<Vec<IngestHistoryEntry>, String> {
    Ok(read_dataset_metadata(path)?
        .map(|metadata| metadata.ingest_history)
        .unwrap_or_default())
}

pub fn append_ingest_history_entry(
    path: &Path,
    entry: IngestHistoryEntry,
    max_entries: usize,
) -> Result<(), String> {
    let mut metadata = read_dataset_metadata(path)?.unwrap_or_default();
    metadata.ingest_history.insert(0, entry);
    metadata.ingest_history.truncate(max_entries);
    write_dataset_metadata(path, &metadata)
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::{
        active_dataset_db_path_for_dir, append_ingest_history_entry, current_dataset_from_metadata,
        dataset_directory_path, dataset_state_path_for_dir, read_dataset_metadata,
        read_ingest_history, staging_dataset_db_path_for_dir, write_dataset_metadata,
        CurrentDataset, StoredDatasetMetadata,
    };
    use health_core::{IngestHistoryEntry, IngestHistoryStatus};
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn dataset_paths_use_stable_app_owned_names() {
        let app_data_dir = PathBuf::from("/tmp/pulse-parse");
        let dataset_dir = dataset_directory_path(&app_data_dir);

        assert_eq!(dataset_dir, app_data_dir.join("datasets"));
        assert_eq!(
            active_dataset_db_path_for_dir(&dataset_dir),
            dataset_dir.join("current-dataset.sqlite")
        );
        assert_eq!(
            staging_dataset_db_path_for_dir(&dataset_dir),
            dataset_dir.join("current-dataset.next.sqlite")
        );
        assert_eq!(
            dataset_state_path_for_dir(&PathBuf::from("/tmp/apple-health-config")),
            PathBuf::from("/tmp/apple-health-config/current-dataset.json")
        );
    }

    #[test]
    fn dataset_metadata_round_trips_through_disk_storage() {
        let temp_dir = unique_temp_dir();
        let metadata_path = dataset_state_path_for_dir(&temp_dir);
        let metadata = StoredDatasetMetadata {
            xml_path: Some("/tmp/export.xml".into()),
            workout_count: 42,
            record_count: 84,
            workout_record_link_count: 126,
            source_xml_size_bytes: 2048,
            ingest_duration_seconds: 12.5,
            last_ingested_epoch_seconds: 1234,
            ingest_history: vec![IngestHistoryEntry {
                id: "ingest-1".into(),
                finished_at: "2026-04-21T01:00:00Z".into(),
                status: IngestHistoryStatus::Success,
                source_xml_path: Some("/tmp/export.xml".into()),
                db_path: Some("/tmp/current-dataset.sqlite".into()),
                workout_count: Some(42),
                record_count: Some(84),
                workout_record_link_count: Some(126),
                ingest_duration_seconds: Some(12.5),
                error: None,
            }],
        };

        write_dataset_metadata(&metadata_path, &metadata).unwrap();
        let loaded = read_dataset_metadata(&metadata_path).unwrap();

        assert_eq!(loaded, Some(metadata));
        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn current_dataset_falls_back_when_metadata_is_missing() {
        let temp_dir = unique_temp_dir();
        let db_path = temp_dir.join("current-dataset.sqlite");
        fs::create_dir_all(&temp_dir).unwrap();
        fs::write(&db_path, "sqlite").unwrap();

        let dataset = current_dataset_from_metadata(&db_path, None).unwrap();

        assert_eq!(
            dataset,
            CurrentDataset {
                db_path: db_path.to_string_lossy().into_owned(),
                xml_path: None,
                workout_count: 0,
                record_count: 0,
                workout_record_link_count: 0,
                source_xml_size_bytes: 0,
                ingest_duration_seconds: 0.0,
                last_ingested_epoch_seconds: 0,
            }
        );
        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn ingest_history_can_be_appended_and_loaded() {
        let temp_dir = unique_temp_dir();
        let metadata_path = dataset_state_path_for_dir(&temp_dir);

        append_ingest_history_entry(
            &metadata_path,
            IngestHistoryEntry {
                id: "ingest-1".into(),
                finished_at: "2026-04-21T01:00:00Z".into(),
                status: IngestHistoryStatus::Success,
                source_xml_path: Some("/tmp/export.xml".into()),
                db_path: Some("/tmp/current-dataset.sqlite".into()),
                workout_count: Some(42),
                record_count: Some(84),
                workout_record_link_count: Some(126),
                ingest_duration_seconds: Some(12.5),
                error: None,
            },
            10,
        )
        .unwrap();

        let history = read_ingest_history(&metadata_path).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].id, "ingest-1");
        assert_eq!(history[0].status, IngestHistoryStatus::Success);

        fs::remove_dir_all(temp_dir).unwrap();
    }

    fn unique_temp_dir() -> PathBuf {
        env::temp_dir().join(format!(
            "pulse-parse-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
}
