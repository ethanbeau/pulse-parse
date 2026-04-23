use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const INGEST_PROGRESS_EVENT_NAME: &str = "desktop://ingest-progress";
pub const INGEST_FINISHED_EVENT_NAME: &str = "desktop://ingest-finished";

pub trait ProgressReporter {
    fn progress(&mut self, message: &str) -> Result<(), String>;
    fn verbose(&mut self, message: &str) -> Result<(), String>;
    fn is_cancelled(&self) -> bool;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct CurrentDataset {
    pub db_path: String,
    pub xml_path: Option<String>,
    pub workout_count: u64,
    pub record_count: u64,
    pub workout_record_link_count: u64,
    pub source_xml_size_bytes: u64,
    pub ingest_duration_seconds: f64,
    pub last_ingested_epoch_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct StoredDatasetMetadata {
    pub xml_path: Option<String>,
    pub workout_count: u64,
    pub record_count: u64,
    pub workout_record_link_count: u64,
    pub source_xml_size_bytes: u64,
    pub ingest_duration_seconds: f64,
    pub last_ingested_epoch_seconds: u64,
    pub ingest_history: Vec<IngestHistoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum IngestHistoryStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngestHistoryEntry {
    pub id: String,
    pub finished_at: String,
    pub status: IngestHistoryStatus,
    pub source_xml_path: Option<String>,
    pub db_path: Option<String>,
    pub workout_count: Option<u64>,
    pub record_count: Option<u64>,
    pub workout_record_link_count: Option<u64>,
    pub ingest_duration_seconds: Option<f64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngestRequest {
    pub xml_path: String,
    pub verbose: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardRequest {
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
    pub requires_route_data: Option<bool>,
    pub requires_heart_rate_samples: Option<bool>,
    pub health_start: Option<String>,
    pub health_end: Option<String>,
    pub health_categories: Vec<String>,
    pub health_metric_query: Option<String>,
    pub health_source_query: Option<String>,
    pub health_only_with_samples: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkoutDetailRequest {
    pub workout_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkoutMetricSeriesRequest {
    pub workout_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExportRequest {
    pub output_path: String,
    pub export_format: String,
    pub summary: bool,
    pub csv_profile: Option<String>,
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
    pub requires_route_data: Option<bool>,
    pub requires_heart_rate_samples: Option<bool>,
    pub verbose: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngestProgressEvent {
    pub label: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngestFinishedEvent {
    pub success: bool,
    pub payload: Option<Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct BridgeProgressLine {
    pub label: String,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct BridgeResultLine {
    pub payload: Value,
}
