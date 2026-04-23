export interface CurrentDataset {
	dbPath: string;
	xmlPath: string | null;
	workoutCount: number;
	recordCount: number;
	workoutRecordLinkCount: number;
	sourceXmlSizeBytes: number;
	ingestDurationSeconds: number;
	lastIngestedEpochSeconds: number;
}

export interface ActivityOption {
	activity_type: string;
	label: string;
	workout_count: number;
	first_start: string | null;
	last_end: string | null;
}

export interface InspectionOverall {
	workout_count: number;
	first_start: string | null;
	last_end: string | null;
	total_duration_seconds: number | null;
	total_distance_meters: number | null;
	total_energy_kilocalories: number | null;
}

export interface InspectionByActivity {
	activity_type: string;
	workout_count: number;
	first_start: string | null;
	last_end: string | null;
	total_duration_seconds: number | null;
	total_distance_meters: number | null;
	total_energy_kilocalories: number | null;
}

export interface SummaryOverall {
	workout_count: number;
	total_duration_hours: number | null;
	average_duration_minutes: number | null;
	total_distance_miles: number | null;
	average_distance_miles: number | null;
	total_energy_kcal: number | null;
	average_energy_kcal: number | null;
	average_heart_rate: number | null;
	max_heart_rate: number | null;
	heart_rate_sample_count: number;
	average_running_cadence_spm: number | null;
	max_running_cadence_spm: number | null;
}

export interface SummaryWorkoutCard {
	db_id: number;
	date: string;
	start: string;
	end: string;
	activity_type: string;
	type: string;
	title: string;
	location: string | null;
	source: string | null;
	duration_minutes: number | null;
	distance_miles: number | null;
	elevation_gain_ft: number | null;
	temperature_f: number | null;
	energy_kcal: number | null;
	avg_heart_rate: number | null;
	max_heart_rate: number | null;
	heart_rate_sample_count: number;
	avg_running_cadence_spm: number | null;
	max_running_cadence_spm: number | null;
	pace_min_per_mile: number | null;
	speed_mph: number | null;
	effort: string | null;
	summary: string;
}

export interface HealthOverviewMetric {
	key: string;
	label: string;
	category: string;
	record_type: string;
	summary_kind: "latest" | "total";
	snapshot: boolean;
	unit: string | null;
	sample_count: number;
	primary_label: string;
	primary_value: number | null;
	latest_value: number | null;
	latest_at: string | null;
	average_value: number | null;
	minimum_value: number | null;
	maximum_value: number | null;
	total_value: number | null;
	daily_average_value: number | null;
	best_day: string | null;
	best_day_value: number | null;
	trend_aggregation: "daily_average" | "daily_total";
	trend: Array<{
		date: string;
		value: number;
	}>;
}

export interface ActivityBreakdownRow {
	activity_type: string;
	type: string;
	count: number;
	total_duration_hours: number | null;
	average_duration_minutes: number | null;
	total_distance_miles: number | null;
	average_distance_miles: number | null;
	total_energy_kcal: number | null;
	average_heart_rate: number | null;
	max_heart_rate: number | null;
	heart_rate_sample_count: number;
	average_running_cadence_spm: number | null;
	max_running_cadence_spm: number | null;
}

export interface MetadataEntry {
	key: string;
	value: string | null;
}

export interface DerivedHeartRate {
	sample_count: number;
	average: number;
	minimum: number;
	maximum: number;
}

export interface DerivedMetrics {
	associated_record_count: number;
	speed_kph?: number;
	pace_seconds_per_km?: number;
	elevation_gain_ft?: number;
	temperature_f?: number;
	heart_rate?: DerivedHeartRate;
	record_type_counts?: Record<string, number>;
}

export interface WorkoutMetricPoint {
	timestamp: string;
	elapsedMinutes: number;
	value: number;
}

export interface WorkoutMetricSeries {
	key: string;
	label: string;
	unit: string | null;
	sampleCount: number;
	average: number;
	minimum: number;
	maximum: number;
	latestAt: string | null;
	latestValue: number | null;
	points: WorkoutMetricPoint[];
}

export interface LoadPerformance {
	total_duration_seconds: number;
	payload_size_bytes?: number;
}

export interface DashboardPerformance extends LoadPerformance {
	available_activity_types_duration_seconds?: number;
	health_overview_duration_seconds?: number;
	workout_sections_duration_seconds?: number;
}

export interface WorkoutMetricSeriesPerformance extends LoadPerformance {
	metric_series_count: number;
	point_count: number;
}

export interface WorkoutLinkedDataCounts {
	records: number;
	metadata: number;
	routes: number;
	events: number;
}

export interface WorkoutDetailPayload {
	db_id: number;
	uuid: string | null;
	activity_type: string;
	source_name: string | null;
	source_version: string | null;
	device: string | null;
	creation_date: string | null;
	start_date: string;
	end_date: string;
	duration_seconds: number | null;
	total_distance: number | null;
	total_distance_unit: string | null;
	total_distance_meters: number | null;
	total_energy_burned: number | null;
	total_energy_burned_unit: string | null;
	total_energy_burned_kilocalories: number | null;
	metadata: MetadataEntry[];
	linked_data_counts: WorkoutLinkedDataCounts;
	derived_metrics: DerivedMetrics;
}

export interface DashboardPayload {
	db_path: string;
	available_activity_types: ActivityOption[];
	health_overview: {
		dataset_info: Record<string, string>;
		filters: {
			start: string | null;
			end: string | null;
		};
		record_count: number;
		available_metric_count: number;
		first_start: string | null;
		last_end: string | null;
		metrics: HealthOverviewMetric[];
	};
	inspection: {
		dataset_info: Record<string, string>;
		filters: {
			start: string | null;
			end: string | null;
			activity_types: string[];
		};
		overall: InspectionOverall;
		by_activity_type: InspectionByActivity[];
	};
	summary: {
		dataset_info: Record<string, string>;
		filters: {
			start: string | null;
			end: string | null;
			activity_types: string[];
		};
		timeframe: {
			start: string | null;
			end: string | null;
			activity_types: string[];
		};
		workout_count: number;
		overall: SummaryOverall;
		activity_breakdown: ActivityBreakdownRow[];
		highlights: string[];
		workouts: SummaryWorkoutCard[];
	};
	performance: DashboardPerformance;
}

export interface WorkoutDetailResponse {
	db_path: string;
	workout: WorkoutDetailPayload;
	performance: LoadPerformance;
}

export interface WorkoutMetricSeriesResponse {
	db_path: string;
	workout_id: number;
	metric_series: WorkoutMetricSeries[];
	performance: WorkoutMetricSeriesPerformance;
}

export interface ExportResponse {
	format: "json" | "csv";
	path: string;
	workout_count: number;
	db_path: string;
}

export interface IngestProgressEvent {
	label: string;
	message: string;
}

export interface IngestHistoryEntry {
	id: string;
	finishedAt: string;
	status: "success" | "failed";
	sourceXmlPath: string | null;
	dbPath: string | null;
	workoutCount: number | null;
	recordCount: number | null;
	workoutRecordLinkCount: number | null;
	ingestDurationSeconds: number | null;
	error: string | null;
}

export interface IngestFinishedEvent {
	success: boolean;
	payload: {
		db_path: string;
		source_xml_path: string;
		counts: {
			workouts: number;
			records: number;
			workout_record_links: number;
		};
		ingest_metrics: {
			source_xml_size_bytes: number;
			ingest_started_at: string;
			ingest_finished_at: string;
			ingest_started_epoch_seconds: number;
			ingest_finished_epoch_seconds: number;
			ingest_duration_seconds: number;
		};
	} | null;
	error: string | null;
}
