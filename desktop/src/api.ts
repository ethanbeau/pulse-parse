import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

import type {
	CurrentDataset,
	DashboardPayload,
	ExportResponse,
	IngestFinishedEvent,
	IngestHistoryEntry,
	IngestProgressEvent,
	WorkoutDetailResponse,
	WorkoutMetricSeriesResponse,
} from "./types";

type DashboardRequestPayload = {
	start?: string;
	end?: string;
	activityTypes: string[];
	sourceQuery?: string;
	minDurationMinutes?: number;
	maxDurationMinutes?: number;
	location?: string;
	minDistanceMiles?: number;
	maxDistanceMiles?: number;
	minEnergyKcal?: number;
	maxEnergyKcal?: number;
	minAvgHeartRate?: number;
	maxAvgHeartRate?: number;
	minMaxHeartRate?: number;
	maxMaxHeartRate?: number;
	efforts: string[];
	requiresRouteData: boolean;
	requiresHeartRateSamples: boolean;
	healthStart?: string;
	healthEnd?: string;
	healthCategories: string[];
	healthMetricQuery?: string;
	healthSourceQuery?: string;
	healthOnlyWithSamples: boolean;
};

export interface DashboardRequest {
	start?: string;
	end?: string;
	activityTypes: string[];
	sourceQuery?: string;
	minDurationMinutes?: number;
	maxDurationMinutes?: number;
	location?: string;
	minDistanceMiles?: number;
	maxDistanceMiles?: number;
	minEnergyKcal?: number;
	maxEnergyKcal?: number;
	minAvgHeartRate?: number;
	maxAvgHeartRate?: number;
	minMaxHeartRate?: number;
	maxMaxHeartRate?: number;
	efforts: string[];
	requiresRouteData: boolean;
	requiresHeartRateSamples: boolean;
	healthStart?: string;
	healthEnd?: string;
	healthCategories: string[];
	healthMetricQuery?: string;
	healthSourceQuery?: string;
	healthOnlyWithSamples: boolean;
}

export interface IngestRequest {
	xmlPath: string;
	verbose?: boolean;
}

export interface ExportRequest {
	outputPath: string;
	exportFormat: "json" | "csv";
	summary: boolean;
	csvProfile?: "full" | "llm";
	start?: string;
	end?: string;
	activityTypes: string[];
	sourceQuery?: string;
	minDurationMinutes?: number;
	maxDurationMinutes?: number;
	location?: string;
	minDistanceMiles?: number;
	maxDistanceMiles?: number;
	minEnergyKcal?: number;
	maxEnergyKcal?: number;
	minAvgHeartRate?: number;
	maxAvgHeartRate?: number;
	minMaxHeartRate?: number;
	maxMaxHeartRate?: number;
	efforts: string[];
	requiresRouteData: boolean;
	requiresHeartRateSamples: boolean;
	verbose?: boolean;
}

export interface WorkoutDashboardResponse {
	db_path: string;
	inspection: DashboardPayload["inspection"];
	summary: DashboardPayload["summary"];
	performance: DashboardPayload["performance"];
}

export interface HealthDashboardResponse {
	db_path: string;
	health_overview: DashboardPayload["health_overview"];
	performance: DashboardPayload["performance"];
}

function serializeDashboardRequest(
	request: DashboardRequest,
): DashboardRequestPayload {
	return {
		start: request.start,
		end: request.end,
		activityTypes: request.activityTypes,
		sourceQuery: request.sourceQuery,
		minDurationMinutes: request.minDurationMinutes,
		maxDurationMinutes: request.maxDurationMinutes,
		location: request.location,
		minDistanceMiles: request.minDistanceMiles,
		maxDistanceMiles: request.maxDistanceMiles,
		minEnergyKcal: request.minEnergyKcal,
		maxEnergyKcal: request.maxEnergyKcal,
		minAvgHeartRate: request.minAvgHeartRate,
		maxAvgHeartRate: request.maxAvgHeartRate,
		minMaxHeartRate: request.minMaxHeartRate,
		maxMaxHeartRate: request.maxMaxHeartRate,
		efforts: request.efforts,
		requiresRouteData: request.requiresRouteData,
		requiresHeartRateSamples: request.requiresHeartRateSamples,
		healthStart: request.healthStart,
		healthEnd: request.healthEnd,
		healthCategories: request.healthCategories,
		healthMetricQuery: request.healthMetricQuery,
		healthSourceQuery: request.healthSourceQuery,
		healthOnlyWithSamples: request.healthOnlyWithSamples,
	};
}

export async function loadCurrentDataset(): Promise<CurrentDataset | null> {
	return invoke<CurrentDataset | null>("load_current_dataset");
}

export async function loadIngestHistory(): Promise<IngestHistoryEntry[]> {
	return invoke<IngestHistoryEntry[]>("load_ingest_history");
}

export async function loadDashboard(
	request: DashboardRequest,
): Promise<DashboardPayload> {
	return invoke<DashboardPayload>("load_dashboard", {
		request: serializeDashboardRequest(request),
	});
}

export async function loadWorkoutDashboard(
	request: DashboardRequest,
): Promise<WorkoutDashboardResponse> {
	return invoke<WorkoutDashboardResponse>("load_workout_dashboard", {
		request: serializeDashboardRequest(request),
	});
}

export async function loadHealthDashboard(
	request: DashboardRequest,
): Promise<HealthDashboardResponse> {
	return invoke<HealthDashboardResponse>("load_health_dashboard", {
		request: serializeDashboardRequest(request),
	});
}

export async function loadWorkoutDetail(
	workoutId: number,
): Promise<WorkoutDetailResponse> {
	return invoke<WorkoutDetailResponse>("load_workout_detail", {
		request: {
			workoutId,
		},
	});
}

export async function loadWorkoutMetricSeries(
	workoutId: number,
): Promise<WorkoutMetricSeriesResponse> {
	return invoke<WorkoutMetricSeriesResponse>("load_workout_metric_series", {
		request: {
			workoutId,
		},
	});
}

export async function runExport(
	request: ExportRequest,
): Promise<ExportResponse> {
	return invoke<ExportResponse>("run_export", {
		request: {
			outputPath: request.outputPath,
			exportFormat: request.exportFormat,
			summary: request.summary,
			csvProfile: request.csvProfile,
			start: request.start,
			end: request.end,
			activityTypes: request.activityTypes,
			sourceQuery: request.sourceQuery,
			minDurationMinutes: request.minDurationMinutes,
			maxDurationMinutes: request.maxDurationMinutes,
			location: request.location,
			minDistanceMiles: request.minDistanceMiles,
			maxDistanceMiles: request.maxDistanceMiles,
			minEnergyKcal: request.minEnergyKcal,
			maxEnergyKcal: request.maxEnergyKcal,
			minAvgHeartRate: request.minAvgHeartRate,
			maxAvgHeartRate: request.maxAvgHeartRate,
			minMaxHeartRate: request.minMaxHeartRate,
			maxMaxHeartRate: request.maxMaxHeartRate,
			efforts: request.efforts,
			requiresRouteData: request.requiresRouteData,
			requiresHeartRateSamples: request.requiresHeartRateSamples,
			verbose: request.verbose ?? false,
		},
	});
}

export async function startIngest(request: IngestRequest): Promise<void> {
	await invoke("start_ingest", {
		request: {
			xmlPath: request.xmlPath,
			verbose: request.verbose ?? false,
		},
	});
}

export async function cancelIngest(): Promise<boolean> {
	return invoke<boolean>("cancel_ingest");
}

export function listenForIngestProgress(
	handler: (event: IngestProgressEvent) => void,
): Promise<() => void> {
	return listen<IngestProgressEvent>("desktop://ingest-progress", (event) => {
		handler(event.payload);
	});
}

export function listenForIngestFinished(
	handler: (event: IngestFinishedEvent) => void,
): Promise<() => void> {
	return listen<IngestFinishedEvent>("desktop://ingest-finished", (event) => {
		handler(event.payload);
	});
}
