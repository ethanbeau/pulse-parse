import { open, save } from "@tauri-apps/plugin-dialog";
import {
	AlertCircleIcon,
	ArrowLeftIcon,
	ChevronDownIcon,
	ChevronUpIcon,
	CircleHelpIcon,
	DownloadIcon,
	FileUpIcon,
	FilterIcon,
	LoaderCircleIcon,
	MonitorIcon,
	MoonIcon,
	RefreshCwIcon,
	SettingsIcon,
	SunIcon,
	XIcon,
} from "lucide-react";
import {
	type ReactNode,
	useCallback,
	useEffect,
	useId,
	useMemo,
	useRef,
	useState,
} from "react";
import {
	Bar,
	BarChart,
	CartesianGrid,
	Line,
	LineChart,
	ResponsiveContainer,
	Tooltip,
	XAxis,
	YAxis,
} from "recharts";

import { ActivityTypePicker } from "@/components/activity-type-picker";
import { DatePickerField } from "@/components/date-picker-field";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import {
	Popover,
	PopoverContent,
	PopoverDescription,
	PopoverHeader,
	PopoverTitle,
	PopoverTrigger,
} from "@/components/ui/popover";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import {
	cancelIngest,
	listenForIngestFinished,
	listenForIngestProgress,
	loadCurrentDataset,
	loadDashboard,
	loadHealthDashboard,
	loadIngestHistory,
	loadWorkoutDashboard,
	loadWorkoutDetail,
	loadWorkoutMetricSeries,
	runExport,
	startIngest,
} from "./api";
import type {
	ActivityBreakdownRow,
	CurrentDataset,
	DashboardPayload,
	HealthOverviewMetric,
	IngestFinishedEvent,
	IngestHistoryEntry,
	IngestProgressEvent,
	SummaryWorkoutCard,
	WorkoutDetailPayload,
	WorkoutMetricSeries,
} from "./types";

interface FiltersState {
	start: string;
	end: string;
	activityTypes: string[];
	sourceQuery: string;
	minDurationMinutes: string;
	maxDurationMinutes: string;
	location: "" | "indoor" | "outdoor";
	minDistanceMiles: string;
	maxDistanceMiles: string;
	minEnergyKcal: string;
	maxEnergyKcal: string;
	minAvgHeartRate: string;
	maxAvgHeartRate: string;
	minMaxHeartRate: string;
	maxMaxHeartRate: string;
	efforts: string[];
	requiresRouteData: boolean;
	requiresHeartRateSamples: boolean;
}

interface HealthFiltersState {
	start: string;
	end: string;
	categories: string[];
	metricQuery: string;
	sourceQuery: string;
	onlyWithSamples: boolean;
}

const EMPTY_FILTERS: FiltersState = {
	start: "",
	end: "",
	activityTypes: [],
	sourceQuery: "",
	minDurationMinutes: "",
	maxDurationMinutes: "",
	location: "",
	minDistanceMiles: "",
	maxDistanceMiles: "",
	minEnergyKcal: "",
	maxEnergyKcal: "",
	minAvgHeartRate: "",
	maxAvgHeartRate: "",
	minMaxHeartRate: "",
	maxMaxHeartRate: "",
	efforts: [],
	requiresRouteData: false,
	requiresHeartRateSamples: false,
};

const EMPTY_HEALTH_FILTERS: HealthFiltersState = {
	start: "",
	end: "",
	categories: [],
	metricQuery: "",
	sourceQuery: "",
	onlyWithSamples: true,
};

type ThemeMode = "light" | "dark" | "system";
type MeasurementSystem = "imperial" | "metric";
type AppPage = "dashboard" | "settings" | "help";
type DashboardSection = "data" | "health" | "workouts";
type ExportKind = "json" | "summary" | "csv" | "csv-summary";

const APP_NAME = "Pulse Parse";
const ACCENT_PRIMARY_COLOR = "#A63B1E";
const ACCENT_SECONDARY_COLOR = "#E6745D";
const THEME_STORAGE_KEY = "pulse-parse-theme";
const DEFAULT_THEME_MODE: ThemeMode = "system";
const MEASUREMENT_SYSTEM_STORAGE_KEY = "pulse-parse-measurement-system";
const DEFAULT_HEALTH_METRICS_EXPANDED_STORAGE_KEY =
	"pulse-parse-default-health-metrics-expanded";
const DEFAULT_WORKOUT_METRICS_EXPANDED_STORAGE_KEY =
	"pulse-parse-default-workout-metrics-expanded";
const HEALTH_SNAPSHOT_LIMIT = 6;
const WORKOUT_METRIC_PREVIEW_LIMIT = 6;
const HEALTH_CATEGORY_ORDER = [
	"Cardio",
	"Recovery",
	"Vitals",
	"Activity",
	"Mobility",
	"Body",
];
const WORKOUT_LOCATION_OPTIONS = [
	{ value: "", label: "All" },
	{ value: "indoor", label: "Indoor" },
	{ value: "outdoor", label: "Outdoor" },
] as const;
const WORKOUT_EFFORT_OPTIONS = [
	"easy",
	"easy-moderate",
	"moderate",
	"hard",
	"very hard",
] as const;

function getChartPalette(resolvedThemeMode: "light" | "dark") {
	return resolvedThemeMode === "dark"
		? {
				grid: "rgba(230, 116, 93, 0.22)",
				tick: "#f3c7bd",
				workouts: ACCENT_PRIMARY_COLOR,
				distance: ACCENT_SECONDARY_COLOR,
			}
		: {
				grid: "rgba(166, 59, 30, 0.16)",
				tick: "#6f4e46",
				workouts: ACCENT_PRIMARY_COLOR,
				distance: ACCENT_SECONDARY_COLOR,
			};
}

function readStoredPreference(key: string) {
	return window.localStorage.getItem(key);
}

function roundLoadMetric(value: number): number {
	return Math.round(value * 10) / 10;
}

function logLoadPerformance(
	label: string,
	startedAt: number,
	performance: { total_duration_seconds: number; payload_size_bytes?: number },
) {
	if (typeof window === "undefined") {
		return;
	}

	const roundTripMs = window.performance.now() - startedAt;
	const backendMs = performance.total_duration_seconds * 1000;
	const bridgeMs = Math.max(roundTripMs - backendMs, 0);

	console.info(`[perf] ${label}`, {
		roundTripMs: roundLoadMetric(roundTripMs),
		backendMs: roundLoadMetric(backendMs),
		bridgeMs: roundLoadMetric(bridgeMs),
		payloadSizeKb:
			performance.payload_size_bytes === undefined
				? undefined
				: roundLoadMetric(performance.payload_size_bytes / 1024),
	});
}

function App() {
	const [currentPage, setCurrentPage] = useState<AppPage>("dashboard");
	const [currentDashboardSection, setCurrentDashboardSection] =
		useState<DashboardSection>("workouts");
	const [filters, setFilters] = useState<FiltersState>(EMPTY_FILTERS);
	const [healthFilters, setHealthFilters] =
		useState<HealthFiltersState>(EMPTY_HEALTH_FILTERS);
	const [currentDataset, setCurrentDataset] = useState<CurrentDataset | null>(
		null,
	);
	const [dashboard, setDashboard] = useState<DashboardPayload | null>(null);
	const [selectedWorkoutId, setSelectedWorkoutId] = useState<number | null>(
		null,
	);
	const [workoutDetail, setWorkoutDetail] =
		useState<WorkoutDetailPayload | null>(null);
	const [workoutMetricSeries, setWorkoutMetricSeries] = useState<
		WorkoutMetricSeries[]
	>([]);
	const [hasLoadedWorkoutMetricSeries, setHasLoadedWorkoutMetricSeries] =
		useState(false);
	const [workoutSearch, setWorkoutSearch] = useState("");
	const [selectedWorkoutMetricKey, setSelectedWorkoutMetricKey] = useState<
		string | null
	>(null);
	const [latestIngestEvent, setLatestIngestEvent] =
		useState<IngestProgressEvent | null>(null);
	const [ingestHistory, setIngestHistory] = useState<IngestHistoryEntry[]>([]);
	const [ingestStartedAt, setIngestStartedAt] = useState<number | null>(null);
	const [ingestElapsedSeconds, setIngestElapsedSeconds] = useState(0);
	const [errorMessage, setErrorMessage] = useState<string | null>(null);
	const [isLoadingDashboard, setIsLoadingDashboard] = useState(false);
	const [isLoadingWorkout, setIsLoadingWorkout] = useState(false);
	const [isLoadingWorkoutMetricSeries, setIsLoadingWorkoutMetricSeries] =
		useState(false);
	const [isIngesting, setIsIngesting] = useState(false);
	const [isExporting, setIsExporting] = useState(false);
	const [isWorkoutFiltersVisible, setIsWorkoutFiltersVisible] = useState(false);
	const [isWorkoutAdvancedFiltersOpen, setIsWorkoutAdvancedFiltersOpen] =
		useState(false);
	const [isHealthFiltersVisible, setIsHealthFiltersVisible] = useState(false);
	const [isExportPopoverOpen, setIsExportPopoverOpen] = useState(false);
	const [measurementSystem, setMeasurementSystem] = useState<MeasurementSystem>(
		() => {
			if (typeof window === "undefined") {
				return "imperial";
			}

			const storedMeasurementSystem = readStoredPreference(
				MEASUREMENT_SYSTEM_STORAGE_KEY,
			);
			if (
				storedMeasurementSystem === "imperial" ||
				storedMeasurementSystem === "metric"
			) {
				return storedMeasurementSystem;
			}

			return "imperial";
		},
	);
	const [defaultHealthMetricsExpanded, setDefaultHealthMetricsExpanded] =
		useState(() => {
			if (typeof window === "undefined") {
				return false;
			}
			return (
				readStoredPreference(DEFAULT_HEALTH_METRICS_EXPANDED_STORAGE_KEY) ===
				"true"
			);
		});
	const [defaultWorkoutMetricsExpanded, setDefaultWorkoutMetricsExpanded] =
		useState(() => {
			if (typeof window === "undefined") {
				return false;
			}
			return (
				readStoredPreference(DEFAULT_WORKOUT_METRICS_EXPANDED_STORAGE_KEY) ===
				"true"
			);
		});
	const [isHealthMetricsExpanded, setIsHealthMetricsExpanded] = useState(() => {
		if (typeof window === "undefined") {
			return false;
		}
		return (
			readStoredPreference(DEFAULT_HEALTH_METRICS_EXPANDED_STORAGE_KEY) ===
			"true"
		);
	});
	const [expandedWorkoutMetricsForId, setExpandedWorkoutMetricsForId] =
		useState<number | null>(null);
	const [selectedHealthMetricKey, setSelectedHealthMetricKey] = useState<
		string | null
	>(null);
	const [themeMode, setThemeMode] = useState<ThemeMode>(() => {
		if (typeof window === "undefined") {
			return DEFAULT_THEME_MODE;
		}

		const storedThemeMode = readStoredPreference(THEME_STORAGE_KEY);
		if (
			storedThemeMode === "light" ||
			storedThemeMode === "dark" ||
			storedThemeMode === "system"
		) {
			return storedThemeMode;
		}

		return DEFAULT_THEME_MODE;
	});
	const [systemPrefersDark, setSystemPrefersDark] = useState(() => {
		if (typeof window === "undefined") {
			return false;
		}

		return window.matchMedia("(prefers-color-scheme: dark)").matches;
	});
	const filtersRef = useRef(filters);
	const healthFiltersRef = useRef(healthFilters);
	const selectedWorkoutIdRef = useRef<number | null>(selectedWorkoutId);
	const workoutDetailCacheRef = useRef(new Map<number, WorkoutDetailPayload>());
	const workoutMetricSeriesCacheRef = useRef(
		new Map<number, WorkoutMetricSeries[]>(),
	);
	const workoutDetailRequestIdRef = useRef(0);
	const workoutMetricSeriesRequestIdRef = useRef(0);
	const workoutDetailLoadingIdRef = useRef<number | null>(null);
	const workoutMetricSeriesLoadingIdRef = useRef<number | null>(null);

	useEffect(() => {
		filtersRef.current = filters;
	}, [filters]);

	useEffect(() => {
		healthFiltersRef.current = healthFilters;
	}, [healthFilters]);

	useEffect(() => {
		selectedWorkoutIdRef.current = selectedWorkoutId;
	}, [selectedWorkoutId]);

	useEffect(() => {
		const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
		const handleChange = (event: MediaQueryListEvent) => {
			setSystemPrefersDark(event.matches);
		};

		mediaQuery.addEventListener("change", handleChange);

		return () => {
			mediaQuery.removeEventListener("change", handleChange);
		};
	}, []);

	const resolvedThemeMode =
		themeMode === "system" ? (systemPrefersDark ? "dark" : "light") : themeMode;

	useEffect(() => {
		document.documentElement.classList.toggle(
			"dark",
			resolvedThemeMode === "dark",
		);
		window.localStorage.setItem(THEME_STORAGE_KEY, themeMode);
	}, [resolvedThemeMode, themeMode]);

	useEffect(() => {
		window.localStorage.setItem(
			MEASUREMENT_SYSTEM_STORAGE_KEY,
			measurementSystem,
		);
	}, [measurementSystem]);

	useEffect(() => {
		window.localStorage.setItem(
			DEFAULT_HEALTH_METRICS_EXPANDED_STORAGE_KEY,
			String(defaultHealthMetricsExpanded),
		);
	}, [defaultHealthMetricsExpanded]);

	useEffect(() => {
		window.localStorage.setItem(
			DEFAULT_WORKOUT_METRICS_EXPANDED_STORAGE_KEY,
			String(defaultWorkoutMetricsExpanded),
		);
	}, [defaultWorkoutMetricsExpanded]);

	const clearWorkoutCaches = useCallback(() => {
		workoutDetailCacheRef.current.clear();
		workoutMetricSeriesCacheRef.current.clear();
		workoutDetailRequestIdRef.current += 1;
		workoutMetricSeriesRequestIdRef.current += 1;
		workoutDetailLoadingIdRef.current = null;
		workoutMetricSeriesLoadingIdRef.current = null;
		setWorkoutDetail(null);
		setWorkoutMetricSeries([]);
		setHasLoadedWorkoutMetricSeries(false);
		setIsLoadingWorkout(false);
		setIsLoadingWorkoutMetricSeries(false);
	}, []);

	const resetSelectedWorkout = useCallback(() => {
		selectedWorkoutIdRef.current = null;
		workoutDetailRequestIdRef.current += 1;
		workoutMetricSeriesRequestIdRef.current += 1;
		workoutDetailLoadingIdRef.current = null;
		workoutMetricSeriesLoadingIdRef.current = null;
		setSelectedWorkoutId(null);
		setWorkoutDetail(null);
		setWorkoutMetricSeries([]);
		setHasLoadedWorkoutMetricSeries(false);
		setIsLoadingWorkout(false);
		setIsLoadingWorkoutMetricSeries(false);
		setSelectedWorkoutMetricKey(null);
		setExpandedWorkoutMetricsForId(null);
	}, []);

	const restoreSelectedWorkoutFromCache = useCallback((workoutId: number) => {
		setWorkoutDetail(workoutDetailCacheRef.current.get(workoutId) ?? null);
		const cachedMetricSeries =
			workoutMetricSeriesCacheRef.current.get(workoutId);
		setWorkoutMetricSeries(cachedMetricSeries ?? []);
		setHasLoadedWorkoutMetricSeries(cachedMetricSeries !== undefined);
		setIsLoadingWorkout(
			workoutDetailCacheRef.current.get(workoutId) === undefined &&
				workoutDetailLoadingIdRef.current === workoutId,
		);
		setIsLoadingWorkoutMetricSeries(
			cachedMetricSeries === undefined &&
				workoutMetricSeriesLoadingIdRef.current === workoutId,
		);
	}, []);

	const ensureWorkoutDetailLoaded = useCallback(async (workoutId: number) => {
		const cachedWorkoutDetail = workoutDetailCacheRef.current.get(workoutId);
		if (cachedWorkoutDetail) {
			if (selectedWorkoutIdRef.current === workoutId) {
				setWorkoutDetail(cachedWorkoutDetail);
				setIsLoadingWorkout(false);
			}
			return cachedWorkoutDetail;
		}

		const requestId = workoutDetailRequestIdRef.current + 1;
		workoutDetailRequestIdRef.current = requestId;
		workoutDetailLoadingIdRef.current = workoutId;
		if (selectedWorkoutIdRef.current === workoutId) {
			setIsLoadingWorkout(true);
		}

		const startedAt = window.performance.now();

		try {
			const response = await loadWorkoutDetail(workoutId);
			logLoadPerformance(
				`selected workout ${workoutId}`,
				startedAt,
				response.performance,
			);
			workoutDetailCacheRef.current.set(workoutId, response.workout);

			if (
				selectedWorkoutIdRef.current === workoutId &&
				workoutDetailRequestIdRef.current === requestId
			) {
				setWorkoutDetail(response.workout);
			}

			return response.workout;
		} catch (error) {
			if (
				selectedWorkoutIdRef.current === workoutId &&
				workoutDetailRequestIdRef.current === requestId
			) {
				setWorkoutDetail(null);
				setErrorMessage(stringifyError(error));
			}
			throw error;
		} finally {
			if (
				workoutDetailRequestIdRef.current === requestId &&
				workoutDetailLoadingIdRef.current === workoutId
			) {
				workoutDetailLoadingIdRef.current = null;
			}
			if (
				selectedWorkoutIdRef.current === workoutId &&
				workoutDetailRequestIdRef.current === requestId
			) {
				setIsLoadingWorkout(false);
			}
		}
	}, []);

	const ensureWorkoutMetricSeriesLoaded = useCallback(
		async (workoutId: number) => {
			const cachedMetricSeries =
				workoutMetricSeriesCacheRef.current.get(workoutId);
			if (cachedMetricSeries !== undefined) {
				if (selectedWorkoutIdRef.current === workoutId) {
					setWorkoutMetricSeries(cachedMetricSeries);
					setHasLoadedWorkoutMetricSeries(true);
					setIsLoadingWorkoutMetricSeries(false);
				}
				return cachedMetricSeries;
			}

			const requestId = workoutMetricSeriesRequestIdRef.current + 1;
			workoutMetricSeriesRequestIdRef.current = requestId;
			workoutMetricSeriesLoadingIdRef.current = workoutId;
			if (selectedWorkoutIdRef.current === workoutId) {
				setIsLoadingWorkoutMetricSeries(true);
			}

			const startedAt = window.performance.now();

			try {
				const response = await loadWorkoutMetricSeries(workoutId);
				logLoadPerformance(
					`workout metric series ${workoutId}`,
					startedAt,
					response.performance,
				);
				workoutMetricSeriesCacheRef.current.set(
					workoutId,
					response.metric_series,
				);

				if (
					selectedWorkoutIdRef.current === workoutId &&
					workoutMetricSeriesRequestIdRef.current === requestId
				) {
					setWorkoutMetricSeries(response.metric_series);
					setHasLoadedWorkoutMetricSeries(true);
				}

				return response.metric_series;
			} catch (error) {
				if (
					selectedWorkoutIdRef.current === workoutId &&
					workoutMetricSeriesRequestIdRef.current === requestId
				) {
					setErrorMessage(stringifyError(error));
				}
				throw error;
			} finally {
				if (
					workoutMetricSeriesRequestIdRef.current === requestId &&
					workoutMetricSeriesLoadingIdRef.current === workoutId
				) {
					workoutMetricSeriesLoadingIdRef.current = null;
				}
				if (
					selectedWorkoutIdRef.current === workoutId &&
					workoutMetricSeriesRequestIdRef.current === requestId
				) {
					setIsLoadingWorkoutMetricSeries(false);
				}
			}
		},
		[],
	);

	const refreshCurrentDataset = useCallback(async () => {
		const dataset = await loadCurrentDataset();
		setCurrentDataset(dataset);
		return dataset;
	}, []);

	const refreshIngestHistory = useCallback(async () => {
		const history = await loadIngestHistory();
		setIngestHistory(history);
		return history;
	}, []);

	const selectWorkout = useCallback(
		(workoutId: number) => {
			selectedWorkoutIdRef.current = workoutId;
			setSelectedWorkoutId(workoutId);
			setSelectedWorkoutMetricKey(null);
			setExpandedWorkoutMetricsForId(
				defaultWorkoutMetricsExpanded ? workoutId : null,
			);
			restoreSelectedWorkoutFromCache(workoutId);
			void ensureWorkoutDetailLoaded(workoutId);
		},
		[
			defaultWorkoutMetricsExpanded,
			ensureWorkoutDetailLoaded,
			restoreSelectedWorkoutFromCache,
		],
	);

	const buildDashboardRequest = useCallback(
		(nextFilters: FiltersState, nextHealthFilters: HealthFiltersState) => ({
			start: nextFilters.start || undefined,
			end: nextFilters.end || undefined,
			activityTypes: nextFilters.activityTypes,
			sourceQuery: nextFilters.sourceQuery || undefined,
			minDurationMinutes: parseOptionalNumber(nextFilters.minDurationMinutes),
			maxDurationMinutes: parseOptionalNumber(nextFilters.maxDurationMinutes),
			location: nextFilters.location || undefined,
			minDistanceMiles: parseOptionalNumber(nextFilters.minDistanceMiles),
			maxDistanceMiles: parseOptionalNumber(nextFilters.maxDistanceMiles),
			minEnergyKcal: parseOptionalNumber(nextFilters.minEnergyKcal),
			maxEnergyKcal: parseOptionalNumber(nextFilters.maxEnergyKcal),
			minAvgHeartRate: parseOptionalNumber(nextFilters.minAvgHeartRate),
			maxAvgHeartRate: parseOptionalNumber(nextFilters.maxAvgHeartRate),
			minMaxHeartRate: parseOptionalNumber(nextFilters.minMaxHeartRate),
			maxMaxHeartRate: parseOptionalNumber(nextFilters.maxMaxHeartRate),
			efforts: nextFilters.efforts,
			requiresRouteData: nextFilters.requiresRouteData,
			requiresHeartRateSamples: nextFilters.requiresHeartRateSamples,
			healthStart: nextHealthFilters.start || undefined,
			healthEnd: nextHealthFilters.end || undefined,
			healthCategories: nextHealthFilters.categories,
			healthMetricQuery: nextHealthFilters.metricQuery || undefined,
			healthSourceQuery: nextHealthFilters.sourceQuery || undefined,
			healthOnlyWithSamples: nextHealthFilters.onlyWithSamples,
		}),
		[],
	);

	const syncSelectedWorkoutSelection = useCallback(
		(workouts: SummaryWorkoutCard[]) => {
			const currentSelectedWorkoutId = selectedWorkoutIdRef.current;
			const sortedWorkouts = sortWorkoutsNewestFirst(workouts);
			const nextSelectedWorkoutId =
				currentSelectedWorkoutId !== null &&
				sortedWorkouts.some(
					(workout) => workout.db_id === currentSelectedWorkoutId,
				)
					? currentSelectedWorkoutId
					: (sortedWorkouts[0]?.db_id ?? null);

			if (nextSelectedWorkoutId === null) {
				resetSelectedWorkout();
				return;
			}

			if (nextSelectedWorkoutId !== currentSelectedWorkoutId) {
				selectWorkout(nextSelectedWorkoutId);
				return;
			}

			restoreSelectedWorkoutFromCache(nextSelectedWorkoutId);
			if (!workoutDetailCacheRef.current.has(nextSelectedWorkoutId)) {
				void ensureWorkoutDetailLoaded(nextSelectedWorkoutId);
			}
		},
		[
			ensureWorkoutDetailLoaded,
			resetSelectedWorkout,
			restoreSelectedWorkoutFromCache,
			selectWorkout,
		],
	);

	const loadDashboardForFilters = useCallback(
		async (
			nextFilters: FiltersState,
			nextHealthFilters: HealthFiltersState,
		) => {
			setErrorMessage(null);
			setIsLoadingDashboard(true);

			try {
				const startedAt = window.performance.now();
				const nextDashboard = await loadDashboard(
					buildDashboardRequest(nextFilters, nextHealthFilters),
				);
				logLoadPerformance("dashboard", startedAt, nextDashboard.performance);
				setDashboard(nextDashboard);
				syncSelectedWorkoutSelection(nextDashboard.summary.workouts);
			} catch (error) {
				setDashboard(null);
				resetSelectedWorkout();
				setErrorMessage(stringifyError(error));
			} finally {
				setIsLoadingDashboard(false);
			}
		},
		[buildDashboardRequest, resetSelectedWorkout, syncSelectedWorkoutSelection],
	);

	const loadWorkoutSectionsForFilters = useCallback(
		async (
			nextFilters: FiltersState,
			nextHealthFilters: HealthFiltersState,
		) => {
			if (!dashboard) {
				await loadDashboardForFilters(nextFilters, nextHealthFilters);
				return;
			}

			setErrorMessage(null);
			setIsLoadingDashboard(true);

			try {
				const startedAt = window.performance.now();
				const nextWorkoutDashboard = await loadWorkoutDashboard(
					buildDashboardRequest(nextFilters, nextHealthFilters),
				);
				logLoadPerformance(
					"workout dashboard section",
					startedAt,
					nextWorkoutDashboard.performance,
				);
				setDashboard((current) =>
					current
						? {
								...current,
								inspection: nextWorkoutDashboard.inspection,
								summary: nextWorkoutDashboard.summary,
								performance: {
									...current.performance,
									...nextWorkoutDashboard.performance,
								},
							}
						: current,
				);
				syncSelectedWorkoutSelection(nextWorkoutDashboard.summary.workouts);
			} catch (error) {
				setDashboard(null);
				resetSelectedWorkout();
				setErrorMessage(stringifyError(error));
			} finally {
				setIsLoadingDashboard(false);
			}
		},
		[
			buildDashboardRequest,
			dashboard,
			loadDashboardForFilters,
			resetSelectedWorkout,
			syncSelectedWorkoutSelection,
		],
	);

	const loadHealthSectionForFilters = useCallback(
		async (
			nextFilters: FiltersState,
			nextHealthFilters: HealthFiltersState,
		) => {
			if (!dashboard) {
				await loadDashboardForFilters(nextFilters, nextHealthFilters);
				return;
			}

			setErrorMessage(null);
			setIsLoadingDashboard(true);

			try {
				const startedAt = window.performance.now();
				const nextHealthDashboard = await loadHealthDashboard(
					buildDashboardRequest(nextFilters, nextHealthFilters),
				);
				logLoadPerformance(
					"health dashboard section",
					startedAt,
					nextHealthDashboard.performance,
				);
				setDashboard((current) =>
					current
						? {
								...current,
								health_overview: nextHealthDashboard.health_overview,
								performance: {
									...current.performance,
									...nextHealthDashboard.performance,
								},
							}
						: current,
				);
			} catch (error) {
				setDashboard(null);
				resetSelectedWorkout();
				setErrorMessage(stringifyError(error));
			} finally {
				setIsLoadingDashboard(false);
			}
		},
		[
			buildDashboardRequest,
			dashboard,
			loadDashboardForFilters,
			resetSelectedWorkout,
		],
	);

	const handleLoadDashboard = useCallback(async () => {
		await loadDashboardForFilters(filters, healthFilters);
	}, [filters, healthFilters, loadDashboardForFilters]);

	const handleIngestFinished = useCallback(
		async (event: IngestFinishedEvent) => {
			setIsIngesting(false);
			setIngestStartedAt(null);
			setIngestElapsedSeconds(0);
			clearWorkoutCaches();
			resetSelectedWorkout();
			await refreshIngestHistory();
			if (!event.success || !event.payload) {
				await refreshCurrentDataset();
				setErrorMessage(event.error ?? "Dataset ingest failed.");
				return;
			}

			await refreshCurrentDataset();
			await loadDashboardForFilters(
				filtersRef.current,
				healthFiltersRef.current,
			);
		},
		[
			clearWorkoutCaches,
			loadDashboardForFilters,
			refreshCurrentDataset,
			refreshIngestHistory,
			resetSelectedWorkout,
		],
	);

	useEffect(() => {
		let isActive = true;

		void (async () => {
			try {
				const [dataset, history] = await Promise.all([
					loadCurrentDataset(),
					loadIngestHistory(),
				]);
				if (!isActive) {
					return;
				}
				setCurrentDataset(dataset);
				setIngestHistory(history);

				if (dataset) {
					await loadDashboardForFilters(EMPTY_FILTERS, EMPTY_HEALTH_FILTERS);
				}
			} catch (error) {
				if (isActive) {
					setErrorMessage(stringifyError(error));
				}
			}
		})();

		return () => {
			isActive = false;
		};
	}, [loadDashboardForFilters]);

	useEffect(() => {
		let unlistenProgress: (() => void) | undefined;
		let unlistenFinished: (() => void) | undefined;

		void listenForIngestProgress((event) => {
			setLatestIngestEvent(event);
		}).then((unlisten) => {
			unlistenProgress = unlisten;
		});

		void listenForIngestFinished((event) => {
			void handleIngestFinished(event);
		}).then((unlisten) => {
			unlistenFinished = unlisten;
		});

		return () => {
			unlistenProgress?.();
			unlistenFinished?.();
		};
	}, [handleIngestFinished]);

	useEffect(() => {
		if (!isIngesting || ingestStartedAt === null) {
			return;
		}

		const intervalId = window.setInterval(() => {
			setIngestElapsedSeconds(
				Math.floor((Date.now() - ingestStartedAt) / 1000),
			);
		}, 1000);

		return () => {
			window.clearInterval(intervalId);
		};
	}, [ingestStartedAt, isIngesting]);

	const sortedWorkouts = useMemo(
		() => sortWorkoutsNewestFirst(dashboard?.summary.workouts ?? []),
		[dashboard],
	);

	const trendData = useMemo(() => {
		const grouped = new Map<
			string,
			{ date: string; workouts: number; distance: number }
		>();
		for (const workout of sortedWorkouts) {
			const current = grouped.get(workout.date) ?? {
				date: workout.date,
				workouts: 0,
				distance: 0,
			};
			current.workouts += 1;
			current.distance += convertDisplayValue(
				workout.distance_miles ?? 0,
				"mi",
				measurementSystem,
			).value;
			grouped.set(workout.date, current);
		}

		return [...grouped.values()].sort((left, right) =>
			left.date.localeCompare(right.date),
		);
	}, [measurementSystem, sortedWorkouts]);

	const selectedWorkoutCard = useMemo(() => {
		if (!dashboard || selectedWorkoutId === null) {
			return null;
		}
		return (
			sortedWorkouts.find((workout) => workout.db_id === selectedWorkoutId) ??
			null
		);
	}, [dashboard, selectedWorkoutId, sortedWorkouts]);

	const visibleWorkouts = useMemo(() => {
		const query = workoutSearch.trim().toLowerCase();
		if (!query) {
			return sortedWorkouts;
		}

		return sortedWorkouts.filter((workout) =>
			[
				workout.title,
				workout.type,
				workout.summary,
				workout.date,
				workout.activity_type,
				workout.effort,
			]
				.filter(Boolean)
				.some((value) => value?.toLowerCase().includes(query)),
		);
	}, [sortedWorkouts, workoutSearch]);

	const selectedWorkoutMetric = useMemo(() => {
		if (!selectedWorkoutMetricKey) {
			return null;
		}

		return (
			workoutMetricSeries.find(
				(metric) => metric.key === selectedWorkoutMetricKey,
			) ?? null
		);
	}, [selectedWorkoutMetricKey, workoutMetricSeries]);

	const chartPalette = useMemo(
		() => getChartPalette(resolvedThemeMode),
		[resolvedThemeMode],
	);

	const healthMetrics = useMemo(
		() => dashboard?.health_overview.metrics ?? [],
		[dashboard],
	);

	const snapshotHealthMetrics = useMemo(() => {
		const preferred = [...healthMetrics]
			.filter((metric) => metric.snapshot)
			.sort((left, right) => right.sample_count - left.sample_count);
		if (preferred.length >= HEALTH_SNAPSHOT_LIMIT) {
			return preferred.slice(0, HEALTH_SNAPSHOT_LIMIT);
		}

		const remaining = [...healthMetrics]
			.filter((metric) => !metric.snapshot)
			.sort((left, right) => right.sample_count - left.sample_count);
		return [...preferred, ...remaining].slice(0, HEALTH_SNAPSHOT_LIMIT);
	}, [healthMetrics]);

	const expandedHealthGroups = useMemo(() => {
		const grouped = new Map<string, HealthOverviewMetric[]>();
		for (const metric of healthMetrics) {
			const current = grouped.get(metric.category) ?? [];
			current.push(metric);
			grouped.set(metric.category, current);
		}

		return HEALTH_CATEGORY_ORDER.filter((category) =>
			grouped.has(category),
		).map((category) => ({
			category,
			metrics: (grouped.get(category) ?? []).sort(
				(left, right) => right.sample_count - left.sample_count,
			),
		}));
	}, [healthMetrics]);

	const selectedHealthMetric = useMemo(() => {
		if (!selectedHealthMetricKey) {
			return null;
		}
		return (
			healthMetrics.find((metric) => metric.key === selectedHealthMetricKey) ??
			null
		);
	}, [healthMetrics, selectedHealthMetricKey]);

	async function handleChooseXmlAndStartIngest() {
		const selection = await open({
			multiple: false,
			filters: [{ name: "Apple Health XML", extensions: ["xml"] }],
		});
		if (typeof selection !== "string") {
			return;
		}

		setErrorMessage(null);
		setLatestIngestEvent(null);
		setIngestStartedAt(Date.now());
		setIngestElapsedSeconds(0);
		setIsIngesting(true);

		try {
			await startIngest({ xmlPath: selection });
		} catch (error) {
			setIsIngesting(false);
			setIngestStartedAt(null);
			setIngestElapsedSeconds(0);
			setErrorMessage(stringifyError(error));
		}
	}

	async function handleCancelIngest() {
		try {
			await cancelIngest();
		} catch (error) {
			setErrorMessage(stringifyError(error));
		}
	}

	async function handleExport(kind: ExportKind) {
		if (!dashboard) {
			setErrorMessage("Load a dataset before exporting.");
			return;
		}

		const exportDateRangeLabel = buildExportDateRangeLabel(
			dashboard.summary.workouts,
			filters.start || undefined,
			filters.end || undefined,
		);
		const outputPath =
			kind === "csv" || kind === "csv-summary"
				? await open({
						directory: true,
						multiple: false,
						defaultPath: "exports",
					})
				: await save({
						defaultPath:
							kind === "summary"
								? `workouts-summary-${exportDateRangeLabel}.json`
								: `workouts-${exportDateRangeLabel}.json`,
						filters: [{ name: "JSON", extensions: ["json"] }],
					});

		if (typeof outputPath !== "string") {
			return;
		}

		setErrorMessage(null);
		setIsExporting(true);

		try {
			await runExport({
				outputPath,
				exportFormat: kind === "csv" || kind === "csv-summary" ? "csv" : "json",
				summary: kind === "summary",
				csvProfile: kind === "csv-summary" ? "llm" : undefined,
				start: filters.start || undefined,
				end: filters.end || undefined,
				activityTypes: filters.activityTypes,
				sourceQuery: filters.sourceQuery || undefined,
				minDurationMinutes: parseOptionalNumber(filters.minDurationMinutes),
				maxDurationMinutes: parseOptionalNumber(filters.maxDurationMinutes),
				location: filters.location || undefined,
				minDistanceMiles: parseOptionalNumber(filters.minDistanceMiles),
				maxDistanceMiles: parseOptionalNumber(filters.maxDistanceMiles),
				minEnergyKcal: parseOptionalNumber(filters.minEnergyKcal),
				maxEnergyKcal: parseOptionalNumber(filters.maxEnergyKcal),
				minAvgHeartRate: parseOptionalNumber(filters.minAvgHeartRate),
				maxAvgHeartRate: parseOptionalNumber(filters.maxAvgHeartRate),
				minMaxHeartRate: parseOptionalNumber(filters.minMaxHeartRate),
				maxMaxHeartRate: parseOptionalNumber(filters.maxMaxHeartRate),
				efforts: filters.efforts,
				requiresRouteData: filters.requiresRouteData,
				requiresHeartRateSamples: filters.requiresHeartRateSamples,
			});
		} catch (error) {
			setErrorMessage(stringifyError(error));
		} finally {
			setIsExporting(false);
		}
	}

	function clearWorkoutFilters() {
		setFilters(EMPTY_FILTERS);
	}

	function clearHealthFilters() {
		setHealthFilters(EMPTY_HEALTH_FILTERS);
	}

	async function applyWorkoutFilters() {
		await loadWorkoutSectionsForFilters(filters, healthFilters);
		setIsWorkoutFiltersVisible(false);
	}

	async function applyHealthFilters() {
		await loadHealthSectionForFilters(filters, healthFilters);
		setIsHealthFiltersVisible(false);
	}

	async function handleExportSelection(kind: ExportKind) {
		setIsExportPopoverOpen(false);
		await handleExport(kind);
	}

	function openSettingsPage() {
		setIsExportPopoverOpen(false);
		setSelectedHealthMetricKey(null);
		setSelectedWorkoutMetricKey(null);
		setCurrentPage("settings");
	}

	function openHelpPage() {
		setIsExportPopoverOpen(false);
		setSelectedHealthMetricKey(null);
		setSelectedWorkoutMetricKey(null);
		setCurrentPage("help");
	}

	function goHome() {
		setCurrentPage("dashboard");
		setCurrentDashboardSection("workouts");
	}

	function switchDashboardSection(section: DashboardSection) {
		setCurrentDashboardSection(section);
		if (section !== "health") {
			setSelectedHealthMetricKey(null);
		}
		if (section !== "workouts") {
			setSelectedWorkoutMetricKey(null);
		}
	}

	function handleDefaultHealthMetricsExpandedChange(nextValue: boolean) {
		setDefaultHealthMetricsExpanded(nextValue);
		setIsHealthMetricsExpanded(nextValue);
	}

	function handleDefaultWorkoutMetricsExpandedChange(nextValue: boolean) {
		setDefaultWorkoutMetricsExpanded(nextValue);
		if (selectedWorkoutId !== null) {
			setExpandedWorkoutMetricsForId(nextValue ? selectedWorkoutId : null);
		}
	}

	const isWorkoutMetricsExpanded =
		selectedWorkoutId !== null &&
		expandedWorkoutMetricsForId === selectedWorkoutId;

	useEffect(() => {
		if (selectedWorkoutId === null || !isWorkoutMetricsExpanded) {
			return;
		}
		if (hasLoadedWorkoutMetricSeries || isLoadingWorkoutMetricSeries) {
			return;
		}
		void ensureWorkoutMetricSeriesLoaded(selectedWorkoutId);
	}, [
		ensureWorkoutMetricSeriesLoaded,
		hasLoadedWorkoutMetricSeries,
		isLoadingWorkoutMetricSeries,
		isWorkoutMetricsExpanded,
		selectedWorkoutId,
	]);

	const hasActiveWorkoutFilters = !isWorkoutFiltersDefault(filters);
	const hasActiveHealthFilters = !isHealthFiltersDefault(healthFilters);
	const activeWorkoutFilterCount = countActiveWorkoutFilters(filters);
	const activeHealthFilterCount = countActiveHealthFilters(healthFilters);

	return (
		<div className="mx-auto flex min-h-[calc(100svh-48px)] max-w-384 flex-col gap-5 px-2 py-1">
			<Card className="gap-3 border-none bg-card/90 backdrop-blur">
				<CardHeader className="gap-4">
					<div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
						<div className="space-y-2">
							<div className="flex flex-wrap items-center gap-3">
								<CardTitle className="text-3xl md:text-4xl">
									{currentPage === "settings"
										? "Settings"
										: currentPage === "help"
											? "Help"
											: APP_NAME}
								</CardTitle>
							</div>
							<CardDescription className="max-w-3xl text-base">
								{currentPage === "settings"
									? "Choose appearance, measurement units, and dashboard defaults, then head back to the dashboard when you are ready."
									: currentPage === "help"
										? "See how to export Apple Health data from your iPhone, move it to your Mac, and load the right file into Pulse Parse."
										: "Load Health data into Pulse Parse for health and health, workout, and overall data analysis."}
							</CardDescription>
							{currentPage === "dashboard" ? (
								<div className="flex flex-wrap gap-2">
									<Badge variant="outline" className="rounded-full px-3 py-1">
										{currentDataset
											? `${formatInteger(currentDataset.workoutCount)} workouts stored`
											: "Load export.xml to begin"}
									</Badge>
									{currentDataset ? (
										<>
											<Badge
												variant="outline"
												className="rounded-full px-3 py-1"
											>
												{formatInteger(currentDataset.recordCount)} records
											</Badge>
											<Badge
												variant="outline"
												className="rounded-full px-3 py-1"
											>
												Last processed{" "}
												{formatRelativeEpoch(
													currentDataset.lastIngestedEpochSeconds,
												)}
											</Badge>
										</>
									) : null}
								</div>
							) : null}
						</div>

						<div className="flex flex-col gap-2 xl:items-end">
							<div className="flex flex-wrap gap-2 xl:self-end">
								{currentPage !== "dashboard" ? (
									<Button
										size="lg"
										variant="outline"
										className="rounded-xl px-4"
										onClick={goHome}
									>
										<ArrowLeftIcon className="size-4" />
										Back to dashboard
									</Button>
								) : null}
								{currentPage !== "help" ? (
									<Button
										size="lg"
										variant="outline"
										className="rounded-xl px-4"
										onClick={openHelpPage}
									>
										<CircleHelpIcon className="size-4" />
										Help
									</Button>
								) : null}
								{currentPage !== "settings" ? (
									<Button
										size="lg"
										variant="outline"
										className="rounded-xl px-4"
										onClick={openSettingsPage}
									>
										<SettingsIcon className="size-4" />
										Settings
									</Button>
								) : null}
							</div>

							{currentPage === "dashboard" ? (
								<div className="flex flex-col gap-2 xl:items-end">
									<div className="flex flex-wrap gap-2">
										<Button
											size="lg"
											className="rounded-xl px-4"
											onClick={() => void handleChooseXmlAndStartIngest()}
											disabled={isIngesting}
										>
											{isIngesting ? (
												<LoaderCircleIcon className="size-4 animate-spin" />
											) : (
												<FileUpIcon className="size-4" />
											)}
											{currentDataset ? "Load new dataset" : "Load dataset"}
										</Button>
										<Button
											size="lg"
											variant="outline"
											className="rounded-xl px-4"
											onClick={() => void handleLoadDashboard()}
											disabled={!currentDataset || isLoadingDashboard}
										>
											{isLoadingDashboard ? (
												<LoaderCircleIcon className="size-4 animate-spin" />
											) : (
												<RefreshCwIcon className="size-4" />
											)}
											Refresh dataset
										</Button>
									</div>

									{isIngesting ? (
										<Button
											size="lg"
											variant="outline"
											className="rounded-xl px-4 xl:self-end"
											onClick={() => void handleCancelIngest()}
										>
											Cancel load
										</Button>
									) : null}
								</div>
							) : null}
						</div>
					</div>
				</CardHeader>

				<CardContent>
					{currentPage === "settings" ? (
						<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
							<InfoMetric label="Theme" value={formatThemeMode(themeMode)} />
							<InfoMetric
								label="Resolved appearance"
								value={formatThemeMode(resolvedThemeMode)}
							/>
							<InfoMetric
								label="Measurements"
								value={
									measurementSystem === "metric"
										? "Metric (km, kph)"
										: "Imperial (mi, mph)"
								}
							/>
							<InfoMetric
								label="Default expansions"
								value={`${defaultHealthMetricsExpanded ? "Health open" : "Health closed"} • ${defaultWorkoutMetricsExpanded ? "Workout open" : "Workout closed"}`}
							/>
						</div>
					) : currentPage === "help" ? (
						<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
							<InfoMetric label="Export source" value="iPhone Health app" />
							<InfoMetric label="File to load" value="export.xml" />
							<InfoMetric
								label="Transfer options"
								value="AirDrop, Files, iCloud Drive"
							/>
							<InfoMetric
								label="Archive format"
								value="Extract the .zip first"
							/>
						</div>
					) : (
						<div className="flex flex-wrap gap-2">
							<Button
								variant={
									currentDashboardSection === "workouts" ? "secondary" : "ghost"
								}
								className="rounded-full px-4"
								onClick={() => switchDashboardSection("workouts")}
							>
								Workouts
							</Button>
							<Button
								variant={
									currentDashboardSection === "health" ? "secondary" : "ghost"
								}
								className="rounded-full px-4"
								onClick={() => switchDashboardSection("health")}
							>
								Health
							</Button>
							<Button
								variant={
									currentDashboardSection === "data" ? "secondary" : "ghost"
								}
								className="rounded-full px-4"
								onClick={() => switchDashboardSection("data")}
							>
								Data
							</Button>
						</div>
					)}
				</CardContent>
			</Card>

			{isIngesting ? (
				<Card size="sm" className="border border-primary/15 bg-primary/5">
					<CardContent className="space-y-3">
						<div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
							<div className="min-w-0">
								<p className="text-sm font-medium text-foreground">
									Loading dataset
								</p>
								<p className="text-sm text-muted-foreground">
									{latestIngestEvent?.message ??
										"Preparing Apple Health ingest..."}
								</p>
							</div>
							<p className="text-sm text-muted-foreground">
								{formatDurationSeconds(ingestElapsedSeconds)}
							</p>
						</div>
						<IndeterminateProgressBar />
						{latestIngestEvent ? (
							<p className="text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
								{latestIngestEvent.label}
							</p>
						) : null}
					</CardContent>
				</Card>
			) : null}

			{errorMessage ? (
				<Alert variant="destructive">
					<AlertCircleIcon className="size-4" />
					<AlertTitle>Desktop action failed</AlertTitle>
					<AlertDescription>{errorMessage}</AlertDescription>
				</Alert>
			) : null}

			<main className="space-y-6">
				{currentPage === "settings" ? (
					<SettingsPage
						themeMode={themeMode}
						resolvedThemeMode={resolvedThemeMode}
						measurementSystem={measurementSystem}
						defaultHealthMetricsExpanded={defaultHealthMetricsExpanded}
						defaultWorkoutMetricsExpanded={defaultWorkoutMetricsExpanded}
						onChangeThemeMode={setThemeMode}
						onChangeMeasurementSystem={setMeasurementSystem}
						onChangeDefaultHealthMetricsExpanded={
							handleDefaultHealthMetricsExpandedChange
						}
						onChangeDefaultWorkoutMetricsExpanded={
							handleDefaultWorkoutMetricsExpandedChange
						}
					/>
				) : currentPage === "help" ? (
					<HelpPage />
				) : currentDashboardSection === "data" ? (
					<DataPage
						currentDataset={currentDataset}
						dashboard={dashboard}
						latestIngestEvent={latestIngestEvent}
						ingestHistory={ingestHistory}
						sortedWorkouts={sortedWorkouts}
						expandedHealthGroups={expandedHealthGroups}
						hasActiveWorkoutFilters={hasActiveWorkoutFilters}
						hasActiveHealthFilters={hasActiveHealthFilters}
						activeWorkoutFilterCount={activeWorkoutFilterCount}
						activeHealthFilterCount={activeHealthFilterCount}
					/>
				) : currentDashboardSection === "health" ? (
					<Card>
						<CardHeader className="gap-4">
							<div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
								<div className="space-y-1">
									<CardTitle>Health analysis</CardTitle>
									<CardDescription>
										Start with the compact snapshot, then open the full library
										only when you need deeper analysis.
									</CardDescription>
								</div>

								<div className="flex flex-wrap gap-2 xl:justify-end">
									<Badge
										variant={hasActiveHealthFilters ? "secondary" : "outline"}
										className="rounded-full px-3 py-1"
									>
										{activeHealthFilterCount === 0
											? "Default filters"
											: `${formatInteger(activeHealthFilterCount)} active filter${activeHealthFilterCount === 1 ? "" : "s"}`}
									</Badge>
									<Button
										variant={isHealthFiltersVisible ? "secondary" : "outline"}
										className="rounded-xl"
										aria-haspopup="dialog"
										disabled={!currentDataset}
										onClick={() =>
											setIsHealthFiltersVisible((current) => !current)
										}
									>
										<FilterIcon className="size-4" />
										Filters
									</Button>
								</div>
							</div>
						</CardHeader>
						<CardContent>
							{dashboard ? (
								<>
									<div className="space-y-6">
										<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
											<MetricCard
												label="Health samples"
												value={dashboard.health_overview.record_count}
											/>
											<MetricCard
												label="Metrics with data"
												value={dashboard.health_overview.available_metric_count}
											/>
											<MetricCard
												label="Covered range"
												value={formatRange(
													dashboard.health_overview.first_start,
													dashboard.health_overview.last_end,
												)}
											/>
										</div>

										{healthMetrics.length === 0 ? (
											<EmptyState
												title="No health metrics match the current health filters"
												description="Try widening the health date range to restore the overall health snapshot."
												compact
											/>
										) : (
											<>
												<div className="space-y-3">
													<div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
														<div>
															<h3 className="text-sm font-semibold text-foreground">
																Health snapshot
															</h3>
															<p className="text-sm text-muted-foreground">
																Open a metric card whenever you want the full
																graph and supporting stats.
															</p>
														</div>
														<Button
															variant="outline"
															className="rounded-xl"
															onClick={() =>
																setIsHealthMetricsExpanded(
																	(current) => !current,
																)
															}
														>
															{isHealthMetricsExpanded ? (
																<ChevronUpIcon className="size-4" />
															) : (
																<ChevronDownIcon className="size-4" />
															)}
															{isHealthMetricsExpanded
																? "Hide full metric list"
																: `View all ${formatInteger(healthMetrics.length)} health metrics`}
														</Button>
													</div>

													<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
														{snapshotHealthMetrics.map((metric) => (
															<HealthSnapshotCard
																key={metric.key}
																metric={metric}
																measurementSystem={measurementSystem}
																onSelect={() =>
																	setSelectedHealthMetricKey(metric.key)
																}
															/>
														))}
													</div>
												</div>

												{isHealthMetricsExpanded ? (
													<div className="space-y-6 border-t border-border/60 pt-4">
														{expandedHealthGroups.map((group) => (
															<div key={group.category} className="space-y-3">
																<div>
																	<h3 className="text-sm font-semibold text-foreground">
																		{group.category}
																	</h3>
																	<p className="text-sm text-muted-foreground">
																		{formatInteger(group.metrics.length)} metric
																		{group.metrics.length === 1 ? "" : "s"}{" "}
																		match the current health filter window.
																	</p>
																</div>
																<div className="grid gap-4 xl:grid-cols-3">
																	{group.metrics.map((metric) => (
																		<HealthMetricCard
																			key={metric.key}
																			metric={metric}
																			measurementSystem={measurementSystem}
																			onSelect={() =>
																				setSelectedHealthMetricKey(metric.key)
																			}
																		/>
																	))}
																</div>
															</div>
														))}
													</div>
												) : null}
											</>
										)}
									</div>
									{isHealthFiltersVisible ? (
										<FilterModalShell
											title="Health filters"
											description="Narrow the non-workout health view by date, category, metric, and source."
											maxWidthClassName="max-w-3xl"
											onClose={() => setIsHealthFiltersVisible(false)}
										>
											<DatePickerField
												label="Health record window"
												start={healthFilters.start}
												end={healthFilters.end}
												description="Click one day for a single-date filter, or choose a second day to expand to a range."
												onChange={({ start, end }) =>
													setHealthFilters((current) => ({
														...current,
														start,
														end,
													}))
												}
											/>
											<div className="space-y-2">
												<p className="text-sm font-medium text-foreground">
													Metric categories
												</p>
												<div className="flex flex-wrap gap-2">
													{HEALTH_CATEGORY_ORDER.map((category) => (
														<FilterButton
															key={category}
															selected={healthFilters.categories.includes(
																category,
															)}
															onClick={() =>
																setHealthFilters((current) => ({
																	...current,
																	categories: toggleStringSelection(
																		current.categories,
																		category,
																	),
																}))
															}
														>
															{category}
														</FilterButton>
													))}
												</div>
											</div>
											<div className="space-y-2">
												<p className="text-sm font-medium text-foreground">
													Metric or record type
												</p>
												<Input
													value={healthFilters.metricQuery}
													onChange={(event) =>
														setHealthFilters((current) => ({
															...current,
															metricQuery: event.target.value,
														}))
													}
													placeholder="Search labels or record types"
													aria-label="Search health metrics"
												/>
											</div>
											<div className="space-y-2">
												<p className="text-sm font-medium text-foreground">
													Source or app
												</p>
												<Input
													value={healthFilters.sourceQuery}
													onChange={(event) =>
														setHealthFilters((current) => ({
															...current,
															sourceQuery: event.target.value,
														}))
													}
													placeholder="Filter by source name"
													aria-label="Filter health source"
												/>
											</div>
											<FilterCheckboxRow
												checked={healthFilters.onlyWithSamples}
												label="Only metrics with samples in range"
												onCheckedChange={(checked) =>
													setHealthFilters((current) => ({
														...current,
														onlyWithSamples: checked,
													}))
												}
											/>
											<div className="flex flex-col gap-2 pt-2 sm:flex-row sm:justify-between">
												<Button
													className="w-full rounded-xl sm:w-auto"
													onClick={() => void applyHealthFilters()}
													disabled={!currentDataset || isLoadingDashboard}
												>
													{isLoadingDashboard ? (
														<LoaderCircleIcon className="size-4 animate-spin" />
													) : (
														<RefreshCwIcon className="size-4" />
													)}
													Apply filters
												</Button>
												<div className="flex flex-col gap-2 sm:flex-row">
													<Button
														variant="ghost"
														className="w-full rounded-xl sm:w-auto"
														onClick={clearHealthFilters}
													>
														Clear filters
													</Button>
													<Button
														variant="outline"
														className="w-full rounded-xl sm:w-auto"
														onClick={() => setIsHealthFiltersVisible(false)}
													>
														Done
													</Button>
												</div>
											</div>
										</FilterModalShell>
									) : null}
								</>
							) : (
								<EmptyState
									title="No dataset loaded yet"
									description="Load a dataset to see the compact health snapshot, then open deeper health drilldowns only when you need them."
								/>
							)}
						</CardContent>
					</Card>
				) : (
					<Card id="workout-analysis">
						<CardHeader className="gap-4">
							<div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
								<div className="space-y-1">
									<CardTitle>Workout analysis</CardTitle>
									<CardDescription>
										Review the filtered workout set, then inspect one workout
										alongside the list.
									</CardDescription>
								</div>

								<div className="flex flex-wrap gap-2 xl:justify-end">
									<Badge
										variant={hasActiveWorkoutFilters ? "secondary" : "outline"}
										className="rounded-full px-3 py-1"
									>
										{activeWorkoutFilterCount === 0
											? "Default filters"
											: `${formatInteger(activeWorkoutFilterCount)} active filter${activeWorkoutFilterCount === 1 ? "" : "s"}`}
									</Badge>
									<Button
										variant={isWorkoutFiltersVisible ? "secondary" : "outline"}
										className="rounded-xl"
										aria-haspopup="dialog"
										disabled={!currentDataset}
										onClick={() =>
											setIsWorkoutFiltersVisible((current) => !current)
										}
									>
										<FilterIcon className="size-4" />
										Filters
									</Button>

									<Popover
										open={isExportPopoverOpen}
										onOpenChange={setIsExportPopoverOpen}
									>
										<PopoverTrigger asChild>
											<Button
												variant="outline"
												className="rounded-xl"
												disabled={!dashboard || isExporting}
											>
												<DownloadIcon className="size-4" />
												Export
											</Button>
										</PopoverTrigger>
										<PopoverContent align="end" className="w-72">
											<PopoverHeader>
												<PopoverTitle>Export workout results</PopoverTitle>
												<PopoverDescription>
													Export the currently visible workout window as full
													JSON, summary JSON, compact CSV summary, or full CSV
													files.
												</PopoverDescription>
											</PopoverHeader>
											<div className="grid gap-2">
												<Button
													variant="outline"
													className="justify-start rounded-xl"
													onClick={() => void handleExportSelection("json")}
													disabled={!dashboard || isExporting}
												>
													<DownloadIcon className="size-4" />
													{isExporting ? "Exporting…" : "Export full JSON"}
												</Button>
												<Button
													variant="outline"
													className="justify-start rounded-xl"
													onClick={() => void handleExportSelection("summary")}
													disabled={!dashboard || isExporting}
												>
													<DownloadIcon className="size-4" />
													Export summary JSON
												</Button>
												<Button
													variant="outline"
													className="justify-start rounded-xl"
													onClick={() =>
														void handleExportSelection("csv-summary")
													}
													disabled={!dashboard || isExporting}
												>
													<DownloadIcon className="size-4" />
													Export CSV summary
												</Button>
												<Button
													variant="outline"
													className="justify-start rounded-xl"
													onClick={() => void handleExportSelection("csv")}
													disabled={!dashboard || isExporting}
												>
													<DownloadIcon className="size-4" />
													Export CSV directory
												</Button>
											</div>
										</PopoverContent>
									</Popover>
								</div>
							</div>
						</CardHeader>
						<CardContent>
							{dashboard ? (
								<>
									<div className="space-y-6">
										<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
											<MetricCard
												label="Selected workouts"
												value={dashboard.summary.overall.workout_count}
											/>
											<MetricCard
												label="Total hours"
												value={formatMaybeNumber(
													dashboard.summary.overall.total_duration_hours,
													"h",
												)}
											/>
											<MetricCard
												label="Total distance"
												value={formatMaybeNumber(
													dashboard.summary.overall.total_distance_miles,
													"mi",
													measurementSystem,
												)}
											/>
											<MetricCard
												label="Total energy"
												value={formatMaybeNumber(
													dashboard.summary.overall.total_energy_kcal,
													"kcal",
												)}
											/>
											<MetricCard
												label="Average heart rate"
												value={formatMaybeNumber(
													dashboard.summary.overall.average_heart_rate,
													"bpm",
												)}
											/>
											<MetricCard
												label="Covered range"
												value={formatRange(
													dashboard.inspection.overall.first_start,
													dashboard.inspection.overall.last_end,
												)}
											/>
										</div>

										<div className="grid gap-4 2xl:grid-cols-2">
											<Card
												size="sm"
												className="h-full border border-border/70 bg-background/80"
											>
												<CardHeader>
													<CardTitle className="text-sm">
														Activity breakdown
													</CardTitle>
													<CardDescription>
														Workout counts by Apple Health activity type.
													</CardDescription>
												</CardHeader>
												<CardContent className="h-72">
													<ResponsiveContainer width="100%" height="100%">
														<BarChart
															data={dashboard.summary.activity_breakdown}
														>
															<CartesianGrid
																strokeDasharray="3 3"
																stroke={chartPalette.grid}
															/>
															<XAxis
																dataKey="type"
																tick={{ fill: chartPalette.tick, fontSize: 12 }}
															/>
															<YAxis
																allowDecimals={false}
																tick={{ fill: chartPalette.tick, fontSize: 12 }}
															/>
															<Tooltip />
															<Bar
																dataKey="count"
																fill={chartPalette.workouts}
																radius={[8, 8, 0, 0]}
															/>
														</BarChart>
													</ResponsiveContainer>
												</CardContent>
											</Card>

											<Card
												size="sm"
												className="h-full border border-border/70 bg-background/80"
											>
												<CardHeader>
													<CardTitle className="text-sm">Daily trend</CardTitle>
													<CardDescription>
														Workouts and{" "}
														{measurementSystem === "metric"
															? "kilometers"
															: "miles"}{" "}
														grouped by workout start date.
													</CardDescription>
												</CardHeader>
												<CardContent className="h-72">
													<ResponsiveContainer width="100%" height="100%">
														<LineChart data={trendData}>
															<CartesianGrid
																strokeDasharray="3 3"
																stroke={chartPalette.grid}
															/>
															<XAxis
																dataKey="date"
																tick={{ fill: chartPalette.tick, fontSize: 12 }}
															/>
															<YAxis
																yAxisId="left"
																allowDecimals={false}
																tick={{ fill: chartPalette.tick, fontSize: 12 }}
															/>
															<YAxis
																yAxisId="right"
																orientation="right"
																tick={{ fill: chartPalette.tick, fontSize: 12 }}
																tickFormatter={(value) =>
																	formatMaybeNumber(
																		typeof value === "number" ? value : null,
																		measurementSystem === "metric"
																			? "km"
																			: "mi",
																		measurementSystem,
																	)
																}
															/>
															<Tooltip
																formatter={(value, name) => {
																	if (name === "distance") {
																		return formatMaybeNumber(
																			typeof value === "number" ? value : null,
																			measurementSystem === "metric"
																				? "km"
																				: "mi",
																			measurementSystem,
																		);
																	}

																	return typeof value === "number"
																		? value.toLocaleString()
																		: "—";
																}}
															/>
															<Line
																yAxisId="left"
																type="linear"
																dataKey="workouts"
																stroke={chartPalette.workouts}
																strokeWidth={3}
																dot={{ r: 4 }}
															/>
															<Line
																yAxisId="right"
																type="linear"
																dataKey="distance"
																stroke={chartPalette.distance}
																strokeWidth={3}
																dot={{ r: 4 }}
															/>
														</LineChart>
													</ResponsiveContainer>
												</CardContent>
											</Card>
										</div>

										<div className="grid gap-4 2xl:grid-cols-2">
											<Card
												size="sm"
												className="border border-border/70 bg-background/80"
											>
												<CardHeader>
													<CardTitle className="text-sm">
														Workout highlights
													</CardTitle>
													<CardDescription>
														Fast scan of the filtered workout summary bundle.
													</CardDescription>
												</CardHeader>
												<CardContent>
													{dashboard.summary.highlights.length === 0 ? (
														<p className="text-sm text-muted-foreground">
															No workout highlights are available for the
															current filter selection.
														</p>
													) : (
														<ul className="space-y-2 text-sm text-muted-foreground">
															{dashboard.summary.highlights.map((highlight) => (
																<li key={highlight} className="flex gap-2">
																	<span className="mt-1 size-1.5 rounded-full bg-primary" />
																	<span>{highlight}</span>
																</li>
															))}
														</ul>
													)}
												</CardContent>
											</Card>

											<Card
												size="sm"
												className="border border-border/70 bg-background/80"
											>
												<CardHeader>
													<CardTitle className="text-sm">
														Activity table
													</CardTitle>
													<CardDescription>
														Distance and energy totals for each visible activity
														type.
													</CardDescription>
												</CardHeader>
												<CardContent>
													<ActivityTable
														rows={dashboard.summary.activity_breakdown}
														measurementSystem={measurementSystem}
													/>
												</CardContent>
											</Card>
										</div>

										<div className="grid gap-4 xl:grid-cols-[360px_minmax(0,1fr)]">
											<Card
												size="sm"
												className="border border-border/70 bg-background/80 xl:h-190"
											>
												<CardHeader className="gap-3">
													<div className="space-y-1">
														<CardTitle className="text-sm">
															Workout list
														</CardTitle>
														<CardDescription>
															Search the visible workouts and pick one to
															inspect beside the list.
														</CardDescription>
													</div>
													<div className="space-y-2">
														<Input
															value={workoutSearch}
															onChange={(event) =>
																setWorkoutSearch(event.target.value)
															}
															placeholder="Search visible workouts"
															aria-label="Search visible workouts"
															disabled={dashboard.summary.workouts.length === 0}
														/>
														<p className="text-xs text-muted-foreground">
															{visibleWorkouts.length.toLocaleString()} of{" "}
															{dashboard.summary.workouts.length.toLocaleString()}{" "}
															workouts shown
														</p>
													</div>
												</CardHeader>
												<CardContent className="xl:flex-1 xl:min-h-0">
													<div className="flex h-full max-h-180 flex-col gap-3 overflow-y-auto pr-1 xl:max-h-full">
														{dashboard.summary.workouts.length === 0 ? (
															<EmptyState
																title="No workouts match the current workout filters"
																description="Try clearing the date range or removing some workout types."
																compact
															/>
														) : visibleWorkouts.length === 0 ? (
															<EmptyState
																title="No workouts match this search"
																description="Try a broader search term or clear the workout search field."
																compact
															/>
														) : (
															visibleWorkouts.map((workout) => (
																<button
																	key={workout.db_id}
																	type="button"
																	onClick={() =>
																		void selectWorkout(workout.db_id)
																	}
																	className={cn(
																		"rounded-2xl border border-border/70 bg-background/80 p-4 text-left transition-colors hover:border-primary/30 hover:bg-primary/5",
																		selectedWorkoutId === workout.db_id &&
																			"border-primary/40 bg-primary/5 ring-1 ring-primary/15",
																	)}
																>
																	<div className="flex items-center justify-between gap-3">
																		<div>
																			<p className="text-sm font-semibold text-foreground">
																				{workout.title}
																			</p>
																			<p className="text-xs text-muted-foreground">
																				{workout.type}
																			</p>
																		</div>
																		<Badge
																			variant="outline"
																			className="rounded-full"
																		>
																			{workout.date}
																		</Badge>
																	</div>
																	<p className="mt-3 text-sm leading-6 text-muted-foreground">
																		{workout.summary}
																	</p>
																	{(() => {
																		const badges = getWorkoutListBadges(
																			workout,
																			measurementSystem,
																		);
																		if (
																			badges.length === 0 &&
																			!workout.effort
																		) {
																			return null;
																		}

																		return (
																			<div className="mt-4 flex flex-wrap gap-2">
																				{workout.effort ? (
																					<WorkoutEffortBadge
																						effort={workout.effort}
																					/>
																				) : null}
																				{badges.map((badge) => (
																					<WorkoutBadge
																						key={`${workout.db_id}-${badge}`}
																					>
																						{badge}
																					</WorkoutBadge>
																				))}
																			</div>
																		);
																	})()}
																</button>
															))
														)}
													</div>
												</CardContent>
											</Card>

											<Card
												size="sm"
												className="border border-border/70 bg-background/80 xl:h-190"
											>
												<CardHeader className="gap-3">
													<div className="space-y-1">
														<CardTitle className="text-sm">
															Selected workout detail
														</CardTitle>
														<CardDescription>
															Inspect linked records, derived metrics, metadata,
															and drilldown charts without leaving the workout
															list.
														</CardDescription>
													</div>
												</CardHeader>
												<CardContent className="xl:flex-1 xl:min-h-0">
													<div className="h-full max-h-180 overflow-y-auto pr-1 xl:max-h-full">
														<SelectedWorkoutDetailSection
															isLoadingWorkout={isLoadingWorkout}
															hasDashboard={Boolean(dashboard)}
															selectedWorkoutCard={selectedWorkoutCard}
															workoutDetail={workoutDetail}
															workoutMetricSeries={workoutMetricSeries}
															hasLoadedWorkoutMetricSeries={
																hasLoadedWorkoutMetricSeries
															}
															isLoadingWorkoutMetricSeries={
																isLoadingWorkoutMetricSeries
															}
															measurementSystem={measurementSystem}
															isWorkoutMetricsExpanded={
																isWorkoutMetricsExpanded
															}
															onToggleWorkoutMetricsExpanded={() => {
																if (selectedWorkoutId === null) {
																	return;
																}
																setExpandedWorkoutMetricsForId((current) =>
																	current === selectedWorkoutId
																		? null
																		: selectedWorkoutId,
																);
															}}
															onSelectWorkoutMetric={
																setSelectedWorkoutMetricKey
															}
														/>
													</div>
												</CardContent>
											</Card>
										</div>
									</div>
									{isWorkoutFiltersVisible ? (
										<FilterModalShell
											title="Workout filters"
											description="Narrow workouts by time, type, source, effort, metric ranges, and linked data."
											maxWidthClassName="max-w-4xl"
											onClose={() => setIsWorkoutFiltersVisible(false)}
										>
											<DatePickerField
												label="Workout window"
												start={filters.start}
												end={filters.end}
												description="Click one day for a single-date filter, or choose a second day to expand to a range."
												onChange={({ start, end }) =>
													setFilters((current) => ({
														...current,
														start,
														end,
													}))
												}
											/>
											<ActivityTypePicker
												options={dashboard.available_activity_types}
												value={filters.activityTypes}
												onChange={(activityTypes) =>
													setFilters((current) => ({
														...current,
														activityTypes,
													}))
												}
												disabled={!currentDataset}
											/>
											<div className="space-y-2">
												<p className="text-sm font-medium text-foreground">
													Source or app
												</p>
												<Input
													value={filters.sourceQuery}
													onChange={(event) =>
														setFilters((current) => ({
															...current,
															sourceQuery: event.target.value,
														}))
													}
													placeholder="Search by source or app"
													aria-label="Filter workout source"
												/>
											</div>
											<div className="space-y-2">
												<p className="text-sm font-medium text-foreground">
													Location
												</p>
												<div className="flex flex-wrap gap-2">
													{WORKOUT_LOCATION_OPTIONS.map((option) => (
														<FilterButton
															key={option.label}
															selected={filters.location === option.value}
															onClick={() =>
																setFilters((current) => ({
																	...current,
																	location: option.value,
																}))
															}
														>
															{option.label}
														</FilterButton>
													))}
												</div>
											</div>
											<div className="space-y-2">
												<p className="text-sm font-medium text-foreground">
													Effort
												</p>
												<div className="flex flex-wrap gap-2">
													{WORKOUT_EFFORT_OPTIONS.map((effort) => (
														<FilterButton
															key={effort}
															selected={filters.efforts.includes(effort)}
															className={
																getWorkoutEffortStyles(effort)[
																	filters.efforts.includes(effort)
																		? "buttonSelected"
																		: "button"
																]
															}
															onClick={() =>
																setFilters((current) => ({
																	...current,
																	efforts: toggleStringSelection(
																		current.efforts,
																		effort,
																	),
																}))
															}
														>
															{effort}
														</FilterButton>
													))}
												</div>
											</div>
											<div className="grid gap-3 sm:grid-cols-2">
												<FilterNumberRangeField
													label="Distance (mi)"
													minValue={filters.minDistanceMiles}
													maxValue={filters.maxDistanceMiles}
													minPlaceholder="Min"
													maxPlaceholder="Max"
													onMinChange={(value) =>
														setFilters((current) => ({
															...current,
															minDistanceMiles: value,
														}))
													}
													onMaxChange={(value) =>
														setFilters((current) => ({
															...current,
															maxDistanceMiles: value,
														}))
													}
												/>
												<FilterNumberRangeField
													label="Average HR (bpm)"
													minValue={filters.minAvgHeartRate}
													maxValue={filters.maxAvgHeartRate}
													minPlaceholder="Min"
													maxPlaceholder="Max"
													onMinChange={(value) =>
														setFilters((current) => ({
															...current,
															minAvgHeartRate: value,
														}))
													}
													onMaxChange={(value) =>
														setFilters((current) => ({
															...current,
															maxAvgHeartRate: value,
														}))
													}
												/>
											</div>
											<div className="space-y-3 rounded-xl border border-border/70 bg-muted/20 p-3">
												<Button
													type="button"
													variant="ghost"
													className="w-full justify-between rounded-lg px-2"
													aria-expanded={isWorkoutAdvancedFiltersOpen}
													onClick={() =>
														setIsWorkoutAdvancedFiltersOpen(
															(current) => !current,
														)
													}
												>
													Advanced filters
													{isWorkoutAdvancedFiltersOpen ? (
														<ChevronUpIcon className="size-4" />
													) : (
														<ChevronDownIcon className="size-4" />
													)}
												</Button>
												{isWorkoutAdvancedFiltersOpen ? (
													<div className="grid gap-3 sm:grid-cols-2">
														<FilterNumberRangeField
															label="Duration (min)"
															minValue={filters.minDurationMinutes}
															maxValue={filters.maxDurationMinutes}
															minPlaceholder="Min"
															maxPlaceholder="Max"
															onMinChange={(value) =>
																setFilters((current) => ({
																	...current,
																	minDurationMinutes: value,
																}))
															}
															onMaxChange={(value) =>
																setFilters((current) => ({
																	...current,
																	maxDurationMinutes: value,
																}))
															}
														/>
														<FilterNumberRangeField
															label="Energy (kcal)"
															minValue={filters.minEnergyKcal}
															maxValue={filters.maxEnergyKcal}
															minPlaceholder="Min"
															maxPlaceholder="Max"
															onMinChange={(value) =>
																setFilters((current) => ({
																	...current,
																	minEnergyKcal: value,
																}))
															}
															onMaxChange={(value) =>
																setFilters((current) => ({
																	...current,
																	maxEnergyKcal: value,
																}))
															}
														/>
														<FilterNumberRangeField
															label="Max HR (bpm)"
															minValue={filters.minMaxHeartRate}
															maxValue={filters.maxMaxHeartRate}
															minPlaceholder="Min"
															maxPlaceholder="Max"
															onMinChange={(value) =>
																setFilters((current) => ({
																	...current,
																	minMaxHeartRate: value,
																}))
															}
															onMaxChange={(value) =>
																setFilters((current) => ({
																	...current,
																	maxMaxHeartRate: value,
																}))
															}
														/>
													</div>
												) : null}
											</div>
											<div className="space-y-2">
												<FilterCheckboxRow
													checked={filters.requiresRouteData}
													label="Only workouts with route data"
													onCheckedChange={(checked) =>
														setFilters((current) => ({
															...current,
															requiresRouteData: checked,
														}))
													}
												/>
												<FilterCheckboxRow
													checked={filters.requiresHeartRateSamples}
													label="Only workouts with heart-rate samples"
													onCheckedChange={(checked) =>
														setFilters((current) => ({
															...current,
															requiresHeartRateSamples: checked,
														}))
													}
												/>
											</div>
											<div className="flex flex-col gap-2 pt-2 sm:flex-row sm:justify-between">
												<Button
													className="w-full rounded-xl sm:w-auto"
													onClick={() => void applyWorkoutFilters()}
													disabled={!currentDataset || isLoadingDashboard}
												>
													{isLoadingDashboard ? (
														<LoaderCircleIcon className="size-4 animate-spin" />
													) : (
														<RefreshCwIcon className="size-4" />
													)}
													Apply filters
												</Button>
												<div className="flex flex-col gap-2 sm:flex-row">
													<Button
														variant="ghost"
														className="w-full rounded-xl sm:w-auto"
														onClick={clearWorkoutFilters}
													>
														Clear filters
													</Button>
													<Button
														variant="outline"
														className="w-full rounded-xl sm:w-auto"
														onClick={() => setIsWorkoutFiltersVisible(false)}
													>
														Done
													</Button>
												</div>
											</div>
										</FilterModalShell>
									) : null}
								</>
							) : (
								<EmptyState
									title="Workout analysis is waiting on a dataset"
									description="Load a dataset to review filtered workout analysis and search the visible workout list."
								/>
							)}
						</CardContent>
					</Card>
				)}
			</main>
			{currentPage === "dashboard" && selectedHealthMetric ? (
				<HealthMetricModal
					metric={selectedHealthMetric}
					measurementSystem={measurementSystem}
					resolvedThemeMode={resolvedThemeMode}
					onClose={() => setSelectedHealthMetricKey(null)}
				/>
			) : null}
			{currentPage === "dashboard" &&
			selectedWorkoutMetric &&
			selectedWorkoutCard ? (
				<WorkoutMetricModal
					metric={selectedWorkoutMetric}
					workout={selectedWorkoutCard}
					measurementSystem={measurementSystem}
					resolvedThemeMode={resolvedThemeMode}
					onClose={() => setSelectedWorkoutMetricKey(null)}
				/>
			) : null}
		</div>
	);
}

function MetricCard({
	label,
	value,
}: {
	label: string;
	value: string | number;
}) {
	return (
		<Card size="sm" className="border border-border/70 bg-background/80">
			<CardContent className="space-y-1">
				<p className="text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
					{label}
				</p>
				<p className="text-xl font-semibold text-foreground">
					{formatInlineMetricValue(value)}
				</p>
			</CardContent>
		</Card>
	);
}

function FilterModalShell({
	title,
	description,
	maxWidthClassName,
	onClose,
	children,
}: {
	title: string;
	description: string;
	maxWidthClassName: string;
	onClose: () => void;
	children: ReactNode;
}) {
	return (
		<div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-sm">
			<div
				role="dialog"
				aria-modal="true"
				aria-label={title}
				className={cn(
					"max-h-[90vh] w-full overflow-y-auto rounded-3xl border border-border/70 bg-background shadow-2xl",
					maxWidthClassName,
				)}
			>
				<div className="sticky top-0 z-10 flex items-start justify-between gap-4 border-b border-border/70 bg-background/95 px-6 py-5 backdrop-blur">
					<div className="space-y-1">
						<h2 className="text-xl font-semibold text-foreground">{title}</h2>
						<p className="text-sm text-muted-foreground">{description}</p>
					</div>
					<Button
						variant="ghost"
						size="icon"
						className="rounded-full"
						onClick={onClose}
					>
						<XIcon className="size-4" />
					</Button>
				</div>
				<div className="space-y-4 px-6 py-6">{children}</div>
			</div>
		</div>
	);
}

function DataPage({
	currentDataset,
	dashboard,
	latestIngestEvent,
	ingestHistory,
	sortedWorkouts,
	expandedHealthGroups,
	hasActiveWorkoutFilters,
	hasActiveHealthFilters,
	activeWorkoutFilterCount,
	activeHealthFilterCount,
}: {
	currentDataset: CurrentDataset | null;
	dashboard: DashboardPayload | null;
	latestIngestEvent: IngestProgressEvent | null;
	ingestHistory: IngestHistoryEntry[];
	sortedWorkouts: SummaryWorkoutCard[];
	expandedHealthGroups: Array<{
		category: string;
		metrics: HealthOverviewMetric[];
	}>;
	hasActiveWorkoutFilters: boolean;
	hasActiveHealthFilters: boolean;
	activeWorkoutFilterCount: number;
	activeHealthFilterCount: number;
}) {
	const datasetInfoEntries = collectDatasetInfoEntries(dashboard);
	const workoutsWithHeartRate = sortedWorkouts.filter(
		(workout) => workout.heart_rate_sample_count > 0,
	).length;
	const uniqueWorkoutSources = new Set(
		sortedWorkouts
			.map((workout) => workout.source?.trim())
			.filter((value): value is string => Boolean(value)),
	).size;
	const latestHistoryEntry = ingestHistory[0] ?? null;

	return (
		<section className="space-y-4">
			<div className="grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_400px]">
				<div className="grid gap-4">
					<Card size="sm" className="border border-border/70 bg-background/80">
						<CardHeader>
							<div className="flex items-center justify-between gap-3">
								<div>
									<CardTitle className="text-base">Current dataset</CardTitle>
									<CardDescription>
										Dataset paths and stable storage details that are useful for
										validation and debugging.
									</CardDescription>
								</div>
								<Badge
									variant={currentDataset ? "secondary" : "outline"}
									className="rounded-full px-3 py-1"
								>
									{currentDataset ? "Ready" : "Empty"}
								</Badge>
							</div>
						</CardHeader>
						<CardContent className="grid gap-4 md:grid-cols-2">
							<InfoMetric
								label="Apple Health export"
								value={currentDataset?.xmlPath ?? "No source XML recorded yet."}
							/>
							<InfoMetric
								label="Current dataset path"
								value={
									currentDataset?.dbPath ??
									"The app will create a managed SQLite file automatically."
								}
							/>
						</CardContent>
					</Card>

					<div className="grid gap-4 lg:grid-cols-2">
						<Card
							size="sm"
							className="border border-border/70 bg-background/80"
						>
							<CardHeader>
								<CardTitle className="text-sm">Dataset counts</CardTitle>
								<CardDescription>
									Stable totals currently stored in the managed SQLite dataset.
								</CardDescription>
							</CardHeader>
							<CardContent className="grid gap-3 sm:grid-cols-2">
								<InfoMetric
									label="Stored workouts"
									value={currentDataset ? currentDataset.workoutCount : "—"}
								/>
								<InfoMetric
									label="Stored records"
									value={currentDataset ? currentDataset.recordCount : "—"}
								/>
								<InfoMetric
									label="Workout-record links"
									value={
										currentDataset ? currentDataset.workoutRecordLinkCount : "—"
									}
								/>
							</CardContent>
						</Card>

						<Card
							size="sm"
							className="border border-border/70 bg-background/80"
						>
							<CardHeader>
								<CardTitle className="text-sm">Latest ingest details</CardTitle>
								<CardDescription>
									Size, runtime, and timing for the most recent import known to
									the app.
								</CardDescription>
							</CardHeader>
							<CardContent className="grid gap-3 sm:grid-cols-2">
								<InfoMetric
									label="Source size"
									value={
										currentDataset
											? formatByteSize(currentDataset.sourceXmlSizeBytes)
											: "—"
									}
								/>
								<InfoMetric
									label="Processing time"
									value={
										currentDataset
											? formatDurationSeconds(
													currentDataset.ingestDurationSeconds,
												)
											: "—"
									}
								/>
								<InfoMetric
									label="Last processed"
									value={
										currentDataset
											? formatRelativeEpoch(
													currentDataset.lastIngestedEpochSeconds,
												)
											: "Not yet loaded"
									}
								/>
							</CardContent>
						</Card>
					</div>
				</div>

				<Card
					size="sm"
					className="border border-border/70 bg-background/80 xl:max-h-175"
				>
					<CardHeader>
						<CardTitle className="text-sm">Import history</CardTitle>
						<CardDescription>
							Persisted ingest outcomes across launches for quick validation and
							debugging.
						</CardDescription>
					</CardHeader>
					<CardContent className="space-y-3 overflow-y-auto xl:flex-1 xl:min-h-0">
						{ingestHistory.length === 0 ? (
							<p className="text-sm text-muted-foreground">
								No imports have been recorded yet.
							</p>
						) : (
							ingestHistory.map((entry) => (
								<div
									key={entry.id}
									className="rounded-2xl border border-border/70 bg-background/80 p-3"
								>
									<div className="flex items-center justify-between gap-3">
										<Badge
											variant={
												entry.status === "success" ? "secondary" : "destructive"
											}
											className="rounded-full px-2.5 py-1"
										>
											{entry.status === "success" ? "Success" : "Failed"}
										</Badge>
										<span className="text-xs text-muted-foreground">
											{formatDateTime(entry.finishedAt)}
										</span>
									</div>
									<div className="mt-3 grid gap-2 sm:grid-cols-2">
										<InfoMetric
											label="Workouts"
											value={entry.workoutCount ?? "—"}
										/>
										<InfoMetric
											label="Records"
											value={entry.recordCount ?? "—"}
										/>
										<InfoMetric
											label="Duration"
											value={
												entry.ingestDurationSeconds === null
													? "—"
													: formatDurationSeconds(entry.ingestDurationSeconds)
											}
										/>
										<InfoMetric
											label="Source"
											value={
												entry.sourceXmlPath ?? entry.error ?? "No path recorded"
											}
										/>
									</div>
								</div>
							))
						)}
					</CardContent>
				</Card>
			</div>

			{!currentDataset ? (
				<EmptyState
					title="Data page is waiting on a dataset"
					description="Load an Apple Health export.xml to inspect dataset paths, ingest details, persisted import history, and debugging metadata."
				/>
			) : (
				<>
					<div className="grid gap-4 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]">
						<Card
							size="sm"
							className="border border-border/70 bg-background/80"
						>
							<CardHeader>
								<div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
									<div>
										<CardTitle className="text-sm">
											Current analysis scope
										</CardTitle>
										<CardDescription>
											Useful when debugging why Health or Workouts looks sparse
											after filters.
										</CardDescription>
									</div>
									<div className="flex flex-wrap gap-2">
										<Badge
											variant={
												hasActiveWorkoutFilters ? "secondary" : "outline"
											}
											className="rounded-full px-3 py-1"
										>
											{hasActiveWorkoutFilters
												? `${formatInteger(activeWorkoutFilterCount)} workout filter${activeWorkoutFilterCount === 1 ? "" : "s"} active`
												: "Workout filters default"}
										</Badge>
										<Badge
											variant={hasActiveHealthFilters ? "secondary" : "outline"}
											className="rounded-full px-3 py-1"
										>
											{hasActiveHealthFilters
												? `${formatInteger(activeHealthFilterCount)} health filter${activeHealthFilterCount === 1 ? "" : "s"} active`
												: "Health filters default"}
										</Badge>
									</div>
								</div>
							</CardHeader>
							<CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
								<InfoMetric
									label="Workout range in scope"
									value={formatRange(
										dashboard?.inspection.overall.first_start ?? null,
										dashboard?.inspection.overall.last_end ?? null,
									)}
								/>
								<InfoMetric
									label="Health range in scope"
									value={formatRange(
										dashboard?.health_overview.first_start ?? null,
										dashboard?.health_overview.last_end ?? null,
									)}
								/>
								<InfoMetric
									label="Activity types in scope"
									value={dashboard?.available_activity_types.length ?? "—"}
								/>
								<InfoMetric
									label="Workout sources found"
									value={uniqueWorkoutSources}
								/>
								<InfoMetric
									label="Heart-rate coverage"
									value={
										sortedWorkouts.length === 0
											? "—"
											: `${formatInteger(workoutsWithHeartRate)} of ${formatInteger(sortedWorkouts.length)} workouts`
									}
								/>
								<InfoMetric
									label="Health categories in scope"
									value={expandedHealthGroups.length}
								/>
							</CardContent>
						</Card>

						<Card
							size="sm"
							className="border border-border/70 bg-background/80"
						>
							<CardHeader>
								<CardTitle className="text-sm">
									Latest import activity
								</CardTitle>
								<CardDescription>
									The freshest ingest progress message and the last completed
									import result.
								</CardDescription>
							</CardHeader>
							<CardContent className="space-y-4">
								<div className="rounded-2xl border border-border/70 bg-background/80 p-3">
									<p className="text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
										Progress event
									</p>
									<p className="mt-2 text-sm font-medium text-foreground">
										{latestIngestEvent?.label ?? "No recent progress event"}
									</p>
									<p className="mt-1 text-sm text-muted-foreground">
										{latestIngestEvent?.message ??
											"Start a dataset load to stream progress here."}
									</p>
								</div>

								<div className="rounded-2xl border border-border/70 bg-background/80 p-3">
									<p className="text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
										Last completed import
									</p>
									{latestHistoryEntry ? (
										<div className="mt-2 space-y-3">
											<div className="flex items-center justify-between gap-3">
												<Badge
													variant={
														latestHistoryEntry.status === "success"
															? "secondary"
															: "destructive"
													}
													className="rounded-full px-2.5 py-1"
												>
													{latestHistoryEntry.status === "success"
														? "Success"
														: "Failed"}
												</Badge>
												<span className="text-xs text-muted-foreground">
													{formatDateTime(latestHistoryEntry.finishedAt)}
												</span>
											</div>
											<p className="text-sm text-muted-foreground">
												{latestHistoryEntry.error ??
													latestHistoryEntry.sourceXmlPath ??
													"No additional import detail was recorded."}
											</p>
										</div>
									) : (
										<p className="mt-2 text-sm text-muted-foreground">
											No completed imports have been recorded yet.
										</p>
									)}
								</div>
							</CardContent>
						</Card>
					</div>

					<Card size="sm" className="border border-border/70 bg-background/80">
						<CardHeader>
							<CardTitle className="text-sm">Dataset metadata</CardTitle>
							<CardDescription>
								Raw key/value metadata returned by the dashboard payload for
								lower-level inspection.
							</CardDescription>
						</CardHeader>
						<CardContent>
							{datasetInfoEntries.length === 0 ? (
								<p className="text-sm text-muted-foreground">
									No additional dataset metadata is currently exposed by the
									backend.
								</p>
							) : (
								<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
									{datasetInfoEntries.map(([key, value]) => (
										<div
											key={key}
											className="rounded-2xl border border-border/70 bg-background/80 p-3"
										>
											<p className="text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
												{humanizeIdentifier(key)}
											</p>
											<p className="mt-2 break-all text-sm text-foreground">
												{formatDataPageValue(value)}
											</p>
										</div>
									))}
								</div>
							)}
						</CardContent>
					</Card>
				</>
			)}
		</section>
	);
}

function SettingsPage({
	themeMode,
	resolvedThemeMode,
	measurementSystem,
	defaultHealthMetricsExpanded,
	defaultWorkoutMetricsExpanded,
	onChangeThemeMode,
	onChangeMeasurementSystem,
	onChangeDefaultHealthMetricsExpanded,
	onChangeDefaultWorkoutMetricsExpanded,
}: {
	themeMode: ThemeMode;
	resolvedThemeMode: "light" | "dark";
	measurementSystem: MeasurementSystem;
	defaultHealthMetricsExpanded: boolean;
	defaultWorkoutMetricsExpanded: boolean;
	onChangeThemeMode: (value: ThemeMode) => void;
	onChangeMeasurementSystem: (value: MeasurementSystem) => void;
	onChangeDefaultHealthMetricsExpanded: (value: boolean) => void;
	onChangeDefaultWorkoutMetricsExpanded: (value: boolean) => void;
}) {
	return (
		<>
			<Card>
				<CardHeader>
					<CardTitle>Appearance and units</CardTitle>
					<CardDescription>
						Personalize how the desktop app looks and which measurement system
						the dashboard uses.
					</CardDescription>
				</CardHeader>
				<CardContent className="grid gap-4 xl:grid-cols-2">
					<Card size="sm" className="border border-border/70 bg-muted/20">
						<CardHeader>
							<CardTitle className="text-sm">Theme</CardTitle>
							<CardDescription>
								Pick a fixed theme or follow the system appearance.
							</CardDescription>
						</CardHeader>
						<CardContent className="space-y-3">
							<div className="grid gap-2 sm:grid-cols-3">
								<Button
									variant={themeMode === "system" ? "default" : "outline"}
									className="justify-start rounded-xl"
									onClick={() => onChangeThemeMode("system")}
								>
									<MonitorIcon className="size-4" />
									System
								</Button>
								<Button
									variant={themeMode === "light" ? "default" : "outline"}
									className="justify-start rounded-xl"
									onClick={() => onChangeThemeMode("light")}
								>
									<SunIcon className="size-4" />
									Light
								</Button>
								<Button
									variant={themeMode === "dark" ? "default" : "outline"}
									className="justify-start rounded-xl"
									onClick={() => onChangeThemeMode("dark")}
								>
									<MoonIcon className="size-4" />
									Dark
								</Button>
							</div>
							<InfoMetric
								label="Current appearance"
								value={formatThemeMode(resolvedThemeMode)}
							/>
						</CardContent>
					</Card>

					<Card size="sm" className="border border-border/70 bg-muted/20">
						<CardHeader>
							<CardTitle className="text-sm">Measurements</CardTitle>
							<CardDescription>
								Switch dashboard distances, pace, speed, and compatible health
								values between unit systems.
							</CardDescription>
						</CardHeader>
						<CardContent className="space-y-3">
							<div className="grid gap-2 sm:grid-cols-2">
								<Button
									variant={
										measurementSystem === "imperial" ? "default" : "outline"
									}
									className="justify-start rounded-xl"
									onClick={() => onChangeMeasurementSystem("imperial")}
								>
									Imperial
								</Button>
								<Button
									variant={
										measurementSystem === "metric" ? "default" : "outline"
									}
									className="justify-start rounded-xl"
									onClick={() => onChangeMeasurementSystem("metric")}
								>
									Metric
								</Button>
							</div>
							<InfoMetric
								label="Active unit set"
								value={
									measurementSystem === "metric"
										? "Metric distances and speeds (km, kph)"
										: "Imperial distances and speeds (mi, mph)"
								}
							/>
						</CardContent>
					</Card>
				</CardContent>
			</Card>

			<Card>
				<CardHeader>
					<CardTitle>Dashboard defaults</CardTitle>
					<CardDescription>
						Choose how much detail the dashboard should reveal by default when
						you return home.
					</CardDescription>
				</CardHeader>
				<CardContent className="space-y-3">
					<FilterCheckboxRow
						checked={defaultHealthMetricsExpanded}
						label="Open the full health metric library by default"
						onCheckedChange={onChangeDefaultHealthMetricsExpanded}
					/>
					<FilterCheckboxRow
						checked={defaultWorkoutMetricsExpanded}
						label="Open the selected workout metric drilldowns by default"
						onCheckedChange={onChangeDefaultWorkoutMetricsExpanded}
					/>
				</CardContent>
			</Card>
		</>
	);
}

function HelpPage() {
	return (
		<>
			<Card>
				<CardHeader>
					<CardTitle>Export Apple Health data</CardTitle>
					<CardDescription>
						Apple Health exports come from the Health app on your iPhone. Once
						you move the export to your Mac and extract it, Pulse Parse only
						needs the <code>export.xml</code> file.
					</CardDescription>
				</CardHeader>
				<CardContent className="grid gap-4 xl:grid-cols-2">
					<Card size="sm" className="border border-border/70 bg-muted/20">
						<CardHeader>
							<CardTitle className="text-sm">
								Step 1: Start the export on iPhone
							</CardTitle>
							<CardDescription>
								Open Apple Health on your iPhone, tap your profile picture, then
								choose{" "}
								<span className="font-medium text-foreground">
									Export All Health Data
								</span>
								.
							</CardDescription>
						</CardHeader>
						<CardContent className="space-y-3">
							<p className="text-sm text-muted-foreground">
								Apple packages the export as a zip archive. Large histories can
								take a little while to prepare.
							</p>
							<InfoMetric
								label="Path in Health"
								value="Profile photo -> Export All Health Data"
							/>
						</CardContent>
					</Card>

					<Card size="sm" className="border border-border/70 bg-muted/20">
						<CardHeader>
							<CardTitle className="text-sm">
								Step 2: Move the archive to your Mac
							</CardTitle>
							<CardDescription>
								Save or share the generated zip somewhere your Mac can open,
								like AirDrop, Files, or iCloud Drive.
							</CardDescription>
						</CardHeader>
						<CardContent className="space-y-3">
							<p className="text-sm text-muted-foreground">
								Keep the original archive if you want a backup, but extract it
								before loading anything into Pulse Parse.
							</p>
							<InfoMetric
								label="Expected transfer"
								value="One .zip archive from Apple Health"
							/>
						</CardContent>
					</Card>
				</CardContent>
			</Card>

			<Card>
				<CardHeader>
					<CardTitle>Load the correct file</CardTitle>
					<CardDescription>
						After extracting the archive, point this app at the XML export file
						instead of the zip.
					</CardDescription>
				</CardHeader>
				<CardContent className="grid gap-4 xl:grid-cols-3">
					<Card size="sm" className="border border-border/70 bg-background/80">
						<CardHeader>
							<CardTitle className="text-sm">1. Extract the archive</CardTitle>
						</CardHeader>
						<CardContent>
							<p className="text-sm text-muted-foreground">
								Double-click the downloaded zip on your Mac so Finder creates
								the extracted export folder.
							</p>
						</CardContent>
					</Card>

					<Card size="sm" className="border border-border/70 bg-background/80">
						<CardHeader>
							<CardTitle className="text-sm">2. Find export.xml</CardTitle>
						</CardHeader>
						<CardContent>
							<p className="text-sm text-muted-foreground">
								Open the extracted folder and locate <code>export.xml</code>.
								That is the file this app ingests.
							</p>
						</CardContent>
					</Card>

					<Card size="sm" className="border border-border/70 bg-background/80">
						<CardHeader>
							<CardTitle className="text-sm">3. Choose Load dataset</CardTitle>
						</CardHeader>
						<CardContent>
							<p className="text-sm text-muted-foreground">
								Return to the dashboard, click{" "}
								<span className="font-medium text-foreground">
									Load dataset
								</span>
								, and select <code>export.xml</code>.
							</p>
						</CardContent>
					</Card>
				</CardContent>
			</Card>

			<Card>
				<CardHeader>
					<CardTitle>Troubleshooting</CardTitle>
					<CardDescription>
						The usual issues are picking the wrong file or trying to load the
						archive before it is extracted.
					</CardDescription>
				</CardHeader>
				<CardContent className="grid gap-4 xl:grid-cols-3">
					<Card size="sm" className="border border-border/70 bg-muted/20">
						<CardHeader>
							<CardTitle className="text-sm">
								If the app rejects the file
							</CardTitle>
						</CardHeader>
						<CardContent>
							<p className="text-sm text-muted-foreground">
								Make sure you selected <code>export.xml</code> and not the zip
								archive or a sibling file like <code>export_cda.xml</code>.
							</p>
						</CardContent>
					</Card>

					<Card size="sm" className="border border-border/70 bg-muted/20">
						<CardHeader>
							<CardTitle className="text-sm">If import takes a while</CardTitle>
						</CardHeader>
						<CardContent>
							<p className="text-sm text-muted-foreground">
								Big Apple Health histories can take time to parse. Keep the app
								open and let the progress indicator finish.
							</p>
						</CardContent>
					</Card>

					<Card size="sm" className="border border-border/70 bg-muted/20">
						<CardHeader>
							<CardTitle className="text-sm">
								If you want a fresh export
							</CardTitle>
						</CardHeader>
						<CardContent>
							<p className="text-sm text-muted-foreground">
								Re-run the export in Apple Health, extract the new archive, then
								load the new <code>export.xml</code> file.
							</p>
						</CardContent>
					</Card>
				</CardContent>
			</Card>
		</>
	);
}

function HealthSnapshotCard({
	metric,
	measurementSystem,
	onSelect,
}: {
	metric: HealthOverviewMetric;
	measurementSystem: MeasurementSystem;
	onSelect: () => void;
}) {
	return (
		<button type="button" className="text-left" onClick={onSelect}>
			<Card
				size="sm"
				className="h-full border border-border/70 bg-background/80 transition-colors hover:border-primary/30 hover:bg-primary/5"
			>
				<CardContent className="space-y-3">
					<div className="flex items-center justify-between gap-2">
						<p className="text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
							{metric.category}
						</p>
						<Badge variant="outline" className="rounded-full">
							{metric.primary_label}
						</Badge>
					</div>
					<div className="space-y-1">
						<p className="text-sm font-medium text-foreground">
							{metric.label}
						</p>
						<p className="text-2xl font-semibold text-foreground">
							{formatHealthMetricValue(
								metric,
								metric.primary_value,
								measurementSystem,
							)}
						</p>
					</div>
					<p className="text-sm text-muted-foreground">
						{metric.summary_kind === "total"
							? `Best day: ${formatHealthMetricSummaryPair(metric.best_day, metric.best_day_value, metric, measurementSystem)}`
							: `Latest sample: ${formatDateTime(metric.latest_at)}`}
					</p>
				</CardContent>
			</Card>
		</button>
	);
}

function HealthMetricCard({
	metric,
	measurementSystem,
	onSelect,
}: {
	metric: HealthOverviewMetric;
	measurementSystem: MeasurementSystem;
	onSelect: () => void;
}) {
	return (
		<button type="button" className="text-left" onClick={onSelect}>
			<Card
				size="sm"
				className="h-full border border-border/70 bg-muted/20 transition-colors hover:border-primary/30 hover:bg-primary/5"
			>
				<CardHeader>
					<div className="flex items-center justify-between gap-2">
						<CardTitle className="text-sm">{metric.label}</CardTitle>
						<Badge variant="outline" className="rounded-full">
							{metric.category}
						</Badge>
					</div>
					<CardDescription>
						{metric.summary_kind === "total"
							? "Range total with daily trend for the current health filters."
							: `Latest non-workout sample: ${formatDateTime(metric.latest_at)}`}
					</CardDescription>
				</CardHeader>
				<CardContent className="space-y-4">
					<div className="space-y-1">
						<p className="text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
							{metric.primary_label}
						</p>
						<p className="text-2xl font-semibold text-foreground">
							{formatHealthMetricValue(
								metric,
								metric.primary_value,
								measurementSystem,
							)}
						</p>
					</div>
					<div className="grid gap-3 sm:grid-cols-2">
						<InfoMetric
							label={
								metric.summary_kind === "total" ? "Daily average" : "Average"
							}
							value={formatHealthMetricValue(
								metric,
								metric.summary_kind === "total"
									? metric.daily_average_value
									: metric.average_value,
								measurementSystem,
							)}
						/>
						<InfoMetric
							label={metric.summary_kind === "total" ? "Best day" : "Range"}
							value={
								metric.summary_kind === "total"
									? formatHealthMetricSummaryPair(
											metric.best_day,
											metric.best_day_value,
											metric,
											measurementSystem,
										)
									: formatHealthMetricRange(metric, measurementSystem)
							}
						/>
						<InfoMetric label="Samples" value={metric.sample_count} />
						<InfoMetric label="Graph" value={describeHealthTrend(metric)} />
					</div>
				</CardContent>
			</Card>
		</button>
	);
}

function HealthMetricModal({
	metric,
	measurementSystem,
	resolvedThemeMode,
	onClose,
}: {
	metric: HealthOverviewMetric;
	measurementSystem: MeasurementSystem;
	resolvedThemeMode: "light" | "dark";
	onClose: () => void;
}) {
	const chartPalette = getChartPalette(resolvedThemeMode);

	return (
		<div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-sm">
			<div className="max-h-[90vh] w-full max-w-5xl overflow-y-auto rounded-3xl border border-border/70 bg-background shadow-2xl">
				<div className="sticky top-0 z-10 flex items-start justify-between gap-4 border-b border-border/70 bg-background/95 px-6 py-5 backdrop-blur">
					<div className="space-y-1">
						<div className="flex flex-wrap items-center gap-2">
							<Badge variant="outline" className="rounded-full">
								{metric.category}
							</Badge>
							<Badge variant="secondary" className="rounded-full">
								{metric.primary_label}
							</Badge>
						</div>
						<h2 className="text-xl font-semibold text-foreground">
							{metric.label}
						</h2>
						<p className="text-sm text-muted-foreground">
							{metric.summary_kind === "total"
								? "Daily totals across the current health filter window."
								: "Daily averages across the current health filter window."}
						</p>
					</div>
					<Button
						variant="ghost"
						size="icon"
						className="rounded-full"
						onClick={onClose}
					>
						<XIcon className="size-4" />
					</Button>
				</div>

				<div className="space-y-6 px-6 py-6">
					<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
						<MetricCard
							label={metric.primary_label}
							value={formatHealthMetricValue(
								metric,
								metric.primary_value,
								measurementSystem,
							)}
						/>
						<MetricCard
							label={
								metric.summary_kind === "total" ? "Daily average" : "Average"
							}
							value={formatHealthMetricValue(
								metric,
								metric.summary_kind === "total"
									? metric.daily_average_value
									: metric.average_value,
								measurementSystem,
							)}
						/>
						<MetricCard
							label={
								metric.summary_kind === "total" ? "Best day" : "Latest sample"
							}
							value={
								metric.summary_kind === "total"
									? formatHealthMetricSummaryPair(
											metric.best_day,
											metric.best_day_value,
											metric,
											measurementSystem,
										)
									: formatDateTime(metric.latest_at)
							}
						/>
						<MetricCard label="Samples" value={metric.sample_count} />
					</div>

					<Card size="sm" className="border border-border/70 bg-muted/20">
						<CardHeader>
							<CardTitle className="text-sm">Trend graph</CardTitle>
							<CardDescription>{describeHealthTrend(metric)}</CardDescription>
						</CardHeader>
						<CardContent className="h-80">
							{metric.trend.length === 0 ? (
								<EmptyState
									title="No trend data is available"
									description="This metric does not have enough samples in the current filter window to draw a graph."
									compact
								/>
							) : (
								<ResponsiveContainer width="100%" height="100%">
									<LineChart data={metric.trend}>
										<CartesianGrid
											strokeDasharray="3 3"
											stroke={chartPalette.grid}
										/>
										<XAxis
											dataKey="date"
											tick={{ fill: chartPalette.tick, fontSize: 12 }}
										/>
										<YAxis
											tick={{ fill: chartPalette.tick, fontSize: 12 }}
											tickFormatter={(value) =>
												formatHealthMetricValue(
													metric,
													typeof value === "number" ? value : null,
													measurementSystem,
												)
											}
										/>
										<Tooltip
											formatter={(value) =>
												formatHealthMetricValue(
													metric,
													typeof value === "number" ? value : null,
													measurementSystem,
												)
											}
											labelFormatter={(value) =>
												typeof value === "string"
													? new Date(value).toLocaleDateString()
													: "—"
											}
										/>
										<Line
											type="monotone"
											dataKey="value"
											stroke={chartPalette.workouts}
											strokeWidth={3}
											dot={{ r: 3 }}
										/>
									</LineChart>
								</ResponsiveContainer>
							)}
						</CardContent>
					</Card>

					<div className="grid gap-4 xl:grid-cols-2">
						<Card size="sm" className="border border-border/70 bg-muted/20">
							<CardHeader>
								<CardTitle className="text-sm">Summary stats</CardTitle>
								<CardDescription>
									Helpful context for the selected health metric.
								</CardDescription>
							</CardHeader>
							<CardContent className="grid gap-3 sm:grid-cols-2">
								<InfoMetric
									label="Average"
									value={formatHealthMetricValue(
										metric,
										metric.average_value,
										measurementSystem,
									)}
								/>
								<InfoMetric
									label="Range"
									value={formatHealthMetricRange(metric, measurementSystem)}
								/>
								<InfoMetric
									label="Range total"
									value={formatHealthMetricValue(
										metric,
										metric.total_value,
										measurementSystem,
									)}
								/>
								<InfoMetric
									label="Best day"
									value={formatHealthMetricSummaryPair(
										metric.best_day,
										metric.best_day_value,
										metric,
										measurementSystem,
									)}
								/>
							</CardContent>
						</Card>

						<Card size="sm" className="border border-border/70 bg-muted/20">
							<CardHeader>
								<CardTitle className="text-sm">Latest sample</CardTitle>
								<CardDescription>
									Most recent non-workout record returned for this metric.
								</CardDescription>
							</CardHeader>
							<CardContent className="grid gap-3 sm:grid-cols-2">
								<InfoMetric
									label="Latest value"
									value={formatHealthMetricValue(
										metric,
										metric.latest_value,
										measurementSystem,
									)}
								/>
								<InfoMetric
									label="Recorded at"
									value={formatDateTime(metric.latest_at)}
								/>
								<InfoMetric
									label="Unit"
									value={
										formatDisplayUnit(metric.unit, measurementSystem) ?? "—"
									}
								/>
								<InfoMetric
									label="Graph type"
									value={describeHealthTrend(metric)}
								/>
							</CardContent>
						</Card>
					</div>
				</div>
			</div>
		</div>
	);
}

function WorkoutMetricModal({
	metric,
	workout,
	measurementSystem,
	resolvedThemeMode,
	onClose,
}: {
	metric: WorkoutMetricSeries;
	workout: SummaryWorkoutCard;
	measurementSystem: MeasurementSystem;
	resolvedThemeMode: "light" | "dark";
	onClose: () => void;
}) {
	const chartPalette = getChartPalette(resolvedThemeMode);
	const firstPoint = metric.points[0] ?? null;
	const lastPoint = metric.points[metric.points.length - 1] ?? null;
	const workoutContextMetrics: Array<{
		label: string;
		value: string | number;
	}> = [
		{ label: "Workout date", value: workout.date },
		...(isPresentNumber(workout.duration_minutes)
			? [
					{
						label: "Duration",
						value: formatMaybeNumber(
							workout.duration_minutes,
							"min",
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(workout.distance_miles)
			? [
					{
						label: "Distance",
						value: formatMaybeNumber(
							workout.distance_miles,
							"mi",
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(workout.avg_heart_rate)
			? [
					{
						label: "Average heart rate",
						value: formatMaybeNumber(
							workout.avg_heart_rate,
							"bpm",
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(workout.elevation_gain_ft)
			? [
					{
						label: "Elevation gain",
						value: formatWorkoutElevationGain(
							workout.elevation_gain_ft,
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(workout.temperature_f)
			? [
					{
						label: "Temperature",
						value: formatWorkoutTemperature(
							workout.temperature_f,
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(workout.avg_running_cadence_spm)
			? [
					{
						label: "Running cadence",
						value: formatMaybeNumber(
							workout.avg_running_cadence_spm,
							"spm",
							measurementSystem,
						),
					},
				]
			: []),
	];

	return (
		<div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-sm">
			<div className="max-h-[90vh] w-full max-w-5xl overflow-y-auto rounded-3xl border border-border/70 bg-background shadow-2xl">
				<div className="sticky top-0 z-10 flex items-start justify-between gap-4 border-b border-border/70 bg-background/95 px-6 py-5 backdrop-blur">
					<div className="space-y-1">
						<div className="flex flex-wrap items-center gap-2">
							<Badge variant="outline" className="rounded-full">
								{workout.type}
							</Badge>
							<Badge variant="secondary" className="rounded-full">
								{metric.label}
							</Badge>
						</div>
						<h2 className="text-xl font-semibold text-foreground">
							{metric.label}
						</h2>
						<p className="text-sm text-muted-foreground">
							{workout.title} • {formatDateTime(workout.start)}
						</p>
					</div>
					<Button
						variant="ghost"
						size="icon"
						className="rounded-full"
						onClick={onClose}
					>
						<XIcon className="size-4" />
					</Button>
				</div>

				<div className="space-y-6 px-6 py-6">
					<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
						<MetricCard
							label="Latest value"
							value={formatWorkoutMetricValue(
								metric,
								metric.latestValue,
								measurementSystem,
							)}
						/>
						<MetricCard
							label="Average"
							value={formatWorkoutMetricValue(
								metric,
								metric.average,
								measurementSystem,
							)}
						/>
						<MetricCard
							label="Range"
							value={formatWorkoutMetricRange(metric, measurementSystem)}
						/>
						<MetricCard label="Samples" value={metric.sampleCount} />
					</div>

					<Card size="sm" className="border border-border/70 bg-muted/20">
						<CardHeader>
							<CardTitle className="text-sm">Workout metric timeline</CardTitle>
							<CardDescription>
								Samples plotted across the selected workout from start to
								finish.
							</CardDescription>
						</CardHeader>
						<CardContent className="h-80">
							{metric.points.length === 0 ? (
								<EmptyState
									title="No metric samples are available"
									description="This workout does not include enough samples to draw a timeline."
									compact
								/>
							) : (
								<ResponsiveContainer width="100%" height="100%">
									<LineChart data={metric.points}>
										<CartesianGrid
											strokeDasharray="3 3"
											stroke={chartPalette.grid}
										/>
										<XAxis
											type="number"
											dataKey="elapsedMinutes"
											domain={["dataMin", "dataMax"]}
											tick={{ fill: chartPalette.tick, fontSize: 12 }}
											tickFormatter={(value) =>
												typeof value === "number"
													? formatElapsedMinutes(value)
													: "—"
											}
										/>
										<YAxis
											tick={{ fill: chartPalette.tick, fontSize: 12 }}
											tickFormatter={(value) =>
												formatWorkoutMetricValue(
													metric,
													typeof value === "number" ? value : null,
													measurementSystem,
												)
											}
										/>
										<Tooltip
											formatter={(value) =>
												formatWorkoutMetricValue(
													metric,
													typeof value === "number" ? value : null,
													measurementSystem,
												)
											}
											labelFormatter={(value) =>
												typeof value === "number"
													? `Elapsed ${formatElapsedMinutes(value)}`
													: "—"
											}
										/>
										<Line
											type="monotone"
											dataKey="value"
											stroke={chartPalette.distance}
											strokeWidth={3}
											dot={{ r: 3 }}
										/>
									</LineChart>
								</ResponsiveContainer>
							)}
						</CardContent>
					</Card>

					<div className="grid gap-4 xl:grid-cols-2">
						<Card size="sm" className="border border-border/70 bg-muted/20">
							<CardHeader>
								<CardTitle className="text-sm">Sample coverage</CardTitle>
								<CardDescription>
									Where this metric appears within the workout timeline.
								</CardDescription>
							</CardHeader>
							<CardContent className="grid gap-3 sm:grid-cols-2">
								<InfoMetric
									label="Timeline span"
									value={formatWorkoutMetricTimeline(metric)}
								/>
								<InfoMetric
									label="Unit"
									value={
										formatDisplayUnit(metric.unit, measurementSystem) ?? "—"
									}
								/>
								<InfoMetric
									label="First sample"
									value={formatDateTime(firstPoint?.timestamp ?? null)}
								/>
								<InfoMetric
									label="Last sample"
									value={formatDateTime(lastPoint?.timestamp ?? null)}
								/>
							</CardContent>
						</Card>

						<Card size="sm" className="border border-border/70 bg-muted/20">
							<CardHeader>
								<CardTitle className="text-sm">Workout context</CardTitle>
								<CardDescription>
									Quick context for the selected workout.
								</CardDescription>
							</CardHeader>
							<CardContent className="grid gap-3 sm:grid-cols-2">
								{workoutContextMetrics.map((metricItem) => (
									<InfoMetric
										key={metricItem.label}
										label={metricItem.label}
										value={metricItem.value}
									/>
								))}
							</CardContent>
						</Card>
					</div>
				</div>
			</div>
		</div>
	);
}

function SelectedWorkoutDetailSection({
	isLoadingWorkout,
	hasDashboard,
	selectedWorkoutCard,
	workoutDetail,
	workoutMetricSeries,
	hasLoadedWorkoutMetricSeries,
	isLoadingWorkoutMetricSeries,
	measurementSystem,
	isWorkoutMetricsExpanded,
	onToggleWorkoutMetricsExpanded,
	onSelectWorkoutMetric,
}: {
	isLoadingWorkout: boolean;
	hasDashboard: boolean;
	selectedWorkoutCard: SummaryWorkoutCard | null;
	workoutDetail: WorkoutDetailPayload | null;
	workoutMetricSeries: WorkoutMetricSeries[];
	hasLoadedWorkoutMetricSeries: boolean;
	isLoadingWorkoutMetricSeries: boolean;
	measurementSystem: MeasurementSystem;
	isWorkoutMetricsExpanded: boolean;
	onToggleWorkoutMetricsExpanded: () => void;
	onSelectWorkoutMetric: (metricKey: string) => void;
}) {
	if (!hasDashboard) {
		return (
			<EmptyState
				title="Selected workout detail is waiting on a dataset"
				description="Load a dataset to inspect one workout in detail and open workout metric charts."
			/>
		);
	}

	if (isLoadingWorkout) {
		return (
			<EmptyState
				title="Loading selected workout detail"
				description="Pulling lightweight detail, linked-data counts, and summary metrics now."
				compact
			/>
		);
	}

	if (!workoutDetail || !selectedWorkoutCard) {
		return (
			<EmptyState
				title="Select a workout to inspect"
				description="Choose a workout from the list to inspect linked records, metadata, and metric drilldowns."
				compact
			/>
		);
	}

	const visibleWorkoutMetrics = isWorkoutMetricsExpanded
		? workoutMetricSeries
		: workoutMetricSeries.slice(0, WORKOUT_METRIC_PREVIEW_LIMIT);
	const hasLinkedWorkoutRecords = workoutDetail.linked_data_counts.records > 0;
	const selectedWorkoutSummaryMetrics: Array<{
		label: string;
		value: string | number;
	}> = [
		...(isPresentNumber(selectedWorkoutCard.distance_miles)
			? [
					{
						label: "Distance",
						value: formatMaybeNumber(
							selectedWorkoutCard.distance_miles,
							"mi",
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(selectedWorkoutCard.duration_minutes)
			? [
					{
						label: "Duration",
						value: formatMaybeNumber(
							selectedWorkoutCard.duration_minutes,
							"min",
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(selectedWorkoutCard.avg_heart_rate)
			? [
					{
						label: "Average heart rate",
						value: formatMaybeNumber(
							selectedWorkoutCard.avg_heart_rate,
							"bpm",
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(selectedWorkoutCard.elevation_gain_ft)
			? [
					{
						label: "Elevation gain",
						value: formatWorkoutElevationGain(
							selectedWorkoutCard.elevation_gain_ft,
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(selectedWorkoutCard.temperature_f)
			? [
					{
						label: "Temperature",
						value: formatWorkoutTemperature(
							selectedWorkoutCard.temperature_f,
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(selectedWorkoutCard.avg_running_cadence_spm)
			? [
					{
						label: "Running cadence",
						value: formatMaybeNumber(
							selectedWorkoutCard.avg_running_cadence_spm,
							"spm",
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(selectedWorkoutCard.pace_min_per_mile)
			? [
					{
						label: "Pace",
						value: formatMaybeNumber(
							selectedWorkoutCard.pace_min_per_mile,
							"min/mi",
							measurementSystem,
						),
					},
				]
			: []),
	];
	const heartRateMetrics = workoutDetail.derived_metrics.heart_rate;
	const derivedWorkoutMetrics: Array<{
		label: string;
		value: string | number;
	}> = [
		{
			label: "Associated records",
			value: workoutDetail.derived_metrics.associated_record_count,
		},
		...(isPresentNumber(workoutDetail.derived_metrics.speed_kph)
			? [
					{
						label: "Speed",
						value: formatMaybeNumber(
							workoutDetail.derived_metrics.speed_kph,
							"kph",
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(heartRateMetrics?.sample_count)
			? [
					{
						label: "Heart-rate samples",
						value: heartRateMetrics.sample_count,
					},
				]
			: []),
		...(isPresentNumber(heartRateMetrics?.maximum)
			? [
					{
						label: "Peak heart rate",
						value: formatMaybeNumber(heartRateMetrics.maximum, "bpm"),
					},
				]
			: []),
		...(isPresentNumber(workoutDetail.derived_metrics.elevation_gain_ft)
			? [
					{
						label: "Elevation gain",
						value: formatWorkoutElevationGain(
							workoutDetail.derived_metrics.elevation_gain_ft,
							measurementSystem,
						),
					},
				]
			: []),
		...(isPresentNumber(workoutDetail.derived_metrics.temperature_f)
			? [
					{
						label: "Temperature",
						value: formatWorkoutTemperature(
							workoutDetail.derived_metrics.temperature_f,
							measurementSystem,
						),
					},
				]
			: []),
	];

	return (
		<div className="space-y-4">
			<Card size="sm" className="border border-primary/15 bg-primary/5">
				<CardHeader>
					<div className="flex flex-wrap items-center gap-2">
						<Badge variant="secondary" className="rounded-full">
							{selectedWorkoutCard.type}
						</Badge>
						{selectedWorkoutCard.effort ? (
							<WorkoutEffortBadge effort={selectedWorkoutCard.effort} />
						) : null}
					</div>
					<CardTitle>{selectedWorkoutCard.title}</CardTitle>
					<CardDescription>{selectedWorkoutCard.summary}</CardDescription>
				</CardHeader>
				<CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
					{selectedWorkoutSummaryMetrics.map((metricItem) => (
						<MetricCard
							key={metricItem.label}
							label={metricItem.label}
							value={metricItem.value}
						/>
					))}
				</CardContent>
			</Card>

			<div className="grid gap-4 2xl:grid-cols-2">
				<Card size="sm" className="border border-border/70 bg-muted/20">
					<CardHeader>
						<CardTitle className="text-sm">Linked data</CardTitle>
						<CardDescription>
							Counts surfaced directly from the workout detail payload.
						</CardDescription>
					</CardHeader>
					<CardContent className="grid gap-3 sm:grid-cols-2">
						<InfoMetric
							label="Records"
							value={workoutDetail.linked_data_counts.records}
						/>
						<InfoMetric
							label="Metadata rows"
							value={workoutDetail.linked_data_counts.metadata}
						/>
						<InfoMetric
							label="Routes"
							value={workoutDetail.linked_data_counts.routes}
						/>
						<InfoMetric
							label="Events"
							value={workoutDetail.linked_data_counts.events}
						/>
					</CardContent>
				</Card>

				<Card size="sm" className="border border-border/70 bg-muted/20">
					<CardHeader>
						<CardTitle className="text-sm">Derived metrics</CardTitle>
						<CardDescription>
							Calculated from the workout payload and linked heart-rate samples.
						</CardDescription>
					</CardHeader>
					<CardContent className="grid gap-3 sm:grid-cols-2">
						{derivedWorkoutMetrics.map((metricItem) => (
							<InfoMetric
								key={metricItem.label}
								label={metricItem.label}
								value={metricItem.value}
							/>
						))}
					</CardContent>
				</Card>
			</div>

			<Card size="sm" className="border border-border/70 bg-muted/20">
				<CardHeader className="gap-3">
					<div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
						<div>
							<CardTitle className="text-sm">
								Workout metric drilldowns
							</CardTitle>
							<CardDescription>
								Open heart rate and other linked numeric workout metrics across
								the full workout timeline.
							</CardDescription>
						</div>
						{!hasLoadedWorkoutMetricSeries && hasLinkedWorkoutRecords ? (
							<Button
								variant="outline"
								className="rounded-xl"
								onClick={onToggleWorkoutMetricsExpanded}
								disabled={isLoadingWorkoutMetricSeries}
							>
								{isLoadingWorkoutMetricSeries ? (
									<LoaderCircleIcon className="size-4 animate-spin" />
								) : (
									<ChevronDownIcon className="size-4" />
								)}
								{isLoadingWorkoutMetricSeries
									? "Loading drilldowns"
									: "Load workout metric drilldowns"}
							</Button>
						) : workoutMetricSeries.length > WORKOUT_METRIC_PREVIEW_LIMIT ? (
							<Button
								variant="outline"
								className="rounded-xl"
								onClick={onToggleWorkoutMetricsExpanded}
							>
								{isWorkoutMetricsExpanded ? (
									<ChevronUpIcon className="size-4" />
								) : (
									<ChevronDownIcon className="size-4" />
								)}
								{isWorkoutMetricsExpanded
									? "Hide full metric list"
									: `View all ${formatInteger(workoutMetricSeries.length)} workout metrics`}
							</Button>
						) : null}
					</div>
				</CardHeader>
				<CardContent>
					{!hasLinkedWorkoutRecords ? (
						<p className="text-sm text-muted-foreground">
							No workout-linked records are available for this workout.
						</p>
					) : isLoadingWorkoutMetricSeries ? (
						<p className="text-sm text-muted-foreground">
							Loading numeric workout metrics now so the main workout detail can
							stay lightweight.
						</p>
					) : !hasLoadedWorkoutMetricSeries ? (
						<p className="text-sm text-muted-foreground">
							Load drilldowns when you want the full linked metric timelines for
							this workout.
						</p>
					) : workoutMetricSeries.length === 0 ? (
						<p className="text-sm text-muted-foreground">
							No numeric workout-linked metric samples are available for this
							workout.
						</p>
					) : (
						<div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
							{visibleWorkoutMetrics.map((metric) => (
								<button
									key={metric.key}
									type="button"
									className="text-left"
									onClick={() => onSelectWorkoutMetric(metric.key)}
								>
									<Card
										size="sm"
										className="h-full border border-border/70 bg-background/80 transition-colors hover:border-primary/30 hover:bg-primary/5"
									>
										<CardHeader>
											<div className="flex items-center justify-between gap-2">
												<CardTitle className="text-sm">
													{metric.label}
												</CardTitle>
												<Badge variant="outline" className="rounded-full">
													{metric.sampleCount.toLocaleString()} samples
												</Badge>
											</div>
											<CardDescription>
												{metric.unit
													? `Recorded in ${metric.unit} across the selected workout.`
													: "Numeric samples recorded across the selected workout."}
											</CardDescription>
										</CardHeader>
										<CardContent className="space-y-4">
											<div className="space-y-1">
												<p className="text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
													Latest value
												</p>
												<p className="text-2xl font-semibold text-foreground">
													{formatWorkoutMetricValue(
														metric,
														metric.latestValue,
														measurementSystem,
													)}
												</p>
											</div>
											<div className="grid gap-3 sm:grid-cols-2">
												<InfoMetric
													label="Average"
													value={formatWorkoutMetricValue(
														metric,
														metric.average,
														measurementSystem,
													)}
												/>
												<InfoMetric
													label="Range"
													value={formatWorkoutMetricRange(
														metric,
														measurementSystem,
													)}
												/>
												<InfoMetric
													label="Timeline"
													value={formatWorkoutMetricTimeline(metric)}
												/>
												<InfoMetric
													label="Latest sample"
													value={formatDateTime(metric.latestAt)}
												/>
											</div>
										</CardContent>
									</Card>
								</button>
							))}
						</div>
					)}
				</CardContent>
			</Card>

			<Card size="sm" className="border border-border/70 bg-muted/20">
				<CardHeader>
					<CardTitle className="text-sm">Workout metadata</CardTitle>
					<CardDescription>
						Useful keys from the Apple Health workout payload.
					</CardDescription>
				</CardHeader>
				<CardContent>
					{workoutDetail.metadata.length === 0 ? (
						<p className="text-sm text-muted-foreground">
							No metadata was attached to this workout.
						</p>
					) : (
						<div className="flex flex-wrap gap-2">
							{workoutDetail.metadata.map((entry) => (
								<Badge
									key={`${entry.key}-${entry.value ?? "empty"}`}
									variant="outline"
									className="rounded-xl px-3 py-2 text-left"
								>
									<span className="font-medium text-foreground">
										{entry.key}
									</span>
									<span className="ml-2 text-muted-foreground">
										{entry.value ?? "null"}
									</span>
								</Badge>
							))}
						</div>
					)}
				</CardContent>
			</Card>
		</div>
	);
}

function FilterButton({
	children,
	selected,
	className,
	onClick,
}: {
	children: string;
	selected: boolean;
	className?: string;
	onClick: () => void;
}) {
	return (
		<Button
			type="button"
			variant={selected ? "default" : "outline"}
			size="sm"
			className={cn("rounded-full", className)}
			onClick={onClick}
		>
			{children}
		</Button>
	);
}

function FilterNumberRangeField({
	label,
	minValue,
	maxValue,
	minPlaceholder,
	maxPlaceholder,
	onMinChange,
	onMaxChange,
}: {
	label: string;
	minValue: string;
	maxValue: string;
	minPlaceholder: string;
	maxPlaceholder: string;
	onMinChange: (value: string) => void;
	onMaxChange: (value: string) => void;
}) {
	return (
		<div className="space-y-2">
			<p className="text-sm font-medium text-foreground">{label}</p>
			<div className="grid grid-cols-2 gap-2">
				<Input
					value={minValue}
					onChange={(event) => onMinChange(event.target.value)}
					placeholder={minPlaceholder}
					inputMode="decimal"
					aria-label={`${label} minimum`}
				/>
				<Input
					value={maxValue}
					onChange={(event) => onMaxChange(event.target.value)}
					placeholder={maxPlaceholder}
					inputMode="decimal"
					aria-label={`${label} maximum`}
				/>
			</div>
		</div>
	);
}

function FilterCheckboxRow({
	checked,
	label,
	onCheckedChange,
}: {
	checked: boolean;
	label: string;
	onCheckedChange: (checked: boolean) => void;
}) {
	const checkboxId = useId();

	return (
		<label
			htmlFor={checkboxId}
			className="flex cursor-pointer items-start gap-3 rounded-xl border border-border/60 bg-muted/20 px-3 py-3"
		>
			<Checkbox
				id={checkboxId}
				checked={checked}
				onCheckedChange={(value) => onCheckedChange(value === true)}
				className="mt-0.5"
			/>
			<span className="text-sm text-foreground">{label}</span>
		</label>
	);
}

function InfoMetric({
	label,
	value,
}: {
	label: string;
	value: string | number;
}) {
	return (
		<div className="rounded-2xl border border-border/60 bg-background/80 p-3">
			<p className="text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
				{label}
			</p>
			<p className="mt-2 break-all whitespace-normal text-sm font-medium text-foreground">
				{formatInlineMetricValue(value)}
			</p>
		</div>
	);
}

function IndeterminateProgressBar() {
	return (
		<div className="relative h-2 overflow-hidden rounded-full bg-muted">
			<div className="absolute inset-y-0 left-0 w-1/3 rounded-full bg-primary apple-health-loading-bar" />
		</div>
	);
}

function ActivityTable({
	rows,
	measurementSystem,
}: {
	rows: ActivityBreakdownRow[];
	measurementSystem: MeasurementSystem;
}) {
	return (
		<Table>
			<TableHeader>
				<TableRow>
					<TableHead>Type</TableHead>
					<TableHead>Count</TableHead>
					<TableHead>
						{measurementSystem === "metric" ? "Distance (km)" : "Distance (mi)"}
					</TableHead>
					<TableHead>Energy</TableHead>
				</TableRow>
			</TableHeader>
			<TableBody>
				{rows.map((row) => (
					<TableRow key={row.activity_type}>
						<TableCell>{row.type}</TableCell>
						<TableCell>{formatInteger(row.count)}</TableCell>
						<TableCell>
							{formatMaybeNumber(
								row.total_distance_miles,
								"mi",
								measurementSystem,
							)}
						</TableCell>
						<TableCell>
							{formatMaybeNumber(
								row.total_energy_kcal,
								"kcal",
								measurementSystem,
							)}
						</TableCell>
					</TableRow>
				))}
			</TableBody>
		</Table>
	);
}

function EmptyState({
	title,
	description,
	compact = false,
}: {
	title: string;
	description: string;
	compact?: boolean;
}) {
	return (
		<div
			className={cn(
				"flex flex-col items-center justify-center rounded-2xl border border-dashed border-border/80 bg-muted/20 px-6 py-14 text-center",
				compact && "py-10",
			)}
		>
			<p className="text-base font-medium text-foreground">{title}</p>
			<p className="mt-2 max-w-xl text-sm leading-6 text-muted-foreground">
				{description}
			</p>
		</div>
	);
}

function WorkoutBadge({ children }: { children: string }) {
	return (
		<Badge variant="outline" className="rounded-full px-2.5 py-1 text-xs">
			{children}
		</Badge>
	);
}

function WorkoutEffortBadge({ effort }: { effort: string }) {
	return (
		<Badge
			variant="outline"
			className={cn(
				"rounded-full px-2.5 py-1 text-xs",
				getWorkoutEffortStyles(effort).badge,
			)}
		>
			{effort}
		</Badge>
	);
}

function getWorkoutEffortStyles(effort: string | null | undefined) {
	switch (effort?.toLowerCase()) {
		case "easy":
			return {
				badge:
					"border-[#E6745D]/35 bg-[#E6745D]/10 text-[#C45D45] dark:border-[#E6745D]/30 dark:bg-[#E6745D]/15 dark:text-[#F2B5A8]",
				button:
					"border-[#E6745D]/35 text-[#C45D45] hover:bg-[#E6745D]/10 hover:text-[#A63B1E] dark:border-[#E6745D]/30 dark:text-[#F2B5A8] dark:hover:bg-[#E6745D]/15 dark:hover:text-[#FFE3DC]",
				buttonSelected:
					"border-[#E6745D]/45 bg-[#E6745D]/15 text-[#A63B1E] hover:bg-[#E6745D]/20 dark:border-[#E6745D]/35 dark:bg-[#E6745D]/22 dark:text-[#FFE3DC] dark:hover:bg-[#E6745D]/26",
			};
		case "easy-moderate":
			return {
				badge:
					"border-[#E6745D]/45 bg-[#E6745D]/12 text-[#B95137] dark:border-[#E6745D]/35 dark:bg-[#E6745D]/18 dark:text-[#F6C0B4]",
				button:
					"border-[#E6745D]/45 text-[#B95137] hover:bg-[#E6745D]/12 hover:text-[#A63B1E] dark:border-[#E6745D]/35 dark:text-[#F6C0B4] dark:hover:bg-[#E6745D]/18 dark:hover:text-[#FFE3DC]",
				buttonSelected:
					"border-[#E6745D]/55 bg-[#E6745D]/18 text-[#8E341C] hover:bg-[#E6745D]/22 dark:border-[#E6745D]/40 dark:bg-[#E6745D]/24 dark:text-[#FFF0EC] dark:hover:bg-[#E6745D]/28",
			};
		case "moderate":
			return {
				badge:
					"border-[#A63B1E]/40 bg-[#A63B1E]/10 text-[#A63B1E] dark:border-[#E6745D]/35 dark:bg-[#A63B1E]/20 dark:text-[#F8C3B8]",
				button:
					"border-[#A63B1E]/40 text-[#A63B1E] hover:bg-[#A63B1E]/10 dark:border-[#E6745D]/35 dark:text-[#F8C3B8] dark:hover:bg-[#A63B1E]/20 dark:hover:text-[#FFF0EC]",
				buttonSelected:
					"border-[#A63B1E]/55 bg-[#A63B1E]/16 text-[#8C3118] hover:bg-[#A63B1E]/20 dark:border-[#E6745D]/35 dark:bg-[#A63B1E]/28 dark:text-[#FFF0EC] dark:hover:bg-[#A63B1E]/32",
			};
		case "hard":
			return {
				badge:
					"border-[#A63B1E]/55 bg-[#A63B1E]/15 text-[#8C3118] dark:border-[#A63B1E]/50 dark:bg-[#A63B1E]/30 dark:text-[#FFD9D0]",
				button:
					"border-[#A63B1E]/55 text-[#8C3118] hover:bg-[#A63B1E]/12 dark:border-[#A63B1E]/50 dark:text-[#FFD9D0] dark:hover:bg-[#A63B1E]/24 dark:hover:text-[#FFF0EC]",
				buttonSelected:
					"border-[#A63B1E]/70 bg-[#A63B1E]/24 text-[#702511] hover:bg-[#A63B1E]/28 dark:border-[#A63B1E]/55 dark:bg-[#A63B1E]/38 dark:text-white dark:hover:bg-[#A63B1E]/44",
			};
		case "very hard":
			return {
				badge:
					"border-[#A63B1E] bg-[#A63B1E] text-white dark:border-[#E6745D]/70 dark:bg-[#A63B1E] dark:text-white",
				button:
					"border-[#A63B1E]/70 text-[#702511] hover:bg-[#A63B1E]/12 dark:border-[#E6745D]/45 dark:text-[#FFD9D0] dark:hover:bg-[#A63B1E]/24 dark:hover:text-white",
				buttonSelected:
					"border-[#A63B1E] bg-[#A63B1E] text-white hover:bg-[#8D3118] dark:border-[#E6745D]/70 dark:bg-[#A63B1E] dark:text-white dark:hover:bg-[#8D3118]",
			};
		default:
			return {
				badge: "border-border/70 bg-background/80 text-foreground",
				button: "",
				buttonSelected: "",
			};
	}
}

function getWorkoutListBadges(
	workout: SummaryWorkoutCard,
	measurementSystem: MeasurementSystem,
): string[] {
	const badges: string[] = [];

	if (isPresentNumber(workout.distance_miles)) {
		badges.push(
			formatMaybeNumber(workout.distance_miles, "mi", measurementSystem),
		);
	}
	if (isPresentNumber(workout.duration_minutes)) {
		badges.push(
			formatMaybeNumber(workout.duration_minutes, "min", measurementSystem),
		);
	}
	if (isPresentNumber(workout.avg_heart_rate)) {
		badges.push(
			formatMaybeNumber(workout.avg_heart_rate, "bpm", measurementSystem),
		);
	}
	if (isPresentNumber(workout.avg_running_cadence_spm)) {
		badges.push(
			formatMaybeNumber(
				workout.avg_running_cadence_spm,
				"spm",
				measurementSystem,
			),
		);
	}

	return badges;
}

function sortWorkoutsNewestFirst(
	workouts: SummaryWorkoutCard[],
): SummaryWorkoutCard[] {
	return [...workouts].sort((left, right) => {
		const leftStart = Date.parse(left.start);
		const rightStart = Date.parse(right.start);
		if (Number.isNaN(leftStart) || Number.isNaN(rightStart)) {
			return right.date.localeCompare(left.date);
		}
		return rightStart - leftStart;
	});
}

function toggleStringSelection(values: string[], nextValue: string): string[] {
	if (values.includes(nextValue)) {
		return values.filter((value) => value !== nextValue);
	}
	return [...values, nextValue];
}

function parseOptionalNumber(value: string): number | undefined {
	const trimmed = value.trim();
	if (!trimmed) {
		return undefined;
	}

	const parsed = Number(trimmed);
	return Number.isFinite(parsed) ? parsed : undefined;
}

function isWorkoutFiltersDefault(filters: FiltersState): boolean {
	return (
		filters.start === EMPTY_FILTERS.start &&
		filters.end === EMPTY_FILTERS.end &&
		filters.activityTypes.length === EMPTY_FILTERS.activityTypes.length &&
		filters.sourceQuery.trim() === EMPTY_FILTERS.sourceQuery &&
		filters.minDurationMinutes.trim() === EMPTY_FILTERS.minDurationMinutes &&
		filters.maxDurationMinutes.trim() === EMPTY_FILTERS.maxDurationMinutes &&
		filters.location === EMPTY_FILTERS.location &&
		filters.minDistanceMiles.trim() === EMPTY_FILTERS.minDistanceMiles &&
		filters.maxDistanceMiles.trim() === EMPTY_FILTERS.maxDistanceMiles &&
		filters.minEnergyKcal.trim() === EMPTY_FILTERS.minEnergyKcal &&
		filters.maxEnergyKcal.trim() === EMPTY_FILTERS.maxEnergyKcal &&
		filters.minAvgHeartRate.trim() === EMPTY_FILTERS.minAvgHeartRate &&
		filters.maxAvgHeartRate.trim() === EMPTY_FILTERS.maxAvgHeartRate &&
		filters.minMaxHeartRate.trim() === EMPTY_FILTERS.minMaxHeartRate &&
		filters.maxMaxHeartRate.trim() === EMPTY_FILTERS.maxMaxHeartRate &&
		filters.efforts.length === EMPTY_FILTERS.efforts.length &&
		filters.requiresRouteData === EMPTY_FILTERS.requiresRouteData &&
		filters.requiresHeartRateSamples === EMPTY_FILTERS.requiresHeartRateSamples
	);
}

function isHealthFiltersDefault(filters: HealthFiltersState): boolean {
	return (
		filters.start === EMPTY_HEALTH_FILTERS.start &&
		filters.end === EMPTY_HEALTH_FILTERS.end &&
		filters.categories.length === EMPTY_HEALTH_FILTERS.categories.length &&
		filters.metricQuery.trim() === EMPTY_HEALTH_FILTERS.metricQuery &&
		filters.sourceQuery.trim() === EMPTY_HEALTH_FILTERS.sourceQuery &&
		filters.onlyWithSamples === EMPTY_HEALTH_FILTERS.onlyWithSamples
	);
}

function countActiveWorkoutFilters(filters: FiltersState): number {
	let count = 0;

	if (filters.start || filters.end) {
		count += 1;
	}
	if (filters.activityTypes.length > 0) {
		count += 1;
	}
	if (filters.sourceQuery.trim()) {
		count += 1;
	}
	if (filters.location) {
		count += 1;
	}
	if (filters.efforts.length > 0) {
		count += 1;
	}
	if (filters.minDistanceMiles.trim() || filters.maxDistanceMiles.trim()) {
		count += 1;
	}
	if (filters.minDurationMinutes.trim() || filters.maxDurationMinutes.trim()) {
		count += 1;
	}
	if (filters.minEnergyKcal.trim() || filters.maxEnergyKcal.trim()) {
		count += 1;
	}
	if (filters.minAvgHeartRate.trim() || filters.maxAvgHeartRate.trim()) {
		count += 1;
	}
	if (filters.minMaxHeartRate.trim() || filters.maxMaxHeartRate.trim()) {
		count += 1;
	}
	if (filters.requiresRouteData) {
		count += 1;
	}
	if (filters.requiresHeartRateSamples) {
		count += 1;
	}

	return count;
}

function countActiveHealthFilters(filters: HealthFiltersState): number {
	let count = 0;

	if (filters.start || filters.end) {
		count += 1;
	}
	if (filters.categories.length > 0) {
		count += 1;
	}
	if (filters.metricQuery.trim()) {
		count += 1;
	}
	if (filters.sourceQuery.trim()) {
		count += 1;
	}
	if (filters.onlyWithSamples !== EMPTY_HEALTH_FILTERS.onlyWithSamples) {
		count += 1;
	}

	return count;
}

function collectDatasetInfoEntries(
	dashboard: DashboardPayload | null,
): Array<[string, string]> {
	if (!dashboard) {
		return [];
	}

	const merged = new Map<string, string>();
	for (const source of [
		dashboard.summary.dataset_info,
		dashboard.inspection.dataset_info,
		dashboard.health_overview.dataset_info,
	]) {
		for (const [key, value] of Object.entries(source)) {
			if (value) {
				merged.set(key, value);
			}
		}
	}

	return [...merged.entries()].sort(([left], [right]) =>
		left.localeCompare(right),
	);
}

function formatDataPageValue(value: string): string {
	const trimmed = value.trim();
	if (!trimmed) {
		return value;
	}

	if (/^-?\d+$/.test(trimmed)) {
		return formatInteger(Number(trimmed));
	}

	if (/^-?\d+\.\d+$/.test(trimmed)) {
		return formatDecimal(Number(trimmed), 3);
	}

	return value;
}

function formatMaybeNumber(
	value: number | null | undefined,
	suffix: string,
	measurementSystem: MeasurementSystem = "imperial",
): string {
	if (value === null || value === undefined) {
		return "—";
	}

	const converted = convertDisplayValue(value, suffix, measurementSystem);
	return `${formatDecimal(converted.value, 1)} ${converted.unit ?? suffix}`;
}

function formatWorkoutElevationGain(
	value: number | null | undefined,
	measurementSystem: MeasurementSystem = "imperial",
): string {
	if (value === null || value === undefined) {
		return "—";
	}

	if (measurementSystem === "metric") {
		return `${formatDecimal(value * 0.3048, 1)} m`;
	}

	return `${formatDecimal(value, 1)} ft`;
}

function formatWorkoutTemperature(
	value: number | null | undefined,
	measurementSystem: MeasurementSystem = "imperial",
): string {
	if (value === null || value === undefined) {
		return "—";
	}

	if (measurementSystem === "metric") {
		return `${formatDecimal(((value - 32) * 5) / 9, 1)} degC`;
	}

	return `${formatDecimal(value, 1)} degF`;
}

function formatRange(start: string | null, end: string | null): string {
	if (!start || !end) {
		return "—";
	}
	return `${formatDate(start)} → ${formatDate(end)}`;
}

function formatDate(value: string): string {
	return new Date(value).toLocaleDateString();
}

function formatDateTime(value: string | null): string {
	if (!value) {
		return "—";
	}
	return new Date(value).toLocaleString();
}

function formatRelativeEpoch(epochSeconds: number): string {
	return new Date(epochSeconds * 1000).toLocaleString();
}

function buildExportDateRangeLabel(
	workouts: SummaryWorkoutCard[],
	filterStart?: string,
	filterEnd?: string,
): string {
	const workoutDates = workouts
		.map(
			(workout) =>
				normalizeExportDateValue(workout.start) ??
				normalizeExportDateValue(workout.date),
		)
		.filter((value): value is string => value !== null);
	if (workoutDates.length > 0) {
		const sorted = [...workoutDates].sort((left, right) =>
			left.localeCompare(right),
		);
		return `${sorted[0]}_to_${sorted[sorted.length - 1]}`;
	}

	const startLabel = normalizeExportDateValue(filterStart);
	const endLabel = normalizeExportDateValue(filterEnd);
	if (startLabel && endLabel) {
		return `${startLabel}_to_${endLabel}`;
	}
	if (startLabel) {
		return `from_${startLabel}`;
	}
	if (endLabel) {
		return `through_${endLabel}`;
	}
	return "all_dates";
}

function normalizeExportDateValue(
	value: string | null | undefined,
): string | null {
	if (!value) {
		return null;
	}
	const candidate = value.trim();
	if (candidate.length >= 10 && candidate[4] === "-" && candidate[7] === "-") {
		return candidate.slice(0, 10);
	}
	return null;
}

function formatByteSize(value: number | null | undefined): string {
	if (value === null || value === undefined || value <= 0) {
		return "—";
	}

	const units = ["B", "KB", "MB", "GB"];
	let size = value;
	let unitIndex = 0;
	while (size >= 1024 && unitIndex < units.length - 1) {
		size /= 1024;
		unitIndex += 1;
	}

	return `${formatDecimal(size, 1)} ${units[unitIndex]}`;
}

function formatDurationSeconds(value: number | null | undefined): string {
	if (value === null || value === undefined) {
		return "—";
	}

	if (value < 1) {
		return `${formatDecimal(value, 1)}s`;
	}

	const rounded = Math.round(value);
	if (rounded < 60) {
		return `${formatInteger(rounded)}s`;
	}

	const minutes = Math.floor(rounded / 60);
	const seconds = rounded % 60;
	if (minutes < 60) {
		return `${formatInteger(minutes)}m ${formatInteger(seconds)}s`;
	}

	const hours = Math.floor(minutes / 60);
	const remainderMinutes = minutes % 60;
	return `${formatInteger(hours)}h ${formatInteger(remainderMinutes)}m`;
}

function formatElapsedMinutes(value: number): string {
	const totalSeconds = Math.max(0, Math.round(value * 60));
	if (totalSeconds < 60) {
		return `${formatInteger(totalSeconds)}s`;
	}

	const totalMinutes = Math.floor(totalSeconds / 60);
	if (totalMinutes < 60) {
		return `${formatInteger(totalMinutes)}m`;
	}

	const hours = Math.floor(totalMinutes / 60);
	const remainderMinutes = totalMinutes % 60;
	return remainderMinutes === 0
		? `${formatInteger(hours)}h`
		: `${formatInteger(hours)}h ${formatInteger(remainderMinutes)}m`;
}

function formatThemeMode(value: ThemeMode | "light" | "dark"): string {
	return value.charAt(0).toUpperCase() + value.slice(1);
}

function formatHealthMetricValue(
	metric: HealthOverviewMetric,
	value: number | null | undefined,
	measurementSystem: MeasurementSystem = "imperial",
): string {
	if (value === null || value === undefined) {
		return "—";
	}

	const converted = convertDisplayValue(value, metric.unit, measurementSystem);
	const maximumFractionDigits = Number.isInteger(converted.value) ? 0 : 1;
	const formatted = formatDecimal(converted.value, maximumFractionDigits);
	return converted.unit ? `${formatted} ${converted.unit}` : formatted;
}

function formatHealthMetricRange(
	metric: HealthOverviewMetric,
	measurementSystem: MeasurementSystem = "imperial",
): string {
	if (metric.minimum_value === null || metric.maximum_value === null) {
		return "—";
	}

	return `${formatHealthMetricValue(metric, metric.minimum_value, measurementSystem)} - ${formatHealthMetricValue(metric, metric.maximum_value, measurementSystem)}`;
}

function formatHealthMetricSummaryPair(
	date: string | null,
	value: number | null | undefined,
	metric: HealthOverviewMetric,
	measurementSystem: MeasurementSystem = "imperial",
): string {
	if (!date || value === null || value === undefined) {
		return "—";
	}
	return `${formatDate(date)} • ${formatHealthMetricValue(metric, value, measurementSystem)}`;
}

function describeHealthTrend(metric: HealthOverviewMetric): string {
	return metric.summary_kind === "total" ? "Daily totals" : "Daily averages";
}

function formatWorkoutMetricValue(
	metric: WorkoutMetricSeries,
	value: number | null | undefined,
	measurementSystem: MeasurementSystem = "imperial",
): string {
	if (value === null || value === undefined) {
		return "—";
	}

	const converted = convertDisplayValue(value, metric.unit, measurementSystem);
	const maximumFractionDigits = Number.isInteger(converted.value) ? 0 : 1;
	const formatted = formatDecimal(converted.value, maximumFractionDigits);
	return converted.unit ? `${formatted} ${converted.unit}` : formatted;
}

function formatWorkoutMetricRange(
	metric: WorkoutMetricSeries,
	measurementSystem: MeasurementSystem = "imperial",
): string {
	return `${formatWorkoutMetricValue(metric, metric.minimum, measurementSystem)} - ${formatWorkoutMetricValue(metric, metric.maximum, measurementSystem)}`;
}

function formatWorkoutMetricTimeline(metric: WorkoutMetricSeries): string {
	const firstPoint = metric.points[0] ?? null;
	const lastPoint = metric.points[metric.points.length - 1] ?? null;
	if (!firstPoint || !lastPoint) {
		return "—";
	}

	return `${formatElapsedMinutes(firstPoint.elapsedMinutes)} - ${formatElapsedMinutes(lastPoint.elapsedMinutes)}`;
}

function humanizeIdentifier(value: string): string {
	return value
		.replace(
			/^(HKWorkoutActivityType|HKQuantityTypeIdentifier|HKCategoryTypeIdentifier|HKWorkoutEventType|HKSeriesType|HKCorrelationTypeIdentifier)/,
			"",
		)
		.replace(/_/g, " ")
		.replace(/([a-z0-9])([A-Z])/g, "$1 $2")
		.trim();
}

function isPresentNumber(value: number | null | undefined): value is number {
	return typeof value === "number" && Number.isFinite(value);
}

function formatInlineMetricValue(value: string | number): string {
	return typeof value === "number"
		? formatDecimal(value, Number.isInteger(value) ? 0 : 1)
		: value;
}

function formatInteger(value: number): string {
	return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(
		value,
	);
}

function formatDecimal(value: number, maximumFractionDigits: number): string {
	return new Intl.NumberFormat(undefined, { maximumFractionDigits }).format(
		value,
	);
}

function formatDisplayUnit(
	unit: string | null | undefined,
	measurementSystem: MeasurementSystem = "imperial",
): string | null {
	if (!unit) {
		return null;
	}

	return convertDisplayValue(1, unit, measurementSystem).unit;
}

function convertDisplayValue(
	value: number,
	unit: string | null | undefined,
	measurementSystem: MeasurementSystem,
): { value: number; unit: string | null } {
	const normalizedUnit = normalizeDisplayUnit(unit);
	if (!normalizedUnit) {
		return { value, unit: unit ?? null };
	}

	if (measurementSystem === "metric") {
		switch (normalizedUnit) {
			case "mi":
				return { value: value * 1.609344, unit: "km" };
			case "mph":
				return { value: value * 1.609344, unit: "kph" };
			case "min/mi":
				return { value: value / 1.609344, unit: "min/km" };
			default:
				return { value, unit: normalizedUnit };
		}
	}

	switch (normalizedUnit) {
		case "km":
			return { value: value / 1.609344, unit: "mi" };
		case "kph":
			return { value: value / 1.609344, unit: "mph" };
		case "min/km":
			return { value: value * 1.609344, unit: "min/mi" };
		default:
			return { value, unit: normalizedUnit };
	}
}

function normalizeDisplayUnit(unit: string | null | undefined): string | null {
	if (!unit) {
		return null;
	}

	const normalized = unit.trim().toLowerCase();
	switch (normalized) {
		case "mile":
		case "miles":
			return "mi";
		case "km/h":
			return "kph";
		case "mi/h":
			return "mph";
		case "pace min/mi":
			return "min/mi";
		case "pace min/km":
			return "min/km";
		default:
			return normalized;
	}
}

function stringifyError(error: unknown): string {
	if (error instanceof Error) {
		return error.message;
	}
	return String(error);
}

export default App;
