import {
	CheckIcon,
	ChevronsUpDownIcon,
	PlusIcon,
	SearchIcon,
	XIcon,
} from "lucide-react";
import { useMemo, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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

interface ActivityOption {
	activity_type: string;
	label: string;
	workout_count: number;
}

interface ActivityTypePickerProps {
	options: ActivityOption[];
	value: string[];
	onChange: (value: string[]) => void;
	disabled?: boolean;
}

interface SelectedActivity {
	key: string;
	label: string;
	value: string;
	workoutCount?: number;
}

const SUGGESTED_ACTIVITY_TYPES = [
	"HKWorkoutActivityTypeAmericanFootball",
	"HKWorkoutActivityTypeArchery",
	"HKWorkoutActivityTypeAustralianFootball",
	"HKWorkoutActivityTypeBadminton",
	"HKWorkoutActivityTypeBarre",
	"HKWorkoutActivityTypeBaseball",
	"HKWorkoutActivityTypeBasketball",
	"HKWorkoutActivityTypeBowling",
	"HKWorkoutActivityTypeBoxing",
	"HKWorkoutActivityTypeClimbing",
	"HKWorkoutActivityTypeCoreTraining",
	"HKWorkoutActivityTypeCricket",
	"HKWorkoutActivityTypeCrossCountrySkiing",
	"HKWorkoutActivityTypeCrossTraining",
	"HKWorkoutActivityTypeCurling",
	"HKWorkoutActivityTypeCycling",
	"HKWorkoutActivityTypeDance",
	"HKWorkoutActivityTypeDanceInspiredTraining",
	"HKWorkoutActivityTypeDiscSports",
	"HKWorkoutActivityTypeDownhillSkiing",
	"HKWorkoutActivityTypeElliptical",
	"HKWorkoutActivityTypeEquestrianSports",
	"HKWorkoutActivityTypeFencing",
	"HKWorkoutActivityTypeFishing",
	"HKWorkoutActivityTypeFitnessGaming",
	"HKWorkoutActivityTypeFunctionalStrengthTraining",
	"HKWorkoutActivityTypeGolf",
	"HKWorkoutActivityTypeGymnastics",
	"HKWorkoutActivityTypeHandball",
	"HKWorkoutActivityTypeHighIntensityIntervalTraining",
	"HKWorkoutActivityTypeHiking",
	"HKWorkoutActivityTypeHockey",
	"HKWorkoutActivityTypeHunting",
	"HKWorkoutActivityTypeJumpRope",
	"HKWorkoutActivityTypeKickboxing",
	"HKWorkoutActivityTypeLacrosse",
	"HKWorkoutActivityTypeMartialArts",
	"HKWorkoutActivityTypeMindAndBody",
	"HKWorkoutActivityTypeMixedCardio",
	"HKWorkoutActivityTypeMixedMetabolicCardioTraining",
	"HKWorkoutActivityTypeOther",
	"HKWorkoutActivityTypePaddleSports",
	"HKWorkoutActivityTypePickleball",
	"HKWorkoutActivityTypePilates",
	"HKWorkoutActivityTypePlay",
	"HKWorkoutActivityTypePreparationAndRecovery",
	"HKWorkoutActivityTypeRacquetball",
	"HKWorkoutActivityTypeRowing",
	"HKWorkoutActivityTypeRugby",
	"HKWorkoutActivityTypeRunning",
	"HKWorkoutActivityTypeSailing",
	"HKWorkoutActivityTypeSkatingSports",
	"HKWorkoutActivityTypeSnowboarding",
	"HKWorkoutActivityTypeSnowSports",
	"HKWorkoutActivityTypeSoccer",
	"HKWorkoutActivityTypeSoftball",
	"HKWorkoutActivityTypeSquash",
	"HKWorkoutActivityTypeStairClimbing",
	"HKWorkoutActivityTypeStepTraining",
	"HKWorkoutActivityTypeSurfingSports",
	"HKWorkoutActivityTypeSwimming",
	"HKWorkoutActivityTypeTableTennis",
	"HKWorkoutActivityTypeTaiChi",
	"HKWorkoutActivityTypeTennis",
	"HKWorkoutActivityTypeTrackAndField",
	"HKWorkoutActivityTypeTraditionalStrengthTraining",
	"HKWorkoutActivityTypeVolleyball",
	"HKWorkoutActivityTypeWalking",
	"HKWorkoutActivityTypeWaterFitness",
	"HKWorkoutActivityTypeWaterPolo",
	"HKWorkoutActivityTypeWaterSports",
	"HKWorkoutActivityTypeWheelchairRunPace",
	"HKWorkoutActivityTypeWheelchairWalkPace",
	"HKWorkoutActivityTypeWrestling",
	"HKWorkoutActivityTypeYoga",
];

function ActivityTypePicker({
	options,
	value,
	onChange,
	disabled = false,
}: ActivityTypePickerProps) {
	const [customValue, setCustomValue] = useState("");
	const [searchValue, setSearchValue] = useState("");

	const pickerOptions = useMemo<ActivityOption[]>(() => {
		const merged = new Map<string, ActivityOption>();

		for (const activityType of SUGGESTED_ACTIVITY_TYPES) {
			merged.set(activityType, {
				activity_type: activityType,
				label: humanizeActivityType(activityType),
				workout_count: 0,
			});
		}

		for (const option of options) {
			merged.set(normalizeActivityType(option.activity_type), option);
		}

		return [...merged.values()].sort((left, right) => {
			if (left.workout_count !== right.workout_count) {
				return right.workout_count - left.workout_count;
			}
			return left.label.localeCompare(right.label);
		});
	}, [options]);

	const visibleOptions = useMemo(() => {
		const query = searchValue.trim().toLowerCase();
		if (!query) {
			return pickerOptions;
		}

		return pickerOptions.filter((option) => {
			const haystacks = [option.label, option.activity_type];
			return haystacks.some((value) => value.toLowerCase().includes(query));
		});
	}, [pickerOptions, searchValue]);

	const selectedActivities = useMemo<SelectedActivity[]>(() => {
		const optionsByKey = new Map(
			pickerOptions.map((option) => [
				normalizeActivityType(option.activity_type),
				option,
			]),
		);

		const selected: SelectedActivity[] = [];
		for (const entry of value) {
			const key = normalizeActivityType(entry);
			if (!key) {
				continue;
			}

			const option = optionsByKey.get(key);
			selected.push({
				key,
				label: option?.label ?? humanizeActivityType(entry),
				value: option?.activity_type ?? entry,
				workoutCount: option?.workout_count,
			});
		}

		return selected;
	}, [pickerOptions, value]);

	const summary = useMemo(() => {
		if (selectedActivities.length === 0) {
			return "All workout types";
		}
		if (selectedActivities.length === 1) {
			return selectedActivities[0].label;
		}
		return `${formatInteger(selectedActivities.length)} workout types selected`;
	}, [selectedActivities]);

	function toggleActivityType(activityType: string) {
		const nextKey = normalizeActivityType(activityType);
		if (!nextKey) {
			return;
		}

		const withoutCurrent = value.filter(
			(entry) => normalizeActivityType(entry) !== nextKey,
		);
		const isSelected = withoutCurrent.length !== value.length;
		onChange(isSelected ? withoutCurrent : [...withoutCurrent, activityType]);
	}

	function addCustomActivityType() {
		const candidate = customValue.trim();
		const normalizedCandidate = normalizeActivityType(candidate);
		if (!normalizedCandidate) {
			return;
		}

		if (
			value.some(
				(entry) => normalizeActivityType(entry) === normalizedCandidate,
			)
		) {
			setCustomValue("");
			return;
		}

		onChange([...value, candidate]);
		setCustomValue("");
	}

	return (
		<div className="flex flex-col gap-3">
			<div className="flex flex-col gap-2">
				<span className="text-sm font-medium text-foreground">
					Workout types
				</span>
				<Popover>
					<PopoverTrigger asChild>
						<Button
							variant="outline"
							disabled={disabled}
							className="w-full justify-between rounded-xl border-border/70 bg-background/70 px-3 py-5 text-left font-normal"
						>
							<span className="truncate">{summary}</span>
							<ChevronsUpDownIcon className="size-4 text-muted-foreground" />
						</Button>
					</PopoverTrigger>
					<PopoverContent align="start" className="w-[380px]">
						<PopoverHeader>
							<PopoverTitle>Filter by workout type</PopoverTitle>
							<PopoverDescription>
								Search across a much larger workout catalog, keep dataset
								matches at the top, or add a custom Apple Health type directly.
							</PopoverDescription>
						</PopoverHeader>

						<div className="relative">
							<SearchIcon className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground" />
							<Input
								value={searchValue}
								onChange={(event) => setSearchValue(event.target.value)}
								placeholder="Search running, swimming, pilates..."
								className="pl-9"
							/>
						</div>

						<div className="max-h-80 space-y-2 overflow-y-auto pr-1">
							{visibleOptions.length === 0 ? (
								<p className="text-sm text-muted-foreground">
									No workout types match that search.
								</p>
							) : (
								visibleOptions.map((option) => {
									const checked = value.some(
										(entry) =>
											normalizeActivityType(entry) ===
											normalizeActivityType(option.activity_type),
									);
									const optionId = `activity-type-${normalizeActivityType(option.activity_type)}`;

									return (
										<label
											key={option.activity_type}
											htmlFor={optionId}
											className="flex cursor-pointer items-start gap-3 rounded-lg border border-transparent px-2 py-2 transition-colors hover:border-border hover:bg-muted/40"
										>
											<Checkbox
												id={optionId}
												checked={checked}
												onCheckedChange={() =>
													toggleActivityType(option.activity_type)
												}
												className="mt-0.5"
											/>
											<div className="min-w-0 flex-1">
												<div className="flex items-center justify-between gap-3">
													<span className="font-medium text-foreground">
														{option.label}
													</span>
													<span className="text-xs text-muted-foreground">
														{option.workout_count > 0
															? formatInteger(option.workout_count)
															: "Suggested"}
													</span>
												</div>
												<p className="truncate text-xs text-muted-foreground">
													{option.activity_type}
												</p>
											</div>
										</label>
									);
								})
							)}
						</div>

						<div className="border-t pt-2">
							<p className="mb-2 text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
								Add custom type
							</p>
							<div className="flex gap-2">
								<Input
									value={customValue}
									onChange={(event) => setCustomValue(event.target.value)}
									placeholder="Running, rowing, or HKWorkoutActivityType..."
									onKeyDown={(event) => {
										if (event.key === "Enter") {
											event.preventDefault();
											addCustomActivityType();
										}
									}}
								/>
								<Button
									type="button"
									variant="outline"
									onClick={addCustomActivityType}
									disabled={!normalizeActivityType(customValue)}
								>
									<PlusIcon className="size-4" />
									Add
								</Button>
							</div>
						</div>
					</PopoverContent>
				</Popover>
			</div>

			{selectedActivities.length > 0 ? (
				<div className="flex flex-wrap gap-2">
					{selectedActivities.map((activity) => (
						<Badge
							key={activity.key}
							variant="secondary"
							className="gap-1 rounded-full px-2.5 py-1 text-xs"
						>
							<CheckIcon className="size-3" />
							{activity.label}
							{typeof activity.workoutCount === "number" &&
							activity.workoutCount > 0 ? (
								<span className="text-[11px] text-muted-foreground">
									({formatInteger(activity.workoutCount)})
								</span>
							) : null}
							<button
								type="button"
								onClick={() => toggleActivityType(activity.value)}
								className="ml-1 rounded-full p-0.5 text-muted-foreground transition-colors hover:bg-foreground/10 hover:text-foreground"
								aria-label={`Remove ${activity.label}`}
							>
								<XIcon className="size-3" />
							</button>
						</Badge>
					))}
				</div>
			) : null}
		</div>
	);
}

function normalizeActivityType(value: string): string {
	const trimmed = value.trim();
	if (!trimmed) {
		return "";
	}
	if (trimmed.startsWith("HKWorkoutActivityType")) {
		return trimmed;
	}

	const words = trimmed.split(/[^A-Za-z0-9]+/).filter(Boolean);
	if (words.length === 0) {
		return "";
	}

	return `HKWorkoutActivityType${words
		.map((word) => `${word[0].toUpperCase()}${word.slice(1)}`)
		.join("")}`;
}

function humanizeActivityType(value: string): string {
	const normalized = normalizeActivityType(value);
	if (!normalized) {
		return value.trim();
	}

	return normalized
		.replace(/^HKWorkoutActivityType/, "")
		.replace(/([a-z0-9])([A-Z])/g, "$1 $2");
}

function formatInteger(value: number): string {
	return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(
		value,
	);
}

export { ActivityTypePicker };
