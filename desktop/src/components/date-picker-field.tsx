import { format, parseISO } from "date-fns";
import { CalendarIcon } from "lucide-react";
import type { DateRange } from "react-day-picker";

import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import {
	Popover,
	PopoverContent,
	PopoverDescription,
	PopoverHeader,
	PopoverTitle,
	PopoverTrigger,
} from "@/components/ui/popover";

interface DatePickerFieldProps {
	label: string;
	start: string;
	end: string;
	onChange: (value: { start: string; end: string }) => void;
	description: string;
}

function DatePickerField({
	label,
	start,
	end,
	onChange,
	description,
}: DatePickerFieldProps) {
	const selectedRange = buildSelectedRange(start, end);

	return (
		<div className="flex flex-col gap-2">
			<span className="text-sm font-medium text-foreground">{label}</span>
			<Popover>
				<PopoverTrigger asChild>
					<Button
						variant="outline"
						className="w-full justify-start gap-2 rounded-xl border-border/70 bg-background/70 px-3 py-5 text-left font-normal"
					>
						<CalendarIcon className="size-4 text-muted-foreground" />
						<span className="truncate">
							{formatDateWindow(start, end) ?? (
								<span className="text-muted-foreground">All dates</span>
							)}
						</span>
					</Button>
				</PopoverTrigger>
				<PopoverContent align="start" className="w-auto p-0">
					<PopoverHeader className="px-3 pt-3">
						<PopoverTitle>{label}</PopoverTitle>
						<PopoverDescription>{description}</PopoverDescription>
					</PopoverHeader>
					<Calendar
						mode="range"
						selected={selectedRange}
						onSelect={(range) => onChange(nextDateWindow(range))}
						numberOfMonths={2}
					/>
					{start || end ? (
						<div className="border-t px-2 py-2">
							<Button
								variant="ghost"
								className="w-full justify-center"
								onClick={() => onChange({ start: "", end: "" })}
							>
								Clear dates
							</Button>
						</div>
					) : null}
				</PopoverContent>
			</Popover>
		</div>
	);
}

function buildSelectedRange(start: string, end: string): DateRange | undefined {
	const from = start ? parseISO(start) : undefined;
	const to = end ? parseISO(end) : from;
	if (!from && !to) {
		return undefined;
	}
	return { from, to };
}

function nextDateWindow(range: DateRange | undefined): {
	start: string;
	end: string;
} {
	if (!range?.from) {
		return { start: "", end: "" };
	}

	const from = format(range.from, "yyyy-MM-dd");
	const to = format(range.to ?? range.from, "yyyy-MM-dd");
	return { start: from, end: to };
}

function formatDateWindow(start: string, end: string): string | null {
	if (!start && !end) {
		return null;
	}
	if (start && end && start === end) {
		return format(parseISO(start), "MMM d, yyyy");
	}
	if (start && end) {
		return `${format(parseISO(start), "MMM d, yyyy")} - ${format(parseISO(end), "MMM d, yyyy")}`;
	}
	if (start) {
		return `From ${format(parseISO(start), "MMM d, yyyy")}`;
	}
	return `Until ${format(parseISO(end), "MMM d, yyyy")}`;
}

export { DatePickerField };
