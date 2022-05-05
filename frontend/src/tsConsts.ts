import dayjs from "dayjs";

export enum TimeRange {
  l14d = "l14d",
  l7d = "l7d",
  l1d = "l1d",
  l12h = "l12h",
  l1h = "l1h",
  l5m = "l5m",
  yesterday = "yesterday",
  last_week = "last_week",
}

export const UTC_FORMAT_STR = "YYYY-MM-DDTHH:mm";

export const TimeRanges: TimeRange[] = [
  TimeRange.l14d,
  TimeRange.l7d,
  TimeRange.l1d,
  TimeRange.l12h,
  TimeRange.l1h,
  TimeRange.l5m,
  TimeRange.yesterday,
  TimeRange.last_week,
];

export function getPropsForTimeRange(
  range: TimeRange,
): [string, number, number] {
  const NOW = dayjs();
  const L5M = NOW.subtract(5, "minute");
  const L1H = NOW.subtract(1, "hour");
  const L12H = NOW.subtract(12, "hour");
  const L1D = NOW.subtract(1, "day");
  const L2D = NOW.subtract(2, "day");
  const L7D = NOW.subtract(7, "day");
  const L14D = NOW.subtract(14, "day");

  switch (range) {
    case TimeRange.l14d:
      return ["L14d", L14D.unix(), NOW.unix()];
    case TimeRange.l7d:
      return ["L7d", L7D.unix(), NOW.unix()];
    case TimeRange.l1d:
      return ["L1d", L1D.unix(), NOW.unix()];
    case TimeRange.l12h:
      return ["L12h", L12H.unix(), NOW.unix()];
    case TimeRange.l1h:
      return ["L1h", L1H.unix(), NOW.unix()];
    case TimeRange.l5m:
      return ["L5m", L5M.unix(), NOW.unix()];
    case TimeRange.yesterday:
      return ["Yesterday", L2D.unix(), L1D.unix()];
    case TimeRange.last_week:
      return ["Last week", L14D.unix(), L7D.unix()];
    default:
      const _: never = range;
      return ["L1d", L1D.unix(), NOW.unix()];
  }
}
