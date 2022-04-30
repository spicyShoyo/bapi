import Immutable from "immutable";
import { useSelector } from "react-redux";
import BapiQueryRecord from "@/bapiQueryRecord";
import { Filter } from "@/filterRecord";
import { ColumnInfo } from "./columnRecord";
import { getPropsForTimeRange, TimeRange } from "./tsConsts";

export default function useQuerySelector<T>(
  selectFn: (record: BapiQueryRecord) => T,
) {
  return useSelector(selectFn, Immutable.is);
}

function toJsOrEmptyArray<T extends {}>(
  values: Immutable.List<DeepRecord<T>> | null | undefined,
): T[] {
  // @ts-ignore v.toJS() is not type covered
  return values?.map((v) => v.toJS()).toJS() ?? [];
}

export function useQueryFilters(): Filter[] {
  return useQuerySelector((r) => toJsOrEmptyArray<Filter>(r.filters));
}

export function useQueryGroupbyCols(): ColumnInfo[] {
  return useQuerySelector((r) => toJsOrEmptyArray<ColumnInfo>(r.groupby_cols));
}

export function useQueryAggCols(): ColumnInfo[] {
  return useQuerySelector((r) => toJsOrEmptyArray<ColumnInfo>(r.agg_cols));
}

export function useQueryTs(): {
  ts_range: TimeRange | null;
  min_ts: number | null | undefined;
  max_ts: number | null | undefined;
} {
  return useQuerySelector((r) => {
    if (r.ts_range != null) {
      const [_, min_ts, max_ts] = getPropsForTimeRange(r.ts_range);
      return {
        ts_range: r.ts_range,
        min_ts,
        max_ts,
      };
    }

    return {
      ts_range: null,
      min_ts: r.min_ts,
      max_ts: r.max_ts,
    };
  });
}
