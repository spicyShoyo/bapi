import BapiQueryRecord from "@/bapiQueryRecord";
import { AggOp, FilterOp, QueryType, QueryUrlPath } from "@/queryConsts";
import { TimeRange, getPropsForTimeRange } from "@/tsConsts";
import { ColumnType } from "./columnRecord";

const [_, min_ts, max_ts] = getPropsForTimeRange(TimeRange.l1d);
export const DEFAULT_RECORD = new BapiQueryRecord({
  query_type: QueryType.Table,
  agg_op: AggOp.COUNT,
  // @ts-ignore TODO: fix typing
  filters: [
    {
      column_name: "",
      column_type: ColumnType.STR,
      filter_op: FilterOp.EQ,
      int_vals: [],
      str_vals: [],
    },
  ],
  ts_range: TimeRange.l1d,
  min_ts,
  max_ts,
});

export function recordToUrl(record: BapiQueryRecord): string {
  switch (record.query_type) {
    case QueryType.Rows:
      return `${QueryUrlPath.Rows}?q=${JSON.stringify(record.toJS())}`;
    case QueryType.Table:
      return `${QueryUrlPath.Table}?q=${JSON.stringify(record.toJS())}`;
    default:
      return "";
  }
}

// TODO: add typing
export function recordToTableQuery(record: BapiQueryRecord): object {
  return {
    min_ts: record.min_ts,
    max_ts: record.max_ts,
    int_filters: record.filters
      ?.filter((filter) => filter.column_type === ColumnType.INT)
      .map((filter) => ({
        column_name: filter.column_name,
        filter_op: filter.filter_op,
        value: filter.int_vals?.get(0),
      }))
      .toJS(),
    str_filters: record.filters
      ?.filter((filter) => filter.column_type === ColumnType.STR)
      .map((filter) => ({
        column_name: filter.column_name,
        filter_op: filter.filter_op,
        value: filter.str_vals?.get(0),
      }))
      .toJS(),
    group_by_int_column_names: record.groupby_cols
      ?.filter((col) => col.column_type === ColumnType.INT)
      .map((col) => col.column_name)
      .toJS(),
    group_by_str_column_names: record.groupby_cols
      ?.filter((col) => col.column_type === ColumnType.STR)
      .map((col) => col.column_name)
      .toJS(),
    agg_op: record.agg_op,
    agg_int_column_names: record.agg_cols?.map((col) => col.column_name).toJS(),
  };
}

export function materializeQuery(record: BapiQueryRecord): BapiQueryRecord {
  let updatedReocrd = record;
  if (record.ts_range != null) {
    const [_, min_ts, max_ts] = getPropsForTimeRange(record.ts_range);
    updatedReocrd = updatedReocrd.set("min_ts", min_ts).set("max_ts", max_ts);
  }

  return updatedReocrd.set(
    "filters",
    record.filters?.filter(
      (filter) =>
        filter.column_name !== "" &&
        ((filter.int_vals?.size ?? 0) > 0 || (filter.str_vals?.size ?? 0) > 0),
    ),
  );
}

export function getRecordFromUrlOrDefault(): BapiQueryRecord {
  const split = window.location.hash.split("?q=");
  if (split.length !== 2) {
    return DEFAULT_RECORD;
  }

  const [query, search] = split;
  const path = query.split("#")[1];

  switch (path) {
    case QueryUrlPath.Rows: {
      try {
        return new BapiQueryRecord({
          query_type: QueryType.Rows,
          ...JSON.parse(decodeURI(search)),
        });
      } catch {
        return DEFAULT_RECORD;
      }
    }
    case QueryUrlPath.Table: {
      try {
        return new BapiQueryRecord({
          query_type: QueryType.Table,
          ...JSON.parse(decodeURI(search)),
        });
      } catch {
        return DEFAULT_RECORD;
      }
    }
    default:
      return DEFAULT_RECORD;
  }
}
