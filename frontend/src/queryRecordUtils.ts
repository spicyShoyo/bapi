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
