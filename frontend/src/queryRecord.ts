import Immutable from "immutable";
import { Location } from "react-router-dom";

import { ColumnInfo, ColumnRecord, ColumnType } from "@/columnRecord";
import { Filter, FilterRecord } from "@/filterRecord";
import { AggOp, AggOpType, QueryType, QueryUrlPath } from "@/queryConsts";
import { L1D, NOW } from "./tsConsts";

export default class QueryRecord extends Immutable.Record<
  DeepRecord<{
    query_type: QueryType;
    min_ts: number;
    max_ts: number;
    filters: Filter[];
    groupby_int_columns: ColumnInfo[];
    groupby_str_columns: ColumnInfo[];
    agg_op: AggOpType;
    agg_int_column_names: ColumnInfo[];
  }>
>({
  query_type: null,
  min_ts: null,
  max_ts: null,
  filters: null,
  groupby_int_columns: null,
  groupby_str_columns: null,
  agg_op: null,
  agg_int_column_names: null,
}) {
  toUrl(): string {
    if (this.query_type === QueryType.Rows) {
      return `${QueryUrlPath.Rows}?q=${JSON.stringify(this.toJS())}`;
    } else if (this.query_type === QueryType.Table) {
      return `${QueryUrlPath.Table}?q=${JSON.stringify(this.toJS())}`;
    }
    return "";
  }
}

export const DEFAULT_RECORD = new QueryRecord({
  query_type: QueryType.Table,
  agg_op: AggOp.COUNT,
  min_ts: L1D.unix(),
  max_ts: NOW.unix(),
});

export function buildRecordFromUrl(): QueryRecord {
  const split = window.location.hash.split("?q=");
  if (split.length !== 2) {
    return DEFAULT_RECORD;
  }

  const [query, search] = split;
  const path = query.split("#")[1];

  switch (path) {
    case QueryUrlPath.Rows: {
      try {
        return new QueryRecord({
          query_type: QueryType.Rows,
          ...JSON.parse(decodeURI(search)),
        });
      } catch {
        return DEFAULT_RECORD;
      }
    }
    case QueryUrlPath.Table: {
      try {
        return new QueryRecord({
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
