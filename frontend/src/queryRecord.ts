import Immutable from "immutable";
import { Location } from "react-router-dom";

import { ColumnInfo, ColumnRecord, ColumnType } from "@/columnRecord";
import { Filter, FilterRecord } from "@/filterRecord";
import { AggOpType, QueryType, QueryUrlPath } from "@/queryConsts";

export default class QueryRecord extends Immutable.Record<
  DeepRecord<{
    query_type: QueryType;
    min_ts: number;
    max_ts: number;
    int_filters: Filter[];
    str_filters: Filter[];
    groupby_int_columns: ColumnInfo[];
    groupby_str_columns: ColumnInfo[];
    agg_op: AggOpType;
    agg_int_column_names: ColumnInfo[];
  }>
>({
  query_type: null,
  min_ts: null,
  max_ts: null,
  int_filters: null,
  str_filters: null,
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

  addStrFilter(filter: Filter): this {
    const newStrFilters = this.str_filters ?? Immutable.List();
    return this.set(
      "str_filters",
      newStrFilters.push(FilterRecord.fromFilter(filter)),
    );
  }

  removeStrFilter(index: number): this {
    return this.removeIn(["str_filters", index]);
  }

  updateStrFilter(filter: Filter, index: number): this {
    return this.setIn(["str_filters", index], FilterRecord.fromFilter(filter));
  }

  addIntFilter(filter: Filter): this {
    const newIntFilters = this.int_filters ?? Immutable.List();
    return this.set(
      "int_filters",
      newIntFilters.push(FilterRecord.fromFilter(filter)),
    );
  }

  removeIntFilter(index: number): this {
    return this.removeIn(["int_filters", index]);
  }

  updateIntFilter(filter: Filter, index: number): this {
    return this.setIn(["int_filters", index], FilterRecord.fromFilter(filter));
  }
}

export function buildRecordFromUrl(): QueryRecord | undefined {
  const split = window.location.hash.split("?q=");
  if (split.length !== 2) {
    // Need to be undefined instead of null for Redux to not set the state to null
    return undefined;
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
        return undefined;
      }
    }
    case QueryUrlPath.Table: {
      try {
        return new QueryRecord({
          query_type: QueryType.Table,
          ...JSON.parse(decodeURI(search)),
        });
      } catch {
        return undefined;
      }
    }
    default:
      return undefined;
  }
}
