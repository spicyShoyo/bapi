import Immutable from "immutable";
import { Location } from "react-router-dom";

import { ColumnInfo, ColumnRecord, ColumnType } from "@/columnRecord";
import { Filter, FilterRecord } from "@/filterRecord";
import { QueryType, QueryUrlPath } from "@/queryConsts";

export default class QueryRecord extends Immutable.Record<{
  query_type: QueryType | null;
  min_ts: number | null;
  max_ts: number | null;
  int_filters: Immutable.List<FilterRecord> | null;
  str_filters: Immutable.List<FilterRecord> | null;
  groupby_int_columns: Immutable.List<ColumnRecord> | null;
  groupby_str_columns: Immutable.List<ColumnRecord> | null;
}>({
  query_type: null,
  min_ts: null,
  max_ts: null,
  int_filters: null,
  str_filters: null,
  groupby_int_columns: null,
  groupby_str_columns: null,
}) {
  static fromUrl(location: Location): QueryRecord {
    if (location.pathname === QueryUrlPath.ROWS) {
      try {
        return new QueryRecord({
          query_type: QueryType.Rows,
          ...JSON.parse(decodeURI(location.search.split("?q=")[1])),
        });
      } catch {
        return new QueryRecord({
          query_type: QueryType.Rows,
        });
      }
    }
    return new QueryRecord();
  }

  toUrl(): string {
    if (this.query_type === QueryType.Rows) {
      return `${QueryUrlPath.ROWS}?q=${JSON.stringify(this.toJS())}`;
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

  setGroupbyCols(cols: ColumnInfo[]): this {
    let intCols = Immutable.List();
    let strCols = Immutable.List();
    cols.forEach((col) => {
      switch (col.column_type) {
        case ColumnType.INT:
          intCols = intCols.push(ColumnRecord.fromColumnInfo(col));
          break;
        case ColumnType.STR:
          strCols = strCols.push(ColumnRecord.fromColumnInfo(col));
          break;
        default:
          throw new Error(`invalid groupby column type ${col.column_type}`);
      }
    });
    return this.setIn(["groupby_int_columns"], intCols).setIn(
      ["groupby_str_columns"],
      strCols,
    );
  }
}
