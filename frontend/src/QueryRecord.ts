import { Record } from "immutable";
import { Location } from "react-router-dom";

import { QueryType, Filter, QueryUrlPath } from "@/queryConsts";

export default class QueryRecord extends Record<{
  query_type: QueryType | null;
  min_ts: number | null;
  max_ts: number | null;
  int_filters: Filter[] | null;
  str_filters: Filter[] | null;
  int_column_names: string[] | null;
  str_column_names: string[] | null;
}>({
  query_type: null,
  min_ts: null,
  max_ts: null,
  int_filters: null,
  str_filters: null,
  int_column_names: null,
  str_column_names: null,
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
}
