import BapiQueryRecord from "@/bapiQueryRecord";
import { AggOp, QueryType, QueryUrlPath } from "@/queryConsts";
import { L1D, NOW } from "@/tsConsts";

export const DEFAULT_RECORD = new BapiQueryRecord({
  query_type: QueryType.Table,
  agg_op: AggOp.COUNT,
  min_ts: L1D.unix(),
  max_ts: NOW.unix(),
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
