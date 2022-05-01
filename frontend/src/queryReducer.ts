import { createAction, PayloadAction } from "@reduxjs/toolkit";
import Immutable from "immutable";
import { ColumnInfo, ColumnRecord, ColumnType } from "@/columnRecord";
import { AggOpType, QueryType } from "@/queryConsts";
import { Filter, FilterRecord } from "@/filterRecord";
import { DEFAULT_RECORD, materializeQuery } from "@/queryRecordUtils";
import { TimeRange } from "./tsConsts";

type SetTsRangePayload = {
  maxTs?: number;
  minTs?: number;
  timeRange?: TimeRange;
};

export const setQueryType = createAction<QueryType>("setQueryType");
export const materialize = createAction<void>("materialize");

export const setTsRange = createAction<SetTsRangePayload>("setTsRange");

export const setGroupbyCols = createAction<ColumnInfo[]>("setGroupbyCols");
export const setAggregateCols = createAction<ColumnInfo[]>("setAggregateCols");
export const setAggOp = createAction<AggOpType>("setAggOp");

export const addFilter = createAction<Filter>("addFilter");
export const removeFilter = createAction<number>("removeFilter");
type UpdateFilterPayload = { idx: number; filter: Filter };
export const updateFilter = createAction<UpdateFilterPayload>("updateFilter");

export default function queryReducer(
  state = DEFAULT_RECORD,
  action:
    | PayloadAction<QueryType, "setQueryType">
    | PayloadAction<void, "materialize">
    | PayloadAction<SetTsRangePayload, "setTsRange">
    | PayloadAction<ColumnInfo[], "setGroupbyCols">
    | PayloadAction<ColumnInfo[], "setAggregateCols">
    | PayloadAction<AggOpType, "setAggOp">
    | PayloadAction<Filter, "addFilter">
    | PayloadAction<number, "removeFilter">
    | PayloadAction<UpdateFilterPayload, "updateFilter">,
) {
  switch (action.type) {
    case "setQueryType": {
      // TODO: convert query spec
      return state.set("query_type", action.payload);
    }
    case "materialize": {
      return materializeQuery(state);
    }
    case "setTsRange": {
      let newState = state;
      if (action.payload.timeRange != null) {
        newState = newState.set("ts_range", action.payload.timeRange);
      }
      if (action.payload.minTs != null) {
        newState = newState.set("min_ts", action.payload.minTs);
      }
      if (action.payload.maxTs != null) {
        newState = newState.set("max_ts", action.payload.maxTs);
      }
      return newState;
    }
    case "setGroupbyCols": {
      let cols = Immutable.List<ColumnRecord>();
      action.payload.forEach((col) => {
        cols = cols.push(new ColumnRecord(col));
      });
      return state.set("groupby_cols", cols);
    }
    case "setAggregateCols": {
      let cols = Immutable.List<ColumnRecord>();
      action.payload.forEach((col) => {
        cols = cols.push(new ColumnRecord(col));
      });
      return state.set("agg_cols", cols);
    }
    case "setAggOp": {
      return state.set("agg_op", action.payload);
    }
    case "addFilter": {
      const record = FilterRecord.fromFilter(action.payload);
      const filters = state.filters ?? Immutable.List();
      return state.set("filters", filters.push(record));
    }
    case "removeFilter": {
      return state.removeIn(["filters", action.payload]);
    }
    case "updateFilter": {
      const record = FilterRecord.fromFilter(action.payload.filter);
      return state.setIn(["filters", action.payload.idx], record);
    }
    default:
      const _: never = action;
      return state;
  }
}
