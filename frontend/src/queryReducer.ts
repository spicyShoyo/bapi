import { createAction, PayloadAction } from "@reduxjs/toolkit";
import Immutable from "immutable";
import {
  AggOp,
  AggOpType,
  QueryType,
  Filter,
  ColumnInfo,
  ColumnType,
} from "@/queryConsts";
import { DEFAULT_RECORD, materializeQuery } from "@/queryRecordUtils";
import { TimeRange } from "./tsConsts";
import BapiQueryRecord from "./bapiQueryRecord";

type SetTsRangePayload = {
  maxTs?: number;
  minTs?: number;
  timeRange?: TimeRange;
};

export const setQueryType = createAction<QueryType>("setQueryType");
export const materialize = createAction<void>("materialize");

export const setTsRange = createAction<SetTsRangePayload>("setTsRange");

export const setTargetCols = createAction<ColumnInfo[]>("setTargetCols");
export const setAggregateCols = createAction<ColumnInfo[]>("setAggregateCols");
export const setAggOp = createAction<AggOpType>("setAggOp");

export const addFilter = createAction<Filter>("addFilter");
export const removeFilter = createAction<number>("removeFilter");
type UpdateFilterPayload = { idx: number; filter: Filter };
export const updateFilter = createAction<UpdateFilterPayload>("updateFilter");

function buildColumnRecord(colInfo: ColumnInfo) {
  return new (BapiQueryRecord.getSpec().agg_cols.getSpec())(colInfo);
}

function buildFilterRecord(filter: Filter) {
  return new (BapiQueryRecord.getSpec().filters.getSpec())(filter);
}

export default function queryReducer(
  state = DEFAULT_RECORD,
  action:
    | PayloadAction<QueryType, "setQueryType">
    | PayloadAction<void, "materialize">
    | PayloadAction<SetTsRangePayload, "setTsRange">
    | PayloadAction<ColumnInfo[], "setTargetCols">
    | PayloadAction<ColumnInfo[], "setAggregateCols">
    | PayloadAction<AggOpType, "setAggOp">
    | PayloadAction<Filter, "addFilter">
    | PayloadAction<number, "removeFilter">
    | PayloadAction<UpdateFilterPayload, "updateFilter">,
) {
  switch (action.type) {
    case "setQueryType": {
      const updatedState = state.set("query_type", action.payload);
      switch (action.payload) {
        case QueryType.Table:
          return updatedState.set("agg_op", AggOp.COUNT).set(
            "agg_cols",
            Immutable.List<DeepRecord<ColumnInfo>>([
              buildColumnRecord({
                column_name: "ts",
                column_type: ColumnType.INT,
              }),
            ]),
          );
        case QueryType.Rows:
          return updatedState.delete("agg_op").delete("agg_cols");
      }
      return updatedState;
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
    case "setTargetCols": {
      let cols = Immutable.List<DeepRecord<ColumnInfo>>();
      action.payload.forEach((col) => {
        cols = cols.push(buildColumnRecord(col));
      });
      return state.set("target_cols", cols);
    }
    case "setAggregateCols": {
      let cols = Immutable.List<DeepRecord<ColumnInfo>>();
      action.payload.forEach((col) => {
        cols = cols.push(buildColumnRecord(col));
      });
      return state.set("agg_cols", cols);
    }
    case "setAggOp": {
      return state.set("agg_op", action.payload);
    }
    case "addFilter": {
      const record = buildFilterRecord(action.payload);
      const filters = state.filters ?? Immutable.List();
      return state.set("filters", filters.push(record));
    }
    case "removeFilter": {
      return state.removeIn(["filters", action.payload]);
    }
    case "updateFilter": {
      const record = buildFilterRecord(action.payload.filter);
      return state.setIn(["filters", action.payload.idx], record);
    }
    default:
      const _: never = action;
      return state;
  }
}
