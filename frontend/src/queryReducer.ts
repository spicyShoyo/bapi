import { createAction, PayloadAction, Update } from "@reduxjs/toolkit";
import Immutable from "immutable";
import { ColumnInfo, ColumnRecord, ColumnType } from "./columnRecord";
import QueryRecord, { DEFAULT_RECORD } from "./queryRecord";
import { AggOpType } from "@/queryConsts";
import { Filter, FilterRecord } from "./filterRecord";

export const initRecord = createAction<QueryRecord>("initRecord");

type SetTsRangePayload = { maxTs?: number; minTs?: number };
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
    | PayloadAction<QueryRecord, "initRecord">
    | PayloadAction<SetTsRangePayload, "setTsRange">
    | PayloadAction<ColumnInfo[], "setGroupbyCols">
    | PayloadAction<ColumnInfo[], "setAggregateCols">
    | PayloadAction<AggOpType, "setAggOp">
    | PayloadAction<Filter, "addFilter">
    | PayloadAction<number, "removeFilter">
    | PayloadAction<UpdateFilterPayload, "updateFilter">,
) {
  switch (action.type) {
    case "initRecord":
      return action.payload;
    case "setTsRange": {
      let newState = state;
      if (action.payload.minTs != null) {
        newState = newState.set("min_ts", action.payload.minTs);
      }
      if (action.payload.maxTs != null) {
        newState = newState.set("max_ts", action.payload.maxTs);
      }
      return newState;
    }
    case "setGroupbyCols": {
      let intCols = Immutable.List<ColumnRecord>();
      let strCols = Immutable.List<ColumnRecord>();
      action.payload.forEach((col) => {
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
      return state
        .set("groupby_int_columns", intCols)
        .set("groupby_str_columns", strCols);
    }
    case "setAggregateCols": {
      let intCols = Immutable.List<ColumnRecord>();
      action.payload.forEach((col) => {
        switch (col.column_type) {
          case ColumnType.INT:
            intCols = intCols.push(ColumnRecord.fromColumnInfo(col));
            break;
          default:
            throw new Error(`invalid agregate column type ${col.column_type}`);
        }
      });
      return state.set("agg_int_column_names", intCols);
    }
    case "setAggOp": {
      return state.set("agg_op", action.payload);
    }
    case "addFilter": {
      const record = FilterRecord.fromFilter(action.payload);
      // TODO: fix typing
      const filters = Immutable.List(state.filters ?? []);
      return state.set("filters", filters.push(record));
    }
    case "removeFilter": {
      return state.removeIn(["filter", action.payload]);
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
