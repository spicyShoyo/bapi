import { createAction, PayloadAction } from "@reduxjs/toolkit";
import Immutable from "immutable";
import { ColumnInfo, ColumnRecord, ColumnType } from "./columnRecord";
import QueryRecord from "./queryRecord";
import { AggOpType } from "@/queryConsts";

export const initRecord = createAction<QueryRecord>("initRecord");

type SetTsRangePayload = { maxTs?: number; minTs?: number };
export const setTsRange = createAction<SetTsRangePayload>("setTsRange");

export const setGroupbyCols = createAction<ColumnInfo[]>("setGroupbyCols");
export const setAggregateCols = createAction<ColumnInfo[]>("setAggregateCols");
export const setAggOp = createAction<AggOpType>("setAggOp");

export default function queryReducer(
  state = new QueryRecord(),
  action:
    | PayloadAction<QueryRecord, "initRecord">
    | PayloadAction<SetTsRangePayload, "setTsRange">
    | PayloadAction<ColumnInfo[], "setGroupbyCols">
    | PayloadAction<ColumnInfo[], "setAggregateCols">
    | PayloadAction<AggOpType, "setAggOp">,
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
    default:
      const _: never = action;
      return state;
  }
}
