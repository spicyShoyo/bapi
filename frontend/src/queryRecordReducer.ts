import { createAction, PayloadAction } from "@reduxjs/toolkit";
import QueryRecord from "./queryRecord";

export const initRecord = createAction<QueryRecord>("initRecord");

type SetTsRangePayload = { maxTs?: number; minTs?: number };
export const setTsRange = createAction<SetTsRangePayload>("setTsRange");

export default function queryRecordReducer(
  state = new QueryRecord(),
  action:
    | PayloadAction<QueryRecord, "initRecord">
    | PayloadAction<SetTsRangePayload, "setTsRange">,
) {
  switch (action.type) {
    case "initRecord":
      return action.payload;
    case "setTsRange":
      let newState = state;
      if (action.payload.minTs != null) {
        newState = newState.set("min_ts", action.payload.minTs);
      }
      if (action.payload.maxTs != null) {
        newState = newState.set("max_ts", action.payload.maxTs);
      }
      return newState;
    default:
      return state;
  }
}
