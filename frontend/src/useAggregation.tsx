import { useCallback, useRef, useState } from "react";

import { ColumnInfo, ColumnType } from "@/columnRecord";
import { UpdateFn } from "@/QueryContext";
import { AggOpType } from "./queryConsts";
import { Record } from "immutable";

export type AggregateManager = {
  setGroupbyCols: (col: ColumnInfo[]) => void;
  setAggregateCols: (col: ColumnInfo[]) => void;
  setAggOp: (op: AggOpType) => void;
};

export default function useAggregation(
  updateQueryRecord: (fn: UpdateFn) => void,
): AggregateManager {
  const setGroupbyCols = useCallback(
    (cols: ColumnInfo[]) =>
      updateQueryRecord((record) => record.setGroupbyCols(cols)),
    [updateQueryRecord],
  );

  const setAggregateCols = useCallback(
    (cols: ColumnInfo[]) =>
      updateQueryRecord((record) => record.setAggregateCols(cols)),
    [updateQueryRecord],
  );
  const setAggOp = useCallback(
    (op: AggOpType) => updateQueryRecord((record) => record.setAggOp(op)),
    [updateQueryRecord],
  );

  return {
    setGroupbyCols,
    setAggregateCols,
    setAggOp,
  };
}
