import Immutable from "immutable";
import React, { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate, useLocation } from "react-router-dom";

import useFilters, { FilterId, FiltersManager } from "./useFilters";
import { ColumnInfo } from "@/columnRecord";
import { Filter } from "@/filterRecord";
import QueryRecord from "@/queryRecord";
import useAggregation, { AggregateManager } from "./useAggregation";
import { AggOpType, QueryType } from "./queryConsts";

export type UpdateFn = (queryRecord: QueryRecord) => QueryRecord;

export const QueryContext = React.createContext<
  AggregateManager &
    FiltersManager & {
      queryRecord: QueryRecord;
      runQuery: () => void;
      updateQueryRecord: (fn: UpdateFn) => void;
    }
>({
  queryRecord: new QueryRecord(),
  runQuery: () => {},
  updateQueryRecord: () => {},

  uiFilters: [],
  addFilter: () => 0,
  removeFilter: (filterId: FilterId) => {},
  updateFilter: (id: FilterId, filter: Filter) => {},

  setGroupbyCols: (col: ColumnInfo[]) => {},
  setAggregateCols: (col: ColumnInfo[]) => {},
  setAggOp: (op: AggOpType) => {},
});

function useQueryRecord(): [QueryRecord, (fn: UpdateFn) => void] {
  const location = useLocation();
  const [queryRecord, setQueryRecord] = useState<QueryRecord>(
    QueryRecord.fromUrl(location),
  );

  // @ts-expect-error: for debug
  window.getRecord = () => queryRecord;

  const updateQueryRecord = useCallback(
    (updateFn: UpdateFn) => {
      const newRecord = updateFn(queryRecord);
      if (!Immutable.is(newRecord, queryRecord)) {
        setQueryRecord(newRecord);
      }
    },
    [queryRecord, setQueryRecord],
  );

  return [queryRecord, updateQueryRecord];
}

export function QueryContextProvider({
  children = null,
}: {
  children: React.ReactElement | null;
}) {
  const navigate = useNavigate();
  const [queryRecord, updateQueryRecord] = useQueryRecord();

  const runQueryRef = useRef(() => {
    navigate(queryRecord.toUrl());
  });

  useEffect(() => {
    runQueryRef.current = () => {
      navigate(queryRecord.toUrl());
    };
  }, [queryRecord, navigate]);

  function runQuery() {
    runQueryRef.current();
  }

  useEffect(() => {
    function onEnter(e: KeyboardEvent) {
      if (e.key === "Enter" && e.ctrlKey) {
        runQuery();
      }
    }
    document.addEventListener("keypress", onEnter);
    return () => document.removeEventListener("keypress", onEnter);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <QueryContext.Provider
      // eslint-disable-next-line react/jsx-no-constructed-context-values
      value={{
        runQuery,
        queryRecord,
        updateQueryRecord,
        ...useFilters(queryRecord, updateQueryRecord),
        ...useAggregation(updateQueryRecord),
      }}
    >
      {children}
    </QueryContext.Provider>
  );
}
