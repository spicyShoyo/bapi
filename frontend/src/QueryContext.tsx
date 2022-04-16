/* eslint-disable no-unused-vars */
import Immutable from "immutable";
import React, { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate, useLocation } from "react-router-dom";

import useFilters, { FilterId, FiltersManager } from "./useFilters";
import { Filter } from "@/filterRecord";
import QueryRecord from "@/queryRecord";

// eslint-disable-next-line no-unused-vars
export type UpdateFn = (queryRecord: QueryRecord) => QueryRecord;

export const QueryContext = React.createContext<
  FiltersManager & {
    queryRecord: QueryRecord;
    runQuery: () => void;
    // eslint-disable-next-line no-unused-vars
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
  setGroupbyCols: (col: string[]) => {},
});

function useQueryRecord(): [
  QueryRecord,
  // eslint-disable-next-line no-unused-vars
  (fn: UpdateFn) => void,
] {
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

  const runQuery = useCallback(() => {
    navigate(queryRecord.toUrl());
  }, [queryRecord, navigate]);

  useEffect(() => {
    function onEnter(e: KeyboardEvent) {
      if (e.key === "Enter" && e.ctrlKey) {
        runQuery();
      }
    }
    document.addEventListener("keydown", onEnter);
    return () => document.removeEventListener("keydown", onEnter);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <QueryContext.Provider
      // eslint-disable-next-line react/jsx-no-constructed-context-values
      value={{
        runQuery,
        queryRecord,
        updateQueryRecord,
        ...useFilters(updateQueryRecord),
      }}
    >
      {children}
    </QueryContext.Provider>
  );
}
