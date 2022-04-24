import Immutable from "immutable";
import React, { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";

import useFilters, { FilterId, FiltersManager } from "./useFilters";
import { Filter } from "@/filterRecord";
import QueryRecord from "@/queryRecord";
import useQuerySelector from "./useQuerySelector";

export type UpdateFn = (queryRecord: QueryRecord) => QueryRecord;

export const QueryContext = React.createContext<
  FiltersManager & {
    queryRecord: QueryRecord;
  }
>({
  queryRecord: new QueryRecord(),
  uiFilters: [],
  addFilter: () => 0,
  removeFilter: (filterId: FilterId) => {},
  updateFilter: (id: FilterId, filter: Filter) => {},
});

function useQueryRecord(): [QueryRecord, (fn: UpdateFn) => void] {
  const [queryRecord, setQueryRecord] = useState<QueryRecord>(
    new QueryRecord(),
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

function useRunQuery(queryRecord: QueryRecord) {
  const navigate = useNavigate();
  const runQueryRef = useRef(() => {
    navigate(queryRecord.toUrl());
  });
  useEffect(() => {
    runQueryRef.current = () => {
      navigate(queryRecord.toUrl());
    };
  }, [queryRecord, navigate]);

  return () => runQueryRef.current();
}

export function QueryContextProvider({
  children = null,
}: {
  children: React.ReactElement | null;
}) {
  const query = useQuerySelector((r) => r);
  const runQuery = useRunQuery(query);

  const [queryRecord, updateQueryRecord] = useQueryRecord();

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
        queryRecord,
        ...useFilters(queryRecord, updateQueryRecord),
      }}
    >
      {children}
    </QueryContext.Provider>
  );
}
