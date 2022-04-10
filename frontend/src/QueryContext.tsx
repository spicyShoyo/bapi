import Immutable from "immutable";
import React, { useCallback, useEffect, useRef } from "react";
import { useNavigate, useLocation } from "react-router-dom";

import { Filter } from "./queryConsts";
import QueryRecord from "@/QueryRecord";

// eslint-disable-next-line no-unused-vars
type UpdateFn = (queryRecord: QueryRecord) => QueryRecord;

export const QueryContext = React.createContext<{
  queryRecord: QueryRecord;
  filterMap: Map<number, React.MutableRefObject<Filter>>;
  runQuery: () => void;
  // eslint-disable-next-line no-unused-vars
  updateQueryRecord: (fn: UpdateFn) => void;
}>({
  queryRecord: new QueryRecord(),
  filterMap: new Map(),
  runQuery: () => {},
  updateQueryRecord: () => {},
});

function useQueryRecord(): [
  React.MutableRefObject<QueryRecord>,
  // eslint-disable-next-line no-unused-vars
  (fn: UpdateFn) => void,
] {
  const location = useLocation();
  const queryRecordRef = useRef<QueryRecord>(QueryRecord.fromUrl(location));

  // @ts-expect-error: for debug
  window.recordRef = queryRecordRef;

  const updateQueryRecord = useCallback(
    (updateFn: UpdateFn) => {
      const newRecord = updateFn(queryRecordRef.current);
      if (!Immutable.is(newRecord, queryRecordRef.current)) {
        queryRecordRef.current = newRecord;
      }
    },
    [queryRecordRef],
  );

  return [queryRecordRef, updateQueryRecord];
}

export function QueryContextProvider({
  children = null,
}: {
  children: React.ReactElement | null;
}) {
  const navigate = useNavigate();
  const [queryRecordRef, updateQueryRecord] = useQueryRecord();
  // TODO: this is gross
  const filterMap = useRef<Map<number, React.MutableRefObject<Filter>>>(
    new Map(),
  );

  const runQuery = useCallback(() => {
    navigate(queryRecordRef.current.toUrl());
  }, [queryRecordRef, navigate]);

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
        filterMap: filterMap.current,
        queryRecord: queryRecordRef.current,
        updateQueryRecord,
      }}
    >
      {children}
    </QueryContext.Provider>
  );
}
