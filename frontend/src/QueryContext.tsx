import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import BapiQueryRecord from "@/bapiQueryRecord";

import queryStore from "./queryStore";
import { recordToUrl } from "./queryRecordUtils";
import { useDispatch } from "react-redux";
import { materialize } from "./queryReducer";
import {
  fetchRowsQueryResult,
  fetchTableQueryResult,
  RowsQueryApiReply,
  TableQueryApiReply,
} from "./dataManager";
import { QueryType } from "./queryConsts";

export type UpdateFn = (queryRecord: BapiQueryRecord) => BapiQueryRecord;

export const QueryContext = React.createContext<{
  tableQueryApiReply: TableQueryApiReply | null;
  rowsQueryApiReply: RowsQueryApiReply | null;
  runQuery: () => void;
}>({
  tableQueryApiReply: null,
  rowsQueryApiReply: null,
  runQuery: () => {},
});

export function QueryContextProvider({
  children = null,
}: {
  children: React.ReactElement | null;
}) {
  const d = useDispatch();
  const navigate = useNavigate();
  const [tableQueryApiReply, setTableQueryApiReply] =
    useState<TableQueryApiReply | null>(null);
  const [rowsQueryApiReply, setRowsQueryApiReply] =
    useState<RowsQueryApiReply | null>(null);

  const runQuery = () => {
    d(materialize());
    setTimeout(() => {
      const record = queryStore.getState();
      navigate(recordToUrl(record));
      switch (record.query_type) {
        case QueryType.Table:
          fetchTableQueryResult(record).then(setTableQueryApiReply);
        case QueryType.Rows:
          fetchRowsQueryResult(record).then(setRowsQueryApiReply);
        default:
          return;
      }
    });
  };

  useEffect(() => {
    // @ts-expect-error: for debug
    window.getRecord = () => queryStore.getState();

    function onEnter(e: KeyboardEvent) {
      if (e.key === "Enter" && e.ctrlKey) {
        runQuery();
      }
    }
    document.addEventListener("keyup", onEnter);
    return () => document.removeEventListener("keyup", onEnter);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <QueryContext.Provider
      // eslint-disable-next-line react/jsx-no-constructed-context-values
      value={{ tableQueryApiReply, rowsQueryApiReply, runQuery }}
    >
      {children}
    </QueryContext.Provider>
  );
}
