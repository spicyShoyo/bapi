import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import BapiQueryRecord from "@/bapiQueryRecord";

import queryStore from "./queryStore";
import { recordToUrl } from "./queryRecordUtils";
import { useDispatch } from "react-redux";
import { materialize } from "./queryReducer";
import { fetchQueryResult } from "./dataManager";

export type UpdateFn = (queryRecord: BapiQueryRecord) => BapiQueryRecord;

export const QueryContext = React.createContext<{ result: any }>({
  result: null,
});

export function QueryContextProvider({
  children = null,
}: {
  children: React.ReactElement | null;
}) {
  const d = useDispatch();
  const navigate = useNavigate();
  const [result, setResult] = useState<any>(null);

  const runQuery = () => {
    d(materialize());
    setTimeout(() => {
      const record = queryStore.getState();
      navigate(recordToUrl(record));
      fetchQueryResult(record).then(setResult);
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
      value={{ result }}
    >
      {children}
    </QueryContext.Provider>
  );
}
