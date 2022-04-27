import React, { useEffect } from "react";
import { useNavigate } from "react-router-dom";
import BapiQueryRecord from "@/bapiQueryRecord";

import queryStore from "./queryStore";
import { recordToUrl } from "./queryRecordUtils";

export type UpdateFn = (queryRecord: BapiQueryRecord) => BapiQueryRecord;

export const QueryContext = React.createContext<{}>({});

export function QueryContextProvider({
  children = null,
}: {
  children: React.ReactElement | null;
}) {
  const navigate = useNavigate();
  const runQuery = () => navigate(recordToUrl(queryStore.getState()));

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
      value={{}}
    >
      {children}
    </QueryContext.Provider>
  );
}
