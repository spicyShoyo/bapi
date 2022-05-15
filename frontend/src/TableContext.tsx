import React, { useEffect } from "react";

import { ColumnInfo } from "@/columnInfo";
import * as dataManager from "@/dataManager";

export type TableInfo = {
  table_name: string;
  row_count: number;
  min_ts: number;
  max_ts: number | null;
  int_columns: ColumnInfo[] | null;
  str_columns: ColumnInfo[] | null;
};

export const TableContext = React.createContext<TableInfo | null>(null);

export function TableContextProvider({
  table,
  children = null,
}: {
  table: string;
  children: React.ReactElement | null;
}) {
  const [tableInfo, setTableInfo] = React.useState<TableInfo | null>(null);

  useEffect(() => {
    dataManager.fetchTableInfo(table).then(setTableInfo);
  }, [table]);

  return tableInfo == null ? null : (
    // eslint-disable-next-line react/jsx-no-constructed-context-values
    <TableContext.Provider value={tableInfo}>{children}</TableContext.Provider>
  );
}
