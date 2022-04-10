import React, { useEffect } from "react";

import * as dataManager from "@/dataManager";

export enum ColumnType {
  NONE = 0,
  INT = 1,
  STR = 2,
}

export type ColumnInfo = {
  column_name: string;
  column_type: ColumnType;
};

export type TableInfo = {
  table_name: string;
  row_count: number;
  min_ts: number;
  max_ts: number | null;
  int_columns: ColumnInfo[] | null;
  str_columns: ColumnInfo[] | null;
};

// Provides data related to the current user such as wallet.
export const TableContext = React.createContext<TableInfo | null>(null);

export function TableContextProvider({
  table,
  children = null,
}: {
  table: string;
  children: React.ReactElement | null;
}) {
  // TODO: connect with table data
  const [tableInfo, setTableInfo] = React.useState<TableInfo | null>(null);

  useEffect(() => {
    dataManager.fetchTableInfo(table).then(setTableInfo);
  }, [table]);

  return tableInfo == null ? null : (
    // eslint-disable-next-line react/jsx-no-constructed-context-values
    <TableContext.Provider value={tableInfo}>{children}</TableContext.Provider>
  );
}
