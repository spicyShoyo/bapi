import React from "react";

export type TableData = {
  int_columns: string[];
  str_columns: string[];
};

// Provides data related to the current user such as wallet.
export const TableContext = React.createContext<{ tableData: TableData }>({
  tableData: {
    int_columns: [],
    str_columns: [],
  },
});

export function TableContextProvider({
  children = null,
}: {
  children: React.ReactElement | null;
}) {
  // TODO: connect with table data
  const [tableData] = React.useState<TableData>({
    int_columns: ["ts", "count"],
    str_columns: ["event"],
  });

  return (
    // eslint-disable-next-line react/jsx-no-constructed-context-values
    <TableContext.Provider value={{ tableData }}>
      {children}
    </TableContext.Provider>
  );
}
