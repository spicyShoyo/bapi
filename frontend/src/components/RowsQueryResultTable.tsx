import { QueryContext } from "@/QueryContext";
import { useContext } from "react";

import type { RowsQueryResult } from "@/dataManager";
import { ResultTableColumns, ResultTableData } from "./ResultTable";
import { ResultTable } from "./ResultTable";

function useBuildTable(
  result: RowsQueryResult | undefined,
): null | [ResultTableColumns, ResultTableData] {
  if (result == null) {
    return null;
  }

  console.log("$$$", result);

  const strCols = result.str_column_names ?? [];
  const intCols = result.int_column_names ?? [];

  const getCol = (col: string) => ({
    Header: col,
    accessor: col,
  });

  const columns = [...strCols.map(getCol), ...intCols.map(getCol)];

  const data = [];
  for (let rowIdx = 0; rowIdx < result.count; rowIdx++) {
    const row: { [key: string]: string | number | null } = {};
    strCols.forEach((col, colIdx) => {
      const i = colIdx * strCols.length + rowIdx;
      row[getCol(col).accessor] =
        result.str_has_value![i] === true
          ? result.str_id_map![result.str_result![i].toString()]
          : null;
    });

    intCols.forEach((col, colIdx) => {
      const i = colIdx * intCols.length + rowIdx;
      row[getCol(col).accessor] =
        result.int_has_value![i] === true ? result.int_result![i] : null;
    });

    data.push(row);
  }

  return [columns, data];
}

export function RowsQueryResultTable() {
  const { rowsQueryApiReply: reply } = useContext(QueryContext);
  const tableData = useBuildTable(reply?.result);
  return tableData != null ? (
    <ResultTable columns={tableData[0]} data={tableData[1]} />
  ) : null;
}
