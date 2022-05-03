import { QueryContext } from "@/QueryContext";
import { useContext } from "react";

import type { TableQueryResult } from "@/dataManager";
import { ResultTableColumns, ResultTableData } from "./ResultTable";
import { ResultTable } from "./ResultTable";

function useBuildTable(
  result: TableQueryResult | undefined,
): null | [ResultTableColumns, ResultTableData] {
  if (result == null) {
    return null;
  }

  const strCols = result.str_column_names ?? [];
  const intCols = result.int_column_names ?? [];

  const aggFloatCols = result.agg_float_column_names ?? [];
  const aggIntCols = result.agg_int_column_names ?? [];

  // We need to assign distinct result table column names otherwise useTable throws.
  // We can have same column used in aggregate and groupby.
  const getCol = (suffix: string, col: string) => ({
    Header: col + ` (${suffix})`,
    accessor: col + ` (${suffix})`,
  });
  const getColGroupby = (col: string) => getCol("G", col);
  const getColAgg = (col: string) => getCol("A", col);

  const columns = [
    ...strCols.map(getColGroupby),
    ...intCols.map(getColGroupby),
    ...aggFloatCols.map(getColAgg),
    ...aggIntCols.map(getColAgg),
  ];

  const data = [];
  for (let rowIdx = 0; rowIdx < result.count; rowIdx++) {
    const row: { [key: string]: string | number | null } = {};
    strCols.forEach((col, colIdx) => {
      const i = colIdx * result.count + rowIdx;
      row[getColGroupby(col).accessor] =
        result.str_has_value![i] === true
          ? result.str_id_map![result.str_result![i].toString()]
          : null;
    });

    intCols.forEach((col, colIdx) => {
      const i = colIdx * result.count + rowIdx;
      row[getColGroupby(col).accessor] =
        result.int_has_value![i] === true ? result.int_result![i] : null;
    });

    aggFloatCols.forEach((col, colIdx) => {
      const i = colIdx * result.count + rowIdx;
      row[getColAgg(col).accessor] =
        result.agg_float_has_value![i] === true
          ? result.agg_float_result![i]
          : null;
    });

    aggIntCols.forEach((col, colIdx) => {
      const i = colIdx * result.count + rowIdx;
      row[getColAgg(col).accessor] =
        result.agg_int_has_value![i] === true
          ? result.agg_int_result![i]
          : null;
    });

    data.push(row);
  }

  return [columns, data];
}

export function TableQueryResultTable() {
  const { tableQueryApiReply: reply } = useContext(QueryContext);
  const tableData = useBuildTable(reply?.result);
  return tableData != null ? (
    <ResultTable columns={tableData[0]} data={tableData[1]} />
  ) : null;
}
