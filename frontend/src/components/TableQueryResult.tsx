import { QueryContext } from "@/QueryContext";
import { useContext } from "react";
import { useTable } from "react-table";

function Table({ columns, data }: any) {
  const { getTableProps, getTableBodyProps, headerGroups, rows, prepareRow } =
    useTable({
      columns,
      data,
    });

  return (
    <table {...getTableProps()} className="bg-white">
      <thead>
        {headerGroups.map((headerGroup: any) => (
          <tr {...headerGroup.getHeaderGroupProps()}>
            {headerGroup.headers.map((column: any) => (
              <th {...column.getHeaderProps()}>{column.render("Header")}</th>
            ))}
          </tr>
        ))}
      </thead>
      <tbody {...getTableBodyProps()}>
        {rows.map((row: any, i: number) => {
          prepareRow(row);
          return (
            <tr {...row.getRowProps()}>
              {row.cells.map((cell: any) => {
                return <td {...cell.getCellProps()}>{cell.render("Cell")}</td>;
              })}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// keep in sync with `bapi.proto`
type TableQueryResult = {
  count: number;

  int_column_names?: string[];
  int_result?: number[];
  int_has_value?: boolean[];

  str_column_names?: string[];
  str_id_map?: { [key: string]: string };
  str_result?: number[];
  str_has_value?: boolean[];

  agg_int_column_names?: string[];
  agg_int_result?: number[];
  agg_int_has_value?: boolean[];

  agg_float_column_names?: string[];
  agg_float_result?: number[];
  agg_float_has_value?: boolean[];
};

function useBuildTable(result: TableQueryResult): null | any[] {
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
      const i = colIdx * strCols.length + rowIdx;
      row[getColGroupby(col).accessor] =
        result.str_has_value![i] === true
          ? result.str_id_map![result.str_result![i].toString()]
          : null;
    });

    intCols.forEach((col, colIdx) => {
      const i = colIdx * intCols.length + rowIdx;
      row[getColGroupby(col).accessor] =
        result.int_has_value![i] === true ? result.int_result![i] : null;
    });

    aggFloatCols.forEach((col, colIdx) => {
      const i = colIdx * aggFloatCols.length + rowIdx;
      row[getColAgg(col).accessor] =
        result.agg_float_has_value![i] === true
          ? result.agg_float_result![i]
          : null;
    });

    aggIntCols.forEach((col, colIdx) => {
      const i = colIdx * aggIntCols.length + rowIdx;
      row[getColAgg(col).accessor] =
        result.agg_int_has_value![i] === true
          ? result.agg_int_result![i]
          : null;
    });

    data.push(row);
  }

  return [columns, data];
}

export function TableQueryResult() {
  const { result } = useContext(QueryContext);
  const tableData = useBuildTable(result?.result);
  return tableData != null ? (
    <Table columns={tableData[0]} data={tableData[1]} />
  ) : null;
}
