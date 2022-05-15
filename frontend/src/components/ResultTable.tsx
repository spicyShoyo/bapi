import { useTable, useBlockLayout, useResizeColumns } from "react-table";

export type ResultTableColumns = { Header: string; accessor: string }[];
export type ResultTableData = { [key: string]: string | number | null }[];

export function ResultTable({
  columns,
  data,
}: {
  columns: ResultTableColumns;
  data: ResultTableData;
}) {
  const innerWidth = window.innerWidth - 312;
  const tableRenderData = useTable(
    {
      columns,
      data,
      defaultColumn: {
        minWidth: 100,
        width: Math.max(innerWidth / columns.length, 100),
      },
    },
    useBlockLayout,
    useResizeColumns,
  );

  const {
    getTableProps,
    getTableBodyProps,
    headerGroups,
    rows,
    prepareRow,
    totalColumnsWidth,
  } = tableRenderData;

  let widthTaken = 0;
  for (let i = 0; i < tableRenderData.allColumns.length - 1; i++) {
    widthTaken += +(tableRenderData.allColumns[i].width ?? 0);
  }
  const lastColWidth =
    innerWidth - widthTaken > 0 ? `${innerWidth - widthTaken}px` : null;

  const tableWidth = Math.max(innerWidth, totalColumnsWidth);
  return (
    <div
      {...getTableProps()}
      className="text-slate-100 border-y-2 border-slate-500"
      style={{ width: tableWidth }}
    >
      <div>
        {headerGroups.map((headerGroup) => {
          const headerProps = headerGroup.getHeaderGroupProps();
          headerProps!.style!.width = tableWidth;
          return (
            <div {...headerProps}>
              {headerGroup.headers.map((column, idx) => {
                const props = column.getHeaderProps();
                if (
                  idx === headerGroup.headers.length - 1 &&
                  lastColWidth != null
                ) {
                  props!.style!.minWidth = lastColWidth;
                }
                return (
                  <div {...props} className="text-center font-bold">
                    {column.render("Header")}
                    {idx < headerGroup.headers.length - 1 ? (
                      <div
                        //@ts-ignore
                        {...column.getResizerProps()}
                        className="inline-block w-2 h-full absolute -right-1 z-50 bg-slate-800 overflow-ellipsis"
                      />
                    ) : null}
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>
      <div {...getTableBodyProps()}>
        {rows.map((row) => {
          prepareRow(row);
          const rowProps = row.getRowProps();
          rowProps!.style!.width = tableWidth;
          return (
            <div {...rowProps}>
              {row.cells.map((cell, idx) => {
                const props = cell.getCellProps();
                if (idx === row.cells.length - 1 && lastColWidth != null) {
                  props!.style!.minWidth = lastColWidth;
                }
                return (
                  <div
                    {...props}
                    className="border-2 border-slate-500 overflow-ellipsis"
                  >
                    {cell.render("Cell")}
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>
    </div>
  );
}
