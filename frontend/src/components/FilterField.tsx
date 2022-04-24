import { Popover, Combobox, Listbox } from "@headlessui/react";
import { useContext, useEffect, useState } from "react";

import TokenizedTextField from "./TokenizedTextField";
import { ColumnInfo, ColumnType } from "@/columnRecord";
import * as dataManager from "@/dataManager";
import { Filter, FilterRecord } from "@/filterRecord";
import nullthrows from "@/nullthrows";
import { FilterOp, FilterOpType, getFilterOpStr } from "@/queryConsts";
import { TableContext, TableInfo } from "@/TableContext";
import { Dropdown } from "./Dropdown";

function findColumn(
  colName: string | undefined,
  tableInfo: TableInfo,
): ColumnInfo | null {
  if (colName == null) {
    return null;
  }
  return (
    tableInfo.str_columns?.find((col) => col.column_name === colName) ??
    tableInfo.int_columns?.find((col) => col.column_name === colName) ??
    null
  );
}

function useFilterSettings(
  initFilter: Filter | null,
  tableData: TableInfo,
  onUpdate: (updatedFilter: Filter) => void,
): {
  intVals: number[];
  strVals: string[];
  column: ColumnInfo;
  setColName: (_: string) => void;
  filterOp: FilterOpType;
  setFilterOp: (_: FilterOpType) => void;
  setIntVals: (_: number[]) => void;
  setStrVals: (_: string[]) => void;
} {
  const [filter, setFilter] = useState<Filter | null>(initFilter);

  const [column, setColumn] = useState<ColumnInfo>(
    nullthrows(
      findColumn(filter?.column_name, tableData) ??
        tableData.str_columns?.[0] ??
        tableData.int_columns?.[0],
    ),
  );
  const [filterOp, setFilterOp] = useState<FilterOpType>(
    filter?.filter_op ?? FilterOp.EQ,
  );
  const [intVals, setIntVals] = useState<number[]>(filter?.int_vals ?? []);
  const [strVals, setStrVals] = useState<string[]>(filter?.str_vals ?? []);

  useEffect(() => {
    const newFilter = {
      column_name: column.column_name,
      filter_op: filterOp,
      int_vals: intVals,
      str_vals: strVals,
    };
    if (FilterRecord.filtersMatch(newFilter, filter)) {
      return;
    }

    setFilter(newFilter);
    onUpdate(newFilter);

    // TODO: this is gross
  }, [column, filterOp, intVals, strVals, onUpdate, filter]);

  return {
    column,
    setColName: (colName: string) => {
      const column = nullthrows(findColumn(colName, tableData));
      setColumn(column);
      setFilterOp(FilterOp.EQ);
      setIntVals([]);
      setStrVals([]);
    },
    filterOp,
    setFilterOp,
    intVals,
    strVals,
    setIntVals,
    setStrVals,
  };
}

export default function FilterField(props: {
  filter: Filter | null;
  onUpdate: (updatedFilter: Filter) => void;
  onRemove: () => void;
}) {
  const tableInfo = useContext(TableContext);
  const {
    column,
    setColName,
    filterOp,
    setFilterOp,
    setIntVals,
    setStrVals,
    intVals,
    strVals,
  } = useFilterSettings(props.filter, nullthrows(tableInfo), props.onUpdate);

  return (
    <div className="flex flex-col gap-4 mx-2 mt-2 py-2 px-4 outline-double outline-slate-200">
      <div className="flex justify-between">
        <div className="flex gap-2">
          <ColSelector
            tableData={tableInfo!}
            colName={column.column_name}
            setColName={setColName}
          />
          <Dropdown
            valToString={getFilterOpStr}
            values={[
              FilterOp.EQ,
              FilterOp.NE,
              FilterOp.LT,
              FilterOp.GT,
              FilterOp.LE,
              FilterOp.GE,
              FilterOp.NONNULL,
              FilterOp.NULL,
            ]}
            selected={filterOp}
            setSelected={setFilterOp}
          />
        </div>
        <button
          className="text-slate-100 bg-slate-700 px-2 py-1 rounded font-bold"
          onClick={props.onRemove}
        >
          <b>{"\u00d7"}</b>
        </button>
      </div>
      <TokenizedTextField
        initValues={
          column.column_type === ColumnType.INT
            ? intVals.map((v) => v.toString())
            : strVals
        }
        queryToValue={(v: string) => v}
        valueToString={(v: string | null) => v ?? ""}
        fetchHints={(query) => {
          if (column.column_type === ColumnType.INT) {
            return Promise.resolve([]);
          }
          return dataManager.fetchStringValues(
            tableInfo!.table_name,
            column.column_name,
            query,
          );
        }}
        setValues={(values) => {
          // TODO: validate ints
          if (column.column_type === ColumnType.INT) {
            setIntVals(values.map((v) => +v));
          } else if (column.column_type === ColumnType.STR) {
            setStrVals(values);
          }
        }}
      />
    </div>
  );
}

function ColSelector({
  colName,
  tableData,
  setColName,
}: {
  colName: string;
  tableData: TableInfo;
  setColName: (_: string) => void;
}) {
  return (
    <Popover>
      <Popover.Button>
        <div className="flex justify-between text-slate-100 bg-slate-700 px-2 py-1 rounded font-bold w-[144px] text-left">
          <div>{colName}</div>
          <div>{"\u2630"}</div>
        </div>
      </Popover.Button>
      <Popover.Panel>
        {({ close }) => (
          <ColCombobox
            setColName={(colName) => {
              setColName(colName);
              close();
            }}
            cols={[
              ...(tableData!.str_columns?.map((col) => col.column_name) ?? []),
              ...(tableData!.int_columns?.map((col) => col.column_name) ?? []),
            ]}
          />
        )}
      </Popover.Panel>
    </Popover>
  );
}

function ColCombobox({
  cols,
  setColName,
}: {
  cols: string[];
  setColName: (_: string) => void;
}) {
  const [query, setQuery] = useState("");

  const filteredCols =
    query === ""
      ? cols
      : cols.filter((col) =>
          col.toLowerCase().includes(query.toLowerCase().trim()),
        );

  return (
    <div className="absolute">
      <Combobox value={null} onChange={setColName}>
        <div className="flex flex-col mt-1 w-[144px]">
          <div className="text-left bg-white rounded-lg shadow-md overflow-hidden">
            <Combobox.Input
              className="py-2 pl-3 pr-10 text-gray-900"
              displayValue={(col: string) => col}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
          <Combobox.Options
            static
            className="py-1 mt-1  bg-white rounded-md shadow-lg max-h-60 focus:outline-none sm:text-sm"
          >
            {filteredCols.length === 0 && query !== "" ? (
              <div className="cursor-default select-none py-2 px-4 text-gray-700">
                No Columns Found
              </div>
            ) : (
              filteredCols.map((col) => (
                <Combobox.Option
                  key={col}
                  className={({ active }) =>
                    `cursor-default select-none py-2 pl-4 pr-4 ${
                      active ? "text-white bg-teal-600" : "text-gray-900"
                    }`
                  }
                  value={col}
                >
                  {col}
                </Combobox.Option>
              ))
            )}
          </Combobox.Options>
        </div>
      </Combobox>
    </div>
  );
}
