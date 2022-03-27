/* eslint-disable no-unused-vars */
import { Popover, Combobox } from "@headlessui/react";
import { useContext, useEffect, useState } from "react";

import { FilterOp, FilterOpType } from "@/queryConsts";
import { QueryContext } from "@/QueryContext";
import { TableContext, TableData } from "@/TableContext";

function useFilterSettings(tableData: TableData): {
  colName: string;
  setColName: (_: string) => void;
  filterOp: FilterOpType;
  setFilterOp: (_: FilterOpType) => void;
  intVal: number | null;
  setIntVal: (_: number | null) => void;
  strVal: string | null;
  setStrVal: (_: string | null) => void;
} {
  const { queryRecord, updateQueryRecord } = useContext(QueryContext);

  const [colName, setColName] = useState(tableData.str_columns[0]);
  const [filterOp, setFilterOp] = useState<FilterOpType>(FilterOp.EQ);
  const [intVal, setIntVal] = useState<number | null>(null);
  const [strVal, setStrVal] = useState<string | null>(null);

  return {
    colName,
    setColName,
    filterOp,
    setFilterOp,
    intVal,
    setIntVal,
    strVal,
    setStrVal,
  };
}

export default function FilterField() {
  const { tableData } = useContext(TableContext);
  const { colName, setColName } = useFilterSettings(tableData);
  return (
    <div className="flex mt-4">
      <ColSelector
        tableData={tableData}
        colName={colName}
        setColName={setColName}
      />
      <FilterSelector />
    </div>
  );
}

function FilterSelector() {
  return <div />;
}

function ColSelector({
  colName,
  tableData,
  setColName,
}: {
  colName: string;
  tableData: TableData;
  setColName: (_: string) => void;
}) {
  return (
    <Popover>
      <Popover.Button>
        <div className="text-slate-100 bg-slate-700 mx-2 p-1 rounded font-bold w-[144px] text-left">
          {colName}
        </div>
      </Popover.Button>
      <Popover.Panel>
        {({ close }) => (
          <ColCombobox
            setColName={(colName) => {
              setColName(colName);
              close();
            }}
            cols={[...tableData.str_columns, ...tableData.int_columns]}
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
    <div className="fixed ml-2">
      <Combobox value="" onChange={setColName}>
        <div className="flex flex-col mt-1 w-[168px]">
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
              <div className="cursor-default select-none relative py-2 px-4 text-gray-700">
                No Columns Found
              </div>
            ) : (
              filteredCols.map((col) => (
                <Combobox.Option
                  key={col}
                  className={({ active }) =>
                    `cursor-default select-none relative py-2 pl-10 pr-4 ${
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
