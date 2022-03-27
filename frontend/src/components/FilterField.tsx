/* eslint-disable no-unused-vars */
import { Popover, Combobox, Listbox } from "@headlessui/react";
import { useContext, useState } from "react";

import { FilterOp, FilterOpType, getFilterOpStr } from "@/queryConsts";
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

export default function FilterField(props: { onRemove: () => void }) {
  const { tableData } = useContext(TableContext);
  const { colName, setColName, filterOp, setFilterOp } =
    useFilterSettings(tableData);
  return (
    <div className="flex justify-between mt-4 mr-2">
      <div className="flex">
        <ColSelector
          tableData={tableData}
          colName={colName}
          setColName={setColName}
        />
        <FilterSelector filterOp={filterOp} setFilterOp={setFilterOp} />
      </div>
      <button
        className="text-slate-100 bg-slate-700 px-2 py-1 rounded font-bold"
        onClick={props.onRemove}
      >
        <b>{"\u00d7"}</b>
      </button>
    </div>
  );
}

function FilterSelector(props: {
  filterOp: FilterOpType;
  setFilterOp: (_: FilterOpType) => void;
}) {
  return (
    <div className="flex">
      <Listbox value={props.filterOp} onChange={props.setFilterOp}>
        <Listbox.Button className="text-slate-100 bg-slate-700 p-1 rounded font-bold w-[72px] text-center">
          {getFilterOpStr(props.filterOp)}
        </Listbox.Button>
        <Listbox.Options className="absolute mt-8 p-2 bg-white rounded overflow-hidden">
          {[
            FilterOp.EQ,
            FilterOp.NE,
            FilterOp.LT,
            FilterOp.GT,
            FilterOp.LE,
            FilterOp.GE,
            FilterOp.NONNULL,
            FilterOp.NULL,
          ].map((op) => (
            <Listbox.Option
              className={({ active }) =>
                `cursor-pointer select-none rounded text-center ${
                  active ? "text-white bg-teal-600" : "text-gray-900"
                }`
              }
              key={op}
              value={op}
            >
              <b>{getFilterOpStr(op)}</b>
            </Listbox.Option>
          ))}
        </Listbox.Options>
      </Listbox>
    </div>
  );
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
        <div className="flex justify-between text-slate-100 bg-slate-700 mx-2 px-2 py-1 rounded font-bold w-[144px] text-left">
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
    <div className="absolute ml-2">
      <Combobox value="" onChange={setColName}>
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
