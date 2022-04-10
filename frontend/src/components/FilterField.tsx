/* eslint-disable no-unused-vars */
import { Popover, Combobox, Listbox } from "@headlessui/react";
import React, {
  InputHTMLAttributes,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";

import * as dataManager from "@/dataManager";
import nullthrows from "@/nullthrows";
import { Filter, FilterOp, FilterOpType, getFilterOpStr } from "@/queryConsts";
import {
  ColumnInfo,
  ColumnType,
  TableContext,
  TableInfo,
} from "@/TableContext";

function useFilterSettings(
  tableData: TableInfo,
  onUpdate: (updatedFilter: Filter) => void,
): {
  column: ColumnInfo;
  setColName: (_: string) => void;
  filterOp: FilterOpType;
  setFilterOp: (_: FilterOpType) => void;
  setIntVals: (_: number[]) => void;
  setStrVals: (_: string[]) => void;
} {
  const [column, setColumn] = useState<ColumnInfo>(
    nullthrows(tableData.str_columns?.[0] ?? tableData.int_columns?.[0]),
  );
  const [filterOp, setFilterOp] = useState<FilterOpType>(FilterOp.EQ);
  const [intVals, setIntVals] = useState<number[]>([]);
  const [strVals, setStrVals] = useState<string[]>([]);

  useEffect(() => {
    onUpdate({
      column_name: column.column_name,
      filter_op: filterOp,
      // TODO: support multiple values
      int_vals: intVals,
      str_vals: strVals,
    });
  }, [column, filterOp, intVals, strVals, onUpdate]);

  return {
    column,
    setColName: (colName: string) => {
      const column = nullthrows(
        tableData.str_columns?.find((col) => col.column_name === colName) ??
          tableData.int_columns?.find((col) => col.column_name === colName),
      );
      setColumn(column);
      setFilterOp(FilterOp.EQ);
      setIntVals([]);
      setStrVals([]);
    },
    filterOp,
    setFilterOp,
    setIntVals,
    setStrVals,
  };
}

export default function FilterField(props: {
  onUpdate: (updatedFilter: Filter) => void;
  onRemove: () => void;
}) {
  const tableInfo = useContext(TableContext);
  const { column, setColName, filterOp, setFilterOp, setIntVals, setStrVals } =
    useFilterSettings(nullthrows(tableInfo), props.onUpdate);

  return (
    <div className="flex flex-col gap-4 mx-2 mt-2 py-2 px-4 outline-double outline-slate-200">
      <div className="flex justify-between">
        <div className="flex gap-2">
          <ColSelector
            tableData={tableInfo!}
            colName={column.column_name}
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
      <ValuesCombobox
        table={tableInfo!.table_name}
        column={column}
        setIntVals={setIntVals}
        setStrVals={setStrVals}
      />
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

function useTypeahead(
  table: string,
  column: ColumnInfo,
  query: string,
): string[] {
  const [hint, setHint] = useState<string[]>([]);
  const queryString = query.trim();

  useEffect(() => {
    setHint(queryString === "" ? [] : [queryString]);

    if (queryString === "" || column.column_type !== ColumnType.STR) {
      return;
    }

    dataManager
      .fetchStringValues(table, column.column_name, queryString)
      .then((hints) => {
        if (hints == null) {
          return;
        }
        setHint(queryString === "" ? hints : [queryString, ...hints]);
      });
  }, [table, column, queryString]);
  return hint;
}

const TextBox = React.forwardRef(
  (props: InputHTMLAttributes<HTMLInputElement>, ref) => (
    // @ts-ignore
    // eslint-disable-next-line react/jsx-props-no-spreading
    <input ref={ref} autoComplete="off" {...props} />
  ),
);

function ValuesCombobox({
  table,
  column,
  setIntVals,
  setStrVals,
}: {
  table: string;
  column: ColumnInfo;
  setIntVals: (_: number[]) => void;
  setStrVals: (_: string[]) => void;
}) {
  const [query, setQuery] = useState("");
  const valuesHint = useTypeahead(table, column, query);
  const [values, setValues] = useState<string[]>([]);
  const onSelect = useCallback(
    (value) => {
      if (values.includes(value)) {
        return;
      }
      setValues([...values, value]);
    },
    [values, setValues],
  );

  const onRemove = useCallback(
    (value) => {
      setValues(values.filter((val) => val !== value));
    },
    [values, setValues],
  );

  useEffect(() => {
    // TODO: validate ints
    if (column.column_type === ColumnType.INT) {
      setIntVals(values.map((v) => +v));
    } else if (column.column_type === ColumnType.STR) {
      setStrVals(values);
    }
  }, [column.column_type, setIntVals, setStrVals, values]);

  return (
    <div className="flex flex-col gap-2">
      <Combobox value="" onChange={onSelect}>
        <div className="flex flex-col">
          <div className="text-left bg-white rounded-lg shadow-md overflow-hidden w-full">
            <Combobox.Input
              as={TextBox}
              className="py-2 pl-3 pr-10 text-gray-900 w-full autocompl"
              displayValue={(col: string) => col}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
          <div className="absolute mt-11 w-[272px]">
            <Combobox.Options className="py-1 bg-white rounded-md shadow-lg max-h-60 focus:outline-none sm:text-sm">
              {valuesHint.map((val) => (
                <Combobox.Option
                  key={val}
                  className={({ active }) =>
                    `cursor-default select-none py-2 pl-4 pr-4 ${
                      active ? "text-white bg-teal-600" : "text-gray-900"
                    }`
                  }
                  value={val}
                >
                  {val}
                </Combobox.Option>
              ))}
            </Combobox.Options>
          </div>
        </div>
      </Combobox>
      <div className="flex flex-wrap gap-1">
        {values.map((val) => (
          <button
            className="bg-slate-500 rounded mx-1 px-2 text-slate-200"
            key={val}
            onClick={() => onRemove(val)}
          >
            {val}
          </button>
        ))}
      </div>
    </div>
  );
}
