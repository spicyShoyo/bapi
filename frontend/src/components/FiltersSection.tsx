import { useContext } from "react";

import FilterField from "./FilterField";
import { useQueryFilters } from "@/useQuerySelector";
import { useDispatch } from "react-redux";
import { removeFilter, updateFilter, addFilter } from "@/queryReducer";
import { TableContext } from "@/TableContext";
import { FilterOp, Filter, ColumnType } from "@/queryConsts";

export default function FiltersSection() {
  const d = useDispatch();
  const filters = useQueryFilters();
  const tableInfo = useContext(TableContext);

  return (
    <div className="flex flex-col gap-2 border-t-2 pt-2 border-slate-500">
      <div className="text-slate-100 font-bold">Filters</div>
      {filters?.map((filter, idx) => (
        <FilterField
          key={idx}
          filter={filter}
          onUpdate={(filter: Filter) =>
            d(
              updateFilter({
                idx,
                filter,
              }),
            )
          }
          onRemove={() => d(removeFilter(idx))}
        />
      ))}
      <button
        className="text-slate-100 bg-slate-700 py-1 rounded font-bold"
        onClick={() => {
          if (
            (tableInfo?.str_columns?.length ?? 0) === 0 &&
            (tableInfo?.int_columns?.length ?? 0) === 0
          ) {
            throw new Error("no columns loaded");
          }

          const column_type =
            tableInfo!.str_columns != null ? ColumnType.STR : ColumnType.INT;
          const column_name =
            column_type === ColumnType.STR
              ? tableInfo!.str_columns![0].column_name
              : tableInfo!.int_columns![0].column_name;
          d(
            addFilter({
              column_name,
              column_type,
              filter_op: FilterOp.EQ,
              int_vals: [],
              str_vals: [],
            }),
          );
        }}
      >
        <b>+ Add Filter</b>
      </button>
    </div>
  );
}
