import { useContext } from "react";

import FilterField from "./FilterField";
import { Filter } from "@/filterRecord";
import useQuerySelector from "@/useQuerySelector";
import { useDispatch } from "react-redux";
import { removeFilter, updateFilter, addFilter } from "@/queryReducer";
import { TableContext } from "@/TableContext";
import { ColumnType } from "@/columnRecord";
import { FilterOp } from "@/queryConsts";
import Immutable from "immutable";

export default function FiltersSection() {
  const d = useDispatch();
  const filters = useQuerySelector((r) => r.filters);
  const tableInfo = useContext(TableContext);

  return (
    <div className="flex flex-col">
      {filters?.map((filter, idx) => (
        <FilterField
          key={idx}
          // TODO: fix typing
          filter={{
            column_name: filter.column_name!,
            column_type: filter.column_type!,
            filter_op: filter.filter_op!,
            // @ts-ignore
            int_vals:
              filter.int_vals! instanceof Immutable.List
                ? filter.int_vals.toJS()
                : filter.int_vals!,
            // @ts-ignore
            str_vals:
              filter.str_vals! instanceof Immutable.List
                ? filter.str_vals.toJS()
                : filter.str_vals!,
          }}
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
        className="text-slate-100 bg-slate-700 mx-2 py-1 mt-2 rounded font-bold"
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
