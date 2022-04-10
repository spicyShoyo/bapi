import { useContext } from "react";

import FilterField from "./FilterField";
import { Filter } from "@/queryConsts";
import { QueryContext } from "@/QueryContext";

export default function FilterFields() {
  const { uiFilters, addFilter, updateFilter, removeFilter } =
    useContext(QueryContext);

  return (
    <div className="flex flex-col">
      {uiFilters.map((filter) => (
        <FilterField
          key={filter.id}
          onUpdate={(updatedFilter: Filter) =>
            updateFilter(filter.id, updatedFilter)
          }
          onRemove={() => removeFilter(filter.id)}
        />
      ))}
      <button
        className="text-slate-100 bg-slate-700 mx-2 py-1 mt-2 rounded font-bold"
        onClick={addFilter}
      >
        <b>+ Add Filter</b>
      </button>
    </div>
  );
}
