import { useCallback, useRef, useState } from "react";

import { Filter, FilterRecord } from "./filterRecord";
import { ColumnInfo, ColumnType } from "@/columnRecord";
import { UpdateFn } from "@/QueryContext";

export type FilterId = number;
export type UiFilter = {
  id: FilterId;
  filter: Filter | null;
};

export type FiltersManager = {
  uiFilters: ReadonlyArray<UiFilter>;
  addFilter: () => FilterId;
  removeFilter: (filterId: FilterId) => void;
  updateFilter: (id: FilterId, filter: Filter) => void;
};

export default function useFilters(
  updateQueryRecord: (fn: UpdateFn) => void,
): FiltersManager {
  const lastFilterId = useRef<FilterId>(0);
  // TODO: init from URL
  const materialized = useRef<Map<FilterId, [ColumnType, number]>>(new Map());
  const [uiFilters, setUiFilters] = useState<UiFilter[]>([
    {
      id: lastFilterId.current,
      filter: null,
    },
  ]);

  const addFilter = useCallback(() => {
    lastFilterId.current += 1;
    setUiFilters([...uiFilters, { id: lastFilterId.current, filter: null }]);
    return lastFilterId.current;
  }, [uiFilters]);

  const dematerializeFilter = useCallback(
    (id: FilterId) => {
      if (!materialized.current.has(id)) {
        return;
      }

      const [colType, idx] = materialized.current.get(id)!;
      materialized.current.delete(id);
      Array.from(materialized.current.keys()).forEach((id) => {
        const [curColType, curIdx] = materialized.current.get(id)!;
        if (curColType === colType && idx < curIdx) {
          materialized.current.set(id, [curColType, curIdx - 1]);
        }
      });

      updateQueryRecord((record) => {
        switch (colType) {
          case ColumnType.INT: {
            return record.removeIntFilter(idx);
          }
          case ColumnType.STR: {
            return record.removeStrFilter(idx);
          }
          default:
            throw new Error(`invalid column type ${colType}`);
        }
      });
    },
    [updateQueryRecord],
  );

  const removeFilter = useCallback(
    (id: FilterId) => {
      dematerializeFilter(id);
      if (uiFilters.length === 1) {
        // keep at least one filter in UI
        lastFilterId.current += 1;
        setUiFilters([
          {
            id: lastFilterId.current,
            filter: null,
          },
        ]);
        return;
      }
      setUiFilters(uiFilters.filter((v) => v.id !== id));
    },
    [dematerializeFilter, uiFilters],
  );

  const materializeFilter = useCallback(
    (id: FilterId, filter: Filter) => {
      if (!materialized.current.has(id)) {
        updateQueryRecord((record) => {
          // TODO: better handling of filter type
          if (filter.int_vals.length !== 0) {
            materialized.current.set(id, [
              ColumnType.INT,
              record.int_filters?.size ?? 0,
            ]);
            return record.addIntFilter(filter);
          }
          if (filter.str_vals.length !== 0) {
            materialized.current.set(id, [
              ColumnType.STR,
              record.str_filters?.size ?? 0,
            ]);
            return record.addStrFilter(filter);
          }
          return record;
        });
        return;
      }
      const [_, idx] = materialized.current.get(id)!;
      updateQueryRecord((record) => {
        // TODO: better handling of filter type
        if (filter.int_vals.length !== 0) {
          return record.updateIntFilter(filter, idx);
        }
        if (filter.str_vals.length !== 0) {
          return record.updateStrFilter(filter, idx);
        }
        return record;
      });
    },
    [updateQueryRecord],
  );

  const updateFilter = useCallback(
    (id: FilterId, filter: Filter) => {
      if (!FilterRecord.isValidFilter(filter)) {
        dematerializeFilter(id);
        return;
      }
      materializeFilter(id, filter);
    },
    [dematerializeFilter, materializeFilter],
  );

  return {
    uiFilters,
    addFilter,
    removeFilter,
    updateFilter,
  };
}
