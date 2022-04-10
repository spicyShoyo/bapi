/* eslint-disable no-unused-vars */
import { useCallback, useRef, useState } from "react";

import nullthrows from "./nullthrows";
import { Filter } from "./queryConsts";
import { ColumnType } from "./TableContext";
import { UpdateFn } from "@/QueryContext";
import QueryRecord from "@/QueryRecord";

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
            console.log("$$$", record.int_filters);
            break;
          }
          case ColumnType.STR: {
            console.log("$$$", record.str_filters);
            break;
          }
          default:
            throw new Error(`invalid column type ${colType}`);
        }

        return record;
      });
    },
    [updateQueryRecord],
  );

  const removeFilter = useCallback(
    (id: FilterId) => {
      dematerializeFilter(id);
      setUiFilters(uiFilters.filter((v) => v.id !== id));
    },
    [dematerializeFilter, uiFilters],
  );

  const materializeFilter = useCallback(
    (id: FilterId, filter: Filter) => {
      updateQueryRecord((record) => record);
    },
    [updateQueryRecord],
  );

  const updateFilter = useCallback(
    (id: FilterId, filter: Filter) => {
      console.log("$$$", id, filter);
      updateQueryRecord((record) => record);
    },
    [updateQueryRecord],
  );

  return {
    uiFilters,
    addFilter,
    removeFilter,
    updateFilter,
  };
}
