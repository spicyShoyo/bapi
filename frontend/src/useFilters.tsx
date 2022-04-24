import { useCallback, useRef, useState } from "react";

import { Filter, FilterRecord } from "./filterRecord";
import { ColumnType } from "@/columnRecord";
import { UpdateFn } from "@/QueryContext";
import QueryRecord from "./queryRecord";

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

const EMPTY_STATE: [Map<FilterId, [ColumnType, number]>, UiFilter[]] = [
  new Map(),
  [],
];

function initFromRecord(
  lastFilterId: React.MutableRefObject<number>,
  initRecord: QueryRecord,
): [Map<FilterId, [ColumnType, number]>, UiFilter[]] {
  if (lastFilterId.current !== 0) {
    // The return value is only used once at first render
    return EMPTY_STATE;
  }

  const materialized = new Map();
  const uiFilters: UiFilter[] = [];
  (initRecord.str_filters ?? []).forEach((filter, idx) => {
    lastFilterId.current++;
    const id = lastFilterId.current;
    uiFilters.push({
      id,
      // @ts-ignore: TODO: fix typing
      filter,
    });
    materialized.set(lastFilterId.current, [ColumnType.STR, idx]);
  });
  (initRecord.int_filters ?? []).forEach((filter, idx) => {
    lastFilterId.current++;
    const id = lastFilterId.current;
    materialized.set(lastFilterId.current, [ColumnType.INT, idx]);
    uiFilters.push({
      id,
      // @ts-ignore TODO: fix typing
      filter,
    });
  });
  return [
    materialized,
    uiFilters.length === 0
      ? [
          {
            id: lastFilterId.current,
            filter: null,
          },
        ]
      : uiFilters,
  ];
}

function useFilterUISpec(
  initRecord: QueryRecord,
): [
  React.MutableRefObject<number>,
  React.MutableRefObject<Map<FilterId, [ColumnType, number]>>,
  UiFilter[],
  (filters: UiFilter[]) => void,
] {
  const lastFilterId = useRef<FilterId>(0);
  const [initMaterialized, initUiFilters] = initFromRecord(
    lastFilterId,
    initRecord,
  );
  // TODO: init from URL
  const materialized =
    useRef<Map<FilterId, [ColumnType, number]>>(initMaterialized);
  const [uiFilters, setUiFilters] = useState<UiFilter[]>(initUiFilters);
  return [lastFilterId, materialized, uiFilters, setUiFilters];
}

export default function useFilters(
  initRecord: QueryRecord,
  updateQueryRecord: (fn: UpdateFn) => void,
): FiltersManager {
  const [lastFilterId, materialized, uiFilters, setUiFilters] =
    useFilterUISpec(initRecord);

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
