import React, { useCallback, useContext, useRef, useState } from "react";

import FilterField from "./FilterField";
import { Filter } from "@/queryConsts";
import { QueryContext } from "@/QueryContext";

export default function FilterFields() {
  const lastId = useRef(0);
  const [fieldIds, setFieldIds] = useState([lastId.current]);
  const { filterMap } = useContext(QueryContext);

  const addField = useCallback(() => {
    lastId.current += 1;
    setFieldIds([...fieldIds, lastId.current]);
  }, [lastId, fieldIds, setFieldIds]);

  const removeField = useCallback(
    (id: number) => {
      if (fieldIds.length === 1) {
        return;
      }
      setFieldIds(fieldIds.filter((fieldId) => fieldId !== id));
      filterMap.delete(id);
    },
    [setFieldIds, fieldIds, filterMap],
  );

  return (
    <div className="flex flex-col">
      {fieldIds.map((fieldId) => (
        <FilterField
          key={fieldId}
          onAdd={(ref: React.MutableRefObject<Filter>) => {
            filterMap.set(fieldId, ref);
          }}
          onRemove={() => removeField(fieldId)}
        />
      ))}
      <button
        className="text-slate-100 bg-slate-700 mx-2 py-1 mt-2 rounded font-bold"
        onClick={addField}
      >
        <b>+ Add Filter</b>
      </button>
    </div>
  );
}
