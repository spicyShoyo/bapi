import { useCallback, useRef, useState } from "react";

import FilterField from "./FilterField";

export default function FilterFields() {
  const lastId = useRef(0);
  const [fieldIds, setFieldIds] = useState([lastId.current]);

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
    },
    [setFieldIds, fieldIds],
  );

  return (
    <div className="flex flex-col">
      {fieldIds.map((fieldId) => (
        <FilterField key={fieldId} onRemove={() => removeField(fieldId)} />
      ))}
      <button className="text-slate-100 bg-slate-700 mx-2 py-1 mt-2 rounded font-bold">
        <b role="button" tabIndex={0} onClick={addField} onKeyDown={addField}>
          + Add Filter
        </b>
      </button>
    </div>
  );
}
