import { useContext } from "react";

import TokenizedTextField from "./TokenizedTextField";
import nullthrows from "@/nullthrows";
import { QueryContext } from "@/QueryContext";
import { ColumnInfo, TableContext } from "@/TableContext";

export default function GroupbySection() {
  const { int_columns, str_columns } = nullthrows(useContext(TableContext));
  const { setGroupbyCols } = useContext(QueryContext);
  return (
    <div className="mt-3 m-2 flex items-center">
      <div className="text-slate-100 font-bold mr-2">Group by</div>
      <div className="flex-1">
        <TokenizedTextField
          queryToValue={null}
          valueToString={(v: ColumnInfo | null) => v?.column_name ?? ""}
          setValues={setGroupbyCols}
          fetchHints={(query) =>
            Promise.resolve(
              [...nullthrows(str_columns), ...nullthrows(int_columns)].filter(
                (col) => query === "" || col.column_name.includes(query),
              ),
            )
          }
        />
      </div>
    </div>
  );
}
