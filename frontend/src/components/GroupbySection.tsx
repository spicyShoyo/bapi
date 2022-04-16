import { useContext } from "react";

import TokenizedTextField from "./TokenizedTextField";
import nullthrows from "@/nullthrows";
import { QueryContext } from "@/QueryContext";
import { TableContext } from "@/TableContext";

export default function GroupbySection() {
  const { int_columns, str_columns } = nullthrows(useContext(TableContext));
  const { setGroupbyCols } = useContext(QueryContext);
  return (
    <div className="mt-3 m-2 flex items-center">
      <div className="text-slate-100 font-bold mr-2">Group by</div>
      <div className="flex-1">
        <TokenizedTextField
          strict
          setValues={setGroupbyCols}
          fetchHints={(query) =>
            Promise.resolve(
              [...nullthrows(str_columns), ...nullthrows(int_columns)]
                .map((col) => col.column_name)
                .filter((col) => query === "" || col.includes(query)),
            )
          }
        />
      </div>
    </div>
  );
}
