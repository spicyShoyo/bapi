import { useContext } from "react";

import TokenizedTextField from "./TokenizedTextField";
import { ColumnInfo } from "@/columnRecord";
import nullthrows from "@/nullthrows";
import { TableContext } from "@/TableContext";
import useQuerySelector from "@/useQuerySelector";
import { setGroupbyCols } from "@/queryReducer";
import { useDispatch } from "react-redux";

export default function GroupbySection() {
  const dispatch = useDispatch();

  const { int_columns, str_columns } = nullthrows(useContext(TableContext));
  const cols = useQuerySelector((r) => [
    ...(r.groupby_str_columns ?? []),
    ...(r.groupby_int_columns ?? []),
  ]);

  return (
    <div className="mt-3 m-2 flex items-center">
      <div className="text-slate-100 font-bold mr-2">Group by</div>
      <div className="flex-1">
        <TokenizedTextField
          // @ts-ignore TODO: fix typing
          initValues={cols}
          queryToValue={null}
          valueToString={(v: ColumnInfo | null) => v?.column_name ?? ""}
          setValues={(cols) => dispatch(setGroupbyCols(cols))}
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
