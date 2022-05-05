import { useContext } from "react";

import TokenizedTextField from "./TokenizedTextField";
import { ColumnInfo } from "@/columnRecord";
import nullthrows from "@/nullthrows";
import { TableContext } from "@/TableContext";
import { useQueryGroupbyCols, useQueryType } from "@/useQuerySelector";
import { setTargetCols } from "@/queryReducer";
import { useDispatch } from "react-redux";
import { QueryType } from "@/queryConsts";

export default function TargetColsSection() {
  const dispatch = useDispatch();

  const { int_columns, str_columns } = nullthrows(useContext(TableContext));
  const cols = useQueryGroupbyCols();
  const queryType = useQueryType();

  return (
    <div className="flex items-center">
      <div className="text-slate-100 font-bold mr-2">
        {queryType === QueryType.Table ? "Group by" : "Columns"}
      </div>
      <div className="flex-1">
        <TokenizedTextField
          values={cols}
          queryToValue={null}
          valueToString={(v: ColumnInfo | null) => v?.column_name ?? ""}
          setValues={(cols) => dispatch(setTargetCols(cols))}
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
