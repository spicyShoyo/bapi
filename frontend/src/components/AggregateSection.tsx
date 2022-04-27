import { useContext } from "react";

import TokenizedTextField from "./TokenizedTextField";
import { ColumnInfo } from "@/columnRecord";
import nullthrows from "@/nullthrows";
import { TableContext } from "@/TableContext";
import { Dropdown } from "./Dropdown";
import { AggOp, getAggOpStr } from "@/queryConsts";
import { setAggOp, setAggregateCols } from "@/queryReducer";
import useQuerySelector, { useQueryAggCols } from "@/useQuerySelector";
import { useDispatch } from "react-redux";

export default function AggregateSection() {
  const dispatch = useDispatch();

  const { int_columns } = nullthrows(useContext(TableContext));
  const aggOp = useQuerySelector((r) => r.agg_op);
  const aggCols = useQueryAggCols();

  return (
    <div className="mt-3 m-2 flex items-center gap-2">
      <div className="text-slate-100 font-bold">Aggregate</div>
      <Dropdown
        values={[AggOp.COUNT, AggOp.COUNT_DISTINCT, AggOp.SUM, AggOp.AVG]}
        valToString={getAggOpStr}
        selected={aggOp ?? AggOp.COUNT}
        setSelected={(op) => dispatch(setAggOp(op))}
      />
      <div className="flex-1">
        <TokenizedTextField
          values={aggCols}
          queryToValue={null}
          valueToString={(v: ColumnInfo | null) => v?.column_name ?? ""}
          setValues={(cols) => dispatch(setAggregateCols(cols))}
          fetchHints={(query) =>
            Promise.resolve(
              nullthrows(int_columns).filter(
                (col) => query === "" || col.column_name.includes(query),
              ),
            )
          }
        />
      </div>
    </div>
  );
}
