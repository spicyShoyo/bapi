import { useContext, useEffect, useState } from "react";

import TokenizedTextField from "./TokenizedTextField";
import { ColumnInfo } from "@/columnRecord";
import nullthrows from "@/nullthrows";
import { QueryContext } from "@/QueryContext";
import { TableContext } from "@/TableContext";
import { Dropdown } from "./Dropdown";
import { AggOp, AggOpType, getAggOpStr } from "@/queryConsts";

export default function AggregateSection() {
  const { int_columns } = nullthrows(useContext(TableContext));
  const { setAggOp, setAggregateCols } = useContext(QueryContext);

  const [aggOp, setLocalAddOp] = useState<AggOpType>(AggOp.COUNT);
  useEffect(() => setAggOp(aggOp), [aggOp, setAggOp]);

  return (
    <div className="mt-3 m-2 flex items-center gap-2">
      <div className="text-slate-100 font-bold">Aggregate</div>
      <Dropdown
        values={[AggOp.COUNT, AggOp.COUNT_DISTINCT, AggOp.SUM, AggOp.AVG]}
        valToString={getAggOpStr}
        selected={aggOp}
        setSelected={setLocalAddOp}
      />
      <div className="flex-1">
        <TokenizedTextField
          queryToValue={null}
          valueToString={(v: ColumnInfo | null) => v?.column_name ?? ""}
          setValues={setAggregateCols}
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
