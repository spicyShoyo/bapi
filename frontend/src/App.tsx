import { HashRouter } from "react-router-dom";

import FiltersSection from "@/components/FiltersSection";
import TargetColsSection from "@/components/TargetColsSection";
import TimeRangeSection from "@/components/TimeRangeSection";
import { QueryContext, QueryContextProvider } from "@/QueryContext";
import { TableContext, TableContextProvider } from "@/TableContext";
import { Provider, useDispatch } from "react-redux";
import AggregateSection from "@/components/AggregateSection";
import queryStore from "@/queryStore";
import { TableQueryResultTable } from "@/components/TableQueryResultTable";
import { QueryType } from "@/queryConsts";
import { useQueryType } from "@/useQuerySelector";
import { setQueryType } from "@/queryReducer";
import { useContext } from "react";
import { RowsQueryResultTable } from "./components/RowsQueryResultTable";
import dayjs from "dayjs";

function classNames(...classes: string[]) {
  return classes.filter(Boolean).join(" ");
}

function FooterItem({
  label,
  value,
}: {
  label: string;
  value: string | number | null;
}) {
  return (
    <div className="flex align-middle text-slate-100 justify-between">
      <b>{label}:</b>
      <code className="inline text-lg">{value}</code>
    </div>
  );
}

export const TS_FORMAT_STR = "YYYY-MM-DD HH:mm";
function Footer() {
  const tableInfo = useContext(TableContext);
  if (tableInfo == null) {
    return null;
  }
  return (
    <div className="flex flex-col border-t-2 pt-2 border-slate-500">
      <FooterItem
        label="Start"
        value={dayjs.unix(tableInfo.min_ts).format(TS_FORMAT_STR)}
      />
      {tableInfo.max_ts != null ? (
        <FooterItem
          label="End"
          value={dayjs.unix(tableInfo.max_ts).format(TS_FORMAT_STR)}
        />
      ) : null}
      <FooterItem label="Rows" value={tableInfo.row_count} />
    </div>
  );
}

function RunQueryButton() {
  const tableInfo = useContext(TableContext);
  const { runQuery } = useContext(QueryContext);
  return (
    <div className="flex justify-between">
      <p className="text-slate-100">
        {tableInfo == null ? (
          "N/A"
        ) : (
          <code className="inline">{tableInfo.table_name}</code>
        )}
      </p>
      <button
        className="bg-green-600 text-slate-100 rounded px-1"
        onClick={runQuery}
      >
        <b>Dive</b>
      </button>
    </div>
  );
}

function QueryTypeSwitch() {
  const d = useDispatch();
  const selectedQueryType = useQueryType();
  const queryTypes = [
    {
      queryType: QueryType.Rows,
      text: "Rows",
    },
    {
      queryType: QueryType.Table,
      text: "Table",
    },
    {
      queryType: QueryType.Timeline,
      text: "Timeline",
      disabled: true,
    },
  ];
  return (
    <div className="border-b-2 pb-4 border-slate-500">
      <div className="flex space-x-1 rounded-lg bg-slate-700">
        {queryTypes.map(({ text, queryType, disabled }) => (
          <button
            key={text}
            disabled={disabled === true}
            className={classNames(
              "w-full rounded-lg py-2",
              selectedQueryType === queryType
                ? "bg-slate-100 shadow text-slate-700"
                : disabled === true
                ? "text-slate-300 cursor-not-allowed"
                : "text-slate-100 hover:bg-white/[0.12]",
            )}
            onClick={() => d(setQueryType(queryType))}
          >
            {text}
          </button>
        ))}
      </div>
    </div>
  );
}

function QueryResult() {
  const queryType = useQueryType();
  switch (queryType) {
    case QueryType.Table:
      return <TableQueryResultTable />;
    case QueryType.Rows:
      return <RowsQueryResultTable />;
    default:
      return null;
  }
}

function AggregateSectionWrapper() {
  return useQueryType() === QueryType.Rows ? null : <AggregateSection />;
}

function App() {
  return (
    <HashRouter>
      <TableContextProvider table="test_table">
        <Provider store={queryStore}>
          <QueryContextProvider>
            <div className="flex flex-col bg-slate-800 h-screen w-screen">
              <div className="flex h-[36px]">
                <div className="flex items-center select-none text-slate-100 text-lg pl-4 font-logo cursor-pointer">
                  Bapi
                </div>
              </div>
              <div className="flex flex-1">
                <div className="flex flex-col justify-between w-[312px] h-full bg-slate-600 p-4">
                  <div className="flex flex-col gap-4">
                    <RunQueryButton />
                    <QueryTypeSwitch />
                    <TimeRangeSection />
                    <AggregateSectionWrapper />
                    <TargetColsSection />
                    <FiltersSection />
                  </div>
                  <Footer />
                </div>
                <div className="flex-1 h-full bg-slate-700">
                  <QueryResult />
                </div>
              </div>
            </div>
          </QueryContextProvider>
        </Provider>
      </TableContextProvider>
    </HashRouter>
  );
}

export default App;
