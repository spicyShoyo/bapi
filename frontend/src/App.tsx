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

function classNames(...classes: string[]) {
  return classes.filter(Boolean).join(" ");
}

function RunQueryButton() {
  const tableInfo = useContext(TableContext);
  const { runQuery } = useContext(QueryContext);
  return (
    <div className="flex justify-between m-2">
      <p className="text-white">
        {tableInfo == null ? (
          "N/A"
        ) : (
          <>
            {"Table: "}
            <code className="inline">{tableInfo.table_name}</code>
            {" | Rows: "}
            <code className="inline">{tableInfo.row_count}</code>
          </>
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
    },
  ];
  return (
    <div className="w-full max-w-md px-2  sm:px-0">
      <div className="flex space-x-1 rounded-xl bg-blue-900/20 p-1">
        {queryTypes.map(({ text, queryType }) => (
          <button
            key={text}
            className={classNames(
              "w-full rounded-lg py-2.5 text-sm font-medium leading-5 text-blue-700",
              selectedQueryType === queryType
                ? "bg-white shadow"
                : "text-blue-100 hover:bg-white/[0.12] hover:text-white",
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
                <div className="flex items-center select-none text-white text-lg pl-4 font-logo cursor-pointer">
                  Bapi
                </div>
              </div>
              <div className="flex flex-1">
                <div className="w-[312px] h-full bg-slate-600 ">
                  <RunQueryButton />
                  <QueryTypeSwitch />
                  <TimeRangeSection />
                  <AggregateSectionWrapper />
                  <TargetColsSection />
                  <FiltersSection />
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
