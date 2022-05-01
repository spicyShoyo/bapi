import { HashRouter } from "react-router-dom";

import FiltersSection from "./components/FiltersSection";
import GroupbySection from "./components/GroupbySection";
import TimeRangeSection from "@/components/TimeRangeSection";
import { QueryContext, QueryContextProvider } from "@/QueryContext";
import { TableContext, TableContextProvider } from "@/TableContext";
import { Provider, useDispatch } from "react-redux";
import AggregateSection from "./components/AggregateSection";
import queryStore from "./queryStore";
import { TableQueryResult } from "./components/TableQueryResult";
import { QueryType } from "./queryConsts";
import { useQueryType } from "./useQuerySelector";
import { setQueryType } from "./queryReducer";
import { useContext } from "react";

function classNames(...classes: any[]) {
  return classes.filter(Boolean).join(" ");
}

function RunQueryButton() {
  const tableInfo = useContext(TableContext);
  const { runQuery } = useContext(QueryContext);
  return (
    <div className="flex justify-between m-2">
      <div className="text-white">{tableInfo?.table_name ?? "N/A"}</div>
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
                  <AggregateSection />
                  <GroupbySection />
                  <FiltersSection />
                </div>
                <div className="flex-1 h-full bg-slate-700">
                  <TableQueryResult />
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
