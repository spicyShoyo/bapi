import { HashRouter } from "react-router-dom";

import FiltersSection from "./components/FiltersSection";
import GroupbySection from "./components/GroupbySection";
import TimeRangeSection from "@/components/TimeRangeSection";
import { QueryContextProvider } from "@/QueryContext";
import { TableContextProvider } from "@/TableContext";

function App() {
  return (
    <HashRouter>
      <TableContextProvider table="test_table">
        <QueryContextProvider>
          <div className="flex flex-col bg-slate-800 h-screen w-screen">
            <div className="flex h-[36px]">
              <div className="flex items-center select-none text-white text-lg pl-4 font-logo cursor-pointer">
                Bapi
              </div>
            </div>
            <div className="flex flex-1">
              <div className="w-[312px] h-full bg-slate-600 ">
                <TimeRangeSection />
                <GroupbySection />
                <FiltersSection />
              </div>
              <div className="flex-1 h-full bg-slate-700" />
            </div>
          </div>
        </QueryContextProvider>
      </TableContextProvider>
    </HashRouter>
  );
}

export default App;
