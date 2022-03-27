import { HashRouter } from "react-router-dom";

import FilterField from "@/components/FilterField";
import TimeRangePicker from "@/components/TimeRangePicker";
import { QueryContextProvider } from "@/QueryContext";
import { TableContextProvider } from "@/TableContext";

function App() {
  return (
    <HashRouter>
      <TableContextProvider>
        <QueryContextProvider>
          <div className="flex flex-col bg-slate-800 h-screen w-screen">
            <div className="flex h-[36px]">
              <div className="flex items-center select-none text-white text-lg pl-4 font-logo cursor-pointer">
                Bapi
              </div>
            </div>
            <div className="flex flex-1">
              <div className="w-[288px] h-full bg-slate-600 ">
                <TimeRangePicker />
                <FilterField />
                <FilterField />
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
