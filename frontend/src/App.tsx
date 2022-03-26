import TimeRangePicker from "@/components/TimeRangePicker";

function App() {
  return (
    <div className="flex flex-col bg-slate-800 h-screen w-screen">
      <div className="flex h-[36px]">
        <div className="flex items-center select-none text-white text-lg pl-4 font-logo cursor-pointer">
          Bapi
        </div>
      </div>
      <div className="flex flex-1">
        <div className="w-[288px] h-full bg-slate-600 ">
          <TimeRangePicker />
        </div>
        <div className="flex-1 h-full bg-slate-700" />
      </div>
    </div>
  );
}

export default App;
