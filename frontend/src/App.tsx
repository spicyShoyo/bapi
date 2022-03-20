import axios from "axios";

function App() {
  return (
    <div className="text-center bg-sky-900	min-h-screen  flex flex-col">
      <div className="h-[32px] text-white text-lg text-left pl-4 font-logo">
        Bapi
      </div>
      <div className="min-w-screen flex grow">
        <div className="w-[324px]  bg-blue-600 min-h-full">
          <p>
            <button
              className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-1 px-2 rounded"
              onClick={() => {
                const useProd = new URL(
                  document.location.toString(),
                ).searchParams.has("prod");
                const path = useProd ? "https://bapi.io" : "";
                axios
                  .get(`${path}/v1/queryRows`, {
                    params: {
                      q: {
                        min_ts: 1641672504,
                        int_column_names: ["ts"],
                        str_column_names: ["event"],
                      },
                    },
                  })
                  .then((v) => {
                    // eslint-disable-next-line
                    console.log(v);
                  })
                  // eslint-disable-next-line
                  .catch((e) => console.log(e));
              }}
            >
              Load
            </button>
          </p>
        </div>
        <div className="bg-blue-300 flex-grow min-h-full" />
      </div>
    </div>
  );
}

export default App;
