import axios from "axios";
import { useState } from "react";

function App() {
  const [count, setCount] = useState(0);
  return (
    <div className="text-center">
      <header
        className="
      bg-sky-900	min-h-screen flex items-center justify-center flex-col text-white text-lg"
      >
        <p>Bapi</p>
        <p>
          <button type="button" onClick={() => setCount((count) => count + 1)}>
            count is: {count}
          </button>
          <br />
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
      </header>
    </div>
  );
}

export default App;
