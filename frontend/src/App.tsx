import axios from "axios";
import tw from "twin.macro";

const Header = tw.div`
h-[32px] text-white text-lg text-left pl-4 font-logo
`;
const SidePane = tw.div`
w-[324px]  bg-blue-600 min-h-full
`;
const ResultView = tw.div`
 bg-blue-300 flex-grow min-h-full
`;

function App() {
  return (
    <div className="text-center bg-sky-900	min-h-screen  flex flex-col">
      <Header>Bapi</Header>
      <div className="min-w-screen flex grow">
        <SidePane>
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
        </SidePane>
        <ResultView />
      </div>
    </div>
  );
}

export default App;
