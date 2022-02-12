import axios from "axios";
import { Immutable, produce } from "immer";
import { useState } from "react";

import logo from "@/logo.svg";

import "@/App.css";

type Todo = Immutable<{
  title: Immutable<{
    title: string;
    done: boolean;
  }>;
  done: boolean;
}>;

const state0: Todo = {
  title: {
    title: "test",
    done: false,
  },
  done: false,
};

produce((draft) => {
  draft.done = !draft.done;
}, state0);

const k = produce<Todo, [boolean, string]>((draft, newState, newState2) => {
  draft.done = newState;
  draft.title.title = newState2;
});
k(state0, true, "str");

function App() {
  const [count, setCount] = useState(0);
  return (
    <div className="App font-bold underline">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>Bapi</p>
        <p>
          <button type="button" onClick={() => setCount((count) => count + 1)}>
            count is: {count}
          </button>
          <br />
          <button
            className="btn btn-blue"
            onClick={() => {
              axios
                .get("/v1/queryRows", {
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
