/* eslint-disable react/jsx-props-no-spreading */
/* eslint-disable no-unused-vars */
import { Popover, Combobox, Listbox } from "@headlessui/react";
import React, {
  ForwardedRef,
  InputHTMLAttributes,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import { ColumnInfo, ColumnType } from "@/TableContext";

function useTypeahead(
  query: string,
  strict: boolean,
  fetchHints: (query: string) => Promise<string[] | null>,
): string[] {
  const [hint, setHint] = useState<string[]>([]);
  const queryString = query.trim();

  const latestPromiseKey = useRef(0);

  useEffect(() => {
    setHint(queryString === "" || strict ? [] : [queryString]);

    latestPromiseKey.current += 1;
    const promiseHandle = latestPromiseKey.current;

    fetchHints(queryString).then((hints) => {
      if (hints == null || promiseHandle !== latestPromiseKey.current) {
        return;
      }
      setHint(queryString === "" ? hints : [queryString, ...hints]);
    });
  }, [queryString, fetchHints, strict]);
  return hint;
}

const TokenContext = React.createContext<{
  selectedValues: string[];
  query: string;
  onRemove: (val: string) => void;
}>({
  selectedValues: [],
  query: "",
  onRemove: () => {},
});

const TextBox = React.forwardRef(
  (
    props: InputHTMLAttributes<HTMLInputElement>,
    ref: ForwardedRef<HTMLInputElement>,
  ) => {
    const { selectedValues, query, onRemove } = useContext(TokenContext);
    return (
      // eslint-disable-next-line jsx-a11y/no-static-element-interactions
      <div
        className="flex flex-wrap gap-1 m-1"
        onKeyDown={(e) => {
          if (
            e.key !== "Backspace" ||
            selectedValues.length === 0 ||
            query !== ""
          ) {
            return;
          }
          onRemove(selectedValues[selectedValues.length - 1]);
        }}
      >
        {selectedValues.map((val) => (
          <button
            className="bg-slate-500 rounded px-2 text-slate-200"
            key={val}
            onClick={() => onRemove(val)}
          >
            {val}
          </button>
        ))}
        <input
          className="pl-1 text-gray-900 w-full focus:outline-none flex-1"
          ref={ref}
          autoComplete="off"
          {...props}
        />
      </div>
    );
  },
);

export default function TokenizedTextField({
  strict,
  setValues,
  fetchHints,
}: {
  strict: boolean;
  setValues: (_: string[]) => void;
  fetchHints: (query: string) => Promise<string[] | null>;
}) {
  const [query, setQuery] = useState("");
  const typeaheadValues = useTypeahead(query, strict, fetchHints);

  const [selectedValues, setSelectedValues] = useState<string[]>([]);
  const onSelect = useCallback(
    (value) => {
      setQuery("");
      if (selectedValues.includes(value)) {
        return;
      }
      const newValues = [...selectedValues, value];
      setSelectedValues(newValues);
      setValues(newValues);
    },
    [selectedValues, setValues, setQuery, setSelectedValues],
  );

  const onRemove = useCallback(
    (value) => {
      const newValues = selectedValues.filter((val) => val !== value);
      setSelectedValues(selectedValues.filter((val) => val !== value));
      setValues(newValues);
    },
    [selectedValues, setValues, setSelectedValues],
  );

  return (
    <TokenContext.Provider
      value={useMemo(
        () => ({ selectedValues, query, onRemove }),
        [selectedValues, query, onRemove],
      )}
    >
      <Combobox value="" onChange={onSelect}>
        <div className="flex flex-col">
          <div className="text-left bg-white rounded-lg shadow-md overflow-hidden w-full">
            <Combobox.Input
              as={TextBox}
              displayValue={(col: string) => col}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
          <div className="absolute mt-11 w-[272px]">
            <Combobox.Options className="py-1 bg-white rounded-md shadow-lg max-h-60 focus:outline-none sm:text-sm">
              {typeaheadValues.map((val) => (
                <Combobox.Option
                  key={val}
                  className={({ active }) =>
                    `cursor-default select-none py-2 pl-4 pr-4 ${
                      active ? "text-white bg-teal-600" : "text-gray-900"
                    }`
                  }
                  value={val}
                >
                  {val}
                </Combobox.Option>
              ))}
            </Combobox.Options>
          </div>
        </div>
      </Combobox>
    </TokenContext.Provider>
  );
}
