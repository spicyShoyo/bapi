/* eslint-disable react/jsx-props-no-spreading */
import { Combobox } from "@headlessui/react";
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

function useTypeahead<T>(
  query: string,
  queryToValue: ((query: string) => T) | null,
  fetchHints: (query: string) => Promise<T[] | null>,
): T[] {
  const [hint, setHint] = useState<T[]>([]);
  const queryString = query.trim();

  const latestPromiseKey = useRef(0);

  useEffect(() => {
    const queryValue =
      queryString !== "" && queryToValue != null
        ? queryToValue(queryString)
        : null;
    setHint(queryValue == null ? [] : [queryValue]);

    latestPromiseKey.current += 1;
    const promiseHandle = latestPromiseKey.current;

    fetchHints(queryString).then((hints) => {
      if (hints == null || promiseHandle !== latestPromiseKey.current) {
        return;
      }

      const queryValue =
        queryString !== "" && queryToValue != null
          ? queryToValue(queryString)
          : null;
      setHint(queryValue == null ? hints : [queryValue, ...hints]);
    });
  }, [queryString, fetchHints, queryToValue]);
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

export default function TokenizedTextField<T>({
  values,
  queryToValue,
  valueToString,
  setValues,
  fetchHints,
}: {
  values: T[];
  queryToValue: ((query: string) => T) | null; // null means only ones from hints is selectable
  valueToString: (_: T | null) => string;
  setValues: (_: T[]) => void;
  fetchHints: (query: string) => Promise<T[] | null>;
}) {
  const [query, setQuery] = useState("");
  const typeaheadValues = useTypeahead(query, queryToValue, fetchHints);

  const onSelect = useCallback(
    (value) => {
      setQuery("");
      if (values.includes(value)) {
        return;
      }
      setValues([...values, value]);
    },
    [setValues, setQuery],
  );

  const onRemove = useCallback(
    (value) => {
      setValues(values.filter((val) => valueToString(val) !== value));
    },
    [values, setValues, valueToString],
  );

  return (
    <TokenContext.Provider
      value={useMemo(
        () => ({
          selectedValues: values.map(valueToString),
          query,
          onRemove,
        }),
        [values, valueToString, query, onRemove],
      )}
    >
      <Combobox value="" onChange={onSelect}>
        <div className="flex flex-col">
          <div className="text-left bg-white rounded-md shadow-md overflow-hidden">
            <Combobox.Input
              as={TextBox}
              displayValue={valueToString}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
          <div className="relative">
            <div className="absolute w-full">
              <Combobox.Options className="py-1 bg-white rounded-md shadow-lg max-h-60 focus:outline-none sm:text-sm">
                {typeaheadValues.map((val, i) => (
                  <Combobox.Option
                    key={i}
                    className={({ active }) =>
                      `cursor-default select-none py-2 pl-4 pr-4 ${
                        active ? "text-white bg-teal-600" : "text-gray-900"
                      }`
                    }
                    value={val}
                  >
                    {valueToString(val)}
                  </Combobox.Option>
                ))}
              </Combobox.Options>
            </div>
          </div>
        </div>
      </Combobox>
    </TokenContext.Provider>
  );
}
