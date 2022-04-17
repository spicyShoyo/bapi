import { Listbox } from "@headlessui/react";

export function Dropdown<T>(props: {
  selected: T;
  values: T[];
  setSelected: (val: T) => void;
  valToString: (val: T) => string;
}) {
  return (
    <div className="flex">
      <Listbox value={props.selected} onChange={props.setSelected}>
        <Listbox.Button className="text-slate-100 bg-slate-700 p-1 rounded font-bold text-center">
          {props.valToString(props.selected)}
        </Listbox.Button>
        <Listbox.Options className="absolute mt-8 p-2 bg-white rounded overflow-hidden">
          {props.values.map((val) => (
            <Listbox.Option
              className={({ active }) =>
                `cursor-pointer select-none rounded text-center ${
                  active ? "text-white bg-teal-600" : "text-gray-900"
                }`
              }
              key={props.valToString(val)}
              value={val}
            >
              <b>{props.valToString(val)}</b>
            </Listbox.Option>
          ))}
        </Listbox.Options>
      </Listbox>
    </div>
  );
}
