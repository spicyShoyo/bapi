import dayjs from "dayjs";
import { useState } from "react";

const NOW = dayjs();
const L5M = NOW.subtract(5, "minute");
const L1H = NOW.subtract(1, "hour");
const L12H = NOW.subtract(12, "hour");
const L1D = NOW.subtract(1, "day");
const L2D = NOW.subtract(2, "day");
const L7D = NOW.subtract(7, "day");
const L14D = NOW.subtract(14, "day");
const FORMAT_STR = "YYYY-MM-DDTHH:mm";

function TimeChip(props: {
  selected: boolean;
  label: string;
  onChange: () => void;
}) {
  return (
    <button
      className={
        props.selected
          ? "bg-slate-900 text-slate-100 mx-2 px-1 rounded"
          : "bg-slate-100 text-slate-900 mx-2 px-1 rounded"
      }
      onClick={props.onChange}
    >
      {props.label}
    </button>
  );
}

function TimePicker(props: {
  label: string;
  value: number;
  // eslint-disable-next-line no-unused-vars
  onChange: (ts: number) => void;
}) {
  return (
    <div className="flex m-2 justify-between">
      <div className="text-slate-100 font-bold mr-2">{props.label}</div>
      <input
        type="datetime-local"
        style={{ width: "224px", borderRadius: "0.25rem" }}
        value={dayjs.unix(props.value).format(FORMAT_STR)}
        min={L14D.format(FORMAT_STR)}
        max={NOW.format(FORMAT_STR)}
        onChange={(e) => props.onChange(dayjs(e.target.value).unix())}
      />
    </div>
  );
}

export default function TimeRangePicker() {
  const [startTs, setStartTs] = useState(L1D.unix());
  const [endTs, setEndTs] = useState(NOW.unix());

  const chips: [string, number, number][] = [
    ["L14d", L14D.unix(), NOW.unix()],
    ["L7d", L7D.unix(), NOW.unix()],
    ["L1d", L1D.unix(), NOW.unix()],
    ["L12h", L12H.unix(), NOW.unix()],
    ["L1h", L1H.unix(), NOW.unix()],
    ["L5m", L5M.unix(), NOW.unix()],
    ["Yesterday", L2D.unix(), L1D.unix()],
    ["Last week", L14D.unix(), L7D.unix()],
  ];

  return (
    <div className="flex flex-col">
      <TimePicker label="Start" value={startTs} onChange={setStartTs} />
      <TimePicker label="End" value={endTs} onChange={setEndTs} />
      <div className="flex flex-wrap gap-y-2">
        {chips.map(([label, start, end]) => (
          <TimeChip
            key={label}
            selected={startTs === start && endTs === end}
            label={label}
            onChange={() => {
              setStartTs(start);
              setEndTs(end);
            }}
          />
        ))}
      </div>
    </div>
  );
}
