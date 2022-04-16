import dayjs from "dayjs";
import { useContext, useEffect, useState } from "react";

import { QueryContext } from "@/QueryContext";
import { NOW, L5M, L1H, L12H, L1D, L2D, L7D, L14D } from "@/tsConsts";

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

export default function TimeRangeSection() {
  const { queryRecord, updateQueryRecord } = useContext(QueryContext);
  const [startTs, setStartTs] = useState(queryRecord.min_ts ?? L1D.unix());
  const [endTs, setEndTs] = useState(queryRecord.max_ts ?? NOW.unix());

  useEffect(() => {
    updateQueryRecord((currentRecord) =>
      currentRecord.set("min_ts", startTs).set("max_ts", endTs),
    );
  }, [updateQueryRecord, startTs, endTs]);

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
