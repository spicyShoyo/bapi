import dayjs from "dayjs";

import { getPropsForTimeRange, TimeRange, TimeRanges } from "@/tsConsts";
import { useDispatch } from "react-redux";
import useQuerySelector, { useQueryTs } from "@/useQuerySelector";
import { setTsRange } from "@/queryReducer";

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
  onChange: (ts: number) => void;
}) {
  const [_, min, max] = getPropsForTimeRange(TimeRange.l14d);
  return (
    <div className="flex m-2 justify-between">
      <div className="text-slate-100 font-bold mr-2">{props.label}</div>
      <input
        type="datetime-local"
        style={{ width: "224px", borderRadius: "0.25rem" }}
        value={dayjs.unix(props.value).format(FORMAT_STR)}
        min={dayjs.unix(min).format(FORMAT_STR)}
        max={dayjs.unix(max).format(FORMAT_STR)}
        onChange={(e) => props.onChange(dayjs(e.target.value).unix())}
      />
    </div>
  );
}

export default function TimeRangeSection() {
  const dispatch = useDispatch();
  const { ts_range, min_ts, max_ts } = useQueryTs();

  const setTs = (payload: { maxTs?: number; minTs?: number }) =>
    dispatch(setTsRange(payload));
  const [_, defaultStartTs, defaultEndTs] = getPropsForTimeRange(TimeRange.l1d);

  return (
    <div className="flex flex-col">
      <TimePicker
        label="Start"
        value={min_ts ?? defaultStartTs}
        onChange={(minTs) => setTs({ minTs })}
      />
      <TimePicker
        label="End"
        value={max_ts ?? defaultEndTs}
        onChange={(maxTs) => setTs({ maxTs })}
      />
      <div className="flex flex-wrap gap-y-2">
        {TimeRanges.map((timeRange) => {
          const [label, minTs, maxTs] = getPropsForTimeRange(timeRange);
          return (
            <TimeChip
              key={label}
              selected={timeRange === ts_range}
              label={label}
              onChange={() => dispatch(setTsRange({ timeRange, minTs, maxTs }))}
            />
          );
        })}
      </div>
    </div>
  );
}
