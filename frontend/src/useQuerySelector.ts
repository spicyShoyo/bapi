import Immutable from "immutable";
import { useSelector } from "react-redux";
import QueryRecord from "./queryRecord";

export default function useQuerySelector<T>(
  selectFn: (record: QueryRecord) => T,
) {
  return useSelector(selectFn, Immutable.is);
}
