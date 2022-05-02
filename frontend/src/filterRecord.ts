import Immutable from "immutable";
import { ColumnType } from "./columnRecord";

import { FilterOpType } from "./queryConsts";

export type Filter = {
  column_name: string;
  column_type: ColumnType;
  filter_op: FilterOpType;
  int_vals: string[];
  str_vals: string[];
};

export class FilterRecord extends Immutable.Record<DeepRecord<Filter>>({
  column_name: null,
  column_type: null,
  filter_op: null,
  int_vals: null,
  str_vals: null,
}) {}

export function filterToFilterRecord(filter: Filter): FilterRecord {
  return new FilterRecord({
    column_name: filter.column_name,
    column_type: filter.column_type,
    filter_op: filter.filter_op,
    int_vals: Immutable.List(filter.int_vals),
    str_vals: Immutable.List(filter.str_vals),
  });
}
