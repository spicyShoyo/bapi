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

function arrayMatch<T>(arr1: T[] | null, arr2: T[] | null): boolean {
  return (
    arr1 != null &&
    arr2 != null &&
    arr1.length === arr2.length &&
    arr1.every((item, idx) => item === arr2[idx])
  );
}

export class FilterRecord extends Immutable.Record<DeepRecord<Filter>>({
  column_name: null,
  column_type: null,
  filter_op: null,
  int_vals: null,
  str_vals: null,
}) {
  static fromFilter(filter: Filter): FilterRecord {
    return new FilterRecord({
      column_name: filter.column_name,
      filter_op: filter.filter_op,
      int_vals: Immutable.List(filter.int_vals),
      str_vals: Immutable.List(filter.str_vals),
    });
  }

  static filtersMatch(
    filter1: Filter | null,
    filters2: Filter | null,
  ): boolean {
    if (filter1 == null && filters2 == null) {
      return true;
    }
    if (filter1 == null || filters2 == null) {
      return false;
    }
    return (
      filter1.column_name === filters2.column_name &&
      filter1.column_type === filters2.column_type &&
      filter1.filter_op === filters2.filter_op &&
      arrayMatch(filter1.int_vals, filters2.int_vals) &&
      arrayMatch(filter1.str_vals, filters2.str_vals)
    );
  }

  toFilter(): Filter | null {
    return this.isValid()
      ? {
          column_name: this.column_name!,
          filter_op: this.filter_op!,
          column_type: this.column_type!,
          int_vals: this.int_vals?.toJSON() ?? [],
          str_vals: this.str_vals?.toJSON() ?? [],
        }
      : null;
  }

  static isValidFilter(filter: Filter): boolean {
    return filter.int_vals.length !== 0 || filter.str_vals.length !== 0;
  }

  isValid(): boolean {
    return (
      this.column_name != null &&
      this.column_type != null &&
      this.filter_op != null &&
      (this.int_vals?.size !== 0 || this.str_vals?.size !== 0)
    );
  }

  isEqual(filter: Filter): boolean {
    return (
      this.column_name === filter.column_name &&
      this.column_type === filter.column_type &&
      this.filter_op === filter.filter_op &&
      Immutable.is(this.int_vals, Immutable.List(filter.int_vals)) &&
      Immutable.is(this.str_vals, Immutable.List(filter.str_vals))
    );
  }
}
