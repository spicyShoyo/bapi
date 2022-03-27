/* eslint-disable no-unused-vars */
// keep in sync with bapi.proto

enum QueryType {
  Rows,
  Table,
  Timeline,
}

enum QueryUrlPath {
  ROWS = "/rows",
}

const FilterOp = {
  EQ: 0,
  NE: 1,
  LT: 2,
  GT: 3,
  LE: 4,
  GE: 5,
  NONNULL: 6,
  NULL: 7,
} as const;

type FilterOpType = typeof FilterOp[keyof typeof FilterOp];

type Filter = {
  column_name: string;
  filter_op: FilterOpType;
  int_val: number | null;
  str_val: string | null;
};

export { QueryType, FilterOp, QueryUrlPath };

export type { FilterOpType, Filter };
