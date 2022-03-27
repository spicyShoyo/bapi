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

function getFilterOpStr(filterOp: FilterOpType): string {
  switch (filterOp) {
    case FilterOp.EQ:
      return "\u003d";
    case FilterOp.NE:
      return "\u2260";
    case FilterOp.LT:
      return "\u003c";
    case FilterOp.GT:
      return "\u003e";
    case FilterOp.LE:
      return "\u2264";
    case FilterOp.GE:
      return "\u2265";
    case FilterOp.NONNULL:
      return "nonnull";
    case FilterOp.NULL:
      return "null";
    default: {
      const _: never = filterOp;
      return "";
    }
  }
}

type Filter = {
  column_name: string;
  filter_op: FilterOpType;
  int_val: number | null;
  str_val: string | null;
};

export { QueryType, FilterOp, QueryUrlPath, getFilterOpStr };

export type { FilterOpType, Filter };
