export enum ColumnType {
  NONE = 0,
  INT = 1,
  STR = 2,
}

export type ColumnInfo = {
  column_name: string;
  column_type: ColumnType;
};

export type Filter = {
  column_name: string;
  column_type: ColumnType;
  filter_op: FilterOpType;
  int_vals: string[];
  str_vals: string[];
};

// keep in sync with bapi.proto
enum QueryType {
  Rows,
  Table,
  Timeline,
}

enum QueryUrlPath {
  Rows = "/rows",
  Table = "/table",
  Timeline = "/timeline",
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

const AggOp = {
  COUNT: 0,
  COUNT_DISTINCT: 1,
  SUM: 2,
  AVG: 3,
} as const;

type FilterOpType = typeof FilterOp[keyof typeof FilterOp];
type AggOpType = typeof AggOp[keyof typeof AggOp];

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

function getAggOpStr(aggOp: AggOpType): string {
  switch (aggOp) {
    case AggOp.COUNT:
      return "Cnt";
    case AggOp.COUNT_DISTINCT:
      return "Cnt Dist";
    case AggOp.SUM:
      return "Sum";
    case AggOp.AVG:
      return "Avg";
    default: {
      const _: never = aggOp;
      return "";
    }
  }
}

export {
  QueryType,
  FilterOp,
  AggOp,
  QueryUrlPath,
  getFilterOpStr,
  getAggOpStr,
};

export type { FilterOpType, AggOpType };
