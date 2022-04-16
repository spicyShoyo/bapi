import Immutable from "immutable";

export enum ColumnType {
  NONE = 0,
  INT = 1,
  STR = 2,
}

export type ColumnInfo = {
  column_name: string;
  column_type: ColumnType;
};

export class ColumnRecord extends Immutable.Record<DeepRecord<ColumnInfo>>({
  column_name: null,
  column_type: null,
}) {
  static fromColumnInfo(colInfo: ColumnInfo): ColumnRecord {
    return new ColumnRecord({
      column_name: colInfo.column_name,
      column_type: colInfo.column_type,
    });
  }
}