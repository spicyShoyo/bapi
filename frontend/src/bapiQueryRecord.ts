import Immutable from "immutable";
import { AggOpType, QueryType, ColumnInfo, Filter } from "./queryConsts";
import { TimeRange } from "./tsConsts";

const BAPI_PREFIX = "_BAPI_";
const NODE_TYPE_KEY = BAPI_PREFIX + "NODE";
const ELEMENT_KEY = BAPI_PREFIX + "ELEMENT";
const ELEMENT_FACTORY_KEY = BAPI_PREFIX + "ELEMENT_FACTORY";

type NodeType = "list" | "object";
const LIST_NODE: NodeType = "list";
const OBJECT_NODE: NodeType = "object";

const FiltersSpecPaths = {
  [NODE_TYPE_KEY]: LIST_NODE,
  [ELEMENT_KEY]: {
    [NODE_TYPE_KEY]: OBJECT_NODE,
    column_name: null,
    column_type: null,
    filter_op: null,
    int_vals: {
      [NODE_TYPE_KEY]: LIST_NODE,
      [ELEMENT_KEY]: null,
    },
    str_vals: {
      [NODE_TYPE_KEY]: LIST_NODE,
      [ELEMENT_KEY]: null,
    },
  },
};

const ColumnsSpecPaths = {
  [NODE_TYPE_KEY]: LIST_NODE,
  [ELEMENT_KEY]: {
    [NODE_TYPE_KEY]: OBJECT_NODE,
    column_name: null,
    column_type: null,
  },
};

const QuerySpecPaths = {
  [NODE_TYPE_KEY]: OBJECT_NODE,
  query_type: null,
  min_ts: null,
  max_ts: null,
  ts_range: null,
  filters: FiltersSpecPaths,
  target_cols: ColumnsSpecPaths,
  agg_op: null,
  agg_cols: ColumnsSpecPaths,
};

type AnyObject = { [key: string]: any };
type DeepRecordFactory = any;
type RecordSpec = { [key: string]: undefined | DeepRecordFactory };

function buildDeepRecordFactory(
  spec: RecordSpec,
  nodeType: NodeType,
): DeepRecordFactory {
  if (nodeType === LIST_NODE) {
    const factory = function innerListFactory(vals: any) {
      if (vals == null) {
        return undefined;
      }

      if (spec[ELEMENT_FACTORY_KEY] == true) {
        return Immutable.List(vals);
      }

      return Immutable.List(
        vals.map((v: any) => new spec[ELEMENT_FACTORY_KEY](v)),
      );
    };

    factory.getSpec = () => spec[ELEMENT_FACTORY_KEY];
    return factory;
  }

  return class InnerRecord extends Immutable.Record<any>(
    mapObject(spec, () => undefined),
  ) {
    constructor(vals: AnyObject) {
      super(
        mapObject(vals ?? {}, (val: any, key: string) => {
          if (val == null || spec[key] == null) {
            // keys not in the spec are dropped
            return undefined;
          }

          if (spec[key] === true) {
            return Immutable.fromJS(val);
          }

          return new spec[key](val);
        }),
      );
    }

    static getSpec() {
      return spec;
    }
  };
}

function recordFromSpecPaths(spec: AnyObject) {
  function recursiveBuilder(
    val: any,
    _key: string,
  ): DeepRecordFactory | undefined {
    switch (val?.[NODE_TYPE_KEY]) {
      case OBJECT_NODE:
        return buildDeepRecordFactory(
          mapObject(val, recursiveBuilder),
          OBJECT_NODE,
        );
      case LIST_NODE:
        return buildDeepRecordFactory(
          {
            [ELEMENT_FACTORY_KEY]: recursiveBuilder(
              val[ELEMENT_KEY],
              ELEMENT_FACTORY_KEY,
            ),
          },
          LIST_NODE,
        );
      default:
        return true;
    }
  }

  return buildDeepRecordFactory(mapObject(spec, recursiveBuilder), OBJECT_NODE);
}

function mapObject<TVal, TValOut>(
  object: { [key: string]: TVal },
  mapFn: (val: TVal, key: string) => TValOut,
): { [key: string]: TValOut } {
  const result: { [key: string]: TValOut } = {};

  Object.keys(object).forEach((key) => {
    if (key.startsWith(BAPI_PREFIX)) {
      return;
    }
    result[key] = mapFn(object[key], key);
  });

  return result;
}

const BapiQueryRecordBase: Immutable.Record.Factory<
  DeepRecord<{
    query_type: QueryType;
    min_ts: number;
    max_ts: number;
    ts_range: TimeRange;
    filters: Filter[];
    target_cols: ColumnInfo[];
    agg_op: AggOpType;
    agg_cols: ColumnInfo[];
  }>
> & { getSpec(): any } = recordFromSpecPaths(QuerySpecPaths);

// Inheritant so that type shows up as BapiQueryRecord instead of Immutable.Record
export default class BapiQueryRecord extends BapiQueryRecordBase {}
