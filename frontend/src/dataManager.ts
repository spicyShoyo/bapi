import axios, { AxiosResponse } from "axios";
import BapiQueryRecord from "./bapiQueryRecord";
import { recordToTableQuery } from "./queryRecordUtils";

import { TableInfo } from "./TableContext";

const path = "/v1";

export type TableQueryApiReply = {
  status: number;
  result: TableQueryResult | undefined;
};

// keep in sync with `bapi.proto`
export type TableQueryResult = {
  count: number;

  int_column_names?: string[];
  int_result?: number[];
  int_has_value?: boolean[];

  str_column_names?: string[];
  str_id_map?: { [key: string]: string };
  str_result?: number[];
  str_has_value?: boolean[];

  agg_int_column_names?: string[];
  agg_int_result?: number[];
  agg_int_has_value?: boolean[];

  agg_float_column_names?: string[];
  agg_float_result?: number[];
  agg_float_has_value?: boolean[];
};

export async function fetchTableQueryResult(
  query: BapiQueryRecord,
): Promise<TableQueryApiReply> {
  return axios
    .get(`${path}/table?q=${JSON.stringify(recordToTableQuery(query))}`)
    .then((res: AxiosResponse<TableQueryApiReply>) => res.data);
}

export async function fetchTableInfo(table: string): Promise<TableInfo> {
  return axios
    .get(`${path}/table_info?table=${table}`)
    .then(
      (res: AxiosResponse<{ table_info: TableInfo }>) => res.data.table_info,
    );
}

export const fetchStringValues = (function () {
  const cache = new Map<string, string[] | null>();
  return async function fetchStringValues(
    table: string,
    column: string,
    searchString: string,
  ): Promise<string[] | null> {
    const key = table + column + searchString;
    if (cache.has(key)) {
      return cache.get(key)!;
    }

    return axios
      .get(
        `${path}/string_values?table=${table}&column=${column}&search_string=${searchString}`,
      )
      .then((res: AxiosResponse<{ values: string[] | null }>) => {
        cache.set(key, res.data.values);
        return res.data.values;
      });
  };
})();
