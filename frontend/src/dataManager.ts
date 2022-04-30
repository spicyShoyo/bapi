import axios, { AxiosResponse } from "axios";
import BapiQueryRecord from "./bapiQueryRecord";
import { QueryType } from "./queryConsts";
import { recordToTableQuery } from "./queryRecordUtils";

import { TableInfo } from "./TableContext";

const path = "/v1";

// TODO: add typing
export async function fetchQueryResult(query: BapiQueryRecord): Promise<any> {
  if (query.query_type === QueryType.Table) {
    return axios
      .get(`${path}/table?q=${JSON.stringify(recordToTableQuery(query))}`)
      .then((res: AxiosResponse<any>) => console.log("$$$", res.data));
  }
  return Promise.reject();
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
