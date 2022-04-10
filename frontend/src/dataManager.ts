import axios, { AxiosResponse } from "axios";

import { TableInfo } from "./TableContext";

const path = "/v1";

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
