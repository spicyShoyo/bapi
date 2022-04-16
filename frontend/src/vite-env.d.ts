/// <reference types="vite/client" />

type DeepRecord<T extends {}> = {
  [k in keyof T]:
    | null
    | (T[k] extends Array<infer I>
        ? Immutable.List<DeepRecord<I>>
        : T[k] extends string
        ? T[k]
        : T[k] extends number
        ? T[k]
        : T[k] extends boolean
        ? T[k]
        : Immutable.Record<DeepRecord<T[k]>>);
};
