export default function nullthrows<T>(nonnull: T | null | undefined): T {
  if (nonnull == null) {
    throw new Error("got unexpected null");
  }
  return nonnull;
}
