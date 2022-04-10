export default function nullthrows<T>(nonnull: T | null): T {
  if (nonnull == null) {
    throw new Error("Got unexpected null");
  }
  return nonnull;
}
