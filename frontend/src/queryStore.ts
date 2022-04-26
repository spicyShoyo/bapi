import { configureStore } from "@reduxjs/toolkit";
import queryReducer from "@/queryReducer";
import { getRecordFromUrlOrDefault } from "@/queryRecordUtils";

export default configureStore({
  reducer: queryReducer,
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: {
        ignoreState: true,
        ignoreActions: true,
      },
    }),
  preloadedState: getRecordFromUrlOrDefault(),
});
