import { configureStore } from "@reduxjs/toolkit";
import queryReducer from "@/queryReducer";
import { buildRecordFromUrl } from "@/queryRecord";

export default configureStore({
  reducer: queryReducer,
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: {
        ignoreState: true,
        ignoreActions: true,
      },
    }),
  preloadedState: buildRecordFromUrl(),
});
