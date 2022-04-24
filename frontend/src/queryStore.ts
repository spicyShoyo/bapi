import { configureStore, PayloadAction } from "@reduxjs/toolkit";
import queryRecordReducer from "./queryRecordReducer";

export default configureStore({
  reducer: queryRecordReducer,
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: {
        ignoreState: true,
        ignoreActions: true,
      },
    }),
});
