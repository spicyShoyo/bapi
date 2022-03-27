import dayjs from "dayjs";

// Good for now: we consider the app init time as "now"
const NOW = dayjs();
const L5M = NOW.subtract(5, "minute");
const L1H = NOW.subtract(1, "hour");
const L12H = NOW.subtract(12, "hour");
const L1D = NOW.subtract(1, "day");
const L2D = NOW.subtract(2, "day");
const L7D = NOW.subtract(7, "day");
const L14D = NOW.subtract(14, "day");

export { NOW, L5M, L1H, L12H, L1D, L2D, L7D, L14D };
