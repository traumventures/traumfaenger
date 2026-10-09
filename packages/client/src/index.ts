export type * from "@traumfaenger/api";
export { DIMENSIONS, LIMITS, PROTOCOLS, ROUTES } from "@traumfaenger/api";
export {
    createDatabase,
    type Database,
    type DatabaseOptions,
    type DatabaseStats,
    type OperationContext,
    type RecordableSocket,
    UNATTRIBUTED,
} from "./recorder.ts";
export { defaultSocketFactory, type Encoding, type IngestSocket, type SocketFactory } from "./sender.ts";
export { prepareData, SseSplitter, splitEvents } from "./text.ts";
export {
    type AnalyticsQuery,
    type AnalyticsSource,
    TraumfaengerError,
    type ClientOptions,
    QueryClient,
    toSearch,
} from "./transport.ts";
