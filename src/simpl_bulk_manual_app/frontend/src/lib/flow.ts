import type {
  DataFlowSnapshot,
  DataplaneResult,
  FlowAction,
  FlowActions,
  ProgressSnapshot,
  StartFormState,
} from "../types";

export const defaultStartForm: StartFormState = {
  dataplaneUrl: "http://localhost:8080",
  transferMode: "PUSH",
  sourceBucket: "source-bucket",
  sourceKey: "payload.bin",
  destinationBucket: "destination-bucket",
  destinationKey: "payload-copy.bin",
  sourceEndpointUrl: "",
  destinationEndpointUrl: "",
  sourceAccessKeyId: "",
  sourceSecretAccessKey: "",
  destinationAccessKeyId: "",
  destinationSecretAccessKey: "",
  processId: "",
  autoStartedNotification: true,
};

const TERMINAL_STATES = new Set<string>(["COMPLETED", "TERMINATED"]);

export function parseDataplaneUrls(raw: string): string[] {
  const seen = new Set<string>();
  const values = raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  for (const value of values) {
    seen.add(value);
  }

  return [...seen];
}

export function optionalValue(value: string): string | null {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeFlowState(raw: string | undefined): string | null {
  if (typeof raw !== "string") {
    return null;
  }

  const normalized = raw.trim().toUpperCase();
  return normalized.length > 0 ? normalized : null;
}

function deriveStateFromProgress(progress: Partial<ProgressSnapshot> | undefined): string | null {
  if (!progress) {
    return null;
  }

  if (progress.finished) {
    return "COMPLETED";
  }

  if (progress.paused) {
    return "SUSPENDED";
  }

  if (progress.running) {
    return "STARTED";
  }

  return null;
}

export function effectiveFlowState(flow: DataFlowSnapshot): string {
  const fromPayload = normalizeFlowState(flow.state);
  const fromProgress = deriveStateFromProgress(flow.progress);

  if (fromProgress === "COMPLETED" || fromProgress === "SUSPENDED") {
    return fromProgress;
  }

  if (
    fromProgress === "STARTED" &&
    (fromPayload === null || fromPayload === "INITIALIZED" || fromPayload === "PREPARED")
  ) {
    return "STARTED";
  }

  if (fromPayload !== null) {
    return fromPayload;
  }

  if (fromProgress !== null) {
    return fromProgress;
  }

  return "UNKNOWN";
}

export function flowActionsForState(
  state: string,
  progress: Partial<ProgressSnapshot> | undefined,
): FlowActions {
  if (progress?.finished || TERMINAL_STATES.has(state)) {
    return { showPause: false, showStart: false };
  }

  if (progress?.paused) {
    return { showPause: false, showStart: true };
  }

  if (progress?.running) {
    return { showPause: true, showStart: false };
  }

  if (state === "SUSPENDED" || state === "PREPARED" || state === "INITIALIZED") {
    return { showPause: false, showStart: true };
  }

  if (state === "STARTED" || state === "STARTING") {
    return { showPause: true, showStart: false };
  }

  return { showPause: false, showStart: false };
}

export function flowStateTone(state: string):
  | "running"
  | "paused"
  | "completed"
  | "terminated"
  | "ready"
  | "unknown" {
  if (state === "STARTED" || state === "STARTING") {
    return "running";
  }

  if (state === "SUSPENDED") {
    return "paused";
  }

  if (state === "COMPLETED") {
    return "completed";
  }

  if (state === "TERMINATED") {
    return "terminated";
  }

  if (state === "PREPARED" || state === "PREPARING" || state === "INITIALIZED") {
    return "ready";
  }

  return "unknown";
}

function formatBytes(rawValue: number | null): string {
  if (rawValue === null || Number.isNaN(rawValue)) {
    return "n/a";
  }

  if (rawValue < 1024) {
    return `${rawValue} B`;
  }

  const units = ["KB", "MB", "GB", "TB"];
  let value = rawValue;
  let unitIndex = -1;

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }

  return `${value.toFixed(1)} ${units[unitIndex]}`;
}

export function progressPercent(flow: DataFlowSnapshot): number {
  const raw = flow.progress?.percentComplete;

  if (typeof raw !== "number") {
    return 0;
  }

  if (raw < 0) {
    return 0;
  }

  if (raw > 100) {
    return 100;
  }

  return raw;
}

export function progressText(flow: DataFlowSnapshot): string {
  const progress = flow.progress ?? {};
  const transferred = typeof progress.bytesTransferred === "number" ? progress.bytesTransferred : null;
  const total = typeof progress.bytesTotal === "number" ? progress.bytesTotal : null;
  const percent = typeof progress.percentComplete === "number" ? progress.percentComplete : null;

  const ratio =
    total === null ? `${formatBytes(transferred)}` : `${formatBytes(transferred)} / ${formatBytes(total)}`;
  const percentText = percent === null ? "n/a" : `${percent.toFixed(2)}%`;
  const errorText =
    typeof progress.lastError === "string" && progress.lastError.length > 0
      ? ` error=${progress.lastError}`
      : "";

  return `${percentText} (${ratio})${errorText}`;
}

export function flowKey(dataplaneUrl: string, dataFlowId: string): string {
  return `${dataplaneUrl}::${dataFlowId}`;
}

export function removeRecordKey<T>(record: Record<string, T>, key: string): Record<string, T> {
  if (!(key in record)) {
    return record;
  }

  const next = { ...record };
  delete next[key];
  return next;
}

export function applyOptimisticFlowUpdate(
  results: DataplaneResult[],
  action: FlowAction,
  dataplaneUrl: string,
  dataFlowId: string,
): DataplaneResult[] {
  let changed = false;

  const nextResults = results.map((result) => {
    if (result.dataplaneUrl !== dataplaneUrl || result.dataFlows.length === 0) {
      return result;
    }

    let resultChanged = false;

    const nextFlows = result.dataFlows.map((flow) => {
      if (flow.dataFlowId !== dataFlowId) {
        return flow;
      }

      resultChanged = true;
      changed = true;

      const nextProgress: Partial<ProgressSnapshot> = {
        ...(flow.progress ?? {}),
      };

      if (action === "pause") {
        nextProgress.paused = true;
        nextProgress.running = false;
      } else {
        nextProgress.paused = false;
        nextProgress.running = true;
        nextProgress.finished = false;
      }

      return {
        ...flow,
        state: action === "pause" ? "SUSPENDED" : "STARTED",
        progress: nextProgress,
      };
    });

    if (!resultChanged) {
      return result;
    }

    return {
      ...result,
      dataFlows: nextFlows,
    };
  });

  return changed ? nextResults : results;
}

export function reconcileOptimisticStates(
  nextResults: DataplaneResult[],
  optimisticStates: Record<string, string>,
): Record<string, string> {
  if (Object.keys(optimisticStates).length === 0) {
    return optimisticStates;
  }

  const observedStates = new Map<string, string>();

  for (const result of nextResults) {
    for (const flow of result.dataFlows) {
      observedStates.set(flowKey(result.dataplaneUrl, flow.dataFlowId), effectiveFlowState(flow));
    }
  }

  const nextOptimistic: Record<string, string> = {};

  for (const [key, expectedState] of Object.entries(optimisticStates)) {
    const observed = observedStates.get(key);
    if (observed !== undefined && observed !== expectedState) {
      nextOptimistic[key] = expectedState;
    }
  }

  return nextOptimistic;
}
