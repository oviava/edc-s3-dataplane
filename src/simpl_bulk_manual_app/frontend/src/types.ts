export type TransferMode = "PUSH" | "PULL";
export type StatusKind = "neutral" | "ok" | "error";
export type FlowAction = "pause" | "start";

export interface StatusState {
  message: string;
  kind: StatusKind;
}

export interface ConfigResponse {
  defaultDataplaneUrls: string[];
  mqttMonitoringEnabled: boolean;
}

export interface ProgressSnapshot {
  bytesTotal: number | null;
  bytesTransferred: number | null;
  percentComplete: number | null;
  running: boolean;
  paused: boolean;
  finished: boolean;
  lastError: string | null;
}

export interface DataFlowSnapshot {
  dataplaneId?: string;
  dataplaneUrl?: string;
  dataFlowId: string;
  processId?: string;
  transferMode?: string;
  state?: string;
  sourceBucket?: string;
  sourceKey?: string;
  destinationBucket?: string;
  destinationKey?: string;
  progress?: Partial<ProgressSnapshot>;
}

export interface DataplaneResult {
  dataplaneUrl: string;
  dataplaneId: string | null;
  dataFlows: DataFlowSnapshot[];
  error: string | null;
}

export interface QueryResponse {
  results: DataplaneResult[];
}

export interface StartTransferResponse {
  dataplaneUrl: string;
  dataFlowId: string;
  processId: string;
  state: string;
}

export interface StreamSnapshotMessage {
  type: "snapshot";
  revision: number;
  results: DataplaneResult[];
}

export interface StreamErrorMessage {
  type: "error";
  detail: string;
}

export type StreamMessage = StreamSnapshotMessage | StreamErrorMessage;

export interface StartFormState {
  dataplaneUrl: string;
  transferMode: TransferMode;
  sourceBucket: string;
  sourceKey: string;
  destinationBucket: string;
  destinationKey: string;
  sourceEndpointUrl: string;
  destinationEndpointUrl: string;
  sourceAccessKeyId: string;
  sourceSecretAccessKey: string;
  destinationAccessKeyId: string;
  destinationSecretAccessKey: string;
  processId: string;
  autoStartedNotification: boolean;
}

export interface FlowActions {
  showPause: boolean;
  showStart: boolean;
}
