import { useCallback, useEffect, useMemo, useState, type FormEvent } from "react";

import { callJson } from "../lib/api";
import {
  applyOptimisticFlowUpdate,
  defaultStartForm,
  effectiveFlowState,
  flowKey,
  optionalValue,
  parseDataplaneUrls,
  reconcileOptimisticStates,
  removeRecordKey,
} from "../lib/flow";
import type {
  ConfigResponse,
  FlowAction,
  QueryResponse,
  StartFormState,
  StartTransferResponse,
  StatusState,
  StreamMessage,
  DataplaneResult,
} from "../types";

interface UseDataflowDashboardResult {
  startForm: StartFormState;
  pollDataplaneUrls: string;
  autoRefresh: boolean;
  mqttMonitoringEnabled: boolean;
  websocketConnected: boolean;
  streamRevision: number | null;
  startStatus: StatusState;
  pollStatus: StatusState;
  results: DataplaneResult[];
  optimisticStates: Record<string, string>;
  pendingFlowActions: Record<string, FlowAction>;
  dataplaneUrls: string[];
  totalFlowCount: number;
  startedFlowCount: number;
  suspendedFlowCount: number;
  setPollDataplaneUrls: (value: string) => void;
  setAutoRefresh: (value: boolean) => void;
  updateStartFormField: <K extends keyof StartFormState>(field: K, value: StartFormState[K]) => void;
  startTransfer: (event: FormEvent<HTMLFormElement>) => Promise<void>;
  refreshFlows: () => Promise<void>;
  invokeFlowAction: (action: FlowAction, dataplaneUrl: string, dataFlowId: string) => Promise<void>;
}

export function useDataflowDashboard(): UseDataflowDashboardResult {
  const [startForm, setStartForm] = useState<StartFormState>(defaultStartForm);
  const [pollDataplaneUrls, setPollDataplaneUrls] = useState<string>(defaultStartForm.dataplaneUrl);
  const [autoRefresh, setAutoRefresh] = useState<boolean>(true);
  const [mqttMonitoringEnabled, setMqttMonitoringEnabled] = useState<boolean>(false);
  const [websocketConnected, setWebsocketConnected] = useState<boolean>(false);
  const [streamRevision, setStreamRevision] = useState<number | null>(null);

  const [startStatus, setStartStatus] = useState<StatusState>({ message: "", kind: "neutral" });
  const [pollStatus, setPollStatus] = useState<StatusState>({ message: "", kind: "neutral" });
  const [results, setResults] = useState<DataplaneResult[]>([]);
  const [optimisticStates, setOptimisticStates] = useState<Record<string, string>>({});
  const [pendingFlowActions, setPendingFlowActions] = useState<Record<string, FlowAction>>({});

  const dataplaneUrls = useMemo(() => parseDataplaneUrls(pollDataplaneUrls), [pollDataplaneUrls]);
  const streamKey = useMemo(() => dataplaneUrls.join(","), [dataplaneUrls]);

  const totalFlowCount = useMemo(
    () =>
      results.reduce((count, result) => {
        return count + result.dataFlows.length;
      }, 0),
    [results],
  );

  const startedFlowCount = useMemo(() => {
    let count = 0;
    for (const result of results) {
      for (const flow of result.dataFlows) {
        const key = flowKey(result.dataplaneUrl, flow.dataFlowId);
        const state = optimisticStates[key] ?? effectiveFlowState(flow);
        if (state === "STARTED" || state === "STARTING") {
          count += 1;
        }
      }
    }

    return count;
  }, [optimisticStates, results]);

  const suspendedFlowCount = useMemo(() => {
    let count = 0;
    for (const result of results) {
      for (const flow of result.dataFlows) {
        const key = flowKey(result.dataplaneUrl, flow.dataFlowId);
        const state = optimisticStates[key] ?? effectiveFlowState(flow);
        if (state === "SUSPENDED") {
          count += 1;
        }
      }
    }

    return count;
  }, [optimisticStates, results]);

  const ingestResults = useCallback((nextResults: DataplaneResult[]) => {
    setResults(nextResults);
    setOptimisticStates((previous) => reconcileOptimisticStates(nextResults, previous));
  }, []);

  const fetchFlowsForUrls = useCallback(
    async (urls: string[]) => {
      if (urls.length === 0) {
        setResults([]);
        setOptimisticStates({});
        setPendingFlowActions({});
        setPollStatus({ kind: "error", message: "Enter at least one dataplane URL." });
        return;
      }

      const response = await callJson<QueryResponse>("/api/dataflows/query", {
        method: "POST",
        body: JSON.stringify({ dataplaneUrls: urls }),
      });

      ingestResults(response.results ?? []);
      setPollStatus({ kind: "ok", message: `Loaded ${urls.length} dataplane target(s).` });
    },
    [ingestResults],
  );

  const refreshFlows = useCallback(async () => {
    await fetchFlowsForUrls(dataplaneUrls);
  }, [dataplaneUrls, fetchFlowsForUrls]);

  useEffect(() => {
    let disposed = false;

    async function bootstrap(): Promise<void> {
      try {
        const config = await callJson<ConfigResponse>("/api/config");

        if (disposed) {
          return;
        }

        const initialUrls =
          config.defaultDataplaneUrls.length > 0 ? config.defaultDataplaneUrls : [defaultStartForm.dataplaneUrl];

        setMqttMonitoringEnabled(config.mqttMonitoringEnabled);
        setPollDataplaneUrls(initialUrls.join(", "));
        setStartForm((previous) => ({
          ...previous,
          dataplaneUrl: initialUrls[0] ?? previous.dataplaneUrl,
        }));

        await fetchFlowsForUrls(initialUrls);
      } catch (error) {
        if (!disposed) {
          setPollStatus({ kind: "error", message: String(error) });
        }
      }
    }

    void bootstrap();

    return () => {
      disposed = true;
    };
  }, [fetchFlowsForUrls]);

  useEffect(() => {
    if (!mqttMonitoringEnabled || streamKey.length === 0) {
      setWebsocketConnected(false);
      return;
    }

    const scheme = window.location.protocol === "https:" ? "wss" : "ws";
    const params = new URLSearchParams();
    params.set("dataplaneUrls", streamKey);

    const socket = new WebSocket(`${scheme}://${window.location.host}/ws/dataflows?${params.toString()}`);
    let closedByCleanup = false;

    socket.onopen = () => {
      setWebsocketConnected(true);
      setPollStatus({ kind: "ok", message: "Live MQTT stream connected." });
    };

    socket.onmessage = (event: MessageEvent<string>) => {
      try {
        const message = JSON.parse(event.data) as StreamMessage;

        if (message.type === "error") {
          setPollStatus({ kind: "error", message: message.detail });
          return;
        }

        ingestResults(message.results);
        setStreamRevision(message.revision);
        setPollStatus({
          kind: "ok",
          message: `Live stream update r${message.revision} (${message.results.length} dataplane target(s)).`,
        });
      } catch {
        setPollStatus({ kind: "error", message: "Received invalid live stream payload." });
      }
    };

    socket.onerror = () => {
      setPollStatus({ kind: "error", message: "Live stream error. Falling back to polling." });
    };

    socket.onclose = () => {
      setWebsocketConnected(false);
      if (!closedByCleanup) {
        setPollStatus({ kind: "neutral", message: "Live stream disconnected. Falling back to polling." });
      }
    };

    return () => {
      closedByCleanup = true;
      socket.close();
    };
  }, [ingestResults, mqttMonitoringEnabled, streamKey]);

  useEffect(() => {
    if (!autoRefresh || websocketConnected) {
      return;
    }

    const interval = window.setInterval(() => {
      void refreshFlows().catch((error: unknown) => {
        setPollStatus({ kind: "error", message: String(error) });
      });
    }, 3000);

    return () => {
      window.clearInterval(interval);
    };
  }, [autoRefresh, refreshFlows, websocketConnected]);

  const updateStartFormField = useCallback(
    <K extends keyof StartFormState>(field: K, value: StartFormState[K]) => {
      setStartForm((previous) => {
        if (field === "sourceKey") {
          const nextSourceKey = String(value);
          if (nextSourceKey.trim().length === 0) {
            return { ...previous, sourceKey: nextSourceKey, destinationKey: "" };
          }
          return { ...previous, sourceKey: nextSourceKey };
        }
        return { ...previous, [field]: value };
      });
    },
    [],
  );

  const startTransfer = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault();

      try {
        const sourceKey = optionalValue(startForm.sourceKey);
        const destinationKey = sourceKey === null ? null : optionalValue(startForm.destinationKey);
        const payload = {
          dataplaneUrl: startForm.dataplaneUrl.trim(),
          transferMode: startForm.transferMode,
          sourceBucket: startForm.sourceBucket.trim(),
          sourceKey,
          destinationBucket: startForm.destinationBucket.trim(),
          destinationKey,
          sourceEndpointUrl: optionalValue(startForm.sourceEndpointUrl),
          destinationEndpointUrl: optionalValue(startForm.destinationEndpointUrl),
          sourceAccessKeyId: optionalValue(startForm.sourceAccessKeyId),
          sourceSecretAccessKey: optionalValue(startForm.sourceSecretAccessKey),
          destinationAccessKeyId: optionalValue(startForm.destinationAccessKeyId),
          destinationSecretAccessKey: optionalValue(startForm.destinationSecretAccessKey),
          processId: optionalValue(startForm.processId),
          autoStartedNotification: startForm.autoStartedNotification,
        };

        const response = await callJson<StartTransferResponse>("/api/transfers/start", {
          method: "POST",
          body: JSON.stringify(payload),
        });

        setStartStatus({
          kind: "ok",
          message: `Started flow ${response.dataFlowId} on ${response.dataplaneUrl} (${response.state}).`,
        });

        await refreshFlows();
      } catch (error) {
        setStartStatus({ kind: "error", message: String(error) });
      }
    },
    [refreshFlows, startForm],
  );

  const invokeFlowAction = useCallback(
    async (action: FlowAction, dataplaneUrl: string, dataFlowId: string) => {
      const key = flowKey(dataplaneUrl, dataFlowId);
      setPendingFlowActions((previous) => ({ ...previous, [key]: action }));

      try {
        if (action === "pause") {
          await callJson<{ status: string }>(`/api/transfers/${dataFlowId}/pause`, {
            method: "POST",
            body: JSON.stringify({ dataplaneUrl }),
          });
          setPollStatus({ kind: "ok", message: `Paused ${dataFlowId}.` });
        } else {
          await callJson<{ status: string }>(`/api/transfers/${dataFlowId}/start`, {
            method: "POST",
            body: JSON.stringify({ dataplaneUrl }),
          });
          setPollStatus({ kind: "ok", message: `Start sent for ${dataFlowId}.` });
        }

        const targetState = action === "pause" ? "SUSPENDED" : "STARTED";
        setOptimisticStates((previous) => ({ ...previous, [key]: targetState }));
        setResults((previous) => applyOptimisticFlowUpdate(previous, action, dataplaneUrl, dataFlowId));

        await refreshFlows();
      } catch (error) {
        setPollStatus({ kind: "error", message: String(error) });
        setOptimisticStates((previous) => removeRecordKey(previous, key));
      } finally {
        setPendingFlowActions((previous) => removeRecordKey(previous, key));
      }
    },
    [refreshFlows],
  );

  return {
    startForm,
    pollDataplaneUrls,
    autoRefresh,
    mqttMonitoringEnabled,
    websocketConnected,
    streamRevision,
    startStatus,
    pollStatus,
    results,
    optimisticStates,
    pendingFlowActions,
    dataplaneUrls,
    totalFlowCount,
    startedFlowCount,
    suspendedFlowCount,
    setPollDataplaneUrls,
    setAutoRefresh,
    updateStartFormField,
    startTransfer,
    refreshFlows,
    invokeFlowAction,
  };
}
