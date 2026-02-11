import React, { useCallback, useEffect, useMemo, useState } from "react";
import ReactDOM from "react-dom/client";

import "./styles.css";

type TransferMode = "PUSH" | "PULL";
type StatusKind = "neutral" | "ok" | "error";

interface StatusState {
  message: string;
  kind: StatusKind;
}

interface ConfigResponse {
  defaultDataplaneUrls: string[];
  mqttMonitoringEnabled: boolean;
}

interface ProgressSnapshot {
  bytesTotal: number | null;
  bytesTransferred: number | null;
  percentComplete: number | null;
  running: boolean;
  paused: boolean;
  finished: boolean;
  lastError: string | null;
}

interface DataFlowSnapshot {
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

interface DataplaneResult {
  dataplaneUrl: string;
  dataplaneId: string | null;
  dataFlows: DataFlowSnapshot[];
  error: string | null;
}

interface QueryResponse {
  results: DataplaneResult[];
}

interface StartTransferResponse {
  dataplaneUrl: string;
  dataFlowId: string;
  processId: string;
  state: string;
}

interface StreamSnapshotMessage {
  type: "snapshot";
  revision: number;
  results: DataplaneResult[];
}

interface StreamErrorMessage {
  type: "error";
  detail: string;
}

type StreamMessage = StreamSnapshotMessage | StreamErrorMessage;

interface StartFormState {
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

const defaultStartForm: StartFormState = {
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

async function callJson<T>(url: string, options: RequestInit = {}): Promise<T> {
  const headers = new Headers(options.headers ?? {});
  headers.set("Content-Type", "application/json");
  const response = await fetch(url, {
    ...options,
    headers,
  });
  const payload = (await response.json().catch(() => ({}))) as Record<string, unknown>;
  if (!response.ok) {
    const detail = typeof payload.detail === "string" ? payload.detail : JSON.stringify(payload);
    throw new Error(detail || `${response.status} ${response.statusText}`);
  }
  return payload as T;
}

function parseDataplaneUrls(raw: string): string[] {
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

function optionalValue(value: string): string | null {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function progressText(flow: DataFlowSnapshot): string {
  const progress = flow.progress ?? {};
  const transferred = typeof progress.bytesTransferred === "number" ? progress.bytesTransferred : 0;
  const total = typeof progress.bytesTotal === "number" ? progress.bytesTotal : null;
  const percent = typeof progress.percentComplete === "number" ? progress.percentComplete : null;

  const parts: string[] = [];
  if (progress.paused) {
    parts.push("paused");
  }
  if (progress.running) {
    parts.push("running");
  }
  if (progress.finished) {
    parts.push("finished");
  }

  const ratio = total === null ? `${transferred} bytes` : `${transferred}/${total} bytes`;
  const percentText = percent === null ? "n/a" : `${percent.toFixed(2)}%`;
  const errorText =
    typeof progress.lastError === "string" && progress.lastError.length > 0
      ? ` error=${progress.lastError}`
      : "";
  const stateText = parts.length > 0 ? ` [${parts.join(", ")}]` : "";
  return `${percentText} (${ratio})${stateText}${errorText}`;
}

function App(): JSX.Element {
  const [startForm, setStartForm] = useState<StartFormState>(defaultStartForm);
  const [pollDataplaneUrls, setPollDataplaneUrls] = useState<string>(defaultStartForm.dataplaneUrl);
  const [autoRefresh, setAutoRefresh] = useState<boolean>(true);
  const [mqttMonitoringEnabled, setMqttMonitoringEnabled] = useState<boolean>(false);
  const [websocketConnected, setWebsocketConnected] = useState<boolean>(false);
  const [streamRevision, setStreamRevision] = useState<number | null>(null);

  const [startStatus, setStartStatus] = useState<StatusState>({ message: "", kind: "neutral" });
  const [pollStatus, setPollStatus] = useState<StatusState>({ message: "", kind: "neutral" });
  const [results, setResults] = useState<DataplaneResult[]>([]);

  const dataplaneUrls = useMemo(() => parseDataplaneUrls(pollDataplaneUrls), [pollDataplaneUrls]);
  const streamKey = useMemo(() => dataplaneUrls.join(","), [dataplaneUrls]);

  const fetchFlowsForUrls = useCallback(async (urls: string[]) => {
    if (urls.length === 0) {
      setResults([]);
      setPollStatus({ kind: "error", message: "Enter at least one dataplane URL." });
      return;
    }

    const response = await callJson<QueryResponse>("/api/dataflows/query", {
      method: "POST",
      body: JSON.stringify({ dataplaneUrls: urls }),
    });
    setResults(response.results ?? []);
    setPollStatus({ kind: "ok", message: `Loaded ${urls.length} dataplane(s).` });
  }, []);

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
          config.defaultDataplaneUrls.length > 0
            ? config.defaultDataplaneUrls
            : [defaultStartForm.dataplaneUrl];
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

        setResults(message.results);
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
  }, [mqttMonitoringEnabled, streamKey]);

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
      setStartForm((previous) => ({ ...previous, [field]: value }));
    },
    [],
  );

  const startTransfer = useCallback(
    async (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      try {
        const payload = {
          dataplaneUrl: startForm.dataplaneUrl.trim(),
          transferMode: startForm.transferMode,
          sourceBucket: startForm.sourceBucket.trim(),
          sourceKey: startForm.sourceKey.trim(),
          destinationBucket: startForm.destinationBucket.trim(),
          destinationKey: startForm.destinationKey.trim(),
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
    async (action: "pause" | "start", dataplaneUrl: string, dataFlowId: string) => {
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

        await refreshFlows();
      } catch (error) {
        setPollStatus({ kind: "error", message: String(error) });
      }
    },
    [refreshFlows],
  );

  return (
    <div className="layout">
      <h1 className="title">Simpl Bulk Manual E2E UI</h1>

      <div className="grid">
        <section className="card">
          <h2>Start Transfer</h2>
          <form className="form-grid" onSubmit={(event) => void startTransfer(event)}>
            <label>
              Dataplane URL
              <input
                type="text"
                value={startForm.dataplaneUrl}
                onChange={(event) => updateStartFormField("dataplaneUrl", event.target.value)}
                required
              />
            </label>

            <label>
              Transfer mode
              <select
                value={startForm.transferMode}
                onChange={(event) =>
                  updateStartFormField("transferMode", event.target.value as TransferMode)
                }
              >
                <option value="PUSH">PUSH</option>
                <option value="PULL">PULL</option>
              </select>
            </label>

            <label>
              Source bucket
              <input
                type="text"
                value={startForm.sourceBucket}
                onChange={(event) => updateStartFormField("sourceBucket", event.target.value)}
                required
              />
            </label>

            <label>
              Source key (file)
              <input
                type="text"
                value={startForm.sourceKey}
                onChange={(event) => updateStartFormField("sourceKey", event.target.value)}
                required
              />
            </label>

            <label>
              Destination bucket
              <input
                type="text"
                value={startForm.destinationBucket}
                onChange={(event) => updateStartFormField("destinationBucket", event.target.value)}
                required
              />
            </label>

            <label>
              Destination key (file)
              <input
                type="text"
                value={startForm.destinationKey}
                onChange={(event) => updateStartFormField("destinationKey", event.target.value)}
                required
              />
            </label>

            <label>
              Source endpoint URL (optional)
              <input
                type="text"
                value={startForm.sourceEndpointUrl}
                onChange={(event) => updateStartFormField("sourceEndpointUrl", event.target.value)}
              />
            </label>

            <label>
              Destination endpoint URL (optional)
              <input
                type="text"
                value={startForm.destinationEndpointUrl}
                onChange={(event) => updateStartFormField("destinationEndpointUrl", event.target.value)}
              />
            </label>

            <label>
              Source access key ID (optional)
              <input
                type="text"
                value={startForm.sourceAccessKeyId}
                onChange={(event) => updateStartFormField("sourceAccessKeyId", event.target.value)}
              />
            </label>

            <label>
              Source secret access key (optional)
              <input
                type="password"
                value={startForm.sourceSecretAccessKey}
                onChange={(event) => updateStartFormField("sourceSecretAccessKey", event.target.value)}
              />
            </label>

            <label>
              Destination access key ID (optional)
              <input
                type="text"
                value={startForm.destinationAccessKeyId}
                onChange={(event) =>
                  updateStartFormField("destinationAccessKeyId", event.target.value)
                }
              />
            </label>

            <label>
              Destination secret access key (optional)
              <input
                type="password"
                value={startForm.destinationSecretAccessKey}
                onChange={(event) =>
                  updateStartFormField("destinationSecretAccessKey", event.target.value)
                }
              />
            </label>

            <label className="full">
              Process ID (optional)
              <input
                type="text"
                value={startForm.processId}
                onChange={(event) => updateStartFormField("processId", event.target.value)}
              />
            </label>

            <label className="full inline-checkbox">
              <input
                type="checkbox"
                checked={startForm.autoStartedNotification}
                onChange={(event) =>
                  updateStartFormField("autoStartedNotification", event.target.checked)
                }
              />
              Auto-send /started for PULL start
            </label>

            <div className="full button-row">
              <button type="submit">Start Transfer</button>
            </div>
          </form>

          <p className={`status ${startStatus.kind === "error" ? "error" : startStatus.kind === "ok" ? "ok" : ""}`}>
            {startStatus.message}
          </p>
        </section>

        <section className="card">
          <h2>Monitor Dataflows (MQTT)</h2>
          <div className="form-grid">
            <label className="full">
              Dataplane URLs (comma separated)
              <input
                type="text"
                value={pollDataplaneUrls}
                onChange={(event) => setPollDataplaneUrls(event.target.value)}
              />
            </label>

            <div className="full button-row">
              <button
                type="button"
                className="secondary"
                onClick={() => {
                  void refreshFlows();
                }}
              >
                Refresh Now
              </button>

              <label className="inline-checkbox">
                <input
                  type="checkbox"
                  checked={autoRefresh}
                  onChange={(event) => setAutoRefresh(event.target.checked)}
                />
                Auto refresh (every 3s)
              </label>

              <span className={`stream-pill ${websocketConnected ? "live" : "offline"}`}>
                {mqttMonitoringEnabled
                  ? websocketConnected
                    ? `LIVE${streamRevision !== null ? ` r${streamRevision}` : ""}`
                    : "POLL"
                  : "MQTT OFF"}
              </span>
            </div>
          </div>

          <p className={`status ${pollStatus.kind === "error" ? "error" : pollStatus.kind === "ok" ? "ok" : ""}`}>
            {pollStatus.message}
          </p>

          <div className="flows-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Dataplane</th>
                  <th>Flow</th>
                  <th>Process</th>
                  <th>Mode</th>
                  <th>State</th>
                  <th>Source</th>
                  <th>Destination</th>
                  <th>Progress</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {results.length === 0 ? (
                  <tr>
                    <td className="muted" colSpan={9}>
                      No dataflows loaded.
                    </td>
                  </tr>
                ) : (
                  results.flatMap((result) => {
                    if (result.error) {
                      return [
                        <tr key={`${result.dataplaneUrl}-error`}>
                          <td className="mono">{result.dataplaneUrl}</td>
                          <td className="muted" colSpan={8}>
                            Error: {result.error}
                          </td>
                        </tr>,
                      ];
                    }

                    if (result.dataFlows.length === 0) {
                      return [
                        <tr key={`${result.dataplaneUrl}-empty`}>
                          <td className="mono">{result.dataplaneUrl}</td>
                          <td className="muted" colSpan={8}>
                            No dataflows
                          </td>
                        </tr>,
                      ];
                    }

                    return result.dataFlows.map((flow) => {
                      const source = `${flow.sourceBucket ?? "-"} / ${flow.sourceKey ?? "-"}`;
                      const destination = `${flow.destinationBucket ?? "-"} / ${flow.destinationKey ?? "-"}`;
                      return (
                        <tr key={`${result.dataplaneUrl}-${flow.dataFlowId}`}>
                          <td className="mono">{result.dataplaneUrl}</td>
                          <td className="mono">{flow.dataFlowId}</td>
                          <td className="mono">{flow.processId ?? "-"}</td>
                          <td>
                            <span className="badge">{flow.transferMode ?? "-"}</span>
                          </td>
                          <td>{flow.state ?? "-"}</td>
                          <td className="mono">{source}</td>
                          <td className="mono">{destination}</td>
                          <td>{progressText(flow)}</td>
                          <td className="button-row">
                            <button
                              type="button"
                              className="warn"
                              onClick={() => {
                                void invokeFlowAction("pause", result.dataplaneUrl, flow.dataFlowId);
                              }}
                            >
                              Pause
                            </button>
                            <button
                              type="button"
                              className="resume"
                              onClick={() => {
                                void invokeFlowAction("start", result.dataplaneUrl, flow.dataFlowId);
                              }}
                            >
                              Start
                            </button>
                          </td>
                        </tr>
                      );
                    });
                  })
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
