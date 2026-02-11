import { DashboardHeader } from "./components/dashboard-header";
import { MonitorDataflowsPanel } from "./components/monitor-dataflows-panel";
import { StartTransferForm } from "./components/start-transfer-form";
import { useDataflowDashboard } from "./hooks/use-dataflow-dashboard";

export function App(): JSX.Element {
  const {
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
  } = useDataflowDashboard();

  return (
    <div className="relative min-h-screen overflow-hidden">
      <div className="pointer-events-none absolute left-[-120px] top-[-120px] h-72 w-72 rounded-full bg-sky-300/25 blur-3xl" />
      <div className="pointer-events-none absolute right-[-150px] top-[160px] h-80 w-80 rounded-full bg-emerald-300/25 blur-3xl" />

      <main className="relative mx-auto flex w-full max-w-[1400px] flex-col gap-4 px-4 py-6 md:px-6">
        <DashboardHeader
          dataplaneCount={dataplaneUrls.length}
          mqttMonitoringEnabled={mqttMonitoringEnabled}
          startedFlowCount={startedFlowCount}
          streamRevision={streamRevision}
          suspendedFlowCount={suspendedFlowCount}
          totalFlowCount={totalFlowCount}
          websocketConnected={websocketConnected}
        />

        <section className="grid gap-4 xl:grid-cols-[minmax(360px,0.95fr)_minmax(0,1.6fr)]">
          <StartTransferForm
            onFieldChange={updateStartFormField}
            onSubmit={(event) => {
              void startTransfer(event);
            }}
            startForm={startForm}
            status={startStatus}
          />

          <MonitorDataflowsPanel
            autoRefresh={autoRefresh}
            onAutoRefreshChange={setAutoRefresh}
            onInvokeFlowAction={(action, dataplaneUrl, dataFlowId) => {
              void invokeFlowAction(action, dataplaneUrl, dataFlowId);
            }}
            onPollDataplaneUrlsChange={setPollDataplaneUrls}
            onRefresh={() => {
              void refreshFlows();
            }}
            optimisticStates={optimisticStates}
            pendingFlowActions={pendingFlowActions}
            pollDataplaneUrls={pollDataplaneUrls}
            results={results}
            status={pollStatus}
          />
        </section>
      </main>
    </div>
  );
}
