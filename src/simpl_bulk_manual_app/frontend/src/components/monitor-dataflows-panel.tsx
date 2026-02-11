import { StatusMessage } from "./status-message";
import { Button } from "./ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "./ui/card";
import { Checkbox } from "./ui/checkbox";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { DataflowsTable } from "./dataflows-table";
import type { DataplaneResult, FlowAction, StatusState } from "../types";

interface MonitorDataflowsPanelProps {
  pollDataplaneUrls: string;
  autoRefresh: boolean;
  status: StatusState;
  results: DataplaneResult[];
  optimisticStates: Record<string, string>;
  pendingFlowActions: Record<string, FlowAction>;
  onPollDataplaneUrlsChange: (value: string) => void;
  onAutoRefreshChange: (value: boolean) => void;
  onRefresh: () => void;
  onInvokeFlowAction: (action: FlowAction, dataplaneUrl: string, dataFlowId: string) => void;
}

export function MonitorDataflowsPanel({
  pollDataplaneUrls,
  autoRefresh,
  status,
  results,
  optimisticStates,
  pendingFlowActions,
  onPollDataplaneUrlsChange,
  onAutoRefreshChange,
  onRefresh,
  onInvokeFlowAction,
}: MonitorDataflowsPanelProps): JSX.Element {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Monitor Dataflows</CardTitle>
        <CardDescription>Track flow snapshots from MQTT and run state-aware control actions.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-4 rounded-lg border border-border bg-white p-4">
          <div className="space-y-2">
            <Label htmlFor="monitor-dataplane-urls">Dataplane URLs (comma separated)</Label>
            <Input
              id="monitor-dataplane-urls"
              value={pollDataplaneUrls}
              onChange={(event) => onPollDataplaneUrlsChange(event.target.value)}
            />
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Button onClick={onRefresh} variant="secondary">
              Refresh Now
            </Button>

            <div className="flex items-center gap-2 rounded-md border border-border px-3 py-2">
              <Checkbox
                checked={autoRefresh}
                id="monitor-auto-refresh"
                onCheckedChange={(checked) => onAutoRefreshChange(checked === true)}
              />
              <Label
                className="text-sm normal-case tracking-normal text-slate-700"
                htmlFor="monitor-auto-refresh"
              >
                Auto refresh (every 3s)
              </Label>
            </div>
          </div>
        </div>

        <StatusMessage kind={status.kind} message={status.message} fallback="Waiting for flow updates." />

        <DataflowsTable
          optimisticStates={optimisticStates}
          onInvokeFlowAction={onInvokeFlowAction}
          pendingFlowActions={pendingFlowActions}
          results={results}
        />
      </CardContent>
    </Card>
  );
}
