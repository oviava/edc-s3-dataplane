import { useMemo } from "react";

import {
  effectiveFlowState,
  flowActionsForState,
  flowKey,
  flowStateTone,
  progressPercent,
  progressText,
} from "../lib/flow";
import type { DataplaneResult, FlowAction } from "../types";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { Progress } from "./ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./ui/table";

interface DataflowsTableProps {
  results: DataplaneResult[];
  optimisticStates: Record<string, string>;
  pendingFlowActions: Record<string, FlowAction>;
  onInvokeFlowAction: (action: FlowAction, dataplaneUrl: string, dataFlowId: string) => void;
}

type NoticeRow = {
  kind: "notice";
  key: string;
  dataplaneUrl: string;
  message: string;
  isError: boolean;
};

type FlowRow = {
  kind: "flow";
  key: string;
  dataplaneUrl: string;
  dataFlowId: string;
  processId: string;
  transferMode: string;
  state: string;
  source: string;
  destination: string;
  progress: string;
  progressValue: number;
  showPause: boolean;
  showStart: boolean;
  pendingAction: FlowAction | undefined;
};

type RenderRow = NoticeRow | FlowRow;

const stateToneClasses: Record<ReturnType<typeof flowStateTone>, string> = {
  running: "border-emerald-300 bg-emerald-100 text-emerald-800",
  paused: "border-amber-300 bg-amber-100 text-amber-800",
  completed: "border-blue-300 bg-blue-100 text-blue-800",
  terminated: "border-rose-300 bg-rose-100 text-rose-800",
  ready: "border-sky-300 bg-sky-100 text-sky-800",
  unknown: "border-slate-300 bg-slate-100 text-slate-700",
};

function buildRows(
  results: DataplaneResult[],
  optimisticStates: Record<string, string>,
  pendingFlowActions: Record<string, FlowAction>,
): RenderRow[] {
  if (results.length === 0) {
    return [
      {
        kind: "notice",
        key: "global-empty",
        dataplaneUrl: "-",
        message: "No dataflows loaded.",
        isError: false,
      },
    ];
  }

  const rows: RenderRow[] = [];

  for (const result of results) {
    if (result.error) {
      rows.push({
        kind: "notice",
        key: `${result.dataplaneUrl}-error`,
        dataplaneUrl: result.dataplaneUrl,
        message: `Error: ${result.error}`,
        isError: true,
      });
      continue;
    }

    if (result.dataFlows.length === 0) {
      rows.push({
        kind: "notice",
        key: `${result.dataplaneUrl}-empty`,
        dataplaneUrl: result.dataplaneUrl,
        message: "No dataflows",
        isError: false,
      });
      continue;
    }

    for (const flow of result.dataFlows) {
      const key = flowKey(result.dataplaneUrl, flow.dataFlowId);
      const state = optimisticStates[key] ?? effectiveFlowState(flow);
      const actions = flowActionsForState(state, flow.progress);

      rows.push({
        kind: "flow",
        key: `${result.dataplaneUrl}-${flow.dataFlowId}`,
        dataplaneUrl: result.dataplaneUrl,
        dataFlowId: flow.dataFlowId,
        processId: flow.processId ?? "-",
        transferMode: flow.transferMode ?? "-",
        state,
        source: `${flow.sourceBucket ?? "-"} / ${flow.sourceKey ?? "-"}`,
        destination: `${flow.destinationBucket ?? "-"} / ${flow.destinationKey ?? "-"}`,
        progress: progressText(flow),
        progressValue: progressPercent(flow),
        showPause: actions.showPause,
        showStart: actions.showStart,
        pendingAction: pendingFlowActions[key],
      });
    }
  }

  return rows;
}

export function DataflowsTable({
  results,
  optimisticStates,
  pendingFlowActions,
  onInvokeFlowAction,
}: DataflowsTableProps): JSX.Element {
  const rows = useMemo(
    () => buildRows(results, optimisticStates, pendingFlowActions),
    [optimisticStates, pendingFlowActions, results],
  );

  return (
    <div className="rounded-lg border border-border bg-white">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Dataplane</TableHead>
            <TableHead>Flow</TableHead>
            <TableHead>Process</TableHead>
            <TableHead>Mode</TableHead>
            <TableHead>State</TableHead>
            <TableHead>Source</TableHead>
            <TableHead>Destination</TableHead>
            <TableHead>Progress</TableHead>
            <TableHead>Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => {
            if (row.kind === "notice") {
              return (
                <TableRow key={row.key}>
                  <TableCell className="font-mono text-xs text-slate-600">{row.dataplaneUrl}</TableCell>
                  <TableCell className={row.isError ? "text-rose-700" : "text-slate-500"} colSpan={8}>
                    {row.message}
                  </TableCell>
                </TableRow>
              );
            }

            const tone = flowStateTone(row.state);
            const noActions = !row.showPause && !row.showStart;

            return (
              <TableRow key={row.key}>
                <TableCell className="font-mono text-xs text-slate-600">{row.dataplaneUrl}</TableCell>
                <TableCell className="font-mono text-xs text-slate-600">{row.dataFlowId}</TableCell>
                <TableCell className="font-mono text-xs text-slate-600">{row.processId}</TableCell>
                <TableCell>
                  <Badge className="border-slate-300 bg-slate-100 text-slate-700" variant="outline">
                    {row.transferMode}
                  </Badge>
                </TableCell>
                <TableCell>
                  <Badge className={stateToneClasses[tone]} variant="outline">
                    {row.state}
                  </Badge>
                </TableCell>
                <TableCell className="font-mono text-xs text-slate-600">{row.source}</TableCell>
                <TableCell className="font-mono text-xs text-slate-600">{row.destination}</TableCell>
                <TableCell>
                  <div className="space-y-2">
                    <span className="text-xs text-slate-600">{row.progress}</span>
                    <Progress value={row.progressValue} />
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex min-w-28 flex-wrap items-center gap-2">
                    {row.showPause ? (
                      <Button
                        className="border-amber-300 bg-amber-100 text-amber-800 hover:bg-amber-200"
                        disabled={row.pendingAction !== undefined}
                        onClick={() => onInvokeFlowAction("pause", row.dataplaneUrl, row.dataFlowId)}
                        size="sm"
                        variant="outline"
                      >
                        {row.pendingAction === "pause" ? "Pausing..." : "Pause"}
                      </Button>
                    ) : null}

                    {row.showStart ? (
                      <Button
                        className="bg-emerald-600 text-white hover:bg-emerald-700"
                        disabled={row.pendingAction !== undefined}
                        onClick={() => onInvokeFlowAction("start", row.dataplaneUrl, row.dataFlowId)}
                        size="sm"
                      >
                        {row.pendingAction === "start" ? "Starting..." : "Start"}
                      </Button>
                    ) : null}

                    {noActions ? <span className="text-xs text-slate-500">No actions</span> : null}
                  </div>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}
