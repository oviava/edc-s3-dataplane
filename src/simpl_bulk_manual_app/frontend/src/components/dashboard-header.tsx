import { Badge } from "./ui/badge";
import { Card } from "./ui/card";

interface DashboardHeaderProps {
  mqttMonitoringEnabled: boolean;
  websocketConnected: boolean;
  streamRevision: number | null;
  dataplaneCount: number;
  totalFlowCount: number;
  startedFlowCount: number;
  suspendedFlowCount: number;
}

function LiveModeBadge({
  mqttMonitoringEnabled,
  websocketConnected,
  streamRevision,
}: Pick<DashboardHeaderProps, "mqttMonitoringEnabled" | "websocketConnected" | "streamRevision">): JSX.Element {
  if (!mqttMonitoringEnabled) {
    return (
      <Badge className="rounded-full border-amber-300 bg-amber-100 px-3 py-1 text-[11px] tracking-[0.12em] text-amber-800" variant="outline">
        MQTT OFF
      </Badge>
    );
  }

  if (websocketConnected) {
    return (
      <Badge className="rounded-full border-emerald-300 bg-emerald-100 px-3 py-1 text-[11px] tracking-[0.12em] text-emerald-800" variant="outline">
        LIVE{streamRevision !== null ? ` r${streamRevision}` : ""}
      </Badge>
    );
  }

  return (
    <Badge className="rounded-full border-sky-300 bg-sky-100 px-3 py-1 text-[11px] tracking-[0.12em] text-sky-800" variant="outline">
      POLL
    </Badge>
  );
}

export function DashboardHeader(props: DashboardHeaderProps): JSX.Element {
  return (
    <Card className="relative overflow-hidden border-white/80 bg-white/80 p-6">
      <div className="absolute -left-20 top-2 h-56 w-56 rounded-full bg-sky-400/20 blur-3xl" aria-hidden />
      <div className="absolute -right-16 top-14 h-52 w-52 rounded-full bg-emerald-400/20 blur-3xl" aria-hidden />
      <div className="relative space-y-4">
        <p className="text-xs font-bold uppercase tracking-[0.18em] text-slate-700">Manual Operations Console</p>
        <h1 className="font-display text-3xl font-bold leading-tight text-slate-950 sm:text-4xl">Simpl Bulk Dataplane</h1>
        <p className="max-w-3xl text-sm text-slate-600 sm:text-base">
          Start, suspend, and resume flow executions while monitoring live transfer progress across dataplane targets.
        </p>

        <div className="flex flex-wrap items-center gap-2">
          <LiveModeBadge
            mqttMonitoringEnabled={props.mqttMonitoringEnabled}
            websocketConnected={props.websocketConnected}
            streamRevision={props.streamRevision}
          />
          <Badge className="rounded-full border-slate-300 bg-white px-3 py-1 text-slate-700" variant="outline">
            <strong className="mr-1 text-slate-900">{props.dataplaneCount}</strong> Targets
          </Badge>
          <Badge className="rounded-full border-slate-300 bg-white px-3 py-1 text-slate-700" variant="outline">
            <strong className="mr-1 text-slate-900">{props.totalFlowCount}</strong> Flows
          </Badge>
          <Badge className="rounded-full border-slate-300 bg-white px-3 py-1 text-slate-700" variant="outline">
            <strong className="mr-1 text-slate-900">{props.startedFlowCount}</strong> Started
          </Badge>
          <Badge className="rounded-full border-slate-300 bg-white px-3 py-1 text-slate-700" variant="outline">
            <strong className="mr-1 text-slate-900">{props.suspendedFlowCount}</strong> Suspended
          </Badge>
        </div>
      </div>
    </Card>
  );
}
