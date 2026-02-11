import { cn } from "../lib/utils";
import type { StatusKind } from "../types";

interface StatusMessageProps {
  kind: StatusKind;
  message: string;
  fallback: string;
}

const statusToneClasses: Record<StatusKind, string> = {
  neutral: "border-slate-300 bg-white text-slate-600",
  ok: "border-emerald-300 bg-emerald-50 text-emerald-700",
  error: "border-rose-300 bg-rose-50 text-rose-700",
};

export function StatusMessage({ kind, message, fallback }: StatusMessageProps): JSX.Element {
  return (
    <p
      className={cn(
        "rounded-md border px-3 py-2 text-sm",
        statusToneClasses[kind],
      )}
    >
      {message || fallback}
    </p>
  );
}
