import type { FormEvent } from "react";

import { StatusMessage } from "./status-message";
import { Button } from "./ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "./ui/card";
import { Checkbox } from "./ui/checkbox";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { cn } from "../lib/utils";
import type { StartFormState, StatusState, TransferMode } from "../types";

interface StartTransferFormProps {
  startForm: StartFormState;
  status: StatusState;
  onFieldChange: <K extends keyof StartFormState>(field: K, value: StartFormState[K]) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
}

const inputGridClass = "grid gap-4 sm:grid-cols-2";

export function StartTransferForm({
  startForm,
  status,
  onFieldChange,
  onSubmit,
}: StartTransferFormProps): JSX.Element {
  const sourceKeyPresent = startForm.sourceKey.trim().length > 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Start Transfer</CardTitle>
        <CardDescription>
          Compose and send a new start command with optional S3 endpoint and credential overrides.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <form className="space-y-4" onSubmit={onSubmit}>
          <div className={inputGridClass}>
            <div className="space-y-2 sm:col-span-2">
              <Label htmlFor="start-dataplane-url">Dataplane URL</Label>
              <Input
                id="start-dataplane-url"
                required
                value={startForm.dataplaneUrl}
                onChange={(event) => onFieldChange("dataplaneUrl", event.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-transfer-mode">Transfer mode</Label>
              <select
                id="start-transfer-mode"
                className={cn(
                  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm text-slate-900",
                  "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                )}
                value={startForm.transferMode}
                onChange={(event) => onFieldChange("transferMode", event.target.value as TransferMode)}
              >
                <option value="PUSH">PUSH</option>
                <option value="PULL">PULL</option>
              </select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-process-id">Process ID (optional)</Label>
              <Input
                id="start-process-id"
                value={startForm.processId}
                onChange={(event) => onFieldChange("processId", event.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-source-bucket">Source bucket</Label>
              <Input
                id="start-source-bucket"
                required
                value={startForm.sourceBucket}
                onChange={(event) => onFieldChange("sourceBucket", event.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-source-key">Source key (optional file)</Label>
              <Input
                id="start-source-key"
                placeholder="Leave empty to transfer full source bucket"
                value={startForm.sourceKey}
                onChange={(event) => onFieldChange("sourceKey", event.target.value)}
              />
              <p className="text-xs text-slate-500">Leave empty to copy all objects from the source bucket.</p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-destination-bucket">Destination bucket</Label>
              <Input
                id="start-destination-bucket"
                required
                value={startForm.destinationBucket}
                onChange={(event) => onFieldChange("destinationBucket", event.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-destination-key">Destination key/prefix (optional)</Label>
              <Input
                id="start-destination-key"
                placeholder="Optional file key or prefix"
                disabled={!sourceKeyPresent}
                value={startForm.destinationKey}
                onChange={(event) => onFieldChange("destinationKey", event.target.value)}
              />
              <p className="text-xs text-slate-500">
                Enabled only when source key is set. Leave empty to preserve source key/path.
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-source-endpoint">Source endpoint URL (optional)</Label>
              <Input
                id="start-source-endpoint"
                value={startForm.sourceEndpointUrl}
                onChange={(event) => onFieldChange("sourceEndpointUrl", event.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-destination-endpoint">Destination endpoint URL (optional)</Label>
              <Input
                id="start-destination-endpoint"
                value={startForm.destinationEndpointUrl}
                onChange={(event) => onFieldChange("destinationEndpointUrl", event.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-source-access-key">Source access key ID (optional)</Label>
              <Input
                id="start-source-access-key"
                value={startForm.sourceAccessKeyId}
                onChange={(event) => onFieldChange("sourceAccessKeyId", event.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-source-secret">Source secret access key (optional)</Label>
              <Input
                id="start-source-secret"
                type="password"
                value={startForm.sourceSecretAccessKey}
                onChange={(event) => onFieldChange("sourceSecretAccessKey", event.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-destination-access-key">Destination access key ID (optional)</Label>
              <Input
                id="start-destination-access-key"
                value={startForm.destinationAccessKeyId}
                onChange={(event) => onFieldChange("destinationAccessKeyId", event.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="start-destination-secret">Destination secret access key (optional)</Label>
              <Input
                id="start-destination-secret"
                type="password"
                value={startForm.destinationSecretAccessKey}
                onChange={(event) => onFieldChange("destinationSecretAccessKey", event.target.value)}
              />
            </div>

            <div className="sm:col-span-2 flex items-start gap-2 rounded-md border border-border bg-white p-3">
              <Checkbox
                id="start-auto-notification"
                checked={startForm.autoStartedNotification}
                onCheckedChange={(checked) => onFieldChange("autoStartedNotification", checked === true)}
              />
              <Label htmlFor="start-auto-notification" className="text-sm normal-case tracking-normal text-slate-700">
                Auto-send /started for PULL start
              </Label>
            </div>
          </div>

          <div className="flex justify-start">
            <Button type="submit">Start Transfer</Button>
          </div>
        </form>

        <StatusMessage kind={status.kind} message={status.message} fallback="Ready to send a new transfer." />
      </CardContent>
    </Card>
  );
}
