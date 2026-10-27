/** Visual countdown only. It never decides that a job is ready. */
export function countdownLabel(expectedReadyAt: string, nowMs: number): { text: string; elapsed: boolean } {
  const target = new Date(expectedReadyAt).getTime();
  if (Number.isNaN(target)) {
    return { text: "Expected time unavailable", elapsed: false };
  }
  const diff = target - nowMs;
  if (diff <= 0) {
    return { text: "Expected delay elapsed; awaiting RabbitMQ delivery", elapsed: true };
  }
  const totalSeconds = Math.ceil(diff / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return { text: `${minutes}:${seconds.toString().padStart(2, "0")}`, elapsed: false };
}

export function formatLocal(iso: string | null | undefined): string {
  if (!iso) {
    return "Pending";
  }
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return iso;
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(date);
}

export function formatDuration(ms: number | null | undefined): string {
  if (ms == null) {
    return "Pending";
  }
  const sign = ms < 0 ? "-" : "";
  const abs = Math.abs(ms);
  if (abs < 1000) {
    return `${sign}${abs} ms`;
  }
  const seconds = abs / 1000;
  if (seconds < 60) {
    return `${sign}${seconds.toFixed(1)} s`;
  }
  const minutes = Math.floor(seconds / 60);
  const rest = seconds - minutes * 60;
  return `${sign}${minutes}m ${rest.toFixed(1)}s`;
}
