import type { Job, PublishInput } from "./types";

async function readError(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { error?: string };
    return body.error || response.statusText;
  } catch {
    return response.statusText;
  }
}

export async function publishJob(input: PublishInput): Promise<Job> {
  const response = await fetch("/api/jobs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      delay_minutes: input.delayMinutes,
      processing_duration_seconds: input.processingDurationSeconds,
      payload: { message: input.message },
    }),
  });
  if (!response.ok) {
    throw new Error(await readError(response));
  }
  return (await response.json()) as Job;
}

export async function listJobs(): Promise<Job[]> {
  const response = await fetch("/api/jobs");
  if (!response.ok) {
    throw new Error(await readError(response));
  }
  const body = (await response.json()) as { jobs: Job[] };
  return body.jobs ?? [];
}

export async function clearJobs(): Promise<number> {
  const response = await fetch("/api/jobs", { method: "DELETE" });
  if (!response.ok) {
    throw new Error(await readError(response));
  }
  const body = (await response.json()) as { deleted: number };
  return body.deleted;
}

export async function getJob(jobID: string): Promise<Job> {
  const response = await fetch(`/api/jobs/${jobID}`);
  if (!response.ok) {
    throw new Error(await readError(response));
  }
  return (await response.json()) as Job;
}
