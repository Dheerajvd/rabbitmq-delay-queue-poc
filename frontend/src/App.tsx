import { useCallback, useEffect, useState } from "react";
import { clearJobs, getJob, listJobs, publishJob } from "./api";
import { JobDetail } from "./components/JobDetail";
import { JobTable } from "./components/JobTable";
import { PublishForm } from "./components/PublishForm";
import type { Job } from "./types";

type LiveState = "connecting" | "live" | "reconnecting";

export function App() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [selected, setSelected] = useState<Job | null>(null);
  const [loading, setLoading] = useState(true);
  const [publishing, setPublishing] = useState(false);
  const [clearing, setClearing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [publishError, setPublishError] = useState<string | null>(null);
  const [live, setLive] = useState<LiveState>("connecting");

  const refresh = useCallback(async () => {
    const listed = await listJobs();
    setJobs(listed);
    setSelected((current) => {
      if (!current) {
        return current;
      }
      const match = listed.find((job) => job.job_id === current.job_id);
      return match ? { ...current, ...match, events: current.events } : null;
    });
  }, []);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    refresh()
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Could not load jobs");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [refresh]);

  useEffect(() => {
    const source = new EventSource("/api/events");
    source.onopen = () => {
      setLive("live");
      refresh().catch((err: unknown) => {
        setError(err instanceof Error ? err.message : "Could not refresh jobs");
      });
    };
    source.onerror = () => setLive("reconnecting");
    source.addEventListener("job", (event) => {
      const job = JSON.parse((event as MessageEvent).data) as Job;
      setJobs((current) => mergeJob(current, job));
      setSelected((current) => (current && current.job_id === job.job_id ? job : current));
    });
    source.addEventListener("cleared", () => {
      setJobs([]);
      setSelected(null);
    });
    return () => source.close();
  }, [refresh]);

  async function handlePublish(message: string, processingDurationSeconds: number, delayMinutes: number) {
    setPublishing(true);
    setPublishError(null);
    try {
      const job = await publishJob({ message, processingDurationSeconds, delayMinutes });
      setJobs((current) => mergeJob(current, job));
      const detail = await getJob(job.job_id);
      setSelected(detail);
      setError(null);
    } catch (err: unknown) {
      setPublishError(err instanceof Error ? err.message : "Publish failed");
      refresh().catch(() => undefined);
    } finally {
      setPublishing(false);
    }
  }

  async function handleClear() {
    if (!window.confirm("Delete all jobs? This removes them from the database and cannot be undone.")) {
      return;
    }
    setClearing(true);
    setError(null);
    try {
      await clearJobs();
      setJobs([]);
      setSelected(null);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Could not delete jobs");
    } finally {
      setClearing(false);
    }
  }

  async function handleSelect(jobID: string) {
    const detail = await getJob(jobID);
    setSelected(detail);
  }

  return (
    <div className="page">
      <header>
        <div>
          <p className="eyebrow">RabbitMQ TTL + dead-letter exchange</p>
          <h1>Delay POC</h1>
        </div>
        <p className={`live live-${live}`}>{live === "live" ? "Live updates" : live === "connecting" ? "Connecting" : "Reconnecting"}</p>
      </header>
      <p className="lede">
        Jobs wait in <span className="mono">poc.delay.1m</span>, <span className="mono">poc.delay.2m</span>, or{" "}
        <span className="mono">poc.delay.5m</span>. RabbitMQ then dead-letters them to <span className="mono">poc.ready</span>.
        The countdown is a local estimate of <span className="mono">expected_ready_at</span>. It does not publish, consume, or
        change status.
      </p>
      {error ? <p className="banner error">{error}</p> : null}
      <div className="layout">
        <PublishForm disabled={publishing} error={publishError} onSubmit={handlePublish} />
        <section className="card explainer">
          <h2>Why ready can be late</h2>
          <ol>
            <li>The broker expires a message at or shortly after the queue TTL.</li>
            <li>Classic queues dead-letter expired messages when they reach the head of the queue.</li>
            <li>The consumer records <span className="mono">x-death</span> time separately from the moment it receives the delivery.</li>
          </ol>
          <p className="hint">Timestamps on screen are your local time. Hover or open a job to see the stored UTC value.</p>
        </section>
      </div>
      {loading ? <p className="hint">Loading jobs…</p> : null}
      <JobTable
        jobs={jobs}
        selectedID={selected?.job_id ?? null}
        clearing={clearing}
        onSelect={(jobID) => {
          handleSelect(jobID).catch((err: unknown) => {
            setError(err instanceof Error ? err.message : "Could not load job");
          });
        }}
        onRefresh={() => {
          refresh().catch((err: unknown) => {
            setError(err instanceof Error ? err.message : "Could not refresh jobs");
          });
        }}
        onClear={() => {
          handleClear().catch((err: unknown) => {
            setError(err instanceof Error ? err.message : "Could not delete jobs");
          });
        }}
      />
      <JobDetail job={selected} />
    </div>
  );
}

function mergeJob(jobs: Job[], job: Job): Job[] {
  const rest = jobs.filter((item) => item.job_id !== job.job_id);
  return [job, ...rest].sort((a, b) => (a.published_at < b.published_at ? 1 : -1));
}
