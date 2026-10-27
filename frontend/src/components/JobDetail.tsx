import type { Job } from "../types";
import { formatDuration, formatLocal } from "../format";
import { Countdown } from "./Countdown";

interface JobDetailProps {
  job: Job | null;
}

interface Step {
  label: string;
  iso: string | null;
  kind: "actual" | "estimate";
  pending: boolean;
}

export function JobDetail({ job }: JobDetailProps) {
  if (!job) {
    return (
      <section className="card detail">
        <h2>Lifecycle</h2>
        <p className="empty">Select a job to inspect timestamps, metrics, and broker warnings.</p>
      </section>
    );
  }

  const steps: Step[] = [
    { label: "Publish attempted", iso: job.published_at, kind: "actual", pending: false },
    { label: "Broker publish confirmed", iso: job.publish_confirmed_at, kind: "actual", pending: !job.publish_confirmed_at },
    { label: "Expected ready time", iso: job.expected_ready_at, kind: "estimate", pending: false },
    { label: "Broker-reported dead-letter time", iso: job.broker_ready_at, kind: "actual", pending: !job.broker_ready_at },
    {
      label: "Consumer received / processing started",
      iso: job.consumption_started_at,
      kind: "actual",
      pending: !job.consumption_started_at,
    },
    { label: "Processing completed", iso: job.consumption_completed_at, kind: "actual", pending: !job.consumption_completed_at },
  ];

  return (
    <section className="card detail">
      <div className="detail-head">
        <div>
          <h2>Lifecycle</h2>
          <p className="mono job-id">{job.job_id}</p>
        </div>
        <span className={`badge status-${job.status}`}>{job.status}</span>
      </div>
      <p className="payload">{job.payload.message}</p>
      <p className="hint">
        Queues {job.delay_queue} → {job.ready_queue}. Requested delay {job.delay_minutes}{" "}
        minute{job.delay_minutes === 1 ? "" : "s"}. Simulated processing{" "}
        {job.processing_duration_seconds}s.
      </p>
      {job.status === "WAITING" ? (
        <p className="countdown-row">
          Time remaining <Countdown status={job.status} expectedReadyAt={job.expected_ready_at} />
        </p>
      ) : null}
      {job.broker_warning ? <p className="banner warn">{job.broker_warning}</p> : null}
      {job.error ? <p className="banner error">{job.error}</p> : null}
      <p className="hint">
        A countdown reaching zero does not mark the job ready. RabbitMQ may dead-letter the message slightly after TTL,
        once it reaches the head of <span className="mono">{job.delay_queue}</span>. Only a consumer receipt updates the lifecycle.
      </p>
      <ol className="timeline">
        {steps.map((step) => (
          <li key={step.label} className={`step ${step.kind} ${step.pending ? "pending" : "done"}`}>
            <div className="step-label">{step.label}</div>
            <div className="step-kind">{step.kind === "estimate" ? "Estimate" : step.pending ? "Pending" : "Observed"}</div>
            <div className="step-time" title={step.iso ?? ""}>
              {step.pending ? "Pending" : formatLocal(step.iso)}
            </div>
            {step.iso && !step.pending ? <div className="utc">{step.iso}</div> : null}
          </li>
        ))}
      </ol>
      <h3>Timing metrics</h3>
      <dl className="metrics">
        <div>
          <dt>Requested delay</dt>
          <dd>
            {job.delay_minutes} minute{job.delay_minutes === 1 ? "" : "s"} ({job.metrics?.requested_delay_ms ?? job.delay_minutes * 60000} ms)
          </dd>
        </div>
        <div>
          <dt>Publish attempt to consumer start</dt>
          <dd>{formatDuration(job.metrics?.publish_attempt_to_consumption_start_ms)}</dd>
        </div>
        <div>
          <dt>Expected ready vs consumer start</dt>
          <dd>
            {formatDuration(job.metrics?.expected_ready_vs_consumer_start_ms)}
            <span className="hint"> Positive means the consumer started after the estimate.</span>
          </dd>
        </div>
        <div>
          <dt>Broker dead-letter vs consumer start</dt>
          <dd>{formatDuration(job.metrics?.broker_dead_letter_vs_consumer_start_ms)}</dd>
        </div>
        <div>
          <dt>Processing duration</dt>
          <dd>{formatDuration(job.metrics?.processing_duration_ms ?? job.processing_duration_ms)}</dd>
        </div>
        <div>
          <dt>Publish attempt to completion</dt>
          <dd>{formatDuration(job.metrics?.publish_attempt_to_completion_ms)}</dd>
        </div>
      </dl>
      {job.events && job.events.length > 0 ? (
        <>
          <h3>Event history</h3>
          <ul className="events">
            {job.events.map((event) => (
              <li key={event.event_id}>
                <span className="mono">{event.event_type}</span>
                <span title={event.event_timestamp}>{formatLocal(event.event_timestamp)}</span>
                <code>{JSON.stringify(event.metadata)}</code>
              </li>
            ))}
          </ul>
        </>
      ) : null}
    </section>
  );
}
