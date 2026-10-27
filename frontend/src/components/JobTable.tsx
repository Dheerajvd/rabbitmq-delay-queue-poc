import type { Job } from "../types";
import { formatDuration, formatLocal } from "../format";
import { Countdown } from "./Countdown";

interface JobTableProps {
  jobs: Job[];
  selectedID: string | null;
  clearing: boolean;
  onSelect: (jobID: string) => void;
  onRefresh: () => void;
  onClear: () => void;
}

export function JobTable({ jobs, selectedID, clearing, onSelect, onRefresh, onClear }: JobTableProps) {
  return (
    <section className="card table-card">
      <div className="table-toolbar">
        <h2>Jobs</h2>
        <div className="toolbar-actions">
          <button type="button" className="secondary" onClick={onRefresh}>
            Refresh
          </button>
          <button type="button" className="danger" onClick={onClear} disabled={clearing || jobs.length === 0}>
            {clearing ? "Clearing…" : "Clear"}
          </button>
        </div>
      </div>
      {jobs.length === 0 ? (
        <p className="empty">No jobs yet. Publish a sample job to start a delayed message.</p>
      ) : (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Job ID</th>
                <th>Status</th>
                <th>Delay queue</th>
                <th>Time remaining</th>
                <th>Publish attempted</th>
                <th>Broker publish confirmed</th>
                <th>Expected ready time</th>
                <th>Broker-reported dead-letter time</th>
                <th>Consumer started</th>
                <th>Processing completed</th>
                <th>Processing duration</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr
                  key={job.job_id}
                  className={job.job_id === selectedID ? "selected" : undefined}
                  onClick={() => onSelect(job.job_id)}
                >
                  <td className="mono">{job.job_id.slice(0, 8)}</td>
                  <td>
                    <span className={`badge status-${job.status}`}>{job.status}</span>
                  </td>
                  <td className="mono">{job.delay_queue}</td>
                  <td>
                    <Countdown status={job.status} expectedReadyAt={job.expected_ready_at} />
                  </td>
                  <td>
                    <Timestamp iso={job.published_at} />
                  </td>
                  <td>
                    <Timestamp iso={job.publish_confirmed_at} />
                  </td>
                  <td>
                    <Timestamp iso={job.expected_ready_at} estimate />
                  </td>
                  <td>
                    <Timestamp iso={job.broker_ready_at} />
                  </td>
                  <td>
                    <Timestamp iso={job.consumption_started_at} />
                  </td>
                  <td>
                    <Timestamp iso={job.consumption_completed_at} />
                  </td>
                  <td>{formatDuration(job.processing_duration_ms)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function Timestamp({ iso, estimate = false }: { iso: string | null; estimate?: boolean }) {
  if (!iso) {
    return <span className="muted">Pending</span>;
  }
  return (
    <span className={estimate ? "stamp estimate" : "stamp"} title={iso}>
      {formatLocal(iso)}
    </span>
  );
}
