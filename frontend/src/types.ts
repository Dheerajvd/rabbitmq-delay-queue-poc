export type JobStatus =
  | "PUBLISH_ATTEMPTED"
  | "WAITING"
  | "PROCESSING"
  | "COMPLETED"
  | "PUBLISH_FAILED"
  | "CONSUMPTION_FAILED";

export interface JobEvent {
  event_id: number;
  job_id: string;
  event_type: string;
  event_timestamp: string;
  metadata: Record<string, unknown>;
}

export interface Metrics {
  requested_delay_ms: number;
  publish_attempt_to_consumption_start_ms: number | null;
  expected_ready_vs_consumer_start_ms: number | null;
  broker_dead_letter_vs_consumer_start_ms: number | null;
  processing_duration_ms: number | null;
  publish_attempt_to_completion_ms: number | null;
}

export interface Job {
  job_id: string;
  payload: { message: string };
  delay_minutes: number;
  processing_duration_seconds: number;
  delay_queue: string;
  ready_queue: string;
  status: JobStatus;
  published_at: string;
  publish_confirmed_at: string | null;
  expected_ready_at: string;
  broker_ready_at: string | null;
  consumption_started_at: string | null;
  consumption_completed_at: string | null;
  processing_duration_ms: number | null;
  error: string | null;
  broker_warning: string | null;
  created_at: string;
  updated_at: string;
  events?: JobEvent[];
  metrics?: Metrics;
}

export interface PublishInput {
  message: string;
  processingDurationSeconds: number;
  delayMinutes: number;
}
