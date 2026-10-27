package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"rabbitmq-delay-poc/backend/internal/jobs"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned when a job id is absent.
var ErrNotFound = errors.New("job not found")

// Store persists jobs and an append-only event history in SQLite.
type Store struct {
	db *sql.DB
}

const jobColumns = `job_id, payload_json, delay_minutes, processing_duration_seconds,
	delay_queue, ready_queue, status, published_at, publish_confirmed_at, expected_ready_at,
	broker_ready_at, consumption_started_at, consumption_completed_at, processing_duration_ms,
	error, broker_warning, created_at, updated_at`

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("sqlite path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite directory: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	store := &Store{db: db}
	if err := store.configure(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Ping() error {
	return s.db.Ping()
}

func (s *Store) configure() error {
	pragmas := []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
	}
	for _, pragma := range pragmas {
		if _, err := s.db.Exec(pragma); err != nil {
			return fmt.Errorf("%s: %w", pragma, err)
		}
	}
	return nil
}

func (s *Store) migrate() error {
	schema := `
CREATE TABLE IF NOT EXISTS jobs (
	job_id TEXT PRIMARY KEY,
	payload_json TEXT NOT NULL,
	delay_minutes INTEGER NOT NULL,
	processing_duration_seconds INTEGER NOT NULL,
	delay_queue TEXT NOT NULL,
	ready_queue TEXT NOT NULL,
	status TEXT NOT NULL,
	published_at TEXT NOT NULL,
	publish_confirmed_at TEXT,
	expected_ready_at TEXT NOT NULL,
	broker_ready_at TEXT,
	consumption_started_at TEXT,
	consumption_completed_at TEXT,
	processing_duration_ms INTEGER,
	error TEXT,
	broker_warning TEXT,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS job_events (
	event_id INTEGER PRIMARY KEY AUTOINCREMENT,
	job_id TEXT NOT NULL REFERENCES jobs(job_id),
	event_type TEXT NOT NULL,
	event_timestamp TEXT NOT NULL,
	metadata_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events(job_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_published_at ON jobs(published_at);
`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("migrate sqlite: %w", err)
	}
	return nil
}

// CreateAttempt inserts the job and the PUBLISH_ATTEMPTED event atomically.
func (s *Store) CreateAttempt(ctx context.Context, job jobs.Job) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)

	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO jobs (
	job_id, payload_json, delay_minutes, processing_duration_seconds,
	delay_queue, ready_queue, status, published_at, publish_confirmed_at, expected_ready_at,
	broker_ready_at, consumption_started_at, consumption_completed_at, processing_duration_ms,
	error, broker_warning, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)`,
		job.JobID,
		string(payload),
		job.DelayMinutes,
		job.ProcessingDurationSeconds,
		job.DelayQueue,
		job.ReadyQueue,
		string(job.Status),
		formatTime(job.PublishedAt),
		formatTime(job.ExpectedReadyAt),
		formatTime(job.CreatedAt),
		formatTime(job.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	if err := insertEvent(ctx, tx, job.JobID, jobs.EventPublishAttempted, job.PublishedAt, map[string]any{
		"delay_minutes": job.DelayMinutes,
		"delay_queue":   job.DelayQueue,
		"ready_queue":   job.ReadyQueue,
	}); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkConfirmed moves PUBLISH_ATTEMPTED to WAITING after a positive confirm.
func (s *Store) MarkConfirmed(ctx context.Context, jobID string, confirmedAt time.Time) (jobs.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return jobs.Job{}, err
	}
	defer rollback(tx)

	job, err := getJobTx(ctx, tx, jobID)
	if err != nil {
		return jobs.Job{}, err
	}
	if job.Status == jobs.StatusWaiting && job.PublishConfirmedAt != nil {
		if err := tx.Commit(); err != nil {
			return jobs.Job{}, err
		}
		return s.Get(ctx, jobID)
	}
	if err := jobs.ValidateTransition(job.Status, jobs.StatusWaiting); err != nil {
		return jobs.Job{}, err
	}
	confirmedAt = confirmedAt.UTC()
	if _, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status = ?, publish_confirmed_at = ?, updated_at = ?, error = NULL
WHERE job_id = ?`,
		string(jobs.StatusWaiting), formatTime(confirmedAt), formatTime(confirmedAt), jobID,
	); err != nil {
		return jobs.Job{}, err
	}
	if err := insertEvent(ctx, tx, jobID, jobs.EventPublishConfirmed, confirmedAt, map[string]any{
		"queue": job.DelayQueue,
	}); err != nil {
		return jobs.Job{}, err
	}
	if err := tx.Commit(); err != nil {
		return jobs.Job{}, err
	}
	return s.Get(ctx, jobID)
}

// MarkPublishFailed records a nack, timeout, or broker error. It does not
// set publish_confirmed_at.
func (s *Store) MarkPublishFailed(ctx context.Context, jobID string, reason string, at time.Time) (jobs.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return jobs.Job{}, err
	}
	defer rollback(tx)

	job, err := getJobTx(ctx, tx, jobID)
	if err != nil {
		return jobs.Job{}, err
	}
	if job.Status == jobs.StatusPublishFailed {
		if err := tx.Commit(); err != nil {
			return jobs.Job{}, err
		}
		return s.Get(ctx, jobID)
	}
	if err := jobs.ValidateTransition(job.Status, jobs.StatusPublishFailed); err != nil {
		return jobs.Job{}, err
	}
	at = at.UTC()
	if _, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status = ?, error = ?, updated_at = ?
WHERE job_id = ? AND publish_confirmed_at IS NULL`,
		string(jobs.StatusPublishFailed), reason, formatTime(at), jobID,
	); err != nil {
		return jobs.Job{}, err
	}
	exists, err := hasEventTx(ctx, tx, jobID, jobs.EventPublishFailed)
	if err != nil {
		return jobs.Job{}, err
	}
	if !exists {
		if err := insertEvent(ctx, tx, jobID, jobs.EventPublishFailed, at, map[string]any{
			"error": reason,
		}); err != nil {
			return jobs.Job{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return jobs.Job{}, err
	}
	return s.Get(ctx, jobID)
}

// RecordReceipt stores broker dead-letter metadata and the consumer start
// time. Existing timestamps are left unchanged. alreadyComplete is true when
// the job was already completed and the caller should only acknowledge.
func (s *Store) RecordReceipt(ctx context.Context, jobID string, brokerReady *time.Time, warning string, now time.Time) (jobs.Job, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return jobs.Job{}, false, err
	}
	defer rollback(tx)

	job, err := getJobTx(ctx, tx, jobID)
	if err != nil {
		return jobs.Job{}, false, err
	}
	if job.Status == jobs.StatusCompleted {
		if err := tx.Commit(); err != nil {
			return jobs.Job{}, false, err
		}
		loaded, err := s.Get(ctx, jobID)
		return loaded, true, err
	}
	if job.Status != jobs.StatusProcessing {
		if err := jobs.ValidateTransition(job.Status, jobs.StatusProcessing); err != nil {
			return jobs.Job{}, false, err
		}
	}

	now = now.UTC()
	meta := map[string]any{}
	if brokerReady != nil && job.BrokerReadyAt == nil {
		t := brokerReady.UTC()
		job.BrokerReadyAt = &t
		job.BrokerWarning = nil
		meta["queue"] = job.DelayQueue
		meta["reason"] = "expired"
		meta["broker_ready_at"] = formatTime(t)
		exists, err := hasEventTx(ctx, tx, jobID, jobs.EventBrokerReady)
		if err != nil {
			return jobs.Job{}, false, err
		}
		if !exists {
			if err := insertEvent(ctx, tx, jobID, jobs.EventBrokerReady, t, meta); err != nil {
				return jobs.Job{}, false, err
			}
		}
	} else if brokerReady == nil && warning != "" && job.BrokerReadyAt == nil {
		job.BrokerWarning = &warning
		exists, err := hasEventTx(ctx, tx, jobID, jobs.EventBrokerReady)
		if err != nil {
			return jobs.Job{}, false, err
		}
		if !exists {
			if err := insertEvent(ctx, tx, jobID, jobs.EventBrokerReady, now, map[string]any{
				"warning": warning,
			}); err != nil {
				return jobs.Job{}, false, err
			}
		}
	}

	if job.ConsumptionStartedAt == nil {
		job.ConsumptionStartedAt = &now
		exists, err := hasEventTx(ctx, tx, jobID, jobs.EventConsumptionStarted)
		if err != nil {
			return jobs.Job{}, false, err
		}
		if !exists {
			if err := insertEvent(ctx, tx, jobID, jobs.EventConsumptionStarted, now, map[string]any{
				"queue": job.ReadyQueue,
			}); err != nil {
				return jobs.Job{}, false, err
			}
		}
	}
	job.Status = jobs.StatusProcessing
	job.UpdatedAt = now

	if err := updateLifecycleTx(ctx, tx, job); err != nil {
		return jobs.Job{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return jobs.Job{}, false, err
	}
	loaded, err := s.Get(ctx, jobID)
	return loaded, false, err
}

// MarkCompleted records a successful processing result once.
// duplicate is true when a completion event already exists; timestamps stay.
func (s *Store) MarkCompleted(ctx context.Context, jobID string, now time.Time) (jobs.Job, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return jobs.Job{}, false, err
	}
	defer rollback(tx)

	job, err := getJobTx(ctx, tx, jobID)
	if err != nil {
		return jobs.Job{}, false, err
	}
	if job.Status == jobs.StatusCompleted || job.ConsumptionCompletedAt != nil {
		if err := tx.Commit(); err != nil {
			return jobs.Job{}, false, err
		}
		loaded, err := s.Get(ctx, jobID)
		return loaded, true, err
	}
	if job.ConsumptionStartedAt == nil {
		return jobs.Job{}, false, fmt.Errorf("%w: completion without consumption start", jobs.ErrInvalidTransition)
	}
	if err := jobs.ValidateTransition(job.Status, jobs.StatusCompleted); err != nil {
		return jobs.Job{}, false, err
	}
	now = now.UTC()
	duration := now.Sub(*job.ConsumptionStartedAt).Milliseconds()
	job.ConsumptionCompletedAt = &now
	job.ProcessingDurationMs = &duration
	job.Status = jobs.StatusCompleted
	job.Error = nil
	job.UpdatedAt = now
	if err := updateLifecycleTx(ctx, tx, job); err != nil {
		return jobs.Job{}, false, err
	}
	if err := insertEvent(ctx, tx, jobID, jobs.EventConsumptionCompleted, now, map[string]any{
		"processing_duration_ms": duration,
	}); err != nil {
		return jobs.Job{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return jobs.Job{}, false, err
	}
	loaded, err := s.Get(ctx, jobID)
	return loaded, false, err
}

// MarkConsumptionFailed records a terminal processing failure. It does not
// set consumption_completed_at.
func (s *Store) MarkConsumptionFailed(ctx context.Context, jobID, reason string, now time.Time) (jobs.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return jobs.Job{}, err
	}
	defer rollback(tx)

	job, err := getJobTx(ctx, tx, jobID)
	if err != nil {
		return jobs.Job{}, err
	}
	if job.Status == jobs.StatusConsumptionFailed {
		if err := tx.Commit(); err != nil {
			return jobs.Job{}, err
		}
		return s.Get(ctx, jobID)
	}
	if err := jobs.ValidateTransition(job.Status, jobs.StatusConsumptionFailed); err != nil {
		return jobs.Job{}, err
	}
	now = now.UTC()
	job.Status = jobs.StatusConsumptionFailed
	job.Error = &reason
	job.UpdatedAt = now
	if err := updateLifecycleTx(ctx, tx, job); err != nil {
		return jobs.Job{}, err
	}
	exists, err := hasEventTx(ctx, tx, jobID, jobs.EventConsumptionFailed)
	if err != nil {
		return jobs.Job{}, err
	}
	if !exists {
		if err := insertEvent(ctx, tx, jobID, jobs.EventConsumptionFailed, now, map[string]any{
			"error": reason,
		}); err != nil {
			return jobs.Job{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return jobs.Job{}, err
	}
	return s.Get(ctx, jobID)
}

func (s *Store) Get(ctx context.Context, jobID string) (jobs.Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE job_id = ?`, jobID)
	job, err := scanJob(row)
	if err != nil {
		return jobs.Job{}, err
	}
	events, err := s.listEvents(ctx, jobID)
	if err != nil {
		return jobs.Job{}, err
	}
	job.Events = events
	return job, nil
}

// DeleteAll removes every job and its event history.
func (s *Store) DeleteAll(ctx context.Context) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)

	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&count); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM job_events`); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM jobs`); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) List(ctx context.Context, status string) ([]jobs.Job, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs`
	args := []any{}
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY published_at DESC, job_id DESC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []jobs.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	if out == nil {
		out = []jobs.Job{}
	}
	return out, rows.Err()
}

func (s *Store) listEvents(ctx context.Context, jobID string) ([]jobs.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT event_id, job_id, event_type, event_timestamp, metadata_json
FROM job_events WHERE job_id = ? ORDER BY event_id ASC`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []jobs.Event
	for rows.Next() {
		var event jobs.Event
		var stamp, meta string
		if err := rows.Scan(&event.EventID, &event.JobID, &event.EventType, &stamp, &meta); err != nil {
			return nil, err
		}
		parsed, err := parseTime(stamp)
		if err != nil {
			return nil, err
		}
		event.EventTimestamp = parsed
		event.Metadata = map[string]any{}
		if err := json.Unmarshal([]byte(meta), &event.Metadata); err != nil {
			return nil, fmt.Errorf("event metadata: %w", err)
		}
		out = append(out, event)
	}
	if out == nil {
		out = []jobs.Event{}
	}
	return out, rows.Err()
}

func updateLifecycleTx(ctx context.Context, tx *sql.Tx, job jobs.Job) error {
	_, err := tx.ExecContext(ctx, `
UPDATE jobs SET
	status = ?,
	publish_confirmed_at = ?,
	broker_ready_at = ?,
	consumption_started_at = ?,
	consumption_completed_at = ?,
	processing_duration_ms = ?,
	error = ?,
	broker_warning = ?,
	updated_at = ?
WHERE job_id = ?`,
		string(job.Status),
		nullTime(job.PublishConfirmedAt),
		nullTime(job.BrokerReadyAt),
		nullTime(job.ConsumptionStartedAt),
		nullTime(job.ConsumptionCompletedAt),
		nullInt(job.ProcessingDurationMs),
		nullString(job.Error),
		nullString(job.BrokerWarning),
		formatTime(job.UpdatedAt),
		job.JobID,
	)
	return err
}

func getJobTx(ctx context.Context, tx *sql.Tx, jobID string) (jobs.Job, error) {
	row := tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE job_id = ?`, jobID)
	return scanJob(row)
}

func hasEventTx(ctx context.Context, tx *sql.Tx, jobID, eventType string) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM job_events WHERE job_id = ? AND event_type = ?)`, jobID, eventType).Scan(&exists)
	return exists == 1, err
}

func insertEvent(ctx context.Context, tx *sql.Tx, jobID, eventType string, at time.Time, metadata map[string]any) error {
	if metadata == nil {
		metadata = map[string]any{}
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO job_events (job_id, event_type, event_timestamp, metadata_json)
VALUES (?, ?, ?, ?)`, jobID, eventType, formatTime(at.UTC()), string(raw))
	if err != nil {
		return fmt.Errorf("insert event %s: %w", eventType, err)
	}
	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row scanner) (jobs.Job, error) {
	var job jobs.Job
	var payload, status, published, expected, created, updated string
	var confirmed, broker, started, completed sql.NullString
	var proc sql.NullInt64
	var errText, warning sql.NullString
	err := row.Scan(
		&job.JobID,
		&payload,
		&job.DelayMinutes,
		&job.ProcessingDurationSeconds,
		&job.DelayQueue,
		&job.ReadyQueue,
		&status,
		&published,
		&confirmed,
		&expected,
		&broker,
		&started,
		&completed,
		&proc,
		&errText,
		&warning,
		&created,
		&updated,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, ErrNotFound
	}
	if err != nil {
		return jobs.Job{}, err
	}
	if err := json.Unmarshal([]byte(payload), &job.Payload); err != nil {
		return jobs.Job{}, fmt.Errorf("payload: %w", err)
	}
	job.Status = jobs.Status(status)
	job.PublishedAt, err = parseTime(published)
	if err != nil {
		return jobs.Job{}, err
	}
	job.ExpectedReadyAt, err = parseTime(expected)
	if err != nil {
		return jobs.Job{}, err
	}
	job.CreatedAt, err = parseTime(created)
	if err != nil {
		return jobs.Job{}, err
	}
	job.UpdatedAt, err = parseTime(updated)
	if err != nil {
		return jobs.Job{}, err
	}
	job.PublishConfirmedAt, err = optionalTime(confirmed)
	if err != nil {
		return jobs.Job{}, err
	}
	job.BrokerReadyAt, err = optionalTime(broker)
	if err != nil {
		return jobs.Job{}, err
	}
	job.ConsumptionStartedAt, err = optionalTime(started)
	if err != nil {
		return jobs.Job{}, err
	}
	job.ConsumptionCompletedAt, err = optionalTime(completed)
	if err != nil {
		return jobs.Job{}, err
	}
	if proc.Valid {
		value := proc.Int64
		job.ProcessingDurationMs = &value
	}
	if errText.Valid {
		value := errText.String
		job.Error = &value
	}
	if warning.Valid {
		value := warning.String
		job.BrokerWarning = &value
	}
	return job, nil
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse timestamp %q: %w", value, err)
	}
	return parsed.UTC(), nil
}

func optionalTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid || value.String == "" {
		return nil, nil
	}
	parsed, err := parseTime(value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func nullTime(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return formatTime(value.UTC())
}

func nullString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullInt(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}
