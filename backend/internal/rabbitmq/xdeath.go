package rabbitmq

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Death is one x-death array entry. Time is RabbitMQ's dead-letter event
// time for that queue and reason (AMQP timestamp, second resolution).
type Death struct {
	Queue   string
	Reason  string
	Time    time.Time
	Count   int64
	HasTime bool
}

// ExtractExpiredDeath finds the TTL expiration entry for delayQueue.
// The returned warning is set when the timestamp must stay null.
// A warning is not a fabricated timestamp.
func ExtractExpiredDeath(headers amqp.Table, delayQueue string) (*time.Time, string, Death) {
	if headers == nil {
		return nil, "x-death header missing", Death{}
	}
	raw, ok := headers["x-death"]
	if !ok || raw == nil {
		return nil, "x-death header missing", Death{}
	}
	entries, err := parseDeaths(raw)
	if err != nil {
		return nil, "x-death metadata is malformed: " + err.Error(), Death{}
	}
	if len(entries) == 0 {
		return nil, "x-death metadata is empty", Death{}
	}
	for _, entry := range entries {
		if entry.Queue != delayQueue || entry.Reason != "expired" {
			continue
		}
		if !entry.HasTime || entry.Time.IsZero() {
			return nil, "x-death expired event for " + delayQueue + " is missing a timestamp", entry
		}
		t := entry.Time.UTC()
		return &t, "", entry
	}
	return nil, "x-death has no expired event for queue " + delayQueue, Death{}
}

func parseDeaths(raw any) ([]Death, error) {
	var items []any
	switch typed := raw.(type) {
	case []any:
		items = typed
	case amqp.Table:
		items = []any{typed}
	default:
		return nil, fmt.Errorf("unexpected type %T", raw)
	}
	out := make([]Death, 0, len(items))
	for _, item := range items {
		table, err := asTable(item)
		if err != nil {
			return nil, err
		}
		death := Death{
			Queue:  asString(table["queue"]),
			Reason: asString(table["reason"]),
			Count:  asInt64(table["count"]),
		}
		if t, ok := asTime(table["time"]); ok {
			death.Time = t.UTC()
			death.HasTime = true
		}
		out = append(out, death)
	}
	return out, nil
}

func asTable(item any) (amqp.Table, error) {
	switch typed := item.(type) {
	case amqp.Table:
		return typed, nil
	case map[string]any:
		return amqp.Table(typed), nil
	default:
		return nil, fmt.Errorf("unexpected x-death entry type %T", item)
	}
}

func asString(value any) string {
	text, _ := value.(string)
	return text
}

func asInt64(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int8:
		return int64(typed)
	case int16:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case uint8:
		return int64(typed)
	case uint16:
		return int64(typed)
	case uint32:
		return int64(typed)
	case uint64:
		return int64(typed)
	case float32:
		return int64(typed)
	case float64:
		return int64(typed)
	default:
		return 0
	}
}

func asTime(value any) (time.Time, bool) {
	switch typed := value.(type) {
	case time.Time:
		if typed.IsZero() {
			return time.Time{}, false
		}
		return typed.UTC(), true
	case int64:
		if typed <= 0 {
			return time.Time{}, false
		}
		return time.Unix(typed, 0).UTC(), true
	case int32:
		if typed <= 0 {
			return time.Time{}, false
		}
		return time.Unix(int64(typed), 0).UTC(), true
	case int:
		if typed <= 0 {
			return time.Time{}, false
		}
		return time.Unix(int64(typed), 0).UTC(), true
	default:
		return time.Time{}, false
	}
}
