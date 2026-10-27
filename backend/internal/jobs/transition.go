package jobs

import "fmt"

// ErrInvalidTransition means a lifecycle move would skip or rewind state.
var ErrInvalidTransition = fmt.Errorf("invalid lifecycle transition")

// ErrDuplicateCompleted means the job already has a successful completion.
var ErrDuplicateCompleted = fmt.Errorf("job already completed")

// allowed lists forward moves. PROCESSING to PROCESSING is not a new
// transition: a redelivery keeps the original timestamps.
var allowed = map[Status]map[Status]bool{
	StatusPublishAttempted: {
		StatusWaiting:       true,
		StatusPublishFailed: true,
		// Recovery when the broker accepted the message but the confirm
		// row was not durable yet. The consumer is the evidence.
		StatusProcessing: true,
	},
	StatusWaiting: {
		StatusProcessing:        true,
		StatusConsumptionFailed: true,
	},
	StatusProcessing: {
		StatusCompleted:         true,
		StatusConsumptionFailed: true,
	},
}

// ValidateTransition reports whether status may move from -> to.
func ValidateTransition(from, to Status) error {
	if from == to && from == StatusProcessing {
		return nil
	}
	next := allowed[from]
	if next != nil && next[to] {
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
}

// IsTerminal reports statuses that must not be overwritten by a later delivery.
func IsTerminal(status Status) bool {
	return status == StatusCompleted || status == StatusPublishFailed || status == StatusConsumptionFailed
}
