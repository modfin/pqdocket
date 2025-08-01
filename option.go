package pqdocket

import (
	"database/sql"
	"log/slog"
	"time"
)

type Option func(docket *docket)

// Namespace specifies namespace, allowing multiple independent Docket systems to coexist in the same database.
// Basically, it just adds a suffix to the default "pqdocket_task" table.
func Namespace(namespace string) Option {
	return func(docket *docket) {
		docket.namespace = namespace
	}
}

// Parallelism specifies the max number of tasks that can run in parallel.
// Requires 2+parallelism free db connections to work well.
func Parallelism(parallelism int) Option {
	return func(docket *docket) {
		docket.parallelism = parallelism
	}
}

// DefaultClaimTime sets the default claim time for created tasks. Can be overridden on Task creation.
func DefaultClaimTime(defaultClaimTimeSeconds int) Option {
	return func(docket *docket) {
		docket.defaultClaimTime = defaultClaimTimeSeconds
	}
}

// PollInterval sets how often to poll for new tasks to run
// this is mostly a backup in the case wake up by pq_notify is not working correctly.
// Note that ±5% random drift is added to his duration.
func PollInterval(pollInterval time.Duration) Option {
	return func(docket *docket) {
		docket.pollInterval = pollInterval
	}
}

// MaxRetryBackoff time in seconds.
// We backoff exponentially starting with 4, 8, 16, 32, ... seconds for the first retries
// Use this to specify the max retry interval (default is 128 seconds).
func MaxRetryBackoff(maxRetryBackoffSeconds int) Option {
	return func(docket *docket) {
		docket.maxRetryBackoff = maxRetryBackoffSeconds
	}
}

// MinRetryBackoff time in seconds.
// We backoff exponentially starting with 4, 8, 16, 32, ... seconds for the first retries
// Use this to specify the min retry interval (default is 4 seconds).
func MinRetryBackoff(minRetryBackoffSeconds int) Option {
	return func(docket *docket) {
		docket.minRetryBackoff = minRetryBackoffSeconds
	}
}

// MaxClaimCount - maximum number of time a task can be claimed, until retries stop.
// default is math.MaxInt32.
func MaxClaimCount(maxClaimCount int) Option {
	return func(docket *docket) {
		docket.maxClaimCount = maxClaimCount
	}
}

// EnableTaskCleaner enabled an automatic task cleaner for completed tasks that are older than the specified MaxAge.
// If MaxAge is left at 0, tasks are deleted after they are completed.
func EnableTaskCleaner(cleaner TaskCleanerSettings) Option {
	return func(docket *docket) {
		docket.taskCleaner = &cleaner
	}
}

// WithLogger enables logging.
func WithLogger(logger *slog.Logger) Option {
	return func(docket *docket) {
		docket.setLogger(logger)
	}
}

// AfterTableInit - Callback with a handle to *sql.DB that executes after task table is created.
// The callback MUST be safe to execute more than once as the callback will execute each time the service starts
// Intended to enable customized DDL, e.g., to create indices.
func AfterTableInit(fn func(db *sql.DB, tableName string) error) Option {
	return func(docket *docket) {
		docket.afterTableInitCallback = fn
	}
}

// UseManuallyCreatedTable - table created must match expected schema, bypasses automatic creation of table.
func UseManuallyCreatedTable(tableName string) Option {
	return func(docket *docket) {
		docket.useManuallyCreatedTable = tableName
	}
}
