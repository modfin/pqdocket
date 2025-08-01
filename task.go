package pqdocket

import (
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/guregu/null/v6"
	"log/slog"
	"sync"
	"time"
)

type Task interface {
	TaskId() string
	RefId() null.String
	BindMetadata(dest interface{}) error
	CompletedAt() null.Time
	ClaimCount() int
	CreatedAt() time.Time
	Func() string

	// Delete deletes a task, if not claimed currently
	Delete(tx *sql.Tx) error

	Docket() Docket
}

type RunningTask interface {
	Task

	// CompleteWithTransaction completes the task within the given transaction,
	// so if the transaction is successfully committed,
	// the task is also marked complete with it.
	CompleteWithTransaction(tx *sql.Tx) error

	// StillClaimed checks if claim time is still valid
	// note: doesn't contact postgres
	StillClaimed() bool

	// ExtendClaim extends the claim time of a running task if the task is still claimed.
	// Returns the updated claim time. Use this for long-running tasks that want to run for an arbitrary time.
	ExtendClaim(duration time.Duration) (*time.Time, error)

	// CancelAndDelete deletes a running and claimed Task.
	// Used for aborting a Task from within.
	// After successfully calling this, no other operations on this Task are valid
	CancelAndDelete(tx *sql.Tx) error
}

type TaskCreator struct {
	task task
}

func (tc TaskCreator) WithClaimTimeSeconds(seconds int) TaskCreator {
	tc.task.claimTimeSeconds = seconds
	return tc
}

func (tc TaskCreator) ScheduleAt(at time.Time) TaskCreator {
	tc.task.scheduledAt = at
	return tc
}

type Metadata []byte

func CreateMetadata(md interface{}) (Metadata, error) {
	return json.Marshal(md)
}

func (tc TaskCreator) WithMetadata(md Metadata) TaskCreator {
	tc.task.metadata = null.ValueFrom[[]byte](md)
	return tc
}

func (tc TaskCreator) WithRefId(refId string) TaskCreator {
	tc.task.refId = null.StringFrom(refId)
	return tc
}

func (d *docket) CreateTask(f TaskFunction) TaskCreator {
	fName := funcName(f)
	return d.CreateTaskWithFuncName(fName)
}

func (d *docket) CreateTaskWithFuncName(funcName string) TaskCreator {
	d.mu.RLock()
	_, ok := d.functions[funcName]
	d.mu.RUnlock()
	if !ok {
		panic("functions need to be registered before being used to create tasks")
	}
	tc := TaskCreator{}
	tc.task.docket = d
	tc.task.function = funcName
	return tc
}

type task struct {
	docket *docket

	mutable *struct {
		mu           sync.RWMutex
		claimedUntil null.Time
	}

	claimedByThisProcess bool
	taskId               string
	refId                null.String
	function             string
	metadata             null.Value[[]byte]
	claimCount           int
	lastError            null.String
	claimTimeSeconds     int
	scheduledAt          time.Time
	completedAt          null.Time
	createdAt            time.Time
}

func newTask() task {
	var t task
	t.mutable = &struct {
		mu           sync.RWMutex
		claimedUntil null.Time
	}{}
	return t
}

func scanTaskFrom(rows *sql.Rows) (task, error) {
	t := newTask()
	t.mutable.mu.Lock()
	defer t.mutable.mu.Unlock()
	err := rows.Scan(
		&t.taskId,
		&t.refId,
		&t.function,
		&t.metadata,
		&t.claimCount,
		&t.lastError,
		&t.claimTimeSeconds,
		&t.mutable.claimedUntil,
		&t.scheduledAt,
		&t.completedAt,
		&t.createdAt,
	)
	return t, err
}

func (tc TaskCreator) preparedTask() task {
	t := tc.task
	if t.scheduledAt.IsZero() {
		t.scheduledAt = time.Now()
	}
	if t.claimTimeSeconds < 5 {
		t.claimTimeSeconds = t.docket.defaultClaimTime
	}
	return t
}

func (t task) TaskId() string {
	return t.taskId
}

func (t task) RefId() null.String {
	return t.refId
}

func (t task) CompletedAt() null.Time {
	return t.completedAt
}

func (t task) StillClaimed() bool {
	claimedUntil := t.getClaimedUntil()
	return t.claimedByThisProcess && claimedUntil.Valid && claimedUntil.Time.After(time.Now())
}

func (t task) BindMetadata(dest any) error {
	if t.metadata.Valid {
		return json.Unmarshal(t.metadata.V, dest)
	}
	return errors.New("metadata is null")
}

func (t task) Docket() Docket {
	return t.docket
}

func (t task) ClaimCount() int {
	return t.claimCount
}

func (t task) CreatedAt() time.Time {
	return t.createdAt
}

func (t task) Func() string {
	return t.function
}

func (t task) logger() *slog.Logger {
	l := t.docket.logger.Load().With("task_id", t.taskId, "claim_count", t.claimCount, "task_func", t.function)
	if t.refId.Valid {
		l = l.With("ref_id", t.refId.String)
	}
	return l
}

func (t task) setClaimedUntil(newTime null.Time) {
	t.mutable.mu.Lock()
	defer t.mutable.mu.Unlock()
	t.mutable.claimedUntil = newTime
}

func (t task) getClaimedUntil() null.Time {
	t.mutable.mu.RLock()
	defer t.mutable.mu.RUnlock()
	return t.mutable.claimedUntil
}
