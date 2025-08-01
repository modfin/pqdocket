package pqdocket

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/guregu/null/v6"
	"github.com/lib/pq"
	"log/slog"
	"math"
	"strings"
	"time"
)

const taskTableName = "pqdocket_task"

func (d *docket) initTables() error {
	if d.useManuallyCreatedTable != "" {
		return nil
	}
	q := `
	CREATE TABLE IF NOT EXISTS pqdocket_task (
		task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		ref_id UUID,
		func TEXT NOT NULL,
		metadata JSONB,
		claim_count INT NOT NULL DEFAULT 0,
		last_error TEXT,
		claim_time_seconds INT NOT NULL,
		claimed_until TIMESTAMPTZ,
		scheduled_at TIMESTAMPTZ NOT NULL,
		completed_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		UNIQUE (ref_id, func)
	)`
	q2 := `
	CREATE INDEX CONCURRENTLY IF NOT EXISTS pqdocket_task_scheduled_at_idx
	ON pqdocket_task (scheduled_at ASC) WHERE completed_at IS NULL
	`
	_, err := d.db.Exec(strings.Replace(q, taskTableName, d.tableName(), 1))
	if err != nil {
		return err
	}
	_, err = d.db.Exec(strings.Replace(q2, taskTableName, d.tableName(), 2))
	if err != nil {
		return err
	}
	if d.afterTableInitCallback == nil {
		return nil
	}
	return d.afterTableInitCallback(d.db, d.tableName())
}

// InsertTasks - supports max 65535/5 = 13,107 tasks in one batch, since postgres supports max 65535 arguments per statement
// if you need more, execute multiple InsertTasks in the same transaction
func (d *docket) InsertTasks(tx *sql.Tx, tcs ...TaskCreator) ([]Task, error) {
	shouldNotify := false
	var placeholders []string
	var args []interface{}
	for _, tc := range tcs {
		t := tc.preparedTask()
		if t.docket != d {
			return nil, errors.New("tried to insert task from another pqdocket instance")
		}

		if t.scheduledAt.Before(time.Now().Add(d.pollInterval)) {
			shouldNotify = true
		}

		b := 5 * len(placeholders)
		p := fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", b+1, b+2, b+3, b+4, b+5)
		placeholders = append(placeholders, p)
		args = append(args, t.scheduledAt, t.refId, t.function, t.metadata, t.claimTimeSeconds)
	}
	q := `
		INSERT INTO pqdocket_task(scheduled_at, ref_id, func, metadata, claim_time_seconds)
		VALUES %s
		RETURNING *
	`
	q = fmt.Sprintf(q, strings.Join(placeholders, ", "))
	q = strings.Replace(q, taskTableName, d.tableName(), 1)

	var rows *sql.Rows
	var err error
	if tx == nil {
		rows, err = d.db.Query(q, args...)
	} else {
		rows, err = tx.Query(q, args...)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		t, err := scanTaskFrom(rows)
		if err != nil {
			return nil, err
		}
		t.docket = d
		tasks = append(tasks, t)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	if shouldNotify {
		d.notifyTaskScheduledTx(tx)
	}
	return tasks, rows.Close()
}

func (d *docket) InsertTask(tx *sql.Tx, tc TaskCreator) (Task, error) {
	tasks, err := d.InsertTasks(tx, tc)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 1 {
		return tasks[0], nil
	}
	return nil, errors.New("shouldn't happen, got wrong number of tasks from InsertTasks, this is a bug")
}

func (d *docket) claimTasks(wantNum int) ([]task, error) {
	q := `
	WITH tasks_to_claim AS (
		SELECT task_id
		FROM pqdocket_task t1
		WHERE completed_at IS NULL
		  AND (scheduled_at < now())
		  AND (claimed_until IS NULL OR now() > claimed_until)
          AND claim_count < $1
		ORDER BY scheduled_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	)
	UPDATE pqdocket_task
	SET claimed_until = now() + make_interval(secs := claim_time_seconds),
		claim_count = claim_count + 1
	WHERE task_id IN (select * from tasks_to_claim)
	RETURNING *
	`
	q = strings.Replace(q, taskTableName, d.tableName(), 2)
	rows, err := d.db.Query(q, d.maxClaimCount, wantNum)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var claimedTasks []task
	for rows.Next() {
		t, err := scanTaskFrom(rows)
		if err != nil {
			return nil, err
		}
		t.docket = d
		t.claimedByThisProcess = true
		claimedTasks = append(claimedTasks, t)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return claimedTasks, rows.Close()
}

func (d *docket) saveTaskResult(l *slog.Logger, t task, taskErr error) {
	if taskErr == nil {
		q := `
		UPDATE pqdocket_task SET (claimed_until, completed_at) = (NULL, now())
		WHERE task_id = $1 AND claimed_until = $2
		`
		q = strings.Replace(q, taskTableName, d.tableName(), 1)
		_, err := d.db.Exec(q, t.taskId, t.getClaimedUntil())
		if err != nil {
			l.With("error", err, "task_status", "success").Error("failed to save task result")
		}
		return
	}

	q := `
		UPDATE pqdocket_task
		SET claimed_until = now() + make_interval(secs := least(greatest($1, pow(2, least(claim_count+1, 32))), $2)),
		    last_error = $3
		WHERE task_id = $4 AND claimed_until = $5
		`
	q = strings.Replace(q, taskTableName, d.tableName(), 1)
	_, err := d.db.Exec(q, d.minRetryBackoff, d.maxRetryBackoff, taskErr.Error(), t.taskId, t.getClaimedUntil())
	if err != nil {
		l.With("error", err, "task_status", "error").Error("failed to save task result")
	}
	l.With("error", taskErr).Warn("task failed")
}

func (t task) CompleteWithTransaction(tx *sql.Tx) error {
	l := t.logger()
	if !t.claimedByThisProcess {
		return ErrClaimedOrNotFound
	}
	q := `
		UPDATE pqdocket_task SET (claimed_until, completed_at) = (NULL, $1)
		WHERE task_id = $2 AND claimed_until = $3
	`
	q = strings.Replace(q, taskTableName, t.docket.tableName(), 1)
	result, err := tx.Exec(q, time.Now(), t.taskId, t.getClaimedUntil())
	if err != nil {
		l.With("error", err).Error("complete task with transaction: db err")
		return err
	}
	numRowsAffected, err := result.RowsAffected()
	if err != nil {
		l.With("error", err).Error("complete task with transaction: db err")
		return err
	}
	if numRowsAffected > 0 {
		return nil
	}
	err = ErrClaimedOrNotFound
	l.With("error", err).Warn("failed to complete task with transaction")
	return err
}

var ErrClaimedOrNotFound = errors.New("tried to modify task that is currently claimed by another worker or not found")

func (t task) Delete(tx *sql.Tx) error {
	l := t.logger()
	q := `
		DELETE FROM pqdocket_task
		WHERE task_id = $1 AND claimed_until IS NULL
	`
	q = strings.Replace(q, taskTableName, t.docket.tableName(), 1)

	var result sql.Result
	var err error
	if tx == nil {
		result, err = t.docket.db.Exec(q, t.taskId)
	} else {
		result, err = tx.Exec(q, t.taskId)
	}
	if err != nil {
		l.With("error", err).Error("delete task: db error")
		return fmt.Errorf("delete task: %w", err)
	}
	numRowsAffected, err := result.RowsAffected()
	if err != nil {
		l.With("error", err).Error("delete task: db error")
		return fmt.Errorf("delete task: %w", err)
	}
	if numRowsAffected > 0 {
		l.Info("task deleted")
		return nil
	}
	err = ErrClaimedOrNotFound
	l.With("error", err).Warn("delete task failed")
	return fmt.Errorf("delete task: %w", err)
}

func (t task) CancelAndDelete(tx *sql.Tx) error {
	l := t.logger()
	if !t.claimedByThisProcess {
		return ErrClaimedOrNotFound
	}
	q := `
		DELETE FROM pqdocket_task
		WHERE task_id = $1 AND claimed_until = $2
	`
	q = strings.Replace(q, taskTableName, t.docket.tableName(), 1)

	var result sql.Result
	var err error
	if tx == nil {
		result, err = t.docket.db.Exec(q, t.taskId, t.getClaimedUntil())
	} else {
		result, err = tx.Exec(q, t.taskId, t.getClaimedUntil())
	}
	if err != nil {
		l.With("error", err).Error("cancel task: db error")
		return err
	}
	numRowsAffected, err := result.RowsAffected()
	if err != nil {
		l.With("error", err).Error("cancel task: db error")
		return err
	}
	if numRowsAffected > 0 {
		l.Info("task cancelled")
		return nil
	}
	err = ErrClaimedOrNotFound
	l.With("error", err).Warn("cancel task failed")
	return fmt.Errorf("cancel task: %w", err)
}

func (t task) ExtendClaim(duration time.Duration) (*time.Time, error) {
	l := t.logger()

	if !t.StillClaimed() {
		l.Warn("extended claim: task is no longer claimed")
		return nil, ErrClaimedOrNotFound
	}

	qUpdateClaimedUntil := `
		UPDATE pqdocket_task SET claimed_until = claimed_until + ($3 * INTERVAL '1 second')
		WHERE task_id = $1 AND claimed_until = $2
		RETURNING claimed_until
	`
	qUpdateClaimedUntil = strings.Replace(qUpdateClaimedUntil, taskTableName, t.docket.tableName(), 1)

	var claimedUntil time.Time
	err := t.docket.db.QueryRow(qUpdateClaimedUntil, t.taskId, t.getClaimedUntil(), duration.Seconds()).Scan(&claimedUntil)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		l.Warn("extended claim: task not found or no longer claimed by this process")
		return nil, ErrClaimedOrNotFound
	}
	if err != nil {
		l.With("error", err).Error("extended claim: db err")
		return nil, fmt.Errorf("extended claim: db err: %w", err)
	}

	t.setClaimedUntil(null.TimeFrom(claimedUntil))
	l.With("extended_to", claimedUntil).Info("extended claim success")
	return &claimedUntil, nil
}

func (d *docket) timeToSleep() time.Duration {
	var seconds float64
	q := `
	SELECT EXTRACT(EPOCH FROM s.ready_at - now()) FROM (
	    SELECT greatest(scheduled_at, claimed_until) as ready_at FROM pqdocket_task t1
		WHERE completed_at IS NULL AND claim_count < $1
		ORDER BY ready_at
		LIMIT 1
	) s
	`
	q = strings.Replace(q, taskTableName, d.tableName(), 1)
	err := d.db.QueryRow(q, d.maxClaimCount).Scan(&seconds)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return time.Duration(math.MaxInt64)
	}
	if err != nil {
		d.logger.Load().With("error", err).Warn("error getting time to sleep")
		return time.Duration(math.MaxInt64)
	}
	timeout := time.Duration(seconds * float64(time.Second))
	if timeout < 0 {
		timeout = 0
	}
	return timeout
}

func (d *docket) getCleanupCandidates(limit int) ([]string, error) {
	if d.taskCleaner == nil {
		return nil, nil
	}
	q := `
		SELECT array_agg(task_id) FROM pqdocket_task
		WHERE created_at < (now() - ($1 * interval '1 second'))
		  AND completed_at IS NOT NULL LIMIT $2
	`
	q = strings.Replace(q, taskTableName, d.tableName(), 1)
	var tasksIds pq.StringArray
	err := d.db.QueryRow(q, d.taskCleaner.MaxAge.Seconds(), limit).Scan(&tasksIds)
	return tasksIds, err
}

func (d *docket) cleanupTask(tx *sql.Tx, taskId string) error {
	q := `
		DELETE FROM pqdocket_task
		WHERE task_id = $1
		  AND created_at < (now() - ($2 * interval '1 second'))
		  AND completed_at IS NOT NULL
	`
	q = strings.Replace(q, taskTableName, d.tableName(), 1)
	res, err := tx.Exec(q, taskId, d.taskCleaner.MaxAge.Seconds())
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	return sql.ErrNoRows
}
