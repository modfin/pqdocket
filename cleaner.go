package pqdocket

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

func (d *docket) startCleaner() {
	l := d.logger.Load()
	pollInterval := d.taskCleaner.PollInterval
	maxDrift := int64(pollInterval / time.Duration(10))              // 10% of pollInterval
	randomDrift := time.Duration(rand.Int63n(maxDrift) - maxDrift/2) // random somewhere between -5% and +5%
	pollInterval += randomDrift
	pollTicker := time.NewTicker(pollInterval)
	defer pollTicker.Stop()

	l.With("max_age", d.taskCleaner.MaxAge, "callback", d.taskCleaner.Callback != nil, "poll_interval", pollInterval).Info("cleaner: started")
	for {
		select {
		case <-d.close:
		case <-pollTicker.C:
		}
		d.mu.RLock()
		closed := d.closed
		d.mu.RUnlock()
		if closed {
			break
		}
		err := cleanupLoop(d)
		if err != nil {
			l.With("error", err).Error("cleaner: error in cleanup loop")
			time.Sleep(5 * time.Second)
		}
	}

	d.logger.Load().Info("close: cleaner terminated")
	close(d.cleanerCloseFinished)
}

func cleanupLoop(d *docket) (err error) {
	l := d.logger.Load()
	limit := 100
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("catched panic: %v", r)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var taskIds []string
	for {
		var err error
		taskIds, err = d.getCleanupCandidates(limit)
		if err != nil {
			return fmt.Errorf("error getting cleanup candidates: %w", err)
		}
		for _, taskId := range taskIds {
			select {
			case <-d.close:
				return nil
			default:
			}
			tx, err := d.db.BeginTx(ctx, nil) // tx rollbacks are handled by context cancel when if return an error
			if err != nil {
				return err
			}
			ts, err := d.FindTasks(tx, WithTaskId(taskId))
			if err != nil {
				return fmt.Errorf("error finding task: %w, taskId: %s", err, taskId)
			}
			if len(ts) != 1 {
				return fmt.Errorf("expected 1 task, got %d", len(ts))
			}
			t := ts[0]
			if err = d.cleanupTask(tx, taskId); err != nil {
				return fmt.Errorf("error cleaning task: %w, taskId: %s", err, taskId)
			}
			if d.taskCleaner.Callback != nil {
				if err := d.taskCleaner.Callback(tx, t); err != nil {
					return fmt.Errorf("error in task cleaner callback: %w, taskId: %s", err, taskId)
				}
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("error commiting task cleanup: %w, taskId: %s", err, taskId)
			}
			l := l.With("task_id", taskId)
			if t.RefId().Valid {
				l = l.With("ref_id", t.RefId().String)
			}
			l.Info("cleaner: deleted task")
		}
		if len(taskIds) < limit {
			return nil
		}
		time.Sleep(time.Second)
	}
}
