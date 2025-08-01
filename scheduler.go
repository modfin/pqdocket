package pqdocket

import (
	"errors"
	"github.com/lib/pq"
	"log/slog"
	"math"
	"math/rand"
	"time"
)

func (d *docket) reinitTablesIfError(l *slog.Logger, err error) {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code.Name() == "undefined_table" {
		l.Info("creating missing tables...")
		err := d.initTables()
		if err != nil {
			l.With("error", err).Error("failed")
			return
		}
		l.Info("success")
	}
}

func (d *docket) startScheduler() {
	taskScheduled := d.listener.NotificationChannel()

	maxDrift := int64(d.pollInterval / time.Duration(10))            // 10% of pollInterval
	randomDrift := time.Duration(rand.Int63n(maxDrift) - maxDrift/2) // random somewhere between -5% and +5%
	pollInterval := d.pollInterval + randomDrift
	pollTicker := time.NewTicker(pollInterval)
	defer pollTicker.Stop()
	d.logger.Load().With("poll_interval", pollInterval, "parallelism", d.parallelism).Info("scheduler started")

	wantNum := d.parallelism
	for {
		l := d.logger.Load()
		if wantNum > 0 {
			tasks, err := d.claimTasks(wantNum)
			if err != nil {
				l.With("error", err).Error("error in claimTasks")
				d.reinitTablesIfError(l, err)
				time.Sleep(20 * time.Second)
				continue
			}
			l.With("want", wantNum, "got", len(tasks)).Info("claimTasks")
			for _, t := range tasks {
				d.claimedTasks <- t
				wantNum--
			}
		}

		timeout := d.timeToSleep()

		// protect against polling if our workers are full
		if wantNum == 0 && timeout < 2*time.Second {
			timeout = 2 * time.Second
		}
		if timeout < time.Duration(math.MaxInt64) {
			l.With("timeout", timeout.Round(time.Millisecond).String()).Info("setting timeout")
		}

		timer := time.NewTimer(timeout)
		select {
		case <-taskScheduled:
			l = l.With("reason", "task_scheduled")
		case <-d.taskCompleted:
			wantNum++
			l = l.With("reason", "task_completed")
		case <-d.close:
			l = l.With("reason", "closed")
		case <-timer.C:
			l = l.With("reason", "timeout")
		case <-pollTicker.C:
			l = l.With("reason", "poll")
		}
		l.Info("wakeup")
		timer.Stop()

		d.mu.RLock()
		closed := d.closed
		d.mu.RUnlock()
		if closed {
			break
		}

		// consume extra buffered taskScheduled/taskCompleted messages
		for {
			select {
			case <-d.taskCompleted:
				wantNum++
				continue
			case <-taskScheduled:
				continue
			default:
			}
			break
		}
	}

	d.logger.Load().Info("close: scheduler terminated")
	d.logger.Load().Info("close: waiting for outstanding tasks...")
	close(d.claimedTasks)
	for {
		d.mu.RLock()
		runningWorkers := d.runningWorkers
		d.mu.RUnlock()
		if runningWorkers == 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	close(d.closeFinished)
}
