package pqdocket

import (
	"errors"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/lib/pq"
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

	// Build exclude list from function parallelism config
	d.mu.RLock()
	var excludeFunctions []string
	for funcName := range d.functionParallelism {
		excludeFunctions = append(excludeFunctions, funcName)
	}
	d.mu.RUnlock()
	cf := claimFilter{
		excludeFunctions: excludeFunctions,
	}

	wantNum := d.parallelism
	for {
		l := d.logger.Load()
		if wantNum > 0 {
			// Use filtered claiming to exclude functions with specific parallelism
			tasks, err := d.claimTasksWithFilter(wantNum, cf)
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

		timeout := d.timeToSleep(cf)

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

func (d *docket) startFunctionScheduler(fs *functionScheduler) {
	taskScheduled := d.listener.NotificationChannel()

	maxDrift := int64(d.pollInterval / time.Duration(10))
	randomDrift := time.Duration(rand.Int63n(maxDrift) - maxDrift/2)
	pollInterval := d.pollInterval + randomDrift
	pollTicker := time.NewTicker(pollInterval)
	defer pollTicker.Stop()
	d.logger.Load().With(
		"poll_interval", pollInterval,
		"parallelism", fs.parallelism,
		"function", fs.funcName,
	).Info("function scheduler started")

	cf := claimFilter{
		onlyFunctions: []string{fs.funcName},
	}

	fs.wantNum = fs.parallelism
	for {
		l := d.logger.Load()
		if fs.wantNum > 0 {
			tasks, err := d.claimTasksWithFilter(fs.wantNum, cf)
			if err != nil {
				l.With("error", err, "function", fs.funcName).Error("error in claimTasksWithFilter")
				d.reinitTablesIfError(l, err)
				time.Sleep(20 * time.Second)
				continue
			}
			l.With("want", fs.wantNum, "got", len(tasks), "function", fs.funcName).Info("claimTasks")
			for _, t := range tasks {
				fs.claimedTasks <- t
				fs.wantNum--
			}
		}

		timeout := d.timeToSleep(cf)

		if fs.wantNum == 0 && timeout < 2*time.Second {
			timeout = 2 * time.Second
		}
		if timeout < time.Duration(math.MaxInt64) {
			l.With("timeout", timeout.Round(time.Millisecond).String(), "function", fs.funcName).Info("setting timeout")
		}

		timer := time.NewTimer(timeout)
		select {
		case <-taskScheduled:
			l = l.With("reason", "task_scheduled", "function", fs.funcName)
		case <-fs.taskCompleted:
			fs.wantNum++
			l = l.With("reason", "task_completed", "function", fs.funcName)
		case <-fs.close:
			l = l.With("reason", "closed", "function", fs.funcName)
		case <-timer.C:
			l = l.With("reason", "timeout", "function", fs.funcName)
		case <-pollTicker.C:
			l = l.With("reason", "poll", "function", fs.funcName)
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
			case <-fs.taskCompleted:
				fs.wantNum++
				continue
			case <-taskScheduled:
				continue
			default:
			}
			break
		}
	}

	close(fs.claimedTasks)
	d.logger.Load().With("function", fs.funcName).Info("close: function scheduler terminated")
	close(fs.closeFinished)
}
