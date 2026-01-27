package pqdocket

import (
	"errors"
	"fmt"
)

func (d *docket) worker(workerId int, taskChan chan task, taskCompleted chan bool) {
	for t := range taskChan {
		l := t.logger()
		l.With("worker_id", workerId).Info("running task")
		err := d.workerBody(t)
		d.saveTaskResult(l, t, err)
		taskCompleted <- true
	}
	d.logger.Load().With("worker_id", workerId).Info("worker terminated")
	d.mu.Lock()
	d.runningWorkers--
	d.mu.Unlock()
}

func (d *docket) workerBody(t task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pqdocket: recover from panic: %+v", r)
		}
	}()
	d.mu.RLock()
	f, ok := d.functions[t.function]
	d.mu.RUnlock()
	if !ok {
		return errors.New("func: '" + t.function + "' not registered")
	}
	return f(t)
}
