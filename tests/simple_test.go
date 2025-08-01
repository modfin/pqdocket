package tests

import (
	"context"
	"database/sql"
	"errors"
	"github.com/modfin/pqdocket"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func SimpleInit() {
	d.RegisterFunction(Task1)
	d.RegisterFunction(Task2)
	d.RegisterFunction(Task3)
	d.RegisterFunction(Task4)
	d2.RegisterFunction(Task1)
	d2.RegisterFunction(Task2)
	d2.RegisterFunction(Task3)
	d2.RegisterFunction(Task4)
}

var wg sync.WaitGroup

func Task1(task pqdocket.RunningTask) error {
	//if rand.Intn(100) > 50 {
	//	return errors.New("rand error")
	//}
	//if rand.Intn(100) > 50 {
	//	panic("rand panic")
	//}
	wg.Done()
	return nil
}

func TestTask1(t *testing.T) {
	for i := 0; i < 100; i++ {
		wg.Add(1)

		tc := d.CreateTask(Task1).
			ScheduleAt(time.Now())

		_, err := d.InsertTask(nil, tc)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
	wg.Wait()
}

var wg2 sync.WaitGroup

func Task2(task pqdocket.RunningTask) error {
	wg2.Done()
	return nil
}

func TestTask2(t *testing.T) {
	var tcs []pqdocket.TaskCreator
	for i := 0; i < 1000; i++ {
		wg2.Add(1)

		tc := d.CreateTask(Task2).
			ScheduleAt(time.Now())

		tcs = append(tcs, tc)
	}
	_, err := d.InsertTasks(nil, tcs...)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	wg2.Wait()
}

var wg3 sync.WaitGroup

func Task3(task pqdocket.RunningTask) error {
	stopAt := time.Now().Add(time.Duration(rand.Float64()*1000) * time.Millisecond)
	for i := 0; ; i++ {
		if i%10000 == 0 && time.Now().After(stopAt) {
			break
		}
	}
	wg3.Done()
	return nil
}

func TestTask3(t *testing.T) {
	var tcs []pqdocket.TaskCreator
	for i := 0; i < 200; i++ {
		wg3.Add(1)

		tc := d.CreateTask(Task3).
			ScheduleAt(time.Now().Add(time.Duration(rand.Float64()*10000) * time.Millisecond))

		tcs = append(tcs, tc)
	}
	_, err := d.InsertTasks(nil, tcs...)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	wg3.Wait()
}

var wg4 sync.WaitGroup

func Task4(task pqdocket.RunningTask) error {
	//if rand.Intn(100) > 50 {
	//	task.Delete(nil)
	//}
	_ = task.CancelAndDelete(nil)
	wg4.Done()
	return errors.New("fake")
}

func TestTask4(t *testing.T) {
	var tcs []pqdocket.TaskCreator
	for i := 0; i < 100; i++ {
		wg4.Add(1)

		tc := d.CreateTask(Task4).
			ScheduleAt(time.Now().Add(time.Duration(rand.Float64()*20) * time.Millisecond))

		tcs = append(tcs, tc)
	}
	_, err := d.InsertTasks(nil, tcs...)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	wg4.Wait()

	tc := d.CreateTask(Task4).ScheduleAt(time.Now().Add(time.Second))
	task, err := d.InsertTask(nil, tc)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	err = runInTransaction(context.Background(), db, func(tx *sql.Tx) error {
		tasks, err := d2.FindTasks(tx, pqdocket.WithTaskId(task.TaskId()))
		if err != nil {
			return err
		}
		time.Sleep(2 * time.Second)
		return tasks[0].Delete(tx)
	})
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	time.Sleep(1 * time.Second)
}

func TestOptionsErrors(t *testing.T) {
	t.Parallel()
	_, err := pqdocket.Init("",
		pqdocket.AfterTableInit(func(db *sql.DB, tableName string) error { return nil }),
		pqdocket.UseManuallyCreatedTable(""),
	)
	if err == nil {
		t.Error("expected error")
	}
	_, err = pqdocket.Init("",
		pqdocket.MinRetryBackoff(10),
		pqdocket.MaxRetryBackoff(5),
	)
	if err == nil {
		t.Error("expected error")
	}
}
