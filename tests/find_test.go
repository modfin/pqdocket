package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/modfin/pqdocket"
	"sync"
	"testing"
	"time"
)

func FindInit() {
	d.RegisterFunction(FindTest)
	d2.RegisterFunction(FindTest)
}

var wgF sync.WaitGroup

func FindTest(task pqdocket.RunningTask) error {
	fmt.Println("FindTest")
	wgF.Done()
	return nil
}

func TestFind(t *testing.T) {

	wgF.Add(3)

	for i := 0; i < 4; i++ {

		tc := d.CreateTask(FindTest).
			ScheduleAt(time.Now().Add(time.Second * 10))

		if i == 3 {
			tc = tc.WithRefId("00000001-cafe-babe-0000-000000000000")
		}

		_, err := d.InsertTask(nil, tc)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}

	tasks, err := d2.FindTasks(nil, pqdocket.WithTaskFunction(FindTest))
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	for _, t := range tasks {
		fmt.Printf("%+v\n", t)
	}
	if len(tasks) != 4 {
		t.Fail()
	}

	tasks, err = d.FindTasks(nil, pqdocket.WithTaskFunction(FindTest), pqdocket.WithRefId("00000001-cafe-babe-0000-000000000000"))
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if len(tasks) != 1 {
		t.Fail()
	}
	err = tasks[0].Delete(nil)
	if err != nil {
		t.Error(err)
	}
	wgF.Wait()
}

func TestFindTx(t *testing.T) {
	err := runInTransaction(context.Background(), db, func(tx *sql.Tx) error {
		tc := d.CreateTask(FindTest).
			ScheduleAt(time.Now())

		t, err := d.InsertTask(tx, tc)
		if err != nil {
			return err
		}
		tasks, err := d.FindTasks(tx, pqdocket.WithTaskId(t.TaskId()))
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			return errors.New("didn't get one task")
		}
		return tasks[0].Delete(tx)
	})
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}

// tests that FindTasks in a transaction locks rows (FOR UPDATE)
func TestFindTx2(t *testing.T) {
	tc := d.CreateTask(FindTest).
		ScheduleAt(time.Now().Add(time.Second * 2))

	t1, err := d.InsertTask(nil, tc)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	err = runInTransaction(context.Background(), db, func(tx *sql.Tx) error {
		tasks, err := d.FindTasks(tx, pqdocket.WithTaskId(t1.TaskId()))
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			return errors.New("didn't get one task")
		}
		time.Sleep(time.Second * 4)
		return tasks[0].Delete(tx)
	})
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}
