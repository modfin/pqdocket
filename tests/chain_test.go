package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/modfin/pqdocket"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func ChainInit() {
	d.RegisterFunction(Chain)
	d2.RegisterFunction(Chain)
}

var wgC sync.WaitGroup
var errCounter = 0
var panicCounter = 0

func Chain(task pqdocket.RunningTask) error {

	err := runInTransaction(context.Background(), db, func(tx *sql.Tx) error {

		var count int
		err := task.BindMetadata(&count)
		if err != nil {
			return err
		}
		fmt.Println("trying: ", count)
		count--

		if count < 1 {
			return nil
		}

		md, err := pqdocket.CreateMetadata(count)
		if err != nil {
			return err
		}
		tc := d.CreateTask(Chain).
			WithMetadata(md).
			ScheduleAt(time.Now())

		_, err = d.InsertTask(tx, tc)
		if err != nil {
			return err
		}
		err = task.CompleteWithTransaction(tx)
		if err != nil {
			return err
		}

		if rand.Intn(100) > 90 && errCounter < 1 {
			errCounter++
			return errors.New("rand error")
		}
		if rand.Intn(100) > 90 && panicCounter < 1 {
			panicCounter++
			panic("rand panic")
		}

		return err
	})

	if err == nil {
		wgC.Done()
	}

	return err
}

func TestChain(t *testing.T) {
	count := 100
	wgC.Add(count)

	md, err := pqdocket.CreateMetadata(count)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	tc := d.CreateTask(Chain).
		WithMetadata(md).
		ScheduleAt(time.Now())

	_, err = d.InsertTask(nil, tc)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	wgC.Wait()
}
