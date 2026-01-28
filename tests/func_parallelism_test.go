package tests

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modfin/pqdocket"
)

var (
	pfpD             pqdocket.Docket
	pfpMaxSlowCon    atomic.Int32
	pfpMaxMediumCon  atomic.Int32
	pfpMaxFastCon    atomic.Int32
	pfpMaxGeneralCon atomic.Int32
	pfpWg            sync.WaitGroup
)

const (
	slowParallelism    = 3
	mediumParallelism  = 5
	fastParallelism    = 2
	generalParallelism = 50
)

func ParallelismGroupsInit() {
	var err error
	pfpD, err = pqdocket.Init("postgres://postgres:qwerty@localhost:9300/postgres?sslmode=disable",
		pqdocket.Namespace("pfp_test"),
		pqdocket.Parallelism(generalParallelism),
		pqdocket.WithDedicatedParallelismGroup(slowParallelism, "slowFunc"),
		pqdocket.WithDedicatedParallelismGroup(mediumParallelism, "mediumFunc"),
		pqdocket.WithDedicatedParallelismGroup(fastParallelism, "fastFunc"),
	)
	if err != nil {
		panic(err)
	}

	pfpMaxSlowCon.Store(0)
	pfpMaxMediumCon.Store(0)
	pfpMaxFastCon.Store(0)
	pfpMaxGeneralCon.Store(0)

}

func slowFunc(t *testing.T) func(task pqdocket.RunningTask) error {
	return func(task pqdocket.RunningTask) error {
		current := pfpMaxSlowCon.Add(1)
		defer pfpMaxSlowCon.Add(-1)
		defer pfpWg.Done()

		if current > slowParallelism {
			t.Error("slowFunc exceeded its parallelism limit of 3")
			return fmt.Errorf("slowFunc exceeded its parallelism limit of 3")
		}

		time.Sleep(250 * time.Millisecond)
		return nil
	}
}
func mediumFunc(t *testing.T) func(task pqdocket.RunningTask) error {
	return func(task pqdocket.RunningTask) error {
		current := pfpMaxMediumCon.Add(1)
		defer pfpMaxMediumCon.Add(-1)
		defer pfpWg.Done()

		if current > mediumParallelism {
			t.Error("mediumFunc exceeded parallelism limit of 5")
			return fmt.Errorf("mediumFunc exceeded parallelism limit of 5")
		}

		time.Sleep(50 * time.Millisecond)
		return nil
	}
}
func fastFunc(t *testing.T) func(task pqdocket.RunningTask) error {
	return func(task pqdocket.RunningTask) error {
		current := pfpMaxFastCon.Add(1)
		defer pfpMaxFastCon.Add(-1)
		defer pfpWg.Done()

		if current > fastParallelism {
			t.Error("fastFunc exceeded parallelism limit of 2")
			return fmt.Errorf("fastFunc exceeded parallelism limit of 2")
		}
		time.Sleep(10 * time.Millisecond)
		return nil
	}
}
func generalFunc(t *testing.T) func(task pqdocket.RunningTask) error {
	return func(task pqdocket.RunningTask) error {
		current := pfpMaxGeneralCon.Add(1)
		defer pfpMaxGeneralCon.Add(-1)
		defer pfpWg.Done()

		if current > generalParallelism {
			t.Error("generalFunc exceeded default parallelism limit of 10")
			return fmt.Errorf("generalFunc exceeded default parallelism limit of 50")
		}
		time.Sleep(20 * time.Millisecond)
		return nil
	}
}

// TestParallelismGroups verifies each function respects its individual limit
func TestParallelismGroups(t *testing.T) {
	var tcs []pqdocket.TaskCreator

	pfpD.RegisterFunctionWithFuncName("slowFunc", slowFunc(t))
	pfpD.RegisterFunctionWithFuncName("mediumFunc", mediumFunc(t))
	pfpD.RegisterFunctionWithFuncName("fastFunc", fastFunc(t))
	pfpD.RegisterFunctionWithFuncName("generalFunc", generalFunc(t))

	for i := 0; i < slowParallelism*10; i++ {
		pfpWg.Add(1)
		tc := pfpD.CreateTaskWithFuncName("slowFunc")
		tcs = append(tcs, tc)
	}

	for i := 0; i < generalParallelism*10; i++ {
		pfpWg.Add(1)
		tc := pfpD.CreateTaskWithFuncName("generalFunc")
		tcs = append(tcs, tc)
	}

	for i := 0; i < mediumParallelism*10; i++ {
		pfpWg.Add(1)
		tc := pfpD.CreateTaskWithFuncName("mediumFunc")
		tcs = append(tcs, tc)
	}

	for i := 0; i < fastParallelism*10; i++ {
		pfpWg.Add(1)
		tc := pfpD.CreateTaskWithFuncName("fastFunc")
		tcs = append(tcs, tc)
	}

	_, err := pfpD.InsertTasks(nil, tcs...)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	pfpWg.Wait()
}
