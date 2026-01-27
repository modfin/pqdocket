package tests

import (
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

func PerFunctionParallelismInit() {
	// Initialize a docket with per-function parallelism
	var err error
	pfpD, err = pqdocket.Init("postgres://postgres:qwerty@localhost:9300/postgres?sslmode=disable",
		pqdocket.Namespace("pfp_test"),
		pqdocket.Parallelism(10000),                   // General pool: 10
		pqdocket.FunctionParallelism("SlowFunc", 3),   // Dedicated: 3
		pqdocket.FunctionParallelism("MediumFunc", 5), // Dedicated: 5
		pqdocket.FunctionParallelism("FastFunc", 2),   // Dedicated: 2
	)
	if err != nil {
		panic(err)
	}

	pfpMaxSlowCon.Store(0)
	pfpMaxMediumCon.Store(0)
	pfpMaxFastCon.Store(0)
	pfpMaxGeneralCon.Store(0)

	pfpD.RegisterFunctionWithFuncName("SlowFunc", SlowFunc)
	pfpD.RegisterFunctionWithFuncName("MediumFunc", MediumFunc)
	pfpD.RegisterFunctionWithFuncName("FastFunc", FastFunc)
	pfpD.RegisterFunctionWithFuncName("GeneralFunc", GeneralFunc)
}

func SlowFunc(task pqdocket.RunningTask) error {
	current := pfpMaxSlowCon.Add(1)
	defer pfpMaxSlowCon.Add(-1)
	defer pfpWg.Done()

	if current > 3 {
		panic("SlowFunc exceeded parallelism limit of 3")
	}

	time.Sleep(100 * time.Millisecond)
	return nil
}
func MediumFunc(task pqdocket.RunningTask) error {
	current := pfpMaxMediumCon.Add(1)
	defer pfpMaxMediumCon.Add(-1)
	defer pfpWg.Done()

	if current > 5 {
		panic("MediumFunc exceeded parallelism limit of 5")
	}

	time.Sleep(50 * time.Millisecond)
	return nil
}

func FastFunc(task pqdocket.RunningTask) error {
	current := pfpMaxFastCon.Add(1)
	defer pfpMaxFastCon.Add(-1)
	defer pfpWg.Done()
	if current > 2 {
		panic("FastFunc exceeded parallelism limit of 2")
	}
	time.Sleep(10 * time.Millisecond)
	return nil
}

func GeneralFunc(task pqdocket.RunningTask) error {
	current := pfpMaxGeneralCon.Add(1)
	defer pfpMaxGeneralCon.Add(-1)
	defer pfpWg.Done()
	if current > 10 {
		panic("GeneralFunc exceeded general parallelism limit of 10")
	}
	time.Sleep(20 * time.Millisecond)
	return nil
}

// TestMultipleFunctionSchedulers verifies each function respects its individual limit
func TestMultipleFunctionSchedulers(t *testing.T) {
	var tcs []pqdocket.TaskCreator

	// Insert tasks for SlowFunc (parallelism: 3)
	for i := 0; i < 150; i++ {
		pfpWg.Add(1)
		tc := pfpD.CreateTaskWithFuncName("SlowFunc").
			ScheduleAt(time.Now())
		tcs = append(tcs, tc)
	}

	// Insert tasks for MediumFunc (parallelism: 5)
	for i := 0; i < 250; i++ {
		pfpWg.Add(1)
		tc := pfpD.CreateTaskWithFuncName("MediumFunc").
			ScheduleAt(time.Now())
		tcs = append(tcs, tc)
	}

	// Insert tasks for FastFunc (parallelism: 2)
	for i := 0; i < 100; i++ {
		pfpWg.Add(1)
		tc := pfpD.CreateTaskWithFuncName("FastFunc").
			ScheduleAt(time.Now())
		tcs = append(tcs, tc)
	}

	// Insert tasks for General
	for i := 0; i < 100; i++ {
		pfpWg.Add(1)
		tc := pfpD.CreateTaskWithFuncName("GeneralFunc").
			ScheduleAt(time.Now())
		tcs = append(tcs, tc)
	}

	_, err := pfpD.InsertTasks(nil, tcs...)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	pfpWg.Wait()

	t.Log("All functions completed with their respective parallelism limits")
}
