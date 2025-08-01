package tests

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/modfin/pqdocket"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var factor = 40

type TaskStatus struct {
	RefIf       string
	TaskType    string
	AddedAt     time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	FinishCount int
}

type TaskLogger struct {
	mu              *sync.Mutex
	tasksByRefId    map[string]*TaskStatus
	tasksByTaskType map[string][]*TaskStatus
	runningTasks    int
}

func (tl *TaskLogger) reportAdded(taskType, refId string) {
	ts := &TaskStatus{
		RefIf:    refId,
		TaskType: taskType,
		AddedAt:  time.Now(),
	}
	tl.mu.Lock()
	tl.tasksByRefId[refId] = ts
	tl.tasksByTaskType[taskType] = append(tl.tasksByTaskType[taskType], ts)
	tl.runningTasks++
	tl.mu.Unlock()
}
func (tl *TaskLogger) reportStart(refId string) {
	now := time.Now()
	tl.mu.Lock()
	ts := tl.tasksByRefId[refId]
	if !ts.StartedAt.After(ts.AddedAt) {
		ts.StartedAt = now
	}
	tl.mu.Unlock()
}
func (tl *TaskLogger) reportFinish(refId string) {
	now := time.Now()
	tl.mu.Lock()
	tl.tasksByRefId[refId].FinishCount++
	tl.tasksByRefId[refId].FinishedAt = now
	tl.runningTasks--
	tl.mu.Unlock()
}

func (tl *TaskLogger) waitForAndPrintResult() error {
	for {
		tl.mu.Lock()
		r := tl.runningTasks
		tl.mu.Unlock()
		fmt.Println(">>>", r)
		if r < 1 {
			break
		}
		time.Sleep(time.Second * 1)
	}
	tl.mu.Lock()
	defer tl.mu.Unlock()
	for tt, tasks := range tl.tasksByTaskType {
		var minRunTime = time.Hour * 10000
		var maxRunTime = time.Second * 0
		var avgRunTime = time.Second * 0

		var numFinishedNormally = 0
		var numFinishedMoreThanOnce = 0

		for _, t := range tasks {
			if t.FinishCount == 1 {
				runTime := t.FinishedAt.Sub(t.StartedAt)
				if runTime > maxRunTime {
					maxRunTime = runTime
				}
				if runTime < minRunTime {
					minRunTime = runTime
				}
				avgRunTime += runTime
				numFinishedNormally++
			}
			if t.FinishCount > 1 {
				numFinishedMoreThanOnce++
			}
		}
		avgRunTime /= time.Duration(numFinishedNormally)

		fmt.Println("TYPE: ", tt)
		fmt.Println("NUM:", len(tasks), "FINISHED:", numFinishedNormally)
		fmt.Println("RUNTIME:", minRunTime.Round(time.Millisecond), avgRunTime.Round(time.Millisecond), maxRunTime.Round(time.Millisecond))

		if numFinishedMoreThanOnce > 0 {
			fmt.Println("FINISHED MORE THAN ONCE: ", numFinishedMoreThanOnce)
			return errors.New("some task finished more than once")
		}
		fmt.Println()

	}

	return nil
}

func MakeLogger() *TaskLogger {
	l := &TaskLogger{}
	l.mu = &sync.Mutex{}
	l.mu.Lock()
	l.tasksByRefId = make(map[string]*TaskStatus)
	l.tasksByTaskType = make(map[string][]*TaskStatus)
	l.mu.Unlock()
	return l
}

var stressLogger *TaskLogger = MakeLogger()

func StressTask(task pqdocket.RunningTask) error {
	var taskType string
	err := task.BindMetadata(&taskType)
	if err != nil {
		return err
	}
	refId := task.RefId()
	if !refId.Valid {
		return errors.New("no refId")
	}
	stressLogger.reportStart(refId.String)
	err = runTask(taskType)
	if err != nil {
		return err
	}
	if !task.StillClaimed() {
		fmt.Println("not claimed anymore, aborting")
		return errors.New("timeout")
	}
	stressLogger.reportFinish(refId.String)
	return nil
}

func runTask(taskType string) error {
	switch taskType {
	case "sleeping":
		time.Sleep(time.Duration(rand.Float64()*3000) * time.Millisecond)
		return nil
	case "failing":
		if rand.Intn(100) > 97 {
			return errors.New("rand error")
		}
		if rand.Intn(100) > 97 {
			panic("rand panic")
		}
	case "too-long-running":
		if rand.Intn(100) > 50 {
			time.Sleep(time.Second * 30)
		}
		return nil
	case "cpu":
		stopAt := time.Now().Add(time.Duration(rand.Float64()*300) * time.Millisecond)
		for i := 0; ; i++ {
			if i%10000 == 0 && time.Now().After(stopAt) {
				break
			}
		}
		return nil
	}

	return nil
}

func TestStress(t *testing.T) {

	d.RegisterFunction(StressTask)
	d2.RegisterFunction(StressTask)

	//go func() {
	//	for {
	//		time.Sleep(time.Second * 30)
	//		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	//	}
	//}()

	t.Run("too-long-running", func(t *testing.T) {

		for i := 0; i < 100; i++ {
			md, _ := pqdocket.CreateMetadata("too-long-running")
			refId := uuid.New().String()
			stressLogger.reportAdded("too-long-running", refId)
			tc := d.CreateTask(StressTask).
				WithRefId(refId).
				WithMetadata(md).
				ScheduleAt(time.Now())

			_, err := d.InsertTask(nil, tc)
			if err != nil {
				t.Error(err)
				t.Fail()
			}
		}
	})

	t.Run("parallel-group", func(t *testing.T) {
		t.Run("sleeping", func(t *testing.T) {
			t.Parallel()

			var tcs []pqdocket.TaskCreator
			for i := 0; i < 100*factor; i++ {
				md, _ := pqdocket.CreateMetadata("sleeping")
				refId := uuid.New().String()
				stressLogger.reportAdded("sleeping", refId)
				tc := d.CreateTask(StressTask).
					WithRefId(refId).
					WithMetadata(md).
					ScheduleAt(time.Now())

				tcs = append(tcs, tc)
			}
			_, err := d.InsertTasks(nil, tcs...)
			if err != nil {
				t.Error(err)
				t.Fail()
			}
		})
		t.Run("failing", func(t *testing.T) {
			t.Parallel()

			var tcs []pqdocket.TaskCreator
			for i := 0; i < 100*factor; i++ {
				md, _ := pqdocket.CreateMetadata("failing")
				refId := uuid.New().String()
				stressLogger.reportAdded("failing", refId)
				tc := d2.CreateTask(StressTask).
					WithRefId(refId).
					WithMetadata(md).
					ScheduleAt(time.Now())

				tcs = append(tcs, tc)
			}
			_, err := d2.InsertTasks(nil, tcs...)
			if err != nil {
				t.Error(err)
				t.Fail()
			}
		})
		t.Run("cpu", func(t *testing.T) {
			t.Parallel()

			var tcs []pqdocket.TaskCreator
			for i := 0; i < 100*factor; i++ {
				md, _ := pqdocket.CreateMetadata("cpu")
				refId := uuid.New().String()
				stressLogger.reportAdded("cpu", refId)
				tc := d.CreateTask(StressTask).
					WithRefId(refId).
					WithMetadata(md).
					ScheduleAt(time.Now())

				tcs = append(tcs, tc)
			}
			_, err := d.InsertTasks(nil, tcs...)
			if err != nil {
				t.Error(err)
				t.Fail()
			}
		})
		t.Run("fast-batch", func(t *testing.T) {
			t.Parallel()
			for b := 0; b < 10; b++ {
				var tcs []pqdocket.TaskCreator
				for i := 0; i < 10*factor; i++ {
					md, _ := pqdocket.CreateMetadata("fast-batch")
					refId := uuid.New().String()
					stressLogger.reportAdded("fast-batch", refId)
					tc := d.CreateTask(StressTask).
						WithRefId(refId).
						WithMetadata(md).
						ScheduleAt(time.Now())

					tcs = append(tcs, tc)
				}
				_, err := d.InsertTasks(nil, tcs...)
				if err != nil {
					t.Error(err)
					t.Fail()
				}
			}
		})
		t.Run("fast-nonbatch", func(t *testing.T) {
			t.Parallel()

			for i := 0; i < 100*factor; i++ {
				md, _ := pqdocket.CreateMetadata("fast-nonbatch")
				refId := uuid.New().String()
				stressLogger.reportAdded("fast-nonbatch", refId)
				tc := d2.CreateTask(StressTask).
					WithRefId(refId).
					WithMetadata(md).
					ScheduleAt(time.Now())

				_, err := d2.InsertTask(nil, tc)
				if err != nil {
					t.Error(err)
					t.Fail()
				}
			}
		})
		t.Run("fast-batch-future-start", func(t *testing.T) {
			t.Parallel()

			for b := 0; b < 10; b++ {
				for i := 0; i < 10*factor; i++ {
					md, _ := pqdocket.CreateMetadata("fast-batch-future-start")
					refId := uuid.New().String()
					stressLogger.reportAdded("fast-batch-future-start", refId)
					tc := d.CreateTask(StressTask).
						WithRefId(refId).
						WithMetadata(md).
						ScheduleAt(time.Now().Add(time.Duration(rand.Float64()*5000) * time.Millisecond))

					_, err := d.InsertTask(nil, tc)
					if err != nil {
						t.Error(err)
						t.Fail()
					}
				}
			}
		})
	})

	err := stressLogger.waitForAndPrintResult()
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}
