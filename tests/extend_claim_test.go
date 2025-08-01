package tests

import (
	"fmt"
	"github.com/guregu/null/v6"
	"github.com/modfin/pqdocket"
	"sync"
	"testing"
	"time"
)

func ExtendedClaimInit() {
	d.RegisterFunction(TaskToExtend)
	d.RegisterFunction(TaskToExtendManyTimes)
	d2.RegisterFunction(TaskToExtend)
	d2.RegisterFunction(TaskToExtendManyTimes)
}

var wgExtended sync.WaitGroup
var wgTestComplete sync.WaitGroup

var wgExtendedMany sync.WaitGroup
var wgTestCompleteMany sync.WaitGroup

func TaskToExtend(task pqdocket.RunningTask) error {
	cu, err := task.ExtendClaim(5 * time.Minute)
	if err != nil {
		return err
	}
	if cu != nil {
		fmt.Println("extended claim time to", cu.Format(time.RFC3339))
	}
	fmt.Println("TaskToExtend awaiting tests...")
	wgExtended.Done()
	wgTestComplete.Wait()
	return nil
}

func TaskToExtendManyTimes(task pqdocket.RunningTask) error {
	for i := 0; i < 5; i++ {
		fmt.Printf("=============================== NEW ITERATION (%d) ===============================\n", i)
		cu, err := task.ExtendClaim(5 * time.Minute)
		if err != nil {
			return err
		}
		if cu != nil {
			fmt.Println("extended claim time to", cu.Format(time.RFC3339))
		}
		time.Sleep(5 * time.Second)
	}
	fmt.Println("TaskToExtendManyTimes awaiting tests...")
	wgExtendedMany.Done()
	wgTestCompleteMany.Wait()
	return nil
}

func TestTaskToExtend(t *testing.T) {

	wgExtended.Add(1)
	wgTestComplete.Add(1)

	scheduleTime := time.Now()

	tc := d.CreateTask(TaskToExtend).WithClaimTimeSeconds(20).ScheduleAt(scheduleTime)

	task, err := d.InsertTask(nil, tc)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	fmt.Println("waiting...")
	wgExtended.Wait()

	tasks, err := d.FindTasks(nil, pqdocket.WithTaskFunction(TaskToExtend), pqdocket.WithTaskId(task.TaskId()))
	if err != nil {
		t.Error(err)
	}

	if len(tasks) != 1 {
		t.Fatalf("expected to find one task")
	}

	var claimedUntil null.Time
	err = db.QueryRow(`SELECT claimed_until FROM pqdocket_task_test WHERE task_id = $1`, tasks[0].TaskId()).Scan(&claimedUntil)
	if err != nil {
		t.Fatalf("could not get task claimed until")
	}

	if !claimedUntil.Valid {
		t.Fatalf("claimed until should be valid")
	}

	// initial claim time addition
	scheduleAndClaimtime := scheduleTime.Add(time.Second * 20)

	initialToExtendedClaimtimeDelta := claimedUntil.Time.Sub(scheduleAndClaimtime).Seconds()

	if initialToExtendedClaimtimeDelta > (time.Minute * 4).Seconds() {
		fmt.Printf("delta from initial claim (%f) time larger than 4 minutes, extension completed!\n", initialToExtendedClaimtimeDelta)
	} else {
		t.Fatalf("claimed time delta was %f seconds, expected atleast 4 minutes", initialToExtendedClaimtimeDelta)
	}

	wgTestComplete.Done()
}

func TestTaskToExtendManyTimes(t *testing.T) {

	wgExtendedMany.Add(1)
	wgTestCompleteMany.Add(1)

	scheduleTime := time.Now()

	tc := d.CreateTask(TaskToExtendManyTimes).WithClaimTimeSeconds(20).ScheduleAt(scheduleTime)

	task, err := d.InsertTask(nil, tc)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	fmt.Println("waiting...")
	wgExtendedMany.Wait()

	tasks, err := d.FindTasks(nil, pqdocket.WithTaskFunction(TaskToExtendManyTimes), pqdocket.WithTaskId(task.TaskId()))
	if err != nil {
		return
	}

	if len(tasks) != 1 {
		t.Fatalf("expected to find one task")
	}

	var claimedUntil null.Time

	err = db.QueryRow(`SELECT claimed_until FROM pqdocket_task_test WHERE task_id = $1`, tasks[0].TaskId()).Scan(&claimedUntil)
	if err != nil {
		t.Fatalf("could not get task claimed until")
	}

	if !claimedUntil.Valid {
		t.Fatalf("claimed until should be valid")
	}

	// initial claim time addition
	scheduleAndClaimtime := scheduleTime.Add(time.Second * 20)

	initialToExtendedClaimtimeDelta := claimedUntil.Time.Sub(scheduleAndClaimtime).Seconds()

	if initialToExtendedClaimtimeDelta > (time.Minute * 8).Seconds() {
		fmt.Printf("delta from initial claim (%f) time larger than 8 minutes, extension completed!\n", initialToExtendedClaimtimeDelta)
	} else {
		t.Fatalf("claimed time delta was %f seconds, expected atleast 8 minutes", initialToExtendedClaimtimeDelta)
	}

	wgTestCompleteMany.Done()
}
