package pqdocket

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"io"
	"log/slog"
	"math"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Docket interface {
	// RegisterFunction registers a TaskFunc / callback / function handler.
	// FuncName will be derived from the name of the TaskFunction via reflection (Not recommended, better to use RegisterFunctionWithFuncName).
	RegisterFunction(f TaskFunction)

	// RegisterFunctionWithFuncName registers a TaskFunc / callback / function handler.
	RegisterFunctionWithFuncName(funcName string, f TaskFunction)

	// CreateTask - initialize creation of a task.
	// FuncName will be derived from the name of the TaskFunction via reflection (Not recommended, better to use CreateTaskWithFuncName).
	// Functions need to be registered before being used to create tasks.
	CreateTask(f TaskFunction) TaskCreator

	// CreateTaskWithFuncName - initialize creation of a task that will run the registered function.
	// Functions need to be registered before being used to create tasks.
	CreateTaskWithFuncName(funcName string) TaskCreator

	// InsertTask - insert a task. Tasks are immediately available to run after being inserted.
	InsertTask(tx *sql.Tx, tc TaskCreator) (Task, error)

	// InsertTasks - batch insert tasks. Tasks are immediately available to run after being inserted.
	//
	// Supports max 65535/5 = 13,107 tasks in one batch, since postgres supports max 65535 arguments per statement.
	// If you need more, execute multiple InsertTasks in the same transaction.
	InsertTasks(tx *sql.Tx, tc ...TaskCreator) ([]Task, error)

	// FindTasks is used to query tasks.
	// If executed within a transaction (tx != nil), it will lock the returned rows for the duration of the transaction.
	FindTasks(tx *sql.Tx, params ...FindParam) ([]Task, error)

	// Close closes the Docket instance,
	// which stops processing new requests,
	// and waits until all outstanding tasks complete. Close is safe to call multiple times.
	Close()
}

type docket struct {
	mu sync.RWMutex

	listener *pq.Listener
	db       *sql.DB
	logger   atomic.Pointer[slog.Logger]

	//options
	namespace               string
	defaultClaimTime        int
	pollInterval            time.Duration
	parallelism             int
	maxRetryBackoff         int
	minRetryBackoff         int
	maxClaimCount           int
	taskCleaner             *TaskCleanerSettings
	afterTableInitCallback  func(db *sql.DB, tableName string) error
	useManuallyCreatedTable string

	claimedTasks  chan task
	taskCompleted chan bool
	functions     map[string]TaskFunction

	closed               bool
	close                chan bool
	closeFinished        chan bool
	cleanerCloseFinished chan bool
	runningWorkers       int
}

type TaskFunction func(task RunningTask) error

type TaskCleanerSettings struct {
	// MaxAge defines the age (since created_at), where completed tasks are ready for cleanup.
	MaxAge time.Duration

	// PollInterval sets how often to poll for completed tasks ready for cleanup.
	// Note that ±5% random drift is added to his duration.
	// If unset, defaults to 10 minutes.
	PollInterval time.Duration

	// Optional, called for each cleaned-up task within a transaction
	// to allow deletion of your own data as the task is deleted.
	Callback func(tx *sql.Tx, task Task) error
}

func Init(dbUrl string, options ...Option) (Docket, error) {
	d := &docket{}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.setLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	d.parallelism = 1
	d.namespace = ""
	d.defaultClaimTime = 10
	d.pollInterval = time.Minute
	d.maxRetryBackoff = 128
	d.minRetryBackoff = 4
	d.maxClaimCount = math.MaxInt32
	for _, opt := range options {
		opt(d)
	}
	if d.minRetryBackoff > d.maxRetryBackoff {
		return nil, errors.New("minRetryBackoff cannot be greater than maxRetryBackoff")
	}
	if d.useManuallyCreatedTable != "" && d.afterTableInitCallback != nil {
		return nil, errors.New("AfterTableInit will never be called when UseManuallyCreatedTable is enabled")
	}

	d.close = make(chan bool)
	d.closeFinished = make(chan bool)

	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(d.parallelism + 1)
	db.SetConnMaxLifetime(d.pollInterval * 2)
	d.db = db

	d.claimedTasks = make(chan task, d.parallelism)
	d.taskCompleted = make(chan bool, d.parallelism*2)
	d.functions = make(map[string]TaskFunction)
	if err = d.initTables(); err != nil {
		return nil, err
	}
	d.listener = pq.NewListener(dbUrl, 10*time.Second, 60*time.Second, d.handleListenerEvent)
	if err = d.listener.Listen(d.channelName()); err != nil {
		return nil, err
	}
	go d.startScheduler()
	for i := 0; i < d.parallelism; i++ {
		go d.worker(i)
		d.runningWorkers++
	}
	if d.taskCleaner != nil {
		if d.taskCleaner.PollInterval == 0 {
			d.taskCleaner.PollInterval = 10 * time.Minute
		}
		go d.startCleaner()
		d.cleanerCloseFinished = make(chan bool)
	}
	return d, nil
}

func funcName(f TaskFunction) string {
	parts := strings.Split(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), ".")
	return parts[len(parts)-1]
}

func (d *docket) RegisterFunction(f TaskFunction) {
	fName := funcName(f)
	if m, err := regexp.MatchString("^func[0-9]+$", fName); err != nil || m {
		panic("cannot register closures")
	}
	d.RegisterFunctionWithFuncName(fName, f)
}

func (d *docket) RegisterFunctionWithFuncName(funcName string, f TaskFunction) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.functions[funcName] = f
}

func (d *docket) setLogger(logger *slog.Logger) {
	subsystem := "pqdocket"
	if d.namespace != "" {
		subsystem = fmt.Sprintf("%s_%s", subsystem, d.namespace)
	}
	logger = logger.With("subsystem", subsystem)
	d.logger.Store(logger)
}

func (d *docket) channelName() string {
	c := "pqdocket_task_scheduled"
	if d.namespace != "" {
		c += "_" + d.namespace
	}
	return c
}
func (d *docket) tableName() string {
	if d.useManuallyCreatedTable != "" {
		return d.useManuallyCreatedTable
	}
	t := taskTableName
	if d.namespace != "" {
		t += "_" + d.namespace
	}
	return t
}

func (d *docket) notifyTaskScheduledTx(tx *sql.Tx) {
	var err error
	if tx == nil {
		_, err = d.db.Exec("SELECT pg_notify($1 :: text, '')", d.channelName())
	} else {
		_, err = tx.Exec("SELECT pg_notify($1 :: text, '')", d.channelName())
	}
	if err != nil {
		d.logger.Load().With("error", err).Warn("ignored notifyTaskScheduled err")
	}
}

func (d *docket) handleListenerEvent(event pq.ListenerEventType, err error) {
	l := d.logger.Load()
	switch event {
	case pq.ListenerEventConnected:
		l.Info("listener connected")
	case pq.ListenerEventDisconnected:
		l.With("error", err).Error("listener disconnected")
	case pq.ListenerEventReconnected:
		l.Warn("listener reconnected")
	case pq.ListenerEventConnectionAttemptFailed:
		l.With("error", err).Error("listener connection attempt failed")
	}
}

func (d *docket) Close() {
	d.mu.Lock()
	alreadyClosed := d.closed
	if !alreadyClosed {
		d.closed = true
		close(d.close)
	}
	d.mu.Unlock()
	<-d.closeFinished
	if d.cleanerCloseFinished != nil {
		<-d.cleanerCloseFinished
	}
	if !alreadyClosed {
		l := d.logger.Load()
		l.Info("close: closing listener...")
		err := d.listener.Close()
		if err != nil {
			l.With("error", err).Warn("err closing listener")
		}
		l.Info("close: closing db...")
		err = d.db.Close()
		if err != nil {
			l.With("error", err).Warn("err closing db")
		}
		l.Info("close: done")
	}
}
