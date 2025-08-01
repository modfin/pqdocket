package tests

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/modfin/pqdocket"
	"log/slog"
	"math/rand"
	"os"
	"testing"
	"time"
)

var d pqdocket.Docket
var d2 pqdocket.Docket
var db *sql.DB

func TestMain(m *testing.M) {

	dbUrl := "postgres://postgres:qwerty@localhost:9300/postgres?sslmode=disable"

	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	var err error
	d, err = pqdocket.Init(dbUrl,
		pqdocket.Namespace("test"),
		pqdocket.Parallelism(40),
		pqdocket.DefaultClaimTime(20),
		pqdocket.WithLogger(l.With("instance", "d1")),
		pqdocket.EnableTaskCleaner(pqdocket.TaskCleanerSettings{PollInterval: time.Second}),
	)
	if err != nil {
		fmt.Println("d1 init err", err)
		os.Exit(1)
	}
	d2, err = pqdocket.Init(dbUrl,
		pqdocket.Namespace("test"),
		pqdocket.Parallelism(40),
		pqdocket.DefaultClaimTime(20),
		pqdocket.WithLogger(l.With("instance", "d2")),
		pqdocket.EnableTaskCleaner(pqdocket.TaskCleanerSettings{
			PollInterval: time.Second,
			Callback: func(tx *sql.Tx, task pqdocket.Task) error {
				if rand.Intn(100) > 90 {
					go func() {
						time.Sleep(2 * time.Second)
						err := tx.Commit()
						if !errors.Is(err, sql.ErrTxDone) {
							panic("returning an error from callback is expected to cause the tx to rollback")
						}
					}()
					return errors.New("rand error")
				}
				return nil
			},
		}),
	)
	if err != nil {
		fmt.Println("d2 init err", err)
		os.Exit(1)
	}

	SimpleInit()
	FindInit()
	ChainInit()
	ExtendedClaimInit()

	db, err = sql.Open("postgres", dbUrl)
	if err != nil {
		fmt.Println("db init err", err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(32)
	db.SetConnMaxLifetime(time.Minute)

	ret := m.Run()
	d.Close()
	d.Close() // intentional
	d2.Close()
	err = db.Close()
	if err != nil {
		fmt.Println("db close err", err)
		os.Exit(1)
	}
	os.Exit(ret)
}
