package tests

import (
	"context"
	"database/sql"
	"fmt"
)

func runInTransaction(ctx context.Context, db *sql.DB, f func(tx *sql.Tx) error) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("db: panic: rolling back:", r)
			panic(r)
		} else if err != nil {
			err = fmt.Errorf("db: rolling back: %w", err)
			return
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
			err = tx.Commit()
		}
	}()
	err = f(tx)
	return
}
