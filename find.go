package pqdocket

import (
	"database/sql"
	"fmt"
	"strings"
)

type FindParam func(d *docket) (string, string)

func WithTaskId(taskId string) FindParam {
	return func(d *docket) (string, string) {
		return "task_id", taskId
	}
}

func WithRefId(refId string) FindParam {
	return func(d *docket) (string, string) {
		return "ref_id", refId
	}
}

func WithTaskFunction(f TaskFunction) FindParam {
	return func(d *docket) (string, string) {
		fName := funcName(f)
		d.mu.RLock()
		_, ok := d.functions[fName]
		d.mu.RUnlock()
		if !ok {
			return "", ""
		}
		return "func", fName
	}
}

func (d *docket) FindTasks(tx *sql.Tx, params ...FindParam) ([]Task, error) {
	q := `SELECT * FROM pqdocket_task WHERE TRUE`

	var args []interface{}
	for _, p := range params {
		paramName, param := p(d)
		if paramName == "" {
			continue
		}
		args = append(args, param)
		q += " AND " + paramName + " = " + fmt.Sprintf("$%d", len(args))
	}
	if tx != nil {
		q += " FOR UPDATE" // lock rows
	}
	q = strings.Replace(q, taskTableName, d.tableName(), 1)

	var rows *sql.Rows
	var err error
	if tx == nil {
		rows, err = d.db.Query(q, args...)
	} else {
		rows, err = tx.Query(q, args...)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		t, err := scanTaskFrom(rows)
		if err != nil {
			return nil, err
		}
		t.docket = d
		tasks = append(tasks, t)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return tasks, rows.Close()
}
