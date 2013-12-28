package freetds

import (
	"database/sql/driver"
	"errors"
	"io"
)

//implements Stmt interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type Stmt struct {
	query string
	numInput int
	conn *Conn
}

func (s *Stmt) Close() error {
	return nil
}

func (s *Stmt) NumInput() int {
	return s.numInput
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	results, err := s.conn.ExecuteSql(s.query, args...)
	if err != nil {
		return nil, err
	}
	return &ExecResult{results: results}, nil
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	results, err := s.conn.ExecuteSql(s.query, args...)
	if err != nil {
		return nil, err
	}
	return &QueryRows{results: results}, nil
}


//implements Rows interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type QueryRows struct {
	results []*Result 
	currentRow int
}

func (r *QueryRows) Columns() []string {
	cols := make([]string, len(r.results[0].Columns))
	for i, c := range r.results[0].Columns {
		cols[i] = c.Name
	}
	return cols
} 

func (r *QueryRows) Close() error {
	return nil
}

func (r *QueryRows) Next(dest []driver.Value) error {
	if len(r.results) == 0 {
		return io.EOF
	}
	if r.currentRow >= len(r.results[0].Rows) {
		return io.EOF
	}
	for i, _ := range dest {
		dest[i] = r.results[0].Rows[r.currentRow][i]
	}
	r.currentRow++
	return nil
}


//implements Result interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type ExecResult struct {
	results []*Result 
}

func (r *ExecResult) RowsAffected() (int64, error){
	if val := r.statusRowValue("rows_affected"); val != -1 {
		return val, nil
	} 
	return 0, errors.New("no RowsAffected available")
}

func (r *ExecResult) LastInsertId() (int64, error){
	if val := r.statusRowValue("last_insert_id"); val != -1 {
		return val, nil
	} 
	return 0, errors.New("no LastInsertId available")
}

func (r *ExecResult) statusRowValue(columnName string) int64 {
	lastResult := r.results[len(r.results) -1]
	idx := -1
	for i, col := range lastResult.Columns {
		if columnName == col.Name {
			idx = i
			break
		}
	}
	if idx >= 0 {
		if val, ok := lastResult.Rows[0][idx].(int64); ok  {
			return val
		}
	}
	return -1
}

