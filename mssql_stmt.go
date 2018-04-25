package freetds

import (
	"database/sql/driver"
	"errors"
	"io"
)

//implements Stmt interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type MssqlStmt struct {
	query    string
	numInput int
	conn     *Conn
}

func (s *MssqlStmt) Close() error {
	return nil
}

func (s *MssqlStmt) NumInput() int {
	return s.numInput
}

func (s *MssqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	results, err := s.conn.ExecuteSql(s.query, args...)
	if err != nil {
		return nil, err
	}
	return &MssqlResult{results: results}, nil
}

func (s *MssqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	results, err := s.conn.ExecuteSql(s.query, args...)
	if err != nil {
		return nil, err
	}
	return &MssqlRows{results: results}, nil
}

//implements Rows interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type MssqlRows struct {
	results       []*Result
	currentRow    int
	currentResult int
}

func (r MssqlRows) result() *Result {
	return r.results[r.currentResult]
}

func (r *MssqlRows) Columns() []string {
	cols := make([]string, len(r.result().Columns))
	for i, c := range r.result().Columns {
		cols[i] = c.Name
	}
	return cols
}

func (r *MssqlRows) Close() error {
	return nil
}

func (r *MssqlRows) Next(dest []driver.Value) error {
	if len(r.results) == 0 {
		return io.EOF
	}
	if r.currentRow >= len(r.result().Rows) {
		return io.EOF
	}
	for i, _ := range dest {
		dest[i] = r.result().Rows[r.currentRow][i]
	}
	r.currentRow++
	return nil
}

// //true and move to next result if exists
// func (r *MssqlRows) MoreResults() bool {
// 	//last result is statusRow, because of that -2
// 	if r.currentResult >= len(r.results) - 2 {
// 		r.currentResult++
// 		return true
// 	}
// 	return false
// }

//implements Result interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type MssqlResult struct {
	results []*Result
}

func (r *MssqlResult) RowsAffected() (int64, error) {
	if val := r.statusRowValue("rows_affected"); val != -1 {
		return val, nil
	}
	return 0, errors.New("no RowsAffected available")
}

func (r *MssqlResult) LastInsertId() (int64, error) {
	if val := r.statusRowValue("last_insert_id"); val != -1 {
		return val, nil
	}
	return 0, errors.New("no LastInsertId available")
}

func (r *MssqlResult) statusRowValue(columnName string) int64 {
	lastResult := r.results[len(r.results)-1]
	idx := -1
	for i, col := range lastResult.Columns {
		if columnName == col.Name {
			idx = i
			break
		}
	}
	if idx >= 0 {
		if val, ok := lastResult.Rows[0][idx].(int64); ok {
			return val
		}
		if val, ok := lastResult.Rows[0][idx].(float64); ok {
			return int64(val)
		}
	}
	return -1
}
