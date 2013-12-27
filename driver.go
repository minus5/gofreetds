package freetds

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
)

func init() {
	sql.Register("freetds", &TdsDriver{})
}

var errNotImplemented  = errors.New("Not implemented.")

type TdsDriver struct {
}

func (d *TdsDriver) Open(dsn string) (driver.Conn, error) {
	conn, err := ConnectWithConnectionString(dsn)
	if err == nil {
		return &TdsConn{conn: conn}, nil
	}
	return nil, err
}

type TdsConn struct {
	conn *Conn
}

func (c *TdsConn) Prepare(query string) (driver.Stmt, error) {
	return NewTdsStmt(query, c.conn), nil
} 

func (c *TdsConn) Close() error {
	c.conn.Close()
	return nil
}

func (c *TdsConn) Begin() (driver.Tx, error) {
	return nil, errNotImplemented
}

func NewTdsStmt(query string, c *Conn) *TdsStmt {
	_, numInput := query2Statement(query)
	s := &TdsStmt{query: query, numInput: numInput, conn: c}
	return s
}

type TdsStmt struct {
	query string
	numInput int
	conn *Conn
}

func (s *TdsStmt) Close() error {
	return nil
}

func (s *TdsStmt) NumInput() int {
	return s.numInput
}

func (s *TdsStmt) Exec(args []driver.Value) (driver.Result, error) {
	results, err := s.conn.ExecuteSql(s.query, toInterfaceA(args)...)
	if err != nil {
		return nil, err
	}
	return &TdsResult{results: results}, nil
}

func (s *TdsStmt) Query(args []driver.Value) (driver.Rows, error) {
	results, err := s.conn.ExecuteSql(s.query, toInterfaceA(args)...)
	if err != nil {
		return nil, err
	}
	return &TdsRows{results: results}, nil
}

//FIXME - cast from []driver.Value to []interface{}, must be better way
func toInterfaceA(args[]driver.Value) []interface{} {
	args2 := make([]interface{}, len(args))
	for i, arg := range args {
		args2[i]= arg
	}
	return args2
}

type TdsRows struct {
	results []*Result 
	currentRow int
}

func (r *TdsRows) Columns() []string {
	cols := make([]string, len(r.results[0].Columns))
	for i, c := range r.results[0].Columns {
		cols[i] = c.Name
	}
	return cols
} 

func (r *TdsRows) Close() error {
	return nil
}

func (r *TdsRows) Next(dest []driver.Value) error {
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

type TdsResult struct {
	results []*Result 
}

func (r *TdsResult) RowsAffected() (int64, error){
	val := r.statusRowValue("rows_affected")
	if val == -1 {
		return 0, errors.New("no RowsAffected available")
	} 
	return val, nil
}

func (r *TdsResult) LastInsertId() (int64, error){
	val := r.statusRowValue("last_insert_id")
	if val == -1 {
		return 0, errors.New("no LastInsertId available")
	} 
	return val, nil
}

func (r *TdsResult) statusRowValue(columnName string) int64 {
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
