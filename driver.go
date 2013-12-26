package freetds

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"fmt"
	"io"
	"log"
	"time"
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
	return errNotImplemented
}

func (c *TdsConn) Begin() (driver.Tx, error) {
	return nil, errNotImplemented
}

func NewTdsStmt(query string, c *Conn) *TdsStmt {
	parts := strings.Split(query, "?")
	var statement string
	numInput := len(parts) - 1
	statement = parts[0]
	for i, part := range parts {
		if i > 0 {
			statement = fmt.Sprintf("%s@p%d%s", statement, i, part)
		}
	}
	s := &TdsStmt{query: query, statement: statement, numInput: numInput, conn: c}
	return s
}

type TdsStmt struct {
	query string
	statement string
	numInput int
	conn *Conn
}

func (s *TdsStmt) Close() error {
	return errNotImplemented
}

func (s *TdsStmt) NumInput() int {
	return s.numInput
}

func (s *TdsStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errNotImplemented
}

func (s *TdsStmt) Query(args []driver.Value) (driver.Rows, error) {
	paramDef := ""
	params := ""
	for i, arg := range args {
		if i > 0 {
			params += ", "
			paramDef += ", "
		}
		sqlType, sqlValue := go2SqlDataType(arg)
		paramName := fmt.Sprintf("@p%d", i+1)
		paramDef += fmt.Sprintf("%s %s", paramName, sqlType)
		params += fmt.Sprintf("%s=%s", paramName, sqlValue)
	}
	sql := fmt.Sprintf("exec sp_executesql N'%s', N'%s', %s", quote(s.statement), paramDef, params)
	results, err := s.conn.Exec(sql)
	if err == nil {
		return &TdsRows{results: results}, nil
	}
	return nil, err
}

func quote(in string) string {
	return strings.Replace(in, "'", "''", -1)
}

func go2SqlDataType(value interface{}) (string, string) {
	strValue := fmt.Sprintf("%v", value)
	switch t := value.(type) { 
	case uint8, int8:
		return "tinyint", strValue
	case uint16, int16:
		return "smallint", strValue
	case uint32, int32, int:
		return "int", strValue
	case uint64, int64:
		return "bigint", strValue
	case float32, float64:
		return "real", strValue
	case string: {
	}
	case time.Time: {
		t, _ := value.(time.Time)
		strValue = t.Format(time.RFC3339)
	}
	case []byte: {
		b, _ := value.([]byte)
		return fmt.Sprintf("varbinary (%d)", len(b)), 
		fmt.Sprintf("0x%x", b)
	}
	default: 
		log.Printf("unknown dataType %t", t)
	}
	return fmt.Sprintf("nvarchar (%d)", len(strValue)), 
	fmt.Sprintf("'%s'", quote(strValue))

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
	return errNotImplemented
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

