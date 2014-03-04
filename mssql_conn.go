package freetds

import (
	"database/sql"
	"database/sql/driver"
)

//register driver for use with database/sql package
func init() {
	sql.Register("mssql", &MssqlDriver{})
}

//implements Driver interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type MssqlDriver struct{}

//implements Open for Driver interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (d *MssqlDriver) Open(dsn string) (driver.Conn, error) {
	conn, err := NewConn(dsn)
	if err != nil {
		return nil, err
	}
	return &MssqlConn{conn: conn}, nil
}

//implements Conn interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type MssqlConn struct {
	conn *Conn
}

//implements Prepare for Conn interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (c *MssqlConn) Prepare(query string) (driver.Stmt, error) {
	_, numInput := query2Statement(query)
	s := &MssqlStmt{query: query, numInput: numInput, conn: c.conn}
	return s, nil
}

//implements Close for Conn interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (c *MssqlConn) Close() error {
	c.conn.Close()
	return nil
}

//implements Begin for Conn interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (c *MssqlConn) Begin() (driver.Tx, error) {
	t := &MssqlConnTx{conn: c.conn}
	return t, t.begin()
}

//implements Tx interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type MssqlConnTx struct {
	conn *Conn
}

func (t *MssqlConnTx) begin() error {
	_, err := t.conn.Exec("begin transaction")
	return err
}

//implements Commit for Tx interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (t *MssqlConnTx) Commit() error {
	_, err := t.conn.Exec("commit transaction")
	return err
}

//implements Rollback for Tx interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (t *MssqlConnTx) Rollback() error {
	_, err := t.conn.Exec("rollback transaction")
	return err
}
