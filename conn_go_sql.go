package freetds

import (
	"database/sql"
	"database/sql/driver"
)


//register driver for use with database/sql package
func init() {
	sql.Register("mssql", &Driver{})
}


//implements Driver interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type Driver struct {}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	return ConnectWithConnectionString(dsn)
}


//implementing Conn interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	_, numInput := query2Statement(query)
	s := &Stmt{query: query, numInput: numInput, conn: c}
	return s, nil
} 

//implementing Conn interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (c *Conn) Close() error {
	c.closeOrRelease()
	return nil
}

//implementing Conn interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (c *Conn) Begin() (driver.Tx, error) {
	t := &ConnTx{conn: c}
	return t, t.begin()
}


//implements Tx interface from http://golang.org/src/pkg/database/sql/driver/driver.go
type ConnTx struct {
	conn *Conn
}

func (t *ConnTx) begin() error {
	_, err := t.conn.Exec("begin transaction")
	return err
}

//implements Tx interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (t *ConnTx) Commit() error {
	_, err := t.conn.Exec("commit transaction")
	return err
}

//implements Tx interface from http://golang.org/src/pkg/database/sql/driver/driver.go
func (t *ConnTx) Rollback() error {
	_, err := t.conn.Exec("rollback transaction")
	return err
}

