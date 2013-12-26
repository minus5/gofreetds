package freetds

import (
	"github.com/stretchrcom/testify/assert"
	"testing"
	"database/sql"
	"log" 
	"time"
)

func open() (*sql.DB, error) {
	return sql.Open("freetds", "user=ianic;pwd=ianic;database=pubs;host=iow")
}

func TestGoSqlOpenConnection(t *testing.T) {
	db, err := open()
	if err != nil || db == nil {
		t.Error(err) 
	}
	log.Printf("db %V", db)
}

func TestGoSqlDbQueryRow(t *testing.T) {
	db, err := open()
	assert.Nil(t, err)
	row := db.QueryRow("SELECT au_fname, au_lname name FROM authors WHERE au_id = ?", "172-32-1176")
	var firstName, lastName string
	err = row.Scan(&firstName, &lastName)
	assert.Nil(t, err)
	assert.Equal(t, firstName, "Johnson")
	assert.Equal(t, lastName, "White")
}

func TestGoSqlDbQuery(t *testing.T) {
	db, err := open()
	assert.Nil(t, err)
	rows, err := db.Query("SELECT au_fname, au_lname name FROM authors WHERE au_lname = ? order by au_id", "Ringer")
	assert.Nil(t, err)
	testRingers(t, rows)
}

func testRingers(t *testing.T, rows *sql.Rows) {
	var firstName, lastName string
	rows.Next() 
	err := rows.Scan(&firstName, &lastName)
	assert.Nil(t, err)
	assert.Equal(t, firstName, "Anne")
	rows.Next() 
	err = rows.Scan(&firstName, &lastName)
	assert.Nil(t, err)
	assert.Equal(t, firstName, "Albert")
}

func TestGoSqlPrepareQuery(t *testing.T) {
	//t.Skip()
	db, err := open()
	assert.Nil(t, err)
	assert.NotNil(t, db)
	stmt, err := db.Prepare("SELECT au_fname, au_lname name FROM authors WHERE au_lname = ? order by au_id")
	assert.Nil(t, err)
	rows, err := stmt.Query("Ringer")
	assert.Nil(t, err)
	testRingers(t, rows)
}

func TestNewTdsStmt(t *testing.T) {
	stmt := NewTdsStmt("select 1", nil)
	assert.Equal(t, stmt.numInput, 0)
	assert.Equal(t, stmt.statement, "select 1")

	stmt = NewTdsStmt("select 1 where 2 = ?", nil)
	assert.Equal(t, stmt.numInput, 1)
	assert.Equal(t, stmt.statement, "select 1 where 2 = @p1")

	stmt = NewTdsStmt("select 1 where 2 = ? and 3 = ?", nil)
	assert.Equal(t, stmt.numInput, 2)
	assert.Equal(t, stmt.statement, "select 1 where 2 = @p1 and 3 = @p2")
}


func TestGoTo2SqlDataType(t *testing.T) {
	var checker = func(value interface{}, sqlType string, sqlFormatedValue string) {
		actualSqlType, actualSqlFormatedValue := go2SqlDataType(value)
		assert.Equal(t, actualSqlType, sqlType)
		assert.Equal(t, actualSqlFormatedValue, sqlFormatedValue)
	}

	checker(123, "int", "123")
	checker(int64(123), "bigint", "123")
	checker(int8(123), "tinyint", "123")
	checker(123.23, "real", "123.23")
	checker(float64(123.23), "real", "123.23")

	checker("iso medo", "nvarchar (8)", "'iso medo'")
	checker("iso medo isn't", "nvarchar (14)", "'iso medo isn''t'")
	
	tm := time.Unix(1136239445, 0)
	checker(tm, "nvarchar (25)", "'2006-01-02T23:04:05+01:00'")

	checker([]byte{1,2,3,4,5,6,7,8}, "varbinary (8)", "0x0102030405060708")
}
