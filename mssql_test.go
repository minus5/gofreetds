package freetds

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"os"
)

func open(t *testing.T) (*sql.DB, error, bool) {
	connStr := os.Getenv("GOFREETDS_CONN_STR")
	conn, err := NewConn(connStr)
	//if()
	if err != nil {
		t.Error(err)
	}
	db, err := sql.Open("mssql", testDbConnStr(1))
	return db, err, conn.sybaseMode125()
}

func TestGoSqlOpenConnection(t *testing.T) {
	db, err, _ := open(t)
	if err != nil || db == nil {
		t.Error(err)
	}
}

func TestMssqlConnOpen(t *testing.T) {
	d := &MssqlDriver{}
	c, err := d.Open(testDbConnStr(1))
	assert.Nil(t, err)
	assert.IsType(t, &MssqlConn{}, c)
	c.Close()
}

func TestMssqlConnOpenSybase125(t *testing.T) {
	d := &MssqlDriver{}
	c, err := d.Open(testDbConnStrSybase125(1))
	assert.Nil(t, err)
	assert.IsType(t, &MssqlConn{}, c)
	c.Close()
}


func TestGoSqlDbQueryRow(t *testing.T) {
	db, err, _ := open(t)
	defer db.Close()
	assert.Nil(t, err)
	row := db.QueryRow("SELECT au_fname, au_lname name FROM authors WHERE au_id = ?", "172-32-1176")
	var firstName, lastName string
	err = row.Scan(&firstName, &lastName)
	assert.Nil(t, err)
	assert.Equal(t, firstName, "Johnson")
	assert.Equal(t, lastName, "White")
}

func TestGoSqlDbQuery(t *testing.T) {
	db, err, _ := open(t)
	defer db.Close()
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
	db, err, _ := open(t)
	defer db.Close()
	assert.Nil(t, err)
	assert.NotNil(t, db)
	stmt, err := db.Prepare("SELECT au_fname, au_lname name FROM authors WHERE au_lname = ? order by au_id")
	assert.Nil(t, err)
	rows, err := stmt.Query("Ringer")
	assert.Nil(t, err)
	testRingers(t, rows)
}

func TestLastInsertIdRowsAffected(t *testing.T) {
	db, _, sybase125 := open(t)
	defer db.Close()
	if sybase125 {
		t.Skip("LastInsertId and RowsEffective not returned in Sybase 12.5")
	}
	createTestTable(t, db, sybase125,"test_last_insert_id", "")
	r, err := db.Exec("insert into [test_last_insert_id] values(?)", "pero")
	assert.Nil(t, err)
	assert.NotNil(t, r)
	id, err := r.LastInsertId()
	assert.Nil(t, err)
	assert.EqualValues(t, id, 1)
	ra, err := r.RowsAffected()
	assert.Nil(t, err)
	assert.EqualValues(t, ra, 1)

	r, err = db.Exec("insert into test_last_insert_id values(?)", "pero")
	assert.Nil(t, err)
	assert.NotNil(t, r)
	id, err = r.LastInsertId()
	assert.Nil(t, err)
	assert.EqualValues(t, id, 2)
	ra, err = r.RowsAffected()
	assert.Nil(t, err)
	assert.EqualValues(t, ra, 1)

	r, err = db.Exec("update test_last_insert_id set name = ?", "jozo")
	assert.Nil(t, err)
	assert.NotNil(t, r)
	id, err = r.LastInsertId()
	assert.NotNil(t, err)
	ra, err = r.RowsAffected()
	assert.Nil(t, err)
	assert.EqualValues(t, ra, 2)

	r, err = db.Exec("delete from test_last_insert_id")
	assert.Nil(t, err)
	ra, err = r.RowsAffected()
	assert.Nil(t, err)
	assert.EqualValues(t, ra, 2)
}

func createTestTable(t *testing.T, db *sql.DB, sybase125 bool, name string, columDef string) {
	if columDef == "" {
		columDef = "id int not null identity,  name varchar(255)"
		if sybase125 {
			columDef = "id int identity not null,  name varchar(255)"
		}
	}

	sql := fmt.Sprintf(`
	if exists(select * from sys.tables where name = 'table_name')
	  drop table table_name
	;
  create table table_name (
    %s
  ) 
  `, columDef)

	if sybase125 {
		sql = `
		if exists(select name from sysobjects where name = "table_name")
	  drop table table_name 
		`
		sql = strings.Replace(sql, "table_name", name, 2)
		result, err := db.Exec(sql)
		result = result
		assert.Nil(t, err)

		sql = fmt.Sprintf(`
		create table [table_name] ( 
		%s 
		)
		`, columDef)
	}
	sql = strings.Replace(sql, "table_name", name, 3)
	_, err := db.Exec(sql)
	assert.Nil(t, err)
}

func TestTransCommit(t *testing.T) {
	db, _, sybase125 := open(t)
	defer db.Close()
	createTestTable(t, db, sybase125, "test_tran", "")
	tx, err := db.Begin()
	assert.Nil(t, err)
	tx.Exec("insert into test_tran values(?)", "pero")
	tx.Exec("insert into test_tran values(?)", "jozo")
	err = tx.Commit()
	assert.Nil(t, err)
	row := db.QueryRow("select count(*)  from test_tran")
	assert.Nil(t, err)
	var count int
	err = row.Scan(&count)
	assert.Nil(t, err)
	assert.Equal(t, count, 2)
}

func TestTransRollback(t *testing.T) {
	db, _, sybase125 := open(t)
	defer db.Close()
	createTestTable(t, db, sybase125, "test_tran", "")
	tx, err := db.Begin()
	assert.Nil(t, err)
	tx.Exec("insert into test_tran values(?)", "pero")
	tx.Exec("insert into test_tran values(?)", "jozo")
	err = tx.Rollback()
	assert.Nil(t, err)
	row := db.QueryRow("select count(*)  from test_tran")
	assert.Nil(t, err)
	var count int
	err = row.Scan(&count)
	assert.Nil(t, err)
	assert.Equal(t, count, 0)
}

func TestBlobs(t *testing.T) {
	db, _, sybase125 := open(t)
	defer db.Close()
	columnDef := "id int identity, blob varbinary(16), blob2 varbinary(MAX)"
	if sybase125 {
		columnDef = "id int identity, blob image, blob2 image"
		//t.Skip("Blobs not supported in Sybase 12.5")
	}
	createTestTable(t, db, sybase125, "test_blobs", columnDef)
	want := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	_, err := db.Exec("insert into test_blobs values(?, ?)", want, want)
	assert.Nil(t, err)

	var got []byte
	err = db.QueryRow("select blob from test_blobs").Scan(&got)
	assert.Nil(t, err)
	assert.Equal(t, 16, len(got))

	strWant := fmt.Sprintf("%x", want)
	strGot := fmt.Sprintf("%x", got)
	assert.Equal(t, strWant, strGot)
	assert.Equal(t, want, got)

	err = db.QueryRow("select blob2 from test_blobs").Scan(&got)
	assert.Nil(t, err)
	assert.Equal(t, 16, len(got))

	strWant = fmt.Sprintf("%x", want)
	strGot = fmt.Sprintf("%x", got)
	assert.Equal(t, strWant, strGot)
	assert.Equal(t, want, got)
}
