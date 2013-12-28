package freetds

import (
	"github.com/stretchrcom/testify/assert"
	"testing"
	"database/sql"
	"strings"
)

func open() (*sql.DB, error) {
	return sql.Open("mssql", "user=ianic;pwd=ianic;database=pubs;host=iow")
}

func TestGoSqlOpenConnection(t *testing.T) {
	db, err := open()
	if err != nil || db == nil {
		t.Error(err) 
	}
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


func TestLastInsertIdRowsAffected(t *testing.T) {
	db, _ := open()
	db.Exec(`
	if exists(select * from sys.tables where name = 'test_last_insert_id')
	  drop table test_last_insert_id
	;
  create table test_last_insert_id (
	  id int not null identity,
	  name varchar(255) 
  ) 
  `)
	r, err := db.Exec("insert into test_last_insert_id values(?)", "pero")
	assert.Nil(t, err)
	assert.NotNil(t, r) 
	id, err := r.LastInsertId()
	assert.Nil(t, err)
	assert.Equal(t, id, 1)
	ra, err := r.RowsAffected()
	assert.Nil(t, err)
	assert.Equal(t, ra, 1)
	
	r, err = db.Exec("insert into test_last_insert_id values(?)", "pero")
	assert.Nil(t, err)
	assert.NotNil(t, r) 
	id, err = r.LastInsertId()
	assert.Nil(t, err)
	assert.Equal(t, id, 2)
	ra, err = r.RowsAffected()
	assert.Nil(t, err)
	assert.Equal(t, ra, 1)
	
	r, err = db.Exec("update test_last_insert_id set name = ?", "jozo")
	assert.Nil(t, err)
	assert.NotNil(t, r) 
	id, err = r.LastInsertId() 
	assert.NotNil(t, err)
	ra, err = r.RowsAffected()
	assert.Nil(t, err)
	assert.Equal(t, ra, 2)

	r, err = db.Exec("delete from test_last_insert_id")
	assert.Nil(t, err) 
	ra, err = r.RowsAffected()
	assert.Nil(t, err)
	assert.Equal(t, ra, 2)
}

func createTestTable(t *testing.T, db *sql.DB, name string) {
	sql := `
	if exists(select * from sys.tables where name = 'table_name')
	  drop table table_name
	;
  create table table_name (
	  id int not null identity,
	  name varchar(255) 
  ) 
  `
	sql = strings.Replace(sql, "table_name", name, 3) 
	_, err := db.Exec(sql)	
	assert.Nil(t, err)
}

func TestTransCommit(t *testing.T) {
	db, _ := open()
	createTestTable(t, db, "test_tran")
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
	db, _ := open()
	createTestTable(t, db, "test_tran")
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
