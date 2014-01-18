package freetds

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
	"github.com/stretchrcom/testify/assert"
)

var CREATE_DB_SCRIPTS = [...]string{`
if exists(select * from sys.tables where name = 'freetds_types')
drop table freetds_types
;
create table freetds_types (
  int int null,
  long bigint null,
  smallint smallint null,
  tinyint tinyint null,
  varchar varchar(255) null,
  nvarchar nvarchar(255) null,
  char char(255) null,
  nchar nchar(255) null,
  text text null,
  ntext ntext null,
  datetime datetime null,
  smalldatetime smalldatetime null,
  money money null,
  smallmoney smallmoney null,
  real real null,
  float float(53) null,
  bit bit null,
  timestamp timestamp null,
  binary binary(10) null
)
;

insert into freetds_types (int, long, smallint, tinyint, varchar, nvarchar, char, nchar, text, ntext, datetime, smalldatetime, money, smallmoney, real, float, bit, binary)
values (2147483647,   9223372036854775807, 32767, 255, 'išo medo u dućan   ','išo medo u dućan    2','išo medo u dućan    3','išo medo u dućan    4','išo medo u dućan    5','išo medo u dućan    6', '1972-08-08T10:11:12','1972-08-08T10:11:12', 1234.5678,   1234.5678,  1234.5678,  1234.5678, 0, 0x123567890)

insert into freetds_types (int, long, smallint, tinyint, varchar, nvarchar, char, nchar, text, ntext, datetime, smalldatetime, money, smallmoney, real, float, bit, binary)
values (-2147483648, -9223372036854775808, -32768,  0, 'nije reko dobar dan','nije reko dobar dan 2','nije reko dobar dan 3','nije reko dobar dan 4','nije reko dobar dan 5','nije reko dobar dan 6', '1998-10-10T16:17:18','1998-10-10T16:17:18', -1234.5678, -1234.5678, -1234.5678, -1234.5678, 1, 0x0987654321abcd)

insert into freetds_types (int) values (3)
`, `
if exists(select * from sys.procedures where name = 'freetds_return_value')
  drop procedure freetds_return_value
`, `
create procedure freetds_return_value as
  return -5`}

func ConnectToTestDb(t *testing.T) *Conn {
	conn, err := ConnectWithConnectionString(testDbConnStr())
	if err != nil {
		t.Errorf("can't connect to the test database")
		return nil
	}
	return conn
}

func testDbConnStr() string {
	connStr := os.Getenv("GOFREETDS_CONN_STR")
	mirror := os.Getenv("GOFREETDS_MIRROR_HOST")
	if mirror != "" {
		connStr = fmt.Sprintf("%s;mirror=%s", connStr, mirror)
	}
	return connStr
}

func IsMirrorHostDefined() bool {
	return os.Getenv("GOFREETDS_MIRROR_HOST") != ""
}

func TestConnect(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	if !conn.isLive() {
		t.Error()
	}
	if conn.isDead() {
		t.Error()
	}
}

func TestItIsSafeToCloseFailedConnection(t *testing.T) {
	conn := new(Conn)
	if conn == nil {
		return
	}
	if conn.isLive() {
		t.Error()
	}
	if !conn.isDead() {
		t.Error()
	}
	conn.Close()
}

func TestCreateTable(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	for _, s := range CREATE_DB_SCRIPTS {
		_, err := conn.Exec(s)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestStoredProcedureReturnValue(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	results, err := conn.Exec("exec freetds_return_value")
	if err != nil {
		t.Error(err)
	}
	if results[0].ReturnValue != -5 {
		t.Errorf("expected return value was -5 got %d", results[0].ReturnValue)
	}
}

func TestReading(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	results, err := conn.Exec("select * from freetds_types")
	if err != nil || len(results) != 1 {
		fmt.Printf("error: %s\n%s\n%s", err, conn.Message, conn.Error)
		return
	}
	//PrintResults(results)
}

func TestRetryOnKilledConnection(t *testing.T) {
	conn1 := ConnectToTestDb(t)
	conn2 := ConnectToTestDb(t)
	if conn1 == nil || conn2 == nil {
		return
	}

	pid1, _ := conn1.SelectValue("select @@spid")
	conn2.Exec(fmt.Sprintf("kill %d", pid1))
	if conn1.isLive() {
		t.Error()
	}
	if !conn1.isDead() {
		t.Error()
	}
	_, err := conn1.exec("select * from authors")
	if err == nil {
		t.Error()
	}
	rst, err := conn1.Exec("select * from authors")
	if err != nil || len(rst) != 1 || len(rst[0].Rows) != 23 || rst[0].RowsAffected != 23 {
		t.Error()
	}
}

func TestExecute(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	rst, err := conn.Exec("select 1")
	if rst == nil || err != nil {
		t.Error()
	}
	if len(rst) != 1 {
		t.Error()
	}
	rst, err = conn.Exec("select missing")
	if rst != nil || err == nil {
		t.Error()
	}
	rst, err = conn.Exec("print 'pero'")
	if err != nil || !strings.Contains(conn.Message, "pero") || len(rst) != 1 || len(rst[0].Rows) > 0 {
		t.Error()
	}
	rst, err = conn.Exec("sp_help 'authors'")
	if err != nil || len(rst) != 9 {
		t.Error()
	}
}

func TestRowsAffected(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	rst, err := conn.Exec("select * from authors")
	if err != nil || len(rst) != 1 || len(rst[0].Rows) != 23 || rst[0].RowsAffected != 23 {
		t.Error()
	}
	rst, err = conn.Exec("update authors set zip = zip")
	if err != nil || len(rst) != 1 || len(rst[0].Rows) > 0 || rst[0].RowsAffected != 23 {
		t.Error()
	}

}

func TestSelectValue(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	val, err := conn.SelectValue("select 1")
	if val.(int32) != 1 || err != nil {
		t.Error()
	}
	val, err = conn.SelectValue("select 1 where 1=2")
	if val != nil || err == nil {
		t.Error()
	}
	val, err = conn.SelectValue("select missing")
	if val != nil || err == nil {
		t.Error()
	}
}

func TestDbUse(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	err := conn.DbUse()
	if err != nil {
		t.Error()
	}
	conn.database = "missing"
	err = conn.DbUse()
	if err == nil && !strings.Contains(err.Error(), "unable to use database missing") {
		t.Error()
	}
}

func TestMirroring(t *testing.T) {
	if !IsMirrorHostDefined() {
		fmt.Printf("skipping TestMirroring\n")
		return
	}
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	rst, err := conn.Exec("select * from authors")
	if err != nil && rst != nil && len(rst) == 1 && len(rst[0].Rows) == 23 {
		t.Error()
	}
	err = failover(conn)
	if err != nil {
		fmt.Printf("failover error %s %s %s\n", err, conn.Error, conn.Message)
		t.Error()
	}
	rst, err = conn.Exec("select * from authors")
	if err != nil && rst != nil && len(rst) == 1 && len(rst[0].Rows) == 23 {
		t.Error()
	}
}

func failover(conn *Conn) error {
	_, err := conn.Exec("use master; ALTER DATABASE pubs SET PARTNER FAILOVER")
	return err
}

func BenchmarkConnectExecute(b *testing.B) {
	for i := 0; i < 100; i++ {
		conn := ConnectToTestDb(nil)
		conn.Exec("select * from authors")
		conn.Close()
	}
}

func BenchmarkParalelConnectExecute(b *testing.B) {
	pool := make(chan int, 5) //connection pool for x connections
	running := 0
	for i := 0; i < 100; i++ {
		go func(i int) {
			pool <- i
			running++
			//fmt.Printf("starting %d\n", i)
			conn := ConnectToTestDb(nil)
			defer conn.Close()
			conn.Exec("select * from authors")
			<-pool
			running--
			//fmt.Printf("finished %d\n", i)
		}(i)
	}
	for {
		time.Sleep(1e8)
		fmt.Printf("running %d\n", running)
		if running == 0 {
			break
		}
	}
}

func TestTransactionCommitRollback(t *testing.T) {
	conn := ConnectToTestDb(t)
	createTestTable2(t, conn, "test_transaction", "")
	err := conn.Begin()
	assert.Nil(t, err)
	conn.Exec("insert into test_transaction values('1')")
	conn.Exec("insert into test_transaction values('2')")
	err = conn.Commit()
	assert.Nil(t, err)
	rows, err := conn.SelectValue("select count(*)  from test_transaction")
	assert.Nil(t, err)
	assert.Equal(t, rows, 2)

	//roollback
	err = conn.Begin()
	assert.Nil(t, err)
	conn.Exec("insert into test_transaction values('3')")
	err = conn.Rollback()
	assert.Nil(t, err)
	rows, err = conn.SelectValue("select count(*)  from test_transaction")
	assert.Nil(t, err)
	assert.Equal(t, rows, 2)
}

func createTestTable2(t *testing.T, conn *Conn, name string, columDef string) {
	if columDef == "" {
		columDef = "id int not null identity,  name varchar(255)"
	}
	sql := fmt.Sprintf(`
	if exists(select * from sys.tables where name = 'table_name')
	  drop table table_name
	;
  create table table_name (
    %s
  ) 
  `, columDef)
	sql = strings.Replace(sql, "table_name", name, 3)
	_, err := conn.Exec(sql)
	assert.Nil(t, err)
}
