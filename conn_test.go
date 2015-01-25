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
	conn, err := NewConn(testDbConnStr(1))
	if err != nil {
		t.Errorf("can't connect to the test database")
		return nil
	}
	return conn
}

func testDbConnStr(maxPoolSize int) string {
	connStr := os.Getenv("GOFREETDS_CONN_STR")
	mirror := os.Getenv("GOFREETDS_MIRROR_HOST")
	if mirror != "" {
		connStr = fmt.Sprintf("%s;mirror=%s", connStr, mirror)
	}
	connStr = fmt.Sprintf("%s;max_pool_size=%d", connStr, maxPoolSize)
	return connStr
}

func IsMirrorHostDefined() bool {
	return os.Getenv("GOFREETDS_MIRROR_HOST") != ""
}

func TestConnect(t *testing.T) {
	conn := ConnectToTestDb(t)
	assert.NotNil(t, conn)
	defer conn.Close()
	assert.True(t, conn.isLive())
	assert.False(t, conn.isDead())
}

func TestItIsSafeToCloseFailedConnection(t *testing.T) {
	conn := new(Conn)
	assert.NotNil(t, conn)
	assert.False(t, conn.isLive())
	assert.True(t, conn.isDead())
}

func TestCreateTable(t *testing.T) {
	conn := ConnectToTestDb(t)
	assert.NotNil(t, conn)
	defer conn.Close()
	for _, s := range CREATE_DB_SCRIPTS {
		_, err := conn.Exec(s)
		assert.Nil(t, err)
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

func TestReadingTextNtext(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	results, err := conn.Exec(`select top 2 text, ntext from dbo.freetds_types`)
	assert.Nil(t, err)
	assert.Equal(t, "išo medo u dućan    5", results[0].Rows[0][0])
	assert.Equal(t, "išo medo u dućan    6", results[0].Rows[0][1])
	assert.Equal(t, "nije reko dobar dan 5", results[0].Rows[1][0])
	assert.Equal(t, "nije reko dobar dan 6", results[0].Rows[1][1])
}

func TestRetryOnKilledConnection(t *testing.T) {
	conn1 := ConnectToTestDb(t)
	conn2 := ConnectToTestDb(t)
	if conn1 == nil || conn2 == nil {
		return
	}

	pid1, _ := conn1.SelectValue("select @@spid")
	conn2.Exec(fmt.Sprintf("kill %d", pid1))
	assert.False(t, conn1.isLive())
	assert.True(t, conn1.isDead())
	_, err := conn1.exec("select * from authors")
	assert.NotNil(t, err)
	rst, err := conn1.Exec("select * from authors")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(rst))
	assert.Equal(t, 23, len(rst[0].Rows))
	assert.Equal(t, 23, rst[0].RowsAffected)
}

func TestExecute(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	rst, err := conn.Exec("select 1")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	assert.Equal(t, 1, len(rst))
	rst, err = conn.Exec("select missing")
	assert.NotNil(t, err)
	assert.Nil(t, rst)
	rst, err = conn.Exec("print 'pero'")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	assert.True(t, strings.Contains(conn.Message, "pero"))
	assert.Equal(t, 1, len(rst))
	assert.Equal(t, 0, len(rst[0].Rows))
	rst, err = conn.Exec("sp_help 'authors'")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	assert.Equal(t, 9, len(rst))
}

func TestRowsAffected(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	rst, err := conn.Exec("select * from authors")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	assert.Equal(t, 1, len(rst))
	assert.Equal(t, 23, len(rst[0].Rows))
	assert.Equal(t, 23, rst[0].RowsAffected)

	rst, err = conn.Exec("update authors set zip = zip")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	assert.Equal(t, 1, len(rst))
	assert.Equal(t, 0, len(rst[0].Rows))
	assert.Equal(t, 23, rst[0].RowsAffected)
}

func TestSelectValue(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	val, err := conn.SelectValue("select 1")
	assert.Nil(t, err)
	assert.Equal(t, 1, val)

	val, err = conn.SelectValue("select 1 where 1=2")
	assert.NotNil(t, err)
	assert.Nil(t, val)

	val, err = conn.SelectValue("select missing")
	assert.NotNil(t, err)
	assert.Nil(t, val)
}

func TestDbUse(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	err := conn.DbUse()
	assert.Nil(t, err)

	conn.database = "missing"
	err = conn.DbUse()
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "unable to use database missing"))
}

func TestMirroring(t *testing.T) {
	if !IsMirrorHostDefined() {
		t.Skip("mirror host is not defined")
	}
	conn := ConnectToTestDb(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	rst, err := conn.Exec("select * from authors")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	assert.Equal(t, 1, len(rst))
	assert.Equal(t, 23, len(rst[0].Rows))
	assert.Equal(t, 23, rst[0].RowsAffected)

	err = failover(conn)
	if err != nil {
		fmt.Printf("failover error %s %s %s\n", err, conn.Error, conn.Message)
		assert.Nil(t, err)
	}
	rst, err = conn.Exec("select * from authors")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	assert.Equal(t, 1, len(rst))
	assert.Equal(t, 23, len(rst[0].Rows))
	assert.Equal(t, 23, rst[0].RowsAffected)
}

func failover(conn *Conn) error {
	_, err := conn.Exec("use master; ALTER DATABASE pubs SET PARTNER FAILOVER")
	time.Sleep(1e9)
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

//usefull for testing n
func printResults(results []*Result) {
	fmt.Printf("results %v", results)
	for _, r := range results {
		if r.Rows != nil {
			fmt.Printf("\n\nColums:\n")
			for j, c := range r.Columns {
				fmt.Printf("\t%3d%20s%10d%10d\n", j, c.Name, c.DbType, c.DbSize)
			}
			for i, _ := range r.Rows {
				for j, _ := range r.Columns {
					fmt.Printf("value[%2d, %2d]: %v\n", i, j, r.Rows[i][j])
				}
				fmt.Printf("\n")
			}
		}
		fmt.Printf("rows affected: %d\n", r.RowsAffected)
		fmt.Printf("return value: %d\n", r.ReturnValue)
	}
}

func TestWrongPassword(t *testing.T) {
	connStr := testDbConnStr(1)
	c := NewCredentials(connStr)
	c.pwd = c.pwd + "_wrong"
	conn, err := connectWithCredentials(c)
	assert.NotNil(t, err)
	assert.Nil(t, conn)
	assert.True(t, strings.Contains(err.Error(), "Login failed for user"))
	//t.Logf("wrong password message: %s", err)
}
