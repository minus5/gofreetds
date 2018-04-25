package freetds

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var CREATE_DB_SCRIPTS = [...]string{`
if exists(select * from sys.tables where name = 'freetds_types')
drop table freetds_types
`, `
create table freetds_types (
  int int null,
  long bigint null,
  smallint smallint null,
  tinyint tinyint null,
  varchar varchar(255) COLLATE latin1_general_ci_as null ,
  nvarchar nvarchar(255) COLLATE latin1_general_ci_as null,
  char char(255) COLLATE latin1_general_ci_as null,
  nchar nchar(255) COLLATE latin1_general_ci_as null,
  text text COLLATE latin1_general_ci_as null,
  ntext ntext COLLATE latin1_general_ci_as null,
  datetime datetime null,
  smalldatetime smalldatetime null,
  money money null,
  smallmoney smallmoney null,
  real real null,
  float float(53) null,
  bit bit null,
  timestamp timestamp null,
  binary binary(10) null,
  nvarchar_max nvarchar(max) COLLATE latin1_general_ci_as null,
  varchar_max varchar(max) COLLATE latin1_general_ci_as null,
  varbinary_max varbinary(max) null
)
;

insert into freetds_types (int, long, smallint, tinyint, varchar, nvarchar, char, nchar, text, ntext, datetime, smalldatetime, money, smallmoney, real, float, bit, binary)
values (2147483647,   9223372036854775807, 32767, 255, 'išo medo u dućan   ',N'išo medo u dućan    2','išo medo u dućan    3',N'išo medo u dućan    4','išo medo u dućan    5',N'išo medo u dućan    6', '1972-08-08T10:11:12','1972-08-08T10:11:12', 1234.5678,   1234.5678,  1234.5678,  1234.5678, 0, 0x123567890)

insert into freetds_types (int, long, smallint, tinyint, varchar, nvarchar, char, nchar, text, ntext, datetime, smalldatetime, money, smallmoney, real, float, bit, binary)
values (-2147483648, -9223372036854775808, -32768,  0, 'nije reko dobar dan',N'nije reko dobar dan 2','nije reko dobar dan 3',N'nije reko dobar dan 4','nije reko dobar dan 5',N'nije reko dobar dan 6', '1998-10-10T16:17:18','1998-10-10T16:17:18', -1234.5678, -1234.5678, -1234.5678, -1234.5678, 1, 0x0987654321abcd)

insert into freetds_types (int) values (3)
`, `
if exists(select * from sys.procedures where name = 'freetds_return_value')
  drop procedure freetds_return_value
`, `
create procedure freetds_return_value as
  return -5`,
	`
if exists(select * from sys.tables where name = 'tm')
drop table tm
`, `
create table [dbo].[tm] (
	[id] int NOT NULL IDENTITY(1, 1),
	[tm] datetime null
)`,
}

var CREATE_DB_SCRIPTS_SYBASE_12_5 = [...]string{`
if exists(select name from sysobjects where name = 'freetds_types')
drop table freetds_types
`, `
create table freetds_types (
  int int null,
  long decimal(19,0) null,
  smallint smallint null,
  tinyint tinyint null,
  varchar varchar(255) null ,
  nvarchar nvarchar(255) null,
  char char(255) null,
  nchar nchar(255) null,
  text text null,
  ntext text null,
  datetime datetime null,
  smalldatetime smalldatetime null,
  money money null,
  smallmoney smallmoney null,
  real real null,
  float float(48) null,
  bit bit,
  timestamp timestamp null,
  binary binary(10) null,
  nvarchar_max text null,
  varchar_max text null,
  varbinary_max image null
)


insert into freetds_types (int, long, smallint, tinyint, varchar, nvarchar, char, nchar, text, ntext, datetime, smalldatetime, money, smallmoney, real, float, bit, binary)
values (2147483647,   9223372036854775807, 32767, 255, 'išo medo u dućan   ',N'išo medo u dućan    2','išo medo u dućan    3',N'išo medo u dućan    4','išo medo u dućan    5',N'išo medo u dućan    6', '1972-08-08 10:11:12','1972-08-08 10:11:12', 1234.5678,   1234.5678,  1234.5678,  1234.5678, 0, 0x123567890)

insert into freetds_types (int, long, smallint, tinyint, varchar, nvarchar, char, nchar, text, ntext, datetime, smalldatetime, money, smallmoney, real, float, bit, binary)
values (-2147483648, -9223372036854775808, -32768,  0, 'nije reko dobar dan',N'nije reko dobar dan 2','nije reko dobar dan 3',N'nije reko dobar dan 4','nije reko dobar dan 5',N'nije reko dobar dan 6', '1998-10-10 16:17:18','1998-10-10 16:17:18', -1234.5678, -1234.5678, -1234.5678, -1234.5678, 1, 0x0987654321abcd)

insert into freetds_types (int, bit) values (3, 0)
`, `
if exists(select name from sysobjects where name='freetds_return_value' and type='P')
  drop procedure freetds_return_value
`, `
create procedure freetds_return_value as
  return -5`,
	`
if exists(select name from sysobjects where name='tm' and type='U')
drop table tm
`, `
create table tm (
	id int IDENTITY NOT NULL ,
	tm datetime null
)`,
}

func TestMain(m *testing.M) {

	err := runCreateDBScripts()
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(m.Run())
}

func runCreateDBScripts() error {
	conn, err := NewConn(testDbConnStr(1))
	if err != nil {
		return err
	}
	defer conn.Close()

	scripts := CREATE_DB_SCRIPTS
	if conn.sybaseMode125() {
		scripts = CREATE_DB_SCRIPTS_SYBASE_12_5
	}

	for _, s := range scripts {
		_, err := conn.Exec(s)
		if err != nil {
			return fmt.Errorf("Error running create db scripts with sql: %v\n%v", s, err)
		}
	}

	return nil
}

func ConnectToTestDb(t *testing.T) *Conn {
	conn, err := NewConn(testDbConnStr(1))
	if err != nil {
		t.Errorf("can't connect to the test database")
		return nil
	}
	return conn
}

func ConnectToTestDbSybase(t *testing.T) *Conn {
	conn, err := NewConn(testDbConnStrSybase(1))
	if err != nil {
		t.Errorf("can't connect to the test database")
		return nil
	}
	return conn
}

func ConnectToTestDbSybase125(t *testing.T) *Conn {
	conn, err := NewConn(testDbConnStrSybase125(1))
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

func testDbConnStrSybase(maxPoolSize int) string {
	connStr := os.Getenv("GOFREETDS_CONN_STR")
	mirror := os.Getenv("GOFREETDS_MIRROR_HOST")
	if mirror != "" {
		connStr = fmt.Sprintf("%s;mirror=%s;compatibility=sybase", connStr, mirror)
	}
	connStr = fmt.Sprintf("%s;max_pool_size=%d;compatibility=sybase", connStr, maxPoolSize)
	return connStr
}

func testDbConnStrSybase125(maxPoolSize int) string {
	connStr := os.Getenv("GOFREETDS_CONN_STR")
	mirror := os.Getenv("GOFREETDS_MIRROR_HOST")
	if mirror != "" {
		connStr = fmt.Sprintf("%s;mirror=%s;compatibility=sybase_12_5", connStr, mirror)
	}
	connStr = fmt.Sprintf("%s;max_pool_size=%d;compatibility=sybase_12_5", connStr, maxPoolSize)
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

func TestConnectSybase(t *testing.T) {
	conn := ConnectToTestDbSybase(t)
	assert.NotNil(t, conn)
	defer conn.Close()
	assert.True(t, conn.isLive())
	assert.False(t, conn.isDead())
	assert.True(t, conn.sybaseMode())
	assert.False(t, conn.sybaseMode125())
}

func TestConnectSybase125(t *testing.T) {
	conn := ConnectToTestDbSybase125(t)
	assert.NotNil(t, conn)
	defer conn.Close()
	assert.True(t, conn.isLive())
	assert.False(t, conn.isDead())
	assert.True(t, conn.sybaseMode125())
	assert.False(t, conn.sybaseMode())
}

func TestItIsSafeToCloseFailedConnection(t *testing.T) {
	conn := new(Conn)
	assert.NotNil(t, conn)
	assert.False(t, conn.isLive())
	assert.True(t, conn.isDead())
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
	if conn.sybaseMode125() {
		t.Skip("Ntext does not exist in Sybase 12.5")
	}
	results, err := conn.Exec(`select top 2 text, ntext from dbo.freetds_types`)
	assert.Nil(t, err)
	assert.Equal(t, "išo medo u ducan    5", results[0].Rows[0][0])
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

	if conn1.sybaseMode125() {
		conn1.Exec("select syb_quit()") //can't kill own session with kill command in sybase, use syb_quit()
	} else {
		pid1, _ := conn1.SelectValue("select @@spid")
		conn2.Exec(fmt.Sprintf("kill %d", pid1))
	}
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
	mincount := 7
	if conn.sybaseMode125() {
		mincount = 5
	}
	assert.True(t, len(rst) > mincount)
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
	assert.EqualValues(t, 1, val)

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
	assert.EqualValues(t, rows, 2)

	//roollback
	err = conn.Begin()
	assert.Nil(t, err)
	conn.Exec("insert into test_transaction values('3')")
	err = conn.Rollback()
	assert.Nil(t, err)
	rows, err = conn.SelectValue("select count(*)  from test_transaction")
	assert.Nil(t, err)
	assert.EqualValues(t, rows, 2)
}

func createTestTable2(t *testing.T, conn *Conn, name string, columDef string) {
	if columDef == "" {
		columDef = "id int not null identity,  name varchar(255)"
		if conn.sybaseMode125() {
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
	if conn.sybaseMode125() {
		_, _ = conn.Exec(strings.Replace(`if exists(select id from sysobjects where name = 'table_name' and type = 'U')
	  drop table table_name
	`, "table_name", name, 2))
		sql = fmt.Sprintf(`create table table_name ( %s )  `, columDef)
	}

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
	assert.True(t, strings.Contains(err.Error(), "Login failed"))
	//t.Logf("wrong password message: %s", err)
}

func TestLockTimeout(t *testing.T) {
	connStr := testDbConnStr(1)
	c := NewCredentials(connStr)
	c.lockTimeout = 1
	conn, err := connectWithCredentials(c)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	conn2, err := connectWithCredentials(c)
	assert.Nil(t, err)
	assert.NotNil(t, conn2)

	delaySql := "begin transaction; update authors set phone = phone; waitfor delay '00:00:01'; commit transaction"
	if conn.sybaseMode125() {
		delaySql = "begin transaction update authors set phone = phone waitfor delay '00:00:03' commit transaction"
	}
	go func() {
		_, err := conn.Exec(delaySql)
		assert.Nil(t, err)
	}()
	time.Sleep(1e8)
	immediateSql := "begin transaction; update authors set phone = phone; commit transaction"
	if conn.sybaseMode125() {
		immediateSql = "begin transaction update authors set phone = phone commit transaction"
	}
	_, err = conn2.Exec(immediateSql)
	assert.NotNil(t, err)
	failText := "Lock request time out period exceeded."
	if conn.sybaseMode125() {
		failText = "Could not acquire a lock within the specified wait period."
	}
	assert.True(t, strings.Contains(err.Error(), failText))
}

func TestParseFreeTdsVersion(t *testing.T) {
	data := []struct {
		version  string
		expected []int
	}{
		{"", []int{}},
		{" freetds 0.95.19 ", []int{}},
		{" freetds v0.95.19 ", []int{0, 95, 19}},
		{" freetds v0.96.01 ", []int{0, 96, 1}},
		{" freetds v1.01.02 ", []int{1, 1, 2}},
		{"  $Id: dblib.c,v 1.378.2.4 2011-06-07 08:52:29 freddy77 Exp $", []int{}},
		{" freetds v0.a.b ", []int{}},
		{" freetds v0.96.0.rc ", []int{0, 96, 0}},
	}
	for _, d := range data {
		assert.Equal(t, d.expected, parseFreeTdsVersion(d.version))
	}
}

func TestVarcharMax(t *testing.T) {
	testNvarcharMax(t, "some short string")
	testNvarcharMax(t, longString(8000))
	testNvarcharMax(t, longString(10000))
}

func longString(size int) string {
	return strings.Repeat("-", size)
}

func testNvarcharMax(t *testing.T, str string) {
	c := ConnectToTestDb(t)
	updateSql := "update dbo.freetds_types set nvarchar_max='%s' where int = 3"
	selectSql := "select nvarchar_max from dbo.freetds_types where int = 3"
	if c.sybaseMode125() {
		updateSql = "update freetds_types set nvarchar_max='%s' where int = 3"
		selectSql = "select nvarchar_max from freetds_types where int = 3"
	}
	_, err := c.Exec(fmt.Sprintf(updateSql, str))
	assert.Nil(t, err)
	val, err := c.SelectValue(selectSql)
	assert.Nil(t, err)
	//t.Logf("nvarchar_max len: %d", len(fmt.Sprintf("%s", val)))
	strVal, ok := val.(string)
	assert.True(t, ok)
	assert.Equal(t, len(str), len(strVal))
	assert.EqualValues(t, str, strVal)
}

func TestTypes(t *testing.T) {
	c := ConnectToTestDb(t)
	sql := "select * from  dbo.freetds_types"
	var err error
	if !c.sybaseMode125() {
		_, err = c.ExecuteSql(sql)
	} else {
		sql = "select * from  freetds_types"
		_, err = c.executeSqlSybase125(sql)
	}
	assert.Nil(t, err)
}

func TestUnknownDataTypeInExecuteSql(t *testing.T) {
	c := ConnectToTestDb(t)
	if c.sybaseMode125() {
		t.Skip("Sybase 12.5 doesn't have sp_executesql")
	}
	var str *string
	_, err := c.ExecuteSql("update dbo.freetds_types set nvarchar_max=? where int = 3", str)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "unknown dataType *string")

	var l *int
	_, err = c.ExecuteSql("update dbo.freetds_types set long=? where int = 3", l)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "unknown dataType *int")
}

// Also run with "go test --race" for race condition checking.
func TestMessageNumbers(t *testing.T) {
	const msgnumOne = 123
	const msgnumTwo = 456

	c := &Conn{
		messageNums: make(map[int]int),
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		c.addMessage("alpha", msgnumOne)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		c.addMessage("beta", msgnumOne)
		c.addMessage("delta", msgnumTwo)
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, c.HasMessageNumber(msgnumOne), 2)
	assert.Equal(t, c.HasMessageNumber(msgnumTwo), 1)
}
