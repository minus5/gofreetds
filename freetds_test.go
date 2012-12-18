package freetds

import ("testing"; "os";"fmt";"time")

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
`,`
if exists(select * from sys.procedures where name = 'freetds_return_value')
  drop procedure freetds_return_value
`,`
create procedure freetds_return_value as
  return -5`}

func ConnectToTestDb(t *testing.T) (*Conn) {
  db   := os.Getenv("GOFREETDS_DB")
  user := os.Getenv("GOFREETDS_USER")
  pwd  := os.Getenv("GOFREETDS_PWD")
  host := os.Getenv("GOFREETDS_HOST")
  conn, err := Connect(user, pwd, host, db)
  if err != nil {
    t.Errorf("can't connect to the test database")
    return nil
  }
  return conn
}

func TestConnect(t *testing.T) {
  conn := ConnectToTestDb(t)
  if conn == nil { return }
  defer conn.Close()
  if !conn.IsLive() {
    t.Error()
  }
  if conn.isDead() {
    t.Error()
  }
}

func TestItIsSafeToCloseFailedConnection(t *testing.T) {
  conn := new(Conn)
  if conn == nil { return }
  if conn.IsLive() {
    t.Error()
  }
  if !conn.isDead() {
    t.Error()
  }
  conn.Close()
}

func TestCreateTable(t *testing.T) {
  conn := ConnectToTestDb(t)
  if conn == nil { return }
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
  if conn == nil { return }
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
  if conn == nil { return }
  defer conn.Close()

  results, err := conn.Exec("select * from freetds_types")
  if err != nil {
    fmt.Printf("error: %s\n%s\n%s", err, conn.Message, conn.Error)
    return
  }
  PrintResults(results)
}

func BenchmarkConnectExecute(b *testing.B) {
  for i := 0; i < 100; i++ {
    conn := ConnectToTestDb(nil)
    conn.Exec("select * from authors")
    conn.Close()
  }
}

func TestRetryOnKilledConnection(t *testing.T) {
  conn1 := ConnectToTestDb(t)
  conn2 := ConnectToTestDb(t)
  if conn1 == nil || conn2 == nil {
    return
  }

  pid1, _ := conn1.SelectValue("select @@spid")
  conn2.Exec(fmt.Sprintf("kill %d", pid1))
  if conn1.IsLive() {
    t.Error()
  }
  _, err := conn1.exec("select * from authors")
  if err == nil {
    t.Error()
  }
  _, err = conn1.Exec("select * from authors")
  if err != nil {
    t.Error()
  }
}

func BenchmarkParalelConnectExecute(b *testing.B) {
  pool := make(chan int, 100) //connection pool for 100 connections
  running := 0
  for i := 0; i < 1000; i++ {
    go func(i int) {
      pool <- i
      running++
      fmt.Printf("starting %d\n", i)
      conn := ConnectToTestDb(nil)
      defer conn.Close()
      conn.Exec("select * from authors")
      <- pool
      running--
      fmt.Printf("finished %d\n", i)
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
