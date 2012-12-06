package freetds

import ("testing"; "os";"fmt")

const CREATE_DB_SCRIPT = `
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
;

`

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
  if conn != nil {
    defer conn.Close()
  }
}

func TestCreateTable(t *testing.T) {
  conn := ConnectToTestDb(t)
  if conn == nil {
    return
  }
  defer conn.Close()
  _, err := conn.Exec(CREATE_DB_SCRIPT)
  if err != nil {
    t.Error(err)
  }
}

func TestReading(t *testing.T) {
  conn := ConnectToTestDb(t)
  if conn == nil {
    return
  }
  defer conn.Close()

  results, err := conn.Exec("select * from freetds_types")
  if err != nil {
    fmt.Printf("error: %s\n%s\n%s", err, conn.Message, conn.Error)
    return
  }
  fmt.Printf("results %v", results)
  for _, r := range results {
    fmt.Printf("\n\nColums:\n")
    for j, c := range r.Columns {
      fmt.Printf("\t%3d%20s%10d%10d\n", j, c.Name, c.DbType, c.DbSize)
    }
    for i, _ := range r.Rows {
      for j, _ := range r.Columns {
        fmt.Printf("value[%2d, %2d]: %v\n", i, j, r.Rows[i][j])
//        fmt.Printf("%20v", r.Rows[i][j])
      }
      fmt.Printf("\n")
    }
    fmt.Printf("rows affected: %d\n", r.RowsAffected)
    fmt.Printf("return value: %d\n", r.ReturnValue)
  }

}
