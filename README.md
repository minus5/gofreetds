## gofreetds

Go [FreeTDS](http://www.freetds.org/) wrapper. Native Sql Server database driver.

Features:

  * can be used as [database/sql](http://golang.org/pkg/database/sql/) driver
  * handles calling [stored procedures](#stored-procedures)
  * handles multiple resultsets
  * supports database mirroring
  * connection pooling
  * scaning resultsets into structs

## Get started

### Install dependencines

[FreeTDS](http://www.freetds.org/) libraries must be installed on the system.

Mac
```shell
brew install freetds
```
Ubuntu, Debian...
```shell
sudo apt-get install freetds-dev
```

### Go get

```
go get github.com/minus5/gofreetds
```

### Docs

  http://godoc.org/github.com/minus5/gofreetds


## Using as database/sql driver

Name of the driver is mssql.
```go
db, err := sql.Open("mssql", connStr)
...
row := db.QueryRow("SELECT au_fname, au_lname name FROM authors WHERE au_id = ?", "172-32-1176")
..
var firstName, lastName string
err = row.Scan(&firstName, &lastName)
```
Full example in example/mssql.

## Stored Procedures

What I'm missing in database/sql is calling stored procedures, handling return values and output params. And especially handling multiple result sets.
Which is all supported by FreeTDS and of course by gofreetds.

Connect:
```go
pool, err := freetds.NewConnPool("user=ianic;pwd=ianic;database=pubs;host=iow")
defer pool.Close()
...
//get connection
conn, err := pool.Get()
defer conn.Close()
```
Execute stored procedure:
```go
rst, err := conn.ExecSp("sp_help", "authors")  
```
Read sp return value, and output params:
```go
returnValue := rst.Status()
var param1, param2 int
rst.ParamScan(&param1, &param2)
```
Read sp resultset (fill the struct):
```go
author := &Author{}
rst.Scan(author)
```
Read next resultset:
```go
if rst.NextResult() {
    for rst.Next() {
        var v1, v2 string
        rst.Scan(&v1, &v2)
    }
}
```
Full example in example/stored_procedure

## Other usage

Executing arbitrary sql is supported with Exec or ExecuteSql.

Execute query:
```go
rst, err := conn.Exec("select au_id, au_lname, au_fname from authors")
```
Rst is array of results.
Each result has Columns and Rows array.
Each row is array of values. Each column is array of ResultColumn objects.

Full example in example/exec.

Execute query with params:
```go
rst, err := conn.ExecuteSql("select au_id, au_lname, au_fname from authors where au_id = ?", "998-72-3567")
```

## Sybase Compatibility Mode

Gofreetds now supports Sybase ASE 16.0 through the driver. In order to support this, this post is very helpful: [Connect to MS SQL Server and Sybase ASE from Mac OS X and Linux with unixODBC and FreeTDS (from Internet Archive)](http://web.archive.org/web/20160325095720/http://2tbsp.com/articles/2012/06/08/connect-ms-sql-server-and-sybase-ase-mac-os-x-and-linux-unixodbc-and-freetds)

To use a Sybase ASE server with Gofreetds, you simply need to set a compatibility mode on your connection string after you've configured your .odbc.ini file and .freetds.conf file.

This mode uses TDS Version 5.

### Connection String Parameter

You can set your connection string up for Sybase by using the 'compatibility_mode' Parameter. The parameter can be named 'compatibility', 'compatibility mode', 'compatibility_mode' or 'Compatibility Mode'. Currently this mode only supports Sybase. To specify you can use 'sybase' or 'Sybase'.

```
Server=myServerAddress;Database=myDatabase;User Id=myUsername;Password=myPassword;Failover Partner=myMirror;Max Pool Size=200;Compatibility Mode=Sybase
```


## Testing

Tests depend on the pubs database.

Pubs sample database install script could be [downloaded](http://www.microsoft.com/en-us/download/details.aspx?id=23654).
After installing that package you will find
instpubs.sql on the disk (C:\SQL Server 2000 Sample
Databases). Execute that script to create pubs database.

Tests and examples are using environment variable GOFREETDS_CONN_STR to connect to the pubs database.

```shell
export GOFREETDS_CONN_STR="user=ianic;pwd=ianic;database=pubs;host=iow"
export GOFREETDS_MIRROR_HOST="iow-mirror"
```
If you don't want to setup and test database mirroring than don't define GOFREETDS_MIRROR_HOST. Mirroring tests will be skipped.
