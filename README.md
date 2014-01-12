##About

Go FreeTDS wrapper. Native Sql Server database driver.

##Dependencies
[FreeTDS](http://www.freetds.org/) libraries must be installed on the system.

Mac
```shell
  brew install freetds
```
Ubuntu, Debian...
```shell
  sudo apt-get install freetds
```

##Usage
Connect:
```go
  //create connection pool (for max. 100 connections)
  pool, err := freetds.NewConnPool("user=ianic;pwd=ianic;database=pubs;host=iow", 100)
  defer pool.Close()
  ...
  //get connection
  conn, err := pool.Get()
  defer conn.Close()
```
Execute query:
```go
  rst, err := conn.Exec("select au_id, au_lname, au_fname from authors")
```
rst is array of results.
Each result has Columns and Rows array.
Each row is array of values. Each column is array of ResultColumn objects.

Execute stored procedure:
```go
  spRst, err := conn.ExecSp("sp_help", "authors")
```

Execute query with params:
```go
  rst, err := conn.ExecuteSql("select au_id, au_lname, au_fname from authors where au_id = ?", "998-72-3567")
```

## Tests
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
