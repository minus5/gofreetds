gofreetds
=========

##Example
Connect:
```go
  conn, err := freetds.Connect("user", "pwd", host", "database")
  if err != nil {
    fmt.Printf("error: %s\n%s\n%s", err, conn.Error, conn.Message)
    return
  }
  defer conn.Close()
```
Execute query:
```go
  rst, err := conn.Exec("select au_id, au_lname, au_fname from authors")
```
rst is array of results. Each result has Columns and Rows array. Each
row is array of values. Each column is array of ResultColumn objects.

## Tests
Tests depend on the pubs database.

Pubs sample database install script could be [downloaded](http://www.microsoft.com/en-us/download/details.aspx?id=23654).
After installing that package you will find
instpubs.sql on the disk (C:\SQL Server 2000 Sample
Databases). Execute that script to create pubs database.

Tests are using environment variables for user, pwd...:

```shell
export GOFREETDS_DB="pubs"
export GOFREETDS_USER="ianic"
export GOFREETDS_PWD="ianic"
export GOFREETDS_HOST="iow"
export GOFREETDS_MIRROR_HOST="iow-mirror"
```
