package main

import (
	"encoding/json"
	"fmt"
	"gofreetds"
	"os"
	"time"
)

type Table struct {
	Name            string
	Owner           string
	Type            string
	CreatedDatetime time.Time
	Columns         []Column
}

type Column struct {
	ColumnName           string
	Type                 string
	Computed             string
	Length               int
	Prec                 string
	Scale                string
	Nullable             string
	TrimTrailingBlanks   string
	FixedLenNullInSource string
	Collation            *string //pointer, because this is nullable column
}

func main() {
	//create connection pool
	connStr := os.Getenv("GOFREETDS_CONN_STR")
	if connStr == "" {
		panic("Set connection string for the pubs database in GOFREETDS_CONN_STR environment variable!\n")
	}
	pool, err := freetds.NewConnPool(connStr)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	//get connection
	conn, err := pool.Get()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	//execute query
	rst, err := conn.ExecSp("sp_help", "authors")
	if err != nil {
		panic(err)
	}
	var tbl Table
	if rst.Next() {
		if err := rst.Scan(&tbl); err != nil {
			panic(err)
		}
	}
	rst.NextResult()
	for rst.Next() {
		var col Column
		err := rst.Scan(&col)
		if err != nil {
			panic(err)
		}
		tbl.Columns = append(tbl.Columns, col)
	}

	// pretty print
	buf, _ := json.MarshalIndent(tbl, "  ", "  ")
	fmt.Printf("%s\n", buf)
}
