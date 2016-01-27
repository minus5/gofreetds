package main

import (
	"fmt"
	"gofreetds"
	"os"
)

type author struct {
	Id        string
	LastName  string
	FirstName string
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
	rst, err := conn.Exec("select au_id Id, au_lname LastName, au_fname FirstName from authors")
	if err != nil {
		panic(err)
	}
	result := rst[0]
	for result.Next() {
		var a author
		//scan into struct, expected 3 values to be scaned
		err := result.MustScan(3, &a)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%-15s%-20s%-20s\n", a.Id, a.FirstName, a.LastName)
	}
}
