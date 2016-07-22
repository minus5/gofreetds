package main

import (
	"fmt"
	"gofreetds"
	"os"
)

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
	rst, err := conn.Exec("select au_id, au_lname, au_fname from authors")
	if err != nil {
		panic(err)
	}
	printResult(rst[0])

	auId := "111-11-1111"

	//insert row
	rst, err = conn.ExecuteSql("insert into authors (au_id, au_lname, au_fname, contract) values (?, ?, ?, ?)",
		auId, "Pero", "Zdero", 1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("inserted %d row(s)\n", rst[0].RowsAffected)

	//delete row
	rst, err = conn.ExecuteSql("delete from authors where au_id = ?", auId)
	if err != nil {
		panic(err)
	}
	fmt.Printf("deleted %d row(s)\n", rst[0].RowsAffected)
}

func printResult(rst *freetds.Result) {
	//print query result, columns
	for _, c := range rst.Columns {
		fmt.Printf("%-20s", c.Name)
	}
	fmt.Printf("\n")
	//rows
	for _, row := range rst.Rows {
		for _, value := range row {
			fmt.Printf("%-20v", value)
		}
		fmt.Printf("\n")
	}
}
