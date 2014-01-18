package main

import (
	"fmt"
	"gofreetds"
	"os"
)

func main() {
	pool := connect()
	defer pool.Close()
	prepareDb(pool)
	//execute stored procedure
	authorId := "998-72-3567"
	conn, err := pool.Get()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	spRst, err := conn.ExecSp("author_titles", authorId)
	if err != nil {
		panic(err)
	}
	rstAuthor := spRst.Results[0]
	rstTitles := spRst.Results[1]
	//process first resultset
	for rstAuthor.Next() {
		var auId, auLname, auFname string
		if err = rstAuthor.Scan(&auId, &auLname, &auFname); err != nil {
			panic(err)
		}
		fmt.Printf("%s %s\n", auFname, auLname)
	}
	//process second resultset
	for rstTitles.Next() {
		var titleId, title, titleType string
		if err := rstTitles.Scan(&titleId, &title, &titleType); err != nil {
			panic(err)
		}
		fmt.Printf("%s %-30s %s\n", titleId, title, titleType)
	}
}

func printResult(rst *freetds.Result) {
	//print query result, columns
	for _, c := range rst.Columns {
		fmt.Printf("%-20s", c.Name)
	}
	fmt.Printf("\n")
	//rows
	for _, r := range rst.Rows {
		for _, v := range r {
			fmt.Printf("%-20v", v)
		}
		fmt.Printf("\n")
	}
}

func connect() *freetds.ConnPool {
	connStr := os.Getenv("GOFREETDS_CONN_STR")
	if connStr == "" {
		panic("Set connection string for the pubs database in GOFREETDS_CONN_STR environment variable!\n")
	}
	pool, err := freetds.NewConnPool(connStr, 1)
	if err != nil {
		panic(err)
	}
	return pool
}

var prepareDbScripts = [...]string{`
if exists(select * from sys.procedures where name = 'author_titles')
  drop procedure author_titles
`, `	
create procedure author_titles (
		@au_id varchar(11)
	)
	as

	select * from authors where au_id = @au_id

	select titles.* 
		from titles 
	inner join titleauthor on titles.title_id = titleauthor.title_id 
	where titleauthor.au_id = @au_id
`}

func prepareDb(pool *freetds.ConnPool) {
	conn, err := pool.Get()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	for _, s := range prepareDbScripts {
		_, err := conn.Exec(s)
		if err != nil {
			panic(err)
		}
	}
}
