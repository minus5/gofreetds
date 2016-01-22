package main

import (
	"fmt"
	"gofreetds"
	"os"
)

type Author struct {
	FirstName string
	LastName  string
	Titles    []*Title
}

type Title struct {
	TitleId string
	Title   string
	Type    string
	Price   float64
}

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
	rst, err := conn.ExecSp("author_titles", authorId)
	if err != nil {
		panic(err)
	}

	//read sp return value (just for example)
	titlesCount := rst.Status()

	//read the output prams
	var fullName string
	if err := rst.ParamScan(&fullName); err != nil {
		panic(err)
	}

	author := &Author{Titles: make([]*Title, 0)}
	//process first resultset
	if err = rst.Scan(author); err != nil {
		panic(err)
	}

	//process second resultset
	if rst.NextResult() {
		for rst.Next() {
			title := &Title{}
			if err := rst.Scan(title); err != nil {
				panic(err)
			}
			author.Titles = append(author.Titles, title)
		}
	}

	fmt.Printf("sp status: %d, sp output param: %s\n\n", titlesCount, fullName)
	fmt.Printf("%s %s\n", author.FirstName, author.LastName)
	for _, title := range author.Titles {
		fmt.Printf("%s %-30s %10.2f\n", title.TitleId, title.Title, title.Price)
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
	pool, err := freetds.NewConnPool(connStr)
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
		@au_id varchar(11),
    @full_name varchar(255) output
	)
	as

  select @full_name = au_fname + ' ' + au_lname from authors where au_id = @au_id

	select au_fname first_name, au_lname last_name from authors where au_id = @au_id

	select titles.* 
		from titles 
	inner join titleauthor on titles.title_id = titleauthor.title_id 
	where titleauthor.au_id = @au_id

  return @@rowcount
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
