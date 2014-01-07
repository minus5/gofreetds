package main

import ("gofreetds";"fmt")

func main() {
  //conect
  //conn, err := freetds.Connect("ianic", "ianic", "iow", "pubs")
	conn, err := freetds.ConnectWithConnectionString("user=ianic;pwd=ianic;database=pubs;host=iow")
  if err != nil {
    fmt.Printf("error: %s", err)
    return
  }
  defer conn.Close()

  //execute query
  rst, err := conn.Exec("select au_id, au_lname, au_fname from authors")
  if err != nil {
    fmt.Printf("error %s", err)
    return
  }
	PrintResults(rst)

	spRst, err := conn.ExecSp("sp_help", "authors")
	if err != nil {
		fmt.Printf("error %s", err)
		return
	}
	PrintResults(spRst.Results)
}

func PrintResults(rsts []*freetds.Result) {
	for _, rst := range rsts {
		PrintResult(rst)
	}
}

func PrintResult(rst *freetds.Result) {
	//print query result, columns
	for _,c := range rst.Columns {
		fmt.Printf("%-20s", c.Name)
	}
	fmt.Printf("\n")
	//rows
	for _,r := range rst.Rows {
		for _,v := range r {
			fmt.Printf("%-20v", v)
		}
		fmt.Printf("\n")
	}
}
