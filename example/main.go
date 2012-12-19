package main

import ("gofreetds";"fmt")

func main() {
  conn, err := freetds.Connect("ianic", "ianic", "iow", "pubs")
  if err != nil {
    fmt.Printf("error: %s\n%s\n%s", err, conn.Error, conn.Message)
    return
  }
  defer conn.Close()

  rst, err := conn.Exec("select au_id, au_lname, au_fname from authors")
  if err != nil {
    fmt.Printf("error %s", err)
    return
  }
  for _,c := range rst[0].Columns {
    fmt.Printf("%-20s", c.Name)
  }
  fmt.Printf("\n")
  for _,r := range rst[0].Rows {
    for _,v := range r {
      fmt.Printf("%-20v", v)
    }
    fmt.Printf("\n")
  }

}