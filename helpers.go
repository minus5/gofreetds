package freetds

import (
	"fmt"
	"strings"
)

func parseConnectionString(connStr string) *credentials {
	parts := strings.Split(connStr, ";")
	crd := &credentials{}
	for _, part := range parts {
		kv := strings.Split(part, "=")
		if len(kv) == 2 {
			key := strings.ToLower(strings.Trim(kv[0], " "))
			value := kv[1]
			switch key {
			case "server", "host":
				crd.host = value
			case "database":
				crd.database = value
			case "user id", "user_id", "user":
				crd.user = value
			case "password", "pwd":
				crd.pwd = value
			case "failover partner", "failover_partner", "mirror", "mirror_host", "mirror host":
				crd.mirrorHost = value
			}
		}
	}
	return crd
}

//usefull for testing n
func printResults(results []*Result) {
	fmt.Printf("results %v", results)
	for _, r := range results {
		if r.Rows != nil {
			fmt.Printf("\n\nColums:\n")
			for j, c := range r.Columns {
				fmt.Printf("\t%3d%20s%10d%10d\n", j, c.Name, c.DbType, c.DbSize)
			}
			for i, _ := range r.Rows {
				for j, _ := range r.Columns {
					fmt.Printf("value[%2d, %2d]: %v\n", i, j, r.Rows[i][j])
				}
				fmt.Printf("\n")
			}
		}
		fmt.Printf("rows affected: %d\n", r.RowsAffected)
		fmt.Printf("return value: %d\n", r.ReturnValue)
	}
}
