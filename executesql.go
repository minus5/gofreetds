package freetds

import (
	"database/sql/driver"
	"fmt"
	"log"
	"strings"
	"time"
)

const statusRow string = `;
   select cast(coalesce(scope_identity(), -1) as bigint) last_insert_id, 
          cast(@@rowcount as bigint) rows_affected
`

//Execute sql query with arguments.
//? in query are arguments placeholders.
//  ExecuteSql("select * from authors where au_fname = ?", "John")
func (conn *Conn) ExecuteSql(query string, params ...driver.Value) ([]*Result, error) {
	statement, numParams := query2Statement(query)
	if numParams != len(params) {
		return nil, fmt.Errorf("Incorrect number of params, expecting %d got %d", numParams, len(params))
	}
	paramDef, paramVal := parseParams(params...)
	statement += statusRow
	sql := fmt.Sprintf("exec sp_executesql N'%s', N'%s', %s", statement, paramDef, paramVal)
	if numParams == 0 {
		sql = fmt.Sprintf("exec sp_executesql N'%s'", statement)
	}
	return conn.Exec(sql)
}

//converts query to SqlServer statement for sp_executesql
//replaces ? in query with params @p1, @p2, ...
//returns statement and number of params
func query2Statement(query string) (string, int) {
	parts := strings.Split(query, "?")
	var statement string
	numParams := len(parts) - 1
	statement = parts[0]
	for i, part := range parts {
		if i > 0 {
			statement = fmt.Sprintf("%s@p%d%s", statement, i, part)
		}
	}
	return quote(statement), numParams
}

func parseParams(params ...driver.Value) (paramDef, paramVal string) {
	paramDef = ""
	paramVal = ""
	for i, param := range params {
		if i > 0 {
			paramVal += ", "
			paramDef += ", "
		}
		sqlType, sqlValue := go2SqlDataType(param)
		paramName := fmt.Sprintf("@p%d", i+1)
		paramDef += fmt.Sprintf("%s %s", paramName, sqlType)
		paramVal += fmt.Sprintf("%s=%s", paramName, sqlValue)
	}
	return
}

func quote(in string) string {
	return strings.Replace(in, "'", "''", -1)
}

func go2SqlDataType(value interface{}) (string, string) {
	max := func(a int, b int) int {
		if a > b {
			return a
		}
		return b
	}

	strValue := fmt.Sprintf("%v", value)
	switch t := value.(type) {
	case bool:
		bitStrValue := "0"
		if strValue == "true" {
			bitStrValue = "1"
		}
		return "bit", bitStrValue
	case uint8, int8:
		return "tinyint", strValue
	case uint16, int16:
		return "smallint", strValue
	case uint32, int32, int:
		return "int", strValue
	case uint64, int64:
		return "bigint", strValue
	case float32, float64:
		return "real", strValue
	case string:
		{
		}
	case time.Time:
		{
			strValue = t.Format(time.RFC3339Nano)
			return "datetimeoffset", fmt.Sprintf("'%s'", quote(strValue))
		}
	case []byte:
		{
			b, _ := value.([]byte)
			return fmt.Sprintf("varbinary (%d)", max(1, len(b))),
				fmt.Sprintf("0x%x", b)
		}
	default:
		log.Printf("unknown dataType %T", t)
	}
	return fmt.Sprintf("nvarchar (%d)", max(1, len(strValue))),
		fmt.Sprintf("'%s'", quote(strValue))

}
