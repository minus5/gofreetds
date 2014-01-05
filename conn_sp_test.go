package freetds

import (
	"github.com/stretchrcom/testify/assert"
	"testing"
	"strings"
)

func TestExecSp(t *testing.T) {
  conn := ConnectToTestDb(t)
	results, _, err := conn.execSp("sp_who")
	assert.Nil(t, err)
	assert.NotNil(t, results)
	assert.Equal(t, 1, len(results))


}

func TestExecSpReturnValue(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_return_value", " as return 123")
	assert.Nil(t, err)
	results, status, err := conn.execSp("test_return_value")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(results))
	assert.Equal(t, 123, status)
}


func TestExecSpResults(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_results", " as select 1 one; select 2 two; return 456")
	assert.Nil(t, err)
	results, status, err := conn.execSp("test_results")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, 456, status)
}

func TestExecSpInputParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_input_params", "@p1 int = 0 as select @p1 = @p1 + 123; return @p1")
	assert.Nil(t, err)
	results, status, err := conn.execSp("test_input_params", 123)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(results))
	assert.Equal(t, 246, status)
}



func createProcedure(conn *Conn, name, body string) error {
	drop := `
	if exists(select * from sys.procedures where name = 'sp_name')
    drop procedure sp_name
  `
	create := `
	create procedure sp_name 
    sp_body
  `
	drop = strings.Replace(drop, "sp_name", name, -1)
	create = strings.Replace(create, "sp_name", name, -1)
	create = strings.Replace(create, "sp_body", body, -1)
	_, err := conn.Exec(drop)
	if err != nil {
		return err
	}
	_, err = conn.Exec(create)
	return err
}
