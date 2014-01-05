package freetds

import (
	"github.com/stretchrcom/testify/assert"
	"testing"
	"strings"
	"time"
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
	err := createProcedure(conn, "test_input_params", "@p1 int = 0, @p2 int, @p3 as varchar(10), @p4 datetime, @p5 varbinary(10) = null as select @p1 = @p1 + @p2; return @p1")
	assert.Nil(t, err)
	results, status, err := conn.execSp("test_input_params", 123, 234, "pero", time.Now(), []byte{1,2,3,4,5,6,7,8,9,0}) 
	assert.Nil(t, err)
	assert.Equal(t, 0, len(results))
	assert.Equal(t, 357, status)
}

func TestGetSpParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	params, err := conn.getSpParams("test_input_params")
	assert.Nil(t, err)
	//assert.Equal(t, len(params), 2)
	p := params[0]
	assert.Equal(t, p.Name, "@p1")
	assert.Equal(t, p.ParameterId, 1)
	assert.Equal(t, p.UserTypeId, SYBINT4) 
	assert.Equal(t, p.IsOutput, false)
	assert.Equal(t, p.MaxLength, 4)
	assert.Equal(t, int(p.Precision), 0xa)
	assert.Equal(t, int(p.Scale), 0x0) 
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
