package freetds

import (
	"github.com/stretchrcom/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestExecSp(t *testing.T) {
	conn := ConnectToTestDb(t)
	rst, err := conn.ExecSp("sp_who")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	assert.Equal(t, 1, len(rst.Results))
}

func TestExecSpReturnValue(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_return_value", " as return 123")
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_return_value")
	assert.Nil(t, err)
	assert.False(t, rst.HasResults())
	assert.Equal(t, 123, rst.Status)
}

func TestExecSpResults(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_results", " as select 1 one; select 2 two; return 456")
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_results")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(rst.Results))
	assert.Equal(t, 456, rst.Status)
}

func TestExecSpInputParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_input_params", "@p1 int = 0, @p2 int, @p3 as varchar(10), @p4 datetime, @p5 varbinary(10) = null as select @p1 = @p1 + @p2; return @p1")
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_input_params", 123, 234, "pero", time.Now(), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0})
	assert.Nil(t, err)
	assert.False(t, rst.HasResults())
	assert.Equal(t, 357, rst.Status)
}

func TestExecSpInputParamsTypes(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_input_params3", `
    @p1 int = 0, @p2 smallint, @p3 bigint, @p4 tinyint, @p5 money, @p6 real as 
    select @p1, @p2, @p3, @p4, @p5, @p6
    return 1`)
	assert.Nil(t, err)
	//all input types are int, but they are converted to apropriate sql types
	rst, err := conn.ExecSp("test_input_params3", 1, 2 ,3, 4, 5, 6)
	assert.Nil(t, err)
	assert.Equal(t, 1, rst.Status)
	var p1, p2, p3, p4, p5, p6 int
	result := rst.Results[0]
	result.Next()
	//returned as various types, and then converted to int
	result.Scan(&p1, &p2, &p3, &p4, &p5, &p6)
	assert.Equal(t, 1, p1)
	assert.Equal(t, 2, p2)
	assert.Equal(t, 3, p3)
	assert.Equal(t, 4, p4)
	assert.Equal(t, 5, p5)
	assert.Equal(t, 6, p6)
}

func TestExecSpInputParams2(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_input_params2", "@p1 nvarchar(255), @p2 varchar(255), @p3 nvarchar(255), @p4 nchar(10), @p5 varbinary(10) as select @p1, @p2, @p3, @p4, @p5;  return")
	assert.Nil(t, err)
	want := "£¢§‹›†€"
	wantp2 := "abc"
	wantp3 := "šđčćžabc"
	wantp4 := "šđčćžabcde"
	wantp5 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rst, err := conn.ExecSp("test_input_params2", want, wantp2, wantp3, wantp4, wantp5)
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	if rst == nil {
		return
	}
	assert.True(t, rst.HasResults())
	var got, gotp2, gotp3, gotp4 string
	var gotp5 []byte
	result := rst.Results[0]
	result.Next()
	result.Scan(&got, &gotp2, &gotp3, &gotp4, &gotp5)
	assert.Equal(t, want, got)
	assert.Equal(t, wantp2, gotp2)
	assert.Equal(t, wantp3, gotp3)
	assert.Equal(t, wantp4, gotp4)
	assert.Equal(t, wantp5, gotp5)
	//PrintResults(rst.Results)
}

func TestExecSpOutputParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_output_params", "@p1 int output as select @p1 = @p1 + 1")
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_output_params", 123)
	assert.Nil(t, err)
	assert.False(t, rst.HasResults())
	assert.Equal(t, 0, rst.Status)
	assert.True(t, rst.HasOutputParams())
	assert.Equal(t, len(rst.OutputParams), 1)
	assert.Equal(t, rst.OutputParams[0].Name, "@p1")
	assert.Equal(t, rst.OutputParams[0].Value, 124)
	var p1 int32
	err = rst.Scan(&p1)
	assert.Nil(t, err)
	assert.Equal(t, p1, 124)
}

func TestGetSpParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	params, err := conn.getSpParams("test_input_params")
	assert.Nil(t, err)
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

func TestHandlingNumericAndDecimalDataTypes(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_sp_result", `as
    select 1.25 f1, cast(1.26 as decimal(10,5)) f2, cast(1.27 as numeric(10,5)) f3
    return 0`)
	assert.Nil(t, err)
		rst, err := conn.ExecSp("test_sp_result")
	assert.Nil(t, err)
	assert.Equal(t, 0, rst.Status)
	assert.Equal(t, 1, len(rst.Results))
	result := rst.Results[0]
	result.Next()
	var f1, f2, f3 float64
	result.Scan(&f1, &f2, &f3)
	assert.Equal(t, 1.25, f1)
	assert.Equal(t, 1.26, f2)
	assert.Equal(t, 1.27, f3)
}
