package freetds

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExecSp(t *testing.T) {
	conn := ConnectToTestDb(t)
	rst, err := conn.ExecSp("sp_who")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	assert.Equal(t, 1, len(rst.results))
}

func TestExecSpReturnValue(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_return_value", " as return 123")
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_return_value")
	assert.Nil(t, err)
	if !conn.sybaseMode125() {
		assert.False(t, rst.HasResults())
	}
	assert.Equal(t, 123, rst.Status())
}

func TestExecSpResults(t *testing.T) {
	conn := ConnectToTestDb(t)
	spBody := " as select 1 one; select 2 two; return 456"
	if conn.sybaseMode125() {
		spBody = " as select 1 one  select 2 two  return 456"
	}
	err := createProcedure(conn, "test_results", spBody)
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_results")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(rst.results))
	assert.Equal(t, 456, rst.Status())
}

func TestExecSpInputParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	spBody := "@p1 int = 0, @p2 int, @p3 as varchar(10), @p4 datetime, @p5 varbinary(10) = null as select @p1 = @p1 + @p2; return @p1"
	if conn.sybaseMode125() {
		spBody = "@p1 int = 0, @p2 int, @p3 varchar(10), @p4 datetime, @p5 varbinary(10) = null as select @p1 = @p1 + @p2  return @p1"
	}
	err := createProcedure(conn, "test_input_params", spBody)
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_input_params", 123, 234, "pero", time.Now(), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0})
	assert.Nil(t, err)
	if !conn.sybaseMode125() {
		assert.False(t, rst.HasResults())
	}
	assert.Equal(t, 357, rst.Status())
}

func TestExecSpInputParamsTypes(t *testing.T) {
	conn := ConnectToTestDb(t)

	spBody := `
    @p1 int = 0, @p2 smallint, @p3 bigint, @p4 tinyint, @p5 money, @p6 real as
    select @p1, @p2, @p3, @p4, @p5, @p6
    return 1`
	if conn.sybaseMode125() {
		spBody = `
    @p1 int = 0, @p2 smallint, @p3 int, @p4 tinyint, @p5 money, @p6 real as
    select @p1, @p2, @p3, @p4, @p5, @p6
    return 1`
	}
		err := createProcedure(conn, "test_input_params3", spBody)
	assert.Nil(t, err)
	//all input types are int, but they are converted to apropriate sql types
	rst, err := conn.ExecSp("test_input_params3", 1, 2, 3, 4, 5, 6)
	assert.Nil(t, err)
	assert.Equal(t, 1, rst.Status())
	var p1, p2, p3, p4, p5, p6 int
	result := rst.results[0]
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

func TestMoneyRead(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_input_params_money", `
       @p1 money as
       select @p1`)
	assert.Nil(t, err)

	for _, f := range []float64{2.3, 2.6390, 4.85, 2.05} {
		rst, err := conn.ExecSp("test_input_params_money", f)
		assert.Nil(t, err)
		var p1 float64
		rst.Scan(&p1)
		assert.Equal(t, f, p1)
	}

	// f64 := 2.6390
	// fmt.Println(int64(f64*10000), int64(f64*100000)/10, int64((f64+0.000000000000001)*10000))
	// f64 = 2.3
	// fmt.Println(int64(f64*10000), int64(f64*100000)/10, int64((f64+0.000000000000001)*10000))
}

func TestExecSpInputParams2(t *testing.T) {
	conn := ConnectToTestDb(t)
	spBody := `
  @p1 nvarchar(255), @p2 varchar(255), @p3 nvarchar(255), @p4 nchar(10), @p5 varbinary(10), @p6 as nvarchar(255) as
  select @p1, @p2, @p3, @p4, @p5, @p6
  if exists(select * from sys.tables where name = 'tbl_test_input_params2')
     drop table tbl_test_input_params2
  select @p1 p1, @p2 p2, @p3 p3, @p4 p4, @p5 p5, @p6 p6 into tbl_test_input_params2
  return`
  	if conn.sybaseMode125() {
  		spBody = `
  @p1 nvarchar(255), @p2 varchar(255), @p3 nvarchar(255), @p4 nchar(20), @p5 varbinary(10), @p6 nvarchar(255) as
  select @p1, @p2, @p3, @p4, @p5, @p6
  select @p1 p1, @p2 p2, @p3 p3, @p4 p4, @p5 p5, @p6 p6 into #tbl_test_input_params2
  return`
	}
	err := createProcedure(conn, "test_input_params2", spBody)
	assert.Nil(t, err)
	want := "£¢§‹›†€ ✓"
	wantp2 := "abc"
	wantp3 := "" //test empty string
	wantp4 := "šđčćžabcd✓"
	wantp5 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	wantp6 := "Mark"
	rst, err := conn.ExecSp("test_input_params2", want, wantp2, wantp3, wantp4, wantp5, wantp6)
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	if rst == nil {
		return
	}
	assert.True(t, rst.HasResults())
	var got, gotp2, gotp3, gotp4, gotp6 string
	var gotp5 []byte
	result := rst.results[0]
	result.Next()
	result.Scan(&got, &gotp2, &gotp3, &gotp4, &gotp5, &gotp6)
	assert.Equal(t, want, got)
	assert.Equal(t, wantp2, gotp2)
	assert.Equal(t, wantp3, gotp3)
	assert.Equal(t, wantp4, gotp4)
	assert.Equal(t, wantp5, gotp5)
	assert.Equal(t, wantp6, gotp6)
	//PrintResults(rst.Results)
}

func TestExecSpOutputParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_output_params", "@p1 int output as select @p1 = @p1 + 1")
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_output_params", 123)
	assert.Nil(t, err)
	if !conn.sybaseMode125() {
		assert.False(t, rst.HasResults())
	}
	assert.Equal(t, 0, rst.Status())
	assert.True(t, rst.HasOutputParams())
	assert.Equal(t, len(rst.outputParams), 1)
	assert.Equal(t, rst.outputParams[0].Name, "@p1")
	assert.EqualValues(t, rst.outputParams[0].Value, 124)
	var p1 int32
	err = rst.ParamScan(&p1)
	assert.Nil(t, err)
	assert.EqualValues(t, p1, 124)
}

func TestGetSpParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	params, err := conn.getSpParams("test_input_params")
	assert.Nil(t, err)
	p := params[0]
	assert.Equal(t, p.Name, "@p1")
	assert.EqualValues(t, p.UserTypeId, SYBINT4)
	assert.Equal(t, p.IsOutput, false)
	assert.EqualValues(t, p.MaxLength, 4)
	assert.Equal(t, int(p.Scale), 0x0)
	if !conn.sybaseMode125() {
		assert.EqualValues(t, p.ParameterId, 1)
		assert.Equal(t, int(p.Precision), 0xa)
	}
}

func TestGetSpParamsSqlMsSql(t *testing.T) {
	testSpName := "test_get_sp_params_sql"
	conn := &Conn{}
	expectedSql := fmt.Sprintf(msSqlGetSpParamsSql, testSpName)
	actualSql := conn.getSpParamsSql(testSpName)
	assert.Equal(t, expectedSql, actualSql)
}

func TestGetSpParamsSqlSybase(t *testing.T) {
	testSpName := "test_get_sp_params_sql"
	creds := NewCredentials("Compatibility Mode=Sybase")
	conn := &Conn{
		credentials: *creds,
	}
	expectedSql := fmt.Sprintf(sybaseAseGetSpParamsSql, testSpName)
	actualSql := conn.getSpParamsSql(testSpName)
	assert.Equal(t, expectedSql, actualSql)
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
	if conn.sybaseMode125() {
		drop = `
	if exists(select id from sysobjects where name = 'sp_name'  and type = 'P')
    drop procedure sp_name
  `
		create = `
	create procedure sp_name
    sp_body
  `
	}

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
	spBody := `as
    select 1.25 as f1, cast(1.26 as decimal(10,5)) as f2, cast(1.27 as numeric(10,5)) as f3
    return 0`
    if conn.sybaseMode125() {
    	spBody = `as
    declare @f1 float
    declare @f2 decimal(10,5)
    declare @f3 numeric(10,5)
    select @f1=1.25, @f2=1.26, @f3=1.27
    select @f1 as f1, @f2 as f2, @f3 as f3
    return 0
`
	}
	err := createProcedure(conn, "test_sp_result", spBody)
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_sp_result")
	assert.Nil(t, err)
	assert.Equal(t, 0, rst.Status())
	assert.Equal(t, 1, len(rst.results))
	result := rst.results[0]
	result.Next()
	var f1, f2, f3 float64
	result.Scan(&f1, &f2, &f3)
	assert.Equal(t, 1.25, f1)
	assert.Equal(t, 1.26, f2)
	assert.Equal(t, 1.27, f3)
}

func TestBugFixEmptyStringInSpParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	spBody := `@p1 varchar(255) as
    select '_' + @p1 + '_', len(@p1)
    return 0`
    if conn.sybaseMode125() {
    	spBody = `@p1 varchar(255) as
    select '_' + @p1 + '_', len(ltrim(rtrim(@p1)))
    return 0`
	}
	err := createProcedure(conn, "test_sp_bug_fix_1", spBody)
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_sp_bug_fix_1", "")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	var s string
	var l int
	rst.Scan(&s, &l)
	//we are treating empty strings as single space
	assert.Equal(t, "_ _", s)
	assert.Equal(t, 0, l)
}

func TestBugGuidInSpParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	spBody := `@p1 uniqueidentifier as
    select cast(@p1 as varchar(255)), @p1
    return 0`
    if conn.sybaseMode125() {
		spBody = `@p1 char(36) as
    select cast(@p1 as varchar(255)), @p1
    return 0`
	}
	err := createProcedure(conn, "test_sp_bug_fix_2", spBody)
	assert.Nil(t, err)
	var in, out, out2 string
	in = "B5A0E32D-3F48-4CC2-A44B-74753D9CACF8"
	rst, err := conn.ExecSp("test_sp_bug_fix_2", in)
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	rst.Scan(&out, &out2)
	assert.Equal(t, in, out)
	assert.Equal(t, in, out2)
}

/*
//ova petlja je ponekad pucala sa:
// SIGSEGV: segmentation violation
// signal arrived during cgo execution
//nisam uspio dotjearti zasto je to
//kada bi stavio onaj select vise ne bi pucalo
//kada bi stavio GC takodjer ne
//a i kada puca to je stohasticki

//SOLVED - nakon sto sam dodao refHolder u ExecSp vise ne puca

func TestBugFixSegmentationFault(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_sp_bug_fix_2", `@p1 int,
     --@p2 varchar(255),
     @p3 varchar(255),
     @p4 varchar(255),
     @p5 money as
        --select @p2
        return 1`)
	assert.Nil(t, err)
	// s := "1"
	s2 := "pero zdero"
	s3 := "description"
	i := 123
	f := 12.34
	for {
		rst, err := conn.ExecSp("test_sp_bug_fix_2", i, s2, s3, f)
		assert.Nil(t, err)
		assert.NotNil(t, rst)
		if err != nil {
		 	break
		}
	}
}
*/

func TestStoredProcedureNotExists(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_sp_not_exists", `as return`)
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_sp_not_exists")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	_, err = conn.Exec("drop procedure test_sp_not_exists")
	assert.Nil(t, err)
	rst, err = conn.ExecSp("test_sp_not_exists")
	assert.NotNil(t, err)
	assert.Nil(t, rst)
}

func TestTimeSpParams(t *testing.T) {
	conn := ConnectToTestDb(t)
	spBody := `@p1 datetime as
    insert into tm (tm) values(@p1)
    select tm, 123 i from tm where id = scope_identity()
    return 0`
    if conn.sybaseMode125() {
    	spBody = `@p1 datetime as
    insert into tm (tm) values(@p1)
    select tm, 123 i from tm where id = @@IDENTITY
    return 0`
	}
	err := createProcedure(conn, "test_sp_time_sp_params", spBody)
	assert.Nil(t, err)

	f := func(tmIn time.Time) {
		var tmOut time.Time
		var i int
		rst, err := conn.ExecSp("test_sp_time_sp_params", tmIn)
		assert.Nil(t, err)
		assert.NotNil(t, rst)
		rst.Next()
		err = rst.Scan(&tmOut, &i)
		assert.Nil(t, err)
		assert.WithinDuration(t, tmIn, tmOut, 4*time.Millisecond)
		//fmt.Println("time", tmIn, tmOut)
	}

	f(time.Date(2014, 7, 8, 23, 59, 59, 0, time.Local))
	f(time.Unix(1404856799, 0))
	f(time.Unix(1404856800, 0))
	f(time.Unix(1404856801, 0))

	f(time.Unix(1404856799, 0).UTC())
	f(time.Unix(1404856800, 0).UTC())
	f(time.Unix(1404856801, 0).UTC())
	f(time.Now())

	// European sumer time begining
	f(time.Date(2018, 3, 25, 0, 1, 1, 0, time.Local))
	f(time.Date(2018, 3, 25, 1, 1, 1, 0, time.Local))
	f(time.Date(2018, 3, 25, 2, 1, 1, 0, time.Local))
	f(time.Date(2018, 3, 25, 3, 1, 1, 0, time.Local))
	// Euorpena sumer time end
	f(time.Date(2018, 10, 28, 0, 1, 1, 0, time.Local))
	f(time.Date(2018, 10, 28, 1, 1, 1, 0, time.Local))
	f(time.Date(2018, 10, 28, 2, 1, 1, 0, time.Local))
	f(time.Date(2018, 10, 28, 3, 1, 1, 0, time.Local))

	var zt time.Time
	f(zt)
	now := time.Now()
	data := &struct {
		Tm time.Time
		I  int
	}{}
	rst, err := conn.ExecSp("test_sp_time_sp_params", now)
	rst.Scan(data)
	assert.WithinDuration(t, now, data.Tm, 4*time.Millisecond)
}

func TestNewDateTypesParam(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn.sybaseMode125() {
		t.Skip("datettimeoffset does not exist in Sybase 12.5")
	}
	err := createProcedure(conn, "test_sp_with_datetimeoffset_param", `
    (@p1 datetimeoffset, @p2 date, @p3 time, @p4 datetime2) as
    DECLARE @datetime datetime = @p1;
    SELECT @datetime, @p1, @p2, @p3, @p4
    return `)
	assert.Nil(t, err)
	p1 := "2025-12-10 12:32:10.1237000 +01:00"
	p2 := "2025-12-10"
	p3 := "12:30"
	p4 := "2025-12-10 12:32:10"
	rst, err := conn.ExecSp("test_sp_with_datetimeoffset_param", p1, p2, p3, p4)
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	rst.Next()
	var op1, op2, op3, op4 string
	var dt time.Time
	err = rst.Scan(&dt, &op1, &op2, &op3, &op4)
	assert.Nil(t, err)
	assert.Equal(t, "Wed Dec 10 12:32:10 2025", dt.Format(time.ANSIC)) //ANSIC format makes test independent of the time zone in which the test is run
	assert.Equal(t, "2025-12-10 12:32:10.1237000 +01:00", op1)
	assert.Equal(t, "2025-12-10", op2)
	assert.Equal(t, "12:30:00.0000000", op3)
	assert.Equal(t, "2025-12-10 12:32:10.0000000", op4)
}

func TestExecSpWithVarcharMax(t *testing.T) {
	conn := ConnectToTestDb(t)
	if conn.sybaseMode125() {
		t.Skip("max is not a valid aregument in Sybase 12.5 for varchar(max)")
	}
	err := createProcedure(conn, "test_sp_varchar_max", `
    (@p1 varchar(max) output) as
    select @p1`)
	assert.Nil(t, err)
	str := longString(8000)
	rst, err := conn.ExecSp("test_sp_varchar_max", str)
	assert.Nil(t, err)
	var str2 string
	result := rst.results[0]
	result.Next()
	err = result.Scan(&str2)
	assert.Nil(t, err)
	assert.Equal(t, str, str2)

	var str3 string
	err = rst.ParamScan(&str3)
	assert.Nil(t, err)
	assert.Equal(t, str, str3)
}

func TestNullValueScan(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_sp_null_value_scan", `
    as
    DECLARE @p1 varchar(255)
    DECLARE @p2 varchar(255)
    set @p2 = 'p2'
    SELECT @p1, @p2
    `)
	assert.Nil(t, err)
	rst, err := conn.ExecSp("test_sp_null_value_scan")
	assert.Nil(t, err)
	assert.NotNil(t, rst)
	rst.Next()
	var p1, p2 *string
	err = rst.Scan(&p1, &p2)
	assert.Nil(t, err)
	assert.Nil(t, p1)
	assert.NotNil(t, p2)
	assert.Equal(t, "p2", *p2)
}

func TestBugFixVarchar(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_bug_fix_varchar", ` 
  @p1 nvarchar(400)
  as
  select @p1
  return`)
	assert.Nil(t, err)

	str := `{\"id\": \"ppf\", \"d\": \"FC Utrecht (ml) - Brabant United (ml) - zbroj golova 1.pol. (4,5) - vi\u0161e - 3,60, Achilles 29 (ml) - AZ Alkmaar (ml) - zbroj golova 1.pol. (0,5) - vi\u0161e - 1,40, VV/Helmond Sport (ml) - Roda JC (ml) - zbroj golova 1.pol. (1,5) - vi\u0161e - 1,65\"}`
	_, err = conn.ExecSp("test_bug_fix_varchar", str)
	assert.Nil(t, err)
}

func TestExecSpBadParameterDataType(t *testing.T) {
	conn := ConnectToTestDb(t)
	err := createProcedure(conn, "test_bad_parameter_data_type", ` 
	@p1 varchar(20)
	as
	select @p1
	return`)
	assert.Nil(t, err)

	spBody := " as select 1 one; select 2 two; return 456"
	if conn.sybaseMode125() {
		spBody = " as select 1 one select 2 two return 456"
	}
	err = createProcedure(conn, "test_bad_parameter_data_type2", spBody)
	assert.Nil(t, err)

	var intval int16
	intval = 1
	_, err = conn.ExecSp("test_bad_parameter_data_type", intval)
	expectedError := "Could not convert int16 to string."
	assert.Equal(t, expectedError, err.Error())

	_, err = conn.ExecSp("test_bad_parameter_data_type", "test")
	assert.Nil(t, err)

	_, err = conn.ExecSp("test_bad_parameter_data_type2")
	assert.Nil(t, err)

}
