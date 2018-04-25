package freetds

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	sqlDateTimeOffSet = "2006-01-02T15:04:05-07:00"
)

func TestGoTo2SqlDataType2(t *testing.T) {
	var checker = func(value interface{}, sqlType string, sqlFormatedValue string) {
		actualSqlType, actualSqlFormatedValue, err := go2SqlDataType(value)
		assert.Nil(t, err)
		assert.Equal(t, actualSqlType, sqlType)
		assert.Equal(t, actualSqlFormatedValue, sqlFormatedValue)
	}

	checker(123, "int", "123")
	checker(int64(123), "bigint", "123")
	checker(int16(123), "smallint", "123")
	checker(int8(123), "tinyint", "123")
	checker(123.23, "real", "123.23")
	checker(float64(123.23), "real", "123.23")

	checker("iso medo", "nvarchar (8)", "'iso medo'")
	checker("iso medo isn't", "nvarchar (14)", "'iso medo isn''t'")

	tm := time.Unix(1136239445, 0)
	paris, _ := time.LoadLocation("Europe/Paris")

	checker(tm.In(paris), "datetimeoffset", "'"+tm.In(paris).Format(sqlDateTimeOffSet)+"'")

	checker([]byte{1, 2, 3, 4, 5, 6, 7, 8}, "varbinary (8)", "0x0102030405060708")

	//go2SqlDataType(t)
}

func TestQuery2Statement(t *testing.T) {
	s, p := query2Statement("select 1 from foo where 1 = ?")
	assert.Equal(t, 1, p)
	assert.Equal(t, s, "select 1 from foo where 1 = @p1")

	s, p = query2Statement("select 1")
	assert.Equal(t, p, 0)
	assert.Equal(t, s, "select 1")

	s, p = query2Statement("select 1 where 2 = ?")
	assert.Equal(t, p, 1)
	assert.Equal(t, s, "select 1 where 2 = @p1")

	s, p = query2Statement("select 1 where 2 = ? and 3 = ?")
	assert.Equal(t, p, 2)
	assert.Equal(t, s, "select 1 where 2 = @p1 and 3 = @p2")
}

func TestGoTo2SqlDataType(t *testing.T) {
	var checker = func(value interface{}, sqlType string, sqlFormatedValue string) {
		actualSqlType, actualSqlFormatedValue, err := go2SqlDataType(value)
		assert.Nil(t, err)
		assert.Equal(t, actualSqlType, sqlType)
		assert.Equal(t, actualSqlFormatedValue, sqlFormatedValue)
	}

	checker(123, "int", "123")
	checker(int64(123), "bigint", "123")
	checker(int8(123), "tinyint", "123")
	checker(123.23, "real", "123.23")
	checker(float64(123.23), "real", "123.23")

	checker("iso medo", "nvarchar (8)", "'iso medo'")
	checker("iso medo isn't", "nvarchar (14)", "'iso medo isn''t'")

	tm := time.Unix(1136239445, 0)
	paris, _ := time.LoadLocation("Europe/Paris")

	checker(tm.In(paris), "datetimeoffset", "'"+tm.In(paris).Format(sqlDateTimeOffSet)+"'")

	checker([]byte{1, 2, 3, 4, 5, 6, 7, 8}, "varbinary (8)", "0x0102030405060708")

	checker("", "nvarchar (1)", "''")
	checker(true, "bit", "1")
	checker(false, "bit", "0")
}

func TestExecuteSqlNumberOfParams(t *testing.T) {
	c := &Conn{}
	_, err := c.ExecuteSql("select 1 from foo where 1 = ? and 2 = ? and 3 = ?", 1, 2)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Incorrect number of params")
}

func TestParseParams(t *testing.T) {
	def, val, err := parseParams(1, 2, "pero")
	assert.Nil(t, err)
	assert.Equal(t, def, "@p1 int, @p2 int, @p3 nvarchar (4)")
	assert.Equal(t, val, "@p1=1, @p2=2, @p3='pero'")
}

func TestExecuteSqlDatetime(t *testing.T) {
	c := ConnectToTestDb(t)
	var err error
	sql := "select top 1 datetime from dbo.freetds_types where datetime < ?"
	if !c.sybaseMode125() {
		_, err = c.ExecuteSql(sql, time.Now())
	} else {
		sql = "select top 1 datetime from freetds_types where datetime < ?"
		_, err = c.executeSqlSybase125(sql, time.Now().Format("2006-01-02 15:04:05"))
	}
	assert.Nil(t, err)
}
