package freetds

import (
	"github.com/stretchrcom/testify/assert"
	"testing"
	"time"
)

func TestGoTo2SqlDataType2(t *testing.T) {
	var checker = func(value interface{}, sqlType string, sqlFormatedValue string) {
		actualSqlType, actualSqlFormatedValue := go2SqlDataType(value)
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
	checker(tm, "nvarchar (25)", "'2006-01-02T23:04:05+01:00'")

	checker([]byte{1,2,3,4,5,6,7,8}, "varbinary (8)", "0x0102030405060708")
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
		actualSqlType, actualSqlFormatedValue := go2SqlDataType(value)
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
	checker(tm, "nvarchar (25)", "'2006-01-02T23:04:05+01:00'")

	checker([]byte{1,2,3,4,5,6,7,8}, "varbinary (8)", "0x0102030405060708")
}
