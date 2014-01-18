package freetds

import (
	"fmt"
	"github.com/stretchrcom/testify/assert"
	"testing"
	"time"
)

func TestInt(t *testing.T) {
	testToSqlToType(t, SYBINT4, 2147483647)
	testToSqlToType(t, SYBINT4, -2147483648)

	testToSqlToType(t, SYBINT4, int(2147483647))
	testToSqlToType(t, SYBINT4, int32(2147483647))
	testToSqlToType(t, SYBINT4, int64(2147483647))

	_, err := typeToSqlBuf(SYBINT4, "pero")
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "Could not convert string to int32.")
}

func TestInt16(t *testing.T) {
	testToSqlToType(t, SYBINT2, int16(32767))
	testToSqlToType(t, SYBINT2, int16(-32768))
	_, err := typeToSqlBuf(SYBINT2, 123)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "Could not convert int to int16.")

	_, err = typeToSqlBuf(SYBINT2, int64(1))
	assert.NotNil(t, err)
}

func TestInt8(t *testing.T) {
	testToSqlToType(t, SYBINT1, uint8(127))
	testToSqlToType(t, SYBINT1, uint8(255))
}

func TestInt64(t *testing.T) {
	testToSqlToType(t, SYBINT8, int64(-9223372036854775808))
	testToSqlToType(t, SYBINT8, int64(9223372036854775807))
}

// func TestString(t *testing.T) {
// 	testToSqlToType(t, SYBNVARCHAR, "pero")
// 	testToSqlToType(t, SYBNVARCHAR, "pero ždero")
// 	testToSqlToType(t, SYBNVARCHAR, "šđčćž")
// }

func TestFloat(t *testing.T) {
	testToSqlToType(t, SYBFLT8, float64(123.45))
	testToSqlToType(t, SYBREAL, float32(123.45))
}

func TestBool(t *testing.T) {
	testToSqlToType(t, SYBBIT, false)
	testToSqlToType(t, SYBBIT, true)
}

func TestMoney(t *testing.T) {
	testToSqlToType(t, SYBMONEY4, float64(1223.45))
	testToSqlToType(t, SYBMONEY, float64(1223.45))
	testToSqlToType(t, SYBMONEY, float64(1234.56))
	testToSqlToType(t, SYBMONEY, float64(1234.56))
}

func TestTime(t *testing.T) {
	value := time.Now()
	typ := SYBDATETIME
	data, err := typeToSqlBuf(typ, value)
	assert.Nil(t, err)
	value2 := sqlBufToType(typ, data)
	value2t, _ := value2.(time.Time)
	diff := value2t.Sub(value)
	if diff > 3000000 && diff < -3000000 {
		t.Error()
		fmt.Printf("TestTime\n%s\n%s\ndiff: %d", value, value2t, diff)
	}
}

func TestTime4(t *testing.T) {
	value := time.Date(2014, 1, 5, 23, 24, 0, 0, time.UTC)
	testToSqlToType(t, SYBDATETIME4, value)
}

func TestBinary(t *testing.T) {
	value := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	testToSqlToType(t, SYBVARBINARY, value)
}

func testToSqlToType(t *testing.T, typ int, value interface{}) {
	data, err := typeToSqlBuf(typ, value)
	assert.Nil(t, err)
	value2 := sqlBufToType(typ, data)
	assert.Equal(t, value, value2)
}
