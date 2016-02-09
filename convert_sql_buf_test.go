package freetds

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInt(t *testing.T) {
	testToSqlToType(t, SYBINT4, 2147483647)
	testToSqlToType(t, SYBINT4, -2147483648)

	testToSqlToType(t, SYBINT4, int(2147483647))
	testToSqlToType(t, SYBINT4, int32(2147483647))
	testToSqlToType(t, SYBINT4, int64(2147483647))

	_, _, err := typeToSqlBuf(SYBINT4, "pero", false)
	assert.NotNil(t, err)
}

func TestInt16(t *testing.T) {
	testToSqlToType(t, SYBINT2, int16(32767))
	testToSqlToType(t, SYBINT2, int16(-32768))
	testToSqlToType(t, SYBINT2, 123)
	//overflow
	data, _, err := typeToSqlBuf(SYBINT2, 32768, false)
	assert.Nil(t, err)
	i16 := sqlBufToType(SYBINT2, data)
	assert.EqualValues(t, i16, -32768)
	//error
	_, _, err = typeToSqlBuf(SYBINT2, "pero", false)
	assert.NotNil(t, err)
}

func TestInt8(t *testing.T) {
	testToSqlToType(t, SYBINT1, uint8(127))
	testToSqlToType(t, SYBINT1, uint8(255))
	data, _, err := typeToSqlBuf(SYBINT1, 127, false)
	assert.Nil(t, err)
	value, _ := sqlBufToType(SYBINT1, data).(uint8)
	assert.Equal(t, int(value), 127)
}

func TestInt64(t *testing.T) {
	testToSqlToType(t, SYBINT8, int64(-9223372036854775808))
	testToSqlToType(t, SYBINT8, int64(9223372036854775807))
}

func TestFloat(t *testing.T) {
	testToSqlToType(t, SYBFLT8, float64(123.45))
	testToSqlToType(t, SYBFLT8, float32(123.5))
	testToSqlToType(t, SYBREAL, float32(123.45))
	testToSqlToType(t, SYBREAL, float64(123.5))
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
	f := func(value time.Time) {
		typ := SYBDATETIME
		data, _, err := typeToSqlBuf(typ, value, false)
		assert.Nil(t, err)
		value2 := sqlBufToType(typ, data)
		value2t, _ := value2.(time.Time)
		diff := value2t.Sub(value)
		if diff > 4000000 || diff < -4000000 {
			t.Errorf("TestTime %s != %s diff: %d", value, value2t, diff)
		}
	}
	f(time.Now())
	f(time.Now().UTC())
	f(time.Unix(1404856800, 0))
	f(time.Unix(1404856800, 0).UTC())
	f(sqlMaxTime)
	f(sqlMinTime)
}

func TestTime4(t *testing.T) {
	f := func(value time.Time) {
		typ := SYBDATETIME4
		data, _, err := typeToSqlBuf(typ, value, false)
		assert.Nil(t, err)
		value2 := sqlBufToType(typ, data)
		value2t, _ := value2.(time.Time)
		if !value.Equal(value2t) {
			t.Errorf("TestTime4 %s != %s", value, value2t)
		}
	}
	f(time.Date(2014, 1, 5, 23, 24, 0, 0, time.UTC))
	f(time.Date(2014, 1, 5, 23, 24, 0, 0, time.Local))
}

func TestBinary(t *testing.T) {
	value := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	testToSqlToType(t, SYBVARBINARY, value)
}

func testToSqlToType(t *testing.T, typ int, value interface{}) {
	data, _, err := typeToSqlBuf(typ, value, false)
	assert.Nil(t, err)
	value2 := sqlBufToType(typ, data)
	assert.EqualValues(t, value, value2)
}
