package freetds

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var now = time.Now()

func testResult() *Result {
	r := NewResult()
	r.addColumn("i", 0, 0)
	r.addColumn("s", 0, 0)
	r.addColumn("tm", 0, 0)
	r.addColumn("f", 0, 0)

	r.addColumn("int", 0, 0)
	r.addColumn("int8", 0, 0)
	r.addColumn("int16", 0, 0)
	r.addColumn("int32", 0, 0)
	r.addColumn("int64", 0, 0)

	r.addColumn("float32", 0, 0)
	r.addColumn("float64", 0, 0)
	for i := 0; i < 3; i++ {
		r.addValue(i, 0, int32(i+1))
		r.addValue(i, 1, "two")
		r.addValue(i, 2, now)
		r.addValue(i, 3, float64(123.45))

		r.addValue(i, 4, int(1))
		r.addValue(i, 5, int8(2))
		r.addValue(i, 6, int16(3))
		r.addValue(i, 7, int32(4))
		r.addValue(i, 8, int64(5))

		r.addValue(i, 9, float32(5.5))
		r.addValue(i, 10, float64(6.5))
	}
	return r
}

func TestResultScan(t *testing.T) {
	r := testResult()
	var i int
	var s string
	var tm time.Time
	var f float64
	assert.True(t, r.Next())
	err := r.Scan(&i, &s, &tm, &f)
	assert.Nil(t, err)
	assert.Equal(t, i, 1)
	assert.Equal(t, s, "two")
	assert.Equal(t, tm, now)
	assert.Equal(t, f, float64(123.45))
}

func TestResultScanSingleTime(t *testing.T) {
	var tm time.Time
	r := NewResult()
	r.addColumn("tm", 0, 0)
	r.addValue(0, 0, now)
	assert.True(t, r.Next())
	err := r.Scan(&tm)
	assert.Nil(t, err)
	assert.Equal(t, tm, now)
}

func TestResultCurrentRow(t *testing.T) {
	r := testResult()
	assert.Equal(t, -1, r.CurrentRow())
	assert.True(t, r.Next())
	assert.Equal(t, 0, r.CurrentRow())
}

func TestResultHasNext(t *testing.T) {
	r := testResult()
	assert.Equal(t, len(r.Rows), 3)
	assert.True(t, r.Next())
	assert.True(t, r.HasNext())
	assert.True(t, r.Next())
	assert.True(t, r.HasNext())
	assert.True(t, r.Next())
	assert.False(t, r.Next())
	assert.False(t, r.HasNext())
}

func TestResultNext(t *testing.T) {
	r := testResult()
	assert.Equal(t, len(r.Rows), 3)
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	assert.False(t, r.Next())
}

func TestResultScanWithoutNext(t *testing.T) {
	r := testResult()
	var i int
	var s string
	var tm time.Time
	var f float64
	err := r.Scan(&i, &s, &tm, &f)
	assert.Error(t, err)
}

func TestResultScanOnNonPointerValues(t *testing.T) {
	r := testResult()
	var i int
	var s string
	var tm time.Time
	var f float64
	assert.True(t, r.Next())
	err := r.Scan(&i, &s, &tm, f)
	assert.Error(t, err)
}

func TestResultScanIntoStruct(t *testing.T) {
	r := testResult()
	var s struct {
		I  int
		S  string
		Tm time.Time
		F  float64
	}
	r.Next()
	err := r.Scan(&s)
	assert.Nil(t, err)
	assert.Equal(t, s.I, 1)
	assert.Equal(t, s.S, "two")
	assert.Equal(t, s.Tm, now)
	assert.Equal(t, s.F, float64(123.45))
	assert.Equal(t, 4, r.scanCount)

	err = r.MustScan(4, &s)
	assert.Nil(t, err)
	err = r.MustScan(5, &s)
	assert.Error(t, err)
}

func TestScanTypesInStructDoesNotMatchThoseInResult(t *testing.T) {
	r := testResult()
	var s struct {
		Int     int
		Int8    int
		Int16   int
		Int32   int
		Int64   int
		Float32 float64
		Float64 float32
	}
	r.Next()
	err := r.Scan(&s)
	assert.Nil(t, err)
	assert.Equal(t, s.Int, 1)
	assert.Equal(t, s.Int8, 2)
	assert.Equal(t, s.Int16, 3)
	assert.Equal(t, s.Int32, 4)
	assert.Equal(t, s.Int64, 5)

	assert.Equal(t, s.Float32, 5.5)
	assert.EqualValues(t, s.Float64, 6.5)
}

func TestResultScanColumn(t *testing.T) {
	r := testResult()
	assert.True(t, r.Next())
	var s string
	err := r.ScanColumn("s", &s)
	assert.Nil(t, err)
	assert.Equal(t, "two", s)
}

func TestResultScanColumnWithoutNext(t *testing.T) {
	r := testResult()
	var s string
	err := r.ScanColumn("s", &s)
	assert.Error(t, err)
}

func TestResultScanColumnOnNonPointerValues(t *testing.T) {
	r := testResult()
	assert.True(t, r.Next())
	var s string
	err := r.ScanColumn("s", s)
	assert.Error(t, err)
}

func TestResultScanColumnMissing(t *testing.T) {
	r := testResult()
	assert.True(t, r.Next())
	var s string
	err := r.ScanColumn("non_existing", &s)
	assert.Error(t, err)
}
