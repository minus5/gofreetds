package freetds

import (
	"github.com/stretchrcom/testify/assert"
	"testing"
	"time"
)

var now = time.Now()

func testResult() *Result {
	r := NewResult()
	r.addColumn("I", 0, 0)
	r.addColumn("S", 0, 0)
	r.addColumn("Tm", 0, 0)
	r.addColumn("F", 0, 0)
	for i := 0; i < 3; i++ {
		r.addValue(i, 0, 1)
		r.addValue(i, 1, "two")
		r.addValue(i, 2, now)
		r.addValue(i, 3, float64(123.45))
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
	assert.NotNil(t, err)
}

func TestResultScanOnNonPointerValues(t *testing.T) {
	r := testResult()
	var i int
	var s string
	var tm time.Time
	var f float64
	assert.True(t, r.Next())
	err := r.Scan(&i, &s, &tm, f)
	assert.NotNil(t, err) //error is raised
}

func TestResultScanIntoStruct(t *testing.T) {
	r := testResult()
	var s struct {
		I int
		S string
		Tm time.Time
		F float64
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
	assert.NotNil(t, err)
}
