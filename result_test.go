package freetds

import (
	"github.com/stretchrcom/testify/assert"
	"testing"
	"time"
)

var now = time.Now()

func testResult() *Result {
	r := NewResult()
	r.AddColumn("1", 0, 0)
	r.AddColumn("2", 0, 0)
	r.AddColumn("3", 0, 0)
	r.AddColumn("4", 0, 0)
	for i:=0; i<3; i++ {
		r.AddValue(i, 0, 1)
		r.AddValue(i, 1, "two")
		r.AddValue(i, 2, now) 
		r.AddValue(i, 3, float64(123.45)) 
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
