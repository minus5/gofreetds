package freetds

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSpResult(t *testing.T) {
	r := NewSpResult()
	assert.Equal(t, -1, r.status)
	assert.Equal(t, -1, r.currentResult)
	assert.False(t, r.HasOutputParams())
	assert.False(t, r.HasResults())
	assert.False(t, r.NextResult())
	assert.Nil(t, r.Result())
}

func TestSpResultNextResult(t *testing.T) {
	r := NewSpResult()
	r1 := testResult()
	r2 := testResult()
	r.results = []*Result{r1, r2}
	assert.Equal(t, 2, r.ResultsCount())
	assert.True(t, r.HasResults())
	assert.True(t, r.NextResult())
	assert.True(t, r1 == r.Result())
	assert.True(t, r.NextResult())
	assert.True(t, r2 == r.Result())
	assert.False(t, r.NextResult())
	assert.Nil(t, r.Result())
}

func TestSpResultScan(t *testing.T) {
	r := NewSpResult()
	r1 := testResult()
	r2 := testResult()
	r.results = []*Result{r1, r2}

	var i int
	err := r.Scan(&i)
	assert.Nil(t, err)
	assert.Equal(t, i, 1)

	assert.True(t, r.Next())

	err = r.Scan(&i)
	assert.Nil(t, err)
	assert.Equal(t, i, 2)
}
