package freetds

import (
	"errors"
)

func NewSpResult() *SpResult {
	return &SpResult{currentResult: -1, status: -1}
}

//Stored procedure execution result.
type SpResult struct {
	results       []*Result
	status        int
	outputParams  []*SpOutputParam
	currentResult int
}

//Does the stored procedure returned any resultsets.
func (r *SpResult) HasResults() bool {
	return len(r.results) > 0
}

//Does the stored procedure has any output params.
func (r *SpResult) HasOutputParams() bool {
	return len(r.outputParams) > 0
}

//Stored procedure return value.
func (r SpResult) Status() int {
	return r.status
}

//Number of returned Results
func (r *SpResult) ResultsCount() int {
	return len(r.results)
}

//Returns current result
func (r *SpResult) Result() *Result {
	if r.currentResult == -1 {
		r.NextResult()
	}
	if !r.resultExists() {
		return nil
	}
	return r.results[r.currentResult]
}

func (r *SpResult) resultExists() bool {
	return len(r.results) > r.currentResult
}

//Navigate to next result. True if sucessfull, false if no more resutls.
func (r *SpResult) NextResult() bool {
	r.currentResult++
	return r.resultExists()
}

//Scan current result.
func (r *SpResult) Scan(dest ...interface{}) error {
	rst := r.Result()
	if rst == nil {
		return errors.New("No current result to scan.")
	}
	if rst.currentRow == -1 {
		rst.Next()
	}
	return rst.Scan(dest...)
}

func (r *SpResult) MustScan(cnt int, dest ...interface{}) error {
	rst := r.Result()
	if rst == nil {
		return errors.New("No current result to scan.")
	}
	if rst.currentRow == -1 {
		rst.Next()
	}
	return rst.MustScan(cnt, dest...)
}

//Call Next on current result. True if next row in current result exists, otherwise false.
func (r *SpResult) Next() bool {
	rst := r.Result()
	if rst == nil {
		return false
	}
	return rst.Next()
}

//Sacaning output parameters of stored procedure
func (r *SpResult) ParamScan(values ...interface{}) error {
	outputValues := make([]interface{}, len(r.outputParams))
	for i := 0; i < len(r.outputParams); i++ {
		outputValues[i] = r.outputParams[i].Value
	}
	return assignValues(outputValues, values)
}

//Stored procedure output parameter name and value.
type SpOutputParam struct {
	Name  string
	Value interface{}
}
