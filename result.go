package freetds

import (
	"errors"
	"fmt"
	"time"
)

type Result struct {
	Columns      []*ResultColumn
	Rows         [][]interface{}
	ReturnValue  int
	RowsAffected int
	Message      string
	currentRow   int
}

func NewResult() *Result {
	return &Result{
		Columns:    make([]*ResultColumn, 0),
		Rows:       nil,
		currentRow: -1,
	}
}

func (r *Result) addColumn(name string, dbSize, dbType int) {
	c := new(ResultColumn)
	c.Name = name
	c.DbSize = dbSize
	c.DbType = dbType
	r.Columns = append(r.Columns, c)
}

func (r *Result) addValue(row, col int, value interface{}) {
	if r.Rows == nil {
		r.Rows = make([][]interface{}, 1)
		r.Rows[0] = make([]interface{}, len(r.Columns))
	}
	for rc := len(r.Rows) - 1; rc < row; rc++ {
		r.Rows = append(r.Rows, make([]interface{}, len(r.Columns)))
	}
	r.Rows[row][col] = value
}

type ResultColumn struct {
	Name   string
	DbSize int
	DbType int
	Type   string
}

func (r *Result) Next() bool {
	if len(r.Rows) == 0 {
		return false
	}
	if r.currentRow >= len(r.Rows)-1 {
		return false
	}
	r.currentRow++
	return true
}

//Scan copies the columns in the current row into the values pointed at by dest.
func (r *Result) Scan(dest ...interface{}) error {
	return assingValues(r.Rows[r.currentRow], dest)
}

//assignValues copies to dest values in src
//dest should be a pointer type
//error is returned if types don't match
//TODO conversion can be performend for some types
//     for example if dest if int64 and src int32
//     this version requires exact type match
//     reference: http://golang.org/src/pkg/database/sql/convert.go
func assingValues(src []interface{}, dest []interface{}) error {
	if len(dest) > len(src) {
		return errors.New(fmt.Sprintf("More dest values %d than src values %d.", len(dest), len(src)))
	}
	for i, value := range dest {
		srcValue := src[i]
		var ok bool
		switch f := value.(type) {
		case *string:
			*f, ok = srcValue.(string)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to string.", srcValue))
			}
		case *int:
			*f, ok = srcValue.(int)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to int.", srcValue))
			}
		case *uint8:
			*f, ok = srcValue.(uint8)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to uint8.", srcValue))
			}
		case *int16:
			*f, ok = srcValue.(int16)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to int16.", srcValue))
			}
		case *int32:
			*f, ok = srcValue.(int32)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to int32.", srcValue))
			}
		case *int64:
			*f, ok = srcValue.(int64)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to int64.", srcValue))
			}
		case *float32:
			*f, ok = srcValue.(float32)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to float32.", srcValue))
			}
		case *float64:
			*f, ok = srcValue.(float64)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to float64.", srcValue))
			}
		case *bool:
			*f, ok = srcValue.(bool)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to bool.", srcValue))
			}
		case *[]byte:
			*f, ok = srcValue.([]byte)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to []byte.", srcValue))
			}
		case *time.Time:
			*f, ok = srcValue.(time.Time)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to time.Time.", srcValue))
			}
		}
	}
	return nil
}
