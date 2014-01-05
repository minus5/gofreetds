package freetds

import (
	"fmt"
	"errors"
	"time"
)

type Result struct {
  Columns []*ResultColumn
  Rows [][]interface{}
  ReturnValue int
  RowsAffected int
  Message string
	currentRow int
}

func NewResult() *Result {
  return &Result{
		Columns: make([]*ResultColumn, 0),
		Rows: nil, 
		currentRow: -1,
	}
}

func (r *Result) AddColumn(name string, dbSize, dbType int) {
  c := new(ResultColumn)
  c.Name = name
  c.DbSize = dbSize
  c.DbType = dbType
  r.Columns = append(r.Columns, c)
}

func (r *Result) AddValue(row, col int, value interface{}) {
  if r.Rows == nil {
    r.Rows = make([][] interface{}, 1)
    r.Rows[0] = make([]interface{}, len(r.Columns))
  }
  for rc := len(r.Rows) - 1; rc < row; rc++ {
    r.Rows = append(r.Rows, make([]interface{}, len(r.Columns)))
  }
  r.Rows[row][col] = value
}

type ResultColumn struct {
  Name string
  DbSize int
  DbType int
  Type string
}

func (r *Result) Next() bool {
	if len(r.Rows) == 0 {
		return false
	}
	if r.currentRow >= len(r.Rows) - 1 {
		return false
	}
	r.currentRow++
	return true
}

func (r *Result) Scan(values ...interface{}) error {
	if len(values) > len(r.Columns) {
		return errors.New(fmt.Sprintf("more values %d than columns %d", len(values), len(r.Columns)))
	}
	for i, value := range(values) {
		rValue := r.Rows[r.currentRow][i]
		var ok bool
		switch f := value.(type) {
    case *string:
			*f, ok = rValue.(string)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to string.", rValue))
			}
    case *int:
			*f, ok = rValue.(int)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to int.", rValue))
			}
    case *uint8:
			*f, ok = rValue.(uint8)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to uint8.", rValue))
			}
    case *int16:
			*f, ok = rValue.(int16)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to int16.", rValue))
			}
    case *int32:
			*f, ok = rValue.(int32)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to int32.", rValue))
			}
    case *int64:
			*f, ok = rValue.(int64)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to int64.", rValue))
			}
    case *float32:
			*f, ok = rValue.(float32)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to float32.", rValue))
			}
    case *float64:
			*f, ok = rValue.(float64)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to float64.", rValue))
			}
    case *bool:
			*f, ok = rValue.(bool)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to bool.", rValue))
			}
    case *[]byte:
			*f, ok = rValue.([]byte)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to []byte.", rValue))
			}
    case *time.Time:
			*f, ok = rValue.(time.Time)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to convert %T to time.Time.", rValue))
			}
		}
	}
	return nil
}
