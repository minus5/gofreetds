package freetds

type Result struct {
  Columns []*ResultColumn
  Rows [][]interface{}
  ReturnValue int
  RowsAffected int
  Message string
}

func NewResult() *Result {
  r := new(Result)
  r.Columns = make([]*ResultColumn, 0)
  r.Rows = nil
  return r
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
