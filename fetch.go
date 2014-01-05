package freetds

import (
  "errors"
)

/*
#include <sybfront.h>
#include <sybdb.h>

static int my_dbcount(DBPROCESS * dbproc) {
 return DBCOUNT(dbproc);
}
*/
import "C"


func (conn *Conn) fetchResults() ([]*Result, error) {
  results := make([]*Result, 0)
  for {
    erc := C.dbresults(conn.dbproc)
    if erc == C.NO_MORE_RESULTS {
      break
    }
    if (erc == C.FAIL) {
      return nil, errors.New("dbresults failed")
    }
    result := NewResult()
    conn.currentResult = result
    cols := int(C.dbnumcols(conn.dbproc))
    columns := make([]column, cols)
    for i:=0; i<cols; i++ {
      no := C.int(i+1)
      name := C.GoString(C.dbcolname(conn.dbproc, no))
      size := C.dbcollen(conn.dbproc, no)
      typ := C.dbcoltype(conn.dbproc, no)
      bindTyp := dbbindtype(typ)
      result.AddColumn(name, int(size), int(typ))
      if bindTyp == C.NTBSTRINGBIND && C.SYBCHAR != typ {
        size = C.DBINT(C.dbwillconvert(typ, C.SYBCHAR))
      }
      col := &columns[i]
      col.name = name
      col.typ = int(typ)
      col.size = int(size)
      col.bindTyp = int(bindTyp)
      col.buffer = make([]byte, size + 1)
      erc = C.dbbind(conn.dbproc, no, bindTyp, size + 1, (*C.BYTE)(&col.buffer[0]))
      if (erc == C.FAIL) {
        return nil, errors.New("dbbind failed: no such column or no such conversion possible, or target buffer too small")
      }
      erc = C.dbnullbind(conn.dbproc, no, &col.status)
      if (erc == C.FAIL) {
        return nil, errors.New("dbnullbind failed")
      }
    }

    for i := 0 ;; i++ {
      rowCode := C.dbnextrow(conn.dbproc)
      if rowCode == C.NO_MORE_ROWS {
        break
      }
      if rowCode == C.REG_ROW {
        for j:=0; j<cols; j++ {
          col := columns[j]
          result.AddValue(i, j, col.Value())
        }
      }
    }

    result.RowsAffected = int(C.my_dbcount(conn.dbproc))

    if C.dbhasretstat(conn.dbproc) == C.TRUE {
      result.ReturnValue = int(C.dbretstatus(conn.dbproc))
    }

    results = append(results, result)
    conn.currentResult = nil
  }
	if len(conn.Error) > 0 {
    return results, errors.New(conn.Error)
  }
  return results, nil
}

type column struct {
  name string
  typ int
  size int
  status C.DBINT
  bindTyp int
  buffer []byte
}

func (col *column) Value() interface{}{
  if col.status == -1 {
    return nil
  }
	return sqlBufToType(col.typ, col.buffer)
} 

