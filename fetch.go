package freetds

import (
//  "fmt"
  "errors"
  "strings"
  "bytes"
  "encoding/binary"
  "time"
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
//  fmt.Printf("%s %v\n", col.name, col.buffer)

  if col.status == -1 {
    return nil
  }

  if col.bindTyp == C.NTBSTRINGBIND {
    len := strings.Index(string(col.buffer), "\x00")
    return string(col.buffer[:len])
  }

  buf := bytes.NewBuffer(col.buffer)
  switch col.typ {
  case C.SYBINT1:
    var value uint8
    binary.Read(buf, binary.LittleEndian, &value)
    return value
  case C.SYBINT2:
    var value int16
    binary.Read(buf, binary.LittleEndian, &value)
    return value
  case C.SYBINT4:
    var value int32
    binary.Read(buf, binary.LittleEndian, &value)
    return value
  case C.SYBINT8:
    var value int64
    binary.Read(buf, binary.LittleEndian, &value)
    return value
  case C.SYBDATETIME:
    var days int32  /* number of days since 1/1/1900 */
    var sec  uint32 /* 300ths of a second since midnight */
    binary.Read(buf, binary.LittleEndian, &days)
    binary.Read(buf, binary.LittleEndian, &sec)
    value := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
    value = value.Add(time.Duration(days) * time.Hour * 24).Add(time.Duration(sec) * time.Second / 300)
    return value
  case C.SYBDATETIME4:
    var days uint16  /* number of days since 1/1/1900 */
    var mins  uint16 /* number of minutes since midnight */
    binary.Read(buf, binary.LittleEndian, &days)
    binary.Read(buf, binary.LittleEndian, &mins)
    value := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
    value = value.Add(time.Duration(days) * time.Hour * 24).Add(time.Duration(mins) * time.Minute)
    return value
  case C.SYBMONEY:
    var high int32
    var low  uint32
    binary.Read(buf, binary.LittleEndian, &high)
    binary.Read(buf, binary.LittleEndian, &low)
    return float64(int64(high) * 4294967296 + int64(low)) / 10000
  case C.SYBMONEY4 :
    var value int32
    binary.Read(buf, binary.LittleEndian, &value)
    return float64(value) / 10000
  case C.SYBREAL:
    var value float32
    binary.Read(buf, binary.LittleEndian, &value)
    return value
  case C.SYBFLT8:
    var value float64
    binary.Read(buf, binary.LittleEndian, &value)
    return value
  case C.SYBBIT:
    return col.buffer[0] == 1
  case C.SYBIMAGE, C.SYBVARBINARY, C.SYBBINARY:
    return append([]byte{},  col.buffer...) // make copy of col.buffer

    //TODO - decimal i numeric datatypes
  }
  return nil
}

func dbbindtype(datatype C.int) C.int {
  switch datatype {
  case C.SYBIMAGE, C.SYBVARBINARY, C.SYBBINARY:
    return C.BINARYBIND;
  case C.SYBBIT:
    return C.BITBIND;
  case C.SYBTEXT, C.SYBVARCHAR, C.SYBCHAR:
    return C.NTBSTRINGBIND;
  case C.SYBDATETIME:
    return C.DATETIMEBIND;
  case C.SYBDATETIME4:
    return C.SMALLDATETIMEBIND;
  case C.SYBDECIMAL:
    return C.DECIMALBIND;
  case C.SYBNUMERIC:
    return C.NUMERICBIND;
  case C.SYBFLT8:
    return C.FLT8BIND;
  case C.SYBREAL:
    return C.REALBIND;
  case C.SYBINT1:
    return C.TINYBIND;
  case C.SYBINT2:
    return C.SMALLBIND;
  case C.SYBINT4:
    return C.INTBIND;
  case C.SYBINT8:
    return C.BIGINTBIND;
  case C.SYBMONEY:
    return C.MONEYBIND;
  case C.SYBMONEY4:
    return C.SMALLMONEYBIND;
  }
  //TODO - log unknown datatype
  return C.NTBSTRINGBIND;
}
