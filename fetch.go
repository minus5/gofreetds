package freetds

import (
	"errors"
//	"fmt"
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
		if erc == C.FAIL {
			return nil, errors.New("dbresults failed")
		}
		result := NewResult()
		conn.currentResult = result
		cols := int(C.dbnumcols(conn.dbproc))
		columns := make([]column, cols)
		for i := 0; i < cols; i++ {
			no := C.int(i + 1)
			name := C.GoString(C.dbcolname(conn.dbproc, no))
			size := C.dbcollen(conn.dbproc, no)
			typ := C.dbcoltype(conn.dbproc, no)
			if typ == SYBUNIQUE {
				size = 36
			}
			bindTyp, typ := dbbindtype(typ)
			result.addColumn(name, int(size), int(typ))
			if bindTyp == C.NTBSTRINGBIND && C.SYBCHAR != typ && C.SYBTEXT != typ {
				size = C.DBINT(C.dbwillconvert(typ, C.SYBCHAR))
			}
			col := &columns[i]
			col.name = name
			col.typ = int(typ)
			col.size = int(size)
			col.bindTyp = int(bindTyp)
			col.buffer = make([]byte, size+1)
			erc = C.dbbind(conn.dbproc, no, bindTyp, size+1, (*C.BYTE)(&col.buffer[0]))
			//fmt.Printf("dbbind %d, %d, %v\n", bindTyp, size+1, col.buffer)
			if erc == C.FAIL {
				return nil, errors.New("dbbind failed: no such column or no such conversion possible, or target buffer too small")
			}
			erc = C.dbnullbind(conn.dbproc, no, &col.status)
			if erc == C.FAIL {
				return nil, errors.New("dbnullbind failed")
			}
		}

		for i := 0; ; i++ {
			rowCode := C.dbnextrow(conn.dbproc)
			if rowCode == C.NO_MORE_ROWS {
				break
			}
			if rowCode == C.REG_ROW {
				for j := 0; j < cols; j++ {
					col := columns[j]
					//fmt.Printf("col: %#v\nvalue:%s\n", col, col.Value())
					result.addValue(i, j, col.Value())
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
		return results, conn.raise(nil)
	}
	return results, nil
}

type column struct {
	name    string
	typ     int
	size    int
	status  C.DBINT
	bindTyp int
	buffer  []byte
}

func (col *column) Value() interface{} {
	if col.status == -1 {
		return nil
	}
	return sqlBufToType(col.typ, col.buffer)
}

func dbbindtype(datatype C.int) (C.int, C.int) {
	switch datatype {
	//this will map decimal, and numeric datatypes to float
	case C.SYBDECIMAL, C.SYBNUMERIC:
		return C.FLT8BIND, C.SYBFLT8
		//for all other types return datatype as second param
	case C.SYBIMAGE, C.SYBVARBINARY, C.SYBBINARY:
		return C.BINARYBIND, datatype
	case C.SYBBIT:
		return C.BITBIND, datatype
	case C.SYBTEXT, C.SYBVARCHAR, C.SYBCHAR:
		return C.NTBSTRINGBIND, datatype
	case C.SYBDATETIME:
		return C.DATETIMEBIND, datatype
	case C.SYBDATETIME4:
		return C.SMALLDATETIMEBIND, datatype
	case C.SYBFLT8:
		return C.FLT8BIND, datatype
	case C.SYBREAL:
		return C.REALBIND, datatype
	case C.SYBINT1:
		return C.TINYBIND, datatype
	case C.SYBINT2:
		return C.SMALLBIND, datatype
	case C.SYBINT4:
		return C.INTBIND, datatype
	case C.SYBINT8:
		return C.BIGINTBIND, datatype
	case C.SYBMONEY:
		return C.MONEYBIND, datatype
	case C.SYBMONEY4:
		return C.SMALLMONEYBIND, datatype
	case SYBUNIQUE: 
		return C.STRINGBIND, C.SYBCHAR
	}
	//TODO - log unknown datatype
	return C.NTBSTRINGBIND, datatype
}
