package freetds

import (
	"errors"
	"unsafe"
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
			return nil, conn.raise(errors.New("dbresults failed"))
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
			if typ == SYBNUMERIC || typ==SYBDECIMAL {
				size = 8
			}
			bindTyp, typ := dbbindtype(typ)
			result.addColumn(name, int(size), int(typ))
			if bindTyp == C.NTBSTRINGBIND && C.SYBCHAR != typ && C.SYBTEXT != typ && XSYBXML != typ {
				size = C.DBINT(C.dbwillconvert(typ, C.SYBCHAR))
			}
			col := &columns[i]
			// detecting varchar(max) or varbinary(max) types
			col.canVary = (size == 2147483647 && typ == SYBCHAR) ||
				(size == 2147483647 && typ == XSYBXML) ||
				(size == 1073741823 && typ == SYBBINARY) ||
				(size == 64512 && typ == SYBIMAGE) //varbinary(MAX)

			col.name = name
			col.typ = int(typ)
			col.size = int(size)
			col.bindTyp = int(bindTyp)
			// If row data can vary, don't bind it now, read the data later using C.dbdata when scanning rows.
			if !col.canVary {
				col.buffer = make([]byte, size+1)
				erc = C.dbbind(conn.dbproc, no, bindTyp, size+1, (*C.BYTE)(&col.buffer[0]))
				//fmt.Printf("dbbind %d, %d, %v\n", bindTyp, size+1, col.buffer)
				if erc == C.FAIL {
					return nil, errors.New("dbbind failed: no such column or no such conversion possible, or target buffer too small")
				}
			}
			// We still use dbnullbind for all variable and non variable columns. Should work fine.
			erc = C.dbnullbind(conn.dbproc, no, &col.status)
			if erc == C.FAIL {
				return nil, errors.New("dbnullbind failed")
			}
		}

	rows_loop:
		for i := 0; ; i++ {
			switch C.dbnextrow(conn.dbproc) {
			case C.NO_MORE_ROWS:
				break rows_loop
			case C.BUF_FULL:
				return nil, errors.New("dbnextrow failed: Buffer Full")
			case C.FAIL:
				return nil, errors.New("dbnextrow failed: Failure")
			case C.REG_ROW:
				for j := 0; j < cols; j++ {
					col := columns[j]
					//fmt.Printf("col: %#v\nvalue:%s\n", col, col.Value())

					no := C.int(j + 1)
					// if canVary is true, we don't rely on dbbind to do it's thing,
					// but instead we will ask C.dbdata() for pointer to the data.
					// We cannot call C.dbbind here, because for that it's too late (we already called C.dbnextrow()).
					if col.canVary {
						// actual size for this row
						// dbdata returns null if data are null.
						// Source: http://lists.ibiblio.org/pipermail/freetds/2015q2/029392.html
						//    From Sybase documentation:
						//    "A NULL BYTE pointer is returned if there is no such column or if the
						//    data has a null value. To make sure that the data is really a null
						//    value, you should always check for a return of 0 from *dbdatlen*."
						//
						//    From Microsoft documentation:
						//    "A NULL BYTE pointer is returned if there is no such column or if the
						//    data has a null value. To make sure that the data is really a null
						//    value, check for a return of 0 from *dbdatlen*."
						//
						//    So you can use: dbdata()==nil && dbdatlen()==0
						// @see http://www.freetds.org/reference/a00341.html#gaee60c306a22383805a4b9caa647a1e16
						size := C.dbdatlen(conn.dbproc, no)
						data := C.dbdata(conn.dbproc, no)
						if data == nil && size != 0 {
							return nil, errors.New("dbdata failed: server returned non-nil data with size 0")
						}
						if data != nil {
							// @see https://github.com/golang/go/wiki/cgo
							if col.typ == SYBBINARY || col.typ == SYBIMAGE {
								size++
							}
							//fmt.Printf("col.typ: %d\n", col.typ)
							col.buffer = C.GoBytes(unsafe.Pointer(data), C.int(size))
						}
					}

					result.addValue(i, j, col.Value())
				}
			default:
				// Continue looping
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
	canVary bool
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
