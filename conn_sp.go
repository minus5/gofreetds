package freetds

import (
	"errors"
	"fmt"
	"unsafe"
)

/*
 #cgo LDFLAGS: -lsybdb
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <assert.h>
 #include <errno.h>
 #include <unistd.h>
 #include <libgen.h>

 #include <sybfront.h>
 #include <sybdb.h>
*/
import "C"

//Execute stored procedure by name and list of params.
//
//Example:
//  conn.ExecSp("sp_help", "authors")
func (conn *Conn) ExecSp(spName string, params ...interface{}) (*SpResult, error) {
	if conn.isDead() || conn.isMirrorSlave() {
		if err := conn.reconnect(); err != nil {
			return nil, err
		}
	}

	//hold references to data sent to the C code until the end of this function
	//without this GC could remove something used later in C, and we will get SIGSEG
	refHolder := make([]*[]byte, 0)
	conn.clearMessages()

	name := C.CString(spName)
	defer C.free(unsafe.Pointer(name))

	if C.dbrpcinit(conn.dbproc, name, 0) == C.FAIL {
		return nil, conn.raiseError("dbrpcinit failed")
	}
	//input params
	spParams, err := conn.getSpParams(spName)
	if err != nil {
		return nil, err
	}
	for i, spParam := range spParams {
		//get datavalue for the suplied stored procedure parametar
		var datavalue *C.BYTE
		datalen := 0
		if i < len(params) {
			param := params[i]
			if param != nil {
				data, sqlDatalen, err := typeToSqlBuf(int(spParam.UserTypeId), param, conn.freetdsVersionGte095)
				if err != nil {
					conn.Close() //close the connection
					return nil, err
				}
				if len(data) > 0 {
					datalen = sqlDatalen
					datavalue = (*C.BYTE)(unsafe.Pointer(&data[0]))
					refHolder = append(refHolder, &data)
				}
			}
		}
		//set parametar valus, call dbrpcparam
		if i < len(params) || spParam.IsOutput {
			maxOutputSize := C.DBINT(-1)
			status := C.BYTE(0)
			if spParam.IsOutput {
				status = C.DBRPCRETURN
				maxOutputSize = C.DBINT(spParam.MaxLength)
				if maxOutputSize == -1 {
					maxOutputSize = 8000
				}
			}
			paramname := C.CString(spParam.Name)
			defer C.free(unsafe.Pointer(paramname))
			if C.dbrpcparam(conn.dbproc, paramname, status,
				C.int(spParam.UserTypeId), maxOutputSize, C.DBINT(datalen), datavalue) == C.FAIL {
				return nil, errors.New("dbrpcparam failed")
			}
		}
	}
	//execute
	if C.dbrpcsend(conn.dbproc) == C.FAIL {
		return nil, conn.raiseError("dbrpcsend failed")
	}
	//results
	result := NewSpResult()
	result.results, err = conn.fetchResults()
	if err != nil {
		return nil, conn.raise(err)
	}
	//return status
	if C.dbhasretstat(conn.dbproc) == C.TRUE {
		result.status = int(C.dbretstatus(conn.dbproc))
	}
	//read output params
	numOutParams := int(C.dbnumrets(conn.dbproc))
	result.outputParams = make([]*SpOutputParam, numOutParams)
	for i := 1; i <= numOutParams; i++ {
		j := C.int(i)
		len := C.dbretlen(conn.dbproc, j)
		name := C.GoString(C.dbretname(conn.dbproc, j))
		typ := int(C.dbrettype(conn.dbproc, j))
		data := C.GoBytes(unsafe.Pointer(C.dbretdata(conn.dbproc, j)), len)
		value := sqlBufToType(typ, data)
		param := &SpOutputParam{Name: name, Value: value}
		result.outputParams[i-1] = param
	}

	return result, nil
}

func (conn *Conn) raise(err error) error {
	if len(conn.Error) != 0 {
		return errors.New(fmt.Sprintf("%s\n%s", conn.Error, conn.Message))
	}
	return err
}

func (conn *Conn) raiseError(errMsg string) error {
	return conn.raise(errors.New(errMsg))
}

// func toRpcParam(datatype int, value interface{}) (datalen C.DBINT, datavalue *C.BYTE, err error) {
//   data, err := typeToSqlBuf(datatype, value)
//   if err != nil {
//     return
//   }
//   datalen = C.DBINT(len(data))
//   if len(data) > 0 {
//     datavalue = (*C.BYTE)(unsafe.Pointer(&data[0]))
//   }
//   //fmt.Printf("\ndatavalue: %v, datalen: %v, data: %v %s\n", datavalue, datalen, data, data)
//   return
// }

//Stored procedure parameter definition
type spParam struct {
	Name        string
	ParameterId int32
	UserTypeId  int32
	IsOutput    bool
	MaxLength   int16
	Precision   uint8
	Scale       uint8
}

//Read stored procedure parameters.
//Will cache params in connection or pool and reuse it.
func (conn *Conn) getSpParams(spName string) ([]*spParam, error) {
	if spParams, ok := conn.spParamsCache.Get(spName); ok {
		return spParams, nil
	}

	sql := conn.getSpParamsSql(spName)

	results, err := conn.exec(sql)
	if err != nil {
		return nil, err
	}
	r := results[0]
	spParams := make([]*spParam, len(r.Rows))
	for i := 0; r.Next(); i++ {
		p := &spParam{}
		err := r.Scan(&p.Name, &p.ParameterId, &p.UserTypeId, &p.IsOutput, &p.MaxLength, &p.Precision, &p.Scale)
		//fixme: mapping uniqueidentifier, datetimeoffset, date, time, datetime2 to string
		if p.UserTypeId == 0x24 || p.UserTypeId == 0x2B || p.UserTypeId == 0x28 || p.UserTypeId == 0x29 || p.UserTypeId == 0x2A {
			p.UserTypeId = 0x27
		}
		if err != nil {
			return nil, err
		}
		spParams[i] = p
	}

	conn.spParamsCache.Set(spName, spParams)
	return spParams, nil
}

const msSqlGetSpParamsSql string = `
select name, parameter_id, user_type_id, is_output, max_length, precision, scale
from sys.all_parameters
where object_id =  (select object_id from sys.all_objects where object_id = object_id('%s'))
order by parameter_id
`

const sybaseAseGetSpParamsSql string = `
  select name = c.name,
         parameter_id = c.id,
         user_type_id = c.type,
         is_output = case
                       when c.status2 = 2 or c.status2 = 4 then 1
                       else 0
                     end,
         max_length = c.length,
         precision = isnull(c.prec,0),
         scale = isnull(c.scale,0)
    from sysobjects o
         join syscolumns c
           on c.id = o.id
  where o.name = '%s'
  order by c.id, c.colid
`

func (conn *Conn) getSpParamsSql(spName string) string {
	if conn.sybaseMode() || conn.sybaseMode125() {
		return fmt.Sprintf(sybaseAseGetSpParamsSql, spName)
	}
	return fmt.Sprintf(msSqlGetSpParamsSql, spName)
}
