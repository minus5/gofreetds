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

//Stored procedure execution result.
type SpResult struct {
	Results      []*Result
	Status       int
	OutputParams []*SpOutputParam
}

//Does the stored procedure returned any resultsets.
func (r *SpResult) HasResults() bool {
	return len(r.Results) > 0
}

//Does the stored procedure has any output params.
func (r *SpResult) HasOutputParams() bool {
	return len(r.OutputParams) > 0
}

func (r *SpResult) Scan(values ...interface{}) error {
	outputValues := make([]interface{}, len(r.OutputParams))
	for i := 0; i < len(r.OutputParams); i++ {
		outputValues[i] = r.OutputParams[i].Value
	}
	return assingValues(outputValues, values)
}

//Stored procedure output parameter name and value.
type SpOutputParam struct {
	Name  string
	Value interface{}
}

//Execute stored procedure by name and list of params.
//
//Example:
//  conn.ExecSp("sp_help", "authors")
func (conn *Conn) ExecSp(spName string, params ...interface{}) (*SpResult, error) {
	conn.clearMessages()
	if C.dbrpcinit(conn.dbproc, C.CString(spName), 0) == C.FAIL {
		return nil, errors.New("dbrpcinit failed")
	}
	//input params
	spParams, err := conn.getSpParams(spName)
	if err != nil {
		return nil, err
	}
	for i, spParam := range spParams {
		datalen := C.DBINT(0)
		datavalue := (*C.BYTE)(unsafe.Pointer(&([]byte{0})[0]))
		maxOutputSize := C.DBINT(0)
		status := C.BYTE(0)
		if spParam.IsOutput {
			status = C.DBRPCRETURN
			maxOutputSize = C.DBINT(spParam.MaxLength)
		}
		if i < len(params) {
			param := params[i]
			if param != nil {
				datalen, datavalue, err = toRpcParam(int(spParam.UserTypeId), param)
				if err != nil {
					return nil, err
				}
			}
		}
		if i < len(params) || spParam.IsOutput {
			if C.dbrpcparam(conn.dbproc, C.CString(spParam.Name), status,
				C.int(spParam.UserTypeId), maxOutputSize, datalen, datavalue) == C.FAIL {
				return nil, errors.New("dbrpcparam failed")
			}
		}
	}
	//execute
	if C.dbrpcsend(conn.dbproc) == C.FAIL {
		if len(conn.Error) != 0 {
			return nil, errors.New(fmt.Sprintf("%s\n%s", conn.Error, conn.Message))
		} else {
			return nil, errors.New("dbrpcsend failed")
		}
	}
	//results
	result := &SpResult{Status: -1}
	result.Results, err = conn.fetchResults()
	if err != nil {

		if len(conn.Error) != 0 {
			return nil, errors.New(fmt.Sprintf("%s\n%s", conn.Error, conn.Message))
		} else {
			return nil, err
		}
	}
	//return status
	if C.dbhasretstat(conn.dbproc) == C.TRUE {
		result.Status = int(C.dbretstatus(conn.dbproc))
	}
	//output params
	numOutParams := int(C.dbnumrets(conn.dbproc))
	result.OutputParams = make([]*SpOutputParam, numOutParams)
	for i := 1; i <= numOutParams; i++ {
		j := C.int(i)
		len := C.dbretlen(conn.dbproc, j)
		name := C.GoString(C.dbretname(conn.dbproc, j))
		typ := int(C.dbrettype(conn.dbproc, j))
		data := C.GoBytes(unsafe.Pointer(C.dbretdata(conn.dbproc, j)), len)
		value := sqlBufToType(typ, data)
		param := &SpOutputParam{Name: name, Value: value}
		result.OutputParams[i-1] = param
	}

	return result, nil
}

func toRpcParam(datatype int, value interface{}) (datalen C.DBINT, datavalue *C.BYTE, err error) {
	data, err := typeToSqlBuf(datatype, value)
	if err != nil {
		return
	}
	datavalue = (*C.BYTE)(unsafe.Pointer(&data[0]))
	datalen = C.DBINT(len(data))
	//fmt.Printf("\ndatavalue: %v, datalen: %v, data: %v %s\n", datavalue, datalen, data, data)
	return
}

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
	if spParams, ok := conn.spParamsCache[spName]; ok {
		return spParams, nil
	}

	sql := fmt.Sprintf(`
select name, parameter_id, user_type_id, is_output, max_length, precision, scale
from sys.all_parameters
where object_id =  (select object_id from sys.all_objects where object_id = object_id('%s'))
order by parameter_id
`, spName)
	results, err := conn.exec(sql)
	if err != nil {
		return nil, err
	}
	r := results[0]
	spParams := make([]*spParam, len(r.Rows))
	for i := 0; r.Next(); i++ {
		p := &spParam{}
		err := r.Scan(&p.Name, &p.ParameterId, &p.UserTypeId, &p.IsOutput, &p.MaxLength, &p.Precision, &p.Scale)
		if err != nil {
			return nil, err
		}
		spParams[i] = p
	}

	conn.spParamsCache[spName] = spParams
	return spParams, nil
}
