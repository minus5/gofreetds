package freetds

import (
	"fmt"
	"sync"
)

/*
#include <sybfront.h>
#include <sybdb.h>
*/
import "C"

var lastError string
var lastErrorMutex sync.Mutex

var lastMessage string
var lastMessageMutex sync.Mutex

//export errHandler
func errHandler(dbprocAddr C.long, severity, dberr, oserr C.int, dberrstr, oserrstr *C.char) C.int {
	var err string
	if dberr != 0 {
		err = fmt.Sprintf("Msg %d, Level %d", dberr, severity)
	} else {
		err = fmt.Sprintf("DB-LIBRARY error")
	}
	if oserr != 0 && oserrstr != nil {
		err += fmt.Sprintf("(OS error %d: %s)", oserr, C.GoString(oserrstr))
	}
	err += fmt.Sprintf("\n%s\n\n", C.GoString(dberrstr))

	lastErrorMutex.Lock()
	lastError = err
	lastErrorMutex.Unlock()

	conn := getConnection(int64(dbprocAddr))
	if conn != nil {
		conn.addError(err)
	}

	//fmt.Printf("err: %s", err)
	return C.INT_CANCEL
}

//export msgHandler
func msgHandler(dbprocAddr C.long, msgno C.DBINT, msgstate, severity C.int, msgtext, srvname, procname *C.char, line C.int) C.int {
	//changed_database = 5701, changed_language = 5703
	if msgno == 5701 || msgno == 5703 {
		return 0
	}

	msg := ""
	if msgno > 0 {
		msg += fmt.Sprintf("Msg %d, Level %d, State %d\n", msgno, severity, msgstate)

		ssrvname := C.GoString(srvname)
		sprocname := C.GoString(procname)

		if len(ssrvname) > 0 {
			msg += fmt.Sprintf("Server '%s', ", ssrvname)
		}
		if len(sprocname) < 0 {
			msg += fmt.Sprintf("Procedure '%s', ", sprocname)
		}
		if line > 0 {
			msg += fmt.Sprintf("Line %d", line)
		}

		msg += fmt.Sprintf("\n\t")
	}
	msg += fmt.Sprintf("%s\n", C.GoString(msgtext))

	lastMessageMutex.Lock()
	lastMessage = msg
	lastMessageMutex.Unlock()

	conn := getConnection(int64(dbprocAddr))
	if conn != nil {
		conn.addMessage(msg, int(msgno))
	}

	//fmt.Printf("msg: %s", msg)
	return 0
}
