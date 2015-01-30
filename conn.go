//Package freetds provides interface to Microsoft Sql Server database by using freetds C lib: http://www.freetds.org.
package freetds

import (
	"errors"
	"fmt"
	"strings"
	"unsafe"
	//	"log"
	"sync"
	"time"
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

 static int err_handler(DBPROCESS * dbproc, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr)
 {
   extern int errHandler(long dbprocAddr, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr);
   return errHandler((long)dbproc, severity, dberr, oserr, dberrstr, oserrstr);
 }

 static int msg_handler(DBPROCESS * dbproc, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname, int line)
 {
   extern int msgHandler(long dbprocAddr, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname, int line);
   return msgHandler((long)dbproc, msgno, msgstate, severity, msgtext, srvname, procname, line);
 }

 static void my_dblogin(LOGINREC* login, char* username, char* password) {
  dbsetlogintime(10);
  dberrhandle(err_handler);
  dbmsghandle(msg_handler);
  DBSETLUSER(login, username);
  DBSETLPWD(login, password);
  dbsetlname(login, "UTF-8", DBSETCHARSET);
  dbsetlversion(login, DBVERSION_72);
 }

 static long dbproc_addr(DBPROCESS * dbproc) {
  return (long) dbproc;
 }
*/
import "C"

var connections map[int64]*Conn = make(map[int64]*Conn)

//Connection to the database.
type Conn struct {
	dbproc          *C.DBPROCESS
	addr            int64
	Error           string
	Message         string
	currentResult   *Result
	expiresFromPool time.Time
	belongsToPool   *ConnPool
	spParamsCache   map[string][]*spParam
	credentials
}

func (conn *Conn) addMessage(msg string) {
	if len(conn.Message) > 0 {
		conn.Message += "\n"
	}
	conn.Message += msg
	if conn.currentResult != nil {
		conn.currentResult.Message += msg
	}
}

func (conn *Conn) addError(err string) {
	if len(conn.Error) > 0 {
		conn.Error += "\n"
	}
	conn.Error += err
}

//Connect to the database with connection string, returns new connection or error.
//Example:
//  conn, err := NewConn("host=myServerA;database=myDataBase;user=myUsername;pwd=myPassword;mirror=myMirror")
//
//Mirror is optional, other params are mandatory.
func NewConn(connStr string) (*Conn, error) {
	return connectWithCredentials(NewCredentials(connStr))
}

func connectWithCredentials(crd *credentials) (*Conn, error) {
	conn := &Conn{
		spParamsCache: make(map[string][]*spParam),
		credentials:   *crd,
	}
	err := conn.reconnect()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (conn *Conn) connect() (*Conn, error) {
	//log.Printf("freetds connecting to %s@%s.%s", conn.user, conn.host, conn.database)
	conn.close()
	conn.clearMessages()
	dbproc, err := conn.getDbProc()
	if err != nil {
		return nil, err
	}
	conn.dbproc = dbproc
	conn.addr = int64(C.dbproc_addr(dbproc))
	connections[conn.addr] = conn
	if err := conn.DbUse(); err != nil {
		conn.close()
		return nil, err
	}
	if err := conn.setDefaults(); err != nil {
		conn.close()
		return nil, err
	}
	//log.Printf("freetds connected to %s@%s.%s", conn.user, conn.host, conn.database)
	return conn, nil
}

//If conn belongs to pool release connection to the pool.
//If not close connection.
func (conn *Conn) Close() {
	if conn.belongsToPool == nil {
		conn.close()
	} else {
		conn.belongsToPool.Release(conn)
	}
}

func (conn *Conn) close() {
	delete(connections, conn.addr)
	if conn.dbproc != nil {
		C.dbclose(conn.dbproc)
		C.dbexit()
		conn.dbproc = nil
		conn.addr = 0
	}
}

//ensure only one getDbProc at a time
var getDbProcMutex = &sync.Mutex{}

func (conn *Conn) getDbProc() (*C.DBPROCESS, error) {
	getDbProcMutex.Lock()
	defer getDbProcMutex.Unlock()
	erc := C.dbinit()
	if erc == C.FAIL {
		return nil, errors.New("cannot allocate an array of TDS_MAX_CONN TDSSOCKET pointers")
	}
	login := C.dblogin()
	if login == nil {
		return nil, errors.New("unable to allocate login structure")
	}
	cuser := C.CString(conn.user)
	defer C.free(unsafe.Pointer(cuser))
	cpwd := C.CString(conn.pwd)
	defer C.free(unsafe.Pointer(cpwd))
	C.my_dblogin(login, cuser, cpwd)

	chost := C.CString(conn.host)
	defer C.free(unsafe.Pointer(chost))
	dbproc := C.dbopen(login, chost)
	if dbproc == nil {
		return nil, dbProcError("dbopen error")
	}
	return dbproc, nil
}

func dbProcError(msg string) error {
	return fmt.Errorf("%s\n%s\n%s", msg, lastError, lastMessage)
}

//Change database.
func (conn *Conn) DbUse() error {
	if len(conn.database) > 0 {
		cdatabase := C.CString(conn.database)
		defer C.free(unsafe.Pointer(cdatabase))
		erc := C.dbuse(conn.dbproc, cdatabase)
		if erc == C.FAIL {
			return errors.New(fmt.Sprintf("unable to use database %s", conn.database))
		}
	}
	return nil
}

func (conn *Conn) clearMessages() {
	conn.Error = ""
	conn.Message = ""
}

//Execute sql query.
func (conn *Conn) Exec(sql string) ([]*Result, error) {
	results, err := conn.exec(sql)
	if err != nil && (conn.isDead() || conn.isMirrorSlave()) {
		if err := conn.reconnect(); err != nil {
			return nil, err
		}
		results, err = conn.exec(sql)

	}
	return results, err
}

func (conn *Conn) reconnect() error {
	var err error
	for i := 0; i < 2; i++ {
		if conn.isMirrorMessage() {
			conn.switchMirror()
		}
		_, err = conn.connect()
	}
	return err
}

func (conn *Conn) mirrorDefined() bool {
	return len(conn.mirrorHost) > 0
}

func (conn *Conn) isMirrorSlave() bool {
	if conn.isMirrorMessage() {
		return true
	}
	if defined, active, isMaster, err := conn.MirrorStatus(); err == nil {
		return defined && active && !isMaster
	}
	return false
}

func (conn *Conn) isMirrorMessage() bool {
	return strings.Contains(conn.Message, "It is acting as a mirror database") ||
		strings.Contains(conn.Message, "It is in the middle of a restore")
}

func (conn *Conn) switchMirror() {
	if !conn.mirrorDefined() {
		return
	}
	tmp := conn.host
	conn.host = conn.mirrorHost
	conn.mirrorHost = tmp
}

func (conn *Conn) exec(sql string) ([]*Result, error) {
	conn.clearMessages()

	cmd := C.CString(sql)
	defer C.free(unsafe.Pointer(cmd))

	if C.dbcmd(conn.dbproc, cmd) == C.FAIL {
		return nil, conn.raiseError("dbcmd failed")
	}
	if C.dbsqlexec(conn.dbproc) == C.FAIL {
		return nil, conn.raiseError("dbsqlexec failed")
	}
	return conn.fetchResults()
}

func (conn *Conn) isDead() bool {
	if conn.dbproc == nil {
		return true
	}
	return C.dbdead(conn.dbproc) == C.TRUE
}

func (conn *Conn) isLive() bool {
	results, err := conn.exec("select 1")
	if err != nil {
		return false
	}
	if results != nil {
		if results[0].Rows[0][0].(int32) == 1 {
			return true
		}
	}
	return false
}

//Begin database transaction.
func (conn *Conn) Begin() error {
	_, err := conn.Exec("begin transaction")
	return err
}

//Commit database transaction.
func (conn *Conn) Commit() error {
	_, err := conn.Exec("commit transaction")
	return err
}

//Rollback database transaction.
func (conn *Conn) Rollback() error {
	_, err := conn.Exec("if @@trancount > 0 rollback transaction")
	return err
}

//Query database and return first column in the first row as result.
func (conn *Conn) SelectValue(sql string) (interface{}, error) {
	results, err := conn.Exec(sql)
	if err != nil || results == nil {
		return nil, conn.raise(err)
	}
	if len(results[0].Rows) == 0 {
		return nil, errors.New("No rows in result.")
	}
	return results[0].Rows[0][0], nil
}

//Checking database mirroring status:
//  isDefined - is mirror defined (mirror parametar passed in connection string)
//  isActive  - is mirroring active for this database
//  isMaster  - is the current host master for this database
//Returns error if could not execute query to get current mirroring status.
func (conn *Conn) MirrorStatus() (bool, bool, bool, error) {
	if !conn.mirrorDefined() {
		return false, false, false, nil
	}
	rst, err := conn.exec(fmt.Sprintf(`
    SELECT
    	case when mirroring_guid is not null then 1 else 0 end mirroring_active,
    	case when mirroring_role = 2 then 0 else 1 end is_master, 
    	mirroring_state, mirroring_state_desc, mirroring_role, mirroring_role_desc,
      database_id, 
    	DB_NAME(database_id) database_name     	
    FROM sys.database_mirroring
    WHERE DB_NAME(database_id)='%s' 
  `, conn.database))
	if err != nil {
		return true, false, false, err
	}
	var active, isMaster bool
	rst[0].Next()
	err = rst[0].Scan(&active, &isMaster)
	return true, active, isMaster, err
}

func (conn *Conn) setDefaults() error {
	//defaults copied from .Net Driver
	_, err := conn.exec(`
    set quoted_identifier on
    set ansi_warnings on
    set ansi_padding on
    set concat_null_yields_null on
   `)
	return err
}
