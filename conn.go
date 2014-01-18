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
  setenv("TDSPORT", "1433", 1);
  setenv("TDSVER", "8.0", 1);
  dbsetlogintime(10);

  dberrhandle(err_handler);
  dbmsghandle(msg_handler);
  DBSETLUSER(login, username);
  DBSETLPWD(login, password);
  dbsetlname(login, "UTF-8", DBSETCHARSET);
 }

 static long dbproc_addr(DBPROCESS * dbproc) {
  return (long) dbproc;
 }
*/
import "C"

var connections map[int64]*Conn = make(map[int64]*Conn)

type credentials struct {
	user, pwd, host, database, mirrorHost string
}

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

//Connect to the database, returns new connection or error.
func Connect(user, pwd, host, database string) (*Conn, error) {
	return connectWithCredentials(&credentials{user: user, pwd: pwd, host: host, database: database})
}

//Connect to the database with mirroring support, returns new connection or error.
func Connect2(user, pwd, host, mirrorHost, database string) (*Conn, error) {
	return connectWithCredentials(&credentials{user: user, pwd: pwd, host: host, database: database, mirrorHost: mirrorHost})
}

//Connect to the database with connection string, returns new connection or error.
//Example:
//  conn, err := ConnectWithConnectionString("host=myServerA;database=myDataBase;user=myUsername;pwd=myPassword;mirror=myMirror")
//
//Mirror is optional, other params are mandatory.
func ConnectWithConnectionString(connStr string) (*Conn, error) {
	return connectWithCredentials(parseConnectionString(connStr))
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
	err = conn.DbUse()
	if err != nil {
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
		return nil, errors.New("dbopen error")
	}
	return dbproc, nil
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
	if conn.isMirrorMessage() {
		err := conn.reconnect()
		if err != nil {
			return nil, err
		}
	}
	results, err := conn.exec(sql)
	if err != nil && conn.isDead() {
		err = conn.reconnect()
		if err != nil {
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

func (conn *Conn) isMirrorMessage() bool {
	return strings.Contains(conn.Message, "It is acting as a mirror database") ||
		strings.Contains(conn.Message, "It is in the middle of a restore")
}

func (conn *Conn) switchMirror() {
	if len(conn.mirrorHost) == 0 {
		return
	}
	tmp := conn.host
	conn.host = conn.mirrorHost
	conn.mirrorHost = tmp
}

func (conn *Conn) exec(sql string) ([]*Result, error) {
	conn.clearMessages()
	if C.dbcmd(conn.dbproc, C.CString(sql)) == C.FAIL {
		return nil, errors.New("dbcmd failed")
	}
	if C.dbsqlexec(conn.dbproc) == C.FAIL {
		if len(conn.Error) != 0 {
			return nil, errors.New(fmt.Sprintf("%s%s", conn.Error, conn.Message))
		} else {
			return nil, errors.New("dbsqlexec failed")
		}
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
		return nil, errors.New(conn.Error + conn.Message)
	}
	if len(results[0].Rows) == 0 {
		return nil, errors.New("No rows in result.")
	}
	return results[0].Rows[0][0], nil
}
