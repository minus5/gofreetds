package freetds

import (
  "fmt"
  "errors"
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

 static int err_handler(DBPROCESS * dbproc, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr)
 {
   extern int ErrHandler(long dbprocAddr, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr);
   return ErrHandler((long)dbproc, severity, dberr, oserr, dberrstr, oserrstr);
 }

 static int msg_handler(DBPROCESS * dbproc, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname, int line)
 {
   extern int MsgHandler(long dbprocAddr, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname, int line);
   return MsgHandler((long)dbproc, msgno, msgstate, severity, msgtext, srvname, procname, line);
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

type Conn struct {
  dbproc *C.DBPROCESS
  addr int64
  Error string
  Message string
  currentResult *Result
  retries int
  user, pwd, host, database, mirrorHost string
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

func Connect(user, pwd, host, database string) (*Conn, error) {
  conn := &Conn{user: user, pwd:pwd, host:host, database: database, retries: 1}
  return conn.connect()
}

func Connect2(user, pwd, host, mirrorHost, database string) (*Conn, error) {
  conn := &Conn{user: user, pwd:pwd, host:host, database: database, retries: 1, mirrorHost: mirrorHost}
  return conn.connect()
}

func (conn *Conn) connect() (*Conn, error){
  conn.Close()
  dbproc, err := conn.getDbProc()
  if err != nil {
    return nil, err
  }
  conn.dbproc = dbproc
  conn.addr = int64(C.dbproc_addr(dbproc))
  connections[conn.addr] = conn
  return conn, nil
}

func (conn *Conn) Close() {
  delete(connections, conn.addr)
  if conn.dbproc != nil {
    C.dbclose(conn.dbproc)
    C.dbexit()
    conn.dbproc = nil
    conn.addr = 0
  }
}

func (conn *Conn) getDbProc() (*C.DBPROCESS, error) {
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
  if len(conn.database) > 0 {
    cdatabase := C.CString(conn.database)
    defer C.free(unsafe.Pointer(cdatabase))
    erc = C.dbuse(dbproc, cdatabase)
    if erc == C.FAIL {
      C.dbclose(dbproc)
      return nil, errors.New(fmt.Sprintf("unable to use to database %s", conn.database))
    }
  }
  return dbproc, nil
}

func (conn *Conn) clearMessages() {
  conn.Error = ""
  conn.Message = ""
}

func (conn *Conn) Exec(sql string) ([]*Result, error) {
  if conn.retries == 0 {
    return conn.exec(sql)
  }
  var err error
  for i:=0; i<=conn.retries; i++ {
    results, err := conn.exec(sql)
    if !(err != nil && conn.isDead()) {
      return results, err
    }
    _, err = conn.connect()
    if err != nil {
      return nil, err
    }
  }
  return nil, err
}

func (conn *Conn) exec(sql string) ([]*Result, error) {
  conn.clearMessages()
  if C.dbfcmd(conn.dbproc, C.CString(sql)) == C.FAIL {
    return nil, errors.New("dbfcmd failed")
  }
  if C.dbsqlexec(conn.dbproc) == C.FAIL {
    if len(conn.Error) != 0 {
      return nil, errors.New(conn.Error)
    } else {
      return nil, errors.New("dbsqlexec failed")
    }
  }
  rst, err := conn.fetchResults()
  if err == nil && len(conn.Error) > 0 {
    return rst, errors.New(conn.Error)
  }
  return rst, err
}

func (conn *Conn) isDead() bool {
  if conn.dbproc == nil {
    return true;
  }
  return C.dbdead(conn.dbproc) == C.TRUE
}

func (conn *Conn) IsLive() bool {
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

func PrintResults(results []*Result) {
  fmt.Printf("results %v", results)
  for _, r := range results {
    fmt.Printf("\n\nColums:\n")
    for j, c := range r.Columns {
      fmt.Printf("\t%3d%20s%10d%10d\n", j, c.Name, c.DbType, c.DbSize)
    }
    for i, _ := range r.Rows {
      for j, _ := range r.Columns {
        fmt.Printf("value[%2d, %2d]: %v\n", i, j, r.Rows[i][j])
      }
      fmt.Printf("\n")
    }
    fmt.Printf("rows affected: %d\n", r.RowsAffected)
    fmt.Printf("return value: %d\n", r.ReturnValue)
  }
}

func (conn *Conn) SelectValue(sql string) (interface{}, error){
  results, err := conn.Exec(sql)
  if err != nil || results == nil {
    return nil, errors.New(conn.Error + conn.Message)
  }
  if len(results[0].Rows) == 0 {
    return nil, errors.New("No rows in result.")
  }
  return results[0].Rows[0][0], nil
}