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
 dberrhandle(err_handler);
 dbmsghandle(msg_handler);
 DBSETLUSER(login, username);
 DBSETLPWD(login, password);
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
  erc := C.dbinit()
  if erc == C.FAIL {
    return nil, errors.New("cannot allocate an array of TDS_MAX_CONN TDSSOCKET pointers")
  }
  login := C.dblogin()
  if login == nil {
    return nil, errors.New("unable to allocate login structure")
  }
  cuser := C.CString(user)
  defer C.free(unsafe.Pointer(cuser))
  cpwd := C.CString(pwd)
  defer C.free(unsafe.Pointer(cpwd))
  C.my_dblogin(login, cuser, cpwd)

  chost := C.CString(host)
  defer C.free(unsafe.Pointer(chost))
  dbproc := C.dbopen(login, chost)
  if dbproc == nil {
    return nil, errors.New("dbopen error")
  }
  if len(database) > 0 {
    cdatabase := C.CString(database)
    defer C.free(unsafe.Pointer(cdatabase))
    erc = C.dbuse(dbproc, cdatabase)
    if erc == C.FAIL {
      C.dbclose(dbproc)
      return nil, errors.New(fmt.Sprintf("unable to use to database %s", database))
    }
  }
  conn := new(Conn)
  conn.dbproc = dbproc
  conn.addr = int64(C.dbproc_addr(dbproc))
//  fmt.Printf("Connect %d\n", conn.addr)
  connections[conn.addr] = conn
  return conn, nil
}

func (conn *Conn) Close() {
  delete(connections, conn.addr)
  if conn.dbproc != nil {
    C.dbclose(conn.dbproc)
//    C.dbexit()
    conn.dbproc = nil
  }
}

func (conn *Conn) Clear() {
  conn.Error = ""
  conn.Message = ""
}

func (conn *Conn) Exec(sql string) ([]*Result, error) {
  conn.Clear()
  if C.dbfcmd(conn.dbproc, C.CString(sql)) == C.FAIL {
    return nil, errors.New("dbfcmd failed")
  }
  if C.dbsqlexec(conn.dbproc) == C.FAIL {
    if len(conn.Error) != 0 {
      return nil, errors.New(conn.Error + conn.Message)
    } else {
      return nil, errors.New("dbsqlexec failed")
    }
  }
  return conn.fetchResults()
}
