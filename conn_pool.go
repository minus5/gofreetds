package freetds

import (
	"sync"
	"time"
)

var poolExpiresInterval = 5 * time.Minute
var poolCleanupInterval = time.Minute

type ConnPool struct {
	connStr string
	maxConn int
	pool []*Conn
  poolGuard chan bool
	poolMutex sync.Mutex
	cleanupTicker *time.Ticker
	connCount int
}

func NewConnPool (connStr string, maxConn int) (*ConnPool, error) {
	conn, err := ConnectWithConnectionString(connStr)
	if err != nil {
		return nil, err
	}
	p := &ConnPool{
		connStr: connStr, 
		maxConn: maxConn,
		pool: []*Conn{},
		poolGuard : make(chan bool, maxConn),
		cleanupTicker: time.NewTicker(poolCleanupInterval),
		connCount : 1,
	}
	p.addToPool(conn)
	go func() {
		for _ = range p.cleanupTicker.C {
			p.cleanup()
		}
	}()
	return p, nil
}

//get connection from the pool
//blocks if there are no free connections
func (p *ConnPool) Get() (*Conn, error) {
	p.poolGuard <- true //make reservation, blocks if poolGuard is full
	conn := p.getPooled()
	if conn != nil {
		return conn, nil
	}
	conn, err := ConnectWithConnectionString(p.connStr)
	if err == nil {
		p.connCount++
		return conn, nil
	}
	return nil, err
}

func (p *ConnPool) getPooled() *Conn {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	if len(p.pool) > 0 {
		conn := p.pool[0]
		if len(p.pool) > 1 {
			p.pool = p.pool[1:]
		} else {
			p.pool = []*Conn{}
		}
		return conn
	} 
	return nil
}

func (p *ConnPool) addToPool(conn *Conn) {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	if (!conn.isDead()) {
		conn.expiresFromPool = time.Now().Add(poolExpiresInterval)
		p.pool = append(p.pool, conn)
	}
}

//release connection
func (p *ConnPool) Release(conn *Conn) {
	p.addToPool(conn)
	<- p.poolGuard  //remove reservation
}

func (p *ConnPool) Close() {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	for _, c := range p.pool {
		c.Close()
	}
	p.pool = nil
}

func (p *ConnPool) cleanup() { 
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	if len(p.pool) <= 1 {
		return
	}
	for i:=len(p.pool)-2; i>=0; i-- {
		conn := p.pool[i]
		if conn.expiresFromPool.Before(time.Now()) {
			conn.Close()
			p.connCount--
			p.pool = append(p.pool[:i], p.pool[i+1:]...)
		}
	}
}
