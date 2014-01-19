package freetds

import (
	"sync"
	"time"
)

var poolExpiresInterval = 5 * time.Minute
var poolCleanupInterval = time.Minute

//ConnPool - connection pool for the maxCount connections.
//
//Connection can be acquired from the pool by pool.Get().
//
//Release conn to the pool by caling conn.Close() or pool.Release(conn).
//
//Destroy pool and all connections by calling pool.Close().
//
//Connections will be removed from the pool if not active for poolExpiresInterval.
//But there is always one connection in the pool.
//
//Example:
//  pool, err := NewConnPool("host=myServerA;database=myDataBase;user=myUsername;pwd=myPassword", 100)
//  ...
//  conn, err := pool.Get()
//  //use conn
//  conn.Close()
//  ...
//  pool.Close()
type ConnPool struct {
	connStr       string
	maxConn       int
	pool          []*Conn
	poolGuard     chan bool
	poolMutex     sync.Mutex
	cleanupTicker *time.Ticker
	connCount     int
	spParamsCache map[string][]*spParam
}

//NewCoonPool creates new connection pool.
//Connection will be created using provided connection string.
//MaxConn is max number of connections in the pool.
//
//New connections will be created when needed.
//There is always one connection in the pool.
//
//Returns err if fails to create initial connection.
func NewConnPool(connStr string, maxConn int) (*ConnPool, error) {
	p := &ConnPool{
		connStr:       connStr,
		maxConn:       maxConn,
		pool:          []*Conn{},
		poolGuard:     make(chan bool, maxConn),
		cleanupTicker: time.NewTicker(poolCleanupInterval),
		connCount:     0,
		spParamsCache: make(map[string][]*spParam),
	}
	conn, err := p.newConn()
	if err != nil {
		return nil, err
	}
	p.addToPool(conn)
	go func() {
		for _ = range p.cleanupTicker.C {
			p.cleanup()
		}
	}()
	return p, nil
}

func (p *ConnPool) newConn() (*Conn, error) {
	conn, err := ConnectWithConnectionString(p.connStr)
	if err == nil {
		conn.belongsToPool = p
		//share stored procedure params cache between connections in the pool
		conn.spParamsCache = p.spParamsCache
		p.connCount++
	}
	return conn, err
}

//Get returns connection from the pool.
//Blocks if there are no free connections, and maxConn is reached.
func (p *ConnPool) Get() (*Conn, error) {
	p.poolGuard <- true //make reservation, blocks if poolGuard is full
	conn := p.getPooled()
	if conn != nil {
		return conn, nil
	}
	return p.newConn()
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
	if !conn.isDead() {
		conn.expiresFromPool = time.Now().Add(poolExpiresInterval)
		//release to the top of the pool
		newPool := []*Conn{}
		newPool = append(newPool, conn)
		newPool = append(newPool, p.pool...)
		p.pool = newPool
	}
}

//Release connection to the pool.
func (p *ConnPool) Release(conn *Conn) {
	if conn.belongsToPool != p {
		return
	}
	p.addToPool(conn)
	<-p.poolGuard //remove reservation
}

//Close connection pool.
//Closes all existing connections in the pool.
func (p *ConnPool) Close() {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	for _, conn := range p.pool {
		conn.close()
	}
	p.pool = nil
}

func (p *ConnPool) cleanup() {
	if len(p.pool) <= 1 {
		return
	}
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	for i := len(p.pool) - 2; i >= 0; i-- {
		conn := p.pool[i]
		if conn.expiresFromPool.Before(time.Now()) {
			conn.close()
			p.connCount--
			p.pool = append(p.pool[:i], p.pool[i+1:]...)
		}
	}
}

//Statistic about connections in the pool.
func (p* ConnPool) Stat() (max, count, active int) {
	max = p.maxConn
	count = p.connCount
	inactive := len(p.pool)
	active = count - inactive
	return
}
