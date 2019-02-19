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
//  pool, err := NewConnPool("host=myServerA;database=myDataBase;user=myUsername;pwd=myPassword")
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
	done          chan bool
	poolGuard     chan bool
	poolMutex     sync.Mutex
	cleanupTicker *time.Ticker
	connCount     int

	spParamsCache *ParamsCache
}

//NewCoonPool creates new connection pool.
//Connection will be created using provided connection string.
//Max number of connections in the pool is controlled by max_pool_size connection string parameter, default is 100.
//
//New connections will be created when needed.
//There is always one connection in the pool.
//
//Returns err if fails to create initial connection.
//Valid connection string examples:
//   "host=myServerA;database=myDataBase;user=myUsername;pwd=myPassword;"
//   "host=myServerA;database=myDataBase;user=myUsername;pwd=myPassword;max_pool_size=500"
//   "host=myServerA;database=myDataBase;user=myUsername;pwd=myPassword;mirror=myMirror"
func NewConnPool(connStr string) (*ConnPool, error) {
	p := &ConnPool{
		connStr:       connStr,
		pool:          []*Conn{},
		cleanupTicker: time.NewTicker(poolCleanupInterval),
		connCount:     0,
		spParamsCache: NewParamsCache(),
		done:          make(chan bool, 1),
	}
	conn, err := p.newConn()
	if err != nil {
		return nil, err
	}
	p.maxConn = conn.maxPoolSize
	p.poolGuard = make(chan bool, p.maxConn)
	p.addToPool(conn)
	go func() {
		for {
			select {
			case <-p.cleanupTicker.C:
				p.cleanup()
			case <-p.done:
				return
			}
		}
	}()
	return p, nil
}

func (p *ConnPool) newConn() (*Conn, error) {
	conn, err := NewConn(p.connStr)
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
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
	conn, err := p.newConn()
	if err != nil {
		<-p.poolGuard //remove reservation
		return nil, err
	}
	return conn, nil
}

//Get connection from pool and execute handler.
//Release connection after handler is called.
func (p *ConnPool) Do(handler func(*Conn) error) error {
	conn, err := p.Get()
	if err != nil {
		return err
	}
	defer conn.Close()
	return handler(conn)
}

//Get new connection from pool, and execute handler in transaction.
//If handler returns error transaction will be rolled back.
//Release connection after handerl is called.
func (p *ConnPool) DoInTransaction(handler func(*Conn) error) error {
	return p.Do(func(conn *Conn) error {
		conn.Begin()
		err := handler(conn)
		if err != nil {
			conn.Rollback()
		} else {
			conn.Commit()
		}
		return err
	})
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
	} else {
		conn.close()
		p.connCount--
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
	close(p.done)
}

func (p *ConnPool) cleanup() {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	if len(p.pool) <= 1 {
		return
	}
	for i := len(p.pool) - 1; i >= 1; i-- {
		conn := p.pool[i]
		if conn.expiresFromPool.Before(time.Now()) {
			conn.close()
			p.connCount--
			p.pool = append(p.pool[:i], p.pool[i+1:]...)
		}
	}
}

//Statistic about connections in the pool.
func (p *ConnPool) Stat() (max, count, active int) {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	max = p.maxConn
	count = p.connCount
	inactive := len(p.pool)
	active = count - inactive
	return
}
