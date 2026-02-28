package falcondb

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// PoolConfig configures the connection pool.
type PoolConfig struct {
	Config             Config
	MinSize            int
	MaxSize            int
	HealthCheckInterval time.Duration
}

// DefaultPoolConfig returns a PoolConfig with sensible defaults.
func DefaultPoolConfig(cfg Config) PoolConfig {
	return PoolConfig{
		Config:             cfg,
		MinSize:            2,
		MaxSize:            10,
		HealthCheckInterval: 30 * time.Second,
	}
}

// Pool is a thread-safe connection pool for FalconDB.
type Pool struct {
	cfg     PoolConfig
	mu      sync.Mutex
	idle    []*pooledConn
	inUse   map[*Conn]struct{}
	closed  bool
}

type pooledConn struct {
	conn     *Conn
	lastUsed time.Time
}

// NewPool creates a new connection pool with pre-filled minimum connections.
func NewPool(ctx context.Context, cfg PoolConfig) (*Pool, error) {
	p := &Pool{
		cfg:   cfg,
		inUse: make(map[*Conn]struct{}),
	}
	for i := 0; i < cfg.MinSize; i++ {
		conn, err := Connect(ctx, cfg.Config)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("falcondb: pool init: %w", err)
		}
		p.idle = append(p.idle, &pooledConn{conn: conn, lastUsed: time.Now()})
	}
	return p, nil
}

// Acquire returns a connection from the pool.
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("falcondb: pool is closed")
	}

	// Try reusing an idle connection
	for len(p.idle) > 0 {
		pc := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		if p.isHealthy(ctx, pc) {
			p.inUse[pc.conn] = struct{}{}
			return pc.conn, nil
		}
		pc.conn.Close()
	}

	// Create a new connection if under limit
	if len(p.inUse) < p.cfg.MaxSize {
		conn, err := Connect(ctx, p.cfg.Config)
		if err != nil {
			return nil, err
		}
		p.inUse[conn] = struct{}{}
		return conn, nil
	}

	return nil, fmt.Errorf("falcondb: pool exhausted (%d/%d in use)", len(p.inUse), p.cfg.MaxSize)
}

// Release returns a connection to the pool.
func (p *Pool) Release(conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.inUse, conn)

	if p.closed || conn.IsClosed() {
		conn.Close()
		return
	}

	p.idle = append(p.idle, &pooledConn{conn: conn, lastUsed: time.Now()})
}

// Size returns the total number of connections (idle + in use).
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle) + len(p.inUse)
}

// IdleCount returns the number of idle connections.
func (p *Pool) IdleCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle)
}

// InUseCount returns the number of connections in use.
func (p *Pool) InUseCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.inUse)
}

// Close closes all connections and shuts down the pool.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, pc := range p.idle {
		pc.conn.Close()
	}
	p.idle = nil
}

func (p *Pool) isHealthy(ctx context.Context, pc *pooledConn) bool {
	if pc.conn.IsClosed() {
		return false
	}
	if time.Since(pc.lastUsed) < p.cfg.HealthCheckInterval {
		return true
	}
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return pc.conn.Ping(pingCtx) == nil
}
