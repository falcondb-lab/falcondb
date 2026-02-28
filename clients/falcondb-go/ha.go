package falcondb

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

// FailoverConfig configures the HA failover connection.
type FailoverConfig struct {
	Seeds          []SeedNode
	Database       string
	User           string
	Password       string
	ConnectTimeout time.Duration
	Autocommit     bool
	MaxRetries     int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// SeedNode is a host:port pair for a FalconDB cluster node.
type SeedNode struct {
	Host string
	Port int
}

// DefaultFailoverConfig returns a FailoverConfig with sensible defaults.
func DefaultFailoverConfig(seeds []SeedNode) FailoverConfig {
	return FailoverConfig{
		Seeds:          seeds,
		Database:       "falcon",
		User:           "falcon",
		Password:       "",
		ConnectTimeout: 10 * time.Second,
		Autocommit:     true,
		MaxRetries:     3,
		RetryBaseDelay: 100 * time.Millisecond,
		RetryMaxDelay:  5 * time.Second,
	}
}

// FailoverConn wraps a Conn with automatic failover across seed nodes.
//
// When a query fails with NOT_LEADER or FENCED_EPOCH, the driver
// automatically reconnects to the next seed node and retries.
type FailoverConn struct {
	cfg     FailoverConfig
	conn    *Conn
	seedIdx int
	mu      sync.Mutex
	closed  bool
}

// ConnectFailover creates a new FailoverConn, connecting to the first
// available seed node.
func ConnectFailover(ctx context.Context, cfg FailoverConfig) (*FailoverConn, error) {
	if len(cfg.Seeds) == 0 {
		return nil, fmt.Errorf("falcondb: at least one seed node is required")
	}
	fc := &FailoverConn{cfg: cfg}
	if err := fc.connectToAny(ctx); err != nil {
		return nil, err
	}
	return fc, nil
}

// Query executes a SQL query with automatic failover.
func (fc *FailoverConn) Query(ctx context.Context, sql string) (*Rows, error) {
	return withRetry(fc, ctx, func(c *Conn) (*Rows, error) {
		return c.Query(ctx, sql)
	})
}

// Exec executes a SQL statement with automatic failover.
func (fc *FailoverConn) Exec(ctx context.Context, sql string) (int64, error) {
	rows, err := fc.Query(ctx, sql)
	if err != nil {
		return 0, err
	}
	return rows.RowsAffected, nil
}

// Ping sends a health check to the current node.
func (fc *FailoverConn) Ping(ctx context.Context) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.closed || fc.conn == nil {
		return fmt.Errorf("falcondb: connection is closed")
	}
	return fc.conn.Ping(ctx)
}

// Epoch returns the current server epoch.
func (fc *FailoverConn) Epoch() uint64 {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.conn != nil {
		return fc.conn.Epoch()
	}
	return 0
}

// NodeID returns the current server node ID.
func (fc *FailoverConn) NodeID() uint64 {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.conn != nil {
		return fc.conn.NodeID()
	}
	return 0
}

// Close gracefully disconnects.
func (fc *FailoverConn) Close() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.closed {
		return nil
	}
	fc.closed = true
	if fc.conn != nil {
		return fc.conn.Close()
	}
	return nil
}

// ── Internal ────────────────────────────────────────────────────────────

func withRetry[T any](fc *FailoverConn, ctx context.Context, fn func(*Conn) (T, error)) (T, error) {
	var zero T
	var lastErr error

	for attempt := 0; attempt <= fc.cfg.MaxRetries; attempt++ {
		fc.mu.Lock()
		if fc.closed {
			fc.mu.Unlock()
			return zero, fmt.Errorf("falcondb: connection is closed")
		}

		if fc.conn == nil || fc.conn.IsClosed() {
			fc.mu.Unlock()
			if err := fc.connectToAny(ctx); err != nil {
				return zero, err
			}
			fc.mu.Lock()
		}

		conn := fc.conn
		fc.mu.Unlock()

		result, err := fn(conn)
		if err == nil {
			return result, nil
		}

		lastErr = err

		if fe, ok := err.(*FalconError); ok {
			if IsFailoverCode(fe.Code) {
				log.Printf("falcondb: failover triggered (code=%d, epoch=%d), reconnecting...",
					fe.Code, fe.ServerEpoch)
				fc.closeCurrent()
				fc.advanceSeed()
				delay := fc.retryDelay(attempt)
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return zero, ctx.Err()
				}
				continue
			}
			if IsRetryable(fe.Code) {
				log.Printf("falcondb: retryable error (code=%d), attempt %d/%d",
					fe.Code, attempt+1, fc.cfg.MaxRetries)
				delay := fc.retryDelay(attempt)
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return zero, ctx.Err()
				}
				continue
			}
		}

		// Non-retryable error
		return zero, err
	}

	return zero, lastErr
}

func (fc *FailoverConn) connectToAny(ctx context.Context) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	var errors []string
	for range fc.cfg.Seeds {
		seed := fc.cfg.Seeds[fc.seedIdx]
		conn, err := Connect(ctx, Config{
			Host:           seed.Host,
			Port:           seed.Port,
			Database:       fc.cfg.Database,
			User:           fc.cfg.User,
			Password:       fc.cfg.Password,
			ConnectTimeout: fc.cfg.ConnectTimeout,
			Autocommit:     fc.cfg.Autocommit,
		})
		if err == nil {
			fc.conn = conn
			log.Printf("falcondb: connected to %s:%d (epoch=%d)", seed.Host, seed.Port, conn.Epoch())
			return nil
		}
		errors = append(errors, fmt.Sprintf("%s:%d -> %v", seed.Host, seed.Port, err))
		fc.seedIdx = (fc.seedIdx + 1) % len(fc.cfg.Seeds)
	}
	return fmt.Errorf("falcondb: cannot connect to any seed node: %v", errors)
}

func (fc *FailoverConn) advanceSeed() {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.seedIdx = (fc.seedIdx + 1) % len(fc.cfg.Seeds)
}

func (fc *FailoverConn) closeCurrent() {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.conn != nil {
		fc.conn.Close()
		fc.conn = nil
	}
}

func (fc *FailoverConn) retryDelay(attempt int) time.Duration {
	delay := float64(fc.cfg.RetryBaseDelay) * math.Pow(2, float64(attempt))
	if delay > float64(fc.cfg.RetryMaxDelay) {
		delay = float64(fc.cfg.RetryMaxDelay)
	}
	return time.Duration(delay)
}
