// Package falcondb provides a native protocol driver for FalconDB
// with HA failover and connection pooling.
package falcondb

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ── Message type tags ───────────────────────────────────────────────────

const (
	msgClientHello   byte = 0x01
	msgServerHello   byte = 0x02
	msgAuthRequest   byte = 0x03
	msgAuthResponse  byte = 0x04
	msgAuthOK        byte = 0x05
	msgAuthFail      byte = 0x06
	msgQueryRequest  byte = 0x10
	msgQueryResponse byte = 0x11
	msgErrorResponse byte = 0x12
	msgBatchRequest  byte = 0x13
	msgBatchResponse byte = 0x14
	msgPing          byte = 0x20
	msgPong          byte = 0x21
	msgDisconnect    byte = 0x30
	msgDisconnectAck byte = 0x31
)

// ── Type IDs ────────────────────────────────────────────────────────────

const (
	TypeNull      byte = 0x00
	TypeBoolean   byte = 0x01
	TypeInt32     byte = 0x02
	TypeInt64     byte = 0x03
	TypeFloat64   byte = 0x04
	TypeText      byte = 0x05
	TypeTimestamp byte = 0x06
	TypeDate      byte = 0x07
	TypeJsonb     byte = 0x08
	TypeDecimal   byte = 0x09
	TypeTime      byte = 0x0A
	TypeInterval  byte = 0x0B
	TypeUUID      byte = 0x0C
	TypeBytea     byte = 0x0D
	TypeArray     byte = 0x0E
)

// ── Feature flags ───────────────────────────────────────────────────────

const (
	featureBatchIngest  uint64 = 1 << 2
	featurePipeline     uint64 = 1 << 3
	featureEpochFencing uint64 = 1 << 4
	featureTLS          uint64 = 1 << 5
	featureBinaryParams uint64 = 1 << 6
)

// ── Session flags ───────────────────────────────────────────────────────

const (
	sessionAutocommit uint32 = 1
	sessionReadOnly   uint32 = 1 << 1
)

const (
	maxFrameSize    = 64 * 1024 * 1024 // 64 MiB
	frameHeaderSize = 5
)

// ── Error codes ─────────────────────────────────────────────────────────

const (
	ErrSyntaxError           = 1000
	ErrInvalidParam          = 1001
	ErrNotLeader             = 2000
	ErrFencedEpoch           = 2001
	ErrReadOnly              = 2002
	ErrSerializationConflict = 2003
	ErrInternalError         = 3000
	ErrTimeout               = 3001
	ErrOverloaded            = 3002
	ErrAuthFailed            = 4000
	ErrPermissionDenied      = 4001
)

// IsRetryable returns true if the error code indicates the client should retry.
func IsRetryable(code int) bool {
	switch code {
	case ErrNotLeader, ErrFencedEpoch, ErrReadOnly,
		ErrSerializationConflict, ErrTimeout, ErrOverloaded:
		return true
	}
	return false
}

// IsFailoverCode returns true if the error requires connecting to a different node.
func IsFailoverCode(code int) bool {
	switch code {
	case ErrNotLeader, ErrFencedEpoch, ErrReadOnly:
		return true
	}
	return false
}

// ── Error type ──────────────────────────────────────────────────────────

// FalconError represents an error returned by the FalconDB server.
type FalconError struct {
	Code        int
	SQLState    string
	Message     string
	Retryable   bool
	ServerEpoch uint64
}

func (e *FalconError) Error() string {
	return fmt.Sprintf("falcondb error %d (%s): %s", e.Code, e.SQLState, e.Message)
}

// ── Column metadata ─────────────────────────────────────────────────────

// ColumnMeta describes a result column.
type ColumnMeta struct {
	Name      string
	TypeID    byte
	Nullable  bool
	Precision int
	Scale     int
}

// ── Query result ────────────────────────────────────────────────────────

// Rows holds the result of a query.
type Rows struct {
	Columns      []ColumnMeta
	Data         [][]interface{}
	RowsAffected int64
	pos          int
}

// Next advances to the next row. Returns false when exhausted.
func (r *Rows) Next() bool {
	if r.pos < len(r.Data) {
		r.pos++
		return true
	}
	return false
}

// Values returns the current row's values.
func (r *Rows) Values() []interface{} {
	if r.pos == 0 || r.pos > len(r.Data) {
		return nil
	}
	return r.Data[r.pos-1]
}

// Close is a no-op (all rows are materialized in memory).
func (r *Rows) Close() error {
	return nil
}

// Reset rewinds the cursor to the beginning.
func (r *Rows) Reset() {
	r.pos = 0
}

// ── Config ──────────────────────────────────────────────────────────────

// Config holds connection parameters.
type Config struct {
	Host           string
	Port           int
	Database       string
	User           string
	Password       string
	ConnectTimeout time.Duration
	TLSConfig      *tls.Config // nil = no TLS
	Autocommit     bool
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Host:           "localhost",
		Port:           6543,
		Database:       "falcon",
		User:           "falcon",
		Password:       "",
		ConnectTimeout: 10 * time.Second,
		Autocommit:     true,
	}
}

// ── Connection ──────────────────────────────────────────────────────────

// Conn is a single connection to a FalconDB server.
type Conn struct {
	conn       net.Conn
	cfg        Config
	reqID      atomic.Uint64
	epoch      uint64
	nodeID     uint64
	features   uint64
	closed     bool
	mu         sync.Mutex
}

// Connect establishes a new connection to a FalconDB server.
func Connect(ctx context.Context, cfg Config) (*Conn, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	dialer := net.Dialer{Timeout: cfg.ConnectTimeout}
	netConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("falcondb: connect %s: %w", addr, err)
	}
	if tc, ok := netConn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	c := &Conn{conn: netConn, cfg: cfg}

	if err := c.handshake(); err != nil {
		netConn.Close()
		return nil, err
	}

	if cfg.TLSConfig != nil && (c.features&featureTLS) != 0 {
		tlsCfg := cfg.TLSConfig.Clone()
		if tlsCfg.ServerName == "" {
			tlsCfg.ServerName = cfg.Host
		}
		tlsConn := tls.Client(netConn, tlsCfg)
		if err := tlsConn.Handshake(); err != nil {
			netConn.Close()
			return nil, fmt.Errorf("falcondb: TLS handshake: %w", err)
		}
		c.conn = tlsConn
	}

	if err := c.authenticate(); err != nil {
		c.conn.Close()
		return nil, err
	}

	return c, nil
}

// Epoch returns the server's epoch at connection time.
func (c *Conn) Epoch() uint64 { return c.epoch }

// NodeID returns the server's node ID.
func (c *Conn) NodeID() uint64 { return c.nodeID }

// IsClosed returns whether the connection has been closed.
func (c *Conn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Query executes a SQL statement and returns the result rows.
func (c *Conn) Query(ctx context.Context, sql string) (*Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("falcondb: connection is closed")
	}
	return c.executeQuery(sql)
}

// Exec executes a SQL statement and returns the number of rows affected.
func (c *Conn) Exec(ctx context.Context, sql string) (int64, error) {
	rows, err := c.Query(ctx, sql)
	if err != nil {
		return 0, err
	}
	return rows.RowsAffected, nil
}

// Ping sends a Ping and waits for Pong.
func (c *Conn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("falcondb: connection is closed")
	}
	if err := c.sendFrame(msgPing, nil); err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if ok {
		_ = c.conn.SetReadDeadline(deadline)
		defer c.conn.SetReadDeadline(time.Time{})
	}
	msgType, _, err := c.recvFrame()
	if err != nil {
		return err
	}
	if msgType != msgPong {
		return fmt.Errorf("falcondb: expected Pong, got 0x%02x", msgType)
	}
	return nil
}

// Close gracefully disconnects.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	_ = c.sendFrame(msgDisconnect, nil)
	_ = c.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	c.recvFrame() // try to read DisconnectAck, ignore errors
	return c.conn.Close()
}

// ── Internal: query execution ───────────────────────────────────────────

func (c *Conn) executeQuery(sql string) (*Rows, error) {
	reqID := c.reqID.Add(1)
	flags := uint32(0)
	if c.cfg.Autocommit {
		flags = sessionAutocommit
	}

	w := newWriter(256)
	w.u64(reqID)
	w.u64(c.epoch)
	w.stringU32(sql)
	w.u16(0) // no params
	w.u32(flags)

	if err := c.sendFrame(msgQueryRequest, w.bytes()); err != nil {
		return nil, err
	}

	msgType, payload, err := c.recvFrame()
	if err != nil {
		return nil, err
	}
	if msgType == msgErrorResponse {
		return nil, parseError(payload)
	}
	if msgType != msgQueryResponse {
		return nil, fmt.Errorf("falcondb: expected QueryResponse, got 0x%02x", msgType)
	}

	r := newReader(payload)
	_ = r.u64() // request_id
	numCols := r.u16()

	columns := make([]ColumnMeta, numCols)
	colTypes := make([]byte, numCols)
	for i := 0; i < int(numCols); i++ {
		columns[i] = ColumnMeta{
			Name:      r.stringU16(),
			TypeID:    r.u8(),
			Nullable:  r.u8() != 0,
			Precision: int(r.u16()),
			Scale:     int(r.u16()),
		}
		colTypes[i] = columns[i].TypeID
	}

	numRows := r.u32()
	data := make([][]interface{}, numRows)
	for i := 0; i < int(numRows); i++ {
		data[i] = decodeRow(r, colTypes)
	}
	rowsAffected := r.u64()

	return &Rows{
		Columns:      columns,
		Data:         data,
		RowsAffected: int64(rowsAffected),
	}, nil
}

// ── Internal: handshake + auth ──────────────────────────────────────────

func (c *Conn) handshake() error {
	w := newWriter(256)
	w.u16(0) // version major
	w.u16(1) // version minor
	features := featureBatchIngest | featurePipeline | featureEpochFencing | featureBinaryParams
	if c.cfg.TLSConfig != nil {
		features |= featureTLS
	}
	w.u64(features)
	w.stringU16("falcondb-go/0.2")
	w.stringU16(c.cfg.Database)
	w.stringU16(c.cfg.User)
	nonce := make([]byte, 16)
	rand.Read(nonce)
	w.raw(nonce)
	w.u16(0) // num_params

	if err := c.sendFrame(msgClientHello, w.bytes()); err != nil {
		return err
	}

	// ServerHello
	msgType, payload, err := c.recvFrame()
	if err != nil {
		return err
	}
	if msgType == msgErrorResponse {
		return parseError(payload)
	}
	if msgType != msgServerHello {
		return fmt.Errorf("falcondb: expected ServerHello, got 0x%02x", msgType)
	}

	r := newReader(payload)
	_ = r.u16() // server major
	_ = r.u16() // server minor
	c.features = r.u64()
	c.epoch = r.u64()
	c.nodeID = r.u64()
	_ = r.raw(16) // server nonce
	numParams := r.u16()
	for i := 0; i < int(numParams); i++ {
		_ = r.stringU16() // key
		_ = r.stringU16() // value
	}

	// AuthRequest
	msgType, payload, err = c.recvFrame()
	if err != nil {
		return err
	}
	if msgType != msgAuthRequest {
		return fmt.Errorf("falcondb: expected AuthRequest, got 0x%02x", msgType)
	}
	// consume auth method + challenge
	return nil
}

func (c *Conn) authenticate() error {
	w := newWriter(64)
	w.u8(0) // AUTH_PASSWORD
	w.raw([]byte(c.cfg.Password))

	if err := c.sendFrame(msgAuthResponse, w.bytes()); err != nil {
		return err
	}

	msgType, payload, err := c.recvFrame()
	if err != nil {
		return err
	}
	if msgType == msgAuthOK {
		return nil
	}
	if msgType == msgAuthFail {
		reason := ""
		if len(payload) > 0 {
			r := newReader(payload)
			reason = r.stringU16()
		}
		return &FalconError{Code: ErrAuthFailed, SQLState: "28P01", Message: "authentication failed: " + reason}
	}
	return fmt.Errorf("falcondb: unexpected auth response: 0x%02x", msgType)
}

// ── Internal: framing ───────────────────────────────────────────────────

func (c *Conn) sendFrame(msgType byte, payload []byte) error {
	header := [frameHeaderSize]byte{msgType}
	binary.LittleEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := c.conn.Write(header[:]); err != nil {
		return fmt.Errorf("falcondb: write frame header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			return fmt.Errorf("falcondb: write frame payload: %w", err)
		}
	}
	return nil
}

func (c *Conn) recvFrame() (byte, []byte, error) {
	header := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return 0, nil, fmt.Errorf("falcondb: read frame header: %w", err)
	}
	msgType := header[0]
	length := binary.LittleEndian.Uint32(header[1:])
	if length > maxFrameSize {
		return 0, nil, fmt.Errorf("falcondb: frame too large: %d", length)
	}
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			return 0, nil, fmt.Errorf("falcondb: read frame payload: %w", err)
		}
	}
	return msgType, payload, nil
}

func parseError(payload []byte) *FalconError {
	r := newReader(payload)
	_ = r.u64() // request_id
	code := int(r.u32())
	sqlstate := string(r.raw(5))
	retryable := r.u8() != 0
	serverEpoch := r.u64()
	message := r.stringU16()
	return &FalconError{
		Code:        code,
		SQLState:    sqlstate,
		Message:     message,
		Retryable:   retryable,
		ServerEpoch: serverEpoch,
	}
}

// ── Internal: wire encode/decode ────────────────────────────────────────

type writer struct {
	buf []byte
}

func newWriter(cap int) *writer {
	return &writer{buf: make([]byte, 0, cap)}
}

func (w *writer) u8(v byte)   { w.buf = append(w.buf, v) }
func (w *writer) u16(v uint16) { w.buf = binary.LittleEndian.AppendUint16(w.buf, v) }
func (w *writer) u32(v uint32) { w.buf = binary.LittleEndian.AppendUint32(w.buf, v) }
func (w *writer) u64(v uint64) { w.buf = binary.LittleEndian.AppendUint64(w.buf, v) }
func (w *writer) i32(v int32)  { w.u32(uint32(v)) }
func (w *writer) i64(v int64)  { w.u64(uint64(v)) }
func (w *writer) f64(v float64) { w.u64(math.Float64bits(v)) }
func (w *writer) raw(data []byte) { w.buf = append(w.buf, data...) }

func (w *writer) stringU16(s string) {
	b := []byte(s)
	w.u16(uint16(len(b)))
	w.buf = append(w.buf, b...)
}

func (w *writer) stringU32(s string) {
	b := []byte(s)
	w.u32(uint32(len(b)))
	w.buf = append(w.buf, b...)
}

func (w *writer) bytes() []byte { return w.buf }

type reader struct {
	data []byte
	pos  int
}

func newReader(data []byte) *reader {
	return &reader{data: data}
}

func (r *reader) u8() byte {
	v := r.data[r.pos]
	r.pos++
	return v
}

func (r *reader) u16() uint16 {
	v := binary.LittleEndian.Uint16(r.data[r.pos:])
	r.pos += 2
	return v
}

func (r *reader) u32() uint32 {
	v := binary.LittleEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return v
}

func (r *reader) u64() uint64 {
	v := binary.LittleEndian.Uint64(r.data[r.pos:])
	r.pos += 8
	return v
}

func (r *reader) i32() int32 { return int32(r.u32()) }
func (r *reader) i64() int64 { return int64(r.u64()) }
func (r *reader) f64() float64 { return math.Float64frombits(r.u64()) }

func (r *reader) raw(n int) []byte {
	v := r.data[r.pos : r.pos+n]
	r.pos += n
	return v
}

func (r *reader) stringU16() string {
	length := int(r.u16())
	s := string(r.data[r.pos : r.pos+length])
	r.pos += length
	return s
}

func (r *reader) stringU32() string {
	length := int(r.u32())
	s := string(r.data[r.pos : r.pos+length])
	r.pos += length
	return s
}

func decodeValue(r *reader, typeID byte) interface{} {
	switch typeID {
	case TypeBoolean:
		return r.u8() != 0
	case TypeInt32:
		return r.i32()
	case TypeInt64:
		return r.i64()
	case TypeFloat64:
		return r.f64()
	case TypeText, TypeJsonb:
		return r.stringU32()
	case TypeTimestamp, TypeTime:
		return r.i64()
	case TypeDate:
		return r.i32()
	case TypeBytea:
		length := r.u32()
		return r.raw(int(length))
	default:
		return r.stringU32()
	}
}

func decodeRow(r *reader, colTypes []byte) []interface{} {
	ncols := len(colTypes)
	bitmapBytes := (ncols + 7) / 8
	bitmap := r.raw(bitmapBytes)
	values := make([]interface{}, ncols)
	for i := 0; i < ncols; i++ {
		isNull := (bitmap[i/8] & (1 << (i % 8))) != 0
		if isNull {
			values[i] = nil
		} else {
			values[i] = decodeValue(r, colTypes[i])
		}
	}
	return values
}
