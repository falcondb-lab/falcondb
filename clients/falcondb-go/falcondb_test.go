package falcondb

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestWriterU8(t *testing.T) {
	w := newWriter(1)
	w.u8(0x42)
	if w.bytes()[0] != 0x42 {
		t.Fatalf("expected 0x42, got 0x%02x", w.bytes()[0])
	}
}

func TestWriterU16LE(t *testing.T) {
	w := newWriter(2)
	w.u16(0x0102)
	got := binary.LittleEndian.Uint16(w.bytes())
	if got != 0x0102 {
		t.Fatalf("expected 0x0102, got 0x%04x", got)
	}
}

func TestWriterU32LE(t *testing.T) {
	w := newWriter(4)
	w.u32(0x04030201)
	got := binary.LittleEndian.Uint32(w.bytes())
	if got != 0x04030201 {
		t.Fatalf("expected 0x04030201, got 0x%08x", got)
	}
}

func TestWriterU64LE(t *testing.T) {
	w := newWriter(8)
	w.u64(1)
	got := binary.LittleEndian.Uint64(w.bytes())
	if got != 1 {
		t.Fatalf("expected 1, got %d", got)
	}
}

func TestWriterI32Negative(t *testing.T) {
	w := newWriter(4)
	w.i32(-1)
	r := newReader(w.bytes())
	if r.i32() != -1 {
		t.Fatal("expected -1")
	}
}

func TestWriterF64(t *testing.T) {
	w := newWriter(8)
	w.f64(3.14)
	r := newReader(w.bytes())
	got := r.f64()
	if math.Abs(got-3.14) > 1e-10 {
		t.Fatalf("expected 3.14, got %f", got)
	}
}

func TestWriterStringU16(t *testing.T) {
	w := newWriter(32)
	w.stringU16("hello")
	r := newReader(w.bytes())
	got := r.stringU16()
	if got != "hello" {
		t.Fatalf("expected 'hello', got '%s'", got)
	}
}

func TestWriterStringU32(t *testing.T) {
	w := newWriter(32)
	w.stringU32("world")
	r := newReader(w.bytes())
	got := r.stringU32()
	if got != "world" {
		t.Fatalf("expected 'world', got '%s'", got)
	}
}

func TestWriterStringUnicode(t *testing.T) {
	w := newWriter(64)
	w.stringU32("你好世界")
	r := newReader(w.bytes())
	got := r.stringU32()
	if got != "你好世界" {
		t.Fatalf("expected '你好世界', got '%s'", got)
	}
}

func TestDecodeValueBool(t *testing.T) {
	w := newWriter(1)
	w.u8(1)
	r := newReader(w.bytes())
	v := decodeValue(r, TypeBoolean)
	if v != true {
		t.Fatal("expected true")
	}
}

func TestDecodeValueInt32(t *testing.T) {
	w := newWriter(4)
	w.i32(-42)
	r := newReader(w.bytes())
	v := decodeValue(r, TypeInt32)
	if v != int32(-42) {
		t.Fatalf("expected -42, got %v", v)
	}
}

func TestDecodeValueInt64(t *testing.T) {
	w := newWriter(8)
	w.i64(1 << 40)
	r := newReader(w.bytes())
	v := decodeValue(r, TypeInt64)
	if v != int64(1<<40) {
		t.Fatalf("expected %d, got %v", int64(1<<40), v)
	}
}

func TestDecodeValueFloat64(t *testing.T) {
	w := newWriter(8)
	w.f64(2.718)
	r := newReader(w.bytes())
	v := decodeValue(r, TypeFloat64)
	f, ok := v.(float64)
	if !ok || math.Abs(f-2.718) > 1e-10 {
		t.Fatalf("expected 2.718, got %v", v)
	}
}

func TestDecodeValueText(t *testing.T) {
	w := newWriter(32)
	w.stringU32("hello")
	r := newReader(w.bytes())
	v := decodeValue(r, TypeText)
	if v != "hello" {
		t.Fatalf("expected 'hello', got %v", v)
	}
}

func TestDecodeValueBytea(t *testing.T) {
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	w := newWriter(16)
	w.u32(uint32(len(data)))
	w.raw(data)
	r := newReader(w.bytes())
	v := decodeValue(r, TypeBytea)
	b, ok := v.([]byte)
	if !ok || len(b) != 4 || b[0] != 0xDE {
		t.Fatalf("expected deadbeef, got %v", v)
	}
}

func TestDecodeRowWithNulls(t *testing.T) {
	colTypes := []byte{TypeInt64, TypeText, TypeBoolean}

	w := newWriter(64)
	// null bitmap: 1 byte, bit 1 set (second column is null)
	w.u8(0x02)
	// col 0: int64 = 42
	w.i64(42)
	// col 1: null (skipped)
	// col 2: bool = true
	w.u8(1)

	r := newReader(w.bytes())
	row := decodeRow(r, colTypes)

	if row[0] != int64(42) {
		t.Fatalf("expected 42, got %v", row[0])
	}
	if row[1] != nil {
		t.Fatalf("expected nil, got %v", row[1])
	}
	if row[2] != true {
		t.Fatalf("expected true, got %v", row[2])
	}
}

func TestDecodeRowAllNulls(t *testing.T) {
	colTypes := []byte{TypeInt32, TypeText}
	w := newWriter(1)
	w.u8(0x03) // both bits set
	r := newReader(w.bytes())
	row := decodeRow(r, colTypes)
	if row[0] != nil || row[1] != nil {
		t.Fatalf("expected all nil, got %v", row)
	}
}

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		code     int
		expected bool
	}{
		{ErrSyntaxError, false},
		{ErrNotLeader, true},
		{ErrFencedEpoch, true},
		{ErrSerializationConflict, true},
		{ErrTimeout, true},
		{ErrOverloaded, true},
		{ErrInternalError, false},
		{ErrAuthFailed, false},
	}
	for _, tc := range cases {
		if got := IsRetryable(tc.code); got != tc.expected {
			t.Errorf("IsRetryable(%d) = %v, want %v", tc.code, got, tc.expected)
		}
	}
}

func TestIsFailoverCode(t *testing.T) {
	cases := []struct {
		code     int
		expected bool
	}{
		{ErrNotLeader, true},
		{ErrFencedEpoch, true},
		{ErrReadOnly, true},
		{ErrSerializationConflict, false},
		{ErrTimeout, false},
	}
	for _, tc := range cases {
		if got := IsFailoverCode(tc.code); got != tc.expected {
			t.Errorf("IsFailoverCode(%d) = %v, want %v", tc.code, got, tc.expected)
		}
	}
}

func TestFalconErrorString(t *testing.T) {
	e := &FalconError{Code: 1000, SQLState: "42601", Message: "parse error"}
	s := e.Error()
	if s != "falcondb error 1000 (42601): parse error" {
		t.Fatalf("unexpected error string: %s", s)
	}
}

func TestRowsIteration(t *testing.T) {
	rows := &Rows{
		Columns: []ColumnMeta{
			{Name: "id", TypeID: TypeInt64},
			{Name: "name", TypeID: TypeText},
		},
		Data: [][]interface{}{
			{int64(1), "alice"},
			{int64(2), "bob"},
		},
	}

	count := 0
	for rows.Next() {
		v := rows.Values()
		if v == nil {
			t.Fatal("expected non-nil values")
		}
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}

	// After exhaustion
	if rows.Next() {
		t.Fatal("expected no more rows")
	}

	// Reset
	rows.Reset()
	if !rows.Next() {
		t.Fatal("expected rows after reset")
	}
}

func TestParseError(t *testing.T) {
	w := newWriter(64)
	w.u64(1)                // request_id
	w.u32(2001)             // error_code = FENCED_EPOCH
	w.raw([]byte("F0001")) // sqlstate
	w.u8(1)                 // retryable
	w.u64(42)               // server_epoch
	w.stringU16("stale epoch")

	err := parseError(w.bytes())
	if err.Code != 2001 {
		t.Fatalf("expected code 2001, got %d", err.Code)
	}
	if err.SQLState != "F0001" {
		t.Fatalf("expected F0001, got %s", err.SQLState)
	}
	if !err.Retryable {
		t.Fatal("expected retryable")
	}
	if err.ServerEpoch != 42 {
		t.Fatalf("expected epoch 42, got %d", err.ServerEpoch)
	}
	if err.Message != "stale epoch" {
		t.Fatalf("expected 'stale epoch', got '%s'", err.Message)
	}
}
