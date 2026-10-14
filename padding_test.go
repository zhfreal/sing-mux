package mux

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
)

type pipeConn struct {
	net.Conn
	reader io.Reader
	writer io.Writer
}

func (p *pipeConn) Read(b []byte) (int, error) {
	return p.reader.Read(b)
}

func (p *pipeConn) Write(b []byte) (int, error) {
	return p.writer.Write(b)
}

func (p *pipeConn) Close() error {
	return nil
}

func (p *pipeConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}

func (p *pipeConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 54321}
}

func (p *pipeConn) SetDeadline(t time.Time) error      { return nil }
func (p *pipeConn) SetReadDeadline(t time.Time) error  { return nil }
func (p *pipeConn) SetWriteDeadline(t time.Time) error { return nil }

func TestPaddingZeroDataFrames(t *testing.T) {
	// Construct a stream of frames:
	// Frame 1: originalDataSize = 0, paddingLen = 10 (Zero-length frame)
	// Frame 2: originalDataSize = 5 ("hello"), paddingLen = 8
	var wire bytes.Buffer

	// Frame 1
	var hdr1 [4]byte
	binary.BigEndian.PutUint16(hdr1[:2], 0)
	binary.BigEndian.PutUint16(hdr1[2:], 10)
	wire.Write(hdr1[:])
	wire.Write(bytes.Repeat([]byte{0xAA}, 10))

	// Frame 2
	var hdr2 [4]byte
	binary.BigEndian.PutUint16(hdr2[:2], 5)
	binary.BigEndian.PutUint16(hdr2[2:], 8)
	wire.Write(hdr2[:])
	wire.WriteString("hello")
	wire.Write(bytes.Repeat([]byte{0xBB}, 8))

	mockConn := &pipeConn{
		reader: &wire,
		writer: io.Discard,
	}

	pConn := newPaddingConn(mockConn)

	// Read should transparently skip the zero-data frame and return "hello"
	buf := make([]byte, 16)
	n, err := pConn.Read(buf)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(buf[:n]))
	}
}

func TestPaddingReadBufferZeroDataFrames(t *testing.T) {
	var wire bytes.Buffer

	// Frame 1: zero data
	var hdr1 [4]byte
	binary.BigEndian.PutUint16(hdr1[:2], 0)
	binary.BigEndian.PutUint16(hdr1[2:], 15)
	wire.Write(hdr1[:])
	wire.Write(bytes.Repeat([]byte{0xCC}, 15))

	// Frame 2: data "world"
	var hdr2 [4]byte
	binary.BigEndian.PutUint16(hdr2[:2], 5)
	binary.BigEndian.PutUint16(hdr2[2:], 6)
	wire.Write(hdr2[:])
	wire.WriteString("world")
	wire.Write(bytes.Repeat([]byte{0xDD}, 6))

	mockConn := &pipeConn{
		reader: &wire,
		writer: io.Discard,
	}

	pConn := newPaddingConn(mockConn)

	b := buf.NewSize(32)
	defer b.Release()

	pExtended, ok := pConn.(interface{ ReadBuffer(b *buf.Buffer) error })
	if !ok {
		t.Fatalf("paddingConn does not implement ReadBuffer")
	}

	err := pExtended.ReadBuffer(b)
	if err != nil {
		t.Fatalf("ReadBuffer failed: %v", err)
	}
	if string(b.Bytes()) != "world" {
		t.Fatalf("expected 'world', got %q", string(b.Bytes()))
	}
}
