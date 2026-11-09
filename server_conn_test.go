package mux

import (
	"io"
	"bytes"
	"net"
	"net/netip"
	"testing"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
)

type mockExtendedPipe struct {
	net.Conn
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
}

func (m *mockExtendedPipe) Read(p []byte) (n int, err error) {
	if m.readBuf == nil {
		return 0, io.EOF
	}
	return m.readBuf.Read(p)
}

func (m *mockExtendedPipe) Write(p []byte) (n int, err error) {
	return m.writeBuf.Write(p)
}

func (m *mockExtendedPipe) WriteBuffer(buffer *buf.Buffer) error {
	defer buffer.Release()
	_, err := m.writeBuf.Write(buffer.Bytes())
	return err
}

func (m *mockExtendedPipe) Close() error {
	return nil
}

func TestServerPacketConnZeroHeadroom(t *testing.T) {
	out := &bytes.Buffer{}
	extPipe := &mockExtendedPipe{writeBuf: out}
	extConn := bufio.NewExtendedConn(extPipe)

	spc := &serverPacketConn{
		ExtendedConn: extConn,
	}

	// Buffer with zero headroom (start == 0, cap == len)
	payload := []byte("test-payload-1234")
	zeroBuf := buf.NewSize(len(payload))
	_, _ = zeroBuf.Write(payload)

	err := spc.WritePacket(zeroBuf, M.Socksaddr{})
	if err != nil {
		t.Fatalf("WritePacket failed: %v", err)
	}

	// Check that statusSuccess (0) and 2-byte length were written
	written := out.Bytes()
	if len(written) != 1+2+len(payload) {
		t.Fatalf("expected written len %d, got %d", 1+2+len(payload), len(written))
	}
	if written[0] != statusSuccess {
		t.Fatalf("expected statusSuccess 0, got %d", written[0])
	}
}

func TestServerPacketAddrConnZeroHeadroom(t *testing.T) {
	out := &bytes.Buffer{}
	extPipe := &mockExtendedPipe{writeBuf: out}
	extConn := bufio.NewExtendedConn(extPipe)

	spac := &serverPacketAddrConn{
		ExtendedConn: extConn,
	}

	dest := M.Socksaddr{
		Addr: netip.MustParseAddr("1.2.3.4"),
		Port: 8080,
	}

	payload := []byte("dns-reply-bytes-48-length-padding-to-match")
	zeroBuf := buf.NewSize(len(payload))
	_, _ = zeroBuf.Write(payload)

	err := spac.WritePacket(zeroBuf, dest)
	if err != nil {
		t.Fatalf("WritePacket with zero headroom failed: %v", err)
	}

	written := out.Bytes()
	if written[0] != statusSuccess {
		t.Fatalf("expected statusSuccess 0, got %d", written[0])
	}
}

func TestH2MuxConnWrapperNilFlusher(t *testing.T) {
	out := &bytes.Buffer{}
	extPipe := &mockExtendedPipe{writeBuf: out}
	extConn := bufio.NewExtendedConn(extPipe)

	wrapper := newHTTP2Wrapper(extConn, nil)
	n, err := wrapper.Write([]byte("test-data"))
	if err != nil || n != 9 {
		t.Fatalf("Write failed: n=%d, err=%v", n, err)
	}

	b := buf.New()
	_, _ = b.WriteString("buffer-data")
	err = wrapper.WriteBuffer(b)
	if err != nil {
		t.Fatalf("WriteBuffer failed: %v", err)
	}

	if !bytes.Equal(out.Bytes(), []byte("test-databuffer-data")) {
		t.Fatalf("unexpected output: %q", out.Bytes())
	}
}

func TestServerPacketConnShortBufferRespectsLenNotCap(t *testing.T) {
	inBuf := &bytes.Buffer{}
	// Write a 20-byte UDP packet frame (2-byte length = 20, followed by 20 bytes)
	_ = writeUint16(inBuf, 20)
	inBuf.Write(bytes.Repeat([]byte{'A'}, 20))

	extPipe := &mockExtendedPipe{readBuf: inBuf, writeBuf: &bytes.Buffer{}}
	spc := &serverPacketConn{
		ExtendedConn: bufio.NewExtendedConn(extPipe),
	}

	// Backing array has capacity 64, but caller passes slice of length 10.
	// ReadFrom must return io.ErrShortBuffer and NEVER overwrite backing[10:20].
	backing := bytes.Repeat([]byte{'Z'}, 64)
	shortSlice := backing[:10]
	_, _, err := spc.ReadFrom(shortSlice)
	if err != io.ErrShortBuffer {
		t.Fatalf("expected io.ErrShortBuffer when len(p)=10 < packet length 20, got %v", err)
	}
	for i := 10; i < 64; i++ {
		if backing[i] != 'Z' {
			t.Fatalf("ReadFrom corrupted backing array past len(p) at index %d: got %c", i, backing[i])
		}
	}
}
