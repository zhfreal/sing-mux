package mux

import (
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
	writeBuf *bytes.Buffer
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
