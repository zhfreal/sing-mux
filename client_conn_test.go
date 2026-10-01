package mux

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type mockCloseWriteConn struct {
	net.Conn
	closeWriteCalled atomic.Bool
	closeCalled      atomic.Bool
}

func (m *mockCloseWriteConn) CloseWrite() error {
	m.closeWriteCalled.Store(true)
	return nil
}

func (m *mockCloseWriteConn) Close() error {
	m.closeCalled.Store(true)
	if m.Conn != nil {
		return m.Conn.Close()
	}
	return nil
}

func TestClientConnCompilation(t *testing.T) {
	// Simple sanity test for sing-mux-mine clientConn
	t.Log("sing-mux-mine client_conn compiles and package loads cleanly")
}

func TestClientConnWithCloseWrite(t *testing.T) {
	p1, p2 := net.Pipe()
	defer p2.Close()

	mock := &mockCloseWriteConn{Conn: p1}
	var onCloseCalled atomic.Bool
	ws := &wrapStream{
		Conn: mock,
		onClose: func() {
			onCloseCalled.Store(true)
		},
	}

	rawConn := &clientConn{
		conn:        ws,
		destination: M.ParseSocksaddr("127.0.0.1:80"),
		ctx:         context.Background(),
	}

	cwConn := &clientConnWithCloseWrite{clientConn: rawConn}

	// 1. Verify clientConnWithCloseWrite implements N.WriteCloser
	var cw N.WriteCloser
	var ok bool
	cw, ok = any(cwConn).(N.WriteCloser)
	if !ok {
		t.Fatalf("expected clientConnWithCloseWrite to implement N.WriteCloser")
	}

	// 2. Verify that clientConnWithCloseWrite does NOT implement Upstream() (Bug 3 prevention)
	if _, hasUpstream := any(cwConn).(interface{ Upstream() any }); hasUpstream {
		t.Fatalf("clientConnWithCloseWrite must NOT implement Upstream() any to prevent relay loops from bypassing wrapper")
	}

	// 3. Test CloseWrite()
	err := cw.CloseWrite()
	if err != nil {
		t.Fatalf("CloseWrite failed: %v", err)
	}
	if !mock.closeWriteCalled.Load() {
		t.Fatalf("expected mock.CloseWrite to be called")
	}
	if onCloseCalled.Load() {
		t.Fatalf("CloseWrite must NOT trigger onClose callback")
	}
	if mock.closeCalled.Load() {
		t.Fatalf("CloseWrite must NOT trigger full Close")
	}

	// 4. Test Close() triggers onClose and underlying Close (Bug 1 prevention)
	err = cwConn.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !mock.closeCalled.Load() {
		t.Fatalf("expected mock.Close to be called on full Close")
	}
	if !onCloseCalled.Load() {
		t.Fatalf("expected onClose callback to be fired on full Close to maintain XMUX retirement accounting")
	}
}

func TestWrapStreamCloseWrite(t *testing.T) {
	mock := &mockCloseWriteConn{}
	ws := &wrapStream{Conn: mock}

	if err := ws.CloseWrite(); err != nil {
		t.Fatalf("ws.CloseWrite failed: %v", err)
	}
	if !mock.closeWriteCalled.Load() {
		t.Fatalf("expected mock.CloseWrite to be called by wrapStream.CloseWrite")
	}

	// wrapStreamCloseWrite wrapper test
	wscw := newWrapStreamCloseWrite(mock)
	mock.closeWriteCalled.Store(false)
	if err := wscw.CloseWrite(); err != nil {
		t.Fatalf("wscw.CloseWrite failed: %v", err)
	}
	if !mock.closeWriteCalled.Load() {
		t.Fatalf("expected mock.CloseWrite to be called by wrapStreamCloseWrite.CloseWrite")
	}
}

func TestClientPacketConnZeroHeadroom(t *testing.T) {
	out := &bytes.Buffer{}
	extPipe := &mockExtendedPipe{writeBuf: out}
	extConn := bufio.NewExtendedConn(extPipe)

	cpc := &clientPacketConn{
		AbstractConn:   extConn,
		conn:           extConn,
		requestWritten: true,
	}

	payload := []byte("test-client-payload-1234")
	zeroBuf := buf.NewSize(len(payload))
	_, _ = zeroBuf.Write(payload)

	err := cpc.WritePacket(zeroBuf, M.Socksaddr{})
	if err != nil {
		t.Fatalf("WritePacket failed: %v", err)
	}

	written := out.Bytes()
	if len(written) != 2+len(payload) {
		t.Fatalf("expected written len %d, got %d", 2+len(payload), len(written))
	}
}

func TestClientPacketAddrConnZeroHeadroom(t *testing.T) {
	out := &bytes.Buffer{}
	extPipe := &mockExtendedPipe{writeBuf: out}
	extConn := bufio.NewExtendedConn(extPipe)

	cpac := &clientPacketAddrConn{
		AbstractConn:   extConn,
		conn:           extConn,
		requestWritten: true,
	}

	dest := M.Socksaddr{
		Addr: netip.MustParseAddr("1.2.3.4"),
		Port: 8080,
	}

	payload := []byte("dns-query-bytes-48-length-padding-to-match")
	zeroBuf := buf.NewSize(len(payload))
	_, _ = zeroBuf.Write(payload)

	err := cpac.WritePacket(zeroBuf, dest)
	if err != nil {
		t.Fatalf("WritePacket with zero headroom failed: %v", err)
	}

	if out.Len() == 0 {
		t.Fatalf("expected written bytes, got 0")
	}
}

