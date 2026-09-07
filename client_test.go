package mux

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type mockSession struct {
	openCount   atomic.Int32
	numStreams  atomic.Int32
	closed      atomic.Bool
	canTake     bool
}

func (m *mockSession) Open(tcpTimeout time.Duration) (net.Conn, error) {
	m.openCount.Add(1)
	m.numStreams.Add(1)
	c1, _ := net.Pipe()
	return c1, nil
}

func (m *mockSession) Accept() (net.Conn, error) {
	c1, _ := net.Pipe()
	return c1, nil
}

func (m *mockSession) NumStreams() int {
	return int(m.numStreams.Load())
}

func (m *mockSession) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockSession) IsClosed() bool {
	return m.closed.Load()
}

func (m *mockSession) CanTakeNewRequest() bool {
	return m.canTake && !m.closed.Load()
}

func TestParseRange(t *testing.T) {
	tests := []struct {
		input    any
		expected Range
	}{
		{nil, Range{}},
		{"", Range{}},
		{"0", Range{}},
		{100, Range{Min: 100, Max: 100}},
		{"100", Range{Min: 100, Max: 100}},
		{"100-200", Range{Min: 100, Max: 200}},
		{" 100 - 200 ", Range{Min: 100, Max: 200}},
		{"200-100", Range{Min: 100, Max: 200}},
		{"-100", Range{}},
		{"-100-200", Range{}},
		{"invalid", Range{}},
	}

	for _, tc := range tests {
		got := parseRange(tc.input)
		if got != tc.expected {
			t.Errorf("parseRange(%v) = %+v, expected %+v", tc.input, got, tc.expected)
		}
	}
}

func TestSessionWrapperRetirement(t *testing.T) {
	raw := &mockSession{canTake: true}
	wrapper := &sessionWrapper{
		abstractSession: raw,
		createdAt:       time.Now(),
	}

	wrapper.leftRequests.Store(2)
	wrapper.leftReuseTimes.Store(2)
	wrapper.activeStreams.Store(1)

	now := time.Now()
	if wrapper.isRetired(now) {
		t.Fatal("expected wrapper not to be retired initially")
	}

	// Consume requests
	wrapper.ConsumeRequest()
	if wrapper.isRetired(now) {
		t.Fatal("expected wrapper not to be retired after 1 request")
	}

	wrapper.ConsumeRequest()
	if !wrapper.isRetired(now) {
		t.Fatal("expected wrapper to be retired after 2 requests")
	}

	// Test deadline expiration
	raw2 := &mockSession{canTake: true}
	wrapper2 := &sessionWrapper{
		abstractSession: raw2,
		createdAt:       time.Now().Add(-10 * time.Minute),
		unreusableAt:    time.Now().Add(-1 * time.Minute),
	}
	wrapper2.leftRequests.Store(100)
	wrapper2.leftReuseTimes.Store(100)
	if !wrapper2.isRetired(time.Now()) {
		t.Fatal("expected wrapper2 to be retired after deadline")
	}
}

func TestWrapStreamCloseCallback(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()

	var closedCount atomic.Int32
	ws := &wrapStream{
		Conn: c1,
		onClose: func() {
			closedCount.Add(1)
		},
	}

	_ = ws.Close()
	_ = ws.Close()

	if closedCount.Load() != 1 {
		t.Fatalf("expected onClose to be called exactly once, got %d", closedCount.Load())
	}
}

type testServiceHandler struct{}

func (h *testServiceHandler) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			break
		}
		_, _ = conn.Write(buf[:n])
	}
	return nil
}

func (h *testServiceHandler) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	return nil
}

type testDialer struct {
	dials atomic.Int32
	addr  net.Addr
}

func (d *testDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	d.dials.Add(1)
	return net.Dial(network, d.addr.String())
}

func (d *testDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, nil
}

func TestClient_SessionRetirementRollover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	service, err := NewService(ServiceOptions{
		Handler:          &testServiceHandler{},
		NewStreamContext: func(ctx context.Context, _ net.Conn) context.Context { return ctx },
	})
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				_ = service.NewConnection(ctx, c, M.Metadata{})
			}(conn)
		}
	}()

	dialer := &testDialer{addr: listener.Addr()}
	client, err := NewClient(Options{
		Dialer:           dialer,
		Protocol:         "yamux",
		HMaxRequestTimes: 1, // Retires immediately after 1 stream
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	dest := M.ParseSocksaddr("127.0.0.1:80")

	// First stream dials connection 1
	stream1, err := client.DialContext(ctx, "tcp", dest)
	if err != nil {
		t.Fatalf("DialContext 1 failed: %v", err)
	}
	defer stream1.Close()

	if dialer.dials.Load() != 1 {
		t.Fatalf("expected 1 dial, got %d", dialer.dials.Load())
	}

	// Verify stream 1 loopback echo works
	payload := []byte("hello stream 1")
	if _, err := stream1.Write(payload); err != nil {
		t.Fatalf("write to stream1 failed: %v", err)
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(stream1, buf); err != nil {
		t.Fatalf("read from stream1 failed: %v", err)
	}
	if string(buf) != string(payload) {
		t.Fatalf("payload mismatch: %s != %s", string(buf), string(payload))
	}

	// Second stream should trigger a new dial because connection 1 was retired by HMaxRequestTimes=1
	stream2, err := client.DialContext(ctx, "tcp", dest)
	if err != nil {
		t.Fatalf("DialContext 2 failed: %v", err)
	}
	defer stream2.Close()

	if dialer.dials.Load() != 2 {
		t.Fatalf("expected 2 dials after connection 1 retired, got %d", dialer.dials.Load())
	}

	// Close stream 1 to allow connection 1 to drain
	_ = stream1.Close()

	// Wait for connection 1 to drain and close
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		client.access.Lock()
		activeCount := 0
		for el := client.connections.Front(); el != nil; el = el.Next() {
			if !el.Value.IsClosed() {
				activeCount++
			}
		}
		client.access.Unlock()
		if activeCount == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	client.access.Lock()
	connCount := client.connections.Len()
	client.access.Unlock()
	if connCount > 1 {
		t.Fatalf("expected connection 1 to be evicted, remaining in list: %d", connCount)
	}
}
